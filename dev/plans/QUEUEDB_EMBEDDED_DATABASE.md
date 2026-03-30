# Plan: MonsterMQ Embedded Queue Database (QueueDB)

## Context

MonsterMQ currently stores QoS > 0 queued messages for persistent sessions in PostgreSQL/MongoDB/SQLite using two tables (`queuedmessages` + `queuedmessagesclients`). This design has significant performance issues: JOINs on every fetch, non-atomic fetch+mark race conditions, 3-step purge cycles, and payload duplication per subscriber.

The solution uses a purpose-built embedded queue database with **sequence-based inboxes** (per-client monotonic sequence numbers instead of UUIDs), **reference-counted payloads** (store once, reference many), and **Hazelcast IMap** (already in MonsterMQ) for clustering with zero new dependencies.

## Design Goals

- Store each payload **once** with N client references (reference counting)
- Run **embedded** in the broker for single-node deployments (pure in-memory, `ConcurrentHashMap`)
- Support **clustering** via **Hazelcast IMap + EntryProcessor** — zero new dependencies
- Communicate **exclusively via Vert.x event bus** (same API regardless of deployment mode)
- Use **sequence numbers** (not UUIDs) for per-client FIFO ordering
- **Small footprint** — purpose-built for this one use case

---

## Key Design Ideas

| Concept | Implementation |
|---------|----------------|
| Per-inbox sequential key range | Per-client `TreeMap<Long, String>` with monotonic sequence counter |
| ACK by advancing sequence pointer | ACK by advancing `ackSeq` — all messages with seq ≤ ackSeq are delivered |
| Payload deduplication | `PayloadStore` — store once, reference many via ref counting |
| Distributed partitioning | Hazelcast consistent hashing (clustered) / 256-stripe lock (embedded) |
| Unified QoS storage | Storage layer treats QoS 1 and QoS 2 identically |
| Stale write prevention | Session generation counter to prevent stale ACKs after reconnect |

### Why Sequence Numbers Beat UUIDs

Monotonic sequence numbers per inbox instead of UUIDs:
- **O(1) range scan**: fetch messages from `startSeq` to `nextSeq` — no index lookup needed
- **O(1) ACK**: advance `ackSeq` pointer, bulk-delete everything below — no per-message status update
- **Natural FIFO**: sequence order IS insertion order
- **Compact**: 8-byte long vs 36-byte UUID string

Our current approach (UUID + status column + 3-step purge) becomes:
```
Before: fetch WHERE status=0 → UPDATE status=1 → UPDATE status=2 → DELETE status=2 → DELETE orphans
After:  fetch [ackSeq..nextSeq] → advance ackSeq (everything below is garbage collected)
```

---

## Architecture

### Deployment Modes

```
Mode A: Embedded (single node)       Mode B: Clustered (Hazelcast IMap)
┌──────────────────────┐            ┌──────────────┐  Hazelcast  ┌──────────────┐
│  Broker JVM          │            │  Broker JVM  │◄───────────►│  Broker JVM  │
│  ┌────────────────┐  │            │  QueueStore  │  IMap +     │  QueueStore  │
│  │ QueueDBEngine  │  │            │  Hazelcast   │  EntryProc  │  Hazelcast   │
│  │ ConcurrentHash │  │            │  (auto-part) │             │  (auto-part) │
│  │ (local only)   │  │            └──────────────┘             └──────────────┘
│  └────────────────┘  │
└──────────────────────┘
```

**Mode A** — Single node: pure `ConcurrentHashMap` + `StripedLock`. Zero overhead.
**Mode B** — Clustered: Hazelcast IMap auto-partitions inboxes across nodes. `EntryProcessor` runs operations on the partition owner (no serialization of data to caller). When a node joins/leaves, Hazelcast rebalances automatically.

### Core Components

```
QueueDBEngine (pure Kotlin, no Vert.x dependency — used in Mode A)
├── PayloadStore: ConcurrentHashMap<String, PayloadEntry>    — one payload per unique message UUID
├── InboxStore: ConcurrentHashMap<String, Inbox>             — per-client inbox with sequence tracking
├── StripedLock (256 stripes)                                — per-client concurrency
└── Journal (optional)                                       — append-only persistence

QueueDBVerticle (Vert.x verticle — wraps either mode)
├── Wraps QueueDBEngine (embedded) or QueueStoreHazelcast (clustered)
├── Registers event bus consumers for mq.queue.*
└── Runs blocking operations via executeBlocking

IQueueStore (interface — extracted from ISessionStoreSync)
├── QueueStoreEmbedded    — wraps QueueDBEngine directly (Mode A)
├── QueueStoreHazelcast   — wraps Hazelcast IMap + EntryProcessor (Mode B)
├── QueueStoreEventBus    — sends event bus messages to QueueDBVerticle
├── QueueStorePostgres    — existing code extracted from SessionStorePostgres
├── QueueStoreSQLite      — existing code extracted from SessionStoreSQLite
└── QueueStoreMongoDB     — existing code extracted from SessionStoreMongoDB
```

---

## Data Structures

### PayloadEntry — stored once per unique message

```kotlin
class PayloadEntry(
    val messageUuid: String,          // Time-ordered UUID (from UuidCreator)
    val messageId: Int,
    val topicName: String,
    val payload: ByteArray,
    val qosLevel: Int,
    val isRetain: Boolean,
    val publisherId: String,          // Publishing client ID
    val creationTime: Long,           // Epoch millis
    val messageExpiryInterval: Long?, // Seconds, null = no expiry
    var refCount: Int                 // Decremented on ACK, deleted at 0
    // Embedded: use AtomicInteger wrapper. Hazelcast: EntryProcessor ensures atomicity.
)
```

### Inbox — per-client state

```kotlin
class Inbox(
    val clientId: String,
    var nextSeq: Long = 0,                                    // Next sequence to assign on enqueue
    var ackSeq: Long = 0,                                     // Everything < ackSeq is delivered
    var inFlightSeq: Long = 0,                                // Everything in [ackSeq, inFlightSeq) is in-flight
    val messages: TreeMap<Long, String> = TreeMap(),           // seq → messageUuid (sorted, range-scannable)
    var generation: Int = 0                                    // Incremented on reconnect (prevents stale ACKs)
)
```

### Why TreeMap instead of ArrayDeque

Key insight: with sequence numbers, the data structure is a **sorted map** from seq→message, not a deque. This gives:
- **O(log N) insert** at `nextSeq`
- **O(1) range scan** from `ackSeq` to `inFlightSeq` (TreeMap's `subMap`)
- **O(log N) bulk delete** via `headMap(ackSeq).clear()` — no per-message iteration
- **No separate pending/inFlight collections** — just two pointers into the same sorted structure

```
Inbox state at any time:

  seq:  0   1   2   3   4   5   6   7   8   9
        ▲               ▲               ▲
      ackSeq         inFlightSeq      nextSeq
        │               │               │
        └── delivered ──┘── in-flight ──┘── pending (not yet sent)
            (can GC)       (sent, awaiting ACK)
```

- `[0, ackSeq)` — delivered, can be garbage collected
- `[ackSeq, inFlightSeq)` — in-flight, sent to client but not yet ACKed
- `[inFlightSeq, nextSeq)` — pending, not yet sent to client
- **Fetch**: read `[inFlightSeq, min(inFlightSeq+limit, nextSeq))`, advance `inFlightSeq`
- **ACK**: advance `ackSeq` to ACKed seq+1, garbage collect `headMap(ackSeq)`
- **Reset** (reconnect): set `inFlightSeq = ackSeq` (moves in-flight back to pending)

### Thread Safety: StripedLock

```kotlin
class StripedLock(val stripes: Int = 256) {
    private val locks = Array(stripes) { ReentrantLock() }
    fun lockFor(clientId: String): ReentrantLock = locks[clientId.hashCode().absoluteValue % stripes]
}
```

Operations on different clients are fully concurrent. Same client serialized by stripe.

---

## Reference Counting for Payload Deduplication

```
enqueueMessages(msg, [A, B, C])
  → payloads[uuid] = PayloadEntry(msg, refCount=3)
  → A.messages[A.nextSeq++] = uuid
  → B.messages[B.nextSeq++] = uuid
  → C.messages[C.nextSeq++] = uuid

Client A ACKs (advances ackSeq):
  → For each seq in A.messages.headMap(newAckSeq):
      uuid = A.messages[seq]
      refCount.decrementAndGet()
      if refCount == 0 → payloads.remove(uuid)
  → A.messages.headMap(newAckSeq).clear()
  → A.ackSeq = newAckSeq
```

Edge cases:
- **Client session deleted**: iterate all entries in inbox, decrement refCount for each, clear inbox
- **Duplicate ACK**: if newAckSeq ≤ current ackSeq → no-op
- **Stale ACK after reconnect**: check `generation` counter, reject if mismatched

---

## Event Bus Protocol

```kotlin
object QueueDB {
    const val ENQUEUE        = "mq.queue.enqueue"
    const val FETCH_PENDING  = "mq.queue.fetch"       // Atomic: fetch AND advance inFlightSeq
    const val ACK            = "mq.queue.ack"          // Advance ackSeq, GC entries + payloads
    const val RESET          = "mq.queue.reset"        // Set inFlightSeq = ackSeq (reconnect)
    const val REMOVE_CLIENT  = "mq.queue.remove"       // Delete entire inbox
    const val PURGE_EXPIRED  = "mq.queue.purge.expired"
    const val COUNT_ALL      = "mq.queue.count.all"
    const val COUNT_CLIENT   = "mq.queue.count.client"
}
```

| Operation | Pattern | Request | Reply |
|-----------|---------|---------|-------|
| ENQUEUE | send (fire & forget) | BrokerMessages + client lists | (none) |
| FETCH_PENDING | request/reply | clientId, limit | List of (seq, BrokerMessage) |
| ACK | send | clientId, ackSeq, generation | (none) |
| RESET | send | clientId | (none) |
| REMOVE_CLIENT | send | clientId | (none) |
| PURGE_EXPIRED | request/reply | (empty) | Int (count) |

**Key simplification vs current design**: No separate `markMessagesInFlight` or `markMessageDelivered` operations. `FETCH_PENDING` atomically advances `inFlightSeq`. `ACK` advances `ackSeq` and garbage collects. Two operations instead of five.

---

## Clustering Strategy: Hazelcast IMap + EntryProcessor

### Why Hazelcast IMap

- **Already in MonsterMQ**: Hazelcast 5.3.8 via `vertx-hazelcast`, used for retained messages, feature flags, leader election
- **Automatic partitioning**: Hazelcast distributes IMap entries across nodes by key hash — each clientId's inbox lives on exactly one node
- **EntryProcessor**: Executes operations **on the partition owner node** — no serialization of the Inbox to the caller. This is critical for performance: the TreeMap with thousands of messages never leaves its owning node.
- **Automatic rebalancing**: When nodes join/leave, Hazelcast migrates partitions seamlessly
- **Backup copies**: Configurable sync/async backup count for redundancy (default: 1 sync backup)

### Two IMaps

```kotlin
// Inbox per client — auto-partitioned by clientId
val inboxes: IMap<String, Inbox> = hazelcast.getMap("queuedb.inboxes")

// Payloads stored once — auto-partitioned by messageUuid
val payloads: IMap<String, PayloadEntry> = hazelcast.getMap("queuedb.payloads")
```

### EntryProcessor Pattern

Operations execute on the node that owns the partition — no network transfer of Inbox data:

```kotlin
// Enqueue: runs on the partition owner of each clientId
class EnqueueProcessor(
    val messageUuid: String,
    val qosLevel: Int
) : EntryProcessor<String, Inbox, Void?> {
    override fun process(entry: MutableMap.MutableEntry<String, Inbox>): Void? {
        val inbox = entry.value ?: Inbox(entry.key)
        inbox.messages[inbox.nextSeq++] = messageUuid
        entry.setValue(inbox)
        return null
    }
}

// Fetch: atomically reads pending messages and advances inFlightSeq
class FetchProcessor(val limit: Int) : EntryProcessor<String, Inbox, List<Pair<Long, String>>> {
    override fun process(entry: MutableMap.MutableEntry<String, Inbox>): List<Pair<Long, String>> {
        val inbox = entry.value ?: return emptyList()
        val pending = inbox.messages.subMap(inbox.inFlightSeq, inbox.nextSeq)
        val result = pending.entries.take(limit).map { it.key to it.value }
        inbox.inFlightSeq = if (result.isNotEmpty()) result.last().first + 1 else inbox.inFlightSeq
        entry.setValue(inbox)
        return result
    }
}

// ACK: advances ackSeq, returns UUIDs whose refCount should be decremented
class AckProcessor(
    val newAckSeq: Long,
    val generation: Int
) : EntryProcessor<String, Inbox, List<String>> {
    override fun process(entry: MutableMap.MutableEntry<String, Inbox>): List<String> {
        val inbox = entry.value ?: return emptyList()
        if (inbox.generation != generation || newAckSeq <= inbox.ackSeq) return emptyList()
        val delivered = inbox.messages.headMap(newAckSeq)
        val uuids = delivered.values.toList()
        delivered.clear()
        inbox.ackSeq = newAckSeq
        entry.setValue(inbox)
        return uuids  // Caller decrements refCount in payloads map
    }
}
```

### Payload Reference Counting (Distributed)

```kotlin
// After ACK returns list of UUIDs to release:
fun releasePayloads(uuids: List<String>) {
    for (uuid in uuids) {
        // AtomicInteger doesn't serialize well in IMap — use EntryProcessor
        val remaining = payloads.executeOnKey(uuid, DecrementRefCountProcessor())
        if (remaining != null && remaining <= 0) {
            payloads.remove(uuid)
        }
    }
}

class DecrementRefCountProcessor : EntryProcessor<String, PayloadEntry, Int?> {
    override fun process(entry: MutableMap.MutableEntry<String, PayloadEntry>): Int? {
        val payload = entry.value ?: return null
        payload.refCount--
        if (payload.refCount <= 0) {
            entry.setValue(null) // Remove from map
            return 0
        }
        entry.setValue(payload)
        return payload.refCount
    }
}
```

### Hazelcast Serialization

MonsterMQ currently uses default Java serialization for Hazelcast. For QueueDB, we should implement `IdentifiedDataSerializable` for `Inbox` and `PayloadEntry` to avoid the overhead of Java serialization:

```kotlin
class Inbox(...) : IdentifiedDataSerializable {
    override fun getFactoryId() = QUEUEDB_FACTORY_ID
    override fun getClassId() = INBOX_CLASS_ID
    override fun writeData(out: ObjectDataOutput) {
        out.writeString(clientId)
        out.writeLong(nextSeq)
        out.writeLong(ackSeq)
        out.writeLong(inFlightSeq)
        out.writeInt(generation)
        out.writeInt(messages.size)
        messages.forEach { (seq, uuid) -> out.writeLong(seq); out.writeString(uuid) }
    }
    override fun readData(inp: ObjectDataInput) { /* reverse */ }
}
```

Register the factory in Hazelcast config (Monster.kt):
```kotlin
hazelcastConfig.serializationConfig.addDataSerializableFactory(
    QUEUEDB_FACTORY_ID, QueueDBDataSerializableFactory()
)
```

### Client Reconnect to Different Node

With Hazelcast IMap, this is **transparent**. The clientId maps to the same partition regardless of which broker node the client connects to. The EntryProcessor runs on the partition owner. No merge logic needed.

```
Client A connects to Node 1 → EnqueueProcessor runs on partition owner (could be Node 2)
Client A disconnects
Client A reconnects to Node 3 → FetchProcessor runs on same partition owner (Node 2)
```

### Backup and Fault Tolerance

Configure via Hazelcast map config:
```kotlin
val inboxMapConfig = MapConfig("queuedb.inboxes")
    .setBackupCount(1)           // 1 synchronous backup (default)
    .setAsyncBackupCount(0)      // No async backups
    .setEvictionConfig(EvictionConfig().setEvictionPolicy(EvictionPolicy.NONE))
    .setTimeToLiveSeconds(0)     // No TTL — managed by QueueDB

hazelcastConfig.addMapConfig(inboxMapConfig)
```

If a node dies, Hazelcast promotes the backup partition to primary — no data loss for queued messages.

---

## Persistence (Optional)

Default: pure in-memory. Opt-in via append-only journal + snapshots.

- **Journal**: Memory-mapped file (same pattern as existing `LoggerQueueDisk`), records ENQUEUE/ACK/RESET
- **Snapshot**: Periodic full state dump, journal truncated after
- **Recovery**: Load snapshot → replay journal → rebuild refCounts defensively

```
Journal entry format:
[4 bytes: length][1 byte: op type][variable: op data][4 bytes: CRC32]

Op types:
  0x01 ENQUEUE:  [uuid][topic][payload][qos][retain][publisherId][creationTime]
                 [expiryInterval][clientCount][client1]...[clientN]
  0x02 ACK:      [clientId][ackSeq][generation]
  0x03 RESET:    [clientId][generation]
  0x04 REMOVE:   [clientId]
```

---

## Memory Footprint

Per unique payload: ~764 bytes (typical 400-byte MQTT message)
Per inbox entry: ~80 bytes (seq long + UUID string ref in TreeMap node)

| Scenario | Clients | Msgs/Client | Fan-out | Unique Payloads | Total RAM |
|----------|---------|-------------|---------|-----------------|-----------|
| Small | 100 | 1,000 | 2 | 50K | ~46 MB |
| Medium | 500 | 5,000 | 3 | 833K | ~770 MB |
| Large | 1,000 | 10,000 | 5 | 2M | ~2.4 GB |

Configurable limits:
```yaml
QueueDB:
  Type: EMBEDDED               # EMBEDDED | HAZELCAST | POSTGRES | SQLITE | MONGODB
  Persistence:                 # Only for EMBEDDED mode
    Enabled: false
    Path: "data/queuedb"
    SnapshotIntervalSeconds: 300
    JournalSyncMode: ASYNC
  Hazelcast:                   # Only for HAZELCAST mode
    BackupCount: 1             # Synchronous backups (0 = no backup)
    AsyncBackupCount: 0
  Limits:
    MaxMessagesPerClient: 100000
    MaxTotalPayloads: 10000000
    MemoryPressureThreshold: 0.85
```

**Type selection logic**: If `Type` is not set, auto-detect: use `HAZELCAST` when `-cluster` is active, `EMBEDDED` otherwise. This gives zero-config clustering.

---

## Integration into Existing Code

### New interface: `IQueueStore`

Extract all queue methods from `ISessionStoreSync` into dedicated interface:

```kotlin
interface IQueueStore {
    fun enqueueMessages(messages: List<Pair<BrokerMessage, List<String>>>)
    fun fetchPendingMessages(clientId: String, limit: Int): List<BrokerMessage>
    fun markMessageDelivered(clientId: String, messageUuid: String)
    fun resetInFlightMessages(clientId: String)
    fun removeMessages(messages: List<Pair<String, String>>)
    fun purgeDeliveredMessages(): Int
    fun purgeExpiredMessages(): Int
    fun purgeQueuedMessages()
    fun countQueuedMessages(): Long
    fun countQueuedMessagesForClient(clientId: String): Long
}
```

The embedded implementation internally uses the sequence-based model but exposes the same interface for backward compatibility with existing PostgreSQL/SQLite/MongoDB implementations.

---

## Files to Create

| File | Description |
|------|-------------|
| `stores/IQueueStore.kt` | New interface (extracted from ISessionStoreSync) |
| `stores/queuedb/QueueDBEngine.kt` | Core engine: PayloadStore, InboxStore, StripedLock, operations |
| `stores/queuedb/QueueDBVerticle.kt` | Event bus consumers wrapping engine |
| `stores/queuedb/QueueStoreEmbedded.kt` | IQueueStore → direct QueueDBEngine calls (Mode A) |
| `stores/queuedb/QueueStoreHazelcast.kt` | IQueueStore → Hazelcast IMap + EntryProcessor (Mode B) |
| `stores/queuedb/QueueStoreEventBus.kt` | IQueueStore → event bus messages |
| `stores/queuedb/HazelcastProcessors.kt` | EntryProcessor implementations (Enqueue, Fetch, Ack, Reset, Remove, DecrementRefCount) |
| `stores/queuedb/QueueDBSerializable.kt` | IdentifiedDataSerializable for Inbox + PayloadEntry |
| `stores/queuedb/Journal.kt` | Append-only journal (optional persistence, Mode A only) |
| `stores/queuedb/Snapshot.kt` | Snapshot serialization/deserialization |
| `stores/QueueStorePostgres.kt` | Extracted from SessionStorePostgres |
| `stores/QueueStoreSQLite.kt` | Extracted from SessionStoreSQLite |
| `stores/QueueStoreMongoDB.kt` | Extracted from SessionStoreMongoDB |

## Files to Modify

| File | Change |
|------|--------|
| `stores/ISessionStoreSync.kt` | Remove queue methods (moved to IQueueStore) |
| `stores/ISessionStoreAsync.kt` | Remove queue methods |
| `stores/SessionStoreAsync.kt` | Remove queue method wrappers |
| `stores/dbs/postgres/SessionStorePostgres.kt` | Extract queue ops to QueueStorePostgres |
| `stores/dbs/sqlite/SessionStoreSQLite.kt` | Extract queue ops to QueueStoreSQLite |
| `stores/dbs/mongodb/SessionStoreMongoDB.kt` | Extract queue ops to QueueStoreMongoDB |
| `handlers/SessionHandler.kt` | Accept IQueueStore, wire queueWorkerThread to it |
| `MqttClient.kt` | Use IQueueStore for queue operations |
| `Monster.kt` | Add QueueDB config, Hazelcast map/serializer config, deploy QueueDBVerticle, create IQueueStore |
| `bus/EventBusAddresses.kt` | Add QueueDB address namespace |

---

## Implementation Phases

### Phase 1: Core Engine + Interface (2-3 days)
- Create `IQueueStore` interface
- Implement `QueueDBEngine` with `PayloadStore`, `Inbox` (TreeMap + sequence pointers), `StripedLock`
- Implement `QueueStoreEmbedded` wrapping `QueueDBEngine`
- Reference counting with `AtomicInteger`
- Unit tests for all operations

### Phase 2: Hazelcast Clustering (2-3 days)
- Implement `IdentifiedDataSerializable` for `Inbox` and `PayloadEntry`
- Implement all `EntryProcessor` classes (Enqueue, Fetch, Ack, Reset, Remove, DecrementRefCount)
- Implement `QueueStoreHazelcast` using IMap + EntryProcessors
- Configure Hazelcast map settings in `Monster.kt` (backup count, eviction policy, serializer factory)
- Unit + integration tests

### Phase 3: Event Bus Integration (1-2 days)
- Add `QueueDB` addresses to `EventBusAddresses`
- Implement `QueueDBVerticle` + `QueueStoreEventBus`
- Wire into `Monster.kt` / `SessionHandler`
- Auto-detect: `HAZELCAST` when clustered, `EMBEDDED` when standalone

### Phase 4: Extract Queue Ops from Session Stores (1 day)
- Extract queue methods from PostgreSQL/SQLite/MongoDB session stores
- Update `SessionHandler` to use `IQueueStore` separately

### Phase 5: Persistence (1-2 days, optional — Mode A only)
- Append-only journal using `MappedByteBuffer`
- Snapshot serialization/deserialization
- Recovery on startup
- Note: In HAZELCAST mode, persistence is handled by Hazelcast's backup mechanism

---

## Verification

1. **Unit tests**: All QueueDBEngine operations (enqueue, fetch, ACK via ackSeq advance, reset, purge, refcount lifecycle, expiry, per-client limits, generation counter)
2. **Integration tests**: Run existing `tests/mqtt3/` and `tests/mqtt5/` pytest suites with `QueueDB.Type: EMBEDDED`
3. **Hazelcast cluster test**: 2-node cluster with `QueueDB.Type: HAZELCAST`, publish QoS 1 messages, disconnect client, reconnect to other node, verify delivery — validates that EntryProcessor correctly routes to partition owner
4. **Hazelcast node failure test**: 3-node cluster, kill one node, verify no message loss (backup promotion)
5. **Memory test**: Enqueue 100K messages, verify footprint matches estimates, verify cleanup after ACK
6. **Persistence test**: Enqueue messages (embedded mode), kill broker, restart, verify messages recovered
7. **Sequence ordering test**: Verify strict FIFO delivery per client across enqueue/fetch/ack/reset cycles
8. **EntryProcessor serialization test**: Verify Inbox and PayloadEntry serialize/deserialize correctly with IdentifiedDataSerializable
