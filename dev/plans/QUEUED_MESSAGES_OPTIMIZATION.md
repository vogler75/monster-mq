# Queued Messages Optimization Plan

## Problem Statement

The queued message implementations for QoS > 0 persistent sessions (PostgreSQL and MongoDB) have several performance bottlenecks and can benefit from ideas used by PGMQ (PostgreSQL Message Queue).

## Current Architecture

### Tables

- **queuedmessages**: Stores message content once (topic, payload, qos, etc.), keyed by `message_uuid`
- **queuedmessagesclients**: Maps each message to each subscribing client with a `status` column (0=pending, 1=in-flight, 2=delivered)

Relationship is one-to-many: one message can have multiple client mappings.

### Current Flow

```
Enqueue:
  INSERT INTO queuedmessages ... ON CONFLICT DO NOTHING
  INSERT INTO queuedmessagesclients (status=0) ... ON CONFLICT DO NOTHING

Fetch pending:
  SELECT ... FROM queuedmessages JOIN queuedmessagesclients WHERE status=0

Mark in-flight:
  UPDATE queuedmessagesclients SET status=1 WHERE status=0  (separate call)

Mark delivered:
  UPDATE queuedmessagesclients SET status=2

Purge (3 steps):
  1. DELETE FROM queuedmessagesclients WHERE status=2
  2. DELETE expired messages via subquery
  3. DELETE orphaned queuedmessages WHERE NOT EXISTS (... queuedmessagesclients)
```

### Identified Bottlenecks

| Issue | Severity | Description |
|-------|----------|-------------|
| Fetch + mark not atomic | Critical | `fetchPendingMessages` and `markMessagesInFlight` are two separate operations — race condition in clustered mode |
| JOIN on two tables | High | Every fetch requires JOIN between queuedmessages and queuedmessagesclients |
| 3-step purge cycle | Medium | Mark delivered → purge mappings → purge orphan messages |
| Expired messages fetched then skipped in Kotlin | Medium | Expiry check happens after DB fetch, wasting I/O |
| `markMessageDelivered` missing status guard | Medium | No `AND status = 1` condition on the UPDATE |
| No transaction rollback on error | Medium | Catch blocks log but don't rollback |
| Single DB connection, no pooling | Low | All operations serialize through one connection |

---

## Phase 1: Quick Wins (No Schema Change)

Improve the current two-table design without breaking changes.

### 1.1 Atomic Fetch-and-Lock with `FOR UPDATE SKIP LOCKED`

Replace the separate `fetchPendingMessages()` + `markMessagesInFlight()` with a single atomic CTE:

```sql
WITH cte AS (
    SELECT c.ctid, m.message_uuid, m.message_id, m.topic, m.payload, m.qos,
           m.retained, m.client_id, m.creation_time, m.message_expiry_interval
    FROM queuedmessagesclients c
    JOIN queuedmessages m USING (message_uuid)
    WHERE c.client_id = $1
      AND c.status = 0
      AND (m.message_expiry_interval IS NULL
           OR (($2 - m.creation_time) / 1000) < m.message_expiry_interval)
    ORDER BY m.message_uuid ASC
    LIMIT $3
    FOR UPDATE OF c SKIP LOCKED
)
UPDATE queuedmessagesclients SET status = 1
FROM cte WHERE queuedmessagesclients.ctid = cte.ctid
RETURNING cte.*;
```

**Benefits:**
- Atomic: no race condition between fetch and mark
- `SKIP LOCKED`: no blocking between concurrent consumers (cluster-safe)
- Expiry filtering in SQL: no wasted I/O for expired messages
- Eliminates separate `markMessagesInFlight()` call

**Files to change:**
- `broker/src/main/kotlin/stores/dbs/postgres/SessionStorePostgres.kt` — merge `fetchPendingMessages()` and `markMessagesInFlight()` into single method
- `broker/src/main/kotlin/stores/ISessionStore.kt` / `ISessionStoreAsync.kt` — add new `fetchAndLockPendingMessages()` interface method
- `broker/src/main/kotlin/MqttClient.kt` — update `fetchNextMessageFromCacheOrDb()` to use new single method

### 1.2 Add Status Guard to `markMessageDelivered`

Change:
```sql
UPDATE queuedmessagesclients SET status = 2 WHERE client_id = ? AND message_uuid = ?
```
To:
```sql
UPDATE queuedmessagesclients SET status = 2 WHERE client_id = ? AND message_uuid = ? AND status = 1
```

### 1.3 Add Rollback on Errors

Add `connection.rollback()` in all catch blocks to prevent aborted transaction state.

### 1.4 Move Expiry Filtering to SQL

Add expiry condition to the fetch query (already included in 1.1 CTE above) so expired messages are never fetched from the database.

---

## Phase 2: Schema Redesign (PGMQ-Inspired)

Redesign the queued messages schema for optimal queue performance. This is a **breaking change** requiring schema migration.

### 2.1 Single Table Design with Visibility Timeout

Replace two tables with one denormalized table:

```sql
CREATE TABLE IF NOT EXISTS queuedmessages (
    msg_id BIGINT GENERATED ALWAYS AS IDENTITY,
    message_uuid VARCHAR(36) NOT NULL,
    client_id VARCHAR(65535) NOT NULL,       -- subscriber client ID
    topic TEXT NOT NULL,
    payload BYTEA,
    qos INT NOT NULL,
    retained BOOLEAN NOT NULL DEFAULT FALSE,
    publisher_id VARCHAR(65535),             -- publishing client ID
    creation_time BIGINT NOT NULL,
    message_expiry_interval BIGINT,
    vt TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),  -- visibility timeout
    read_ct INT NOT NULL DEFAULT 0,          -- delivery attempt counter
    PRIMARY KEY (msg_id)
);

CREATE INDEX queuedmessages_fetch_idx
    ON queuedmessages (client_id, vt ASC);

CREATE INDEX queuedmessages_client_uuid_idx
    ON queuedmessages (client_id, message_uuid);
```

**Key changes:**
- **`msg_id BIGINT IDENTITY`** instead of UUID ordering — sequential, sortable, no UUID comparison overhead
- **`client_id` in same table** — no JOIN needed, each subscriber gets their own row
- **`vt` (visibility timeout)** replaces integer `status` column:
  - `vt <= now()` = pending (visible, ready to read)
  - `vt > now()` = in-flight (invisible, being processed)
  - Message ACKed = row deleted immediately
- **`read_ct`** tracks delivery attempts for monitoring/debugging
- **Payload duplication** per subscriber — trade-off: more storage, but eliminates JOIN entirely

### 2.2 New Operations

**Enqueue** (replaces two INSERTs):
```sql
INSERT INTO queuedmessages
    (message_uuid, client_id, topic, payload, qos, retained, publisher_id,
     creation_time, message_expiry_interval)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
```
One INSERT per subscriber. Batch with `addBatch()` / `executeBatch()` as before.

**Fetch and lock** (replaces SELECT + UPDATE):
```sql
WITH cte AS (
    SELECT msg_id FROM queuedmessages
    WHERE client_id = $1
      AND vt <= clock_timestamp()
      AND (message_expiry_interval IS NULL
           OR ((EXTRACT(EPOCH FROM clock_timestamp()) * 1000 - creation_time) / 1000)
              < message_expiry_interval)
    ORDER BY msg_id ASC
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE queuedmessages m SET
    vt = clock_timestamp() + make_interval(secs => $3),
    read_ct = read_ct + 1
FROM cte WHERE m.msg_id = cte.msg_id
RETURNING m.msg_id, m.message_uuid, m.topic, m.payload, m.qos, m.retained,
          m.publisher_id, m.creation_time, m.message_expiry_interval;
```
- `$3` = visibility timeout in seconds (e.g. 30s, configurable)
- Single atomic operation: fetch + lock + mark in-flight

**ACK / Delete** (replaces mark-delivered + purge):
```sql
DELETE FROM queuedmessages WHERE client_id = $1 AND message_uuid = $2
```
No more status=2 → purge cycle. One step.

**Reset on reconnect** (replaces `resetInFlightMessages`):
```sql
UPDATE queuedmessages SET vt = now() WHERE client_id = $1 AND vt > now()
```
Makes all in-flight messages visible again. Or simply let the visibility timeout expire naturally.

**Purge expired messages:**
```sql
DELETE FROM queuedmessages
WHERE message_expiry_interval IS NOT NULL
  AND creation_time IS NOT NULL
  AND ((EXTRACT(EPOCH FROM now()) * 1000 - creation_time) / 1000) >= message_expiry_interval
```
Single statement, no subqueries, no orphan cleanup needed.

**Remove all messages for client** (on session cleanup):
```sql
DELETE FROM queuedmessages WHERE client_id = $1
```

### 2.3 Visibility Timeout Configuration

Add a configurable visibility timeout parameter:

```yaml
# config.yaml
QueueVisibilityTimeoutSeconds: 30   # default 30 seconds
```

The VT should be longer than the expected PUBACK round-trip time but short enough that failed deliveries are retried quickly.

### 2.4 Migration Strategy

Since queued messages are transient (pending deliveries), migration can be simple:

1. Create new `queuedmessages` table with new schema (different name temporarily, e.g. `queuedmessages_v2`)
2. Deploy new code that writes to new table
3. Drain old tables (let existing messages be delivered or expire)
4. Drop old `queuedmessages` and `queuedmessagesclients` tables
5. Rename `queuedmessages_v2` to `queuedmessages`

Alternatively, since these are in-flight messages, a clean restart with empty queue tables is acceptable in most deployments.

---

---

## MongoDB Implementation

The MongoDB session store (`SessionStoreMongoDB.kt`) uses the same two-collection design and has analogous bottlenecks plus some MongoDB-specific issues.

### Current MongoDB Architecture

Same two-collection pattern:
- **queuedmessages** collection: message content documents
- **queuedmessagesclients** collection: per-client delivery tracking with `status` field (0/1/2)

Indexes created at startup:
```javascript
queuedmessages:          { message_uuid: 1 }
queuedmessagesclients:   { client_id: 1 }, { client_id: 1, status: 1 }
```

### MongoDB-Specific Bottlenecks

| Issue | Severity | Description |
|-------|----------|-------------|
| Fetch + mark not atomic | Critical | Separate `aggregate()` then `updateOne()` — race condition |
| No batch insert for enqueue | High | Per-message `updateOne()` loops — multiple round-trips |
| `$lookup` aggregation for every fetch | Medium | MongoDB's JOIN equivalent, expensive on large collections |
| `purgeExpiredMessages` loads all UUIDs into memory | Medium | Finds all expired UUIDs first, then bulk-deletes |
| Expiry check in Kotlin, not in pipeline | Medium | Expired messages fetched then skipped client-side |
| `Thread.sleep()` in purge batches | Medium | Blocks Vert.x worker thread |
| `markMessageDelivered` missing status guard | Medium | No `status: 1` condition on the update |

### MongoDB Phase 1: Quick Wins (No Schema Change)

#### 1.1 Atomic Fetch-and-Mark with `findOneAndUpdate()`

MongoDB's `findOneAndUpdate()` is the equivalent of PostgreSQL's `FOR UPDATE SKIP LOCKED` — it atomically finds and updates a document, guaranteeing no two consumers process the same message.

For **single message** fetch (used in the one-at-a-time processing loop):

```kotlin
val options = FindOneAndUpdateOptions()
    .returnDocument(ReturnDocument.AFTER)
    .sort(Document("_id", 1))  // FIFO order

val clientDoc = queuedMessagesClientsCollection.findOneAndUpdate(
    and(
        eq("client_id", clientId),
        eq("status", 0)
    ),
    Document("\$set", Document("status", 1)),
    options
)

// Then fetch message details
if (clientDoc != null) {
    val messageUuid = clientDoc.getString("message_uuid")
    val messageDoc = queuedMessagesCollection.find(eq("message_uuid", messageUuid)).first()
    // Convert to BrokerMessage...
}
```

For **batch fetch** (filling the 1000-message cache), use a two-step atomic approach:

```kotlin
// Step 1: Atomically claim a batch by updating status 0→1
val pendingDocs = queuedMessagesClientsCollection.find(
    and(eq("client_id", clientId), eq("status", 0))
).sort(Document("_id", 1)).limit(limit).toList()

val uuids = pendingDocs.map { it.getString("message_uuid") }

queuedMessagesClientsCollection.updateMany(
    and(
        eq("client_id", clientId),
        `in`("message_uuid", uuids),
        eq("status", 0)  // Re-check status to handle races
    ),
    Document("\$set", Document("status", 1))
)

// Step 2: Fetch message content for claimed UUIDs
val messages = queuedMessagesCollection.find(`in`("message_uuid", uuids)).toList()
```

Or ideally, use a **MongoDB transaction** to make the find + update truly atomic:

```kotlin
val session = mongoClient.startSession()
session.startTransaction()
try {
    val pendingDocs = queuedMessagesClientsCollection.find(session,
        and(eq("client_id", clientId), eq("status", 0))
    ).sort(Document("_id", 1)).limit(limit).toList()

    val uuids = pendingDocs.map { it.getString("message_uuid") }
    queuedMessagesClientsCollection.updateMany(session,
        and(eq("client_id", clientId), `in`("message_uuid", uuids)),
        Document("\$set", Document("status", 1))
    )
    session.commitTransaction()

    // Fetch message content outside transaction
    val messages = queuedMessagesCollection.find(`in`("message_uuid", uuids)).toList()
} catch (e: Exception) {
    session.abortTransaction()
} finally {
    session.close()
}
```

**Note:** MongoDB transactions require a replica set. For standalone MongoDB, the `findOneAndUpdate()` approach per-message is the safest option.

#### 1.2 Batch Insert with `insertMany()`

Replace per-message `updateOne()` loops with bulk operations:

```kotlin
// Message documents
val messageDocs = messages.map { (message, _) ->
    Document(mapOf(
        "message_uuid" to message.messageUuid,
        "message_id" to message.messageId,
        "topic" to message.topicName,
        "payload" to Binary(message.payload),
        "qos" to message.qosLevel,
        "retained" to message.isRetain,
        "client_id" to message.clientId,
        "creation_time" to message.time.toEpochMilli(),
        "message_expiry_interval" to message.messageExpiryInterval
    ))
}

// Use bulkWrite with upserts for idempotency
val messageOps = messageDocs.map { doc ->
    UpdateOneModel<Document>(
        eq("message_uuid", doc.getString("message_uuid")),
        Document("\$setOnInsert", doc),
        UpdateOptions().upsert(true)
    )
}
queuedMessagesCollection.bulkWrite(messageOps, BulkWriteOptions().ordered(false))

// Client mapping documents
val clientOps = messages.flatMap { (message, clientIds) ->
    clientIds.map { clientId ->
        UpdateOneModel<Document>(
            and(eq("client_id", clientId), eq("message_uuid", message.messageUuid)),
            Document("\$setOnInsert", Document(mapOf(
                "client_id" to clientId,
                "message_uuid" to message.messageUuid,
                "status" to 0
            ))),
            UpdateOptions().upsert(true)
        )
    }
}
queuedMessagesClientsCollection.bulkWrite(clientOps, BulkWriteOptions().ordered(false))
```

**Benefit:** Single network round-trip per collection instead of N round-trips.

#### 1.3 Push Expiry Filter into Aggregation Pipeline

```javascript
{ $lookup: { from: "queuedmessages", localField: "message_uuid", foreignField: "message_uuid", as: "message" } },
{ $unwind: "$message" },
{ $match: {
    $or: [
      { "message.message_expiry_interval": null },
      { $expr: { $lt: [
        { $divide: [{ $subtract: [currentTimeMillis, "$message.creation_time"] }, 1000] },
        "$message.message_expiry_interval"
      ]}}
    ]
}}
```

#### 1.4 Add Status Guard to `markMessageDelivered`

```kotlin
queuedMessagesClientsCollection.updateOne(
    and(
        eq("client_id", clientId),
        eq("message_uuid", messageUuid),
        eq("status", 1)  // Add this guard
    ),
    Document("\$set", Document("status", 2))
)
```

#### 1.5 Add Covering Index

```kotlin
queuedMessagesClientsCollection.createIndex(
    Document("client_id", 1).append("status", 1).append("message_uuid", 1)
)
```

### MongoDB Phase 2: Single Collection with Visibility Timeout

Same PGMQ-inspired redesign as PostgreSQL — merge into a single collection with `vt` field:

```javascript
// Single collection: queuedmessages
{
    _id: ObjectId,                     // Auto-generated, naturally ordered (FIFO)
    message_uuid: "...",
    client_id: "subscriber-id",
    topic: "sensors/temp",
    payload: BinData(...),
    qos: 1,
    retained: false,
    publisher_id: "publisher-id",
    creation_time: NumberLong(1711800000000),
    message_expiry_interval: NumberLong(3600),
    vt: ISODate("2026-03-30T12:00:00Z"),  // visibility timeout
    read_ct: 0                             // delivery attempt counter
}

// Index for fetch
{ client_id: 1, vt: 1 }
```

**Fetch and lock** using `findOneAndUpdate()`:

```kotlin
val now = Date()
val vtExpiry = Date(now.time + visibilityTimeoutMs)

val doc = queuedMessagesCollection.findOneAndUpdate(
    and(
        eq("client_id", clientId),
        lte("vt", now),
        // Expiry filter
        or(
            exists("message_expiry_interval", false),
            Document("\$expr", Document("\$lt", listOf(
                Document("\$divide", listOf(
                    Document("\$subtract", listOf(now.time, "\$creation_time")), 1000
                )),
                "\$message_expiry_interval"
            )))
        )
    ),
    combine(
        set("vt", vtExpiry),
        inc("read_ct", 1)
    ),
    FindOneAndUpdateOptions()
        .sort(Document("_id", 1))
        .returnDocument(ReturnDocument.AFTER)
)
```

**ACK / Delete:**
```kotlin
queuedMessagesCollection.deleteOne(
    and(eq("client_id", clientId), eq("message_uuid", messageUuid))
)
```

**Reset on reconnect:**
```kotlin
queuedMessagesCollection.updateMany(
    and(eq("client_id", clientId), gt("vt", Date())),
    set("vt", Date())  // Make visible immediately
)
```

**Benefits over current MongoDB implementation:**
- No `$lookup` aggregation — single collection, no JOINs
- `findOneAndUpdate()` is atomic — no race conditions
- `_id` (ObjectId) is naturally ordered — no UUID sorting needed
- No 3-step purge — just DELETE on ACK
- VT auto-expires — no `resetInFlightMessages()` needed on crash

---

## SQLite Implementation

The SQLite session store (`SessionStoreSQLite.kt`) uses the same two-table design but has additional SQLite-specific constraints and missing optimizations compared to the PostgreSQL implementation.

### Current SQLite Architecture

Same two-table pattern with notable differences:

```sql
-- queuedmessages (same structure, TEXT/INTEGER instead of VARCHAR/BIGINT)
CREATE TABLE IF NOT EXISTS queuedmessages (
    message_uuid TEXT PRIMARY KEY,
    message_id INTEGER,
    topic TEXT,
    payload BLOB,
    qos INTEGER,
    retained BOOLEAN,
    client_id TEXT,
    creation_time INTEGER,
    message_expiry_interval INTEGER
)

-- queuedmessagesclients (same structure)
CREATE TABLE IF NOT EXISTS queuedmessagesclients (
    client_id TEXT,
    message_uuid TEXT,
    status INTEGER DEFAULT 0,
    PRIMARY KEY (client_id, message_uuid)
)
```

**Critical difference: No indexes created** beyond the PRIMARY KEY. The PostgreSQL implementation creates 3 additional indexes on `queuedmessagesclients` — these are entirely missing in SQLite.

SQLite connection configured with WAL mode and pragmas:

| Pragma | Value | Purpose |
|--------|-------|---------|
| `journal_mode` | WAL | Concurrent readers during writes |
| `synchronous` | NORMAL | Balance safety/speed |
| `cache_size` | -64000 | 64MB in-memory cache |
| `busy_timeout` | 60000 | 60s lock wait |
| `temp_store` | MEMORY | Temp tables in RAM |

### SQLite-Specific Bottlenecks

| Issue | Severity | Description |
|-------|----------|-------------|
| No indexes on queuedmessagesclients | Critical | Missing `(client_id, status)`, `(status)`, `(message_uuid)` indexes — full table scans on every fetch |
| Fetch + mark not atomic | Critical | Same race condition as other backends |
| `markMessagesInFlight` uses N separate UPDATEs | High | SQLite lacks array parameters — one UPDATE per UUID instead of `ANY(?)` |
| No batching in `purgeQueuedMessages` | Medium | Deletes ALL orphans in single statement — can lock DB for extended period |
| Two enqueue batches not in single transaction | Medium | Message batch and client batch executed separately — orphans possible on failure |
| Expiry check in Kotlin, not in SQL | Medium | Expired messages fetched then skipped client-side |
| `markMessageDelivered` missing status guard | Medium | No `AND status = 1` condition |
| `message_expiry_interval` uses -1 sentinel | Low | Uses -1 instead of NULL for absent expiry — inconsistent with other backends |
| Single-writer serialization via Vert.x event bus | Low | All SQLite ops serialize through single `SQLiteVerticle` — by design, but limits throughput |

### SQLite Phase 1: Quick Wins (No Schema Change)

#### 1.1 Add Missing Indexes

The most impactful single change — add the indexes that PostgreSQL already has:

```sql
CREATE INDEX IF NOT EXISTS queuedmessagesclients_status_idx
    ON queuedmessagesclients (status);
CREATE INDEX IF NOT EXISTS queuedmessagesclients_message_uuid_idx
    ON queuedmessagesclients (message_uuid);
CREATE INDEX IF NOT EXISTS queuedmessagesclients_client_status_idx
    ON queuedmessagesclients (client_id, status);
```

**Files to change:**
- `broker/src/main/kotlin/stores/dbs/sqlite/SessionStoreSQLite.kt` — add index creation after table creation

#### 1.2 Atomic Fetch-and-Mark

SQLite doesn't support `FOR UPDATE SKIP LOCKED`, but since all operations are serialized through a single `SQLiteVerticle`, there's no concurrent access concern. We can still merge fetch + mark into a single operation for efficiency.

Use a CTE (SQLite supports CTEs since 3.8.3):

```sql
WITH pending AS (
    SELECT c.rowid AS crow, m.message_uuid, m.message_id, m.topic, m.payload,
           m.qos, m.retained, m.client_id, m.creation_time, m.message_expiry_interval
    FROM queuedmessagesclients c
    JOIN queuedmessages m ON c.message_uuid = m.message_uuid
    WHERE c.client_id = ? AND c.status = 0
      AND (m.message_expiry_interval IS NULL
           OR m.message_expiry_interval < 0
           OR ((? - m.creation_time) / 1000) < m.message_expiry_interval)
    ORDER BY c.rowid
    LIMIT ?
)
UPDATE queuedmessagesclients SET status = 1
WHERE rowid IN (SELECT crow FROM pending)
RETURNING *;
```

**Note:** SQLite's `RETURNING` clause requires SQLite 3.35.0+ (2021). If targeting older versions, use two statements in a single transaction:

```kotlin
connection.autoCommit = false
// SELECT pending messages
val messages = fetchPending(clientId, limit)
// UPDATE all fetched to in-flight
val uuids = messages.map { it.messageUuid }
markInFlight(clientId, uuids)
connection.commit()
```

Since SQLite serializes all writes through the single verticle, this is effectively atomic without `FOR UPDATE SKIP LOCKED`.

#### 1.3 Batch `markMessagesInFlight` with Dynamic IN Clause

Replace N separate UPDATEs with a single statement using a dynamic IN clause:

```kotlin
override fun markMessagesInFlight(clientId: String, messageUuids: List<String>) {
    if (messageUuids.isEmpty()) return
    val placeholders = messageUuids.joinToString(",") { "?" }
    val sql = """
        UPDATE queuedmessagesclients SET status = 1
        WHERE client_id = ? AND message_uuid IN ($placeholders) AND status = 0
    """
    val params = JsonArray().add(clientId)
    messageUuids.forEach { params.add(it) }
    sqlClient.executeUpdateSync(sql, params)
}
```

**Benefit:** 1 SQL statement instead of N. SQLite has a default `SQLITE_MAX_VARIABLE_NUMBER` of 999, so for batches >998, chunk into groups.

#### 1.4 Add Batching to `purgeQueuedMessages`

Port the PostgreSQL batched approach:

```kotlin
override fun purgeQueuedMessages() {
    val batchSize = 5000
    val sql = """
        DELETE FROM queuedmessages
        WHERE message_uuid IN (
            SELECT qm.message_uuid FROM queuedmessages qm
            WHERE NOT EXISTS (
                SELECT 1 FROM queuedmessagesclients qmc
                WHERE qmc.message_uuid = qm.message_uuid
            )
            LIMIT ?
        )
    """
    var totalDeleted = 0
    var deleted: Int
    do {
        deleted = sqlClient.executeUpdateSync(sql, JsonArray().add(batchSize))
        totalDeleted += deleted
    } while (deleted == batchSize)
}
```

#### 1.5 Move Expiry Filtering to SQL

Add expiry condition to the fetch query:

```sql
WHERE c.client_id = ? AND c.status = 0
  AND (m.message_expiry_interval IS NULL
       OR m.message_expiry_interval < 0
       OR ((? - m.creation_time) / 1000) < m.message_expiry_interval)
```

The `-1` sentinel for null expiry needs the `< 0` check (or normalize to NULL on insert).

#### 1.6 Wrap Both Enqueue Batches in Single Transaction

Currently the message batch and client batch are executed as separate operations. Wrap them in a single transaction:

```kotlin
// Execute both batches atomically via a new executeBatchTransaction method
sqlClient.executeBatchTransaction(listOf(
    Pair(messageSql, messageBatch),
    Pair(clientSql, clientBatch)
))
```

#### 1.7 Add Status Guard to `markMessageDelivered`

```sql
UPDATE queuedmessagesclients SET status = 2
WHERE client_id = ? AND message_uuid = ? AND status = 1
```

### SQLite Phase 2: Single Table with Visibility Timeout

Same PGMQ-inspired redesign as PostgreSQL and MongoDB — merge into a single table with `vt` column:

```sql
CREATE TABLE IF NOT EXISTS queuedmessages (
    msg_id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_uuid TEXT NOT NULL,
    client_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    payload BLOB,
    qos INTEGER NOT NULL,
    retained INTEGER NOT NULL DEFAULT 0,
    publisher_id TEXT,
    creation_time INTEGER NOT NULL,
    message_expiry_interval INTEGER,
    vt INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),  -- Unix timestamp
    read_ct INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX queuedmessages_fetch_idx ON queuedmessages (client_id, vt);
CREATE INDEX queuedmessages_client_uuid_idx ON queuedmessages (client_id, message_uuid);
```

**SQLite-specific notes:**
- `AUTOINCREMENT` ensures monotonically increasing IDs (FIFO order)
- `vt` stored as Unix timestamp (INTEGER) — SQLite has no native TIMESTAMP type
- `retained` as INTEGER (0/1) — SQLite BOOLEAN is just INTEGER

**Fetch and lock** (simplified — no `FOR UPDATE` needed due to single-writer):

```sql
WITH pending AS (
    SELECT msg_id FROM queuedmessages
    WHERE client_id = ?
      AND vt <= strftime('%s', 'now')
      AND (message_expiry_interval IS NULL
           OR message_expiry_interval < 0
           OR ((strftime('%s', 'now') * 1000 - creation_time) / 1000) < message_expiry_interval)
    ORDER BY msg_id ASC
    LIMIT ?
)
UPDATE queuedmessages SET
    vt = strftime('%s', 'now') + ?,
    read_ct = read_ct + 1
WHERE msg_id IN (SELECT msg_id FROM pending)
RETURNING msg_id, message_uuid, topic, payload, qos, retained,
          publisher_id, creation_time, message_expiry_interval;
```

**ACK / Delete:**
```sql
DELETE FROM queuedmessages WHERE client_id = ? AND message_uuid = ?
```

**Reset on reconnect:**
```sql
UPDATE queuedmessages SET vt = strftime('%s', 'now')
WHERE client_id = ? AND vt > strftime('%s', 'now')
```

**Benefits for SQLite specifically:**
- No JOIN — huge win since SQLite's query planner is simpler than PostgreSQL's
- `AUTOINCREMENT` gives natural FIFO without UUID ordering
- Single table = fewer indexes, less write amplification
- No 3-step purge = fewer write operations = less WAL pressure
- Single-writer model means VT is sufficient — no `SKIP LOCKED` needed

---

## Phase 3: Alternative Database Engines

### Analysis: What This Use Case Actually Needs

The queued messages workload has very specific characteristics:

| Characteristic | Description |
|---------------|-------------|
| **Write pattern** | High-frequency appends (every QoS>0 message for persistent sessions) |
| **Read pattern** | Sequential per client (FIFO), read-once-then-delete |
| **Update pattern** | Status transitions: pending → in-flight → delete |
| **Lifetime** | Transient — messages are deleted after ACK, not stored long-term |
| **Key access** | Always by `client_id` — per-client queues |
| **Durability** | Nice-to-have but not critical — messages can be re-sent |
| **Clustering** | Must work when clients reconnect to different nodes |

This is essentially a **persistent per-client FIFO queue** — not a relational workload.

---

### Option 1: Hazelcast IMap with EntryProcessor — Already Available

MonsterMQ already uses Hazelcast for clustering (retained messages, leader election, feature flags). The queued messages could use Hazelcast's distributed data structures directly.

**Approach A: IMap as per-client queue**

```kotlin
// Key: clientId, Value: List<BrokerMessage> (or a custom queue structure)
val queuedMessages: IMap<String, MutableList<BrokerMessage>> =
    hazelcastInstance.getMap("queued-messages")

// Enqueue — atomic via EntryProcessor (runs on owning node)
queuedMessages.executeOnKey(clientId, EnqueueProcessor(message))

// Fetch + mark in-flight — atomic via EntryProcessor
val messages = queuedMessages.executeOnKey(clientId, FetchAndLockProcessor(limit, vtSeconds))

// ACK — atomic via EntryProcessor
queuedMessages.executeOnKey(clientId, AckProcessor(messageUuid))
```

```kotlin
class EnqueueProcessor(val message: BrokerMessage) : EntryProcessor<String, MutableList<BrokerMessage>, Unit> {
    override fun process(entry: MutableMap.MutableEntry<String, MutableList<BrokerMessage>>): Unit {
        val queue = entry.value ?: mutableListOf()
        queue.add(message)
        entry.setValue(queue)
    }
}
```

| Pro | Con |
|-----|-----|
| **Already in the stack** — no new dependency | All data in memory (RAM-bound) |
| Distributed by default — clustering works out of the box | IMap values serialized on every update |
| EntryProcessor = atomic per-key operations, no race conditions | Large message lists = expensive serialization |
| Automatic backup (sync/async configurable) | Not persistent by default (lost on full cluster restart) |
| No JOIN, no SQL, no separate table — just per-key operations | EntryProcessor must be serializable |
| Client reconnects to any node — data accessible everywhere | IMap not optimized for queue patterns |

**Approach B: IMap<String, IQueue> pattern — one IQueue per client**

```kotlin
// Create a queue per client
val queue: IQueue<BrokerMessage> = hazelcastInstance.getQueue("mqtt-queue-$clientId")
queue.offer(message)           // Enqueue
val msg = queue.poll()         // Dequeue (destructive, FIFO)
```

| Pro | Con |
|-----|-----|
| True FIFO queue semantics | **IQueue data lives on single partition** — not distributed across nodes |
| Simple API (offer/poll/peek) | Creating thousands of IQueues has overhead |
| Backed by distributed map internally | No visibility timeout — poll is destructive |
| Can add QueueStore for persistence | Each queue = separate Hazelcast partition |

**Approach C: Hazelcast Ringbuffer + tracking offset**

```kotlin
val ringbuffer: Ringbuffer<BrokerMessage> = hazelcastInstance.getRingbuffer("mqtt-rb-$clientId")
val seq = ringbuffer.add(message)  // Returns sequence number

// Read without destroying
val msg = ringbuffer.readOne(lastReadSequence + 1)
```

| Pro | Con |
|-----|-----|
| Non-destructive reads (can replay) | Fixed capacity — old messages overwritten |
| Sequence-based = natural ordering | Need external offset tracking per client |
| Backed by ReliableTopic internally | More complex than IQueue |

**Recommendation for Hazelcast:** Approach A (IMap + EntryProcessor) is the most pragmatic. It's already in the stack, gives atomic per-client operations, and handles clustering transparently. The main concern is memory — all queued messages live in RAM. For most MQTT deployments with reasonable queue depths this is fine.

---

### Comparison Matrix

| Database | Type | Clustering | Latency | Throughput | Memory | JVM Native | Complexity |
|----------|------|-----------|---------|------------|--------|------------|------------|
| **Hazelcast IMap** | Distributed | ✅ Built-in | ~100μs | High | RAM-only | ✅ Pure Java | Low |
| **PostgreSQL** | External | ✅ Shared DB | ~500μs | Medium | N/A | N/A | Low |
| **SQLite** | Embedded | ❌ | ~200μs | Medium | Disk+cache | ❌ JNI | Low |

---

### Recommended Strategy: Tiered Approach

```
┌─────────────────────────────────────────────────┐
│              Configuration Choice                │
├─────────────────────────────────────────────────┤
│                                                  │
│  Single-Node (no -cluster):                      │
│    QueueStoreType: SQLITE   ← already supported  │
│                                                  │
│  Clustered (-cluster):                           │
│    QueueStoreType: HAZELCAST ← best fit          │
│    QueueStoreType: POSTGRES  ← already supported │
│    QueueStoreType: MONGODB   ← already supported │
│                                                  │
└─────────────────────────────────────────────────┘
```

**Priority implementation order:**

1. **Hazelcast IMap + EntryProcessor** (clustered) — Already in the stack, no new dependency, solves the clustering problem elegantly. Atomic per-client operations via EntryProcessor eliminate all race conditions. Memory-only is acceptable since queued messages are transient.

2. **Keep PostgreSQL/MongoDB/SQLite** with Phase 1+2 optimizations as fallback options for deployments that prefer simplicity or already have those databases.

---

### Additional PostgreSQL Optimizations

#### UNLOGGED Tables

For deployments where queued message durability across PostgreSQL restarts is not required:

```sql
CREATE UNLOGGED TABLE IF NOT EXISTS queuedmessages (...)
```

UNLOGGED tables skip WAL writes, significantly improving write performance (~2-5x). Messages are lost on PostgreSQL crash but this is acceptable for most MQTT deployments since:
- Clean session clients don't use the queue anyway
- Persistent session clients will get new messages after reconnect
- The queue is transient by nature

This could be a configuration option:
```yaml
QueuedMessagesUnlogged: true   # default false
```

#### Partitioning by Client ID

For very high client counts (>100K persistent sessions), consider hash partitioning:

```sql
CREATE TABLE queuedmessages (...) PARTITION BY HASH (client_id);
CREATE TABLE queuedmessages_p0 PARTITION OF queuedmessages FOR VALUES WITH (MODULUS 16, REMAINDER 0);
CREATE TABLE queuedmessages_p1 PARTITION OF queuedmessages FOR VALUES WITH (MODULUS 16, REMAINDER 1);
-- ... up to p15
```

This distributes the index load and allows parallel vacuum across partitions.

#### Connection Pooling

Replace single `DriverManager.getConnection()` with HikariCP pool for the queue operations, allowing concurrent fetch/ACK operations from multiple MqttClient instances.

---

## Summary

### PostgreSQL

| Phase | Effort | Impact | Breaking Change |
|-------|--------|--------|-----------------|
| Phase 1: FOR UPDATE SKIP LOCKED + fixes | Low | Medium | No |
| Phase 2: Single table + VT redesign | Medium | High | Yes (schema migration) |
| Phase 3: Hazelcast / UNLOGGED / partitioning | High | Medium-High | Configurable |

### MongoDB

| Phase | Effort | Impact | Breaking Change |
|-------|--------|--------|-----------------|
| Phase 1: findOneAndUpdate + bulkWrite + fixes | Low | Medium | No |
| Phase 2: Single collection + VT redesign | Medium | High | Yes (collection migration) |

### SQLite

| Phase | Effort | Impact | Breaking Change |
|-------|--------|--------|-----------------|
| Phase 1: Add indexes + batch IN clause + fixes | Low | High (indexes!) | No |
| Phase 2: Single table + VT redesign | Medium | High | Yes (schema migration) |

### Comparison: Atomic Patterns Across All Backends

| Capability | PostgreSQL | MongoDB | SQLite |
|-----------|-----------|---------|--------|
| Atomic fetch-and-lock | `FOR UPDATE SKIP LOCKED` (CTE) | `findOneAndUpdate()` with sort | CTE + UPDATE (single-writer makes it safe) |
| Skip locked rows | `SKIP LOCKED` clause | Implicit — single doc return | Not needed — serialized via Vert.x event bus |
| Batch fetch-and-lock | CTE with LIMIT + UPDATE | Transaction (requires replica set) | Single transaction (single connection) |
| Visibility timeout | `vt` TIMESTAMP column | `vt` Date field | `vt` INTEGER (Unix timestamp) |
| Batch insert | `executeBatch()` | `bulkWrite()` with `ordered: false` | `executeBatch()` |
| Batch mark in-flight | `ANY(?)` array operator | `$in` operator | Dynamic `IN (?, ?, ...)` clause |
| Expiry in query | SQL WHERE clause | `$expr` in aggregation | SQL WHERE clause |
| Purge batching | LIMIT in subquery | `$limit` in aggregation | LIMIT in subquery (currently missing) |

**Recommended approach:** Implement Phase 1 for all three backends first (immediate gains, no risk), then Phase 2 when ready for the schema/collection redesign. The Phase 2 changes are structurally identical across all backends.

**SQLite note:** Phase 1 index addition alone will likely give the biggest single improvement of any change across all backends, since the current implementation has no indexes beyond the primary key.

---

## References

- [PGMQ — PostgreSQL Message Queue](https://github.com/pgmq/pgmq)
- [PGMQ: Lightweight Message Queue on Postgres](https://legacy.tembo.io/blog/pgmq-self-regulating-queue/)
- [FOR UPDATE SKIP LOCKED for Queue Workflows](https://www.netdata.cloud/academy/update-skip-locked/)
- [Hazelcast IQueue Documentation](https://docs.hazelcast.com/hazelcast/5.5/data-structures/queue)
- [Hazelcast EntryProcessor](https://docs.hazelcast.com/hazelcast/5.4/computing/entry-processor)
- [Hazelcast Ringbuffer](https://docs.hazelcast.com/hazelcast/5.5/data-structures/ringbuffer)
