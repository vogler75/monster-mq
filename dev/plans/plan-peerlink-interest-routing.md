# Plan: PeerLink interest routing for the main broker (forward only what a peer subscribes to)

**Status: draft (2026-10-09). Not reviewed, not committed by the owner.**

This is the Kotlin counterpart of the edge plan
`edge/dev/plans/plan-peerlink-interest-routing.md`. Both brokers speak the same
`mmq-peer/1`, and an edge node and a main node can be peers. So the design,
the wire format, the semantics and the YAML keys are **shared**, and the edge
plan is normative for them (sections 3–9, 11, 12 there). This plan restates
them briefly, pins the wire numbers for both brokers, and maps every step to
the Kotlin code.

Paths are relative to `broker/src/main/kotlin/` unless stated otherwise. Line
numbers are from 2026-10-09.

Out of scope, as in the redundancy plan: the Vert.x / Hazelcast cluster
(`cluster/SetMapReplicator.kt`, `SessionHandler.kt:52,206`). PeerLink and
`-cluster` are mutually exclusive (`Monster.kt:1146`).

Estimates are marked **(est.)**. None of them may be claimed until gate G-IR1
has measured them (section 9).

---

## 1. Goal, scope, non-goals

Today the source captures every publish that passes `Capture.Include` /
`Exclude` (`peerlink/CaptureHook.kt:194`) and serves it to every consumer.
With interest routing on, the flow changes:

1. Each consumer announces its local topic filters to each source. The
   filters are refcounted; a full snapshot follows the handshake, then deltas.
2. The source appends a publish only if some consumer is interested. Each
   record is tagged with a consumer bitmask.
3. Each consumer is served only the records tagged for it, in sparse batches.
4. A filter is **persistent** (`PER`) if its session survives a disconnect,
   and **volatile** (`VOL`) otherwise. When a consumer restarts, the source
   drops its `VOL` interest. It keeps `PER` interest until the session expiry.

The feature is off by default; with it off, PeerLink behaves exactly as today.

**Non-goals (v1)**, same as edge:
- cluster-wide shared subscriptions
- multi-hop forwarding and gossip
- filtering retained publishes
- persisting the interest table
- filter subsumption
- GraphQL SDL changes and storage DDL changes

---

## 2. Governance and required sign-offs

| # | Item | Proposal |
|---|---|---|
| IR-S1 | Engine change: subscription observer in `data/SubscriptionManager.kt` (section 5) | It does nothing when unset. **Needs owner sign-off for main** (the edge E8 sign-off covers only `internal/mqtt/topics.go`). |
| IR-S2 | Protocol extension | Capability-gated. The numbers are pinned in section 4 and must be identical in edge and main. |
| IR-S3 | Semantics when enabled | A publish made before the interest reaches the source is not forwarded: `FlushMs` + RTT/2 after a SUBSCRIBE, as in NATS. Off = unchanged. |
| IR-S4 | Archive and bus coverage | As edge. With `Receive.Archive: true`, all archive groups are announced, including `Default` on `#` (`handlers/ArchiveHandler.kt:323-334`); such a peer therefore gets everything. The message bus is announced only with `Receive.Bus: true`. |
| IR-P1 | Prerequisite: forwarded messages are queued like local ones | `Receive.Queue` default `true` (today `false`, `peerlink/config/PeerLinkConfig.kt:192`). The offline gate is `SessionHandler.kt:1902`. Online persistent clients are already enqueued regardless of the peer flag (`SessionHandler.kt:2111-2116`), so only the offline path changes. Same decision as edge (decided there 2026-10-08); **confirm for main.** |
| IR-S5 | GraphQL | None. Status goes to `/peerlink/v1/status` (`peerlink/PeerServer.kt:510`), JSON. |

---

## 3. Model

As in edge section 3:

```
consumer B                                   source A
SubscriptionManager ─observer─┐              remote interest table per peer
message listeners (bus) ──────┼─▶ tracker ──INTEREST_SNAPSHOT/DELTA──▶ union FilterNode trie (filter → Long mask)
archive groups ───────────────┘  (refcount)        (puller conn)            │
                                                     capture: mask = match(topic) & active; 0 → skip
                                                     PeerLog record {frame, mask}
                         sparse BATCH ◀──────────── serveFetch: only records with bit B
```

Interest frames travel on the puller connection. That connection already
carries `FETCH`, `COMMIT` and `PING`, so a delta written before a `FETCH` is
processed before it.

---

## 4. Wire additions (pinned for edge and main)

| Item | Value | Note |
|---|---|---|
| `CapRole` | `1L shl 4` | from the redundancy plan (II.4); reserved here so the bits do not collide |
| `CapInterest` | `1L shl 5` | new, in `ServerHello` and `Hello` capabilities |
| `FrameType.InterestSnapshot` | `0x20` | consumer → source (free today, `peerlink/wire/Frames.kt:32-60`) |
| `FrameType.InterestDelta` | `0x21` | consumer → source |
| `BatchFlagSparse` | `1 shl 6` | after `Truncated` (`1 shl 5`, `Frames.kt:84-89`) |

- **InstanceId: no new field.** `Hello.instanceID` already exists in both
  brokers. Main draws it once per process start
  (`PeerLinkManager.kt:29`, `SecureRandom().nextLong() or 1L`) and stores it
  per session (`PeerServer.kt:710`); edge does the same (`puller.go:479`).
  Restart detection compares it with the last value seen for that NodeId
  (edge 5.2). *The edge plan still speaks of a new TLV; that should be aligned
  (section 11, Q-IR8).*
- **`INTEREST_SNAPSHOT`:** `u32 generation`, `u8 flags` (FIRST=1, LAST=2),
  `u32 count`, then per filter `{u8 class (1=VOL, 2=PER); u32 expirySec;
  u16 len; bytes}`. The source applies it as mark and sweep, atomically at
  `LAST`.
- **`INTEREST_DELTA`:** `u32 generation` (strictly increasing; a delta whose
  generation is ≤ the snapshot's is ignored), `u32 count`, then per filter
  `{u8 class (0=NONE, 1=VOL, 2=PER); u32 expirySec; u16 len; bytes}`. `class`
  is absolute, so the latest value wins.
- **Sparse `BATCH`:** when `BatchFlagSparse` is set, `u32 span` and
  `u32 deltas[count]` follow the 68-byte header (`BatchHeaderLen`,
  `Frames.kt:23`). The consumer sets `appliedNext = base + span`.
  `count == 0` with `span > 0` is a pure skip batch.
- The size limits are the existing ones: `MaxConsumerFrame` 64 KiB
  (`Frames.kt:19`) for consumer → source frames, so snapshots are split into
  several frames.
- Golden vectors are shared: the same files in `edge/internal/peerlink/wire/testdata/`
  and in the main wire tests, checked by both test suites.

---

## 5. Consumer side (Kotlin mapping)

### 5.1 Engine observer (IR-S1)

`SubscriptionManager` (`data/SubscriptionManager.kt:27`) gets:

```kotlin
interface SubscriptionObserver {
    fun added(clientId: String, filter: String)
    fun removed(clientId: String, filter: String)
}
fun setObserver(o: SubscriptionObserver?)
```

- It is called from `subscribe` (line 59) and `unsubscribe` (line 133), only
  when the entry is new or actually existed.
- It is also called for every filter dropped by `disconnectClient` (line 301).
- Calls happen outside any index lock, and the observer only queues work.
- This covers every path that adds a subscription:
  - network clients (`SessionHandler.addSubscription`, line 1212)
  - internal clients (`subscribeInternalClient`, line 2230: NATS, logger, agents, REST)
  - message listeners (`registerMessageListener`, line 965, client id `graphql-<id>`)
  - subscriptions restored at start (`iterateSubscriptions`, `SessionHandler.kt:613-619`)
- No observer set: one null check per subscribe.

### 5.2 Interest tracker — new `peerlink/InterestTracker.kt`

The tracker follows edge 4.2:
- It keeps `filter → {vol, per, maxExpiry}`.
- A delta is sent only when the announced class changes. A change of
  `maxExpiry` alone counts only if it moves by more than 10 % or crosses
  "never".
- Pending deltas are coalesced per filter and flushed every `FlushMs` or at
  1024 entries.
- There is one tracker per node, with a generation cursor per puller.

How each source of interest is classified:

| Source | Announced when | Class |
|---|---|---|
| Network client subscriptions | always | `PER` if persistent, otherwise `VOL` |
| `subscribeInternalClient` subscriptions | always, except bridge-outbound owners while `Receive.BridgeOutbound: false` | `VOL` |
| Message listeners (`graphql-*`; GraphQL, Kafka streams, HMI sync, Redis, I3X) | only with `Receive.Bus: true` | `VOL` |
| Archive group `topicFilter` (`handlers/ArchiveGroup.kt:39`), all groups including `Default` | only with `Receive.Archive: true` | `PER`, never expires while configured |

How a client is classified:
- **Persistent:** `isPersistent = isMqtt5 ? sessionExpiryInterval > 0 : !cleanStart`
  (`MqttClient.kt:577,725`). The tracker asks through a small
  `SessionClass(clientId) → (persistent, expirySec)` lookup on
  `SessionHandler`.
- **Expiry:** MQTT 5 uses `sessionExpiryInterval`. MQTT 3.1.1 with
  `cleanSession = false` counts as "never" (`0xFFFFFFFF`), because main has no
  maximum session expiry setting (edge uses `MaximumSessionExpiryInterval`).
- **Withdrawal:** offline persistent sessions keep counting until
  `scheduleSessionExpiry` (`SessionHandler.kt:739`) reaches `delClient`
  (line 1081). That deletes the subscriptions, and the observer sees it.

Never announced:
- the injector's own clients (`peerlink:` prefix)
- filters starting with `$`
- filters that fail `TopicFilter.validFilter` (`peerlink/core/TopicFilter.kt:44`)
- filters longer than `MaxFilterBytes`

A filter that is not announced is counted as `interestRejected` and logged at
WARN.

**Shared subscriptions:** main has no `$share` support. CONNACK advertises
them as unavailable (`doc/mqtt5.md:61`). `Receive.SharedSubscriptions` stays
config-only, so nothing is announced for it. If `$share` support arrives, edge
9.3 applies.

### 5.3 Puller — `peerlink/Puller.kt`

- `doHandshake` (line 368) sets `CapInterest` when `Interest.Enabled`.
- After `HelloOK`, before `doSnapshotPhase` (line 504) and the first `FETCH`,
  the puller sends the snapshot frames.
- The writer thread (`doStreaming`, line 569) gets an `InterestCmd` next to
  `FetchCmd` / `CommitCmd`. It is written before the next `FETCH`.
- The reader thread (line 633) handles `BatchFlagSparse`: it advances
  `appliedNext` (lines 696-710) by `span` instead of `count`, and passes only
  the present records to `Injector.applyBatch` (`peerlink/Injector.kt:249`).

---

## 6. Source side (Kotlin mapping)

| Step | Where | Change |
|---|---|---|
| Remote interest table | new `peerlink/InterestTable.kt` | per peer: entries, state (`UNKNOWN`/`LIVE`/`DISCONNECTED`), instanceId, disconnectedAt, generation; one union `FilterNode` trie (`core/TopicFilter.kt:64`) extended with a `Long` mask per node, under a read/write lock |
| Frame handling | `PeerServer.runSessionLoops` (line 805) | new cases for `InterestSnapshot` / `InterestDelta` next to `Fetch` / `Commit` / `Ping` |
| Restart detection | `PeerServer.servePeer` (line 594, instance at 710) | a new instanceId for a known NodeId drops that peer's `VOL` entries |
| Capture mask | `CaptureHook.capture` (line 194), after `accept` (line 174), before encoding | `mask = alwaysMask \| (union.match(topic) and activeMask)`; retained / clears / snapshot → all consumers; `mask == 0L` → count `interestSkipped`, no encode, no allocation |
| Log | `core/PeerLog.kt` | `append` (line 296) takes `mask: Long`; `LogChunk` (line 61) gets `masks = LongArray(1024)` |
| Sparse read | `PeerLog.readFor` (line 497) / `readInternal` (line 516) | collect records with bit `c`, bounded by `MaxScanPerFetch`; return base, span and deltas |
| Serve | `PeerServer.serveFetch` (line 930), `writeBatch` (line 992) | set `BatchFlagSparse` and the delta table only for `CapInterest` consumers |
| LWM | `PeerLog.append`, `commit` (line 600), `updateLWMLocked` (line 622) | caught-up auto-advance on append; lagging advance over records without bit `c` after `COMMIT` and every 100 ms for disconnected consumers |
| Periodic work | `PeerLinkManager` (no tick exists today) | one virtual thread every 100 ms: lagging advance; persistent expiry check once a second per peer |

The consumer index is `PeerLog.consumerIndex` (line 670). The consumer list is
fixed when the log is built (`LogConfig`, line 54), so a bit stays stable for
a peer's lifetime. There is a limit of 64 consumers per source (`Long` mask);
with interest routing on, more consumers is a startup error.

The lifecycle (edge section 7) is unchanged:
- `UNKNOWN` follows `Interest.Unknown` (`ALL` default).
- `DISCONNECTED` keeps `VOL` entries. Each `PER` entry stops matching when
  `now > disconnectedAt + expirySec`.
- The log limits (`MaxMessages`, `MaxBytes`) bound what is held for a dead
  peer.

---

## 7. Configuration

The keys are the same as edge 11:

```yaml
PeerLink:
  Interest:
    Enabled: false
    Unknown: ALL                # ALL | NONE
    FlushMs: 5
    MaxScanPerFetch: 65536
    MaxFiltersPerPeer: 100000
    MaxFilterBytes: 1024
  Peers:
    - NodeId: node-b
      Interest: INHERIT         # INHERIT | OFF
  Receive:
    Queue: true                 # default changes from false (IR-P1)
```

- Parser: `peerlink/config/PeerLinkConfig.kt`.
  - Add `Interest` to the allowed key sets (lines 258-279). `checkUnknownKeys`
    (line 281) rejects anything else.
  - The per-peer `Interest` key goes into `PeerConfig` (line 85) and into the
    `Peers` parsing (lines 394-415).
- Validation in `validatePeerLink` (line 426) gives a startup error for each
  of these:
  - more than 64 consumers with `Enabled: true`
  - `FlushMs` ≤ 0
  - `MaxScanPerFetch` < 1024
- Documentation: `doc/peerlink.md` gets a new "Interest routing" section,
  and the `Receive.Queue` default is changed there.

---

## 8. Status and metrics

`peerlink/PeerStatus.kt` gets a per-peer `interest` object with the same
fields as edge 13: `state`, `filters`, `filtersPersistent`,
`snapshotGeneration`, `lastSnapshotAt`, `instanceId` (hex) and
`holdRemainingMs`.

The counters are also the same as edge 13:
- `interestSkipped`, `interestMatched`
- `sparseBatches`
- `volatileDropped`, `persistentExpired`
- `interestRejected`
- `deltasSent`, `deltasReceived`

Main has no PeerLink metrics in the metrics store today, so v1 exposes these
counters only through `/peerlink/v1/status`. There is no metrics-store or
GraphQL change.

Each state change is logged at INFO; `DISCONNECTED` is logged at WARN.

---

## 9. Gate G-IR1 (benchmark)

The scenarios and pass criteria are the same as edge 12.4:
- 0 %, 10 % and 100 % of publishes with peer interest
- 1k and 10k remote filters, half of them wildcards
- 2 consumers

Pass criteria:
- at 100 % interest, throughput regresses by at most 5 %
- at 10 % interest, throughput is at least 3 times better
- skipped publishes cost 0 allocations on the capture path

The benchmark is a JMH benchmark or a test-scoped benchmark next to the
existing PeerLink tests (CI gradle build).

---

## 10. Milestones

| Milestone | Content | Files |
|---|---|---|
| IR-M0 | IR-P1: `Receive.Queue` default `true`. Test: a forwarded QoS 1 message is queued for an offline persistent session and delivered on reconnect. | `PeerLinkConfig.kt:192`, `SessionHandler.kt:1902`, `doc/peerlink.md` |
| IR-M1 | Observer on `SubscriptionManager`; `InterestTracker` with classes, expiry and the bus / archive / internal-client sources; unit tests | `data/SubscriptionManager.kt`, `peerlink/InterestTracker.kt` (new), `handlers/SessionHandler.kt`, `handlers/ArchiveHandler.kt` |
| IR-M2 | Wire: `CapInterest`, frames `0x20` / `0x21`, `BatchFlagSparse`; shared golden vectors with edge; fuzz tests | `peerlink/wire/Frames.kt` (`fromCode` line 46, `decodeFrame` line 370) |
| IR-M3 | Source: interest table, union trie with masks, capture mask, log masks, sparse read, LWM advance | `peerlink/InterestTable.kt` (new), `core/TopicFilter.kt`, `CaptureHook.kt`, `core/PeerLog.kt`, `PeerServer.kt` |
| IR-M4 | Lifecycle: instanceId restart detection, `DISCONNECTED`, persistent expiry, mark and sweep, `Unknown`; consumer snapshot before the first `FETCH` | `PeerServer.kt`, `PeerLinkManager.kt`, `Puller.kt` |
| IR-M5 | Config, validation, status, docs | `PeerLinkConfig.kt`, `PeerStatus.kt`, `doc/peerlink.md` |
| IR-M6 | G-IR1 benchmark; integration tests; **mixed-pair tests against the edge broker** | PeerLink test sources |

---

## 11. Tests

The unit tests and the integration scenarios 1–12 are the same as edge 15,
except scenario 4.3, which uses MQTT 3.1.1 `cleanSession = false` with
"never". Main-specific tests:

- **M1.** A GraphQL subscription (message listener) on `g/#` with
  `Receive.Bus: true` is announced. With `Receive.Bus: false` it is not, and
  `g/1` is not forwarded.
- **M2.** `subscribeInternalClient` of a bridge outbound connector with
  `Receive.BridgeOutbound: false` is not announced.
- **M3.** A session expires via `scheduleSessionExpiry`. A `NONE` delta
  follows, and the source stops appending.
- **M4.** **Mixed pair, edge ↔ main, in both directions.** Both nodes have
  `CapInterest` and are filtered. Then one node runs without `CapInterest`,
  and the other serves it dense and unfiltered.
- **M5.** Golden vectors from the edge repo decode identically in main, and
  the other way round.

---

## 12. Open questions (owner)

| # | Question | Recommendation |
|---|---|---|
| Q-IR1 … Q-IR7 | As edge 17 | same answers, to keep the brokers alike |
| Q-IR8 | Restart detection: use the existing `Hello.instanceID` (both brokers already send it per process start) instead of a new TLV | **Yes.** No `HELLO` layout change. The edge plan 5.2 has to be aligned. |
| Q-IR9 | Confirm IR-P1 (`Receive.Queue` default `true`) for main | Yes, same as edge |
| Q-IR10 | Sign-off for the `SubscriptionManager` observer (IR-S1) | Yes. It is the only engine change, and a no-op when unset. |
| Q-IR11 | PeerLink counters in the metrics store as well? | Not in v1; status JSON only |
