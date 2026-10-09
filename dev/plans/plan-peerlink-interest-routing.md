# Plan: PeerLink interest routing for the main broker (forward only what a peer subscribes to)

**Status: reviewed and clarified (2026-10-09); implementation planned.**

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

## Implementation readiness and cross-plan order (2026-10-09)

Owner authorized resolving the review recommendations on 2026-10-09. The observer, queue-default change, archive/bus coverage, `Unknown: ALL`, 64-consumer startup limit and status-only interest observability are accepted for both brokers. This authorizes the plan decisions; no code has been implemented by this revision.

1. Implement IR-M0 in both brokers (`Receive.Queue: true` is an intentional default change even with interest routing off).
2. Implement redundancy roles/wire and component configuration plus lifecycle (main R1/R2, edge P1/P2). Run shared role golden vectors and mixed-pair tests. No GraphQL/dashboard dependency.
3. Implement IR-M1–IR-M5; IR-M1's configured HOT/COLD provider depends on the component config from step 2. IR-M2 wire codec can be developed independently, reserving `CapRole` bit 4.
4. Complete IR-M6 benchmarks, expiry sweeps and mixed-pair acceptance tests before declaring routing ready. Main uses Maven/test-scoped benchmarks, not a Gradle build.
5. Implement witness phases with the revised shared C3/C8 rules and failure tests. GraphQL/main R7/edge PG and their dashboard work remain separately gated by explicit human commitment in both repositories.

For tracker synchronization, take snapshots at a fixed generation and retain subsequent absolute deltas per puller until delivered. Serialize snapshot frames and following deltas, never interleave a delta into an open snapshot. If a slow puller outruns the retained change history, send a new FIRST/LAST snapshot before further deltas. All chunks must use one generation; a mismatching chunk generation is a protocol error. Before u32 generation wrap, reconnect and reset with a snapshot; comparisons must not silently wrap.

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

The feature is off by default; with it off, routing stays dense; the separately approved IR-M0 queue-default change still applies.

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
| IR-S1 | Engine change: subscription observer in `data/SubscriptionManager.kt` (section 5) | It does nothing when unset. **Accepted for main (owner authorized review recommendations, 2026-10-09)** (the edge E8 sign-off covers only `internal/mqtt/topics.go`). |
| IR-S2 | Protocol extension | Capability-gated. The numbers are pinned in section 4 and must be identical in edge and main. |
| IR-S3 | Semantics when enabled | A publish made before the interest reaches the source is not forwarded: `FlushMs` + RTT/2 after a SUBSCRIBE, as in NATS. Off = dense routing; IR-M0 still changes the queue default. |
| IR-S4 | Archive and bus coverage | As edge (decided by the owner for edge; **confirmed for main on 2026-10-09**, together with Q-IR9). With `Receive.Archive: true`, all archive groups are announced, including `Default` on `#` (`handlers/ArchiveHandler.kt:323-334`); such a peer therefore gets everything. The message bus is announced only with `Receive.Bus: true`. |
| IR-P1 | Prerequisite: forwarded messages are queued like local ones | `Receive.Queue` default `true` (today `false`, `peerlink/config/PeerLinkConfig.kt:192`). The offline gate is `SessionHandler.kt:1902`. Online persistent clients are already enqueued regardless of the peer flag (`SessionHandler.kt:2111-2116`), so only the offline path changes. Same decision as edge (decided there 2026-10-08); **confirmed for main (2026-10-09).** |
| IR-S5 | GraphQL | As edge (decided by the owner for edge; **confirmed for main on 2026-10-09**, together with Q-IR9): none. Status goes to `/peerlink/v1/status` (`peerlink/PeerServer.kt:510`), JSON. |

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
  (edge 5.2).

**Final capability agreement.** `SERVER_HELLO` advertises node-wide `CapInterest` support when `Interest.Enabled` is true; it is sent before the source knows the consumer NodeId. The consumer offers the bit in `HELLO` only when its own interest setting is enabled and that source is not `Interest: OFF`. After authentication identifies the consumer, the source computes the intersection and clears `CapInterest` if its per-peer setting is OFF; `HELLO_OK.capabilities` is the final agreed set. The consumer uses that final set, never just the first two offers. Only then may interest frames and sparse batches be sent. On either node OFF therefore yields dense serving in both directions, including shared-secret/plain connections without a client certificate. Unexpected interest frames or sparse batches without final agreement are protocol errors.

- **Encoding (as edge 5.6).** All integers are little-endian, like every
  `mmq-peer/1` field. Filter bytes are UTF-8, `len > 0`. `expirySec`: `VOL`
  and `NONE` entries send 0, ignored on read; `PER` entries that never expire
  (archive groups; main MQTT 3.1.1 persistent sessions; on edge, sessions
  with an unlimited `MaximumSessionExpiryInterval`) send `0xFFFFFFFF`.
  Unknown flag bits are sent as 0 and ignored on read.
- **`INTEREST_SNAPSHOT` (as edge 5.3):** `u32 generation`, `u8 flags`
  (FIRST=1, LAST=2; other bits sent as 0, ignored on read), `u32 count`, then
  per filter `{u8 class (1=VOL, 2=PER); u32 expirySec; u16 len; bytes}`.
  Split into frames of at most `MaxConsumerFrame` (64 KiB, `Frames.kt:19`);
  all frames of one snapshot share one generation. The source applies it as
  mark and sweep, atomically at `LAST`, whatever its generation, and then sets
  the peer's last applied generation to it. A `FIRST` frame while a snapshot
  is open discards the open one (and its marks) and starts over.
- **`INTEREST_DELTA` (as edge 5.4):** `u32 generation` (strictly increasing
  per frame), `u32 count`, then per filter `{u8 class (0=NONE, 1=VOL, 2=PER);
  u32 expirySec; u16 len; bytes}`. `class` is absolute, so the latest value
  wins. No delta frame exceeds 64 KiB: the consumer flushes when the pending
  map reaches 1024 entries or the encoded delta would exceed 64 KiB, whichever
  comes first, and each delta frame takes the next generation. The source
  ignores a delta whose generation is ≤ the last applied generation (snapshot
  or delta).
- **Errors (as edge 5.6).** Protocol error, `GOAWAY(protocol)` and close: a
  truncated frame, a `count` that does not match the body, a non-`FIRST`
  snapshot frame with no snapshot open, a sparse `BATCH` violation (below).
  Invalid entry, ignored and counted in `interestRejected`, rest of the frame
  applied: `class` not 1/2 in a snapshot or not 0/1/2 in a delta, a filter
  outside the MQTT filter grammar, `len == 0`, `len > MaxFilterBytes`, not
  UTF-8. A bad `class` is always an invalid entry, never a frame error.
- **Sparse `BATCH` (as edge 5.5):** when `BatchFlagSparse` is set, `u32 span`
  and `u32 deltas[count]` follow the 68-byte header (`BatchHeaderLen`,
  `Frames.kt:23`); record `i` is at `base + deltas[i]`. The deltas are
  strictly increasing and each is `< span`. The consumer sets
  `appliedNext = base + span`. `count == 0` with `span > 0` is a pure skip
  batch. A violation of the delta rule, `span < count`, or a sparse batch with
  `span == 0` is a protocol error. The deltas table counts against the batch
  byte limit. `BatchFlagSparse` is set only for `CapInterest` consumers and
  only if at least one record in the span was skipped.
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
- Pending deltas are coalesced per filter and flushed every `FlushMs`, or at
  once when the pending map reaches 1024 entries or the encoded delta would
  exceed 64 KiB, whichever comes first (section 4).
- There is one tracker per node, with a generation cursor per puller.

How each source of interest is classified:

| Source | Announced when | Class |
|---|---|---|
| Network client subscriptions | always | `PER` if persistent, otherwise `VOL` |
| `subscribeInternalClient` subscriptions | always, except bridge-outbound owners while `Receive.BridgeOutbound: false`. HOT_STANDBY/COLD_STANDBY components (redundancy contract C6) are always announced, whatever the broker role or `BridgeOutbound`, including COLD components that are not running. Their filters come from the redundancy component provider (below) | `VOL` |
| Message listeners (`graphql-*`; GraphQL, Kafka streams, HMI sync, Redis, I3X) | only with `Receive.Bus: true` | `VOL` |
| Archive group `topicFilter` (`handlers/ArchiveGroup.kt:39`), all groups including `Default` | only with `Receive.Archive: true`. HOT/COLD archive groups follow the same rule (C6): announced on every node, active or not | `PER`, never expires while configured (`expirySec` `0xFFFFFFFF`) |
| Redundancy component provider: configured filters of `HOT_STANDBY`/`COLD_STANDBY` components (C6) | always, from configuration, whether the component runs or not (archive groups: only with `Receive.Archive: true`) | `VOL` (archive groups: `PER`, as above) |

**Redundancy component provider (as edge 4.1).** The tracker has a provider
that reads the component configuration, not the running components. It
announces the configured topic filters of every component with `Redundancy:
HOT_STANDBY` or `COLD_STANDBY`: bridges' outbound and subscription filters,
scripts' subscriptions, archive groups' `topicFilter`. Class `VOL`; archive
groups `PER` with no expiry. The filters are announced whatever the broker
role and `Receive.BridgeOutbound`, and whether or not the component is
running. The provider's counts are refcounted separately from runtime
subscriptions (which the observer sees as usual), so a component that stops
withdraws only its runtime counts. On a configuration change the provider
recomputes its set and the tracker emits deltas for the filters whose
announced class changes. `ALWAYS` components are not covered by it.

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
- empty filters, filters longer than `MaxFilterBytes`, and filters that are
  not valid UTF-8

An invalid filter (the last two items) is counted as `interestRejected` and
logged at WARN (as edge 4.4).

**Shared subscriptions:** main has no `$share` support. CONNACK advertises
them as unavailable (`doc/mqtt5.md:61`). `Receive.SharedSubscriptions` stays
config-only, so nothing is announced for it. If `$share` support arrives, edge
9.3 applies.

### 5.3 Puller — `peerlink/Puller.kt`

- `doHandshake` offers `CapInterest` according to local/per-peer settings, then uses final `HelloOK.capabilities`. `PeerServer` advertises node-wide support in `ServerHello` and applies per-peer OFF after identifying the consumer (section 4).
- After `HelloOK`, before `doSnapshotPhase` (line 504) and the first `FETCH`,
  the puller sends the snapshot frames (split at `MaxConsumerFrame`).
- The source (`PeerServer`) accepts `INTEREST_SNAPSHOT`/`INTEREST_DELTA` at any
  time after `HelloOK`, including during the snapshot phase (as edge 5.3).
- The writer thread (`doStreaming`, line 569) gets an `InterestCmd` next to
  `FetchCmd` / `CommitCmd`. It is written before the next `FETCH`.
- The reader thread (line 633) handles `BatchFlagSparse`: it checks the
  sparse rules (section 4; a violation is a protocol error), advances
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
| Serve | `PeerServer.serveFetch` (line 930), `writeBatch` (line 992) | set `BatchFlagSparse` and the delta table only for `CapInterest` consumers and only if at least one record in the span was skipped (as edge 5.5); the delta table counts against the batch byte limit |
| LWM | `PeerLog.append`, `commit` (line 600), `updateLWMLocked` (line 622) | caught-up auto-advance on append; lagging advance over records without bit `c` after `COMMIT` and every 100 ms for disconnected consumers |
| Periodic work | `PeerLinkManager` (no tick exists today) | one virtual thread every 100 ms: lagging advance; persistent expiry check once a second per peer |

The consumer index is `PeerLog.consumerIndex` (line 670). The consumer list is
fixed when the log is built (`LogConfig`, line 54), so a bit stays stable for
a peer's lifetime. There is a limit of 64 consumers per source (`Long` mask);
with interest routing on, more consumers is a startup error.

The source applies the same validity checks to received entries as the
consumer (section 4, edge 5.6) and ignores invalid ones, counted in
`interestRejected`.

`MaxFiltersPerPeer` (as edge 6.1): the limit is checked on the peer's filter
set after each applied snapshot (at `LAST`) and after each applied delta. If
the set exceeds it, the source serves that peer ALL (dense, unfiltered) until
a later snapshot, at `LAST`, is within the limit; while in ALL it clears the
peer's entries from the union trie. One WARN per transition; each transition
into ALL increments `interestOverLimit`.

**Expiry backlog reclamation (shared rule).** Expiring a PER entry changes future capture and triggers a bounded sweep of that peer's outstanding log records, at most `MaxScanPerFetch` offsets per 100 ms tick. For each previously tagged non-retained record, clear only that peer's bit if the topic matches none of its remaining VOL/PER interests. Preserve retained publishes, clears, snapshot records and their tombstones, and preserve records still covered by remaining interests. Keep minimal immutable topic/kind metadata beside records where encoded tombstones do not contain it; allocate this only for records actually appended. Update masks under the log lock so fetch/chunk snapshots and LWM see a consistent result; never clear another consumer's bit. The normal skip/LWM rules then reclaim the abandoned backlog without `Lost`/`GAP`; expose `interestBacklogDiscarded`. Completion may take several ticks for a large log. Ordinary unsubscribe and restart VOL withdrawal preserve already captured backlog (restart scenario 5); only persistent expiry triggers abandonment. Tests cover partial overlap, retained/tombstones, concurrent fetch and multi-tick completion.

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
- `Enabled: true` advertises node-wide support. Per-peer OFF clears the consumer offer or the final `HELLO_OK` agreement as specified in section 4; the link is dense in both directions.
- Validation in `validatePeerLink` (line 426), the same list as edge 11:
  - `Enabled: true` with more than 64 configured consumers: startup error (Q-IR5)
  - `FlushMs` ≤ 0: startup error
  - `MaxScanPerFetch` < 1024: startup error
  - `Unknown` not `ALL` or `NONE`: schema error
  - `MaxFilterBytes` outside 1..32768: startup error
  - `MaxFiltersPerPeer` < 1: startup error
- Documentation: `doc/peerlink.md` gets a new "Interest routing" section,
  and the `Receive.Queue` default is changed there.

---

## 8. Status and metrics

`peerlink/PeerStatus.kt` gets a per-peer `interest` object with the same
fields as edge 13: `state`, `filters`, `filtersPersistent`,
`snapshotGeneration`, `lastSnapshotAt` and `instanceId` (hex).

The counters are also the same as edge 13:
- `interestSkipped`, `interestMatched`
- `sparseBatches`
- `volatileDropped`, `persistentExpired`, `interestBacklogDiscarded`
- `interestRejected`, `interestOverLimit`
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
existing PeerLink tests (Maven test build).

---

## 10. Milestones

| Milestone | Content | Files |
|---|---|---|
| IR-M0 | IR-P1: `Receive.Queue` default `true`. Test: a forwarded QoS 1 message is queued for an offline persistent session and delivered on reconnect. | `PeerLinkConfig.kt:192`, `SessionHandler.kt:1902`, `doc/peerlink.md` |
| IR-M1 | Observer on `SubscriptionManager`; `InterestTracker` with classes, expiry and the bus / archive / internal-client sources; redundancy component provider (5.2, C6): configured filters of all `HOT_STANDBY` / `COLD_STANDBY` bridges, scripts and archive groups, from configuration, running or not, refcounted separately from runtime subscriptions, updated on config change; unit tests | `data/SubscriptionManager.kt`, `peerlink/InterestTracker.kt` (new), `handlers/SessionHandler.kt`, `handlers/ArchiveHandler.kt`, component configuration (bridges, scripts) for the provider |
| IR-M2 | Wire: `CapInterest`, frames `0x20` / `0x21`, `BatchFlagSparse`; shared golden vectors with edge; fuzz tests | `peerlink/wire/Frames.kt` (`fromCode` line 46, `decodeFrame` line 370) |
| IR-M3 | Source: interest table, union trie with masks, capture mask, log masks, sparse read, LWM advance | `peerlink/InterestTable.kt` (new), `core/TopicFilter.kt`, `CaptureHook.kt`, `core/PeerLog.kt`, `PeerServer.kt` |
| IR-M4 | Lifecycle: instanceId restart detection, `DISCONNECTED`, persistent expiry, mark and sweep, `Unknown`; consumer snapshot before the first `FETCH` | `PeerServer.kt`, `PeerLinkManager.kt`, `Puller.kt` |
| IR-M5 | Config, validation, status, docs | `PeerLinkConfig.kt`, `PeerStatus.kt`, `doc/peerlink.md` |
| IR-M6 | G-IR1 benchmark; integration tests; **mixed-pair tests against the edge broker** | PeerLink test sources |

---

## 11. Tests

Additional shared acceptance tests from the review:
- A source-only per-peer OFF setting on a shared-secret or plain connection clears the final HELLO_OK bit; consumer sends no interest frames and receives dense batches. Repeat with consumer-only OFF and both directions.
- Expire one of overlapping PER filters while other VOL/PER interests remain: abandon only uncovered non-retained backlog, preserve other peers' bits and retained/clear/tombstone obligations, and do not increment Lost/GAP. A log larger than MaxScanPerFetch is reclaimed over multiple ticks.
- Snapshot during concurrent subscription churn, slow puller exceeding retained change history, chunk generation mismatch and u32 generation rollover follow the readiness section; none silently loses a state update.


The unit tests and the integration scenarios 1–12 are the same as edge 15,
with these differences:
- Scenario 7, MQTT 3.1.1 variant: on main the `PER` entry never expires
  (`expirySec` `0xFFFFFFFF`); check that only the log limits bound it.
- Edge scenario 13 (HOT/COLD components) is mirrored by M2b; edge scenarios
  14, 15, 16 and 17 are M1, M2, M3 and M4/M5 below.
- Main has no `$share`: the edge tracker test "`$share` handling for SKIP and
  DELIVER" is replaced by "`$share` filters are never announced" (5.2).

The unit tests inherited from edge 15.1 include, in main as in edge:
- delta split at 1024 entries and at 64 KiB, strictly increasing generations;
  a delta with generation ≤ the last applied generation is ignored
- malformed frames → `GOAWAY(protocol)` (truncated, `count` not matching the
  body, non-`FIRST` snapshot frame with no snapshot open); a bad entry (bad
  class, `len 0`, over `MaxFilterBytes`, not UTF-8, bad grammar) is ignored and
  counted in `interestRejected`; `FIRST` while a snapshot is open restarts it
- golden vectors with little-endian integers, UTF-8 filters and `expirySec`
  0 (`VOL`/`NONE`), `0xFFFFFFFF` (never-expiring `PER`) and a finite value
- sparse `BATCH` violations → protocol error (non-increasing delta, delta ≥
  `span`, `span < count`, sparse with `span == 0`); no `BatchFlagSparse`
  without a skipped record
- `MaxFiltersPerPeer` exceeded → the peer is served ALL, one WARN,
  `interestOverLimit` +1; a later snapshot within the limit restores filtering
- redundancy component provider: a `COLD_STANDBY` component that is not
  running is announced

Main-specific tests:

- **M1.** A GraphQL subscription (message listener) on `g/#` with
  `Receive.Bus: true` is announced. With `Receive.Bus: false` it is not, and
  `g/1` is not forwarded.
- **M2.** `subscribeInternalClient` of a bridge outbound connector with
  `Receive.BridgeOutbound: false` is not announced.
- **M2b.** The same connector with `Redundancy: HOT_STANDBY` (or
  `COLD_STANDBY`, not running) on the STANDBY node is announced despite
  `Receive.BridgeOutbound: false` (C6); after a takeover it gets the feed
  without a new snapshot. Same vectors as edge 15.2 #13.
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
| Q-IR1, Q-IR2, Q-IR4, Q-IR7 | As edge 17 | **Closed (2026-10-09):** same answers as edge; IR-S4, IR-S5 and Q-IR9 confirmed for main |
| Q-IR3 | Default for `Unknown`? | **Closed (2026-10-09)**: `ALL` |
| Q-IR5 | Over 64 consumers? | **Closed (2026-10-09)**: startup error with interest routing on |
| Q-IR6 | Should offline persistent sessions count? | **Closed:** always; IR-P1 confirmed for main. |
| Q-IR8 | Restart detection: use the existing `Hello.instanceID` (both brokers already send it per process start) instead of a new TLV | **Yes.** No `HELLO` layout change. **Closed:** edge 5.2 already uses `HELLO.InstanceID`. |
| Q-IR9 | Confirm IR-P1 (`Receive.Queue` default `true`) for main | **Closed (2026-10-09):** yes, same as edge |
| Q-IR10 | Sign-off for the `SubscriptionManager` observer (IR-S1) | **Closed (2026-10-09):** accepted; a no-op when unset. |
| Q-IR11 | PeerLink counters in the metrics store as well? | **Closed:** not in v1; status JSON only |
