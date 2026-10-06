# Plan: PeerLink for the Kotlin MonsterMQ broker (wire- and config-compatible with MonsterMQ Edge)

**Status: draft, 2026-10-06. Waiting for owner sign-off on the J0 decisions (section 2).**

## 0. Sources and parity rule

| Document | Role |
|---|---|
| `edge/dev/plans/spec-peerlink-redundancy.md` (edge repo, last changed in `c19866b`) | **Normative.** Section 3 (wire protocol), sections 4-9 (behaviour), section 10 (configuration), sections 11-12 (security, observability). Section 17 is the first JVM code map; this plan replaces it. |
| `edge/dev/plans/plan-peerlink.md` | Design rationale and rejected alternatives. |
| `edge/internal/peerlink/**` | Reference implementation. Where the spec and the Go code disagree, the Go code wins, as on the edge. |

Below, "spec §x" means the edge spec, and `K/` means `main/broker/src/main/kotlin/`.

**Parity rule.** The Kotlin broker implements PeerLink **the same way as the edge broker**:

1. **Same protocol.** `mmq-peer/1` is implemented byte-exactly (spec §3). A Kotlin node and a Go edge node can be linked in either direction, as source, consumer, or both.
2. **Same configuration.** The `PeerLink` YAML block has the same keys, types, defaults, validation and fail-closed rules as spec §10. A `PeerLink` block copied from an edge config starts unchanged on the Kotlin broker; the only differences are the documented JVM mappings in 3.2.
3. **Same behaviour and observability.** Capture filters, drop reasons, counters, log events, the status JSON field names (spec §12.1) and the status/resync HTTP endpoints are identical.
4. **Deviations** are allowed only where the Kotlin broker core has no equivalent (section 3.2, section 10). Each one is listed in this plan and in `doc/peerlink.md`.

---

## 1. Goal, scope, non-goals

### 1.1 Goal

Link a Kotlin MonsterMQ broker ("main") with one or more MonsterMQ Edge brokers, or with other Kotlin brokers. A publish accepted on one node is delivered on the other nodes with the same MQTT semantics:

- QoS, retain, and retained deletes;
- MQTT 5 properties;
- publisher client id and username;
- publish time and remaining expiry.

Typical topologies:

```
 edge-a  <==mmq-peer/1==>  main        (edge to central broker, both directions)
 edge-a  <==>  edge-b                  (existing edge pair)
     \          /
      \=> main <=/                     (full mesh of three; one hop only, spec §7)
```

### 1.2 In scope

The full feature set of the edge v1 (spec §1-§12), except the WinCC OA native parts:

- capture on the source;
- bounded in-memory log with epochs, offsets and per-consumer commits;
- the pull protocol with long-poll fetch, batching, CRC, tombstones, retained snapshot FILL and operator NEWER resync;
- injection on the receiver with all its gates;
- split-horizon loop prevention;
- shutdown drain;
- TLS, mTLS, pins, shared-secret MAC, admission control and CIDR allow-list;
- the loopback status/resync endpoint, metrics and log events;
- cross-implementation tests against the Go edge broker.

### 1.3 Non-goals (same as the edge, spec §1)

- Replication of sessions, subscriptions, offline queues or inflight state.
- A durable log or spilling to disk.
- Multi-hop forwarding.
- PUBACK gated on the peer, fencing, or role ownership.
- PeerLink configuration through GraphQL or the dashboard, and hot reload. The YAML is static, as on the edge.
- WinCC OA native redundancy features (spec §14: `connectToRedundantHosts`, `oaRetained`, the native status object). The Kotlin broker always announces `oaSystem = ""` and `topicRoot = ""` (3.3).
- Running PeerLink together with Hazelcast clustering (`-cluster`) or the Kafka message bus (D3). Zenoh federation **is** supported (section 5a).

---

## 2. Decisions (milestone J0)

| # | Decision | Recommendation | Status |
|---|---|---|---|
| D1 | Wire compatibility with Go `mmq-peer/1` | Yes, byte-exact; golden vectors from the Go codec (section 9) | **Decided by owner 2026-10-06** |
| D2 | Configuration parity | Same `PeerLink` block as spec §10; JVM mappings per 3.2 | **Decided by owner 2026-10-06** |
| D3 | Coexistence with `-cluster`, Kafka bus, Zenoh | **Zenoh: supported** (section 5a). Kafka bus: not supported, startup error. Hazelcast `-cluster`: startup error in v1. | **Decided by owner 2026-10-06** |
| D4 | NodeId source | **Same as the edge:** top-level `NodeId`, else the hostname; canonical lowercase `[a-z0-9._-]{1,64}`; first DNS label rule as on the edge. `NodeName` (used by Zenoh as broker id) is not used. | **Decided by owner 2026-10-06** |
| D5 | Shared-secret mode on the JVM (TLS 1.3 exporter, spec §3.9) | **No JDK 25 needed.** Use the BouncyCastle JSSE provider (BCJSSE) for the PeerLink sockets only. `bctls-jdk18on` 1.85 is already on the runtime classpath (transitively through `plc4j-driver-modbus`; becomes an explicit dependency), and its `org.bouncycastle.jsse.BCExtendedSSLSession` has `exportKeyingMaterialData(label, context, length)` (checked with `javap` on the 1.85 jar). Integration: `SSLContext.getInstance("TLSv1.3", BouncyCastleJsseProvider())` (not registered globally), wrapped in Netty `JdkSslContext`, plugged into the Vert.x `NetServer`/`NetClient` through custom `SSLEngineOptions`. JDK 25 `ExtendedSSLSession` is used instead when present. Fallbacks if the Vert.x integration fails the J0 spike: a Netty pipeline for the peer port only, or Conscrypt (`Conscrypt.exportKeyingMaterial`, 2.5.2 already on the classpath). Only if every route fails do shared secrets drop out of v1; TLS, mTLS and pins do not need the exporter and work on JDK 21. | **Decided by owner 2026-10-06** (BCJSSE; spike in J0) |
| D6 | Retained writes of replicas | **Reuse the existing retained queue** (`MessageHandler.retainedQueueStore`, section 6.3). No separate PeerLink store path. For replicas, enqueue with a blocking `put` on the injector's worker thread: a full queue slows the puller down instead of dropping. Fix the silent drop for local publishes too (counter plus rate-limited WARN). | **Decided by owner 2026-10-06** |
| D7 | `userProperties` fidelity | `BrokerMessage.userProperties` is a `Map<String,String>`, so order and duplicate keys are lost on Kotlin receivers. Accepted and documented for v1 (the encoder still writes one TLV per pair). | **Decided by owner 2026-10-06** |
| D8 | Feature flag | **None, same as the edge.** The edge `Features` block (`edge/internal/config/config.go:229-242`) has no PeerLink entry; PeerLink is switched only by `PeerLink.Enabled`. `Features` flags gate runtime-managed subsystems (devices, GraphQL/dashboard). PeerLink is static YAML only. | **Decided by owner 2026-10-06** |
| D9 | PeerLink ↔ Zenoh forwarding (section 5a) | No switches. Replicas are broker data: Zenoh `Allow`/`Deny` decides what goes to Zenoh, and `PeerLink.Capture` decides what goes to peers. An internal guard limits a message to one PeerLink hop, and uuid dedup handles double paths. | **Decided by owner 2026-10-06** |

---

## 3. Configuration

### 3.1 The `PeerLink` block

The block is identical to spec §10. Keys, defaults and validation are not restated here; the edge table is the source of truth, and the Kotlin implementation copies it into `K/peerlink/PeerLinkConfig.kt` with one unit test per row.

Example that works unchanged on an edge node and on a main node (only the top-level `NodeId` differs per host):

```yaml
NodeId: main                        # edge-a on the edge host
PeerLink:
  Enabled: true
  Tls:
    Enabled: true
    AutoGenerate: true              # certs/peer-{NodeId}.pem / .key on first start
  SharedSecrets: ["<openssl rand -base64 32>"]   # same on all hosts
  Peers:
    - { NodeId: main,   Address: "main.local:1890" }
    - { NodeId: edge-a, Address: "edge-a.local:1890" }
```

Rules that must match the edge exactly:

- **Unknown keys inside `PeerLink` fail startup**, even while `Enabled: false`. The Kotlin broker does not validate `yaml-json-schema.json` at runtime, so `PeerLinkConfig` validates explicitly and calls `exitProcess(1)` with the key path.
- Validation runs only when `Enabled: true`.
- All fail-closed rules and startup warnings from spec §10 apply.
- `{NodeId}` expansion in `Tls.CertPath`/`KeyPath`.
- The `Peers` entry equal to the own NodeId is ignored, so one file can serve every host. The hostname first-label rule applies too.
- Secrets: standard or URL base64, padded or not, at least 16 decoded bytes.
- Pins: 64 hex digits; colons and spaces allowed.

### 3.2 JVM mappings (the only differences)

| Edge item | Kotlin broker |
|---|---|
| Top-level `NodeId` | Same key and same resolution as the edge (D4): `NodeId`, else the hostname. Not `NodeName`, and never `Monster.getClusterNodeId()`, which returns `local` when standalone. |
| `Runtime.MemoryLimitMB` (Go soft limit) | Not applicable. The startup WARN "memory below 2.2 × `Log.MaxBytes` + 150 MiB" uses `Runtime.getRuntime().maxMemory()` (`-Xmx`). The key is accepted and ignored with an INFO, so shared config files load. |
| `MaxMessageSize` (default for `Log.MaxRecordBytes` and the receiver size drop) | `TCP.MaxMessageSizeKb × 1024` (default 512 KiB; `K/Monster.kt:1061`) |
| `HMI.SyncBaseTopic` (default `Capture.Exclude`) | `HMI.SyncBaseTopic`, same default `monstermq/hmi/sync` (`K/Monster.kt:1408`) |
| `RetainedStoreType` → announced `retainedClass` | `MEMORY` → MEMORY (0); `SQLITE`, `POSTGRES`, `CRATEDB`, `MONGODB` → DB (1); `NONE` → DB with "no retained store" (no snapshot offered); `HAZELCAST` only exists with `-cluster`, which D3 excludes |
| `UserManagement.Enabled` (forbids `AllowUnauthenticatedPeers`) | `UserManagement.Enabled`, same rule |
| WinCC OA native namespace exclusion | Not applicable. `topicRoot = ""`, `oaSystem = ""`. While `Oa4jBridge` / MonsterOA is active, `!OA/#` is excluded locally on capture, snapshot and receive (spec §17.1 recommendation). |
| Default listener port 1890 | Same. Conflicts with no current Kotlin default port. |

The schema entry in `broker/yaml-json-schema.json` gets a `PeerLink` block with `additionalProperties: false`, modelled on the `Zenoh` block. The edge `yaml-json-schema.json` block is the template, so both schemas stay identical.

---

## 4. Architecture in the Kotlin broker

New package `at.rocworks.peerlink` (`K/peerlink/`), one file per Go counterpart, so reviews can be done side by side:

| Kotlin | Go counterpart | Content |
|---|---|---|
| `wire/Frames.kt` | `wire/frame.go` | Preamble, frame header, all frame types, caps, GOAWAY codes, capability bits |
| `wire/Records.kt` | `wire/record.go` | Record encode/decode, TLVs, tombstones, decode checks |
| `wire/Mac.kt` | `wire/mac.go` | `lp()`, HMAC-SHA256, constant-time compare |
| `PeerLog.kt` | `log.go` | Chunked in-memory log (1024 slots of `ByteArray` frames), epoch, offsets, commits, eviction, loss accounting, drain/seal, waiters, `capacitySeconds` |
| `CaptureHook.kt` | `hook.go` | Capture chain (spec §4.1), filter trie, echo table, recapture |
| `PeerServer.kt` | `server.go` | Vert.x `NetServer`, sniffing, admission, handshake, sessions, takeover, duplicate detection, snapshot serving, status HTTP |
| `Puller.kt` | `puller.go` | Consumer state machine, backoff, snapshot phase, streaming |
| `Injector.kt` | `inject.go` | Validation and drop reasons, pacing, packet construction, apply, pending retained map |
| `PeerLinkManager.kt` | `manager.go` | Wiring, capabilities, lifecycle, drain |
| `PeerTls.kt` | `tlsutil/` | PEM key/cert, PEM/PKCS12 truststore, pins, identity, AutoGenerate, exporter |
| `PeerStatus.kt` | `status.go` | Status JSON (spec §12.1, identical field names) |
| `PeerLinkConfig.kt` | `config/config.go` | Parsing, defaults, validation, warnings |

**Transport decision (J0, 2026-10-06).** PeerLink does not use Vert.x `NetServer`/`NetClient`. It uses blocking `java.net.Socket` / BCJSSE `SSLSocket` on **JDK 21 virtual threads** (`Thread.ofVirtual()`), a 1:1 port of the Go goroutine model, so the Go code can be reviewed side by side. The sniffed first byte is replayed with BCJSSE `SSLSocketFactory.createSocket(socket, consumedInput, autoClose)`. ALPN uses `SSLParameters.applicationProtocols`, and the exporter comes from `BCSSLSocket.getBCSession().exportKeyingMaterialData`. The rest of this section keeps its original wording; where it mentions Vert.x contexts, read virtual threads.

**Threading model (original wording).**

- The source server and the sessions run on one Vert.x event-loop context per session.
- Long-poll waiters use Vert.x timers and promises; nothing blocks an event loop.
- Each puller runs on its own context: reader → handoff (capacity = `Fetch.Pipeline`) → injector.
- The injector runs on a dedicated **ordered worker context** per source, so per-source offset order is kept. It may block on the retained flush.
- The log is shared between the capture threads (any publisher thread) and the session contexts. It is guarded by one lock that is never held across I/O (the edge rule).

**JVM codec notes.** All integers are little-endian (`ByteBuffer.order(LITTLE_ENDIAN)`), and unsigned fields use the `java.lang.Long/Integer.compareUnsigned` / `toUnsignedLong` helpers (spec §3.13). CRC-32C comes from `java.util.zip.CRC32C`. Strings are cut on a UTF-8 boundary at 255 / 65535 bytes. Decoders accept and ignore trailing bytes in frame bodies. Monotonic clock: `System.nanoTime()` relative to log creation, in ms.

---

## 5. Source side: capture

### 5.1 Tap point

There is one tap at the top of `SessionHandler.publishMessage(message, forwardToExternalBus)` (`K/handlers/SessionHandler.kt:1539`). It runs before the bulk-buffer branch, so capture order equals accept order even with `publishBulkProcessingEnabled`. Condition: `message.peer == null && message.peerSource == null` (not a PeerLink replica, and not a Zenoh copy of one; section 5a).

- Every client publish, will and internal publish passes here once, after ACL and schema checks. Internal publishes include GraphQL, REST, MCP, bridges, OA, flows, agents, scripts and `publishInternal`.
- QoS 1 is captured before PUBACK, QoS 2 at PUBREL. The J1 spike verifies this against `MqttClient.kt`.
- Messages received from Zenoh (`forwardToExternalBus = false`) **are** captured, filtered by `Capture.Include/Exclude`, like MQTT bridge inbound messages on the edge (section 5a). The Hazelcast cluster path does not exist with PeerLink (D3).
- The capture chain and its counters follow spec §4.1 steps 1-10. `inline` = publishes without a network client (internal publishers). Their `clientId` is the internal sender id, or `inline` when there is none.

### 5.2 Message model

Extend `BrokerMessage` (`K/data/BrokerMessage.kt:15-35`) with:

- `peer: PeerForward?` = `{sourceNode, clientId, username, timeNs, epoch, offset, dup, will, snapshot}`, the equivalent of Go `Packet.Forward`;
- `isWill: Boolean`, set in the `MqttWill` constructor (equivalent of `Packet.Will`);
- `username: String?` of the publishing session.

All of them must survive every copy:

- `cloneWith*` (`:179-203`);
- `publishInternal` (`SessionHandler.kt:1974-1994`);
- the bulk buffer.

A single `copy` helper is used everywhere, and a unit test reflects over all copy paths. A dropped marker means the message is captured again: an echo loop (risk R2).

`BrokerMessageCodec` gets a versioned trailing section with `senderId`, MQTT 5 properties, `isWill`, `username` and `peer`. Old decoders ignore the trailer. It is needed whenever a replica crosses the Vert.x event bus, and it also fixes the existing loss of properties.

### 5.3 Retained ordering

The Kotlin broker writes retained values asynchronously through the `RM` writer thread. Spec §4.1 requires log order to equal retained apply order per topic. For publishes, the capture happens on the publisher thread before `messageHandler.saveMessage` enqueues the retained write. Both preserve per-publisher FIFO order, so per-topic order holds for a single publisher. Concurrent publishers on the same retained topic may diverge, as documented for active-active on the edge (spec §1, arrival order). The J3 tests check this explicitly; optional per-topic striping as in Go `SerializeRetained` is a J3 contingency.

### 5.4 Wills

- Wills are captured with `expirySec = 0` and the WILL flag.
- They are skipped (`skipWill`) when `Capture.Wills: false` or while draining.
- Server-initiated disconnects during shutdown publish wills. The drain flag must therefore be set before `disconnectClient` (section 7).

---

## 5a. Coexistence with Zenoh federation (D3, D9)

Zenoh federates independent Kotlin brokers (`K/bus/MessageBusZenoh.kt`). PeerLink links Kotlin and edge brokers. Both can run on the same node.

**Rule (owner decision 2026-10-06): no extra switches.** Data that arrives on this broker is normal broker data, whichever link brought it. Each outgoing link decides with **its own existing topic filters** what it forwards:

| Direction | Decided by | Code change |
|---|---|---|
| PeerLink → Zenoh | `Zenoh.Allow` / `Zenoh.Deny`, as for every local publish | The external-bus forward at `SessionHandler.kt:1569` does **not** skip replicas. The replica goes through `messageBus.publishMessageToBus` like any other message, and `MessageBusZenoh` applies Allow/Deny and `RemotePrefix` as today. |
| Zenoh → PeerLink | `PeerLink.Capture.Include` / `Exclude`, as for every local publish | The PeerLink tap also runs on the Zenoh-inbound path (`SessionHandler.kt:479-482`, `forwardToExternalBus = false`), so the tap condition is `message.peer == null` only. This matches the edge, where messages coming in through an MQTT bridge are captured like any internal publish (spec §4.1, §7). |

Example: main-1 pulls `plant/#` from edge-a, and its Zenoh config has `Allow: ["plant/#"]`. Then edge-a's `plant/...` values reach every Zenoh broker. If main-2 has PeerLink to edge-b with default `Capture.Include: ["#"]`, they also reach edge-b.

**Loop guard (internal, no config).** A message crosses **at most one PeerLink hop**, however many Zenoh hops it makes. Without this, edge-a → main-1 → Zenoh → main-2 → edge-a would deliver edge-a's own message back to it, if main-2 is also linked to edge-a.

- `ZenohMessageEnvelope` gets an optional trailing field with the PeerLink source NodeId of a replica. Old Zenoh peers ignore it.
- On receipt, `MessageBusZenoh` restores it as `BrokerMessage.peerSource`. The PeerLink tap skips messages with `peerSource` set. Only the capture decision changes: the receiver gates of 6.2 do not apply, because the message arrived through Zenoh and is handled as Zenoh messages are today.
- Zenoh's own loop prevention (origin broker id = `NodeName`, uuid dedup cache) is unchanged.
- Split horizon between PeerLink links stays as on the edge: a replica is never captured into the PeerLink log again.

**Double paths (handled by uuid dedup).** A replica's `messageUuid` is derived from `(sourceNodeId, epoch, offset)` (6.1). Every broker that pulls the same record from edge-a therefore produces the **same** uuid. This covers two cases:

- **edge-a linked to main-1 and main-2, both in one federation.** Both publish the replica to Zenoh. Zenoh's dedup cache (`MessageBusZenoh.kt:142`, `remember(messageUuid)`) drops the second copy on every other broker.
- **main-2 gets the record directly from edge-a and through Zenoh from main-1.** The injector registers the uuid of every applied replica in the Zenoh dedup cache, so the later Zenoh copy is dropped. If the Zenoh copy comes first, the injector finds the uuid already in the cache and skips the replica, counted as `zenohDupSkipped` (Kotlin-only status field, added next to the edge fields).

This dedup is limited by the cache window (`Zenoh.Deduplication.TtlSeconds`, default 300 s, and `CacheSize`). Docs: prefer linking each edge to one broker of a federation.

**Retained snapshot.** The FILL snapshot serves the local retained store, which also contains values received through Zenoh. That is consistent with the rule above: they are broker data and are filtered by `Capture.Include/Exclude` like the live stream.

**Status and metrics.** `messageBusIn/Out` sum both transports (6.2). The PeerLink status JSON is per link. Zenoh keeps its own logging.

---

## 6. Consumer side: injection and gates

### 6.1 Injector

- There is one injector per source with the logical client id `peerlink:<sourceNodeId>`. It is not a session and not in the client registry, and it bypasses ACL, size and quota checks (spec §6.1).
- The injector builds a replica `BrokerMessage` per spec §6.3:
  - `clientId = senderId =` the original publisher, so NoLocal works per logical client;
  - `time` = the backdated capture instant, `max(now − ageMs, 1 s)`;
  - `messageExpiryInterval = expirySec`;
  - `messageId = 0`, `isDup = false`;
  - `messageUuid` derived from `fnv64(src) ^ epoch` and the offset, random for snapshot values;
  - `mmq-peer-src` user property with `Receive.MarkReplicas`;
  - `peer` set.
- Replicas are applied with `sessionHandler.publishMessage(replica)`.
- **RetainOnly** (spec §6.4: stale retained replicas, expired retained deletes) becomes a new `SessionHandler.applyRetainedOnly(msg)`. It updates the retained store only: no delivery, no listeners, no counters.
- Validation order, drop reasons, pacing, the dedup rule and mid-batch commits are exactly spec §6.2 and §5.
- **Reserved client ids.** Network clients with id `inline` or `peerlink:*` are refused at CONNECT with 0x85 (`refusedClientIds`). This check goes in `MqttClient` before auth.

### 6.2 Gates (no hook system; `msg.peer != null` checks)

| Gate (spec §6.5) | Kotlin location |
|---|---|
| Archives / last value (`Receive.Archive`) | `MessageHandler.saveMessage` (`K/handlers/MessageHandler.kt:324-361`); retained always applies, through the existing retained queue (6.3) |
| Bridge outbound (`Receive.BridgeOutbound`, default false) | `MqttClientConnector` outbound (`K/devices/mqttclient/MqttClientConnector.kt:571-575`); other outbound connectors (Kafka, NATS, Redis, ...) follow the same predicate |
| Bus / GraphQL subscriptions (`Receive.Bus`) | `SessionHandler.notifyMessageListeners` (`:2011`) |
| Offline queues (`Receive.Queue`, default false) | Offline/created branches only (`SessionHandler.kt:1868-1915`, `2093-2108`). **Do not gate** the queue-first path of online persistent sessions (`:1795-1830`, `:2081-2086`), or they receive nothing (risk R3). |
| External bus (Zenoh) | `publishMessage` `:1569`: **not gated**. Replicas go to Zenoh like any message, filtered by `Zenoh.Allow`/`Deny`, and carry the PeerLink source in the envelope (section 5a) |
| Shared subscriptions (`Receive.SharedSubscriptions`) | The Kotlin broker has none; the key is validated and has no effect |
| Will supersession | `sessionHandler.isConnected(clientId)` (`:709`), plus a session-established time map (16 shards, 24 h) |
| Metrics | Replicas count `messageBusIn` instead of `messagesIn`, and live records served count `messageBusOut`. These counters already exist (`SessionHandler.kt:59-64`) and feed GraphQL `BrokerMetrics` and Prometheus: no SDL change. With Zenoh enabled, both transports add to the same counters; the per-transport split is in the PeerLink status JSON (`sources[].injected`, `consumers[].servedRecords`). |

### 6.3 Retained replicas: the existing retained queue (D6)

**How retained works today.**

- `MessageHandler` writes retained values asynchronously. `saveMessage` puts every retained publish into `retainedQueueStore` (an `ArrayBlockingQueue` of 100,000 entries, `K/handlers/MessageHandler.kt:29`).
- A single `RM` writer thread (`:297-322`) takes up to 4000 entries at a time. It keeps only the last value per topic (`:225-242`) and writes them with one `retainedStore.addAll` / `delAll`.
- This queue is the only retained path, used by all publishes; it is not a separate queue next to another one.
- It exists so that slow store writes (SQLite, PostgreSQL, MongoDB, CrateDB, one row per topic) never block the MQTT publish path, and so that many updates of the same topic collapse into one write.
- Retained delivery to new subscribers reads from the store (`findRetainedMessages`, `:367`), so a value becomes visible to new subscribers after the writer has flushed it (normally within about 100 ms).

**Problems today, independent of PeerLink:**

- When the queue is full, `add()` throws and the exception is swallowed (`:326-330`, `// TODO`). The retained update is lost silently.
- The writer has no error handling: a failed `addAll` loses the block.

**PeerLink design:**

- Replicas use the same queue, with the same semantics as local publishes. There is no separate PeerLink retained path and no pending map.
- `saveMessage` gets a variant for replicas that uses a blocking `put`. The injector runs on its own worker thread, so a full queue applies backpressure: the injector waits, the puller stops fetching, and the source keeps the records in its log. Nothing is dropped.
- Local publishes keep the non-blocking `add` (they must never block an event loop), but a full queue now counts `retainedQueueDropped` and logs a rate-limited WARN.
- The commit is sent after the batch is enqueued, not after it is written. This is the same guarantee as for a local publish ("accepted locally"). A consumer crash loses the queued values; spec §9 documents the same for the edge DB flush.
- **Snapshot FILL "absent" check.** Before the snapshot phase, the injector waits until the retained queue is empty. During the phase, a topic counts as present if it is in the store **or** in a small "queued retained topics" set that `MessageHandler` maintains (add on enqueue, remove after the write). This avoids overwriting a local value that is still in the queue.
- **RetainOnly** (stale retained replicas, expired retained deletes) enqueues into the same queue without delivery.

---

## 7. Shutdown and drain

The Kotlin broker has no shutdown orchestration today. A `Runtime.addShutdownHook` blocks on a Vert.x future chain in the order of spec §8:

1. `stopPullers` (10 s budget): finish the batch, flush, send the final COMMIT and `GOAWAY(shutdown)`.
2. `beginDrain`: wills are no longer captured.
3. Undeploy the MQTT TCP/TLS/WS/WSS, NATS and Kafka-protocol servers, and refuse new connections. Disconnect every client with 0x8B; wills fire now but are not captured.
4. Stop the internal publishers: devices/connectors, flows, scripts, agents, HMI sync, Redfish, Oa4jBridge, then the GraphQL/REST/MCP publish APIs.
5. `drain(DrainOnShutdownMs + 5 s)`:
   - drain to `drainTarget = LEO`;
   - seal the log, close the listener, send GOAWAY;
   - log `shutdownUnserved` per consumer and `uncapturedAtShutdown`.
6. Close the stores.

MonsterOA / JManager exit uses the same hook. Without PeerLink the hook keeps today's behaviour.

---

## 8. Security, status, observability

- **TLS**: Vert.x `NetServer`/`NetClient`.
  - Sniff the first byte, then `NetSocket.upgradeToSsl(SSLOptions, Buffer)` replays the bytes already read.
  - ALPN `mmq-peer/1` and `http/1.1`.
  - TLS 1.3 minimum when secrets are configured, 1.2 otherwise.
  - PEM keys (PKCS8) and PEM or PKCS12 truststores; never system roots, never JKS (spec §1 "PEM only").
  - The existing `MqttServer.buildKeyCertOptions`/`buildTrustOptions` (`K/MqttServer.kt:118-131`) are reused only where they keep these rules.
  - Verification runs in a custom `TrustManager`: pins, chain against the peer roots, EKU checks, no hostname check.
  - Identity is the URI SAN `urn:monstermq:node:<NodeId>`, or `CertificateIdentity`, with `IdentityFallback` DNS/CN.
  - `AutoGenerate` uses BouncyCastle (bcpkix 1.85 is already present): ECDSA P-256, PKCS8 key with mode 0600, self-signed for 10 years with the URI SAN and both EKUs, written atomically, SPKI SHA-256 logged.
- **Shared secret** (D5): HMAC-SHA256 per spec §3.9 over the TLS 1.3 exporter (label `monstermq-peer/1`, empty context, 32 bytes). The exporter comes from `BCExtendedSSLSession.exportKeyingMaterialData` (BCJSSE), or from JDK 25 `ExtendedSSLSession` when present. TLS 1.3 is enforced whenever secrets are configured. J4 exit: the exporter output and MAC equal Go's for the same session (the cross-implementation handshake succeeds in both directions), and a wrong secret gives `auth_failed`.
- **Admission**: CIDR allow-list before TLS (new small CIDR matcher), pre-auth slots per IPv4 address / IPv6 /64, and the global cap with bypass for configured and recently authenticated IPs (spec §4.3).
- **Status endpoint**:
  - `GET /peerlink/v1/status` and `POST /peerlink/v1/resync?source=` on the peer port.
  - Plaintext from loopback only, with the Origin/Host guard; over TLS, status only for certificate-authenticated Serve peers.
  - A pull-only node binds `127.0.0.1:<Port>`.
  - The JSON is identical to spec §12.1, so one monitoring script serves edge and main.
- **Log events**: the levels, texts and rate limits of spec §12.4, through `java.util.logging` with the existing logger names. Status counters may later appear in GraphQL; that is out of scope for v1 (as on the edge).

---

## 9. Tests

| Level | What | Where |
|---|---|---|
| Golden vectors | **Needs a small edge change:** a Go test with `-update` (or a `cmd/peerlink-vectors`) writes frames, records, tombstones, batches with CRC, and MAC inputs/outputs to `edge/internal/peerlink/wire/testdata/vectors.json`. The file is copied into `main/broker/src/test/resources/peerlink/`, and the Kotlin tests require byte-identical encoding and equal decoding. A CI check flags drift. | edge + main |
| Unit | Codec round trips and fuzz-style mutation (decoders never throw outside the declared errors); log (eviction, commit/trim, loss accounting, resume table spec §3.10, drain/seal); filters; config rows and fail-closed rules; TLS identity/pins | `mvn test` |
| Broker-level | Two Kotlin brokers in one test JVM are not possible (one broker per JVM, risk R9). New pytest process fixture that starts two broker processes with separate configs and ports. | `tests/pytest_tests/peerlink/` |
| **Cross-implementation (J6)** | Go edge binary ↔ Kotlin broker, both directions and bidirectional. Each case maps to the edge PL ids: QoS 0/1/2, retained set/delete, MQTT 5 properties, wills and supersession, resume after consumer and source restarts, overflow/GAP, snapshot FILL and NEWER, drain, mTLS, pins, secrets (D5), wrong secret → `auth_failed`, `wrong_node`, `self_connection`, duplicate NodeId, oversize/tombstone, MaxMessageSize mismatch | pytest, edge binary from `edge/bin` |
| Malformed peer | The scripted Go test peer from `edge/test/integration` drives the Kotlin server with bad preambles, short frames, unknown frame types and older minors | edge test harness, pointed at the Kotlin port |

---

## 10. Mixed edge/main specifics

| Topic | Behaviour |
|---|---|
| Message size | Default `MaxMessageSize` differs (edge 1 MiB, main 512 KiB). An edge record > 512 KiB is dropped on main (`dropped{size}`, retained also `retainedDiverged{size}`), and `HELLO.maxRecordBytes` makes the edge send a tombstone. Document: align `TCP.MaxMessageSizeKb` with the edge `MaxMessageSize`. |
| User properties | Order and duplicate keys are lost on a Kotlin receiver (D7) |
| Retained class | Main is usually DB (SQLite/Postgres) and the edge often MEMORY; `retainedClassMismatch` is a WARN, as on the edge |
| WinCC OA | An edge in native mode excludes `winccoa/#` itself, and main never receives it. Main announces an empty `topicRoot`, so no mismatch WARN. |
| Devices on both nodes | Duplicate output unless each device is assigned to one node. Main's `DeviceConfig.isAssignedToNode` must accept the PeerLink NodeId (R5); WARN when PeerLink is on and devices are assigned to `local`/`*` with a shared config store. |
| Bridges to the peer | Startup WARN when an MQTT client connector's host equals a peer host (spec §7) |

---

## 11. Milestones

| M | Scope | Exit criteria |
|---|---|---|
| **J0** Decisions | D5 spike: BCJSSE `SSLContext` → Netty `JdkSslContext` → Vert.x `NetServer`/`NetClient`, ALPN, `upgradeToSsl` after sniffing, exporter output equal to Go for one session; golden-vector generator merged in edge | Decisions recorded in this file; spike result recorded (BCJSSE, or the fallback chosen) |
| **J1** Codec and log | `wire/*`, `PeerLog`, `PeerLinkConfig` (parsing and validation of every spec §10 row, unknown-key rejection, schema block) | Byte-identical against Go vectors; log and config unit tests green |
| **J2** One-way link, plain TCP | `PeerServer` (sniffing, handshake, sessions), `Puller`, `Injector`, capture tap, `BrokerMessage`/codec extensions, gates, retained replicas through the existing queue with backpressure and the drop counter (6.3), manager wiring in `Monster.startMonster`, coexistence (D3: cluster and Kafka bus refused, Zenoh allowed), NodeId (D4), reserved ids | Kotlin → Kotlin and **Go edge → Kotlin → Go edge**: QoS 0/1/2, retained set/delete, MQTT 5 properties, no echo |
| **J2a** Zenoh coexistence | Replicas forwarded through the Zenoh `Allow`/`Deny` filters; tap on the Zenoh-inbound path; `peerSource` in `ZenohMessageEnvelope`; one-PeerLink-hop guard; replica uuid registered in and checked against the Zenoh dedup cache | edge-a ↔ main-1 ⇄ Zenoh ⇄ main-2 ↔ edge-b: data flows end to end, respecting the Zenoh and Capture filters; edge-a never receives its own messages back; edge-a linked to main-1 and main-2 gives no duplicates within the dedup window |
| **J3** Failure semantics | Resume/dedup, GAP/overflow, takeover and duplicate detection, keepalive/deadlines, backoff, wills and supersession, snapshot FILL/NEWER, pacing, clock skew, shutdown hook and drain | Restart, drop, overflow and drain scenarios with exact counters, against Kotlin and Go peers |
| **J4** Security | TLS listener/dialer (BCJSSE per D5), ALPN, identity, pins, AutoGenerate, admission/CIDR, fail-closed validation, shared secrets | mTLS, pin and secret matrix interoperates with Go; fail-closed startup cases |
| **J5** Observability and docs | Status/resync endpoint, status JSON, metrics, log events, startup warnings; `doc/peerlink.md`, `doc/configuration.md`, schema, README | Status JSON field names diffed against the Go output: identical |
| **J6** Cross-implementation suite | Full PL-mapped pytest suite edge ↔ main; mixed mesh of three (edge-a, edge-b, main) | All cases green in CI; N-1 interop rule (edge plan PL-27) applies to both code bases from now on |

The order is strict: J2 needs J1's vectors, and J4 needs J2's server.

---

## 12. Risks

| ID | Risk | Mitigation |
|---|---|---|
| R1 | Asynchronous retained writes drop silently on overflow; FILL "absent" checks race the writer | 6.3: blocking `put` for replicas (backpressure), drop counter for local publishes, queue drained before the snapshot, queued-topics set |
| R2 | Echo loops when a clone, the codec, `publishInternal` or the Zenoh envelope drops `peer` | One copy helper; reflective test over every copy path; J2 "no echo" exit; J2a test |
| R3 | Gating the queue-first path starves online persistent sessions | Gate only the offline/created branches; dedicated test |
| R4 | `userProperties` as a Map | D7 (accepted) |
| R5 | `local` node ids make per-node device assignment impossible | D4, `isAssignedToNode` change, WARN |
| R6 | No shutdown sequence; server disconnects publish wills | Section 7; draining flag first |
| R7 | Coexistence with cluster and Kafka bus; loops and duplicates with Zenoh | D3: cluster and Kafka bus refused at startup; Zenoh per 5a (one-PeerLink-hop guard, uuid dedup for double paths) |
| R8 | Expired retained entries stay in stores (`isExpired` only at delivery) | Snapshot skips expired values |
| R9 | One broker per JVM; no process harness | pytest process fixture; Go edge as counterpart |
| R10 | BCJSSE integration with Vert.x/Netty (ALPN, `upgradeToSsl` after sniffing) needs more work than expected | J0 spike; fallbacks: Netty pipeline for the peer port, Conscrypt, JDK 25 |
| R11 | Spec drift between the two code bases | Golden vectors in CI; spec §3 and §10 stay normative in the edge repo; every protocol or config change lands in both repos with an updated vector file |
| R12 | GC pauses on large logs (256 MiB default `Log.MaxBytes`) | Records stored as `ByteArray` frames (no object graph per record); J3 measures pause impact; document `-Xmx` ≥ 2.2 × `Log.MaxBytes` + headroom |
| R13 | Two BouncyCastle provider versions on the classpath (`bcprov` 1.81 and 1.85.2 in `target/dependencies`) | Pin `bcprov`/`bctls`/`bcpkix` to one version in `pom.xml` |

---

## 13. Files touched (expected)

- New: `K/peerlink/**`, `broker/src/test/kotlin/peerlink/**`, `broker/src/test/resources/peerlink/vectors.json`, `tests/pytest_tests/peerlink/**`, `doc/peerlink.md`.
- Changed:
  - `K/Monster.kt` (config, NodeId, coexistence, wiring, shutdown hook);
  - `K/data/BrokerMessage.kt` and `K/data/BrokerMessageCodec.kt`;
  - `K/handlers/SessionHandler.kt` (tap, gates, `applyRetainedOnly`, metrics);
  - `K/handlers/MessageHandler.kt` (archive gate, blocking replica enqueue, drop counter, queued-topics set);
  - `K/bus/MessageBusZenoh.kt` and `K/bus/ZenohMessageEnvelope.kt` (`peerSource` envelope field, replica uuid in the dedup cache);
  - `K/MqttClient.kt` (reserved ids, will flag, session-established time);
  - `K/devices/mqttclient/MqttClientConnector.kt` and the other outbound connectors;
  - `K/stores/devices/DeviceConfig.kt`;
  - `broker/pom.xml` (explicit `bctls-jdk18on`, aligned BouncyCastle versions);
  - `broker/yaml-json-schema.json` (`PeerLink` block), `broker/config-default.yaml` (commented example), `doc/configuration.md`, `doc/zenoh.md`.
- Edge repo: golden-vector generator and `testdata/vectors.json`. The edge spec §17 is replaced by a pointer to this plan.
