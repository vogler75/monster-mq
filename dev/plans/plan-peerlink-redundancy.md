# Plan: PeerLink and broker redundancy (Kotlin MonsterMQ broker)

**Status (2026-10-09): reviewed and clarified; implementation planned for Parts II/III. GraphQL remains gated.**

| Part | Content | Status |
|---|---|---|
| I | PeerLink replication, wire- and config-compatible with MonsterMQ Edge | **Implemented** in `2795cde0` (`broker/src/main/kotlin/peerlink/**`, `doc/peerlink.md`). Kept here as the design record and the base for Part II. |
| II | Redundancy roles on top of PeerLink: broker role (`ACTIVE`/`STANDBY`), role sources incl. the `WITNESS` lease, split handling, per-component `Redundancy` setting | **Plan, not implemented.** Open decisions in III.1. |
| III | Combined decisions, phases, tests, docs, risks | Plan |

This file replaces three earlier documents, merged without loss of content:

- `plan-peerlink.md` → Part I
- `PEER_REDUNDANCY_ROLES.md` → Part II (roles, component setting, nodeId cleanup)
- `plan-redundancy-witness.md` → Part II (role source `WITNESS`, split mode)

Normative edge spec: `edge/winccoa/doc/spec-peerlink-redundancy.md`. The edge redundancy plan is `edge/dev/plans/plan-peerlink-redundancy.md`; the edge PeerLink plan `edge/dev/plans/plan-peerlink.md` was removed, its last version is at commit `2fe2282:dev/plans/plan-peerlink.md` in the edge repo. "spec §x" means that spec. `K/` means `main/broker/src/main/kotlin/`.

**How the parts fit together.** PeerLink (Part I) is active-active: every node accepts clients and replicates every publish. That is correct for data but wrong for components that talk to external systems (bridges, devices, archive groups on a shared DB, outbound loggers): they run on every node and duplicate work. Part II adds one broker role per node and one gate in the code, so such components act only on the `ACTIVE` node. The role travels over the existing PeerLink connection (`CapRole`), and is decided by a role source: a static setting or an external witness lease. The main broker has no WinCC OA role source; deriving the role from WinCC OA redundancy needs the native WinCC OA link and is done by the edge broker (`edge/dev/plans/plan-peerlink-redundancy.md`, source `WINCCOA`).

Inside Part I, section numbers ("section 5a", "6.3", "spec §x") refer to Part I and the edge spec, as in the original document.

---

# Part I: PeerLink (implemented)

### I.0 Sources and parity rule

| Document | Role |
|---|---|
| `edge/winccoa/doc/spec-peerlink-redundancy.md` (edge repo, last changed in `c19866b`) | **Normative.** Section 3 (wire protocol), sections 4-9 (behaviour), section 10 (configuration), sections 11-12 (security, observability). Section 17 is the first JVM code map; it is superseded by this plan and carries a note pointing here. |
| `edge/dev/plans/plan-peerlink.md` (removed from the edge repo; last version at commit `2fe2282:dev/plans/plan-peerlink.md`) | Design rationale and rejected alternatives. |
| `edge/internal/peerlink/**` | Reference implementation. Where the spec and the Go code disagree, the Go code wins, as on the edge. |

Below, "spec §x" means the edge spec, and `K/` means `main/broker/src/main/kotlin/`.

**Parity rule.** The Kotlin broker implements PeerLink **the same way as the edge broker**:

1. **Same protocol.** `mmq-peer/1` is implemented byte-exactly (spec §3). A Kotlin node and a Go edge node can be linked in either direction, as source, consumer, or both.
2. **Same configuration.** The `PeerLink` YAML block has the same keys, types, defaults, validation and fail-closed rules as spec §10. A `PeerLink` block copied from an edge config starts unchanged on the Kotlin broker; the only differences are the documented JVM mappings in 3.2.
3. **Same behaviour and observability.** Capture filters, drop reasons, counters, log events, the status JSON field names (spec §12.1) and the status/resync HTTP endpoints are identical.
4. **Deviations** are allowed only where the Kotlin broker core has no equivalent (section 3.2, section 10). Each one is listed in this plan and in `doc/peerlink.md`.

---

### I.1 Goal, scope, non-goals

#### I.1.1 Goal

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

#### I.1.2 In scope

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

#### I.1.3 Non-goals (same as the edge, spec §1)

- Replication of sessions, subscriptions, offline queues or inflight state.
- A durable log or spilling to disk.
- Multi-hop forwarding.
- PUBACK gated on the peer, fencing, or role ownership.
- PeerLink configuration through GraphQL or the dashboard, and hot reload. The YAML is static, as on the edge.
- WinCC OA native redundancy features (spec §14: `connectToRedundantHosts`, `oaRetained`, the native status object). The Kotlin broker always announces `oaSystem = ""` and `topicRoot = ""` (3.3).
- Running PeerLink together with Hazelcast clustering (`-cluster`) or the Kafka message bus (D3). Zenoh federation **is** supported (section 5a).

---

### I.2 Decisions (milestone J0)

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

### I.3 Configuration

#### I.3.1 The `PeerLink` block

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

#### I.3.2 JVM mappings (the only differences)

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

### I.4 Architecture in the Kotlin broker

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

### I.5 Source side: capture

#### I.5.1 Tap point

There is one tap at the top of `SessionHandler.publishMessage(message, forwardToExternalBus)` (`K/handlers/SessionHandler.kt:1539`). It runs before the bulk-buffer branch, so capture order equals accept order even with `publishBulkProcessingEnabled`. Condition: `message.peer == null && message.peerSource == null` (not a PeerLink replica, and not a Zenoh copy of one; section 5a).

- Every client publish, will and internal publish passes here once, after ACL and schema checks. Internal publishes include GraphQL, REST, MCP, bridges, OA, flows, agents, scripts and `publishInternal`.
- QoS 1 is captured before PUBACK, QoS 2 at PUBREL. The J1 spike verifies this against `MqttClient.kt`.
- Messages received from Zenoh (`forwardToExternalBus = false`) **are** captured, filtered by `Capture.Include/Exclude`, like MQTT bridge inbound messages on the edge (section 5a). The Hazelcast cluster path does not exist with PeerLink (D3).
- The capture chain and its counters follow spec §4.1 steps 1-10. `inline` = publishes without a network client (internal publishers). Their `clientId` is the internal sender id, or `inline` when there is none.

#### I.5.2 Message model

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

#### I.5.3 Retained ordering

The Kotlin broker writes retained values asynchronously through the `RM` writer thread. Spec §4.1 requires log order to equal retained apply order per topic. For publishes, the capture happens on the publisher thread before `messageHandler.saveMessage` enqueues the retained write. Both preserve per-publisher FIFO order, so per-topic order holds for a single publisher. Concurrent publishers on the same retained topic may diverge, as documented for active-active on the edge (spec §1, arrival order). The J3 tests check this explicitly; optional per-topic striping as in Go `SerializeRetained` is a J3 contingency.

#### I.5.4 Wills

- Wills are captured with `expirySec = 0` and the WILL flag.
- They are skipped (`skipWill`) when `Capture.Wills: false` or while draining.
- Server-initiated disconnects during shutdown publish wills. The drain flag must therefore be set before `disconnectClient` (section 7).

---

### I.5a Coexistence with Zenoh federation (D3, D9)

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

### I.6 Consumer side: injection and gates

#### I.6.1 Injector

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

#### I.6.2 Gates (no hook system; `msg.peer != null` checks)

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

#### I.6.3 Retained replicas: the existing retained queue (D6)

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

### I.7 Shutdown and drain

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

### I.8 Security, status, observability

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

### I.9 Tests

| Level | What | Where |
|---|---|---|
| Golden vectors | **Needs a small edge change:** a Go test with `-update` (or a `cmd/peerlink-vectors`) writes frames, records, tombstones, batches with CRC, and MAC inputs/outputs to `edge/internal/peerlink/wire/testdata/vectors.json`. The file is copied into `main/broker/src/test/resources/peerlink/`, and the Kotlin tests require byte-identical encoding and equal decoding. A CI check flags drift. | edge + main |
| Unit | Codec round trips and fuzz-style mutation (decoders never throw outside the declared errors); log (eviction, commit/trim, loss accounting, resume table spec §3.10, drain/seal); filters; config rows and fail-closed rules; TLS identity/pins | `mvn test` |
| Broker-level | Two Kotlin brokers in one test JVM are not possible (one broker per JVM, risk R9). New pytest process fixture that starts two broker processes with separate configs and ports. | `tests/pytest_tests/peerlink/` |
| **Cross-implementation (J6)** | Go edge binary ↔ Kotlin broker, both directions and bidirectional. Each case maps to the edge PL ids: QoS 0/1/2, retained set/delete, MQTT 5 properties, wills and supersession, resume after consumer and source restarts, overflow/GAP, snapshot FILL and NEWER, drain, mTLS, pins, secrets (D5), wrong secret → `auth_failed`, `wrong_node`, `self_connection`, duplicate NodeId, oversize/tombstone, MaxMessageSize mismatch | pytest, edge binary from `edge/bin` |
| Malformed peer | The scripted Go test peer from `edge/test/integration` drives the Kotlin server with bad preambles, short frames, unknown frame types and older minors | edge test harness, pointed at the Kotlin port |

---

### I.10 Mixed edge/main specifics

| Topic | Behaviour |
|---|---|
| Message size | Default `MaxMessageSize` differs (edge 1 MiB, main 512 KiB). An edge record > 512 KiB is dropped on main (`dropped{size}`, retained also `retainedDiverged{size}`), and `HELLO.maxRecordBytes` makes the edge send a tombstone. Document: align `TCP.MaxMessageSizeKb` with the edge `MaxMessageSize`. |
| User properties | Order and duplicate keys are lost on a Kotlin receiver (D7) |
| Retained class | Main is usually DB (SQLite/Postgres) and the edge often MEMORY; `retainedClassMismatch` is a WARN, as on the edge |
| WinCC OA | An edge in native mode excludes `winccoa/#` itself, and main never receives it. Main announces an empty `topicRoot`, so no mismatch WARN. |
| Devices on both nodes | Duplicate output unless each device is assigned to one node. Main's `DeviceConfig.isAssignedToNode` must accept the PeerLink NodeId (R5); WARN when PeerLink is on and devices are assigned to `local`/`*` with a shared config store. |
| Bridges to the peer | Startup WARN when an MQTT client connector's host equals a peer host (spec §7) |

---

### I.11 Milestones

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

### I.12 Risks

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

### I.13 Files touched (expected)

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
- Edge repo: golden-vector generator and `testdata/vectors.json`. Edge spec §17 is superseded by this plan; §17 carries a note pointing here.

---

# Part II: Redundancy roles on PeerLink broker pairs (plan)

## II.0 Shared contract with the edge broker

This contract is word for word the same in `edge/dev/plans/plan-peerlink-redundancy.md` (section 3.0) and `main/dev/plans/plan-peerlink-redundancy.md` (section II.0). Change both or neither. Where the rest of either plan disagrees with this contract, the contract wins. The only intended difference between the brokers: `Source: WINCCOA` exists on the edge broker only (it needs the native WinCC OA link); the main broker rejects it at startup.

**C1 Wire: `CapRole` (`mmq-peer/1`, spec section 3.6).** One wire change for both code bases; accepted for both brokers with the owner-authorized review recommendations (2026-10-09); requires one new golden-vector set (Kotlin and Go).

- Capability bit `CapRole = 1<<4` (after `CapTombstone`). `CapInterest = 1<<5` belongs to the interest-routing plans.
- Role block, 18 bytes, little-endian:

  | Offset | Field | Type | Meaning |
  |---|---|---|---|
  | 0 | `role` | u8 | `UNKNOWN=0`, `ACTIVE=1`, `STANDBY=2`; any other value reads as `UNKNOWN` |
  | 1 | `roleSeq` | u64 | sender's counter, starts at 1 with the process, +1 on each own role change |
  | 9 | `leaseEpoch` | u64 | witness lease epoch the sender acts on; 0 without `WITNESS` |
  | 17 | `flags` | u8 | bit 0 `witnessReachable`, bit 1 `leaseHolder`; other bits sent as 0, ignored on read |

- The block is appended after the last field of `HELLO` (when the consumer offers `CapRole`) and of `HELLO_OK`, `PING` and `PONG` (when `CapRole` is agreed). Decoders ignore trailing bytes, so a peer without `CapRole` reads all four frames unchanged; its role counts as `UNKNOWN`. Data replication is not affected.
- The field is called `leaseEpoch` to keep it apart from the log `epoch` in `HELLO_OK` and in the PeerLink status.
- With `CapRole` agreed:
  - the consumer sends `PING` every `keepAlive/2` also while a `FETCH` is outstanding, and at once after its own role changes;
  - the source answers every `PING` with a `PONG` carrying its role block, and sends an unsolicited `PONG` with token 0 at once after its own role changes. Token 0 is never used for RTT; consumers never send token 0.
- Every build that implements `CapRole` offers it in `SERVER_HELLO` and `HELLO` whatever `Redundancy.Source` is, so a `Source: NONE` node is never seen as `UNKNOWN`.
- `Source: NONE` is sent as `ACTIVE`. `STANDALONE` and the WinCC OA view `PASSIVE` never go on the wire.

**C2 Peer reachability and peer role.**

- A peer is reachable when a `HELLO`, `HELLO_OK`, `PING` or `PONG` from it arrived on any PeerLink connection, in either direction, within the last `PeerTimeoutMs`. Frames without a role block count for reachability too.
- The peer's role is taken from the role block with the highest `roleSeq` received from it; a new `HELLO` or `HELLO_OK` from the peer resets the stored `roleSeq` (the peer may have restarted). An unreachable peer has role `UNKNOWN`.
- `Partner` set: only that peer counts in the role rules (C4). `Partner` empty: every PeerLink peer counts; "peer reachable" means any peer is reachable, "peer `ACTIVE`" means any reachable peer reports `ACTIVE`. On edge with `WINCCOA`, an empty `Partner` defaults to the peer with `RedundancyPartner: true`.

**C3 Timing.**

- `STANDBY → ACTIVE`: at once when the rule says so (peer unreachable for `PeerTimeoutMs`, lease won, own WinCC OA host active, `STATIC` switch).
- `ACTIVE → STANDBY`: after `StandbyGraceMs`, and only if the rule still says `STANDBY` then.
- Fencing, at once without grace: local lease validity expires (except the explicit total-isolation `KEEP` case), lease lost to a higher `leaseEpoch`, a split resolved against this node, or voluntary handover. `StandbyGraceMs` never extends a valid lease.
- `WITNESS`: the preferred node acquires an expired lease at once, the others only after `Witness.TakeoverDelayMs` past expiry. A confirmed voluntary release bypasses that delay. There is no other hold-down.

**C4 Role rules.**

- `NONE` (default): always `ACTIVE`; component modes (C6) have no effect.
- `STATIC` and `WINCCOA` share one table. Own input: `STATIC` = `Static.Role` or its runtime override (C9); `WINCCOA` = own WinCC OA host active → `ACTIVE`, passive → `STANDBY`, unknown → `UNKNOWN`.

  | Own input | Peer reachable | Peer role | Role |
  |---|---|---|---|
  | `ACTIVE` | any | any | `ACTIVE` |
  | `STANDBY` / `UNKNOWN` | yes | `ACTIVE` | `STANDBY` |
  | `STANDBY` / `UNKNOWN` | yes | `STANDBY` / `UNKNOWN` | `ACTIVE` |
  | `STANDBY` / `UNKNOWN` | no | – | `ACTIVE` (fail open) |

  Both `ACTIVE` with the link up (e.g. both WinCC OA hosts active in split mode, or a misconfigured `STATIC` pair): both stay `ACTIVE`, WARN, `split: SPLIT_DUAL_ACTIVE`.
- `WITNESS`: the role table, isolation rule (`OnIsolation`), split cases and resolution are the same in both plans (main II.6.2, II.8; edge 3.4). Resolution order: lease holder, then higher `leaseEpoch`, then lower `Priority`, then lower `NodeId`; the loser goes `STANDBY` at once.
- `ELECTION` is dropped from v1 (owner authorized the review recommendations on 2026-10-09). `WITNESS` covers pairs and larger groups; `ELECTION` is a startup error in both brokers.

**C5 YAML.** Same key set and the same JSON schema in both brokers:

```yaml
Redundancy:
  Source: NONE             # NONE | STATIC | WITNESS | WINCCOA (edge only)
  Partner: ""              # PeerLink NodeId of the other half; empty = all peers
                           # (edge with WINCCOA: the peer with RedundancyPartner: true)
  Priority: 100            # lower = preferred (WITNESS); tie: lower NodeId
  StandbyGraceMs: 5000     # ACTIVE -> STANDBY delay (C3)
  PeerTimeoutMs: 10000     # peer unreachable after this (C2)
  Static:
    Role: ACTIVE           # ACTIVE | STANDBY; required with Source: STATIC
  Witness:
    Group: plant-a         # lease name, same on all nodes of the group; required with WITNESS
    Store: POSTGRES        # POSTGRES | MONGODB
    Connection: default    # default = the broker's Postgres / MongoDB section,
                           # or an own Url/User/Password block
    LeaseTtlMs: 10000
    RenewIntervalMs: 2500
    SafetyMarginMs: 2000
    TakeoverDelayMs: 3000
    Failback: false
    OnIsolation: STANDBY   # STANDBY | KEEP
```

- Keys outside this block are a schema error in both brokers. A key or value that a broker cannot act on is accepted with one WARN at start, except `Source: WINCCOA` on main, which is a startup error (otherwise the broker would run without a role source).
- Validation, same in both:
  - `Source != NONE` without PeerLink: WARN (no peer fallback, no split detection over the link);
  - `STATIC` without `Static.Role`: startup error;
  - `Partner` names no configured PeerLink peer: WARN;
  - `WITNESS` without `Witness.Group`, or with `SafetyMarginMs >= LeaseTtlMs` or `RenewIntervalMs > LeaseTtlMs / 2`: startup error;
  - `WITNESS` with `Connection: default` whose store host is this node: WARN (witness not independent);
  - `Priority` outside 0..2147483647, non-positive lease/renew/peer timeout, negative safety margin/takeover delay/standby grace: startup error;
  - `Source: ELECTION`: startup error (dropped from v1).
- `Witness.Connection` is either the string `default`, or `{Url: <connection URL>, User: <optional username>, Password: <optional password>}` for the selected store; unknown keys fail validation. Group is a non-empty, case-sensitive string used verbatim in both stores; NodeId follows PeerLink canonicalization. Do not apply hostname or lowercase normalization to Group.

**C6 Component setting `Redundancy`.** On every device / bridge config, every archive group and every script:

```text
Redundancy: ALWAYS | HOT_STANDBY | COLD_STANDBY      (default ALWAYS)
```

| Mode | On `ACTIVE` | On `STANDBY` |
|---|---|---|
| `ALWAYS` | runs (today's behaviour) | runs (today's behaviour) |
| `HOT_STANDBY` | runs; inbound is published; outbound sends **all** messages, local and replicas | runs and stays connected; inbound is discarded; nothing is sent outbound; device writes rejected with a log line; scripts run with outputs suppressed; archive groups stay connected and drop writes (the last-value store keeps being fed) |
| `COLD_STANDBY` | as `HOT_STANDBY` | not started, no connection (archive groups: writer and DB connection stopped, last-value store kept); the persisted `enabled` flag is not changed |

- Bidirectional bridges apply the mode to both directions together.
- Server-type components (listeners) offer only `ALWAYS` and `COLD_STANDBY`.
- With `Source: NONE` every mode behaves like `ALWAYS`.
- `HOT_STANDBY` / `COLD_STANDBY`: `PeerLink.Receive.BridgeOutbound` is ignored; the role decides.
- `ALWAYS`: the `BridgeOutbound` guard applies as today (deprecated, one WARN when set with `Source != NONE`). One WARN at start for every outbound bridge left at `ALWAYS` while `Source != NONE`.
- Interest routing (`CapInterest`): the filters of `HOT_STANDBY` and `COLD_STANDBY` components are announced to the peers whatever the role and `BridgeOutbound`, also while a `COLD_STANDBY` component is not running, so a switchover never waits for an interest update.

**C7 Status, metrics, log.**

- `$SYS/broker/redundancy/role`, retained, published on every change by both brokers (edge also in native mode):
  `{role, source, epoch, holder, witnessReachable, peers: [{nodeId, reachable, role}], split, since, reason}`.
  `role`: `ACTIVE` | `STANDBY` | `UNKNOWN` (`NONE` reports `ACTIVE`); `source`: the YAML value; `epoch`: `leaseEpoch`, 0 without `WITNESS`; `holder`: lease holder `NodeId` or `null`; `witnessReachable`: `null` without `WITNESS`; `split`: `NONE` | `SPLIT_LINK` | `SPLIT_DUAL_ACTIVE`; `since`: ISO-8601 UTC with milliseconds; `reason`: text of the last change.
- Metrics: `redundancy_role` (0 `UNKNOWN`, 1 `ACTIVE`, 2 `STANDBY`), `redundancy_epoch`, `redundancy_split` (0 / 1), `redundancy_lease_renew_failures_total`, `redundancy_role_changes_total`.
- PeerLink status JSON (spec section 12.1): every entry of `consumers[]` and `sources[]` gets `role`, `roleSeq`, `leaseEpoch`, `witnessReachable`, `leaseHolder` of that peer (`"UNKNOWN"`, 0, `false` without `CapRole`). The existing `epoch` there stays the log epoch.
- Log: one INFO line per role change with the reason; WARN on split entry and exit.

**C8 Witness storage.** One layout, so edge and main nodes can share one witness group.

- Postgres: tables `redundancylease` and `redundancynodes` exactly as main II.5.1, acquire / renew statement exactly as main II.5.2 (edge 3.4 repeats it). `role` holds `ACTIVE` | `STANDBY` | `UNKNOWN`. Lease `released` is Boolean, default false; heartbeat `priority` is integer, default 100.
- MongoDB, field names and types fixed:
  - `redundancylease`: `{_id: <group>, holder: <NodeId>, epoch: Int64, expiresAt: Date, updatedAt: Date, released: Boolean}`;
  - `redundancynodes`: `{_id: {group: <group>, nodeId: <NodeId>}, role: "ACTIVE" | "STANDBY" | "UNKNOWN", epoch: Int64, linkUp: Boolean, priority: Int32, version: String, heartbeatAt: Date}`.
- Expiry uses the store clock only (`now()` / `$$NOW`); nodes measure durations on their monotonic clock.
- Priority is learned from witness heartbeat rows, not from the 18-byte role block. Fresh means age no greater than `PeerTimeoutMs`, measured with the store clock. Cache the last known priorities for resolution during witness loss; if either priority is unknown, use lower NodeId for that tie-break. The preferred candidate is the lowest `(priority, NodeId)` among self and known group members that are configured PeerLink peers. Retain their last recorded priority even when their heartbeat becomes stale, so a dead preferred node does not remove the non-preferred takeover delay. Ignore rows for peers removed from configuration. Before a peer has ever reported its priority, first-winner startup applies; freshness is required for failback availability, not for priority ranking. A simultaneous first start may elect whichever node acquires first; `Failback: false` does not promise preferred ownership on a cold start.
- `released: true` distinguishes voluntary handover from timeout. Acquire/renew sets it false. Release matches group, holder and epoch, sets it true and expiry to store-now, and keeps the row/epoch. Quiesce component outputs and complete in-flight external operations before release; if quiescence cannot be confirmed, do not release early. A releasing node must not reacquire during shutdown or while a fresh preferred candidate is available for failback.
- Lease validity is checked before each external operation, including after a process pause, and again after every acquire/renew response. A response received after its local deadline cannot enable outputs. Timers alone are insufficient; cold components are also gated while stopping. Operations already sent cannot be fenced by a broker role change, so strict exactly-once side effects across failover are not promised.

**C9 GraphQL.** Same SDL in both brokers (both schemas already have `scalar Long`; timestamps are `String`). Needs human commitment per AGENTS.md in both repositories.

```graphql
enum RedundancyRole { UNKNOWN ACTIVE STANDBY }
enum RedundancySource { NONE STATIC WITNESS WINCCOA }
enum RedundancySplit { NONE SPLIT_LINK SPLIT_DUAL_ACTIVE }
enum ComponentRedundancy { ALWAYS HOT_STANDBY COLD_STANDBY }

type RedundancyPeer {
    nodeId: String!
    reachable: Boolean!
    role: RedundancyRole!
}

type BrokerRedundancy {
    role: RedundancyRole!
    source: RedundancySource!
    epoch: Long!
    holder: String
    witnessReachable: Boolean
    split: RedundancySplit!
    peers: [RedundancyPeer!]!
    since: String!
    reason: String
}

# added to type Query
brokerRedundancy: BrokerRedundancy!

# added to type Mutation (top level in both brokers)
setRedundancyRole(role: RedundancyRole!): BrokerRedundancy!
```

- `setRedundancyRole` is valid only with `Source: STATIC` and `role` `ACTIVE` or `STANDBY`; otherwise a GraphQL error. It overrides `Static.Role` at runtime, is not persisted, and is lost on restart.
- Per component: `redundancy: ComponentRedundancy` on every device / bridge, archive group and script input (optional, default `ALWAYS`) and output (non-null), next to `nodeId`.

Implementation order is coordinated with the interest plan's "Implementation readiness and cross-plan order" section. Role/component core and configured-filter providers are implemented before interest integration; GraphQL and all dashboard work ship only in main R7 / edge PG after the separate commitment gate. This plan revision accepts the review recommendations; it does not authorize GraphQL implementation or claim native-pair verification.

## II.1 Problem

Two (or more) MonsterMQ brokers are connected with PeerLink (Part I). PeerLink is **active-active**: both brokers accept clients and replicate every publish to each other. The main broker has no native WinCC OA connectivity, so it cannot take its role from a WinCC OA redundant system; that case is covered by the edge broker.

Components that talk to external systems run on both brokers, and nothing coordinates them:

| Component | What goes wrong today |
|---|---|
| Inbound bridges/devices (MQTT client, OPC UA, PLC4X, WinCC OA, Kafka, NATS, …) | Both brokers receive the same source data and publish it. PeerLink replicates each copy to the other side, so every value exists twice. |
| Outbound bridges/loggers | Only MQTT/NATS/Kafka/Redis have the `Receive.BridgeOutbound` guard (`msg.peer != null` check at `MqttClientConnector.kt:582`, `NatsClientConnector.kt:327`, `KafkaClientConnector.kt:329`, `RedisClientConnector.kt:559`). Telegram, Neo4j, the JDBC/Influx/TimeBase loggers, Sparkplug, Script and Flows forward replicated messages again. |
| Archive groups on a central HA database | `Receive.Archive` defaults to `true`, so both brokers write every message to the same DB. |
| Archive groups on a per-node DB (SQLite) | These work correctly and must keep working: each node wants the full data set. |

The Kotlin broker has no notion of WinCC OA redundancy (`_ReduManager`), and this plan does not add one: a role source based on WinCC OA belongs to the edge broker, which has the native link (`edge/internal/winccoanative/redu.go`).

Without a witness, the only role sources would be `STATIC` with fail-open peer fallback, so a pair goes dual-active on every link break. The `WITNESS` source (II.5) fixes this: the broker decides internally which node is active, with a third-party store as tie breaker, and handles the worst case where the store shows both nodes alive but the nodes have no PeerLink connection.

## II.2 Design principles

1. **Placement and role are different things.**
   - `nodeId` answers *on which Hazelcast cluster node does this component run*. That doesn't change.
   - The new `Redundancy` setting answers *does this component act on this broker, given the broker's current role*.
   - PeerLink and cluster mode are mutually exclusive (startup aborts, `Monster.kt:1145`). In a peer setup every broker is a single node, so the two settings never interact. We do **not** treat a peer pair as a cluster, and the Vert.x cluster (`-cluster`) is neither used nor touched.
2. **One role per broker, one gate in the code.** Connectors must not each implement their own redundancy logic. That's how the current `BridgeOutbound` guard ended up implemented in only 4 of ~15 connectors. The role check for `HOT_STANDBY` / `COLD_STANDBY` is therefore central; the old `BridgeOutbound` guard for `ALWAYS` stays where it is today and is not extended here (II.11.3, D-R7).
3. **Fail open by default.** If in doubt, a broker becomes ACTIVE. Duplicate data is acceptable; lost data is not. Exceptions, chosen explicitly: `WITNESS` without a valid local lease, except the explicit total-isolation `KEEP` policy (II.5.6).
4. **Default = today's behavior.** `Redundancy: ALWAYS` on every component, and no role source configured means the broker is always ACTIVE.
5. **For `WITNESS`: the witness decides, the link confirms.** The holder of a lease in an external store is `ACTIVE`. PeerLink carries role and epoch so a split is detected even if the store is fine.
6. **The witness is outside both nodes.** A Postgres or MongoDB both brokers reach, not running on either broker host. A witness on one node gives that node the tie break by construction.
7. **Store clock, not node clock.** Lease expiry is computed with `now()` / `$$NOW` of the store; nodes only measure durations on their monotonic clock.
8. **Fencing by epoch.** Every lease takeover increments `epoch`; the higher epoch wins any conflict.

## II.3 Broker role and configuration

Each broker has exactly one role: `ACTIVE` or `STANDBY` (plus `UNKNOWN` as a transient state at startup, before the first decision; defined in II.10).

New top-level config block (all sources in one schema):

```yaml
Redundancy:
  Source: WITNESS          # NONE (default) | STATIC | WITNESS (WINCCOA: edge only, startup error here)
  Partner: ""              # PeerLink NodeId of the other half; empty = all peers (C2)
  Priority: 10             # lower = preferred; used by WITNESS; tie: lower NodeId; default 100
  StandbyGraceMs: 5000     # delay before ACTIVE -> STANDBY takes effect (II.7)
  PeerTimeoutMs: 10000     # peer considered unreachable after this (II.4)
  Static:
    Role: ACTIVE           # for Source=STATIC; can be switched at runtime (II.10, D-R5)
  Witness:                 # for Source=WITNESS (II.5)
    Group: plant-a         # lease name; all brokers of the pair use the same
    Store: POSTGRES        # POSTGRES | MONGODB
    Connection: default    # default = the broker's Postgres / MongoDB section;
                           # or an own Url/User/Password block (recommended
                           # when the main store runs on one of the nodes)
    LeaseTtlMs: 10000
    RenewIntervalMs: 2500  # ~ TTL / 4
    SafetyMarginMs: 2000
    TakeoverDelayMs: 3000  # non-preferred node waits this long after expiry
    Failback: false        # preferred node takes the lease back when it returns
    OnIsolation: STANDBY   # STANDBY | KEEP (II.5.6)
```

The key set, defaults and validation are those of the shared contract (C5); this block shows the main broker's typical values.

Merge notes: `Priority` was `Election.Priority` in the roles spec and top-level `Priority` in the witness plan; it is now one top-level key for WITNESS; ELECTION is dropped. `Group`, `Failback` and `OnIsolation` were top-level in the witness plan and now live under `Witness`, since only that source uses them.

| Source | Behavior |
|---|---|
| `NONE` | Always ACTIVE. Redundancy settings on components have no effect. This is the default. |
| `STATIC` | Role comes from `Static.Role` and can be switched at runtime with `setRedundancyRole` (C9). Automatic fail-open takeover on peer loss (II.6.1); it does not provide witness fencing. |
| `WITNESS` | The holder of a lease in an external Postgres/MongoDB is ACTIVE (II.5). For pairs or N brokers. Recommended. |
| `ELECTION` | Dropped from v1; startup error. Use `WITNESS`. |

There is deliberately no `WINCCOA` source in main. The GraphQL-based WinCC OA connector (`devices/winccoa/WinCCOaConnector.kt`) stays a normal inbound device; a pair that must follow WinCC OA redundancy uses edge brokers with the native link (edge source `WINCCOA`), which also announce their role over `CapRole`.

## II.4 Exchanging roles over PeerLink: `CapRole`

Each broker must know whether its peer is reachable and which role it claims. This goes over the existing PeerLink connection. **One wire change** covers both the roles spec and the witness plan. C1 is normative; where this section is shorter, C1 applies.

- New capability bit `CapRole` (`1L shl 4`, after `CapTombstone` in `K/peerlink/wire/Frames.kt:62-66`). Both the Kotlin broker and the Go edge implement it, since the wire protocol is shared.
- The 18-byte role block of C1 (`role` u8, `roleSeq` u64, `leaseEpoch` u64, `flags` u8) is appended to `Hello` when `CapRole` is offered, and to `HelloOK`, `Ping` and `Pong` when it is agreed. The field is called `leaseEpoch` on the wire and in status to keep it apart from the log `epoch` in `HelloOK`.
- With `CapRole` the consumer (`Puller`) sends `Ping` every keepAlive/2 also while a `Fetch` is outstanding, and at once on its own role change. The source (`PeerServer`) answers every `Ping` with a `Pong` carrying its role block, and sends an unsolicited `Pong` with token 0 on its own role change. Token 0 is never used for RTT, and consumers never send token 0. Each side learns about a role change within one frame round trip, at worst one `Ping` interval.
- If the peer doesn't support `CapRole`, its role is treated as `UNKNOWN`. Data replication keeps working.
- **Peer reachable** (C2): a `Hello`, `HelloOK`, `Ping` or `Pong` from that peer arrived on any PeerLink connection, in either direction, within `PeerTimeoutMs`. The earlier "Puller session established and last `Pong` younger than `PeerTimeoutMs`" is replaced, so a peer that only pulls from this node also counts.

No extra port or topic is needed. The role is tied to the same connection that carries the data, so "the peer is alive" and "the peer is replicating to me" are the same signal.

Shipping all four fields at once avoids a second negotiation step later. The Go edge decodes and uses all of them (edge plan 3.0, same contract). **Accepted for both brokers (2026-10-09)**; requires a new golden-vector set (Part I, section 9).

## II.5 Role source `WITNESS`

### II.5.1 Tables

Two new tables/collections, created on start like the other stores (`CREATE TABLE IF NOT EXISTS`, lowercase names):

```sql
CREATE TABLE IF NOT EXISTS redundancylease (
  groupname  TEXT PRIMARY KEY,
  holder     TEXT NOT NULL,
  epoch      BIGINT NOT NULL,
  expiresat  TIMESTAMPTZ NOT NULL,
  updatedat  TIMESTAMPTZ NOT NULL,
  released   BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE TABLE IF NOT EXISTS redundancynodes (
  groupname   TEXT NOT NULL,
  nodeid      TEXT NOT NULL,
  role        TEXT NOT NULL,
  epoch       BIGINT NOT NULL,
  linkup      BOOLEAN NOT NULL,   -- PeerLink to the partner up, as seen by this node
  priority    INTEGER NOT NULL DEFAULT 100,
  version     TEXT,
  heartbeatat TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (groupname, nodeid)
);
```

MongoDB: collections `redundancylease` and `redundancynodes` with the document layout of C8 (`_id` = group; `_id` = `{group, nodeId}`; lease fields `holder`, `epoch` (Int64), `expiresAt`, `updatedAt`, `released`; node fields `role`, `epoch`, `linkUp`, `priority` (Int32), `version`, `heartbeatAt`).

### II.5.2 Acquire and renew (one statement)

```sql
INSERT INTO redundancylease (groupname, holder, epoch, expiresat, updatedat, released)
VALUES ($1, $2, 1, now() + $3 * interval '1 millisecond', now(), FALSE)
ON CONFLICT (groupname) DO UPDATE SET
  holder    = EXCLUDED.holder,
  epoch     = CASE WHEN redundancylease.holder = EXCLUDED.holder
                   AND NOT redundancylease.released AND redundancylease.expiresat > now()
                   THEN redundancylease.epoch ELSE redundancylease.epoch + 1 END,
  expiresat = EXCLUDED.expiresat,
  updatedat = now(),
  released  = FALSE
WHERE (redundancylease.holder = EXCLUDED.holder AND NOT redundancylease.released
       AND redundancylease.expiresat > now())
   OR (redundancylease.expiresat <= now()
       AND (redundancylease.released OR $4
            OR redundancylease.expiresat + $5 * interval '1 millisecond' <= now()))
RETURNING holder, epoch;
```

- `$4` is whether this node is the preferred candidate; `$5` is `TakeoverDelayMs`; `$3` is `LeaseTtlMs`. Preference uses the latest group heartbeat read (C8); the final SQL atomically rechecks expiry, release and non-preferred delay. Initial insert on an empty group is first-winner. Use autocommit and bounded request timeouts; re-read/retry on contention.
- Conditional release after quiescence:

  ```sql
  UPDATE redundancylease SET expiresat = now(), updatedat = now(), released = TRUE
  WHERE groupname = $1 AND holder = $2 AND epoch = $3 AND NOT released
  RETURNING epoch;
  ```

  Zero rows means ownership has changed or release already completed; never overwrite a new holder. Retain the row so the next acquisition increments epoch.
- A row returned → this node holds the lease with that epoch, subject to the local deadline check.
- No row → acquisition/renewal was not permitted; read the lease for status and eligibility. An expired lease still waiting for takeover delay is also possible.

MongoDB: atomic `findOneAndUpdate` with the same holder/validity, released, preference and expiry/delay predicates as Postgres, using `$$NOW`; update pipeline sets `released: false`, expiry and epoch (increment on every acquisition, including an expired lease acquired again by the same NodeId). Upsert only for initial creation; a duplicate-key error means "not won". Return the document after the update.

### II.5.3 Local validity

- `tSend` = monotonic time before the statement is sent.
- After success the lease is valid locally until `tSend + LeaseTtlMs - SafetyMarginMs`.
- The holder stays `ACTIVE` only while local validity has not passed, except explicit total-isolation `KEEP`. Expiry fences immediately without standby grace. Check validity on each external operation and after every lease response, including process resume. The no-overlap claim concerns authorization to start operations and assumes clock drift stays below the margin; it does not fence operations already sent to external systems.
- Renew every `RenewIntervalMs`; a failed renew is retried until the local validity runs out.

### II.5.4 Takeover, priority, release

- Candidate preference and heartbeat freshness follow C8. Preferred candidates acquire at once; others wait `TakeoverDelayMs` after expiry, except confirmed voluntary release. First acquisition on an empty group is first-winner; preference is enforced subsequently only with `Failback: true`.
- `Failback: false` (default): a returning preferred node does not take the lease from a valid holder. `true`: the holder begins quiescence at the next renew when it sees the preferred node's heartbeat row fresh and its link up, then releases conditionally (C8).
- Graceful shutdown and failback follow C8: fence new outputs, complete in-flight external operations, then conditionally release with holder/epoch and `released = true`. Do this before `stopPullers`, while keeping replication available. Confirmed release bypasses takeover delay; the next acquisition increments epoch. Failed release never re-enables the quiesced node. A stale release must not modify a newer holder.

### II.5.5 Heartbeat row

Every renew cycle each node upserts its `redundancynodes` row (role, epoch, `linkup`, priority, version, `heartbeatat = now()`). This is what makes a split visible in the store (II.8).

### II.5.6 Isolation (`OnIsolation`)

A node that reaches neither the witness nor the partner:

- `STANDBY` (default): steps down after its local lease validity ends. Safe against dual-active; if both are isolated, nothing is active (loss instead of duplicates).
- `KEEP`: keeps its last role. Duplicates instead of loss; for sites where missing data is worse than double writes.

## II.6 Role decision

Evaluated on every input change (role source, peer state, peer role, lease state).

### II.6.1 Source `STATIC`

| My source says | Peer reachable? | Peer role | **My role** |
|---|---|---|---|
| active | – | – | **ACTIVE** |
| passive / standby | yes | ACTIVE | **STANDBY** |
| passive / standby | yes | STANDBY / UNKNOWN | **ACTIVE** (nobody else is active) |
| passive / standby | no | – | **ACTIVE** (fail open) |
| unknown | yes | ACTIVE | **STANDBY** |
| unknown | yes | STANDBY / UNKNOWN | **ACTIVE** |
| unknown | no | – | **ACTIVE** |

Important cases:

- **The `STATIC` active broker crashes.** The other broker is configured standby, but its peer is unreachable, so it becomes ACTIVE. Without this rule nobody would publish.
- **Network partition between the brokers.** Both become ACTIVE. Inbound data is duplicated and a central DB may get double writes for the duration. That's an accepted trade-off, documented as split-brain behavior. Archive writes should be idempotent where the backend supports it (upsert on topic + time); optional, out of scope for the first phases.
- **Both brokers see ACTIVE from their source** (misconfigured `STATIC`). Both stay ACTIVE (fail open), a WARN is logged and the status shows `split: SPLIT_DUAL_ACTIVE` (C4, C7).

This table is the C4 table of the shared contract, with "unknown" and "passive / standby" folded into one input there; the edge uses the same table for `WINCCOA`.

### II.6.2 Source `WITNESS`

| Witness | Lease | PeerLink to partner | **My role** |
|---|---|---|---|
| reachable | I hold it | any | **ACTIVE** |
| reachable | partner holds it | any | **STANDBY** |
| reachable | expired / none | any | try acquire (II.5.4); result decides |
| unreachable | I hold it, local validity left | up | **ACTIVE** |
| unreachable | – | up, partner `ACTIVE` | **STANDBY** |
| unreachable | no valid local lease | up, partner not `ACTIVE` | **STANDBY**; neither node may act without a valid lease |
| unreachable | – | down | `OnIsolation` (II.5.6) |

## II.7 Flapping

- STANDBY → ACTIVE takes effect **immediately** (for `WITNESS`: after a won acquire).
- ACTIVE → STANDBY takes effect only after `StandbyGraceMs`, and only if the decision is still STANDBY at that point.
- Exception (`WITNESS`): lease validity expires (except total-isolation `KEEP`), a higher epoch wins, a split is resolved against this node, or voluntary handover begins: fence at once without grace (C3/C8).

During a switchover (lease takeover, runtime `STATIC` switch, a peer flapping) the states can briefly read *both active* or *both passive*. The grace period keeps cold-standby components from being torn down and rebuilt during such a brief flip.

## II.8 Split mode

### II.8.1 Cases

| Case | Seen by | Result |
|---|---|---|
| Link down, both reach the witness | store: both heartbeats fresh, `linkup = false` on both | one lease holder → one `ACTIVE`; replication paused, reported as `SPLIT_LINK` |
| Link down, one isolated | holder or standby with witness | witness side runs; isolated node per `OnIsolation` |
| Store shows two `ACTIVE` rows, no link | store | worst case: e.g. `OnIsolation: KEEP`, or a node with a stale lease that did not notice. Reported as `SPLIT_DUAL_ACTIVE` |
| Link up, both claim `ACTIVE` | PeerLink role block (`Ping` / `Pong`, C1) | resolved at once (II.8.2) |

For non-witness sources only the last row applies (link up, both ACTIVE): with `STATIC` (and edge `WINCCOA`) both stay ACTIVE per II.6.1 (fail open), a WARN is logged and the status shows `SPLIT_DUAL_ACTIVE`.

### II.8.2 Resolution (`WITNESS`)

When a node learns the partner is also `ACTIVE` (via the role block in `Ping` / `Pong` or via the partner's heartbeat row):

1. The current lease holder wins (checked in the store if reachable).
2. Otherwise the higher `leaseEpoch` wins.
3. Otherwise the lower `Priority`, then the lower NodeId.

The loser goes `STANDBY` at once (no grace). Duplicates written while split are accepted; archive upserts on topic + time reduce them where the backend supports it.

### II.8.3 After the link returns

PeerLink resync fills the gaps in both directions (`CapResyncNewer`, `CapTombstone`, Part I). The role does not change on link return unless II.8.2 applies.

## II.9 More than two brokers

- **Data:** PeerLink supports N brokers as a **full mesh**. Delivery is one hop, so chains and rings don't propagate (Part I, spec §7).
- **Mixed with edge brokers on `WINCCOA`:** main brokers in the mesh use `Source: NONE` (always ACTIVE, components `ALWAYS`) or their own `WITNESS` group; they do not take part in the WinCC OA pair.
- **`Partner` in mixed meshes** (same rule as the edge): when the mesh has more PeerLink peers than the redundant nodes, `Partner` must be set on every `STATIC` / `WITNESS` node here (and on the edge's `WINCCOA` nodes). A `Source: NONE` node reports `ACTIVE` (C1), so with an empty `Partner` it counts as an `ACTIVE` peer (C2) and both nodes of the pair go `STANDBY`. Validation: one WARN at start when `Source` is `STATIC`, `Partner` is empty and more than one PeerLink peer is configured.
- **With `WITNESS`:** works for N brokers unchanged: one lease holder among N, all with the same `Witness.Group`.
- **`ELECTION` is dropped from v1.** Use `WITNESS` for N brokers; no election phase or priority extension to the role wire block.

## II.10 RoleManager (Kotlin)

New `K/redundancy/RoleManager.kt`, created in `Monster.kt` after PeerLink (~line 1139):

```kotlin
object RedundancyRole {
    fun current(): Role                 // UNKNOWN | ACTIVE | STANDBY
    fun isActive(): Boolean             // hot path, a volatile read; false for UNKNOWN
    const val ROLE_CHANGED = "mq.redundancy.role"   // event bus address, payload {role, seq, reason}
}
```

- **Own role `UNKNOWN`.** The own role is `UNKNOWN` only at startup, before the first decision. With `NONE` and `STATIC` the first decision is made at once when `RoleManager` starts; with `WITNESS` the role stays `UNKNOWN` until the first bounded lease attempt (II.5.2) completes. A failed attempt or timeout chooses STANDBY, including with KEEP when no ACTIVE role has ever been established. After that the own role is always `ACTIVE` or `STANDBY`. `isActive()` (and the component checks built on it, II.12) treat `UNKNOWN` as not active, so `HOT_STANDBY` and `COLD_STANDBY` components do not act until the first decision; `ALWAYS` components are not affected.

- `isActive()` must be a plain volatile read, because it is called per message on the hot path.
- Inputs: the configured source (static value, `WitnessLease`), and per peer the reachability and the `CapRole` fields from the `Puller`/`PeerServer` sessions.
- One INFO log line per role change with the reason (e.g. `"peer unreachable"`, `"static role switched"`, `"lease lost to epoch 7"`); WARN on split entry and exit.
- **Status** (always, no human commitment needed), exactly as C7:
  - `$SYS/broker/redundancy/role` (retained): `{role, source, epoch, holder, witnessReachable, peers: [{nodeId, reachable, role}], split, since, reason}`;
  - metrics: `redundancy_role` (0 `UNKNOWN`, 1 `ACTIVE`, 2 `STANDBY`), `redundancy_epoch`, `redundancy_lease_renew_failures_total`, `redundancy_split` (0 / 1), `redundancy_role_changes_total`;
  - the PeerLink status JSON (`K/peerlink/PeerStatus.kt`): every `consumers[]` and `sources[]` entry gets `role`, `roleSeq`, `leaseEpoch`, `witnessReachable`, `leaseHolder`; the existing `epoch` stays the log epoch.
- **GraphQL** (C9, in both brokers; held behind the human commitment gate per AGENTS.md in both repositories and shipped together with the edge, D-R5, R7; until then `$SYS` + metrics only): query `brokerRedundancy: BrokerRedundancy!` and top-level mutation `setRedundancyRole(role: RedundancyRole!): BrokerRedundancy!` (runtime override of `Static.Role`, only with `Source: STATIC`, not persisted). In main the SDL goes into a new `schema-redundancy.graphqls` with `extend type Query` / `extend type Mutation`; the resulting schema is the same as the edge's.
- Dashboard: role badge on the PeerLink page / header when `Source != NONE`, read from GraphQL.

## II.11 Component setting: `Redundancy`

New field on `DeviceConfig` (all connector types), on `ArchiveGroupConfig` and on the script config (C6):

```
Redundancy: ALWAYS | HOT_STANDBY | COLD_STANDBY      (default ALWAYS)
```

| Mode | On ACTIVE broker | On STANDBY broker | Failover | Source connections |
|---|---|---|---|---|
| `ALWAYS` | runs | runs (today's behavior) | – | one per broker |
| `HOT_STANDBY` | runs; inbound is published, outbound sends **all** messages incl. replicas | runs and stays connected; **inbound is discarded, nothing is sent outbound** | fast, almost no gap | one per broker |
| `COLD_STANDBY` | runs; same as HOT on the active side | **not started**, no connection | slower: connect, authenticate, subscribe, read initial values | one in total |

Discarding inbound data on the standby is safe. The active broker publishes the same data, and PeerLink replicates it to the standby, so the standby's retained store, last-value stores and subscribers stay current.

### II.11.1 Semantics per component kind

| Kind | Examples | ALWAYS | HOT_STANDBY | COLD_STANDBY |
|---|---|---|---|---|
| Inbound connector | MQTT client (inbound), OPC UA client, PLC4X, WinCC OA, WinCC UA, I3X client, Kafka/NATS/Redis client (inbound) | ✓ | ✓ | ✓ |
| Outbound connector / logger | MQTT client (outbound), Kafka/NATS/Redis (outbound), Telegram, Neo4j, JDBC/Influx/TimeBase loggers | ✓ | ✓ | ✓ |
| Processing | Script, Flow engine, Sparkplug B decoder | ✓ | ✓ (outputs suppressed on standby) | ✓ |
| Archive group | any backend | ✓ (per-node DB, e.g. SQLite) | ✓ (DB connected, writes dropped) | ✓ (typical for central DB) |
| Server-type | OPC UA server, Kafka protocol server, MCP server, Agent | ✓ | – (not offered) | ✓ (standby does not listen) |

Bidirectional connectors such as the MQTT bridge apply the mode to both directions together. There is no separate inbound/outbound mode, to keep the config simple.

With `Source: NONE` every mode behaves like `ALWAYS` (C4).

Interest routing (`CapInterest`, plan-peerlink-interest-routing.md): the filters of `HOT_STANDBY` and `COLD_STANDBY` components are announced to the peers whatever the role and `BridgeOutbound`, also while a `COLD_STANDBY` component is not running (C6).

### II.11.2 Recommended configurations

| Setup | Archive groups | Inbound bridges | `Receive.Archive` |
|---|---|---|---|
| Central HA database | `COLD_STANDBY` (or `HOT_STANDBY`) | `HOT_STANDBY` or `COLD_STANDBY` | `true` |
| DB per node (SQLite) | `ALWAYS` | `HOT_STANDBY` or `COLD_STANDBY` | `true` |

In both cases the active broker archives the messages it got from its peer (clients connected to the standby broker still publish there). `Receive.Archive=false` is therefore only needed for unusual setups, and the doc should say so.

### II.11.3 Relation to `Receive.BridgeOutbound`

- `HOT_STANDBY` / `COLD_STANDBY`: the role decides. Outbound on the ACTIVE broker sends everything, including replicas. On the STANDBY broker nothing is sent. `BridgeOutbound` is ignored for these components.
- `ALWAYS`: today's behaviour, unchanged. The `BridgeOutbound` guard stays exactly where it is today (MQTT, NATS, Kafka and Redis client connectors) and checks `msg.peer` as today; it is deprecated (one WARN when set with `Source != NONE`, and one WARN for every outbound connector left at `ALWAYS` while `Source != NONE`, C6). The other outbound connectors keep forwarding replicas with `ALWAYS`, as today. Extending the guard to all outbound connectors and to Zenoh-relayed replicas is out of scope (D-R7).

## II.12 Implementation

### II.12.1 Inbound gate (hot standby)

All device connectors publish through `sessionHandler.publishMessage(...)`. Add one entry point for device-originated data:

```kotlin
fun publishFromDevice(device: DeviceConfig, msg: BrokerMessage) {
    if (device.redundancy == HOT_STANDBY && !RedundancyRole.isActive()) { metrics.droppedStandby++; return }
    publishMessage(msg)
}
```

Migrate every connector's publish call to `publishFromDevice`. That's mechanical: grep for `publishMessage(` under `devices/`, `extensions/` and `logger/`. Archive groups check the same condition in `MessageHandler.saveMessage` before enqueuing to `archiveQueues[name]`.

### II.12.2 Outbound gate

The internal subscription that feeds outbound connectors (`subscribeInternalClient` → `handleLocalMqttMessage` and equivalents) gets a shared filter:

```kotlin
fun shouldForwardOutbound(device: DeviceConfig, msg: BrokerMessage): Boolean = when (device.redundancy) {
    ALWAYS -> true                                          // today's behaviour; the gate does not decide
    HOT_STANDBY, COLD_STANDBY -> RedundancyRole.isActive()  // role decides; BridgeOutbound ignored
}
```

Add the call to every outbound connector. Only `HOT_STANDBY` / `COLD_STANDBY` are decided by the role. With `ALWAYS` the gate returns `true`, and the existing per-connector `BridgeOutbound` guard still runs where it exists today (`MqttClientConnector.kt:582`, `NatsClientConnector.kt:327`, `KafkaClientConnector.kt:329`, `RedisClientConnector.kt:559`); in those four connectors it is skipped for `HOT_STANDBY` / `COLD_STANDBY` (C6). The guard is not moved into the gate and not added to other connectors (II.11.3, D-R7).

Correction against the roles spec: it wrote `msg.peerSource == null`. In the implemented code the replica marker is `BrokerMessage.peer` (`K/data/BrokerMessage.kt:48`), which the four guards check today; `peerSource` (`:49`) marks a replica relayed over Zenoh (Part I, section 5a). The `ALWAYS` guard keeps checking `msg.peer` only; treating Zenoh-relayed replicas as replicas is out of scope (D-R7).

### II.12.2a Write gate (hot standby)

C6 requires that on a `STANDBY` node a `HOT_STANDBY` device rejects writes. Main has no such gate today, so one is added. A write here is a value or command that the broker sends **to a device**: the device connector's write/command path, where an MQTT publish on the connector's write topics is turned into a write to the field system (e.g. writing a value to an OPC UA node or a PLC tag). One shared check is used by every connector that has such a path:

```kotlin
fun allowDeviceWrite(device: DeviceConfig, topic: String): Boolean {
    if (device.redundancy == HOT_STANDBY && !RedundancyRole.isActive()) {
        logRateLimited(device.name, "write to $topic rejected: redundancy role ${RedundancyRole.current()}")
        metrics.writesRejectedStandby++
        return false
    }
    return true
}
```

- Called at the start of each connector's write handler, before anything is sent to the device. A rejected write is dropped; nothing is queued for later.
- One log line per rejected write, rate-limited per device (e.g. at most one line per device per 10 s with the count of writes rejected since the last line).
- `ALWAYS`: no check, writes go through as today. `COLD_STANDBY`: the connector is not running on the `STANDBY` node, so there is no write path to gate. `UNKNOWN` counts as not active (II.10).
- The write handlers are found the same way as the publish calls in II.12.1 (grep for the write/command subscriptions under `devices/`); this plan does not name them per connector.

### II.12.3 Cold standby lifecycle

- Effective run state = `enabled && isAssignedToNode(nodeId) && (redundancy != COLD_STANDBY || RedundancyRole.isActive())`.
- Each extension subscribes to `ROLE_CHANGED`. For its `COLD_STANDBY` devices it reuses the existing deploy/undeploy path, the same one used for enable/disable toggles.
- The persisted `enabled` flag is **not** changed. The dashboard shows a distinct state, "Standby (cold)", so it is not confused with disabled.
- Archive groups: start/stop the group's writer and DB connection, but **not** the last-value store, which keeps being fed from replicas.

### II.12.4 Config, GraphQL, UI

- `DeviceConfig.redundancy`, persisted in all config stores (SQLite, Postgres, Mongo, CrateDB). The schema migration adds a column/field with default `ALWAYS`.
- `ArchiveGroupConfig.redundancy` and the script config (`ScriptConfig` / `ScriptConfigInput`), same treatment.
- GraphQL: `redundancy: ComponentRedundancy` on every device type, archive group and script input (optional, default `ALWAYS`) and output (non-null), next to `nodeId` (C9). It needs commitment like the broker-level query (D-R5).
- `broker/yaml-json-schema.json`: `Redundancy` block with the full key set of C5 (including `Partner` and `Static.Role`) and `additionalProperties: false`, as for `PeerLink`.
- Dashboard:
  - a role badge in the header when `Source != NONE`;
  - a Redundancy dropdown in every device/archive-group form, where server-type components offer only ALWAYS/COLD;
  - the runtime state "Standby (cold)" / "Standby (hot, dropping)" in device lists.

### II.12.5 Witness code

| File | Change |
|---|---|
| `K/redundancy/RoleManager.kt` | II.10; tables II.6.1 and II.6.2 |
| `K/redundancy/witness/WitnessLease.kt` | interface: `acquireOrRenew()`, `release()`, `read()`, `heartbeat()`, `nodes()` |
| `K/redundancy/witness/PostgresWitnessLease.kt` | SQL of II.5.2 on the Vert.x pg client |
| `K/redundancy/witness/MongoWitnessLease.kt` | II.5.2 MongoDB variant |
| `K/peerlink/wire/Frames.kt` | `CapRole`; role block (`role`, `roleSeq`, `leaseEpoch`, `flags`) in `Hello`, `HelloOK`, `Ping`, `Pong` (C1) |
| `K/peerlink/PeerStatus.kt` | per-peer `role`, `roleSeq`, `leaseEpoch`, `witnessReachable`, `leaseHolder` (C7) |
| `K/peerlink/Puller.kt`, `PeerServer.kt` | `Ping` also with a `Fetch` outstanding and on role change; unsolicited `Pong` token 0 on role change; feed reachability (C2) and role into `RoleManager` |
| `K/Monster.kt` (~1139) | create the witness and `RoleManager` after PeerLink; validation II.12.6; release in the shutdown hook |

### II.12.6 Validation

The shared validation list is C5 (and the `Partner` WARN of II.9). Main-only checks:

- `Source != NONE` together with `-cluster`: startup error (PeerLink and cluster are already exclusive).
- `Source: WINCCOA`: startup error (edge-only source; use `WITNESS` or `STATIC`).

## II.13 Cluster `nodeId` cleanup (separate track, independent)

These bugs were found while analyzing placement. They are not caused by redundancy, but they make "run once" unreliable in cluster mode today:

1. `nodeId = "local"` in a cluster: startup loading (`getEnabledDevicesByNode`, `WHERE node_id=? OR node_id='*'`) does not load it, but `isAssignedToNode` on update/toggle deploys it on **every** node. Define `"local"` as "this node only in non-cluster mode" and reject or migrate it in cluster mode.
2. Reassign doesn't deploy on the new node until restart. The extension handlers (`MqttClientExtension.kt:386-408`, OPC UA, WinCC OA, PLC4X) only act on devices already in `activeDevices`. Fix: on `reassign`, reload the device from the config store and deploy if assigned, as `KafkaServerExtension.kt:258` already does.
3. GraphQL create mutations default `nodeId` to `"*"` (`ScriptMutations.kt:54`, `McpServerMutations.kt:118`, `OpcUaServerMutations.kt:238`), which runs the component N times. Default to the current node instead.
4. `Oa4jBridge` ignores `nodeId`. `AgentExtension.kt:134` and `OpcUaServerExtension.kt:99/173` use custom checks that exclude `"local"`. Unify everything on `DeviceConfig.isAssignedToNode`.
5. `HealthHandler` leader re-election: the dead member is identified by `member.uuid`, but the leader value is `getClusterNodeId()`, which may be the `nodeName` attribute. If they differ, `remove(LEADER_KEY, deadId)` never matches and no new leader is elected. Verify and fix.

Related: Part I risk R5 (`DeviceConfig.isAssignedToNode` must accept the PeerLink NodeId).

---

# Part III: Decisions, phases, tests, docs

## III.1 Decisions and remaining gates (Part II)

Part I decisions D1-D9 are closed (Part I, section 2).

| # | Question | Recommendation |
|---|---|---|
| D-R1 | Witness: own `Witness.Connection` required, or allow `default`? | **Decided (C5):** `default` allowed, WARN when the store host is this node |
| D-R2 | `OnIsolation` default `STANDBY` (no dual-active) or `KEEP` (no loss)? | **Decided (C5):** `STANDBY`; it contradicts the general fail-open principle, so it must be explicit in the docs |
| D-R3 | Does `WITNESS` replace `ELECTION` for N≥3? | **Closed (owner authorized review recommendations, 2026-10-09):** yes; drop phase R6 and the planned enum value. |
| D-R4 | `Witness.Failback` default `false`? | **Decided (C5):** `false` |
| D-R5 | GraphQL: `brokerRedundancy` query, `setRedundancyRole` mutation and per-component `redundancy` field | Decided: in both brokers with the same SDL (C9). Held behind the human commitment gate per AGENTS.md in both repositories: GraphQL ships only after the commitment in both, together with the edge. Until then `$SYS` + metrics only |
| D-R6 | `CapRole` wire change: role block (`role`, `roleSeq`, `leaseEpoch`, `flags`) in `Hello`, `HelloOK`, `Ping`, `Pong` in `mmq-peer/1` | Accepted for both code bases (2026-10-09, C1); ship with shared golden vectors |
| D-R7 | Extend the `ALWAYS` outbound guard to all outbound connectors, and count a Zenoh-relayed replica (`peerSource != null`, `peer == null`) as a replica for it? | **Out of scope:** kept as today (guard only on the MQTT, NATS, Kafka and Redis connectors, checking `msg.peer`); a separate change may extend the guard to all connectors and to Zenoh-relayed replicas (`peerSource`) |
| D-R8 | Storage parity: the edge plan adds `WITNESS` too (same lease tables), next to its native `WINCCOA` source. | Closed: one layout for Postgres and MongoDB (C8), so an edge+main pair can share one witness group |
| D-R9 | Remove the Vert.x cluster mode later? | Out of scope; nothing here depends on it |

## III.2 Phases

| Phase | Scope | Depends on |
|---|---|---|
| **P** PeerLink | Part I, milestones J0-J6 | **Done** (`2795cde0`) |
| **R1** Roles core | `RoleManager` with `NONE`/`STATIC`, `CapRole` role block in `Hello`/`HelloOK`/`Ping`/`Pong` (Kotlin **and** Go edge, new golden vectors), reachability C2, table II.6.1, grace period, `$SYS` status, metrics and PeerLink status fields (C7); no dashboard work | P; D-R6 |
| **R2** Component setting | `Redundancy` on devices, archive groups and scripts: inbound gate, outbound gate (`BridgeOutbound` guard unchanged, II.11.3), write gate (II.12.2a), cold-standby lifecycle, config store migrations and YAML schema; no GraphQL or UI | R1 |
| **R3** Witness lease | `WitnessLease` for Postgres, lease + heartbeat rows, unit tests against a test DB | – (parallel to R1) |
| **R4** `WITNESS` source | Table II.6.2, `OnIsolation`, release on shutdown, split detection and resolution (II.8) via `epoch`/`flags`, witness status fields | R1, R3 |
| **R5** MongoDB witness | II.5.2 MongoDB variant | R3 |
| **R6** | Removed: `ELECTION` dropped | – |
| **R7** GraphQL | `brokerRedundancy`, `setRedundancyRole`, per-component field, SDL identical to the edge (C9), dashboard badge, component forms and runtime states; ships together with edge PG | D-R5: human commitment in both repositories |
| **C** nodeId cleanup | II.13 | independent |

## III.3 Tests

PeerLink tests: Part I, section 9 (implemented).

Redundancy, unit:

- Decision tables II.6.1 and II.6.2 as table-driven tests, including grace-period timing and the fencing exception.
- `CapRole` encode/decode of `Hello`, `HelloOK`, `Ping`, `Pong` with and without the role block against shared Go golden vectors; a peer without `CapRole` reads as `UNKNOWN`; reachability (C2) from frames in either direction; `roleSeq` reset on a new `Hello`.
- Validation list of C5, YAML schema accepts exactly the C5 key set; `Source: WINCCOA` and `-cluster` are startup errors (II.12.6); `Partner` WARN (II.9): `STATIC` with empty `Partner` and two or more PeerLink peers logs one WARN, with one peer or with `Partner` set none.
- Own role `UNKNOWN` (II.10): `STATIC` decides at once; `WITNESS` stays `UNKNOWN` until the first lease attempt completes; `isActive()` is false while `UNKNOWN`.
- PeerLink status JSON (C7): every `consumers[]` and `sources[]` entry has `role`, `roleSeq`, `leaseEpoch`, `witnessReachable`, `leaseHolder` with the peer's values; for a peer without `CapRole` they are `"UNKNOWN"`, 0, 0, `false`, `false`; the existing `epoch` stays the log epoch.
- Lease SQL and MongoDB: acquire, renew, expiry takeover, same-NodeId reacquisition increments epoch, conditional release cannot alter a newer holder, voluntary release bypasses delay, late lease response cannot activate, and priority/freshness tie-breaks. Test delayed replies, expiry during output activity, cold-stop races and failback while writes are in flight.

Integration, two brokers + PeerLink + a mocked role source (pytest process fixture from Part I):

- HOT_STANDBY MQTT bridge: a value published at the remote source appears exactly once on each broker.
- COLD_STANDBY: the standby has no connection to the source; after a role switch it connects and publishes within N seconds.
- Central-DB archive group with COLD_STANDBY: each message is written exactly once in steady state.
- SQLite archive group with ALWAYS: both node DBs contain all messages.
- Kill the active broker: the standby becomes ACTIVE within `PeerTimeoutMs` + connect time.
- Partition (non-witness source): both brokers go ACTIVE, and after healing exactly one returns to STANDBY.
- `ALWAYS` keeps today's behaviour: an MQTT client connector with `BridgeOutbound: false` does not forward replicas; an outbound connector without the guard today (e.g. a JDBC logger) forwards replicas as before.
- HOT_STANDBY device write path: on the STANDBY node an MQTT write to the device is rejected, nothing reaches the device, and the log shows one rate-limited line (not one per write under load); on the ACTIVE node the write reaches the device; with `ALWAYS` writes go through on both nodes.
- CapRole always: a `Source: NONE` node offers `CapRole` in `SERVER_HELLO` / `HELLO` and its peers see it as `ACTIVE`, not `UNKNOWN`.

Witness scenarios:

| # | Scenario | Expected |
|---|---|---|
| T1 | Start A (prio 10), then B (prio 20); also test simultaneous cold start | Sequential: A `ACTIVE` epoch 1, B `STANDBY`; simultaneous: one holder, first acquisition need not be preferred |
| T2 | Kill A | B `ACTIVE` after TTL + `TakeoverDelayMs`, epoch 2 |
| T3 | Stop A gracefully | B `ACTIVE` within one renew interval |
| T4 | A returns, `Failback: false` | A `STANDBY` |
| T5 | Block PeerLink only | roles unchanged, `split = SPLIT_LINK`, resync after unblock |
| T6 | Block witness only, link up | Holder stays ACTIVE until local validity expires, then fences immediately; both STANDBY until witness recovers |
| T7 | Isolate A completely, `OnIsolation: STANDBY` | A `STANDBY` after local validity, B `ACTIVE` after store expiry, never both |
| T8 | Same with `KEEP` | both `ACTIVE`, `SPLIT_DUAL_ACTIVE` reported; after reconnect the lower epoch steps down |
| T9 | Pause A (SIGSTOP) longer than TTL, let B acquire, resume A | A cannot start an external operation before checking expiry; no grace, even before the next renew/Pong |
| T10 | Clock skew on one node of ±5 s | no overlap (store clock only) |

Mixed pair: an edge broker and a main broker in one `WITNESS` group (same lease tables, Postgres and MongoDB), failover in both directions; the same pair with `STATIC`, role change seen by the other side within one `Ping` interval. GraphQL: `brokerRedundancy` matches `$SYS/broker/redundancy/role`; `setRedundancyRole` errors without `STATIC`; the introspected redundancy SDL equals the edge's. WinCC OA redundancy tests live in the edge plan.

## III.4 Documentation

- `doc/peerlink.md`:
  - add a "Redundancy roles" section (sources, decision tables, split behavior, `OnIsolation`);
  - replace the current loop-guard rules ("run device connectors on one broker only", "archive group on a shared DB on one broker only") with the `Redundancy` setting.
- `doc/archiving.md`: central DB vs per-node DB recommendations (II.11.2).
- `doc/configuration.md`, `broker/config-default.yaml`: `Redundancy` block.
- `doc/clustering.md`: clarify `nodeId` semantics after II.13.
- Edge spec: done as "planned" text (3.6.1 `CapRole`, 3.6.2 `CapInterest`, 3.5/3.12 `PING` rule, 12.1 status fields); drop the "planned" marks when built.

## III.5 Risks (Part II)

Part I risks R1-R13: Part I, section 12.

| ID | Risk | Mitigation |
|---|---|---|
| RR1 | `isActive()` on the per-message hot path adds overhead | Plain volatile read; no locking, no allocation |
| RR2 | Connectors bypass `publishFromDevice` / `shouldForwardOutbound` and keep duplicating | Grep-based migration; integration test per connector kind; code review rule |
| RR3 | Role flapping tears down cold-standby components | `StandbyGraceMs`; fencing only on epoch loss |
| RR4 | Witness on a broker host gives that node the tie break | WARN (C5); docs recommend an independent store |
| RR5 | Clock drift over one TTL larger than `SafetyMarginMs` → overlapping holders | Store clock for expiry; monotonic durations locally; T10 |
| RR6 | `OnIsolation: STANDBY` with both nodes isolated → nothing active, data loss | Explicit default (D-R2), documented; `KEEP` for loss-sensitive sites |
| RR7 | Wire drift between Kotlin and Go for `CapRole` | Golden vectors, as Part I R11; shared contract II.0 / edge 3.0 changed only together |
