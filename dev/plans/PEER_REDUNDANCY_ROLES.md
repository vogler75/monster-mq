# Redundancy Roles for PeerLink Broker Pairs

Status: **Spec / not implemented**
Related: [plan-peerlink.md](plan-peerlink.md), [../../doc/peerlink.md](../../doc/peerlink.md), [../../doc/clustering.md](../../doc/clustering.md), Edge spec `edge/dev/plans/spec-peerlink-redundancy.md`

## 1. Problem

Two (or more) MonsterMQ brokers are connected with PeerLink. PeerLink is **active-active**: both brokers accept clients and replicate every publish to each other. Behind the brokers there is often a **WinCC OA redundant system**, which is **active-passive**.

Components that talk to external systems run on both brokers, and nothing coordinates them:

| Component | What goes wrong today |
|---|---|
| Inbound bridges/devices (MQTT client, OPC UA, PLC4X, WinCC OA, Kafka, NATS, …) | Both brokers receive the same source data and publish it. PeerLink replicates each copy to the other side, so every value exists twice. |
| Outbound bridges/loggers | Only MQTT/NATS/Kafka/Redis have the `Receive.BridgeOutbound` guard. Telegram, Neo4j, the JDBC/Influx/TimeBase loggers, Sparkplug, Script and Flows forward replicated messages again. |
| Archive groups on a central HA database | `Receive.Archive` defaults to `true`, so both brokers write every message to the same DB. |
| Archive groups on a per-node DB (SQLite) | These work correctly and must keep working: each node wants the full data set. |

The Kotlin broker has no notion of WinCC OA redundancy (`_ReduManager`). Only the Go edge reads it, and only for status display (`edge/internal/winccoanative/redu.go`).

## 2. Design principles

1. **Placement and role are different things.**
   - `nodeId` answers *on which Hazelcast cluster node does this component run*. That doesn't change.
   - The new `Redundancy` setting answers *does this component act on this broker, given the broker's current role*.
   - PeerLink and cluster mode are mutually exclusive (startup aborts, `Monster.kt:1145`). In a peer setup every broker is a single node, so the two settings never interact. We do **not** treat a peer pair as a cluster.
2. **One role per broker, one gate in the code.** Connectors must not each implement their own redundancy logic. That's how the current `BridgeOutbound` guard ended up implemented in only 4 of ~15 connectors.
3. **Fail open.** If in doubt, a broker becomes ACTIVE. Duplicate data is acceptable; lost data is not.
4. **Default = today's behavior.** `Redundancy: ALWAYS` everywhere, and no role source configured means the broker is always ACTIVE.

## 3. Broker role

Each broker has exactly one role: `ACTIVE` or `STANDBY` (plus `UNKNOWN` as an internal transient state, see §3.3).

### 3.1 Role sources

New top-level config block:

```yaml
Redundancy:
  Source: WINCCOA          # NONE (default) | STATIC | WINCCOA | ELECTION
  Static:
    Role: ACTIVE           # for Source=STATIC; can be switched at runtime via GraphQL
  WinCCOa:
    Device: oa-local       # name of the WinCC OA device/connection whose redundancy state is used
  Election:                # phase 4
    Priority: 10           # lower wins
  StandbyGraceMs: 5000     # delay before ACTIVE -> STANDBY takes effect (see §3.4)
  PeerTimeoutMs: 10000     # peer considered unreachable after this (see §3.3)
```

| Source | Behavior |
|---|---|
| `NONE` | Always ACTIVE. Redundancy settings on components have no effect. This is the default. |
| `STATIC` | Role comes from config and can be switched through GraphQL/REST (`setRedundancyRole`). There's no automatic failover, but the peer fallback in §3.3 still applies. |
| `WINCCOA` | The broker watches the redundancy state of the WinCC OA node it is connected to: `_ReduManager.Status.Active` / `_ReduManager_2.Status.Active`, in the same way as the edge's `redu.go`. **WinCC OA decides which broker is ACTIVE.** Only two brokers can take part in this, because WinCC OA redundancy is a pair. |
| `ELECTION` | Phase 4, for 3+ brokers without WinCC OA. See §7. |

For `WINCCOA` the connection is the existing WinCC OA connector: GraphQL + `dpQueryConnectSingle` in `devices/winccoa/WinCCOaConnector.kt`. A small `ReduWatcher` subscribes to the `_ReduManager` datapoints through that connection and reports `myOaActive: true | false | unknown`. If the referenced device is not running on this broker, or its connection is down, the result is `unknown`.

### 3.2 Exchanging roles over PeerLink

Each broker must know whether its peer is reachable and which role it claims. This goes over the existing PeerLink connection:

- New capability bit `CapRole`. Both the Kotlin broker and the Go edge implement it, since the wire protocol is shared.
- When `CapRole` is negotiated:
  - `HelloOK` gets two new fields: `role` (u8) and `roleSeq` (u64, incremented on every role change).
  - `Pong` gets the same two fields, so each side learns about role changes within one keep-alive interval.
- If the peer doesn't support `CapRole`, its role is treated as `UNKNOWN`. Data replication keeps working.
- **Peer reachable** means the Puller session to that peer is established and the last `Pong` is younger than `PeerTimeoutMs`.

No extra port or topic is needed. The role is tied to the same connection that carries the data, so "the peer is alive" and "the peer is replicating to me" are the same signal.

### 3.3 Role decision

Evaluated on every input change (role source, peer state, peer role):

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

- **The broker on the active WinCC OA node crashes.** WinCC OA stays active on that node, and the other broker sees itself as passive. Its peer is unreachable, though, so it becomes ACTIVE. Without this rule nobody would publish.
- **Network partition between the brokers.** Both become ACTIVE. Inbound data is duplicated and a central DB may get double writes for the duration. That's an accepted trade-off, documented as split-brain behavior. Archive writes should be idempotent where the backend supports it (upsert on topic + time); that's optional and out of scope for phase 1.
- **Both brokers see ACTIVE from their source.** This is a WinCC OA split brain. Both stay ACTIVE (fail open), and a warning is logged and shown in the dashboard.

### 3.4 Flapping

- STANDBY → ACTIVE takes effect **immediately**.
- ACTIVE → STANDBY takes effect only after `StandbyGraceMs`, and only if the decision is still STANDBY at that point.

During a WinCC OA switchover, the states can briefly read *both active* or *both passive*. The grace period keeps cold-standby components from being torn down and rebuilt during such a brief flip.

### 3.5 RoleManager (Kotlin)

New `redundancy/RoleManager.kt`, created in `Monster.kt` after PeerLink:

```kotlin
object RedundancyRole {
    fun current(): Role                 // ACTIVE | STANDBY
    fun isActive(): Boolean             // hot path, a volatile read
    const val ROLE_CHANGED = "mq.redundancy.role"   // event bus address, payload {role, seq, reason}
}
```

- `isActive()` must be a plain volatile read, because it is called per message on the hot path.
- Role changes are logged at INFO with the reason (e.g. `"peer unreachable"`, `"WinCC OA switched"`).
- The role is exposed through GraphQL (`brokerRedundancy { role source myOaActive peers { nodeId reachable role } since reason }`), through `$SYS`/metrics, and in the dashboard on the PeerLink page.

## 4. Component setting: `Redundancy`

New field on `DeviceConfig` (all connector types) and on `ArchiveGroupConfig`:

```
Redundancy: ALWAYS | HOT_STANDBY | COLD_STANDBY      (default ALWAYS)
```

| Mode | On ACTIVE broker | On STANDBY broker | Failover | Source connections |
|---|---|---|---|---|
| `ALWAYS` | runs | runs (today's behavior) | – | one per broker |
| `HOT_STANDBY` | runs; inbound is published, outbound sends **all** messages incl. replicas | runs and stays connected; **inbound is discarded, nothing is sent outbound** | fast, almost no gap | one per broker |
| `COLD_STANDBY` | runs; same as HOT on the active side | **not started**, no connection | slower: connect, authenticate, subscribe, read initial values | one in total |

Discarding inbound data on the standby is safe. The active broker publishes the same data, and PeerLink replicates it to the standby, so the standby's retained store, last-value stores and subscribers stay current.

### 4.1 Semantics per component kind

| Kind | Examples | ALWAYS | HOT_STANDBY | COLD_STANDBY |
|---|---|---|---|---|
| Inbound connector | MQTT client (inbound), OPC UA client, PLC4X, WinCC OA, WinCC UA, I3X client, Kafka/NATS/Redis client (inbound) | ✓ | ✓ | ✓ |
| Outbound connector / logger | MQTT client (outbound), Kafka/NATS/Redis (outbound), Telegram, Neo4j, JDBC/Influx/TimeBase loggers | ✓ | ✓ | ✓ |
| Processing | Script, Flow engine, Sparkplug B decoder | ✓ | ✓ (outputs suppressed on standby) | ✓ |
| Archive group | any backend | ✓ (per-node DB, e.g. SQLite) | ✓ (DB connected, writes dropped) | ✓ (typical for central DB) |
| Server-type | OPC UA server, Kafka protocol server, MCP server, Agent | ✓ | – (not offered) | ✓ (standby does not listen) |

Bidirectional connectors such as the MQTT bridge apply the mode to both directions together. There is no separate inbound/outbound mode, to keep the config simple.

### 4.2 Recommended configurations

| Setup | Archive groups | Inbound bridges | `Receive.Archive` |
|---|---|---|---|
| Central HA database | `COLD_STANDBY` (or `HOT_STANDBY`) | `HOT_STANDBY` or `COLD_STANDBY` | `true` |
| DB per node (SQLite) | `ALWAYS` | `HOT_STANDBY` or `COLD_STANDBY` | `true` |

In both cases the active broker archives the messages it got from its peer (clients connected to the standby broker still publish there). `Receive.Archive=false` is therefore only needed for unusual setups, and the doc should say so.

### 4.3 Relation to `Receive.BridgeOutbound`

- `HOT_STANDBY` / `COLD_STANDBY`: the role decides. Outbound on the ACTIVE broker sends everything, including replicas. On the STANDBY broker nothing is sent. `BridgeOutbound` is ignored for these components.
- `ALWAYS`: unchanged; the `BridgeOutbound` guard applies. As part of this work, that guard moves into the central outbound gate (§5.2), so it finally applies to **all** outbound connectors, not just the four that have it today.

## 5. Implementation

### 5.1 Inbound gate (hot standby)

All device connectors publish through `sessionHandler.publishMessage(...)`. Add one entry point for device-originated data:

```kotlin
fun publishFromDevice(device: DeviceConfig, msg: BrokerMessage) {
    if (device.redundancy == HOT_STANDBY && !RedundancyRole.isActive()) { metrics.droppedStandby++; return }
    publishMessage(msg)
}
```

Migrate every connector's publish call to `publishFromDevice`. That's mechanical: grep for `publishMessage(` under `devices/`, `extensions/` and `logger/`. Archive groups check the same condition in `MessageHandler.saveMessage` before enqueuing to `archiveQueues[name]`.

### 5.2 Outbound gate

The internal subscription that feeds outbound connectors (`subscribeInternalClient` → `handleLocalMqttMessage` and equivalents) gets a shared filter:

```kotlin
fun shouldForwardOutbound(device: DeviceConfig, msg: BrokerMessage): Boolean = when (device.redundancy) {
    ALWAYS -> msg.peerSource == null || peerLinkBridgeOutbound   // today's guard, now everywhere
    HOT_STANDBY, COLD_STANDBY -> RedundancyRole.isActive()
}
```

Replace the four ad-hoc guards (`MqttClientConnector.kt:582`, `NatsClientConnector.kt:327`, `KafkaClientConnector.kt:329`, `RedisClientConnector.kt:559`), and add the call to the remaining outbound connectors.

### 5.3 Cold standby lifecycle

- Effective run state = `enabled && isAssignedToNode(nodeId) && (redundancy != COLD_STANDBY || RedundancyRole.isActive())`.
- Each extension subscribes to `ROLE_CHANGED`. For its `COLD_STANDBY` devices it reuses the existing deploy/undeploy path, the same one used for enable/disable toggles.
- The persisted `enabled` flag is **not** changed. The dashboard shows a distinct state, "Standby (cold)", so it is not confused with disabled.
- Archive groups: start/stop the group's writer and DB connection, but **not** the last-value store, which keeps being fed from replicas.

### 5.4 Config, GraphQL, UI

- `DeviceConfig.redundancy`, persisted in all config stores (SQLite, Postgres, Mongo, CrateDB). The schema migration adds a column/field with default `ALWAYS`.
- `ArchiveGroupConfig.redundancy`, same treatment.
- GraphQL inputs/outputs for every device type and archive group, plus the `brokerRedundancy` query and `setRedundancyRole` mutation.
- Dashboard:
  - a role badge in the header when `Source != NONE`
  - a Redundancy dropdown in every device/archive-group form, where server-type components offer only ALWAYS/COLD
  - the runtime state "Standby (cold)" / "Standby (hot, dropping)" in device lists

### 5.5 Validation

- If `Redundancy.Source` is set and PeerLink is not configured, log a warning: the role then only follows the source, without the peer fallback.
- `Source: WINCCOA` requires `WinCCOa.Device` to reference an existing WinCC OA device on this broker.
- `Source != NONE` together with cluster mode fails at startup, because PeerLink and cluster mode are already exclusive.

## 6. Cluster `nodeId` cleanup (separate track, independent)

These bugs were found while analyzing placement. They are not caused by redundancy, but they make "run once" unreliable in cluster mode today:

1. `nodeId = "local"` in a cluster: startup loading (`getEnabledDevicesByNode`, `WHERE node_id=? OR node_id='*'`) does not load it, but `isAssignedToNode` on update/toggle deploys it on **every** node. Define `"local"` as "this node only in non-cluster mode" and reject or migrate it in cluster mode.
2. Reassign doesn't deploy on the new node until restart. The extension handlers (`MqttClientExtension.kt:386-408`, OPC UA, WinCC OA, PLC4X) only act on devices already in `activeDevices`. Fix: on `reassign`, reload the device from the config store and deploy if assigned, as `KafkaServerExtension.kt:258` already does.
3. GraphQL create mutations default `nodeId` to `"*"` (`ScriptMutations.kt:54`, `McpServerMutations.kt:118`, `OpcUaServerMutations.kt:238`), which runs the component N times. Default to the current node instead.
4. `Oa4jBridge` ignores `nodeId`. `AgentExtension.kt:134` and `OpcUaServerExtension.kt:99/173` use custom checks that exclude `"local"`. Unify everything on `DeviceConfig.isAssignedToNode`.
5. `HealthHandler` leader re-election: the dead member is identified by `member.uuid`, but the leader value is `getClusterNodeId()`, which may be the `nodeName` attribute. If they differ, `remove(LEADER_KEY, deadId)` never matches and no new leader is elected. Verify and fix.

## 7. More than two brokers

- **Data:** PeerLink supports N brokers as a **full mesh**. Delivery is one hop, so chains and rings don't propagate.
- **With WinCC OA:** only the two brokers attached to the WinCC OA redundancy pair use `Source: WINCCOA`. Any additional brokers use `Source: NONE`, so they're always ACTIVE, and their components should be `ALWAYS`.
- **Without WinCC OA (phase 4, `Source: ELECTION`):**
  - Each broker has `Election.Priority`. Ties are broken by NodeId.
  - A broker is ACTIVE if it has the lowest priority among the brokers it can reach (itself included) **and** it reaches a strict majority of the configured brokers.
  - Without a majority it goes STANDBY. This deliberately deviates from fail-open for N≥3, because a majority makes split brain impossible.
  - For N=2, election has no quorum and degrades to §3.3: fail open, possible dual-active. Document that two brokers without WinCC OA need either `STATIC` or acceptance of dual-active.
  - Roles travel in the same `HelloOK`/`Pong` fields. A peer's role for election purposes is its *claimed* priority plus reachability; no extra rounds are needed.

## 8. Phases

| Phase | Scope |
|---|---|
| 1 | `RoleManager` with `NONE`/`STATIC`/`WINCCOA` sources, the `ReduWatcher`, the `CapRole` wire extension (Kotlin **and** Go edge), the decision table, the grace period, GraphQL `brokerRedundancy`, and the dashboard badge. |
| 2 | The `Redundancy` field on devices and archive groups: inbound gate, outbound gate (incl. migrating `BridgeOutbound` to all connectors), cold-standby lifecycle, config store migrations, UI. |
| 3 | `nodeId` cleanup (§6). Independent of phases 1 and 2. |
| 4 | `ELECTION` source for 3+ brokers without WinCC OA. |

## 9. Tests

- Unit: the decision table (§3.3) as a table-driven test, including grace-period timing.
- Integration, two brokers + PeerLink + a mocked role source:
  - HOT_STANDBY MQTT bridge: a value published at the remote source appears exactly once on each broker.
  - COLD_STANDBY: the standby has no connection to the source; after a role switch it connects and publishes within N seconds.
  - Central-DB archive group with COLD_STANDBY: each message is written exactly once in steady state.
  - SQLite archive group with ALWAYS: both node DBs contain all messages.
  - Kill the active broker: the standby becomes ACTIVE within `PeerTimeoutMs` + connect time.
  - Partition: both brokers go ACTIVE, and after healing exactly one returns to STANDBY.
  - Outbound connectors without `BridgeOutbound` today (e.g. a JDBC logger): no duplicate forwarding with `ALWAYS`.
- Redundant WinCC OA: extend `edge/test/integration/winccoa_native_redu_test.go` and add a Kotlin counterpart against a WinCC OA redu test system, switching with `_ReduManager` commands.

## 10. Documentation

- `doc/peerlink.md`:
  - add a "Redundancy roles" section
  - replace the current loop-guard rules ("run device connectors on one broker only", "archive group on a shared DB on one broker only") with the `Redundancy` setting
- `doc/archiving.md`: central DB vs per-node DB recommendations (§4.2).
- `doc/winccoa.md`: `Source: WINCCOA` setup.
- `doc/clustering.md`: clarify `nodeId` semantics after §6.
