# Configuration Reference

MonsterMQ reads settings from a YAML file. This guide covers common settings parsed by the broker and links to detailed component references.

## File Basics

- The file is passed via `-config path/to/config.yaml` (see `broker/src/main/kotlin/Monster.kt`).
- Unknown keys may be ignored. Validate against the YAML schema and check the relevant component guide; this page covers common settings.
- Example starter configuration: `broker/example-config.yaml`.

## Core Server Settings

```yaml
TCP: 1883      # MQTT TCP listener (set 0 to disable)
TCPS: 8883     # MQTT TLS listener
WS: 9000       # MQTT WebSocket listener
WSS: 9001      # MQTT Secure WebSocket listener
QueuedMessagesEnabled: true
AllowRootWildcardSubscription: true  # When false, SUBSCRIBE to '#' yields failure (0x80) per-topic
NodeName: node-a        # Optional, used in cluster mode
```

All ports default to `0` (disabled) except `TCP`, which defaults to 1883 if omitted (`broker/src/main/kotlin/Monster.kt`). Cluster node IDs use `NodeName` when running with `-cluster`; standalone device assignment uses `local`. Zenoh also uses `NodeName` as the federation identity.

If `AllowRootWildcardSubscription` is set to `false`, any client requesting a subscription to the root wildcard topic filter `#` receives a SUBACK failure (0x80) for that specific topic while the connection stays open. Internal components (e.g. OPC UA extensions) are unaffected.

## MQTT TCP Server Configuration

```yaml
MqttTcpServer:
  MaxMessageSizeKb: 512          # Maximum MQTT message size (default: 512KB)
  NoDelay: true                  # TCP_NODELAY - disable Nagle's algorithm for immediate packet transmission
  ReceiveBufferSizeKb: 512       # Socket receive buffer size (default: 512KB)
  SendBufferSizeKb: 512          # Socket send buffer size (default: 512KB)
  Instances: 8                   # MQTT server verticle instances per listener (default: number of CPU cores)
  MaxInFlightMessages: 10000     # Outbound QoS 1/2 in-flight window per client (default: 10000)
  MaxQueuedMessages: 100000      # Pending QoS 1/2 messages per client when the in-flight window is full (default: 100000)
  ServerReceiveMaximum: 100      # Inbound QoS 1/2 limit advertised in CONNACK (default: 100)
  MaxQueuedQoS0Messages: 10000   # QoS 0 messages buffered per slow client (default: 10000, 0 = unbounded)
  QoS0OverflowPolicy: DROP_OLDEST  # DROP_OLDEST or DROP_NEWEST (default: DROP_OLDEST)
```

The `MqttTcpServer` section controls low-level TCP socket behavior for MQTT connections:

- **MaxMessageSizeKb**: Maximum size of any single MQTT message. Messages exceeding this limit are rejected. Default is 512 KB.
- **NoDelay**: When `true`, disables TCP's Nagle algorithm (`TCP_NODELAY`), ensuring packets are sent immediately without waiting for coalescing. This can reduce transmission latency; it does not change TCP stream integrity. Default is `true`.
- **ReceiveBufferSizeKb**: Kernel-level socket receive buffer size. Larger buffers can absorb bursts at the cost of memory. Default is 512 KB.
- **SendBufferSizeKb**: Kernel-level socket send buffer size. Default is 512 KB.
- **Instances**: Number of MQTT server verticle instances deployed per listener (TCP, WS, TCPS, WSS). Incoming connections are spread over this many event loops. Default is the number of CPU cores.
- **MaxInFlightMessages**: Maximum number of unacknowledged outbound QoS 1/2 messages per client. MQTT 5 clients use the Receive Maximum they send in CONNECT instead. Default is 10000.
- **MaxQueuedMessages**: Maximum number of QoS 1/2 messages queued per connected client while its in-flight window is full. When the limit is exceeded, further messages for that client are dropped and a rate-limited warning is logged. Default is 100000.
- **ServerReceiveMaximum**: Inbound QoS 1/2 limit advertised to MQTT 5 clients in CONNACK. Default is 100.
- **MaxQueuedQoS0Messages**: Maximum number of QoS 0 messages buffered per client while its socket write queue is full (a slow subscriber). When the buffer is full, a message is dropped according to `QoS0OverflowPolicy`, the drop is counted in the session metrics (`messagesDropped`) and a rate-limited warning is logged. `0` disables the limit (unbounded buffering). Default is 10000.
- **QoS0OverflowPolicy**: `DROP_OLDEST` drops the oldest buffered message to make room for the new one, `DROP_NEWEST` drops the new message. Default is `DROP_OLDEST`.

## Backpressure

```yaml
Backpressure:
  Enabled: true            # default: true
  MaxEventLoopLagMs: 500   # default: 500
  MaxPauseMs: 1000         # default: 1000
```

When the broker is overloaded, it stops reading from publishing MQTT clients, so TCP flow control slows the publishers down instead of letting backlogs and latency grow without limit. The broker counts as overloaded when one of its event loops runs a probe task more than `MaxEventLoopLagMs` late (its task queue has a backlog), or when a bulk processing worker queue is above its high watermark. Reading from a publisher resumes as soon as the broker has recovered, or after `MaxPauseMs` at the latest, so keep-alive and acknowledgements are not held back for long. With bulk processing enabled, batches are no longer dropped when a worker queue is full.

Tune socket buffers using measured throughput, latency, and operating-system limits.
There is no universal buffer size based only on messages per second; payload size,
client count, and network behavior matter.

## Rate Limiting

```yaml
MaxPublishRate: 1000     # Max messages/second a client can publish (0 = unlimited)
MaxSubscribeRate: 1000   # Max messages/second a client can receive (0 = unlimited)
```

Rate limits protect the broker from client overload by monitoring message throughput per client. Both settings default to `0` (unlimited).

When enabled, the broker checks each MQTT client's message rate every second using atomic counters:
- **MaxPublishRate** tracks incoming messages from the client (PUBLISH packets)
- **MaxSubscribeRate** tracks outgoing messages to the client (delivered subscriptions)

If a client exceeds the configured threshold, the broker immediately disconnects them with a reason message. Rate limiting only applies to **real MQTT clients**—internal bridges, archives, and connectors bypass these checks (`broker/src/main/kotlin/MqttClient.kt`).

The periodic check uses the client message counters; it is a disconnect threshold, not a traffic-shaping queue.

## MQTT 5 Limits

Configure the limits the implementation actually reads:

```yaml
MqttTcpServer:
  MaxMessageSizeKb: 512
  MaxInFlightMessages: 10000  # Outbound QoS 1/2 messages per client
  ServerReceiveMaximum: 100 # Inbound QoS 1/2 limit advertised in CONNACK
```

There is no parsed top-level `Mqtt` capability block. CONNACK advertises retained
messages and wildcard subscriptions as available, topic alias maximum 10, and
shared subscriptions and subscription identifiers as unavailable. Maximum QoS is
omitted, allowing QoS 2. The advertised maximum packet size is 268435455 bytes;
the listener's `MaxMessageSizeKb` still limits accepted messages. Keep-alive is
negotiated from the client connection. See [MQTT 5](mqtt5.md).

## Store Selection

MonsterMQ supports multiple backends for sessions, retained messages, archive groups, metrics, and user storage. Select store types with the following keys:

```yaml
DefaultStoreType: POSTGRES   # Optional global fallback
SessionStoreType: POSTGRES
RetainedStoreType: SQLITE
ConfigStoreType: POSTGRES
```

See `doc/databases.md` for backend-specific connection options (`Postgres`, `CrateDB`, `MongoDB`, `SQLite`).

## Kafka Integration

```yaml
Kafka:
  Servers: kafka1:9092,kafka2:9092
  Bus:
    Enabled: true
    Topic: monster-bus
```

`Kafka.Servers` configures the Kafka message bus. `Kafka.Config` accepts native Kafka client properties, including SASL/SSL settings. The Kafka client bridge has separate stored device configuration. Kafka is not a current archive-store type; see [Kafka integration](kafka.md).

## User Management

```yaml
UserManagement:
  Enabled: true
  StoreType: POSTGRES
  PasswordAlgorithm: bcrypt
  CacheRefreshInterval: 60
  DisconnectOnUnauthorized: true
  AclCheckOnSubscription: true
```

When enabled, the broker provisions the default `Admin` account on first start and stores users in the configured backend (`broker/src/main/kotlin/auth/UserManager.kt`).

### ACL Check Mode

`AclCheckOnSubscription` controls when ACL rules are evaluated:

- **`true` (default):** ACL is checked at subscribe time against the topic filter. Wildcard subscriptions (e.g. `#` or `sensor/+/temp`) are rejected unless an ACL rule explicitly covers the filter. This is the most restrictive mode with zero per-message overhead.
- **`false` (Mosquitto-compatible):** Wildcard subscriptions are accepted without ACL checks. Instead, ACL is checked at delivery time against each concrete message topic. Clients only receive messages on topics their ACL rules allow. Exact (non-wildcard) subscriptions are still checked at subscribe time for fast feedback.

Example: A user with ACL rule `dummy/#` (canSubscribe=true):

| AclCheckOnSubscription | Subscribe to `#` | Message on `dummy/temp` | Message on `other/topic` |
|------------------------|-------------------|-------------------------|--------------------------|
| `true` (default) | **Denied** | N/A | N/A |
| `false` | Accepted | **Delivered** | **Dropped** |

## MCP Server

```yaml
MCP:
  Enabled: true
  Port: 3000
```

The MCP server starts independently of archive groups. Historical-data tools require a suitable archive backend; retained-data tools use the retained store. See [MCP](mcp.md).

## GraphQL Server

```yaml
GraphQL:
  Enabled: true
  Port: 4000
  Path: /graphql
```

If `Enabled` is `false` the GraphQL server is not deployed. Otherwise it listens on the supplied port and path (`broker/src/main/kotlin/Monster.kt`). Default values are port `4000` and path `/graphql` when omitted (`broker/src/main/kotlin/graphql/GraphQLServer.kt`).

## Metrics Collection

```yaml
Metrics:
  Enabled: true
  CollectionIntervalSeconds: 10
  RetentionHours: 24
  MaxHistoryRows: 3600
  # StoreType: MEMORY  # Optional override
```

The default metrics store follows `StoreType`, then `DefaultStoreType`, then
`SQLITE`. An explicit persistent `Metrics.StoreType` must match that broker store
type. `MEMORY` and `NONE` overrides are also supported. `MetricsStore.Type` and
`Metrics.CollectionInterval` remain accepted legacy aliases.

## SparkplugB Decoders

`Features.SparkplugB` enables the decoder extension. Create and configure decoder
devices through the dashboard or GraphQL. The old `SparkplugMetricExpansion`
startup block is no longer read.

## Bulk Messaging

```yaml
BulkMessaging:
  Enabled: true              # Enable/disable bulk message batching for client delivery
  TimeoutMS: 100             # Flush batches after this many milliseconds (100ms recommended)
  BulkSize: 1000             # Flush when batch reaches this many messages (1000 recommended)
```

Bulk messaging batches messages destined for the same client into a single eventBus operation, reducing serialization and network overhead. This is particularly beneficial when many clients subscribe to the same topic.

Batching trades delivery latency for fewer event-bus operations. Measure throughput and latency with your workload.

**Tuning Guidelines:**
- **TimeoutMS**: Shorter values (50ms) reduce latency but increase eventBus operations; longer values (200ms) batch better but may increase perceived latency
- **BulkSize**: Smaller values (500) favor latency; larger values (5000) favor throughput

This feature is independent and can be enabled/disabled separately from BulkProcessing.

## Bulk Processing

```yaml
BulkProcessing:
  Enabled: false             # Enable/disable publish bulk processing and worker threads
  TimeoutMS: 50              # Flush inbound batch after this many milliseconds (50ms recommended)
  BulkSize: 1000             # Flush when batch reaches this many messages (1000 recommended)
  WorkerThreads: 4           # Number of parallel worker threads (2-8 recommended)
```

Bulk processing groups repeated subscription lookups for topics such as `sensor/+/temperature` and dispatches batches to worker threads.

**How it Works:**
1. **Phase 1 - Collection**: Messages from multiple publishers are collected into a batch (up to `BulkSize` or after `TimeoutMS`)
2. **Phase 2 - Grouping**: Each batch is grouped by topic name, minimizing subscription lookup operations
3. **Phase 3 - Parallel Processing**: Worker threads process grouped messages in parallel

Tune `WorkerThreads`, `TimeoutMS`, and `BulkSize` using measured queue depth,
CPU use, throughput, and delivery latency. Shorter timeouts reduce batching delay;
larger batches and more workers can increase memory use. There is no universal
messages-per-second capacity or worker-count formula.

The benefit depends on repeated topics, subscription patterns, payload sizes, and workload; compare with bulk processing disabled before adopting a setting.

## Cluster Mode

Cluster mode is activated with the `-cluster` flag on the command line. Use `HAZELCAST_CONFIG` for Hazelcast XML and `HAZELCAST_MEMBERS` for TCP/IP discovery; see [Clustering](clustering.md).

## Archive Groups

Archive groups can be seeded in the configuration file or managed through GraphQL. Each entry must at least include the group name, topic filter, and store types. Example:

```yaml
ArchiveGroups:
  - Name: Default
    TopicFilter: ["#"]
    RetainedOnly: false
    LastValType: POSTGRES
    ArchiveType: POSTGRES
```

The parser validates that the required database sections exist for the selected store types (`broker/src/main/kotlin/handlers/ArchiveGroup.kt`).

## Feature Flags

All named feature flags default to **true**; individual services can still have separate disabled-by-default `Enabled` settings. Use the top-level `Features` key to selectively disable individual extensions on a node.

```yaml
Features:
  OpcUa: false        # OPC UA client bridge (disabled)
  OpcUaServer: true   # OPC UA server
  MqttClient: true    # MQTT client bridge
  Kafka: true
  Nats: true
  Telegram: true
  WinCCOa: true
  WinCCUa: true
  Plc4x: true
  Neo4j: true
  JdbcLogger: true
  SparkplugB: true
  FlowEngine: true
  Agents: true
  I3xClient: true
```

Omitting a key is equivalent to `true`. To disable a feature, set it explicitly to `false`. GraphQL mutations for a disabled feature return an error response immediately; the extension's verticle is not deployed on that node at startup.

### Cluster behaviour

Device-to-node assignment is **explicit**: every device has a `nodeId` field stored in the config database. When you create a device via the GraphQL API you supply which node should own it; extensions load devices assigned to their node ID, or `*` where supported to deploy on every eligible node. There is no automatic hash-based distribution.

This means **heterogeneous cluster nodes are fully supported**. You can run one node with OPC UA enabled and another as a pure MQTT broker — as long as you assign OPC UA devices only to nodes where OPC UA is enabled. The create-device mutations enforce this: they check the *target* node's feature set (not the calling node's) and return an error if the feature is disabled there. Reassign mutations apply the same check.

**Note on high availability:** MQTT clients can reconnect to a surviving node when the deployment shares the required persistent session/queue and retained stores. Existing TCP connections are not migrated. It does **not** apply to device connectors. Each device connector (OPC UA, Kafka bridge, etc.) is pinned to its assigned node; if that node goes down, those devices go offline until the node recovers or an operator reassigns them via GraphQL.

Typical use cases:

- **Block a feature cluster-wide** — set `OpcUa: false` on every node when OPC UA is not part of your deployment, preventing accidental device creation through the API.
- **Dedicated cluster roles** — enable only `Kafka: true` on an ingestion node and `OpcUa: true` on a field node; devices can then be created and assigned to the appropriate node from any GraphQL endpoint in the cluster.
- **Standalone (non-clustered) brokers with different roles** — a standalone ingestion node and a standalone MQTT-only node can each carry a different `Features` set safely.

A `WARNING` is logged at startup if the Hazelcast IMap shows that other cluster members have a different feature set. The mismatch is also highlighted in the dashboard cluster overview table.

## PeerLink Inter-Broker Replication

PeerLink replicates MQTT messages and retained states across independent MonsterMQ instances (both MonsterMQ Core and MonsterMQ Edge brokers) over point-to-point connections with low overhead, split-horizon loop protection, and end-to-end TLS 1.3 / mTLS / shared-secret mutual authentication.

```yaml
NodeId: central-broker
PeerLink:
  Enabled: true
  Listener:
    Address: 0.0.0.0
    Port: 1890
  Tls:
    Enabled: true
    AutoGenerate: true
  SharedSecrets: ["<32-byte-base64-secret>"]
  Peers:
    - NodeId: edge-a
      Address: "edge-a.local:1890"
      Serve: true
```

For complete details on configuration, security policies, TLS setup, retained resync, HTTP status endpoints, and Zenoh federation coexistence, see [PeerLink Guide](peerlink.md).

## Additional Settings and Reference

The [YAML schema](../broker/yaml-json-schema.json) describes additional settings,
including `SSL`, `Queues`, `Logging`, `Prometheus`, `I3x`, `RedisServer`, `Zenoh`,
`Dashboard`, and `HMI`. Use the [starter configuration](../broker/example-config.yaml)
for a working baseline. Runtime parsing in `Monster.kt` is authoritative when
older configurations or schema descriptions differ.

The complete feature-name list is maintained in
[Features.kt](../broker/src/main/kotlin/Features.kt). Service switches such as
`MCP.Enabled` and `RedisServer.Enabled` are separate from feature flags; enabling
a feature alone does not necessarily start a listener.
