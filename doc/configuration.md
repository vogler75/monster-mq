# Configuration Reference

MonsterMQ reads settings from a YAML file. This guide lists the keys that are currently parsed by the broker so you can keep your configuration lean and reliable.

## File Basics

- The file is passed via `-config path/to/config.yaml` (see `broker/src/main/kotlin/Monster.kt:181-219`).
- Unknown keys are ignored; keep only the options documented below.
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

All ports default to `0` (disabled) except `TCP`, which defaults to 1883 if omitted (`broker/src/main/kotlin/Monster.kt:447-472`). `NodeName` is only used when the broker runs with `-cluster`.

If `AllowRootWildcardSubscription` is set to `false`, any client requesting a subscription to the root wildcard topic filter `#` receives a SUBACK failure (0x80) for that specific topic while the connection stays open. Internal components (e.g. OPC UA extensions) are unaffected.

## MQTT TCP Server Configuration

```yaml
MqttTcpServer:
  MaxMessageSizeKb: 512          # Maximum MQTT message size (default: 512KB)
  NoDelay: true                  # TCP_NODELAY - disable Nagle's algorithm for immediate packet transmission
  ReceiveBufferSizeKb: 512       # Socket receive buffer size (default: 512KB, recommended: 512KB-2MB for high load)
  SendBufferSizeKb: 512          # Socket send buffer size (default: 512KB, recommended: 512KB-2MB for high load)
```

The `MqttTcpServer` section controls low-level TCP socket behavior for MQTT connections:

- **MaxMessageSizeKb**: Maximum size of any single MQTT message. Messages exceeding this limit are rejected. Default is 512 KB.
- **NoDelay**: When `true`, disables TCP's Nagle algorithm (`TCP_NODELAY`), ensuring packets are sent immediately without waiting for coalescing. This is essential for low-latency MQTT and prevents packet corruption under high message rates. Default is `true`.
- **ReceiveBufferSizeKb**: Kernel-level socket receive buffer size. Larger buffers handle burst traffic better. For low-load deployments, 64 KB is sufficient; for high-load scenarios (1000+ msg/sec), increase to 1-2 MB. Default is 512 KB.
- **SendBufferSizeKb**: Kernel-level socket send buffer size. Should match receive buffer size. Default is 512 KB.

### Tuning for Different Load Profiles

**Low-load deployments (< 100 msg/sec):**
```yaml
MqttTcpServer:
  MaxMessageSizeKb: 512
  NoDelay: true
  ReceiveBufferSizeKb: 64
  SendBufferSizeKb: 64
```

**High-load production (1000+ msg/sec):**
```yaml
MqttTcpServer:
  MaxMessageSizeKb: 512
  NoDelay: true
  ReceiveBufferSizeKb: 1024
  SendBufferSizeKb: 1024
```

**Extreme load (10,000+ msg/sec):**
```yaml
MqttTcpServer:
  MaxMessageSizeKb: 512
  NoDelay: true
  ReceiveBufferSizeKb: 2048
  SendBufferSizeKb: 2048
```

## Rate Limiting

```yaml
MaxPublishRate: 1000     # Max messages/second a client can publish (0 = unlimited)
MaxSubscribeRate: 1000   # Max messages/second a client can receive (0 = unlimited)
```

Rate limits protect the broker from client overload by monitoring message throughput per client. Both settings default to `0` (unlimited).

When enabled, the broker checks each MQTT client's message rate every second using atomic counters:
- **MaxPublishRate** tracks incoming messages from the client (PUBLISH packets)
- **MaxSubscribeRate** tracks outgoing messages to the client (delivered subscriptions)

If a client exceeds the configured threshold, the broker immediately disconnects them with a reason message. Rate limiting only applies to **real MQTT clients**—internal bridges, archives, and connectors bypass these checks (`broker/src/main/kotlin/MqttClient.kt:404-438`).

This feature uses a periodic check with zero per-message overhead, leveraging existing metrics infrastructure for efficient enforcement.

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

`Servers` is reused by the Kafka message bus and the Kafka archive store. No additional security or topic-management settings are read (`broker/src/main/kotlin/Monster.kt:640-688`, `broker/src/main/kotlin/handlers/ArchiveGroup.kt:298-315`).

## User Management

```yaml
UserManagement:
  Enabled: true
  StoreType: POSTGRES
  PasswordAlgorithm: bcrypt
  CacheRefreshInterval: 60
  DisconnectOnUnauthorized: true
```

When enabled, the broker provisions the default `Admin` account on first start and stores users in the configured backend (`broker/src/main/kotlin/auth/UserManager.kt:39-118`).

## MCP Server

```yaml
MCP:
  Enabled: true
  Port: 3000
```

The MCP server requires an archive group named `Default` with an archive backend that implements `IMessageArchiveExtended` (`broker/src/main/kotlin/Monster.kt:526-540`). Only the `Enabled` flag and `Port` value are used.

## GraphQL Server

```yaml
GraphQL:
  Enabled: true
  Port: 4000
  Path: /graphql
```

If `Enabled` is `false` the GraphQL server is not deployed. Otherwise it listens on the supplied port and path (`broker/src/main/kotlin/Monster.kt:563-611`). Default values are port `8080` and path `/graphql` when omitted (`broker/src/main/kotlin/extensions/graphql/GraphQLServer.kt:34-45`).

## Metrics Collection

```yaml
Metrics:
  Enabled: true
  CollectionInterval: 5   # seconds (optional)
```

When enabled the broker chooses a metrics store based on the available database configuration or an optional `MetricsStore.Type` override (`broker/src/main/kotlin/Monster.kt:541-561`, `broker/src/main/kotlin/stores/factories/MetricsStoreFactory.kt:23-62`).

## Sparkplug Metric Expansion

```yaml
SparkplugMetricExpansion:
  Enabled: true
```

Toggles the Sparkplug metric expansion helper (`broker/src/main/kotlin/Monster.kt:139-152`).

## Bulk Messaging

```yaml
BulkMessaging:
  Enabled: true              # Enable/disable bulk message batching for client delivery
  TimeoutMS: 100             # Flush batches after this many milliseconds (100ms recommended)
  BulkSize: 1000             # Flush when batch reaches this many messages (1000 recommended)
```

Bulk messaging batches messages destined for the same client into a single eventBus operation, reducing serialization and network overhead. This is particularly beneficial when many clients subscribe to the same topic.

**Performance Impact:**
- Reduces eventBus operations by up to 10x for high-fanout scenarios
- Minimal latency impact (100ms batching window)
- Negligible memory overhead (buffers cleaned up automatically)

**Tuning Guidelines:**
- **TimeoutMS**: Shorter values (50ms) reduce latency but increase eventBus operations; longer values (200ms) batch better but may increase perceived latency
- **BulkSize**: Smaller values (500) favor latency; larger values (5000) favor throughput

This feature is independent and can be enabled/disabled separately from BulkProcessing.

## Bulk Processing

```yaml
BulkProcessing:
  Enabled: true              # Enable/disable publish bulk processing and worker threads
  TimeoutMS: 50              # Flush inbound batch after this many milliseconds (50ms recommended)
  BulkSize: 10000            # Flush when batch reaches this many messages (10000 recommended)
  WorkerThreads: 4           # Number of parallel worker threads (2-8 recommended)
```

Bulk processing is a **critical optimization** for systems with wildcard subscriptions. It addresses the performance cliff that occurs when clients use topic filters like `sensor/+/temperature`.

**How it Works:**
1. **Phase 1 - Collection**: Messages from multiple publishers are collected into a batch (up to `BulkSize` or after `TimeoutMS`)
2. **Phase 2 - Grouping**: Each batch is grouped by topic name, minimizing subscription lookup operations
3. **Phase 3 - Parallel Processing**: Worker threads process grouped messages in parallel

**Performance Impact (compared to baseline without subscriptions):**

| Scenario | Throughput | Notes |
|----------|-----------|-------|
| No subscriptions | 250,000 msg/s | Baseline |
| With subscriptions (disabled) | 60,000 msg/s | 76% degradation |
| Phase 1 only (topic grouping) | 120,000-150,000 msg/s | 2-2.5x improvement |
| Phase 1 + Phase 2 (workers) | 180,000-250,000 msg/s | 3-4x total improvement |

**Tuning Guidelines:**

**Light Load (< 100k msg/s):**
```yaml
BulkProcessing:
  Enabled: true
  TimeoutMS: 100
  BulkSize: 5000
  WorkerThreads: 2
```

**Medium Load (100k-300k msg/s) - RECOMMENDED:**
```yaml
BulkProcessing:
  Enabled: true
  TimeoutMS: 50
  BulkSize: 10000
  WorkerThreads: 4
```

**Heavy Load (300k-500k msg/s):**
```yaml
BulkProcessing:
  Enabled: true
  TimeoutMS: 25
  BulkSize: 20000
  WorkerThreads: 8
```

**Configuration Rules:**
- `WorkerThreads` should be tuned to ~1 thread per 75k msg/s target throughput
- Recommended range: 2-8 threads (more threads = more parallelism but higher context switching overhead)
- `TimeoutMS` controls batching aggression: shorter = lower latency, longer = better batching
- `BulkSize` should reflect expected message burst size in your workload

**Memory Considerations:**
- Each batch buffer consumes approximately 20-40 KB per client/node (with default BulkSize)
- Worker thread stack overhead: ~1 MB per thread
- Total memory impact: negligible for typical deployments (< 50 MB for 4 workers + 1000 clients)

**Implementation Details:**
- Worker threads are dedicated background threads (not Vert.x managed) to avoid blocked-thread warnings
- Uses optimized lock-free operations for high-frequency message adds
- Graceful shutdown on application termination
- Integration with existing BulkMessaging system for final client delivery

This feature requires **wildcard subscriptions to provide significant benefit**. For systems with only exact-match topic subscriptions, the gain is minimal.

## Cluster Mode

Cluster mode is activated with the `-cluster` flag on the command line. The YAML file has no additional Hazelcast configuration knobs; see `doc/clustering.md` for details.

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

The parser validates that the required database sections exist for the selected store types (`broker/src/main/kotlin/handlers/ArchiveGroup.kt:1035-1123`).

## Summary

- Stick to the keys documented above—older options such as `PlaygroundEnabled`, `Authentication`, `MaxPoolSize`, or custom Hazelcast blocks are not consumed by the current implementation.
- Database performance tuning (pool sizes, replication, etc.) must be configured on the database side.
- Keep `Kafka`, `MCP`, and `GraphQL` sections minimal; each module reads only its `Enabled` flag plus the connection port/path.
