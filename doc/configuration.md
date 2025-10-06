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
MaxMessageSizeKb: 512
QueuedMessagesEnabled: true
AllowRootWildcardSubscription: true  # When false, SUBSCRIBE to '#' yields failure (0x80) per-topic
NodeName: node-a        # Optional, used in cluster mode
```

All ports default to `0` (disabled) except `TCP`, which defaults to 1883 if omitted (`broker/src/main/kotlin/Monster.kt:447-472`). `NodeName` is only used when the broker runs with `-cluster`.

If `AllowRootWildcardSubscription` is set to `false`, any client requesting a subscription to the root wildcard topic filter `#` receives a SUBACK failure (0x80) for that specific topic while the connection stays open. Internal components (e.g. OPC UA extensions) are unaffected.

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
