# Redis Protocol Server for Topic Access

## Goal

Add a Redis-compatible protocol endpoint to MonsterMQ so Redis clients can connect to the broker and read/write MQTT topics through Redis commands.

This is not intended to turn MonsterMQ into a full Redis replacement. The first version should expose broker topics, archive group last-value stores, and MQTT pub/sub through a practical Redis command subset.

Redis Streams are explicitly out of scope for this phase.

## Core Model

Redis logical database numbers map to MonsterMQ archive groups.

Each archive group may declare an optional Redis database number:

```yaml
ArchiveGroups:
  - Name: Default
    RedisDbNumber: 0
    TopicFilter: ["#"]
    LastValType: MEMORY
    ArchiveType: POSTGRES

  - Name: Production
    RedisDbNumber: 1
    TopicFilter: ["production/#"]
    LastValType: MEMORY
    ArchiveType: POSTGRES
```

Rules:

- `RedisDbNumber` is optional.
- If omitted, the archive group is not exposed through Redis, except that `Default` may default to `0` for convenience.
- Redis DB numbers must be non-negative integers.
- Redis DB numbers must be unique among enabled archive groups.
- `SELECT <n>` switches the client connection to the archive group mapped to `<n>`.
- `SELECT` for an unmapped DB returns a Redis-style error.

## Command Mapping

Commands operate on the selected archive group.

| Redis command | MonsterMQ behavior |
| --- | --- |
| `SELECT n` | Select archive group with `RedisDbNumber == n` |
| `GET key` | Read `lastValStore[key]` from selected archive group |
| `MGET key...` | Read multiple last values |
| `SET key value` | Publish MQTT message to topic `key` |
| `MSET key value...` | Publish multiple MQTT messages |
| `DEL key...` | Delete last-value/retained topic entries where supported |
| `EXISTS key...` | Check selected last-value store |
| `SCAN` | Discover topics from selected last-value store |
| `KEYS pattern` | Optional compatibility command, guarded/limited |
| `PUBLISH channel payload` | Publish MQTT message to topic `channel` |
| `SUBSCRIBE channel...` | Subscribe to MQTT topics |
| `PSUBSCRIBE pattern...` | Subscribe to MQTT topic filters, with Redis pattern mapping |

`SET`, `MSET`, and `PUBLISH` must publish through the normal broker path, not write directly to the store. This keeps MQTT subscribers, archive handling, metrics, retained/last-value updates, and device bridges consistent.

## Initial Command Set

Implement enough Redis protocol surface for normal Redis clients to connect:

- Connection: `PING`, `ECHO`, `QUIT`, `HELLO`, `AUTH`, `SELECT`
- Client compatibility: `CLIENT SETNAME`, `CLIENT SETINFO`, `COMMAND`, `COMMAND INFO`, `INFO`
- Values: `GET`, `MGET`, `SET`, `MSET`, `DEL`, `EXISTS`
- Discovery: `SCAN`, optionally `KEYS`
- Pub/sub: `PUBLISH`, `SUBSCRIBE`, `PSUBSCRIBE`, `UNSUBSCRIBE`, `PUNSUBSCRIBE`

Out of scope:

- Redis Streams: `XADD`, `XRANGE`, `XREAD`, consumer groups
- Transactions: `MULTI`, `EXEC`, `WATCH`
- Lua/functions: `EVAL`, `EVALSHA`, `FUNCTION`
- Replication and persistence commands: `REPLICAOF`, `PSYNC`, `BGSAVE`, `SAVE`
- Redis Cluster commands
- Modules and Redis Stack commands
- Lists, sets, sorted sets, hashes, geospatial, probabilistic structures

## Configuration

Add a new top-level server configuration block:

```yaml
RedisServer:
  Enabled: true
  Host: "0.0.0.0"
  Port: 6379
  RequireAuth: true
  DefaultDb: 0
  SetRetained: false
  PublishQos: 0
```

Recommended fields:

- `Enabled`: whether to start the Redis protocol server.
- `Host`: bind address.
- `Port`: bind port. Redis default is `6379`.
- `RequireAuth`: whether unauthenticated commands are rejected except handshake commands.
- `DefaultDb`: initial DB number for new connections.
- `SetRetained`: whether Redis `SET` publishes retained MQTT messages.
- `PublishQos`: MQTT QoS for Redis writes. Start with `0`.

Add a separate feature flag:

```yaml
Features:
  RedisServer: true
```

Do not reuse the existing `Redis` feature flag because that currently represents the Redis client bridge.

## Runtime Architecture

Add a new extension:

```text
broker/src/main/kotlin/extensions/RedisServer.kt
```

Responsibilities:

- Start a Vert.x TCP server.
- Parse RESP commands.
- Maintain per-connection state:
  - authenticated user
  - selected Redis DB number
  - selected archive group
  - protocol version
  - client name
  - subscription mode
- Dispatch commands to broker services.
- Encode Redis-compatible RESP responses.

The Redis server needs access to:

- `ArchiveHandler`, to resolve Redis DB numbers to deployed archive groups.
- `SessionHandler`, to publish MQTT messages through normal broker flow.
- Auth context/user store, to validate `AUTH` and ACLs.

## RESP Protocol Strategy

Implement RESP2 first. It is simple and enough for broad client compatibility.

`HELLO` behavior:

- `HELLO` without unsupported options returns server metadata.
- `HELLO 2` stays in RESP2.
- `HELLO 3` may initially return `-NOPROTO` until RESP3 is implemented.

Potential implementation options:

- Use Netty Redis codec classes if integration with Vert.x TCP handling is clean.
- Otherwise implement a small RESP2 parser/writer directly.

RESP2 parsing is small compared with the Redis command semantics. The command layer should remain independent of the parser so RESP3 can be added later.

## Auth and ACL

`AUTH username password` maps to MonsterMQ users.

ACL checks:

- `GET`, `MGET`, `EXISTS`, `SCAN`, `KEYS`: require subscribe/read permission for each topic.
- `SET`, `MSET`, `PUBLISH`: require publish permission for each topic.
- `SUBSCRIBE`, `PSUBSCRIBE`: require subscribe permission for each topic/filter.
- Discovery commands must not leak unauthorized topics.

If `RequireAuth` is enabled, only these commands are accepted before authentication:

- `AUTH`
- `HELLO ... AUTH ...`
- `PING`
- `QUIT`
- `CLIENT SETINFO`

## Topic and Pattern Rules

Redis keys map directly to MQTT topic names.

For publish commands:

- Reject keys containing MQTT wildcard characters `+` or `#`.
- Reject keys that do not match the selected archive group's `TopicFilter`.
- Publish through `SessionHandler.publishMessage`.

For subscriptions:

- Redis `SUBSCRIBE topic` maps to exact MQTT topic subscription.
- Redis `PSUBSCRIBE pattern` needs a defined conversion:
  - Prefer MQTT filters directly when the pattern contains `/`, `+`, or `#`.
  - Optionally support simple Redis glob conversion for `*` to MQTT `#` only when this can be done safely.
  - Document any unsupported pattern shapes and return a Redis error.

## Store Access

Read commands use the selected archive group's `lastValStore`.

Implementation details:

- `GET`: return a Redis bulk string containing the raw payload bytes.
- Missing value: return null bulk string.
- `MGET`: array of bulk strings/nulls.
- `SCAN`: use `IMessageStore.findMatchingTopics`.
- `KEYS`: use `findMatchingTopics`, but should have a configurable result limit.
- `DEL`: call `IMessageStore.delAll` for selected topics where available.

No archive history commands are planned in this phase.

## Config and API Changes

Files likely affected:

- `broker/src/main/kotlin/Features.kt`
- `broker/src/main/kotlin/Monster.kt`
- `broker/src/main/kotlin/handlers/ArchiveGroup.kt`
- `broker/src/main/kotlin/handlers/ArchiveHandler.kt`
- `broker/src/main/kotlin/stores/IArchiveConfigStore.kt`
- `broker/src/main/kotlin/stores/dbs/postgres/ArchiveConfigStorePostgres.kt`
- `broker/src/main/kotlin/stores/dbs/cratedb/ArchiveConfigStoreCrateDB.kt`
- `broker/src/main/kotlin/stores/dbs/mongodb/ArchiveConfigStoreMongoDB.kt`
- `broker/src/main/kotlin/stores/dbs/sqlite/ArchiveConfigStoreSQLite.kt`
- `broker/yaml-json-schema.json`
- GraphQL archive group schema/resolvers if Redis DB numbers are managed via dashboard/API
- Dashboard archive group UI if Redis DB numbers should be editable there

Persisted archive config stores need a schema migration-compatible approach:

- Add nullable `redis_db_number`.
- Existing rows default to null.
- Runtime validation detects duplicates and invalid values.

## Deployment Flow

Startup order:

1. Load config.
2. Initialize archive groups.
3. Build Redis DB mapping from deployed enabled archive groups.
4. Validate DB mapping.
5. Start Redis protocol server if `RedisServer.Enabled` and `Features.RedisServer` are true.

Dynamic archive group changes:

- When an archive group starts/stops or is updated, rebuild the Redis DB mapping.
- Active connections keep their selected DB number.
- If the selected DB disappears, subsequent commands return an error until `SELECT` chooses a valid DB.

## Testing

Unit tests:

- RESP2 parser/writer.
- Command dispatcher.
- Redis DB mapping validation.
- Topic filter enforcement.
- ACL enforcement.

Integration tests:

- `redis-cli PING`
- `redis-cli AUTH`
- `redis-cli SELECT 0`
- `redis-cli SET topic value`
- `redis-cli GET topic`
- Redis `SET` is received by an MQTT subscriber.
- MQTT publish is visible through Redis `GET`.
- Redis `PUBLISH` is received by MQTT subscribers.
- Redis `SUBSCRIBE` receives MQTT publishes.
- Unauthorized users cannot read, write, subscribe, or discover restricted topics.
- Duplicate `RedisDbNumber` config fails clearly.

Manual compatibility smoke tests:

```bash
redis-cli -h localhost -p 6379 PING
redis-cli -h localhost -p 6379 SELECT 0
redis-cli -h localhost -p 6379 SET test/topic hello
redis-cli -h localhost -p 6379 GET test/topic
```

## Phased Implementation

### Phase 1: Config and Mapping

- Add `RedisDbNumber` to archive group model.
- Add schema support and persisted config store support.
- Expose Redis DB number in GraphQL/dashboard if required.
- Add validation for duplicates and invalid numbers.

### Phase 2: RESP Server Skeleton

- Add `RedisServer` extension.
- Add top-level config and feature flag.
- Implement TCP server, connection state, RESP2 parser/writer.
- Implement `PING`, `ECHO`, `QUIT`, `AUTH`, `SELECT`, `CLIENT SETINFO`, `CLIENT SETNAME`, `HELLO`, `COMMAND`, `INFO`.

### Phase 3: Topic Read/Write

- Implement `GET`, `MGET`, `EXISTS`, `SET`, `MSET`, `DEL`.
- Ensure writes publish MQTT messages through normal broker flow.
- Enforce selected archive group topic filters and ACLs.

### Phase 4: Discovery and Pub/Sub

- Implement `SCAN` and guarded `KEYS`.
- Implement `PUBLISH`, `SUBSCRIBE`, `PSUBSCRIBE`, unsubscribe commands.
- Add integration tests with Redis and MQTT clients.

### Phase 5: Hardening

- Add connection limits and command payload size limits.
- Add metrics for Redis connections, commands, errors, and published messages.
- Add operational logging.
- Document supported command subset.

