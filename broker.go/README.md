# MonsterMQ Edge (broker.go)

A native, single-binary MQTT broker written in Go, designed for edge deployments
(Raspberry Pi 4/5, small Linux/macOS hosts) and aligned in protocol, storage and
GraphQL surface with the JVM-based MonsterMQ broker that lives in `../broker/`.
The same SQLite/PostgreSQL/MongoDB schemas are used so a single physical
database can be opened by either implementation.

This document describes **exactly what is implemented today** so it can also
serve as the spec for a re-implementation in another language. The companion
`GRAPHQL.md` lists every GraphQL type and resolver in detail.

---

## Project layout

```
broker.go/
├── cmd/monstermq-edge/main.go          # entry point, CLI flags, signals
├── internal/
│   ├── broker/                         # mochi-mqtt bootstrap, hooks, TLS, lifecycle
│   ├── archive/                        # archive group manager + retention
│   ├── auth/                           # user/ACL cache backed by UserStore
│   ├── bridge/mqttclient/              # MQTT-to-MQTT bridge (paho-based)
│   ├── config/                         # YAML config + loader
│   ├── graphql/                        # gqlgen schema, generated code, resolvers
│   ├── log/                            # log Bus (ring buffer + pub/sub) + slog Handler
│   ├── metrics/                        # in-memory rate counters → MetricsStore
│   ├── pubsub/                         # in-process pub/sub for GraphQL topicUpdates
│   ├── stores/                         # storage interfaces + 4 backends
│   │   ├── interfaces.go               # MessageStore, MessageArchive, SessionStore,
│   │   │                               # QueueStore, UserStore, ArchiveConfigStore,
│   │   │                               # DeviceConfigStore, MetricsStore
│   │   ├── memory/                     # in-memory MessageStore
│   │   ├── sqlite/                     # SQLite implementation of all 7 stores
│   │   ├── postgres/                   # PostgreSQL implementation
│   │   └── mongodb/                    # MongoDB implementation
│   └── version/
├── test/integration/                   # 18 integration tests
├── config.yaml.example                 # documented YAML config
├── run.sh                              # build + run wrapper
├── Makefile                            # build / cross-compile / test
├── Dockerfile                          # multi-arch (amd64 + arm64)
└── systemd/monstermq-edge.service      # systemd unit file
```

About **9,000 lines of production Go** plus generated GraphQL code.

---

## Implemented vs intentionally skipped

This broker is the **edge profile** of MonsterMQ. The table below maps every
feature of the JVM broker to its status in `broker.go`.

| Feature group                              | broker.go | Notes |
|--------------------------------------------|-----------|-------|
| MQTT 3.1 / 3.1.1 / 5.0 protocol            | ✅        | via [mochi-mqtt/server](https://github.com/mochi-mqtt/server) v2 |
| TCP / TLS / WebSocket / WSS listeners      | ✅        | TLS via X509 cert+key files |
| Retained messages (persisted, restart-safe)| ✅        | Restored from store into mochi on boot |
| QoS 0 / 1 / 2 + inflight                   | ✅        | mochi handles inflight + ack |
| Persistent sessions (clean=false)          | ✅        | Sessions + subscriptions persisted |
| Last Will & Testament (incl. v5 delay)     | ✅        | mochi-native |
| MQTT v5 properties (topic alias, expiry…)  | ✅        | mochi-native |
| Authentication (bcrypt user store)         | ✅        | Disabled / anonymous / DB-backed |
| ACL (per-user topic patterns + priority)   | ✅        | Cached in-memory, refreshed every 30s |
| Offline message queue (persistent)         | ✅        | Configurable; gates against mochi inflight |
| Archive groups (last-value + history)      | ✅        | All 3 DB backends + MEMORY for last-value |
| Archive retention (background purge)       | ✅        | Per-group `lastValRetention`/`archiveRetention` |
| MQTT → MQTT bridge (in & out)              | ✅        | Per-device deploy on this node only |
| GraphQL HTTP/WS API                        | ✅        | Schema-parity subset of the JVM broker |
| Dashboard hosting                          | ✅        | Serves `dashboard/dist` or a placeholder |
| Periodic broker metrics → DB               | ✅        | 1s default tick |
| Cross-restart system log query             | ✅        | In-memory ring buffer (default 1000 entries) |
| Live system log subscription (WS)          | ✅        | All slog records (ours + mochi) |
| **Clustering (Hazelcast, multi-node)**     | ❌        | Single-node only by design |
| **OPC UA client / server**                 | ❌        | Out of scope |
| **Kafka bridge (in or out)**               | ❌        | Out of scope |
| **NATS / Redis / WinCC OA / WinCC Unified / PLC4X / Neo4j bridges** | ❌ | Out of scope |
| **GenAI integration (Gemini/Claude/Ollama)** | ❌      | Out of scope |
| **Flow engine (JS automation)**            | ❌        | Out of scope |
| **MCP server**                             | ❌        | Out of scope |
| **Sparkplug B decoder**                    | ❌        | Out of scope |
| **JDBC / InfluxDB / TimeBase loggers**     | ❌        | Out of scope |
| **Prometheus exporter**                    | ❌        | Could be added trivially via metrics store |
| **Custom NATS protocol listener**          | ❌        | Out of scope |

When the dashboard queries `Broker.enabledFeatures`, this broker returns
`["MqttBroker", "MqttClient"]` so the dashboard hides pages it can't use.

---

## Storage backends

Three production backends, all sharing the same Go interfaces in
`internal/stores/interfaces.go`:

| Interface             | SQLite | Postgres | MongoDB | MEMORY |
|-----------------------|--------|----------|---------|--------|
| `MessageStore` (retained / per-group last-value) | ✅ | ✅ | ✅ | ✅ (last-value only) |
| `MessageArchive` (history)                       | ✅ | ✅ | ✅ | — |
| `SessionStore` (sessions + subscriptions)        | ✅ | ✅ | ✅ | — |
| `QueueStore` (persistent offline queue, V2 / PGMQ-style) | ✅ | ✅ | ✅ | — |
| `UserStore` (users + ACL)                        | ✅ | ✅ | ✅ | — |
| `ArchiveConfigStore` (archive group configs)     | ✅ | ✅ | ✅ | — |
| `DeviceConfigStore` (MQTT bridge configs)        | ✅ | ✅ | ✅ | — |
| `MetricsStore` (broker metrics history)          | ✅ | ✅ | ✅ | — |

Schema names are byte-compatible with the JVM broker (e.g. SQLite tables
`sessions`, `subscriptions`, `messagequeue`, `users`, `usersacl`,
`archiveconfigs`, `deviceconfigs`, `metrics`, plus per-archive-group
`<groupname>lastval` / `<groupname>archive`).

The default backend is configured by `DefaultStoreType`. Per-store overrides
exist (`SessionStoreType`, `RetainedStoreType`, `ConfigStoreType`) but are
currently informational — all stores live on the chosen backend.

### SQLite specifics

- Pure-Go driver `modernc.org/sqlite` — no CGO.
- WAL mode, busy_timeout=5000, synchronous=NORMAL, foreign_keys=ON.
- 8 max-open connections (SQLite WAL allows concurrent readers); writes are
  serialised through a `sync.Mutex` in the `*DB` wrapper.
- `MessageStore` schema preserves the JVM broker's 9-column literal-prefix
  layout (`topic_1` … `topic_9`, `topic_r` JSON) so the same `.db` file
  works in either implementation.

### PostgreSQL specifics

- `jackc/pgx/v5` connection pool.
- DDL uses `BIGSERIAL`, `BYTEA`, `JSONB`, `TIMESTAMPTZ` matching the JVM broker.
- `ON CONFLICT … DO UPDATE` upserts where the JVM uses the same.

### MongoDB specifics

- `go.mongodb.org/mongo-driver/v2`.
- Collection names match table names. Documents use `bson.M` with the same
  field names as the SQL columns. Indexes mirror the SQL ones.

---

## Archive groups

Archive groups are the unit of "I want this slice of MQTT to be archived". Each
group owns:

- A **last-value store** (`MessageStore`) — most recent value per matching topic.
- A **message archive** (`MessageArchive`) — full append-only history, optionally
  retention-purged.

```yaml
# Example archive group, persisted in archiveconfigs:
name: "Sensors"
enabled: true
topicFilter: ["sensor/#", "alarm/#"]
retainedOnly: false             # if true, only retained=1 publishes are archived
lastValType: SQLITE             # MEMORY | SQLITE | POSTGRES | MONGODB
archiveType: SQLITE             # SQLITE | POSTGRES | MONGODB | NONE
payloadFormat: DEFAULT          # DEFAULT | JSON
lastValRetention: "30d"         # "<n>d", "<n>h", "<n>m", "<n>s", or empty for forever
archiveRetention: "90d"
purgeInterval: "1h"
```

The archive `Manager` owns the live deployment. On boot it provisions a
"Default" group if none exists (using the broker's `DefaultStoreType`).
GraphQL mutations call `Manager.Reload(ctx)` which diffs config vs running
groups and starts/stops/restarts as needed.

Per-group store names match the JVM broker:
- `<groupname>Lastval` (SQL table: lowercase `<groupname>lastval`)
- `<groupname>Archive` (SQL table: lowercase `<groupname>archive`)

A group is reported as `deployed: true` in GraphQL when it has a running
`Group` instance in the manager. `connectionStatus[].messageArchive` and
`.lastValueStore` are reported per-store (null when the corresponding type is
`NONE`). If a group fails to deploy (e.g. configured for Postgres but no PG
DB available), `connectionStatus.error` carries the reason.

Every published message goes through the storage hook → `archive.Manager.Dispatch`
→ each matching group buffers it, then a 250 ms ticker (or a 100-message
threshold) flushes to both the last-value store and the archive.

Group names are validated with `^[A-Za-z][A-Za-z0-9_]{0,62}$` because they
flow into DDL via `fmt.Sprintf`.

---

## Authentication & ACL

- Disabled (anonymous): every connection is allowed; `currentUser` returns
  `Anonymous` admin.
- Anonymous-enabled with user management on: same as above for unauthenticated
  clients.
- User-managed: bcrypt password hashing (cost = `bcrypt.DefaultCost`, same as
  the JVM broker's jBCrypt default).

ACL rules are per-user, with a topic pattern (MQTT-style `+` and `#`), a
priority (higher first), and `canSubscribe` / `canPublish` flags. The cache is
loaded at startup, refreshed every 30 s and on every mutation. Admin users
bypass ACL checks.

`mochi-mqtt`'s `OnConnectAuthenticate` and `OnACLCheck` hooks delegate to the
cache.

`login()` always returns a successful result with `username: "anonymous"` and
`isAdmin: true` when user management is disabled or anonymous mode is allowed,
matching the JVM broker's response shape exactly.

---

## Persistent offline queue

When `QueuedMessagesEnabled = true` (default) every publish destined for an
offline persistent (`clean=false`, `connected=false`) subscriber is enqueued
to the `messagequeue` table.

- **Storage**: PGMQ-inspired single table, `vt` (visibility timeout) +
  `read_ct` columns.
- **On reconnect**: the `OnSessionEstablished` hook checks
  `cl.State.Inflight.Len()`. If non-zero, mochi already replayed from its
  in-memory inflight buffer (same-process reconnect) and the DB queue is
  purged for that client. If zero, this is a post-restart reconnect — mochi
  has no history, so the DB queue is drained and each row replayed via
  `cl.WritePacket(...)`.
- **Ack**: best-effort on successful socket write; the row is removed
  immediately. For mid-flight broker crashes between WritePacket and the
  client's PUBACK, the message is lost (mochi's in-memory inflight has the
  same window).

`QueuedMessagesEnabled = false` falls back to mochi's in-memory inflight only;
no DB rows are written and queued messages are lost on broker restart.

---

## MQTT bridge (mqttclient)

The bridge acts as an MQTT client of one or more remote brokers. Each device
config is stored in `deviceconfigs` (typed `MQTT_CLIENT`), with a structured
JSON config and a list of address mappings.

**Address mapping**:

```
mode:        "SUBSCRIBE" | "PUBLISH"
remoteTopic: pattern on the remote broker
localTopic:  pattern on the local broker
removePath:  bool — when true, the literal prefix of the source-side pattern
             (everything before the first + or #) is stripped from the matched
             topic before mapping to the destination side.
qos:         0 | 1 | 2
noLocal, retainHandling, retainAsPublished:  MQTT v5 subscribe options (SUBSCRIBE)
messageExpiryInterval, contentType, responseTopicPattern,
payloadFormatIndicator, userProperties:      MQTT v5 publish properties
```

The connector is paho-mqtt-golang based, supports `tcp://`, `ssl://`, `tls://`,
`ws://`, `wss://`, auto-reconnect, and per-address topic transformation. The
manager owns one connector per device; `Manager.Reload(ctx)` reconciles the
live set against the persisted state after every GraphQL mutation, so the
dashboard's create/addAddress/etc. take effect without a broker restart.

Outbound publishes flow via the in-process `pubsub.Bus` (a `BusAdapter`
translates `stores.BrokerMessage` into the bridge's `LocalMessage`).

**Not yet implemented** (present in the JVM broker, would need extension):
- Disk-backed buffer when remote is offline (currently only the paho client
  buffer)
- Per-connector metrics persisted to `MetricsStore` (the GraphQL field
  `MqttClient.metrics` returns a single empty snapshot)

---

## Metrics

A 1-second-tick `Collector` aggregates atomic counters from the storage hook:

- `messagesIn` — local PUBLISH from clients
- `messagesOut` — PUBLISH packets written to clients
- `mqttClientIn` / `mqttClientOut` — bridge counters (wired but not yet
  surfaced from the bridge)
- `nodeSessionCount` — counted from `SessionStore.IterateSessions`
- `subscriptionCount` — counted from `SessionStore.IterateSubscriptions`
- `queuedMessagesCount` — `QueueStore.CountAll`

Each tick the snapshot is JSON-marshalled and persisted as a row in the
`metrics` table (`metric_type = "broker"`, `identifier = nodeId`).
`Broker.metrics` returns the latest in-memory snapshot; `Broker.metricsHistory`
loads from the DB.

`SessionMetrics`, `MqttClientMetrics`, `ArchiveGroupMetrics` resolvers exist
but currently return empty / single-tick stubs — to surface real per-session
or per-bridge throughput would require additional counters.

---

## System logs

Every `slog` record produced by our code AND by mochi-mqtt (passed via
`mqtt.Options.Logger`) flows through a shared `*log.Bus`:

- A ring buffer of `RingBufferSize` (default 1000) entries — backs the
  `systemLogs` query.
- A live pub/sub fan-out — backs the `systemLogs` GraphQL subscription.

The slog handler wraps the existing stderr handler; logs are still printed.
GraphQL exposes optional filter args (`node`, `level: [String!]`, `logger`,
`thread: Long`, `sourceClass`, `sourceMethod`, `message`, plus time-window
arguments on the query).

---

## Configuration

`config.yaml` (full example in `config.yaml.example`):

```yaml
NodeId: edge-rpi-01

TCP:  { Enabled: true,  Port: 1883 }
TCPS: { Enabled: false, Port: 8883, KeyStorePath: "", KeyStorePassword: "" }
WS:   { Enabled: true,  Port: 1884 }
WSS:  { Enabled: false, Port: 8884 }
MaxMessageSize: 1048576

DefaultStoreType:  SQLITE          # SQLITE | POSTGRES | MONGODB
SessionStoreType:  SQLITE
RetainedStoreType: SQLITE
ConfigStoreType:   SQLITE

SQLite:   { Path: ./data/monstermq.db }
Postgres: { Url: "postgres://localhost:5432/monstermq", User: ..., Pass: ... }
MongoDB:  { Url:  "mongodb://localhost:27017", Database: monstermq }

UserManagement:
  Enabled: false
  PasswordAlgorithm: BCRYPT
  AnonymousEnabled: true
  AclCacheEnabled: true

Metrics:
  Enabled: true
  CollectionIntervalSeconds: 1
  RetentionHours: 168

Logging:
  Level: INFO
  MqttSyslogEnabled: false           # not yet implemented
  RingBufferSize: 1000

GraphQL:   { Enabled: true, Port: 8080 }
Dashboard: { Enabled: true, Path: "" }    # Path = optional dir to dashboard/dist

Bridges:
  Mqtt:
    Enabled: true

QueuedMessagesEnabled: true            # see "Persistent offline queue" above
```

CLI flags:

- `-config <path>` — YAML file (defaults to built-in defaults when omitted)
- `-log-level <DEBUG|INFO|WARN|ERROR>`
- `-version`

---

## Build / run / package

`./run.sh -h` for the wrapper script. Common combinations:

```bash
./run.sh -b              # build + run
./run.sh -b -n           # build only
./run.sh                 # run an existing binary
./run.sh -- -version     # pass any flag through to the binary
```

Cross-compile via Makefile:

```bash
make build-arm64    # bin/monstermq-edge-linux-arm64  (Pi 4/5)
make build-armv7    # bin/monstermq-edge-linux-armv7  (Pi 3, 32-bit)
make build-amd64    # bin/monstermq-edge-linux-amd64
```

Docker (multi-arch via buildx):

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t monstermq-edge:latest .
```

systemd unit at `systemd/monstermq-edge.service`.

Resulting binary: ~25 MB static, `CGO_ENABLED=0`. Idle RSS on Linux: ~25–50 MB.

---

## Test coverage

Unit tests for SQLite stores and 18 integration tests covering:

- MQTT pub/sub QoS 0/1/2, retained delivery
- Retained-survives-restart (broker restarts, SQLite reopens)
- bcrypt auth, wrong-password rejection
- Persistent session offline queue + replay across restart, no-duplicate-on-
  in-process-reconnect
- Archive group writes (verified via raw SQLite SELECT)
- MQTT-to-MQTT bridge (static at boot AND dynamic deploy via GraphQL)
- GraphQL: brokerConfig, sessions, archiveGroups CRUD, user management,
  publish + currentValue, browseTopics (4 cases), metrics persistence,
  login (3 modes)

Run with:

```bash
go test ./... -count=1
make test
```

---

## Re-implementing in another language

Use this README as the feature checklist and `GRAPHQL.md` as the API
contract. The DB schemas are the bridge between implementations:

- **SQLite/Postgres tables**: see `internal/stores/sqlite/*.go` and
  `internal/stores/postgres/postgres.go` `EnsureTable` methods. The DDL is
  intentionally shared with the JVM broker (`broker/src/main/kotlin/stores/dbs/`).
- **MongoDB collections**: see `internal/stores/mongodb/mongodb.go`
  `EnsureTable` methods.
- **Storage interfaces**: `internal/stores/interfaces.go` is a complete spec
  of what every backend must implement.
- **Hooks contract**: see `internal/broker/hook_*.go` — each hook's
  responsibility (auth/ACL, persistence, queueing) is small and isolated.
- **GraphQL surface**: full SDL in `internal/graphql/schema/schema.graphqls`,
  per-resolver behaviour in `GRAPHQL.md`.

Two opinionated choices to preserve for compatibility with the dashboard and
the JVM broker:

1. **Payload encoding** in `TopicValue/RetainedMessage/ArchivedMessage/TopicUpdate`:
   `payload: String!` plus `format: DataFormat!` discriminator — `JSON` if the
   bytes parse as JSON, otherwise the bytes as base64 with `format = BINARY`.
2. **Timestamps**: GraphQL outputs use Unix milliseconds (`Long`), not ISO
   strings, for `*.timestamp` fields. (Some metadata fields like `createdAt`
   on `MqttClient` and archive groups still use ISO strings — see GRAPHQL.md.)
