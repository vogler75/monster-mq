# broker.go — design + status + follow-ups

A native-Go single-binary MQTT broker that mirrors the JVM MonsterMQ broker
(`../broker/`) on protocol, storage and GraphQL surface, slimmed down for
edge deployments (Raspberry Pi 4/5, small Linux/macOS hosts).

This document is the canonical plan. It folds in the original design
brief and the post-build follow-ups list. The mochi-mqtt vendoring effort
lives separately in [`PLAN-inline-mochi.md`](PLAN-inline-mochi.md).

---

## 1. Goal

Ship a **second, slimmer broker** in `broker.go/` that:

- Is a single statically-linked native binary (`CGO_ENABLED=0`, ~25 MB).
- Keeps **schema parity** with the JVM broker's GraphQL API for the core
  MQTT/storage surface — so the existing dashboard works unchanged.
  Pages for absent features (OPC UA, Kafka, etc.) just hide via
  `Broker.enabledFeatures`.
- Supports the same three production storage backends — **SQLite**,
  **PostgreSQL**, **MongoDB** — with **byte-compatible schemas**, so the
  same physical database can be opened by either implementation.
- Implements the same **archive group / last-value / message archive /
  metrics / users+ACL / device config** persistence model.
- Ships only the **MQTT bridge** (no OPC UA, Kafka, WinCC, PLC4X, NATS,
  Redis, Neo4j, GenAI, flows, MCP, Sparkplug, JDBC/InfluxDB/TimeBase
  loggers).
- **No clustering** — single node only.

Use case: a field-deploy MQTT node that talks to the central MonsterMQ
via MQTT bridge and exposes the same API surface for local management.

---

## 2. Status (high-level)

The original 13-milestone build plan is **fully delivered** and committed.
The repo contains:

- ~9k LOC of production Go (excluding generated GraphQL code).
- 18 integration tests + 5 SQLite unit tests, all green against SQLite.
- All 8 storage interfaces implemented for SQLite, PostgreSQL and
  MongoDB — byte-compatible schemas with the JVM broker.
- Full GraphQL surface for MQTT broker management; dashboard works.

For details on what's implemented today and what's not, see:

- [`broker.go/README.md`](../../broker.go/README.md) — feature-by-feature
  inventory + storage / archive / bridge / metrics / log subsystems
- [`broker.go/GRAPHQL.md`](../../broker.go/GRAPHQL.md) — authoritative
  per-resolver API spec

The original milestones (M1–M13) are kept as a build-history appendix at
the end of this document.

---

## 3. Language decision (recap)

**Go**, not Rust. Two factors decided it:

1. `github.com/mochi-mqtt/server/v2` exposes a first-class hook system
   (`OnConnectAuthenticate`, `OnPublished`, `OnSelectSubscribers`,
   `OnSessionEstablished`, `OnRetainMessage`, `OnQosPublish`, …) that
   maps almost 1:1 onto the JVM broker's `IMessageStore` /
   `ISessionStoreAsync` / `IRetainedStore` / `IQueueStoreAsync`
   interfaces. Rust's `rumqttd` only exposes an auth handler and a
   "local link" observer — custom retained/session/queue stores would
   need a fork.
2. `github.com/99designs/gqlgen` is schema-first with `.graphqls` glob
   support and codegen of strongly-typed resolver stubs — the perfect
   fit for keeping schema parity with the dashboard's existing queries.
   `async-graphql` is code-first; we'd reverse-map the schema by hand.

Other deciding factors: `modernc.org/sqlite` is pure-Go (CGO-free static
binary), ARM64/ARMv7 cross-compile is `GOOS=linux GOARCH=arm64 go build`,
and the dev velocity is materially higher. On Pi Zero / sub-512 MB the
RSS difference (~25–50 MB Go vs ~5–15 MB Rust) would matter; on Pi 4/5
it doesn't.

---

## 4. Architecture (as built)

```
broker.go/
├── cmd/monstermq-edge/main.go      # entry point, CLI flags, signal handling
├── internal/
│   ├── broker/                     # mochi-mqtt bootstrap, hooks, TLS, lifecycle
│   ├── archive/                    # archive group manager + retention
│   ├── auth/                       # user/ACL cache backed by UserStore
│   ├── bridge/mqttclient/          # MQTT-to-MQTT bridge (paho)
│   ├── config/                     # YAML config + loader
│   ├── graphql/                    # gqlgen schema, generated, resolvers
│   ├── log/                        # log Bus + slog Handler
│   ├── metrics/                    # counters → MetricsStore
│   ├── pubsub/                     # in-process bus for topicUpdates
│   ├── stores/                     # 8 interfaces + 4 backends
│   │   ├── memory/                 # in-memory MessageStore
│   │   ├── sqlite/                 # SQLite, all 7 stores
│   │   ├── postgres/               # PostgreSQL, all 7 stores
│   │   └── mongodb/                # MongoDB, all 7 stores
│   └── version/
├── test/integration/               # 18 tests
├── config.yaml.example
├── run.sh / Makefile / Dockerfile / systemd/
├── README.md / GRAPHQL.md
└── go.mod
```

**Library picks (locked)**:

| Concern | Library |
|---|---|
| MQTT broker | `github.com/mochi-mqtt/server/v2` (vendoring planned — see PLAN-inline-mochi.md) |
| MQTT bridge client | `github.com/eclipse/paho.mqtt.golang` |
| GraphQL | `github.com/99designs/gqlgen` |
| HTTP router | `github.com/go-chi/chi/v5` |
| YAML | `gopkg.in/yaml.v3` |
| SQLite | `modernc.org/sqlite` (pure-Go, no CGO) |
| PostgreSQL | `github.com/jackc/pgx/v5` |
| MongoDB | `go.mongodb.org/mongo-driver/v2` |
| bcrypt | `golang.org/x/crypto/bcrypt` |
| Logging | stdlib `log/slog` |

**Storage interfaces** (`internal/stores/interfaces.go`):
`MessageStore`, `MessageArchive`, `SessionStore`, `QueueStore`,
`UserStore`, `ArchiveConfigStore`, `DeviceConfigStore`, `MetricsStore`.
Each backend implements them with table/collection names matching the
JVM broker so the same DB file is portable.

---

## 5. Outstanding follow-ups

Twelve items, prioritised. Each lists current state, JVM behaviour, the
proposed change, files to touch, and rough size.

Priority key:
- **P0** — correctness or data-loss risk under stress
- **P1** — feature parity that the dashboard or operators rely on
- **P2** — nice-to-have / robustness
- **P3** — explicitly out of scope for the edge profile (parked)

Suggested execution order at the bottom.

### 5.1 — Archive group buffer: bound the queue & batch size (P0) — **DONE**

**As built**: `internal/archive/group.go` rewritten around a bounded
`chan BrokerMessage` of size `Archive.BufferSize`. `Submit` is
non-blocking — overflow increments `dropped atomic.Uint64` (exposed
via `Group.Dropped()` and logged at warn on first drop and every
10000th). The flush loop batches up to `Archive.MaxBatchSize` per
cycle and flushes on either size or `Archive.FlushIntervalMs`
ticker. `Stop()` drains remaining buffered messages then exits, so
no in-flight data is lost on graceful shutdown. Defaults: 100_000 /
1000 / 250 ms, matching the JVM broker. Per-group overrides on
`ArchiveGroupConfig` deferred — broker-wide is sufficient for the
P0 fix; a follow-up can add columns to the three config-store
schemas + GraphQL when a user needs per-group tuning. Tests:
`group_test.go` (5 cases) — fast-path drains everything,
size-trigger fires the right number of batches, **slow store keeps
memory bounded and `dropped > 0`**, RetainedOnly gate, and default
fallback for zero-valued ctor args.

---

### 5.2 — Honour `purgeInterval` per group (P1) — **DONE**

**As built**: Retention scheduling moved from a single Manager-wide
15-minute ticker into `archive.Group`. Each group spawns its own
goroutine that ticks every `cfg.PurgeInterval` (parsed via
`parseDuration`, default 1h) and purges both stores when their
retention is set. `Group.Start` runs one synchronous initial purge
so stale rows are cleaned up at boot. `Manager.RunRetention` /
`purgeOnce` removed; `retention.go` shrunk to just
`parseDuration`. Reload already restarts groups on config change so
new intervals take effect automatically. Tests:
`group_test.go` adds three cases — initial purge fires on Start,
ticker keeps firing, and groups with no retention skip purging
entirely.

---

### 5.3 — Implement `GetAggregatedHistory` (P1)

**Current**: GraphQL `aggregatedMessages` returns the stub
`{columns:["timestamp"], rows:[]}`. The dashboard's archive-explorer
trend chart is non-functional.

**JVM**: `IMessageArchive.getAggregatedHistory(topics, startTime,
endTime, intervalMinutes, functions, fields)` returns
`{columns: [...], rows: [...]}`:
- `columns[0]` is `"timestamp"`, then one column per
  `topic[.field]_func` combo.
- Each row: `[bucketTimestampISO, val_topic1_avg, val_topic1_max, …]`.
- Functions: AVG, MIN, MAX, COUNT.
- If `fields` is empty, payload is treated as a numeric scalar
  (parses payload_blob as UTF-8 text → REAL).
- If `fields` provided, JSON-extract `$.field` from `payload_json` →
  REAL.

**Plan**:
1. Extend `stores.MessageArchive`:
   ```go
   GetAggregatedHistory(ctx, topics []string, from, to time.Time,
       intervalMinutes int, functions, fields []string) (AggregatedResult, error)
   ```
2. **SQLite impl** — port the JVM's `strftime` bucketing + `CASE WHEN
   topic = ? THEN value END` aggregation per topic. Mirror the bucket
   expressions: 1 / 5 / 15 / 60 / 1440 minute fast paths, generic
   fallback for arbitrary intervals.
3. **Postgres impl** — `date_trunc(...)` + `FILTER (WHERE topic = ...)`.
4. **MongoDB impl** — aggregation pipeline with `$bucket` /
   `$dateTrunc` + `$group`.
5. Resolver: parse start/end (RFC3339), look up the named archive group,
   delegate to its `Archive`. Convert result to GraphQL
   `AggregatedResult`.
6. Integration test: publish 100 numeric values at known intervals,
   query bucketed AVG, assert the buckets match.

**Files**: `internal/stores/interfaces.go`,
`internal/stores/sqlite/message_archive.go`,
`internal/stores/postgres/postgres.go` (MessageArchive methods),
`internal/stores/mongodb/mongodb.go`,
`internal/graphql/resolvers/resolver.go` (AggregatedMessages),
`test/integration/archive_aggregate_test.go`.

**LOC**: ~400 prod + ~80 test.

---

### 5.4 — Per-session and per-bridge metrics (P1)

**Current**: Only `BrokerMetrics` is collected and persisted.
`Session.metrics`, `MqttClient.metrics`, `ArchiveGroupMetrics` resolvers
return single empty stubs.

**JVM**: All four metric kinds tick at the same 1 s cadence and persist
as separate rows in the `metrics` table (`metric_type` discriminator).

**Plan**:
1. Per-session counters in `metrics.Collector` keyed by clientID;
   storage hook bumps on `OnPublished` (in/out); tick → `MetricsStore.
   StoreMetrics(MetricSession, clientID, ...)`; resolver replaces stub
   with `MetricsStore.GetLatest(MetricSession, clientID)`.
2. Per-bridge counters: `Connector.IncIn` / `IncOut` already exist —
   wire them to the collector; tick fanout per connector name →
   `StoreMetrics(MetricMqttClient, name, ...)`.
3. Per-archive-group counters: `Group` increments `messagesOut` on each
   successful flush (already partly tracked); also track average
   `bufferSize` over the tick; tick →
   `StoreMetrics(MetricArchiveGroup, name, ...)`.

**Files**: `internal/metrics/collector.go`,
`internal/broker/hook_storage.go`,
`internal/bridge/mqttclient/{connector,manager}.go`,
`internal/archive/group.go`,
`internal/graphql/resolvers/resolver.go`,
`internal/stores/types.go` (new `MetricKind` constants if needed).

**LOC**: ~250 prod + ~50 test.

---

### 5.5 — MQTT bridge offline disk buffer (P2) — **DONE** (b9713e3)

**As built**: Buffer interface in
`internal/bridge/mqttclient/buffer.go` with two implementations —
`memoryBuffer` (mutex-guarded slice) and `sqliteBuffer` (per-bridge
file at `./data/bridge_<name>.db`, FIFO via `id` autoincrement,
WAL+busy_timeout(5000)). Capacity policy is reject-new
(`ErrBufferFull`) or evict-oldest, picked via
`DeleteOldestMessages`. Connector pushes to the buffer when
`!client.IsConnectionOpen()` (strict) or when a connected publish
fails; drain runs from `OnConnect` and stops on first publish error,
leaving the rest queued for the next reconnect. Tests:
`buffer_test.go` exercises a shared contract (FIFO, reject overflow,
evict-oldest overflow, drain-stops-on-error) against both
implementations plus a process-restart test for the SQLite
variant.

---

### 5.6 — Bearer-token authentication on GraphQL (P1 if user mgmt is on)

**Current**: With user management ON, login validates credentials and
returns an opaque session token. **The token is never checked on
subsequent requests.** All mutations are accessible to anyone who can
reach the GraphQL endpoint.

**JVM**: Token-protected via per-request `Authorization` header check;
admin-only mutations (user CRUD, ACL CRUD) check `isAdmin`.

**Plan**:
1. Server-side session map keyed by token:
   `{username, isAdmin, expiresAt}`.
2. `login()` puts a freshly issued token in the map.
3. Chi middleware reads `Authorization: Bearer <tok>`, looks up,
   attaches `currentUser` to request context.
4. `currentUser` query reads from context.
5. Mutations that should be admin-only (`user.*`, `archiveGroup.*`,
   `mqttClient.*`, `session.*`) check the context user; reject with
   GraphQL error if not admin.
6. Anonymous mode: skip the middleware entirely (current behaviour).
7. Token expiry: 24 h sliding by default, configurable.

**Files**: `internal/graphql/server.go`,
`internal/graphql/resolvers/resolver.go`,
`internal/auth/sessions.go` (new),
`test/integration/auth_token_test.go`.

**LOC**: ~200 prod + ~80 test.

---

### 5.7 — Postgres + MongoDB integration tests (P2)

**Current**: All 18 integration tests use SQLite. Postgres and MongoDB
backends compile and have compile-time interface assertions, but no
end-to-end test exercises them. A bson type quirk or pgx column-mapping
bug would only show up in production.

**Plan**:
1. Add `github.com/testcontainers/testcontainers-go` test-only
   dependency.
2. Run the existing `TestRetainedSurvivesRestart`,
   `TestQueuedMessagesPersistAcrossRestart`, `TestArchiveGroupWrites`,
   `TestGraphQLArchiveGroupCRUD` against Postgres and MongoDB
   containers via build-tagged variants.
3. CI matrix `STORE_BACKEND={SQLITE,POSTGRES,MONGODB}`; tests skip with
   `t.Skip` if Docker isn't available locally.

**Files**: `test/integration/multistore_test.go`, `Makefile`,
`.github/workflows/ci.yml` (when CI is added).

**LOC**: ~150 test.

---

### 5.8 — Track `createdAt` / `updatedAt` on archive groups (P2) — **DONE**

**As built**: `stores.ArchiveGroupConfig` gains two `time.Time`
fields. SELECTs across all three backends now read the columns and
the resolver formats them as RFC3339Nano (nil when zero, so old
rows that predate timestamp tracking still render gracefully). DB
DDLs already had `created_at`/`updated_at` columns and the SQL
upserts already advanced `updated_at` on conflict — nothing to
change there. SQLite scans use a tolerant parser that accepts both
RFC3339 and SQLite's default `2006-01-02 15:04:05` shape.

---

### 5.9 — Better slog-record extraction for SystemLogEntry (P2)

**Current**: `SystemLogEntry.thread` is always 0; `parameters` and
`exception` are never populated.

**JVM**: `thread` is the JVM thread ID; `parameters` is the message
format args; `exception` is filled when a Throwable is logged.

**Plan**:
1. **Thread**: capture goroutine ID via `runtime.Stack()` parsing in
   the slog `Handler.Handle` (imperfect but cheap; or skip — Go
   semantics diverge enough that this might be best left at 0).
2. **Parameters**: collect attrs that aren't well-known (`logger`,
   `source_class`, `source_method`, `thread`) into the `parameters`
   slice as `"<key>=<value>"`.
3. **Exception**: if any attr value is an `error`, populate `Exception`
   with `Class=fmt.Sprintf("%T", err)`, `Message=err.Error()`,
   `StackTrace=""`.

**Files**: `internal/log/bus.go`.

**LOC**: ~40.

---

### 5.10 — Wire `MqttSyslogEnabled` to publish system logs as MQTT messages (P3)

**Current**: `Logging.MqttSyslogEnabled` is documented in config but
not implemented.

**JVM**: Republishes every log entry as MQTT message under
`$SYS/syslogs/<node>/<level>` so subscribers can consume the broker's
logs over MQTT.

**Plan**:
1. When enabled, attach a slog handler that publishes to
   `$SYS/syslogs/<node>/<level>` via `mqtt.Server.Publish(...)` with a
   JSON-encoded payload mirroring `SystemLogEntry`.
2. Avoid recursion: skip records logged while inside the publish path.

**Files**: `internal/log/mqtt_publisher.go` (new),
`internal/broker/server.go`.

**LOC**: ~60.

---

### 5.11 — Honour `RetentionHours` for the broker `metrics` table (P2) — **DONE**

**As built**: `Collector.RunRetention(ctx, retention, interval)`
spawns a goroutine that purges metrics rows older than `retention`
every `interval`. Server wires it from `cfg.Metrics.RetentionHours`
with a 1h tick. Initial purge fires synchronously so stale rows
from a previous broker run are dropped at boot. Setting
`RetentionHours: 0` disables retention entirely (no goroutine
started). Tests in `internal/metrics/retention_test.go`.

---

### 5.12 — Inline mochi-mqtt source

Tracked separately in [`PLAN-inline-mochi.md`](PLAN-inline-mochi.md).
The released `v2.7.9` we use is several commits behind upstream main
(message-expiry fixes, atomic.Bool `IsTakenOver`); vendoring lets us
apply local patches (e.g. a pre-resend hook so the persistent-queue
replay can be exact rather than gated on inflight length).

---

## 6. Out of scope (parked)

These are JVM features the edge broker is **not** meant to implement.
Recorded so we don't keep re-auditing.

- Cluster (Hazelcast multi-node coordination, distributed lock,
  message bus)
- OPC UA client / server
- Kafka bridge (in or out)
- NATS / Redis / WinCC OA / WinCC Unified / PLC4X / Neo4j bridges
- GenAI (Gemini/Claude/Ollama) integration
- Flow engine (JS automation)
- MCP server
- Sparkplug B decoder
- JDBC / InfluxDB / TimeBase loggers
- Prometheus exporter (could be added trivially via metrics store; if a
  user asks, promote to TODO)
- NATS protocol listener (port 4222)

---

## 7. Suggested execution order

1. **P0**: §5.1 (archive bounding) — silently dropping data under load
   is the only currently-known data-loss path.
2. **P1, fast wins first**: §5.2 (purgeInterval) → §5.8
   (createdAt/updatedAt) → §5.11 (metrics retention) — small, surgical.
3. **P1 features**: §5.3 (getAggregatedHistory) → §5.4 (per-session and
   per-bridge metrics) — these unlock dashboard pages.
4. **P1 security**: §5.6 (token auth) — needed before any production
   deployment with user management on.
5. **P2**: §5.5 (offline buffer), §5.7 (PG/Mongo tests), §5.9 (slog
   extraction).
6. **P3**: §5.10 (MQTT syslog), §5.12 (inline mochi).

Total: ~1,500 lines of production Go + ~400 lines of tests across all
P0–P2 items, parallelizable.

---

## Appendix A — Original build milestones (history)

The plan that built the broker initially. All milestones complete.

1. Skeleton + config + logging
2. Mochi-mqtt with in-memory stores + auth hook (TCP/WS, allow-all)
3. SQLite backend (all 7 stores, byte-compatible schemas)
4. Users + ACL with bcrypt (mochi auth/ACL hooks)
5. Archive groups (config store + group orchestrator + last-value +
   archive fanout)
6. GraphQL read surface (gqlgen, copy/slim `.graphqls`, resolvers)
7. GraphQL write surface + subscriptions (`publish`, user/ACL CRUD,
   archiveGroup CRUD, `topicUpdates`, `topicUpdatesBulk`, systemLogs)
8. Metrics (in-memory counters → `IMetricsStore`, `Broker.metrics` etc.)
9. PostgreSQL + MongoDB backends (port SQLite implementations to pgx
   and mongo-driver)
10. MQTT bridge (DeviceConfigStore + manager + paho connector + topic
    transform + GraphQL `mqttClient.*`)
11. Dashboard packaging (embed `dashboard/dist` or external path)
12. TLS + WSS listeners, system-log MQTT publishing scheduled,
    retention/purge background loops
13. Docker multi-arch image + systemd unit + README

Subsequent dashboard-parity rounds have layered the Features rename,
JVM-shaped login/login-result/MqttClientResult/ConnectionStatus/etc.,
the persistent offline queue, the in-process log bus, dynamic deploy
of archive groups and bridges, and full alignment of Mqtt bridge
schema. See git log on `broker.go/`.

---

## Appendix B — Repository pointers

- `broker.go/README.md` — feature inventory + subsystem details
- `broker.go/GRAPHQL.md` — per-resolver API spec
- `broker.go/internal/stores/interfaces.go` — storage interface contract
- `broker.go/internal/stores/sqlite/*.go` — reference backend
  implementation
- `broker.go/internal/graphql/schema/schema.graphqls` — GraphQL SDL
