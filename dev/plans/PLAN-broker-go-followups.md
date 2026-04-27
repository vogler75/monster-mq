# broker.go — implementation follow-ups

This is a TODO plan covering every gap between `broker.go/` and the JVM
broker that surfaced during the recent dashboard-parity work, plus a few
minor hygiene items. It is meant to be executed top-to-bottom; each item
lists current state, JVM behaviour, the proposed change, files to touch,
rough size, and priority.

Priority key:
- **P0** — correctness or data-loss risk under stress
- **P1** — feature parity that the dashboard or operators rely on
- **P2** — nice-to-have / robustness
- **P3** — explicitly out of scope for the edge profile (parked)

---

## 1. Archive group buffer: bound the queue & batch size  (P0)

**Current**: Per-group `pending []BrokerMessage` slice grows unboundedly. A
ticker (250 ms) or a 100-message threshold triggers `flush()` which writes
the whole pending slice in one bulk call. If a backend stalls (DB lock,
network), memory grows without limit.

**JVM**: `ArrayBlockingQueue<BrokerMessage>(100_000)` — bounded queue;
writer thread drains up to `maxWriteBlockSize` per cycle. Producer-side
`saveMessage` swallows `IllegalStateException` when the queue is full
(messages drop on overflow).

**Plan**:
1. Replace `pending []BrokerMessage` with a buffered channel
   `chan stores.BrokerMessage` of size `cfg.BufferSize` (default 100_000).
2. `Submit()` does a non-blocking send — drops on full and increments a
   `droppedCount` atomic that surfaces via the `messagesOut` /
   `bufferSize` metrics later.
3. `flush()` drains up to `cfg.MaxBatchSize` (default 1000) into a slice
   then writes; loops while channel still has items.
4. Make `BufferSize`, `MaxBatchSize`, `FlushInterval` per-group fields in
   `stores.ArchiveGroupConfig` (with broker-wide defaults from a new
   `Archive: { BufferSize, MaxBatchSize, FlushIntervalMs }` config block).
5. New integration test: 200k publishes against a deliberately slow store
   (mock that sleeps 1s in `AddAll`) — assert memory bounded and dropped
   counter rises.

**Files**:
- `internal/archive/group.go`, `internal/archive/manager.go`
- `internal/stores/types.go` (ArchiveGroupConfig)
- `internal/stores/{sqlite,postgres,mongodb}/archive_config_store.go` —
  new optional columns
- `internal/config/config.go` (ArchiveDefaults block)
- `test/integration/archive_overflow_test.go` (new)

**LOC**: ~150 prod + ~80 test.

---

## 2. Honour `purgeInterval` per group  (P1)

**Current**: A single Manager-wide ticker fires every 15 minutes hard-coded
and purges every group in lockstep. `cfg.PurgeInterval` is read from the
DB config but never consulted.

**JVM**: Each group schedules its own `Vertx.setPeriodic(purgeIntervalMs)`
timer (default `1h`); separate timers for last-value and archive stores.

**Plan**:
1. Move retention scheduling into `archive.Group` (not `Manager`):
   - `Group.Start()` reads `Config().PurgeInterval`, parses via
     `parseDuration`, defaults to 1h.
   - Spawns a per-group goroutine with its own ticker.
   - On each tick: if `LastValRetention` set → purge lastVal; if
     `ArchiveRetention` set → purge archive.
2. `Manager.Reload` already restarts the Group when config changes, so
   the new schedule picks up automatically.
3. Drop `Manager.RunRetention` and the old `purgeOnce`.
4. Run an initial purge on `Group.Start()` (preserves boot-time cleanup).

**Files**:
- `internal/archive/group.go` (add per-group retention loop)
- `internal/archive/retention.go` (delete or shrink to just `parseDuration`)
- `internal/broker/server.go` (drop `archives.RunRetention(...)` call)

**LOC**: ~60 net change.

---

## 3. Implement `GetAggregatedHistory` for the dashboard's trend chart  (P1)

**Current**: GraphQL `aggregatedMessages` resolver returns `{columns:["timestamp"], rows:[]}` — total stub. The dashboard's archive-explorer trend chart is non-functional.

**JVM**: `IMessageArchive.getAggregatedHistory(topics, startTime, endTime, intervalMinutes, functions, fields)` returns `{columns: [...], rows: [...]}` where:
- `columns[0]` is `"timestamp"`, then one column per `topic[.field]_func` combo.
- Each row is `[bucketTimestampISO, val_topic1_avg, val_topic1_max, val_topic2_avg, ...]`.
- Functions: AVG, MIN, MAX, COUNT.
- If `fields` is empty, the payload itself is treated as a numeric scalar
  (parses payload_blob as UTF-8 text → REAL).
- If `fields` is provided, JSON-extract `$.field` from `payload_json` →
  REAL.

**Plan**:
1. Extend `stores.MessageArchive` interface:
   ```go
   GetAggregatedHistory(ctx, topics []string, from, to time.Time,
       intervalMinutes int, functions, fields []string) (AggregatedResult, error)
   ```
2. **SQLite impl** — port the JVM's `strftime` bucketing + `CASE WHEN
   topic = ? THEN value END` aggregation per topic. Mirror the bucket
   expressions: 1 / 5 / 15 / 60 / 1440 minute fast paths, generic
   fallback for arbitrary intervals.
3. **Postgres impl** — `date_trunc(...)` plus `FILTER (WHERE topic = ...)`
   for clean per-topic aggregation.
4. **MongoDB impl** — aggregation pipeline with `$bucket` /
   `$dateTrunc` + `$group`.
5. Resolver: parse start/end (RFC3339), look up the named archive group,
   delegate to its `Archive`. Convert result to GraphQL `AggregatedResult`.
6. Integration test: publish 100 numeric values at known intervals,
   query bucketed AVG, assert the buckets match.

**Files**:
- `internal/stores/interfaces.go` (extend MessageArchive)
- `internal/stores/sqlite/message_archive.go`
- `internal/stores/postgres/postgres.go` (MessageArchive methods)
- `internal/stores/mongodb/mongodb.go`
- `internal/graphql/resolvers/resolver.go` (AggregatedMessages resolver)
- `test/integration/archive_aggregate_test.go` (new)

**LOC**: ~400 prod + ~80 test.

---

## 4. Per-session and per-bridge metrics  (P1)

**Current**: Only `BrokerMetrics` is collected and persisted. `Session.metrics`
returns a single empty stub; `MqttClient.metrics` ditto;
`ArchiveGroupMetrics` ditto.

**JVM**: All four metric kinds tick at the same 1 s cadence and persist as
separate rows in the `metrics` table (`metric_type` discriminator).

**Plan**:
1. Per-session counters:
   - Add `SessionCounters` map in `metrics.Collector` keyed by clientID.
   - Storage hook bumps the per-session counter on `OnPublished` (in/out).
   - Tick: drain into `SessionMetrics{}` JSON and `MetricsStore.StoreMetrics(MetricSession, ...)`.
   - Resolver: replace stub with `MetricsStore.GetLatest(MetricSession, clientID)`.
2. Per-bridge counters:
   - Bridge `Connector` has `IncIn` / `IncOut` callbacks already (passed via Manager) — wire them to the collector.
   - Tick fanout for each connector by name → `MetricsStore.StoreMetrics(MetricMqttClient, name, ...)`.
3. Per-archive-group counters:
   - Archive `Group` increments `messagesOut` on each successful flush
     (pre-existing); also track `bufferSize` average over the tick.
   - Tick → `MetricsStore.StoreMetrics(MetricArchiveGroup, name, ...)`.

**Files**:
- `internal/metrics/collector.go` (multi-kind tracking)
- `internal/broker/hook_storage.go` (per-session bump)
- `internal/bridge/mqttclient/{connector,manager}.go` (bridge bump)
- `internal/archive/group.go` (group bump)
- `internal/graphql/resolvers/resolver.go` (real metric resolvers)
- `internal/stores/types.go` (new MetricKind constants if not present)

**LOC**: ~250 prod + ~50 test.

---

## 5. MQTT bridge: offline buffer  (P2)

**Current**: When the remote broker is unreachable, paho buffers in memory
(no disk persistence). Outbound publishes that paho can't deliver are
either lost or queued in paho's own bounded buffer.

**JVM**: Optional disk-backed buffer (file-system queue) with FIFO /
delete-oldest policy. `bufferEnabled`, `bufferSize`, `persistBuffer`,
`deleteOldestMessages` config fields exist on
`MqttClientConnectionConfig` and are honored.

**Plan**:
1. When `cfg.PersistBuffer && cfg.BufferEnabled`, use a small SQLite file
   per bridge (`./data/bridge_<name>.db`) instead of paho's in-memory buffer.
2. Wrap paho's publish: instead of calling `client.Publish()` directly,
   enqueue to local SQLite. A drain goroutine reads from SQLite and
   publishes when paho reports connected.
3. On overflow (`bufferSize` rows): if `deleteOldestMessages`, drop the
   oldest row; else reject the new publish.
4. On ack from paho (token Done with no error), delete the row.
5. Integration test: stop the remote, publish 50 messages, restart the
   remote, assert all 50 arrive after reconnect.

**Files**:
- `internal/bridge/mqttclient/buffer.go` (new — SQLite-backed FIFO)
- `internal/bridge/mqttclient/connector.go` (use buffer when configured)
- `test/integration/bridge_buffer_test.go` (new)

**LOC**: ~250 prod + ~80 test.

---

## 6. Bearer-token authentication on GraphQL requests  (P1 if user mgmt is in use)

**Current**: With user management ON, login validates credentials and
returns an opaque session token. **The token is never checked on
subsequent requests.** All mutations are accessible to anyone who can
reach the GraphQL endpoint.

**JVM**: Token-protected via per-request `Authorization` header check;
admin-only mutations (user CRUD, ACL CRUD) check `isAdmin`.

**Plan**:
1. Server-side session map keyed by token: `{username, isAdmin, expiresAt}`.
2. `login()` puts a freshly issued token in the map.
3. Chi middleware reads `Authorization: Bearer <tok>`, looks up, attaches
   `currentUser` to request context.
4. `currentUser` query reads from context.
5. Mutations that should be admin-only (`user.*`, `archiveGroup.*`,
   `mqttClient.*`, `session.*`) check the context user; reject with
   GraphQL error if not admin.
6. Anonymous mode: skip the middleware entirely (current behaviour
   unchanged).
7. Token expiry: 24 h sliding by default, configurable.

**Files**:
- `internal/graphql/server.go` (auth middleware)
- `internal/graphql/resolvers/resolver.go` (mutation guards, currentUser)
- `internal/auth/sessions.go` (new — token map)
- `test/integration/auth_token_test.go` (new)

**LOC**: ~200 prod + ~80 test.

---

## 7. Postgres + MongoDB integration tests via testcontainers  (P2)

**Current**: All 18 integration tests use SQLite. Postgres and MongoDB
backends compile and have compile-time interface assertions, but no
end-to-end test exercises them. A bson type quirk or pgx column-mapping
bug would only show up in production.

**Plan**:
1. Add `github.com/testcontainers/testcontainers-go` test-only dependency.
2. Run the existing `TestRetainedSurvivesRestart`,
   `TestQueuedMessagesPersistAcrossRestart`, `TestArchiveGroupWrites`,
   `TestGraphQLArchiveGroupCRUD` against Postgres and MongoDB containers
   via build-tagged variants.
3. CI job: matrix over `STORE_BACKEND={SQLITE,POSTGRES,MONGODB}`; tests
   skip with `t.Skip` if Docker isn't available locally.

**Files**:
- `test/integration/multistore_test.go` (parameterised setup)
- `Makefile` (new `test-multistore` target)
- `.github/workflows/ci.yml` (if/when CI is added)

**LOC**: ~150 test + Makefile target.

---

## 8. Track createdAt / updatedAt on archive groups  (P2)

**Current**: `ArchiveGroupInfo.createdAt` and `updatedAt` are always
returned as `null` because `ArchiveGroupConfig` doesn't track the
columns even though the DB has them.

**JVM**: Surfaces both as ISO strings.

**Plan**:
1. Add `CreatedAt`, `UpdatedAt time.Time` to
   `stores.ArchiveGroupConfig`.
2. Update SQLite/PG/Mongo `ArchiveConfigStore` row scans to populate them
   (DB columns already exist).
3. Resolver: format as RFC3339Nano.
4. On Save (insert), populate via `CURRENT_TIMESTAMP` (already in DDL).
5. On Update, set `updated_at = CURRENT_TIMESTAMP` (PG / SQLite already
   do this; verify Mongo does too).

**Files**:
- `internal/stores/types.go`
- `internal/stores/{sqlite,postgres,mongodb}/archive_config_store.go`
- `internal/graphql/resolvers/resolver.go` (`archiveGroupInfoTo`)

**LOC**: ~40 net.

---

## 9. Better slog-record extraction for SystemLogEntry  (P2)

**Current**: `SystemLogEntry.thread` is always 0; `parameters` and
`exception` are never populated.

**JVM**: `thread` is the JVM thread ID; `parameters` is the message
format args; `exception` is filled when a Throwable is logged.

**Plan**:
1. **Thread**: capture goroutine ID via `runtime.Stack()` parsing in the
   slog `Handler.Handle`. (Imperfect but cheap. Or skip — Go semantics
   diverge enough that this might be best left at 0.)
2. **Parameters**: collect all attrs that aren't well-known
   (`logger`, `source_class`, `source_method`, `thread`) into the
   `parameters` slice as `"<key>=<value>"` strings.
3. **Exception**: if any attr value is an `error`, populate `Exception`
   with `Class=fmt.Sprintf("%T", err)`, `Message=err.Error()`,
   `StackTrace=""` (no Java-style stack traces in Go without explicit
   `runtime.Stack`).

**Files**:
- `internal/log/bus.go` (Handler.Handle)

**LOC**: ~40.

---

## 10. Wire `MqttSyslogEnabled` to publish system logs as MQTT messages  (P3)

**Current**: `Logging.MqttSyslogEnabled` is a documented config field but
not implemented.

**JVM**: Republishes every log entry as MQTT message under
`$SYS/syslogs/<node>/<level>` so subscribers (or other brokers) can
consume the broker's logs over MQTT.

**Plan**:
1. When enabled, attach a slog handler that publishes to
   `$SYS/syslogs/<node>/<level>` via `mqtt.Server.Publish(...)` with a
   JSON-encoded payload mirroring `SystemLogEntry`.
2. Avoid recursion: skip records logged while inside the publish path.

**Files**:
- `internal/log/mqtt_publisher.go` (new)
- `internal/broker/server.go` (wire when enabled)

**LOC**: ~60.

---

## 11. Handle `cfg.RetentionHours` for the broker `metrics` table  (P2)

**Current**: `Metrics.RetentionHours: 168` is in config but the metrics
table grows forever — there's no scheduled purge.

**Plan**: A 1 h ticker calls `MetricsStore.PurgeOlderThan(now - hours)`.

**Files**:
- `internal/metrics/collector.go` (or a new `purge.go`)
- `internal/broker/server.go` (wire on Serve)

**LOC**: ~30.

---

## 12. Inline mochi-mqtt source  (P3 — separate plan)

**Current**: `github.com/mochi-mqtt/server/v2 v2.7.9` from the public
proxy; several commits behind upstream.

**Plan**: see [`PLAN-inline-mochi.md`](PLAN-inline-mochi.md).
Track here as a sibling.

---

## Out of scope (explicitly skipped, document only)

These are JVM features the edge broker is NOT meant to implement. Recorded
here so we don't keep re-auditing.

- Cluster (Hazelcast multi-node coordination, distributed lock, message
  bus)
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

## Suggested execution order

1. **P0**: #1 (archive bounding) — silently dropping data under load is the
   only currently-known data-loss path.
2. **P1, fast wins first**: #2 (purgeInterval) → #8 (createdAt/updatedAt) →
   #11 (metrics retention) — small, surgical.
3. **P1 features**: #3 (getAggregatedHistory) → #4 (per-session/bridge
   metrics) — these unlock dashboard pages.
4. **P1 security**: #6 (token auth) — needed before any production
   deployment with user management on.
5. **P2**: #5 (offline buffer), #7 (PG/Mongo tests), #9 (slog extraction).
6. **P3**: #10 (MQTT syslog), #12 (inline mochi).

Total: ~1,500 lines of production Go + ~400 lines of tests across all P0–P2
items, parallelizable.
