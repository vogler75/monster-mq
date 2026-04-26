# MonsterMQ Edge

A native, single-binary MQTT broker that ships the same GraphQL API and storage
schemas as the JVM-based MonsterMQ broker, slimmed down for edge deployments
on devices like the Raspberry Pi 4/5.

## Highlights

- **Single static binary** (~25 MB), zero CGO. Pure-Go SQLite via `modernc.org/sqlite`.
- **MQTT 3.1 / 3.1.1 / 5.0** via [mochi-mqtt/server](https://github.com/mochi-mqtt/server) — TCP, WebSocket, TLS, WSS.
- **Storage**: SQLite (default), PostgreSQL, MongoDB. Schemas are byte-compatible with the Kotlin broker, so the same DB can be opened by either implementation.
- **Archive groups** (last-value + history fanout, retention purging) — same model as the Kotlin broker.
- **GraphQL API** with subscriptions, schema-parity with the existing dashboard.
- **MQTT bridge** — forward local topics to a remote broker and vice versa.
- **Users + ACL** with bcrypt password hashing.
- **Periodic metrics** persisted to the `metrics` table; surfaced via `Broker.metrics`/`metricsHistory`.
- No clustering, no OPC UA / Kafka / WinCC / Sparkplug / GenAI / flows.

## Quickstart

```bash
make build
./bin/monstermq-edge -config config.yaml.example
```

- MQTT: `mqtt://localhost:1883`
- WebSocket MQTT: `ws://localhost:1884/mqtt`
- GraphQL HTTP/WS: `http://localhost:8080/graphql`
- GraphQL playground: `http://localhost:8080/playground`

## Cross-compile

```bash
make build-arm64    # Linux ARM64 (Pi 4/5, generic 64-bit)
make build-armv7    # Linux ARMv7 (Pi 3 / older 32-bit)
make build-amd64    # Linux x86_64
```

## Docker

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t monstermq-edge:latest .
docker run --rm -p 1883:1883 -p 8080:8080 monstermq-edge:latest
```

## Storage backends

Pick the backend in `config.yaml`:

```yaml
DefaultStoreType: SQLITE   # or POSTGRES, or MONGODB
SQLite: { Path: ./data/monstermq.db }
Postgres: { Url: "postgres://localhost:5432/monstermq", User: monstermq, Pass: monstermq }
MongoDB:  { Url: "mongodb://localhost:27017", Database: monstermq }
```

All seven stores (retained, sessions, subscriptions, queue, users, archive
config, device config, metrics) live on the chosen backend.

## Dashboard

Set `Dashboard.Path` to a built `dashboard/dist` directory to serve the existing
MonsterMQ dashboard against this broker. With no path set, a placeholder index
page is served at `/` linking to the GraphQL playground.

```yaml
Dashboard:
  Enabled: true
  Path: /opt/monstermq-dashboard/dist
```

## Architecture

```
cmd/monstermq-edge/      → main, flag parsing, signal handling
internal/
  config/                → YAML schema + loader
  broker/                → mochi-mqtt bootstrap, hook wiring, TLS, lifecycle
  stores/                → MessageStore / MessageArchive / SessionStore /
                           QueueStore / UserStore / ArchiveConfigStore /
                           DeviceConfigStore / MetricsStore interfaces
  stores/sqlite/         → byte-compatible SQLite implementations
  stores/postgres/       → byte-compatible PostgreSQL implementations
  stores/mongodb/        → MongoDB implementations
  archive/               → archive group orchestrator + retention
  bridge/mqttclient/     → MQTT-to-MQTT bridge (paho client)
  auth/                  → user+ACL cache
  metrics/               → in-memory counters + periodic snapshot writer
  pubsub/                → in-process bus for GraphQL topicUpdates
  graphql/               → gqlgen-generated server, resolvers, dashboard handler
```

## Running tests

```bash
make test
```

Integration tests cover MQTT pub/sub, retained survives-restart, bcrypt auth +
ACL, archive group fanout, GraphQL queries/mutations, metrics persistence, and
end-to-end MQTT bridging between two brokers.

## Status

Single-node. No clustering. Production-ready for edge use on Pi 4/5;
PostgreSQL/MongoDB backends compile but require a live DB to integration test.
