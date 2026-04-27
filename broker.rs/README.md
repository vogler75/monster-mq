# MonsterMQ Edge — broker.rs

A Rust port of [`broker.go`](../broker.go), the single-binary MQTT edge broker.
Same on-disk schema (SQLite/Postgres/MongoDB) so a database file can be opened
by either the JVM, Go, or this Rust implementation.

This README mirrors the structure of `../broker.go/README.md` and tracks what
is currently implemented, what is stubbed, and what is intentionally skipped.

---

## Project layout

```
broker.rs/
├── src/
│   ├── main.rs                              # CLI flags, config load, lifecycle
│   ├── lib.rs                               # module re-exports
│   ├── version.rs
│   ├── config.rs                            # YAML config + defaults (serde)
│   ├── log_bus.rs                           # tracing layer + ring buffer + broadcast
│   ├── pubsub.rs                            # in-process pub/sub for GraphQL subscriptions
│   ├── auth.rs                              # user/ACL cache (bcrypt + 30 s refresh)
│   ├── metrics.rs                           # 1 s tick collector → MetricsStore
│   ├── archive/
│   │   ├── group.rs                         # buffered flush per group (250 ms / 100 msgs)
│   │   ├── manager.rs                       # manager + reload + group-name validator
│   │   └── retention.rs                     # background retention purge
│   ├── bridge/
│   │   └── mqttclient/                      # rumqttc-based MQTT-to-MQTT bridge
│   ├── broker/
│   │   └── server.rs                        # rumqttd embed + storage hook loop
│   ├── graphql/
│   │   ├── schema.rs / payload.rs           # SDL types + payload encoder
│   │   ├── query.rs / mutation.rs           # resolvers
│   │   ├── subscription.rs                  # topicUpdates / topicUpdatesBulk / systemLogs
│   │   └── server.rs                        # axum HTTP + GraphQL-WS
│   └── stores/
│       ├── traits.rs / types.rs / topic.rs  # async traits + types + MQTT match helpers
│       ├── sqlite/                          # full sqlx-based SQLite implementation
│       ├── memory/                          # in-memory MessageStore (for archive groups)
│       ├── postgres/                        # TODO
│       └── mongodb/                         # TODO
└── config.yaml.example
```

## Implemented vs intentionally skipped

| Feature                                  | broker.rs | Notes |
|------------------------------------------|-----------|-------|
| MQTT 3.1 / 3.1.1 / 5.0 protocol          | ✅ via rumqttd | TCP listener wired; WS/TLS = TODO |
| Retained messages (persisted, restart-safe) | ✅     | Restored from store at boot |
| QoS 0/1/2 + inflight                     | ✅ (rumqttd native) | |
| Persistent sessions (clean=false)        | ⚠️         | rumqttd manages sessions in-memory; SQLite session table mirrors the schema but post-restart replay is TODO |
| Authentication (bcrypt user store)       | ✅         | Auth cache mirrors broker.go; rumqttd hook integration TODO |
| ACL (per-user topic patterns + priority) | ✅         | Validated/cache-driven; rumqttd hook integration TODO |
| Offline message queue (persistent)       | ⚠️         | QueueStore present; replay on reconnect = TODO |
| Archive groups (last-value + history)    | ✅ (SQLite + MEMORY) | Postgres / MongoDB last-value/archive backends not yet wired |
| Archive retention (background purge)     | ✅         | Per-group, parsed from "30d" / "1h" / "5m" / "10s" |
| MQTT → MQTT bridge (in & out)            | ✅         | rumqttc + per-address mapping + manager.reload() |
| GraphQL HTTP/WS API                      | ✅         | Schema parity with broker.go (see GRAPHQL.md) |
| Dashboard hosting                        | ✅         | Serves `Dashboard.Path` if set |
| Periodic broker metrics → DB             | ✅         | 1 s default tick |
| Cross-restart system log query           | ✅         | In-memory ring buffer |
| Live system log subscription (WS)        | ✅         | tokio broadcast → GraphQL Subscription |
| **Postgres / MongoDB stores**            | ❌         | TODO — SQLite is the priority for parity testing |
| **TLS / WS / WSS listeners**             | ❌         | TODO — JKS-style KeyStorePath needs PEM conversion |
| **Clustering (Hazelcast etc.)**          | ❌         | Single-node only by design (matches broker.go) |
| **OPC UA / Kafka / WinCC / NATS / Redis / GenAI / Flow / MCP / Sparkplug / JDBC / InfluxDB / TimeBase / Prometheus / NATS listener** | ❌ | Out of scope |

When the dashboard queries `Broker.enabledFeatures`, this broker returns
`["MqttBroker", "MqttClient"]` — same as broker.go.

## Build / run

```bash
cd broker.rs
cargo run --release -- --config config.yaml.example
```

## GraphQL API contract

See [GRAPHQL.md](GRAPHQL.md) (copied verbatim from broker.go) — the resolvers
in this Rust port aim for byte-identical responses where the underlying data
is available.

## Storage compatibility

Matches the schema documented in `../broker.go/README.md`:

- SQLite tables: `sessions`, `subscriptions`, `messagequeue`, `users`,
  `usersacl`, `archiveconfigs`, `deviceconfigs`, `metrics`, plus per-group
  `<groupname>lastval` / `<groupname>archive`. The retained message store
  uses the 9-column literal-prefix layout (`topic_1` … `topic_9`, `topic_r` JSON).
- Group name validation: `^[A-Za-z][A-Za-z0-9_]{0,62}$`.
