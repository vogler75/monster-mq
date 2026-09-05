# Grafana Integration

MonsterMQ exposes a Prometheus-compatible HTTP API for archive data and broker
metrics. Use Grafana's built-in **Prometheus** datasource. The former
`Grafana.Enabled` configuration and `/api/grafana` JSON datasource routes are no
longer implemented.

## Enable the API

```yaml
Prometheus:
  Enabled: true
  Port: 3001
  RawQueryLimit: 10000
Metrics:
  Enabled: true
  CollectionIntervalSeconds: 10
```

The default port in the implementation is 3001; the Docker starter overrides it
with 4002. Publish the configured port when running a container.

Create a Prometheus datasource in Grafana pointing to `http://<broker-host>:3001`.
Use a hostname reachable from the Grafana server; `localhost` inside a Grafana
container refers to that container. When MonsterMQ user management is enabled,
configure Basic authentication with a MonsterMQ account or supply a valid JWT in
the `Authorization: Bearer <token>` header.

## Queries

Select a numeric MQTT payload from an archive group's last-value store:

```promql
topics{group="Default",topic="sensors/temperature"}
```

For a JSON payload such as `{"data":{"temperature":23.5}}`, add a field path:

```promql
topics{group="Default",topic="sensors/data",field="data.temperature"}
```

Broker metrics use the `metrics` selector:

```promql
metrics{node="local",metric="messages_in"}
```

Other metric labels include `messages_out`, `sessions`, `subscriptions`,
`queue_depth`, `bus_in`, `bus_out`, and connector input/output rates. Discover
available label values through the API instead of assuming all connectors have
both directions.

Instant topic queries use the selected last-value store; range queries require
an archive backend implementing historical queries. Broker metric history
requires an enabled metrics store. In range queries, steps under 60 seconds use
raw topic history, capped by `RawQueryLimit` (0 means unlimited). Larger steps use
supported archive aggregation intervals.

This is a limited compatibility API, not a full PromQL engine. Use the simple
selectors above. Do not assume arbitrary PromQL expressions, recording rules,
or alert-rule evaluation are implemented. A separate Prometheus deployment can
scrape `/metrics` when those capabilities are needed.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/metrics` | Prometheus text exposition for scraping |
| GET | `/api/v1/labels` | Discover label names |
| GET | `/api/v1/label/{name}/values` | Discover label values |
| POST | `/api/v1/series` | Discover series matching a selector |
| GET, POST | `/api/v1/query` | Current values |
| GET, POST | `/api/v1/query_range` | Historical values |

```bash
curl -u Admin:Admin --get http://localhost:3001/api/v1/query \
  --data-urlencode 'query=topics{group="Default",topic="sensors/temperature"}'
```

If results are empty, check the datasource host/port, the archive group name,
whether its last-value/archive stores are enabled, whether the selected period
contains data, and whether the payload or selected JSON field is numeric.

See [Archiving](archiving.md), [Monitoring](monitoring.md), and the implementation
in [PrometheusServer.kt](../broker/src/main/kotlin/extensions/PrometheusServer.kt).
