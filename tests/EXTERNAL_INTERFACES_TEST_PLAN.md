# External Interfaces Test Plan

## Purpose

Create a focused standard test track for MonsterMQ external interfaces other than MQTT. MQTT 3.1.1, MQTT 5, retained messages, QoS behavior, and publish/subscription protocol behavior already have dedicated tests and are not part of this plan except as a data producer for cross-interface verification.

The goal is a repeatable suite that can be run after changes to prove that every non-MQTT public interface starts, authenticates, publishes or reads data correctly, and returns consistent history/current values from supported backends.

## Scope

Included interfaces:

| Interface | Default endpoint | Coverage target |
|-----------|------------------|-----------------|
| GraphQL HTTP | `http://localhost:4000/graphql` | Existing partial pytest coverage in `tests/graphql/test_features.py`; add or standardize health/schema/login/mutation/publish/current value/retained/history/error tests |
| GraphQL WebSocket | `ws://localhost:4000/graphqlws` | Existing scripts and partial pytest coverage in `tests/graphql/test_bulk_e2e.py`; standardize subscription connect, subscribe, receive update after publish, reconnect/error behavior |
| REST API | `http://localhost:4000/api/v1/*` | Login, OpenAPI/docs, publish, bulk write, retained read, last value read, history read, SSE subscribe, auth/ACL |
| Prometheus API | `http://localhost:3001/*` | `/metrics`, labels, series, instant query, range query, numeric topic values, auth |
| MCP server | `http://localhost:3000/mcp` | JSON-RPC initialize, tools/list, selected tool calls for retained/current/history data, SSE connect, auth |
| I3X API | `http://localhost:3002/i3x/*` | Namespaces, object types, objects, current value, history, update value, subscription sync/stream smoke |
| NATS protocol server | Configured `NATS` port | CONNECT, PING/PONG, PUB/SUB, auth rejection |
| Dashboard HTTP | `http://localhost:4000/` | Index loads, static assets load, no server error, dashboard route does not shadow API routes |

Excluded for this plan:

- MQTT protocol feature tests, because they already exist under `tests/mqtt3/` and `tests/mqtt5/`.
- External device bridges such as OPC UA client/server, PLC4X, Kafka client bridge, Redis, Telegram, WinCC, Neo4j, SparkplugB, and loggers.
- Browser-level dashboard UI behavior. This plan only checks the HTTP/static interface; UI interaction tests should live in a dashboard-specific plan.

## Recommended Language

Use Python with `pytest` for this external interface suite.

Reasons:

- Existing integration tests are already pytest-based.
- Python can drive HTTP, WebSocket, SSE, NATS, and JSON-RPC checks from one runner.
- Pytest markers and parametrization fit the backend matrix.
- JUnit XML output can be consumed by CI.

Keep Kotlin/JUnit for internal broker unit tests, but do not mix unit tests into this external interface plan.

## Proposed Test Layout

```text
tests/
  external/
    conftest.py
    auth.py
    data_contract.py
    test_graphql_http.py
    test_graphql_ws.py
    test_rest_api.py
    test_prometheus_api.py
    test_mcp_server.py
    test_i3x_api.py
    test_nats_protocol.py
    test_dashboard_http.py
    test_cross_interface_consistency.py
```

Add pytest markers:

```ini
markers =
    external: Non-MQTT external interface regression tests
    graphql: GraphQL HTTP and WebSocket tests
    rest: REST API tests
    prometheus: Prometheus API tests
    mcp: MCP server tests
    i3x: I3X API tests
    nats: NATS protocol tests
    dashboard: Dashboard HTTP/static tests
```

## Standard Data Contract

Each external interface test run should use a shared test topic prefix:

```text
test/external/{run_id}/...
```

For each backend/configuration:

1. Publish seed data through one known-good path, preferably MQTT because MQTT tests already validate broker publish behavior.
2. Use unique numeric, string, and JSON payload topics.
3. Verify current/last value through GraphQL, REST, Prometheus, MCP, and I3X where supported.
4. Verify history readback through GraphQL, REST, Prometheus range query, MCP, and I3X.
5. Verify write-capable interfaces can publish/update and that the published data is visible through at least one independent read interface.
6. Verify auth behavior consistently: no credentials, invalid credentials, valid Basic/JWT/Bearer where supported.

## Backend Matrix

External interface tests must verify history/current value reads against:

| Backend | Required in standard run |
|---------|--------------------------|
| SQLite | Yes |
| PostgreSQL | Yes |
| MongoDB | Yes |
| CrateDB | Yes |
| Kafka archive | Optional profile, because it is an archive backend and requires Kafka |

The same cross-interface consistency tests should run for every backend where the interface feature is supported.

## Target Commands

```bash
cd tests
pytest -m external
pytest -m "external and graphql"
pytest -m "external and rest"
pytest -m "external and prometheus"
pytest -m "external and mcp"
pytest -m "external and i3x"
pytest -m "external and nats"
pytest --external-backend sqlite
pytest --external-backend postgres
pytest --external-backend mongodb
pytest --external-backend cratedb
pytest --external-backend all
```

CI output:

```bash
pytest -m external --junitxml=reports/external-interfaces.xml
```

## Implementation Phases

### Phase 1: Harness And Markers

- [ ] Add `tests/external/`.
- [ ] Add pytest markers for `external`, `graphql`, `rest`, `prometheus`, `mcp`, `i3x`, `nats`, and `dashboard`.
- [ ] Add shared config fixtures for endpoint URLs, credentials, auth headers, and unique run IDs.
- [ ] Add readiness checks for enabled external services.

### Phase 2: HTTP Interfaces

- [ ] Review existing GraphQL tests and mark stable cases as `external`.
- [ ] Add missing GraphQL HTTP smoke/schema/login/mutation/history tests.
- [ ] Move or mark stable REST tests into the external suite.
- [ ] Add dashboard HTTP/static smoke tests.

### Phase 3: Streaming Interfaces

- [ ] Convert useful GraphQL WebSocket helper scripts into pytest assertions where needed.
- [ ] Add missing GraphQL WebSocket subscription/reconnect tests.
- [ ] Add REST SSE subscribe tests if not already covered by stable existing tests.
- [ ] Add MCP SSE connection smoke test.
- [ ] Add I3X subscription sync/stream smoke test.

### Phase 4: Specialized APIs

- [ ] Add Prometheus `/metrics` scrape tests.
- [ ] Add Prometheus labels, series, instant query, and range query tests.
- [ ] Add MCP JSON-RPC initialize, tools/list, and selected data tool calls.
- [ ] Add I3X namespace/object/value/history/update tests.
- [ ] Add NATS CONNECT/PING/PUB/SUB/auth tests.

### Phase 5: Cross-Interface Consistency

- [ ] Add shared seed publisher using MQTT as the known-good input path.
- [ ] Verify the same seed data through GraphQL, REST, Prometheus, MCP, and I3X.
- [ ] Verify write operations through REST, GraphQL, MCP, and I3X are visible through an independent read path.
- [ ] Verify auth/ACL behavior is consistent across interfaces.

### Phase 6: Backend Matrix

- [ ] Add backend selection option `--external-backend`.
- [ ] Add or reuse isolated configs for SQLite, PostgreSQL, MongoDB, and CrateDB.
- [ ] Decide whether external database services are provided by Docker Compose, Testcontainers, or pre-running services.
- [ ] Emit per-backend logs and JUnit XML.

## Open Decisions

- [ ] Should the external suite start/stop the broker automatically, or assume it is already running?
- [ ] Should external database services be managed by Docker Compose or `testcontainers-python`?
- [ ] Which MCP tools are considered stable enough for the standard interface contract?
- [ ] Should NATS be in the default local run or only in the full external profile because it is disabled by default?
- [ ] Should dashboard HTTP smoke tests build `dashboard/dist/` first or rely on broker-packaged resources?
