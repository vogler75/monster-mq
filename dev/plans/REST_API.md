# REST API Implementation Plan

**GitHub Issue**: [#103 — Add REST API for publish, read, subscribe](https://github.com/vogler75/monster-mq/issues/103)
**Branch**: `feature/rest-api-103`

## Summary

Add a REST API to MonsterMQ hosted on the same HTTP server as GraphQL (port 4000).
The API provides publish, read (last value, retained, history), bulk write, and SSE-based
subscribe functionality under `/api/v1/`. Includes OpenAPI spec and Swagger UI.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/topics/{topic...}` | Publish (raw body) |
| PUT | `/api/v1/topics/{topic...}` | Publish (inline via query param) |
| POST | `/api/v1/write` | Bulk publish |
| GET | `/api/v1/topics/{topic...}?retained` | Read retained value |
| GET | `/api/v1/topics/{topic...}?group=X` | Read last value from archive group |
| GET | `/api/v1/topics/{topic...}?group=X&start=...&end=...` | Read history |
| GET | `/api/v1/subscribe?topic=...` | SSE subscribe |
| GET | `/api/v1/docs` | Swagger UI |

## Authentication

- Reuse auth pattern from `PrometheusServer.kt`
- If `UserManagement.Enabled` is false → all requests allowed
- `Authorization: Bearer <JWT>` → validate via `JwtService`
- `Authorization: Basic <base64>` → validate via `UserManager.authenticate()`
- 401 with `WWW-Authenticate` header on failure
- ACL: `canPublish()` for write, `canSubscribe()` for read/subscribe

## Architecture Decision

**Approach: Register routes on GraphQL's router** — REST routes are registered on the
same `Router` in `GraphQLServer.kt` before the catch-all static handler. No separate
port or verticle needed. The `RestApiServer` class is instantiated and passed the router
to register its handlers.

## Files to Create

1. `broker/src/main/kotlin/extensions/RestApiServer.kt` — All REST API handlers
2. `broker/src/main/resources/openapi.yaml` — OpenAPI 3.0 specification
3. `tests/rest/` — Python integration tests

## Files to Modify

1. `broker/src/main/kotlin/graphql/GraphQLServer.kt` — Accept `RestApiServer` instance, call `registerRoutes(router)` before static handler
2. `broker/src/main/kotlin/Monster.kt` — Instantiate `RestApiServer`, pass to `GraphQLServer`
3. `broker/yaml-json-schema.json` — Add `RestApi` config section
4. `broker/example-config.yaml` — Add example config

## Dependencies (all existing)

- `SessionHandler` — publishing messages, internal subscriptions for SSE
- `ArchiveHandler` — archive groups (last value store, history)
- `IMessageStore` (retained store) — reading retained messages
- `UserManager` — authentication and ACL checks
- `JwtService` — JWT token validation
- `vertx-web` — Router, BodyHandler, SSE via chunked response

## Implementation Steps

### Step 1: RestApiServer.kt
- Auth middleware (JWT + Basic, replicating PrometheusServer pattern)
- Topic path extraction (URL-decoded, wildcard support)
- POST publish handler (raw body)
- PUT publish handler (inline payload)
- POST bulk write handler
- GET topics handler (retained / last value / history dispatch)
- GET subscribe handler (SSE with internal subscription)
- GET /api/v1/docs (serve Swagger UI)

### Step 2: OpenAPI spec
- Define all endpoints in `openapi.yaml`
- Serve as static resource at `/api/v1/openapi.yaml`

### Step 3: Integration into GraphQLServer + Monster
- Pass RestApiServer to GraphQLServer constructor
- Call `restApiServer.registerRoutes(router)` before static handler
- Add `RestApi.Enabled` config flag

### Step 4: Config schema updates
- Add `RestApi` section to `yaml-json-schema.json`
- Add example to `example-config.yaml`

### Step 5: Regression tests
- Python integration tests using `requests` library
- Test publish (POST/PUT), read (retained, last value, history), bulk write
- Test SSE subscribe
- Test auth (JWT, Basic, anonymous, ACL)

## Configuration

```yaml
RestApi:
  Enabled: true   # default: true (when GraphQL is enabled)
```

No separate port — hosted on GraphQL port (default 4000).
