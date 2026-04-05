# REST API

The REST API extension exposes MonsterMQ publish, read, and subscribe operations over plain HTTP (`broker/src/main/kotlin/extensions/RestApiServer.kt`). It runs on the same HTTP server as GraphQL (default port 4000) and provides an alternative for clients that cannot use MQTT or GraphQL — shell scripts, webhooks, low-code platforms, browser `EventSource`, etc.

## Enabling the Server

```yaml
GraphQL:
  Enabled: true
  Port: 4000

RestApi:
  Enabled: true   # default when GraphQL is enabled
```

The REST API requires the GraphQL HTTP server to be enabled (it registers routes on the same router). All endpoints are served under `/api/v1/`.

## Authentication

The REST API is fully self-contained — no GraphQL dependency needed for auth.

### Login (get a JWT)

```bash
curl -s -X POST http://localhost:4000/api/v1/login \
  -H "Content-Type: application/json" \
  -d '{"username":"Admin","password":"Admin"}'
```

Response:
```json
{
  "success": true,
  "token": "eyJhbGciOiJIUzI1NiJ9...",
  "username": "Admin"
}
```

### Using the token

Pass the JWT in subsequent requests:

```bash
curl -H "Authorization: Bearer <token>" \
  http://localhost:4000/api/v1/topics/sensor/temperature?retained
```

### Alternative: Basic Auth

For simple scripts where token management is not needed:

```bash
curl -u "Admin:Admin" \
  -X PUT "http://localhost:4000/api/v1/topics/sensor/temp?payload=23.5"
```

### Auth Rules

- **UserManagement disabled** → all requests pass (anonymous).
- `Authorization: Bearer <JWT>` → validated via `JwtService`.
- `Authorization: Basic <base64>` → validated via `UserManager.authenticate()`.
- Missing/invalid credentials → `401` with `WWW-Authenticate` header.
- ACL enforcement: `canPublish()` for write operations, `canSubscribe()` for read/subscribe. Denied → `403`.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/login` | Authenticate and get JWT token (no auth required) |
| `POST` | `/api/v1/topics/{topic}` | Publish with raw body as payload |
| `PUT` | `/api/v1/topics/{topic}` | Publish with inline query-param payload |
| `POST` | `/api/v1/write` | Bulk publish (JSON array) |
| `GET` | `/api/v1/topics/{topic}?retained` | Read retained value(s) |
| `GET` | `/api/v1/topics/{topic}?group=X` | Read last value from archive group |
| `GET` | `/api/v1/topics/{topic}?group=X&start=...&end=...` | Read history |
| `GET` | `/api/v1/subscribe?topic=...` | SSE live subscription |
| `GET` | `/api/v1/docs` | Swagger UI |
| `GET` | `/api/v1/openapi.yaml` | OpenAPI 3.0 specification |

## Publishing

### POST — Raw body payload

The entire request body becomes the MQTT payload. Supports any content type (text, JSON, binary).

```bash
# Text payload
curl -u Admin:Admin -X POST \
  "http://localhost:4000/api/v1/topics/sensor/temperature" \
  -H "Content-Type: text/plain" \
  -d "23.5"

# JSON payload
curl -u Admin:Admin -X POST \
  "http://localhost:4000/api/v1/topics/sensor/data" \
  -H "Content-Type: application/json" \
  -d '{"temperature": 23.5, "humidity": 65}'
```

Query parameters:
- `qos` — QoS level: 0, 1, or 2 (default: 0)
- `retain` — Retain flag: `true` or `false` (default: `false`)

Response:
```json
{"success": true, "topic": "sensor/temperature"}
```

### PUT — Inline payload

The payload is passed as a query parameter. Convenient for simple values.

```bash
curl -u Admin:Admin -X PUT \
  "http://localhost:4000/api/v1/topics/sensor/temperature?payload=23.5&retain=true&qos=1"
```

Response:
```json
{"success": true, "topic": "sensor/temperature"}
```

### Bulk write

Publish multiple messages in a single request. The endpoint supports two payload formats:

1. **Standard `messages` format** (array of JSON objects):
```bash
curl -u Admin:Admin -X POST \
  "http://localhost:4000/api/v1/write" \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {"topic": "sensor/temp", "value": "23.5", "retain": true},
      {"topic": "sensor/humidity", "value": "65", "qos": 1},
      {"topic": "actuator/valve", "value": "open"}
    ]
  }'
```

2. **Compressed `records` format** (array of positional arrays `[topic, value, qos, retain]`):
This format is highly optimized for performance and high-throughput data ingestion, avoiding the overhead of JSON object keys. The `qos` and `retain` fields are optional and default to `0` and `false`.
```bash
curl -u Admin:Admin -X POST \
  "http://localhost:4000/api/v1/write" \
  -H "Content-Type: application/json" \
  -d '{
    "records": [
      ["sensor/temp", "23.5", 0, true],
      ["sensor/humidity", "65", 1],
      ["actuator/valve", "open"]
    ]
  }'
```

Response:
```json
{"success": true, "count": 3}
```

If some messages are invalid (missing `topic` or `value`), valid messages are still published and errors are reported:

```json
{"success": true, "count": 1, "errors": ["Message at index 0: missing 'topic'", "Message at index 1: missing 'value'"]}
```

### InfluxDB Line Protocol

Ingest raw InfluxDB Line Protocol metrics. Telegraf and other Influx-compatible clients can POST directly to this endpoint.

```bash
curl -u Admin:Admin -X POST \
  "http://localhost:4000/api/v1/write/influx?base=enterpriseA/siteA/" \
  -H "Content-Type: text/plain" \
  -d 'sensor,room=living temp=22.5,hum=50i 123456789'
```

Query parameters:
- `base` (optional): Prefix added to the generated MQTT topics.
- `format` (optional):
  - `simple` (default): Unrolls fields into multiple basic MQTT topics. From the example: 
    - Topic: `enterpriseA/siteA/sensor/living/temp` → Payload: `22.5`
    - Topic: `enterpriseA/siteA/sensor/living/hum` → Payload: `50`
  - `json`: Creates a single comprehensive JSON payload per line. From the example: 
    - Topic: `enterpriseA/siteA/sensor/living` 
    - Payload: `{"temp": 22.5, "hum": 50, "timestamp_ns": 123456789, "timestamp": "1970-01-02T10:17:36.789Z"}`

## Reading Data

### Retained messages

Read retained values from the broker's retained store. Supports MQTT wildcards (URL-encoded: `+` → `%2B`, `#` → `%23`).

```bash
# Single topic
curl -u Admin:Admin \
  "http://localhost:4000/api/v1/topics/sensor/temperature?retained"

# Wildcard — all sensors
curl -u Admin:Admin \
  "http://localhost:4000/api/v1/topics/sensor/%23?retained"
```

Response:
```json
{
  "messages": [
    {
      "topic": "sensor/temperature",
      "value": "23.5",
      "timestamp": "2026-04-01T10:30:00Z",
      "qos": 0,
      "retain": true
    }
  ]
}
```

### Last value (from archive group)

Read the most recent value from a configured archive group's last-value store.

```bash
curl -u Admin:Admin \
  "http://localhost:4000/api/v1/topics/sensor/temperature?group=Default"
```

### Historical data

Query time-series data from an archive group. Times are in ISO 8601 format.

```bash
curl -u Admin:Admin \
  "http://localhost:4000/api/v1/topics/sensor/temperature?group=Default&start=2026-04-01T00:00:00Z&end=2026-04-01T12:00:00Z&limit=100"
```

Query parameters:
- `group` (required) — Archive group name
- `start` — Start time (ISO 8601), defaults to 1 hour ago
- `end` — End time (ISO 8601), defaults to now
- `limit` — Maximum number of records (default: 1000)

Response format is the same as retained reads.

## Live Subscription (SSE)

Subscribe to real-time topic updates via Server-Sent Events. Works with the browser-native `EventSource` API — no WebSocket or MQTT library needed.

```bash
# Single topic
curl -u Admin:Admin \
  "http://localhost:4000/api/v1/subscribe?topic=sensor/temperature"

# Multiple topics
curl -u Admin:Admin \
  "http://localhost:4000/api/v1/subscribe?topic=sensor/%23&topic=actuator/%23"
```

Each event is a JSON object:

```
data: {"topic":"sensor/temperature","value":"23.5","timestamp":"2026-04-01T10:30:00.123Z"}

data: {"topic":"sensor/humidity","value":"65","timestamp":"2026-04-01T10:30:01.456Z"}
```

The server sends `:keepalive` comments every 30 seconds to detect broken connections.

### Browser usage

```javascript
const source = new EventSource(
  'http://localhost:4000/api/v1/subscribe?topic=sensor/%23',
  // Note: EventSource doesn't support custom headers.
  // Use Basic auth in the URL or disable UserManagement for browser SSE.
);

source.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log(`${data.topic}: ${data.value} @ ${data.timestamp}`);
};

source.onerror = (err) => {
  console.error('SSE connection error', err);
};
```

For authenticated browser SSE, pass credentials via the URL if Basic auth is configured, or use a proxy that injects the JWT header.

## Topic Path Encoding

MQTT topic separators (`/`) are passed directly in the URL path. MQTT wildcards must be URL-encoded:

| Wildcard | URL encoding | Example |
|----------|-------------|---------|
| `+` (single level) | `%2B` | `/api/v1/topics/sensor/%2B/temperature?retained` |
| `#` (multi level) | `%23` | `/api/v1/topics/sensor/%23?retained` |

## Error Responses

All errors return JSON:

```json
{"error": "Description of the problem"}
```

| Status | Meaning |
|--------|---------|
| `200` | Success |
| `400` | Bad request (missing parameters, invalid JSON, empty topic) |
| `401` | Authentication required or invalid credentials |
| `403` | Forbidden (ACL denied for the requested topic) |
| `404` | Resource not found (e.g., archive group does not exist) |
| `500` | Internal server error |

## OpenAPI & Swagger UI

- **Swagger UI**: `http://localhost:4000/api/v1/docs` — interactive API explorer
- **OpenAPI spec**: `http://localhost:4000/api/v1/openapi.yaml` — machine-readable specification

The OpenAPI spec can be imported into tools like Postman, Insomnia, or code generators.

## Comparison with GraphQL

The REST API covers the **data plane** — publish, read, subscribe. The **management plane** (users, ACLs, devices, flows, etc.) remains GraphQL-only:

| Capability | REST API | GraphQL |
|------------|----------|---------|
| Authentication (login) | ✅ | ✅ |
| Publish (single + batch) | ✅ | ✅ |
| Read retained values | ✅ | ✅ |
| Read last values | ✅ | ✅ |
| Read history | ✅ | ✅ |
| Live subscription | ✅ (SSE) | ✅ (WebSocket) |
| Aggregation (AVG, MIN, MAX) | ❌ | ✅ |
| Topic browsing/search | ❌ | ✅ |
| User/ACL management | ❌ | ✅ |
| Device management | ❌ | ✅ |
| Flow engine | ❌ | ✅ |
| System logs | ❌ | ✅ |

## Usage Notes

1. The REST API and GraphQL API share the same HTTP port and authentication backend.
2. JWT tokens obtained via `POST /api/v1/login` are identical to those from the GraphQL `login` mutation — they work interchangeably.
3. Binary payloads that cannot be decoded as UTF-8 are returned as `value_base64` (Base64-encoded) instead of `value`.
4. SSE connections create internal MQTT subscriptions. They are automatically cleaned up when the client disconnects.
5. The `RestApi.Enabled` config key defaults to `true`. Set it to `false` to disable the REST API while keeping GraphQL active.
