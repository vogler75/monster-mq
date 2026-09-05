# GraphQL API

The GraphQL extension exposes MonsterMQ state and control operations over HTTP/WebSocket (`broker/src/main/kotlin/graphql/GraphQLServer.kt`). This page documents the configuration switches and the queries/mutations that exist today.

## Enabling the Server

```yaml
GraphQL:
  Enabled: true
  Port: 4000      # Defaults to 4000 when omitted
  Path: /graphql  # Defaults to /graphql
```

Once enabled the server listens on `http://<host>:<port><path>` and serves both the HTTP API and a WebSocket endpoint for subscriptions (`ws://<host>:<port><path>`).

## Authentication

- User management disabled? `login` returns `success: true` with `token = null` and no further checks are enforced (`broker/src/main/kotlin/graphql/AuthenticationResolver.kt`).
- User management enabled? Call the `login` mutation and supply the returned JWT in the `Authorization: Bearer <token>` header for subsequent requests.
- Admin-only mutations/queries (user management, ACLs, archive administration, etc.) are enforced through `GraphQLAuthContext` (`broker/src/main/kotlin/graphql/GraphQLServer.kt`).

Example login:

```graphql
mutation {
  login(username: "Admin", password: "Admin") {
    success
    token
    isAdmin
  }
}
```

## Queries

### Kafka Client Bridge Metrics (Embedded Fields)

Kafka client bridge configurations expose their current throughput sample and optional persisted history directly on `KafkaClient`.

Example:
```graphql
{
  kafkaClients {
    name
    namespace
    metrics { messagesIn messagesOut timestamp }
  }
}
```
Historical window:
```graphql
{
  kafkaClients(name: "MyKafkaClient") {
    metricsHistory(lastMinutes: 30) { messagesIn messagesOut timestamp }
  }
}
```
Semantics:
- messagesIn: Kafka records/sec consumed (latest sample).
- messagesOut: MQTT messages/sec forwarded to Kafka.
- Live fallback: If no persisted sample exists, resolver fetches a live point via the Vert.x event bus for immediate visibility.
- History requires a configured metrics store; otherwise only a single current sample is returned.

Broker metrics expose aggregate `kafkaClientIn` and `kafkaClientOut` rates.


The top-level `Query` type exposes the following fields (`broker/src/main/resources/schema-queries.graphqls`):

### OPC UA Device Metrics (Embedded Fields)

OPC UA device metrics are now accessed as embedded fields on `OpcUaDevice`:

```graphql
{
  opcUaDevices(name: "MyDevice") {
    name
    metrics { messagesIn messagesOut timestamp }
    metricsHistory(lastMinutes: 15) { messagesIn messagesOut timestamp }
  }
}
```

Previously exposed root queries `opcUaDeviceMetrics` and `opcUaDeviceMetricsHistory` have been removed. Use `OpcUaDevice.metrics` and `OpcUaDevice.metricsHistory` instead.

| Category | Field | Description |
|----------|-------|-------------|
| Current values | `currentValue(topic, archiveGroup)` | Latest value from the selected archive group’s last-value store. |
| | `currentValues(topicFilter, archiveGroup, limit)` | Latest values for topics matching the MQTT filter. |
| Retained messages | `retainedMessage(topic)` / `retainedMessages(topicFilter)` | Access the retained store. |
| Historical data | `archivedMessages(topicFilter, startTime, endTime, limit, archiveGroup)` | Query time-series data from archive groups. |
| Topic discovery | `searchTopics(pattern, archiveGroup)` | SQL-like wildcard search against topic names. |
| | `browseTopics(topic, archiveGroup)` | Browse one level of the topic tree. |
| Monitoring | `broker(nodeId)` / `brokers` | Cluster/node metrics and status. |
| | `sessions(nodeId, cleanSession, connected)` / `session(clientId, nodeId)` | MQTT session information. |
| User management | `users(username)` | List users and their ACL rules (admin only). |
| Archive groups | `archiveGroups` / `archiveGroup(name)` | Inspect archive configuration and connection status. |
| OPC UA client | `opcUaDevices(name, node)`, `clusterNodes` | Available when a device config store is configured. |
| OPC UA server | `opcUaServers(name, node)`, `opcUaServerCertificates` | Available when the config store supports server records. |

## Mutations

Available mutations are wired in `GraphQLServer.buildRuntimeWiring()` (`broker/src/main/kotlin/graphql/GraphQLServer.kt`). Highlights:

| Category | Mutations |
|----------|-----------|
| Authentication | `login` |
| Publishing | `publish`, `publishBatch` |
| Queued messages | `purgeQueuedMessages(clientId)` |
| User management | `user { createUser, updateUser, deleteUser, setPassword }` — see [Users](users.md) |
| ACL management | `user { createAclRule, updateAclRule, deleteAclRule }` — see [ACLs](acl.md) |
| Archive groups | `archiveGroup { create, update, delete, enable, disable }` — see [Archiving](archiving.md) |
| OPC UA client | `opcUaDevice { add, update, delete, toggle, reassign, addAddress, deleteAddress }` |
| OPC UA server | `opcUaServer { create, add, update, delete, toggle, addAddress, removeAddress }` |

These tables summarize the API; consult the split `schema-*.graphqls` files for exact arguments and the connector guides for complete operations.

All user and archive mutations require an admin-level JWT. Publishing requires publish permission for the target topic (enforced through the ACL system).

## Subscriptions

WebSocket subscriptions stream real-time data via the configured message bus (`broker/src/main/kotlin/graphql/GraphQLServer.kt`):

- `topicUpdates(topicFilters: [String!]!)` — Stream updates from one or more MQTT filters
- `topicUpdatesBulk(topicFilters: [String!]!, timeoutMs, maxSize)` — Batch updates by size or timeout
- `systemLogs(...)` - Stream system logs with advanced filtering (see [GraphQL System Logs](graphql-system-logs.md))

The current WebSocket route does not install the HTTP authentication-context handler, and topic subscription resolvers do not check per-user ACLs. Do not rely on HTTP JWT enforcement to restrict WebSocket subscriptions; restrict access at the HTTP gateway when needed.

### WebSocket Protocol

MonsterMQ uses the **GraphQL over WebSocket Protocol** (also known as **graphql-transport-ws**) for GraphQL subscriptions, implemented via Vert.x's `GraphQLWSHandler`. This is the newer protocol specified in the [graphql-ws library](https://github.com/enisdenjo/graphql-ws).

**Connection Flow:**

```javascript
const ws = new WebSocket('ws://localhost:4000/graphql', 'graphql-transport-ws');
ws.onopen = () => ws.send(JSON.stringify({ type: 'connection_init', payload: {} }));
ws.onmessage = ({ data }) => {
  const message = JSON.parse(data);
  if (message.type === 'connection_ack') {
    ws.send(JSON.stringify({
      id: '1', type: 'subscribe',
      payload: { query: 'subscription { topicUpdates(topicFilters: ["sensor/#"], format: TEXT) { topic payload timestamp } }' }
    }));
  } else if (message.type === 'ping') {
    ws.send(JSON.stringify({ type: 'pong', payload: message.payload }));
  } else {
    console.log(message); // next, error, or complete
  }
};
// When finished: ws.send(JSON.stringify({ id: '1', type: 'complete' })); ws.close();
```

**Message Types:**
- `connection_init` - Initialize WebSocket connection
- `connection_ack` - Server acknowledges connection
- `ping` - Ping message (client can send to keep connection alive)
- `pong` - Pong response to ping
- `subscribe` - Start a new subscription
- `next` - Subscription data payload
- `error` - Subscription error
- `complete` - Complete/stop a subscription

**Protocol Comparison:**

| graphql-transport-ws (MonsterMQ) | subscriptions-transport-ws (legacy Apollo) |
|----------------------------------|-------------------------------------------|
| WebSocket subprotocol: `'graphql-transport-ws'` | WebSocket subprotocol: `'graphql-ws'` |
| Subscribe: `type: 'subscribe'` | Start: `type: 'start'` |
| Next: `type: 'next'` | Data: `type: 'data'` |
| Complete: `type: 'complete'` | Stop: `type: 'stop'` |
| Ping/Pong: `type: 'ping'/'pong'` | Keep-alive: `type: 'ka'` |
| No explicit termination | Terminate: `type: 'connection_terminate'` |

**Client Libraries:**
- JavaScript/Browser: Use `graphql-ws` npm package or raw WebSocket implementation as shown above
- Python: Use `websockets` library with `subprotocols=["graphql-transport-ws"]` (see `tests/pytest_tests/graphql/`)
- Apollo Client v3+: Use `graphql-ws` package's `createClient`
- For legacy clients expecting `subscriptions-transport-ws`: Not compatible - use adapter or upgrade

## Usage Notes

1. Payload fields are GraphQL strings. `JSON`, `TEXT`, and `BINARY` select conversion, not a different GraphQL return type; see Data Formats below.
2. The schema is loaded independently of connector availability. Disabled features or missing stores can return empty results, nulls, or mutation errors. Read `broker { enabledFeatures }` before exposing feature-specific controls.
3. The dashboard is served at `/` on the same HTTP server, when packaged or supplied using `-dashboardPath`.
4. Use list queries with a `name` filter for device lookups and embedded `metrics`/`metricsHistory` fields for device rates.
5. Archive `PayloadFormat` (`DEFAULT` or `JSON`) is separate from API `DataFormat`.

## Endpoints

Unless overridden in configuration:

- HTTP GraphQL endpoint: `http://localhost:4000/graphql` (example port)
- WebSocket endpoint (subscriptions): `ws://localhost:4000/graphql` (same path as HTTP GraphQL)
- Health endpoint: `http://localhost:4000/health`

Adjust host/port/path according to your `GraphQL` configuration block.

## Examples

These examples use the current split schema and grouped mutation API.

### Current Value

```graphql
query GetCurrentValue {
  currentValue(topic: "sensor/temperature", format: JSON) {
    topic
    payload
    format
    timestamp
    qos
  }
}
```

### Current Values (Wildcard)

```graphql
query GetCurrentValues {
  currentValues(topicFilter: "sensor/+/temperature", format: JSON, limit: 10) {
    topic
    payload
    format
    timestamp
    qos
  }
}
```

### Retained Message

```graphql
query GetRetainedMessage {
  retainedMessage(topic: "config/device1", format: JSON) {
    topic
    payload
    format
    timestamp
    qos
  }
}
```

### Retained Messages

```graphql
query GetRetainedMessages {
  retainedMessages(topicFilter: "config/#", format: JSON, limit: 50) {
    topic
    payload
    format
    timestamp
    qos
  }
}
```

### Archived Messages (Time Range)

```graphql
query GetArchivedMessages {
  archivedMessages(
    topicFilter: "sensor/#"
    startTime: "2024-01-01T00:00:00Z"
    endTime:   "2024-01-02T00:00:00Z"
    format: JSON
    limit: 100
  ) {
    topic
    payload
    format
    timestamp
    qos
    clientId
  }
}
```

### Publish Single Message

```graphql
mutation PublishMessage {
  publish(input: {
    topic: "sensor/temperature"
    payload: "{\"value\": 23.5, \"unit\": \"celsius\"}"
    format: JSON
    qos: 1
    retained: false
  }) {
    success
    topic
    timestamp
    error
  }
}
```

### Publish Binary (Base64)

```graphql
mutation PublishBinary {
  publish(input: {
    topic: "camera/snapshot"
    payload: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
    format: BINARY
    qos: 0
    retained: true
  }) {
    success
    topic
    timestamp
    error
  }
}
```

### Batch Publish

```graphql
mutation BatchPublish {
  publishBatch(inputs: [
    { topic: "sensor/temperature", payload: "{\"value\": 23.5}", format: JSON, qos: 1 },
    { topic: "sensor/humidity",    payload: "{\"value\": 65}",  format: JSON, qos: 1 }
  ]) {
    success
    topic
    timestamp
    error
  }
}
```

### Subscribe (Single Filter)

```graphql
subscription SubscribeToTopic {
  topicUpdates(topicFilters: ["sensor/+/temperature"], format: JSON) {
    topic
    payload
    format
    timestamp
    qos
    retained
    clientId
  }
}
```

### Subscribe (Multiple Filters)

```graphql
subscription SubscribeToMultipleTopics {
  topicUpdates(
    topicFilters: ["sensor/temperature", "sensor/humidity", "sensor/pressure"]
    format: JSON
  ) {
    topic
    payload
    format
    timestamp
    qos
    retained
    clientId
  }
}
```

### MQTT Bridge Client Metrics

```graphql
query GetMqttClientMetrics {
  mqttClients {
    name
    metrics { messagesIn messagesOut timestamp }
  }
}
```

### MQTT Bridge Client Metrics History

```graphql
query GetMqttClientMetricsHistory {
  mqttClients(name: "bridge-client-1") {
    name
    metricsHistory(lastMinutes: 60) {
      messagesIn
      messagesOut
      timestamp
    }
  }
}
```

### curl Examples

```bash
curl -X POST http://localhost:4000/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query { currentValue(topic: \"sensor/temperature\") { topic payload timestamp } }"
  }'

curl -X POST http://localhost:4000/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { publish(input: { topic: \"test/topic\", payload: \"Hello MQTT\", format: TEXT }) { success timestamp } }"
  }'
```

## Date/Time Formats

All ISO‑8601 variants supported, e.g.:

```text
2024-01-01T10:30:00Z
2024-01-01T10:30:00+01:00
2024-01-01T10:30:00.123Z
```

## Data Formats

Three API data formats are available:

- `JSON`: decodes payload bytes as UTF-8 and returns a string marked `JSON`. It does not verify JSON syntax or return a structured GraphQL object; parse it in the client when appropriate.
- `TEXT`: reads/writes UTF-8 text; use it for plain strings and numeric sensor payloads. `JSON` and `TEXT` are not reliable binary detectors.
- `BINARY`: reads/writes Base64-encoded bytes.

Publishing `JSON` and `TEXT` currently converts the input string to UTF-8 bytes; `JSON` does not perform full JSON validation. Query timestamps are Unix epoch milliseconds. Historical query boundaries use ISO 8601 instants with a timezone.

## Migration Notes

Use `topicUpdates(topicFilters: [...])` for both single and multiple topic filters.
The former `multiTopicUpdates` field and singular device lookup fields are absent
from the schema. Device lookups now use a list query with a `name` filter. Management
operations are grouped beneath `user`, `archiveGroup`, and the respective device
mutation fields; see the linked guides for exact input shapes.
