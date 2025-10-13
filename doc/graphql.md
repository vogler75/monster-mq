# GraphQL API

The GraphQL extension exposes MonsterMQ state and control operations over HTTP/WebSocket (`broker/src/main/kotlin/extensions/graphql/GraphQLServer.kt`). This page documents the configuration switches and the queries/mutations that exist today.

## Enabling the Server

```yaml
GraphQL:
  Enabled: true
  Port: 4000      # Defaults to 8080 when omitted
  Path: /graphql  # Defaults to /graphql
```

Once enabled the server listens on `http://<host>:<port><path>` and serves both the HTTP API and a WebSocket endpoint for subscriptions (`ws://<host>:<port><path>ws`).

## Authentication

- User management disabled? `login` returns `success: true` with `token = null` and no further checks are enforced (`broker/src/main/kotlin/extensions/graphql/AuthenticationResolver.kt:20-80`).
- User management enabled? Call the `login` mutation and supply the returned JWT in the `Authorization: Bearer <token>` header for subsequent requests.
- Admin-only mutations/queries (user management, ACLs, archive administration, etc.) are enforced through `GraphQLAuthContext` (`broker/src/main/kotlin/extensions/graphql/GraphQLServer.kt:205-356`).

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
  kafkaClient(name: "MyKafkaClient") {
    metricsHistory(lastMinutes: 30) { messagesIn messagesOut timestamp }
  }
}
```
Semantics:
- messagesIn: Kafka records/sec consumed (latest sample).
- messagesOut: MQTT messages/sec published (after transformation & filtering).
- Live fallback: If no persisted sample exists, resolver fetches a live point via the Vert.x event bus for immediate visibility.
- History requires a configured metrics store; otherwise only a single current sample is returned.

(Planned) Aggregate broker-level sums will surface as `kafkaClientIn` / `kafkaClientOut` fields in Broker metrics once implemented; for now derive aggregates client-side.


The top-level `Query` type exposes the following fields (`broker/src/main/resources/schema.graphqls:141-356`):

### OPC UA Device Metrics (Embedded Fields)

OPC UA device metrics are now accessed as embedded fields on `OpcUaDevice`:

```graphql
{
  opcUaDevice(name: "MyDevice") {
    name
    metrics { messagesIn messagesOut timestamp }
    metricsHistory(lastMinutes: 15) { messagesIn messagesOut timestamp }
  }
}
```

Previously exposed root queries `opcUaDeviceMetrics` and `opcUaDeviceMetricsHistory` have been removed. Use `OpcUaDevice.metrics` and `OpcUaDevice.metricsHistory` instead.

| Category | Field | Description |
|----------|-------|-------------|
| Current values | `currentValue(topic, archiveGroup)` | Latest retained value for a single topic. |
| | `currentValues(topicFilter, archiveGroup, limit)` | Latest values for topics matching the MQTT filter. |
| Retained messages | `retainedMessage(topic)` / `retainedMessages(topicFilter)` | Access the retained store. |
| Historical data | `archivedMessages(topicFilter, startTime, endTime, limit, archiveGroup)` | Query time-series data from archive groups. |
| Topic discovery | `searchTopics(pattern, archiveGroup)` | SQL-like wildcard search against topic names. |
| | `browseTopics(topic, archiveGroup)` | Browse one level of the topic tree. |
| Monitoring | `broker(nodeId)` / `brokers` | Cluster/node metrics and status. |
| | `sessions(nodeId, cleanSession, connected)` / `session(clientId, nodeId)` | MQTT session information. |
| User management | `users(username)` | List users and their ACL rules (admin only). |
| Archive groups | `archiveGroups` / `archiveGroup(name)` | Inspect archive configuration and connection status. |
| OPC UA client | `opcUaDevices`, `opcUaDevice`, `opcUaDevicesByNode`, `clusterNodes` | Available when a device config store is configured. |
| OPC UA server | `opcUaServers`, `opcUaServer`, `opcUaServersByNode`, `opcUaServerCertificates` | Available when the config store supports server records. |

## Mutations

Available mutations are wired in `GraphQLServer.buildRuntimeWiring()` (`broker/src/main/kotlin/extensions/graphql/GraphQLServer.kt:236-356`). Highlights:

| Category | Mutations |
|----------|-----------|
| Authentication | `login` |
| Publishing | `publish`, `publishBatch` |
| Queued messages | `purgeQueuedMessages(clientId)` |
| User management | `createUser`, `updateUser`, `deleteUser`, `setPassword` |
| ACL management | `createAclRule`, `updateAclRule`, `deleteAclRule` |
| Archive groups | `createArchiveGroup`, `updateArchiveGroup`, `deleteArchiveGroup`, `enableArchiveGroup`, `disableArchiveGroup` |
| OPC UA client | `addOpcUaDevice`, `updateOpcUaDevice`, `deleteOpcUaDevice`, `toggleOpcUaDevice`, `reassignOpcUaDevice`, `addOpcUaAddress`, `deleteOpcUaAddress` |
| OPC UA server | `createOpcUaServer`, `startOpcUaServer`, `stopOpcUaServer`, `deleteOpcUaServer`, `addOpcUaServerAddress`, `removeOpcUaServerAddress`, `trustOpcUaServerCertificates`, `deleteOpcUaServerCertificates` |

All user and archive mutations require an admin-level JWT. Publishing requires publish permission for the target topic (enforced through the ACL system).

## Subscriptions

Two WebSocket subscriptions stream MQTT data via the configured message bus (`broker/src/main/kotlin/extensions/graphql/GraphQLServer.kt:358-365`):

- `topicUpdates(topicFilter: String!)`
- `multiTopicUpdates(topicFilters: [String!]!)`

Authentication rules for subscriptions mirror the ones used for queries and mutations.

## Usage Notes

1. Payloads are returned according to the `DataFormat` argument (`JSON` or `BINARY`). JSON mode automatically parses payloads that contain valid JSON strings.
2. When user management is disabled you can still call `login`; it simply announces that authentication is off.
3. The OPC UA fields appear only when a device configuration store is available. If you run without a persistent config store those queries/mutations are absent from the schema at runtime.
4. The HTTP endpoint also serves the static dashboard under `/dashboard` (see `GraphQLServer.start()` for details).
5. Migration: Root queries `opcUaDeviceMetrics` and `opcUaDeviceMetricsHistory` were removed in favor of embedded fields on `OpcUaDevice` (`metrics` and `metricsHistory`). Update clients accordingly.
6. PayloadFormat enum: Supported values are `DEFAULT` and `JSON`. The former `JAVA` name has been removed; use `DEFAULT`.

## Endpoints

Unless overridden in configuration:

- HTTP GraphQL endpoint: `http://localhost:4000/graphql` (example port)
- WebSocket endpoint (subscriptions): `ws://localhost:4000/graphqlws` (path suffix `ws` is appended internally)
- Health endpoint: `http://localhost:4000/health`

Adjust host/port/path according to your `GraphQL` configuration block.

## Examples

The following end‑to‑end examples consolidate and supersede the legacy `broker/graphql-examples.md` file.

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
  topicUpdates(topicFilter: "sensor/+/temperature", format: JSON) {
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
  multiTopicUpdates(
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
  mqttClient(name: "bridge-client-1") {
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
    "query": "mutation { publish(input: { topic: \"test/topic\", payload: \"Hello MQTT\", format: JSON }) { success timestamp } }"
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

Two payload formats are supported when publishing / querying:

- JSON  – Payload treated (and validated) as JSON text
- BINARY – Base64 encoded arbitrary bytes (images, protobuf, etc.)

If a stored payload is binary but requested as JSON it is returned as Base64 with `format: BINARY`.

## Migration Notes

The standalone examples file `broker/graphql-examples.md` has been merged into this document for a single source of truth. Remove or stop referencing the old file to avoid drift.
