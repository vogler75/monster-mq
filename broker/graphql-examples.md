# MonsterMQ GraphQL API Examples

## Configuration

Add the following to your `config.yaml` to enable the GraphQL server:

```yaml
GraphQL:
  Enabled: true
  Port: 8080
  Path: /graphql
  CorsEnabled: true
```

## GraphQL Endpoints

- **GraphQL HTTP**: `http://localhost:8080/graphql`
- **GraphQL WebSocket** (for subscriptions): `ws://localhost:8080/graphqlws`
- **Health Check**: `http://localhost:8080/health`

## Query Examples

### Get Current Value of a Topic

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

### Get Current Values with Topic Filter

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

### Get Retained Message

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

### Get All Retained Messages

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

### Query Archived Messages

```graphql
query GetArchivedMessages {
  archivedMessages(
    topicFilter: "sensor/#"
    startTime: "2024-01-01T00:00:00Z"  # ISO-8601 datetime string
    endTime: "2024-01-02T00:00:00Z"
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

#### Alternative DateTime Formats

All these ISO-8601 formats are supported:

```graphql
# UTC time
startTime: "2024-01-01T10:30:00Z"

# With timezone offset
startTime: "2024-01-01T10:30:00+01:00"

# With milliseconds
startTime: "2024-01-01T10:30:00.123Z"

# Query messages from last hour
query RecentMessages {
  archivedMessages(
    topicFilter: "sensor/temperature"
    startTime: "2024-01-01T09:00:00Z"
    endTime: "2024-01-01T10:00:00Z"
  ) {
    topic
    payload
    timestamp
  }
}
```

## Mutation Examples

### Publish a Message

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

### Publish Binary Data (Base64 Encoded)

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
    {
      topic: "sensor/temperature"
      payload: "{\"value\": 23.5}"
      format: JSON
      qos: 1
    },
    {
      topic: "sensor/humidity"
      payload: "{\"value\": 65}"
      format: JSON
      qos: 1
    }
  ]) {
    success
    topic
    timestamp
    error
  }
}
```

## Subscription Examples

### Subscribe to Topic Updates

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

### Subscribe to Multiple Topics

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

## Using with curl

### Query Example

```bash
curl -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query { currentValue(topic: \"sensor/temperature\") { topic payload timestamp } }"
  }'
```

### Mutation Example

```bash
curl -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { publish(input: { topic: \"test/topic\", payload: \"Hello MQTT\", format: JSON }) { success timestamp } }"
  }'
```

## Using with GraphQL Playground

You can use any GraphQL client like GraphQL Playground, Insomnia, or Postman to interact with the API. Simply point to `http://localhost:8080/graphql` as your endpoint.

## Data Formats

The API supports two data formats:

- **JSON**: Payload is treated as JSON string. The API will attempt to validate it as JSON.
- **BINARY**: Payload is Base64 encoded binary data. Use this for non-text data like images, protocol buffers, etc.

When querying, if you request JSON format but the data is binary, it will automatically be returned as Base64 encoded with format set to BINARY.

### Get MQTT Client Metrics

```graphql
query GetMqttClientMetrics {
  mqttClients {
    name
    metrics { messagesIn messagesOut timestamp }
  }
}
```

### Get MQTT Client Metrics History (Last 60 Minutes)

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
