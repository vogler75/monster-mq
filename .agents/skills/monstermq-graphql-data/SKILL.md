---
name: monstermq-graphql-data
description: >
  Guide for querying MonsterMQ broker data, telemetry, metrics, sessions, live topic values,
  retained messages, historical message archives, time-series aggregations, ArchiveGroups, OPC UA node browsing,
  topic schemas, flow engine statuses, AI agent conversations, and real-time GraphQL subscriptions.
  Use this skill whenever you need to fetch data, monitor broker health, read topic values, query
  historical data, check available archive groups/backends, or subscribe to live MQTT updates from MonsterMQ via GraphQL.
  Trigger on "query topic", "get metrics", "read message archive", "browse topics", "check sessions",
  "get broker status", "list archive groups", "check archives", "archive stats", "GraphQL query", "GraphQL subscription",
  "opcua read", "aggregated messages", or any read-only/monitoring action on MonsterMQ.
---

# MonsterMQ GraphQL Data Retrieval AI Skill

This skill provides instructions, query schemas, parameter reference, and code examples for reading data, monitoring status, fetching telemetry, querying historical archives, discovering storage backends, and streaming real-time updates from MonsterMQ via the GraphQL API.

---

> [!IMPORTANT]
> **Archive Group Isolation & Parameter Requirement**:
> In MonsterMQ, topic values and historical messages are stored in isolated databases/tables per **ArchiveGroup**.
> When reading historical data (`archivedMessages`, `aggregatedMessages`), current values (`currentValue`, `currentValues`), or searching/browsing topics (`searchTopics`, `browseTopics`), you **MUST specify the `archiveGroup` argument** if your data is stored in a group other than `"Default"`.
>
> **Recommended 2-Step Data Access Pattern**:
> 1. **Discover Archives**: Run `archiveGroups` to find which group covers your target MQTT topic filter.
> 2. **Fetch Data**: Pass the discovered `archiveGroup` name (e.g. `archiveGroup: "Industrial"`) into `archivedMessages`, `aggregatedMessages`, or `currentValue`.

---

## 1. Connection & Endpoint Overview

- **HTTP GraphQL Endpoint**: `http://localhost:4000/graphql` (default port `4000`)
- **WebSocket Endpoint (Subscriptions)**: `ws://localhost:4000/graphql`
- **HTTP Method**: `POST`
- **Headers**:
  ```http
  Content-Type: application/json
  Authorization: Bearer <jwt-token>   # Required if Auth.UserStoreType / User Management is enabled
  ```

---

## 2. Broker Status & Telemetry Queries

### Current User (`currentUser`)
Returns authentication details for the requesting API connection.
```graphql
query GetCurrentUser {
  currentUser {
    username
    isAdmin
  }
}
```

### Broker Configuration (`brokerConfig`)
Returns the complete static configuration loaded from `config.yaml`.
```graphql
query GetBrokerConfig {
  brokerConfig {
    nodeId
    version
    clustered
    tcpPort
    wsPort
    tcpsPort
    wssPort
    sessionStoreType
    retainedStoreType
    configStoreType
    userManagementEnabled
    anonymousEnabled
    mcpEnabled
    prometheusEnabled
    i3xEnabled
    graphqlEnabled
    metricsEnabled
  }
}
```

### Node Metrics & Cluster Status (`broker`, `brokers`)
Query real-time throughput, active session counts, message bus rates, and node health.

```graphql
# Current node or specific node status
query GetBroker($nodeId: String) {
  broker(nodeId: $nodeId) {
    nodeId
    version
    userManagementEnabled
    isLeader
    isCurrent
    enabledFeatures
    metrics {
      messagesIn
      messagesOut
      nodeSessionCount
      clusterSessionCount
      queuedMessagesCount
      subscriptionCount
      messageBusIn
      messageBusOut
      mqttClientIn
      mqttClientOut
      opcUaClientIn
      opcUaClientOut
      winCCOaClientIn
      timestamp
    }
  }
}

# All nodes in cluster
query GetAllBrokers {
  brokers {
    nodeId
    isLeader
    isCurrent
    enabledFeatures
    metrics {
      messagesIn
      messagesOut
      clusterSessionCount
    }
  }
}
```

---

## 3. Archive Groups & Storage Discovery Queries

Before querying historical messages or time-series aggregations, you should discover which Archive Groups, storage backends, database connections, and topic scopes are available in the broker.

### Discover All Archive Groups (`archiveGroups`, `archiveGroup`)
Query all registered ArchiveGroups to inspect their topic filters, storage backends (`archiveType`, `lastValType`), connection status, and retention policies.

```graphql
query GetArchiveGroups($enabled: Boolean) {
  archiveGroups(enabled: $enabled) {
    name
    enabled
    deployed
    topicFilter        # Match this filter against your topic of interest!
    retainedOnly
    lastValType        # MEMORY, HAZELCAST, POSTGRES, CRATEDB, MONGODB, SQLITE
    archiveType        # POSTGRES, CRATEDB, MONGODB, SQLITE, NONE
    databaseConnectionName
    payloadFormat
    lastValRetention
    archiveRetention
    purgeInterval
    connectionStatus {
      nodeId
      messageArchive
      lastValueStore
      error
    }
  }
}

query GetSingleArchiveGroup($name: String!) {
  archiveGroup(name: $name) {
    name
    enabled
    topicFilter
    archiveType
    lastValType
    databaseConnectionName
    archiveRetention
  }
}
```

### Archive Volume & Daily Statistics (`archiveStats`)
Fetch storage statistics for an archive group, including the earliest recorded timestamp (`minTimestamp`) and daily historical message counts (`dailyCounts`).

```graphql
query GetArchiveStats($archiveGroup: String!, $startTime: String, $endTime: String) {
  archiveStats(
    archiveGroup: $archiveGroup,
    startTime: $startTime,
    endTime: $endTime
  ) {
    minTimestamp
    dailyCounts {
      date
      count
    }
  }
}
```

### Database Connections (`databaseConnections`, `databaseConnectionNames`, `databaseConnection`)
Inspect configured database connections backing the message archives (PostgreSQL, MongoDB, SQLite).

```graphql
query GetDatabaseConnections {
  databaseConnections {
    name
    type
    url
    database
    schema
  }
  databaseConnectionNames(type: POSTGRES)
}
```

---

## 4. Topic Values & Live Telemetry Queries

### Current Topic Value (`currentValue`)
Retrieves the most recent message for an exact topic from the `LastValueStore`.
- **`topic`**: Exact topic string (no wildcards allowed).
- **`format`**: `JSON` or `BINARY` (base64).
- **`archiveGroup`**: Name of the ArchiveGroup containing the last value store (default: `"Default"`).

```graphql
query GetCurrentValue($topic: String!, $archiveGroup: String = "Default") {
  currentValue(topic: $topic, format: JSON, archiveGroup: $archiveGroup) {
    topic
    payload
    format
    timestamp
    qos
    retained
  }
}
```

### Multi-Topic Values (`currentValues`)
Retrieves the most recent messages matching an MQTT wildcard filter.
- **`topicFilter`**: MQTT filter with `+` (single-level) or `#` (multi-level). E.g. `"factory/+/temperature"`.
- **`limit`**: Max records to return (default: `100`).
- **`archiveGroup`**: Target ArchiveGroup name.

```graphql
query GetCurrentValues($filter: String!, $archiveGroup: String = "Default", $limit: Int = 100) {
  currentValues(topicFilter: $filter, format: JSON, limit: $limit, archiveGroup: $archiveGroup) {
    topic
    payload
    timestamp
    qos
  }
}
```

### Topic Discovery & Hierarchical Browsing (`searchTopics`, `browseTopics`)

> Note: Topic search and browsing operate on the `LastValueStore` of the specified `archiveGroup`.

```graphql
# Search topics using SQL LIKE pattern (% and _)
query SearchTopics($pattern: String!, $archiveGroup: String = "Default") {
  searchTopics(pattern: $pattern, limit: 50, archiveGroup: $archiveGroup)
}

# Browse topic hierarchy one level down
query BrowseTopics($topic: String!, $archiveGroup: String = "Default") {
  browseTopics(topic: $topic, archiveGroup: $archiveGroup) {
    name
    hasChildren
    messageCount
  }
}
```

### Retained Messages (`retainedMessage`, `retainedMessages`)
Query messages published with `retain=true`.

```graphql
query GetRetainedMessages($filter: String) {
  retainedMessages(topicFilter: $filter, format: JSON, limit: 100) {
    topic
    payload
    timestamp
    qos
  }
}
```

---

## 5. Historical Message Archives & Analytics Queries

> [!IMPORTANT]
> Always specify `archiveGroup` when querying history. Different archive groups can be backed by different databases (e.g. PostgreSQL vs CrateDB vs MongoDB).

### Raw Message History (`archivedMessages`)
Queries historical MQTT messages matching a time window and topic filter.
- **`topicFilter`**: MQTT wildcard topic filter (e.g. `"sensors/#"`).
- **`archiveGroup`**: Name of the target ArchiveGroup (default `"Default"`).
- **`startTime` / `endTime`**: ISO 8601 strings (e.g. `"2026-07-28T00:00:00Z"`).
- **`includeTopic`**: `Boolean` (set to `false` for performance when querying a single topic).

```graphql
query GetArchivedMessages(
  $topicFilter: String!,
  $archiveGroup: String = "Default",
  $startTime: String,
  $endTime: String,
  $limit: Int = 100
) {
  archivedMessages(
    topicFilter: $topicFilter,
    archiveGroup: $archiveGroup,
    startTime: $startTime,
    endTime: $endTime,
    format: JSON,
    limit: $limit
  ) {
    topic
    payload
    timestamp
    qos
  }
}
```

### Time-Series Aggregations (`aggregatedMessages`)
Performs database-level bucketing and aggregation across archived messages.
- **`topics`**: Array of exact topic strings.
- **`archiveGroup`**: ArchiveGroup to perform aggregation on (default `"Default"`).
- **`interval`**: `SECOND_1`, `SECOND_5`, `SECOND_10`, `MINUTE_1`, `MINUTE_5`, `MINUTE_15`, `HOUR_1`, `DAY_1`.
- **`functions`**: `AVG`, `MIN`, `MAX`, `SUM`, `COUNT`, `FIRST`, `LAST`.
- **`fields`**: Optional JSON path fields (e.g. `["temperature", "humidity"]`).

```graphql
query GetAggregatedMessages(
  $topics: [String!]!,
  $archiveGroup: String = "Default",
  $interval: AggregationInterval!,
  $startTime: String!,
  $endTime: String!
) {
  aggregatedMessages(
    topics: $topics,
    archiveGroup: $archiveGroup,
    interval: $interval,
    startTime: $startTime,
    endTime: $endTime,
    functions: [AVG, MAX, MIN],
    fields: ["value"]
  ) {
    timestamps
    topics {
      topic
      field
      function
      values
    }
  }
}
```

---

## 6. Client Sessions & System Logs

### Active Sessions (`sessions`, `session`)
```graphql
query GetSessions($connected: Boolean, $clientIdFilter: String) {
  sessions(connected: $connected, clientId: $clientIdFilter) {
    clientId
    nodeId
    connected
    cleanSession
    subscriptions {
      topicFilter
      qos
    }
    metrics {
      messagesIn
      messagesOut
    }
  }
}

query GetSingleSession($clientId: String!) {
  session(clientId: $clientId) {
    clientId
    nodeId
    connected
    cleanSession
    keepAlive
    lastConnectedTime
  }
}
```

### System Logs (`systemLogs`)
Inspect in-memory circular log buffer with filtering.
```graphql
query GetSystemLogs($lastMinutes: Int, $level: [String!], $message: String) {
  systemLogs(
    lastMinutes: $lastMinutes,
    level: $level,
    message: $message,
    limit: 50,
    orderByTime: DESC
  ) {
    timestamp
    level
    logger
    message
    node
    sourceClass
    sourceMethod
  }
}
```

---

## 7. Device Connector & OPC UA Queries

### Device Configurations & Runtime Status
Fetch active bridge and connector configurations:
- `mqttClientConfigs`, `opcUaClientConfigs`, `plc4xClientConfigs`, `winccOaClientConfigs`, `winccUaClientConfigs`
- `kafkaClientConfigs`, `natsClientConfigs`, `redisClientConfigs`, `telegramClientConfigs`, `neo4jClientConfigs`
- `opcUaServerConfigs`, `jdbcLoggerConfigs`, `influxDbLoggerConfigs`, `timeBaseLoggerConfigs`, `sparkplugBDecoderConfigs`

```graphql
query GetDeviceConfigs {
  opcUaClientConfigs {
    name
    endpointUrl
    enabled
    nodeId
    connectState
    subscriptions {
      nodeId
      mqttTopic
    }
  }
  plc4xClientConfigs {
    name
    connectionString
    enabled
    connectState
  }
}
```

### Live OPC UA Browsing & Reading (`opcUaBrowse`, `opcUaRead`)
```graphql
# Browse OPC UA address space
query BrowseOpcUaNode($name: String!, $nodePath: String) {
  opcUaBrowse(name: $name, nodePath: $nodePath) {
    nodeId
    displayName
    browseName
    nodeClass
    hasChildren
  }
}

# Read OPC UA tag value directly
query ReadOpcUaNode($name: String!, $nodePath: String!) {
  opcUaRead(name: $name, nodePath: $nodePath) {
    value
    statusCode
    serverTimestamp
  }
}
```

---

## 8. Flow Engine, AI Agents & Topic Schemas

### Flow Engine Status (`flowClasses`, `flowInstances`)
```graphql
query GetFlows {
  flowClasses {
    name
    namespace
    version
    description
  }
  flowInstances {
    name
    flowClassId
    nodeId
    enabled
    status {
      running
      lastExecution
      executionCount
      errorCount
      lastError
    }
  }
}
```

### AI Agents (`agents`, `agent`)
```graphql
query GetAgents {
  agents {
    name
    namespace
    nodeId
    enabled
    provider
    model
    inputTopics
    outputTopics
  }
}
```

### Topic Schemas (`topicSchemas`, `topicSchema`)
```graphql
query GetTopicSchemas {
  topicSchemas {
    topicFilter
    jsonSchema
    description
  }
}
```

---

## 9. Real-Time GraphQL Subscriptions

MonsterMQ supports WebSocket subscriptions for live streaming data (`ws://localhost:4000/graphql`).

### Real-Time Topic Updates (`topicUpdates`)
```graphql
subscription OnTopicUpdates($filters: [String!]!) {
  topicUpdates(topicFilters: $filters, format: JSON) {
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

### Bulk Batch Updates (`topicUpdatesBulk`)
```graphql
subscription OnTopicUpdatesBulk($filters: [String!]!) {
  topicUpdatesBulk(topicFilters: $filters, format: JSON, timeoutMs: 500, maxSize: 50) {
    count
    timestamp
    updates {
      topic
      payload
      timestamp
    }
  }
}
```

### Real-Time System Log Streaming (`systemLogs`)
```graphql
subscription OnSystemLogs($level: [String!]) {
  systemLogs(level: $level) {
    timestamp
    level
    logger
    message
    node
  }
}
```

---

## 10. End-to-End Client Implementation Example

### Complete Python Archive Discovery & Historical Query Pattern
```python
import requests

GRAPHQL_URL = "http://localhost:4000/graphql"
headers = {"Content-Type": "application/json"}

# Step 1: Discover available Archive Groups & match target topic filter
archive_query = """
query DiscoverArchives {
  archiveGroups(enabled: true) {
    name
    topicFilter
    archiveType
    lastValType
  }
}
"""
res = requests.post(GRAPHQL_URL, json={"query": archive_query}, headers=headers).json()
groups = res["data"]["archiveGroups"]
print("Available Archive Groups:")
for g in groups:
    print(f" - Group: {g['name']} | Filters: {g['topicFilter']} | Store: {g['archiveType']}")

# Assume target topic "opcua/plc1/temperature" matches group "Industrial"
target_group = "Industrial"

# Step 2: Query historical messages specifying the discovered archive group
history_query = """
query GetHistory($topicFilter: String!, $archiveGroup: String!) {
  archivedMessages(
    topicFilter: $topicFilter,
    archiveGroup: $archiveGroup,
    limit: 50
  ) {
    topic
    payload
    timestamp
  }
}
"""
params = {
    "topicFilter": "opcua/plc1/temperature",
    "archiveGroup": target_group
}

history_res = requests.post(GRAPHQL_URL, json={"query": history_query, "variables": params}, headers=headers).json()
print("Historical Messages:", history_res["data"]["archivedMessages"])
```

### Curl Example with ArchiveGroup Argument
```bash
curl -X POST http://localhost:4000/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query GetHistory($filter: String!, $group: String!) { archivedMessages(topicFilter: $filter, archiveGroup: $group, limit: 10) { topic payload timestamp } }",
    "variables": {
      "filter": "telemetry/#",
      "group": "Industrial"
    }
  }'
```
