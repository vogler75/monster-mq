# Neo4j Graph Database Integration

**MQTT Topic Hierarchy as Graph Database**

MonsterMQ includes a native Neo4j client that automatically converts MQTT topic hierarchies into graph database structures, enabling powerful path-based queries and relationship analysis.

## Why Graph Database for MQTT?

MQTT topics naturally form hierarchical structures (e.g., `factory/building/floor/sensor/temperature`). Traditional relational databases struggle with these deep hierarchies, while Neo4j excels at:

- **Path Queries** - Find all sensors under a specific building or floor
- **Relationship Analysis** - Discover connections between devices and locations
- **Hierarchical Visualization** - Graph-based UI for topic exploration
- **Dynamic Schema** - No need to predefine topic structures

## Key Features

- **Automatic Topic Parsing** - MQTT topics are split into graph nodes (e.g., `input/Meter_Output/Value` → `input → Meter_Output → Value`)
- **Flexible Topic Filters** - Subscribe to specific topic patterns with MQTT wildcards (`#`, `+`)
- **Message Rate Limiting** - Configurable suppression to prevent database overload (min seconds between value changes per topic)
- **Batch Writing** - Efficient bulk inserts for high-throughput scenarios
- **Real-time Metrics** - Track messages in, messages written, suppressed count, and queue size

## Configuration

Neo4j clients are **not configured in YAML files**. Instead, they are managed through:

1. **Web Dashboard** - Navigate to "Neo4j Clients" in the sidebar
2. **GraphQL API** - Use mutations to create/update/delete clients

**Example GraphQL Mutation:**

```graphql
mutation CreateNeo4jClient {
  neo4jClient {
    create(input: {
      name: "neo4j-factory"
      namespace: "factory"
      nodeId: "local"
      enabled: true
      config: {
        url: "bolt://localhost:7687"
        username: "neo4j"
        password: "password"
        topicFilters: ["sensors/#", "machines/+/status"]
        queueSize: 10000
        batchSize: 100
        reconnectDelayMs: 5000
        maxChangeRateSeconds: 5  # Suppress updates faster than 5 seconds per topic
      }
    }) {
      success
      errors
      client { name }
    }
  }
}
```

Neo4j client configurations are stored in the database and persist across restarts.

## Configuration Parameters

### Connection Settings

- **url** - Neo4j Bolt connection URL (e.g., `bolt://localhost:7687`, `neo4j://localhost:7687`)
- **username** - Neo4j database username
- **password** - Neo4j database password

### Topic Subscription

- **topicFilters** - Array of MQTT topic patterns to subscribe to
  - One filter per line in the UI
  - Supports MQTT wildcards: `#` (multi-level) and `+` (single-level)
  - Examples: `sensors/#`, `factory/+/temperature`, `machines/+/status`

### Performance Tuning

- **queueSize** (default: 10000) - Size of the internal message queue
- **batchSize** (default: 100) - Number of messages to batch before writing to Neo4j
- **reconnectDelayMs** (default: 5000) - Delay in milliseconds before reconnecting on failure

### Rate Limiting

- **maxChangeRateSeconds** (default: 0) - Minimum seconds between value changes per topic
  - **0** - No suppression, all messages are written
  - **5** - Only write value changes if last update was >5 seconds ago (per topic)
  - **60** - Only write value changes once per minute (per topic)

This is particularly useful for high-frequency sensors that update multiple times per second but don't need every value stored in the database.

## Graph Data Model and Queries

The connector creates a namespace root and path nodes labeled `MqttNode`, with
case-sensitive properties `System`, `Path`, and `Name`. Leaf values have label
`MqttValue` and properties `System`, `NodeId`, `Name`, `Value`, `ServerTime`, and
`Topic`. Parent-to-child edges use `HAS`.

`System` is the connector's namespace. Query values using their full topic:

```cypher
MATCH (n:MqttValue {System: "factory", Topic: "sensors/temperature"})
RETURN n.Topic, n.Value, n.ServerTime
```

Inspect a subtree:

```cypher
MATCH path = (root:MqttNode {System: "factory", Path: "sensors"})-[:HAS*]->(leaf:MqttValue)
RETURN path
```

Find latest values under a prefix:

```cypher
MATCH (n:MqttValue {System: "factory"})
WHERE n.Topic STARTS WITH "factory/building1/"
RETURN n.Topic, n.Value, n.ServerTime
ORDER BY n.ServerTime DESC
LIMIT 10
```

Lowercase `nodeId`, `value`, and `timestamp` properties and a `CHILD` relationship
from older examples do not describe the current connector's graph.

## Management via Web Dashboard

1. Navigate to **Neo4j Clients** in the sidebar
2. Click **+ New Neo4j Client**
3. Configure:
   - Name (unique identifier)
   - Namespace (MQTT topic prefix)
   - Node assignment (for clustering)
   - Connection URL
   - Credentials (username/password)
   - Topic filters (one per line)
   - Performance settings
   - Rate limiting
4. Save and enable

## Clustering Support

Neo4j clients are cluster-aware:
- Each client is assigned to a specific node
- Only the assigned node runs the client
- Clients can be reassigned to different nodes
- Configuration is shared across the cluster via database

## Metrics

Real-time metrics available via GraphQL and web dashboard:

- **messagesIn** - MQTT input rate in persisted metrics (the live event-bus fallback counts the sampling interval)
- **messagesWritten** - Neo4j write rate in persisted metrics (the live event-bus fallback counts the sampling interval)
- **messagesSuppressed** - Messages suppressed due to rate limiting
- **errors** - Number of write errors
- **pathQueueSize** - Current size of the write queue
- **messagesInRate** - Messages per second received
- **messagesWrittenRate** - Messages per second written

## Performance Optimization

### Batch Writing

Increase `batchSize` for better throughput:
- Default: 100 messages per batch
- High-volume: 500-1000 messages per batch
- Consider Neo4j server capacity

### Rate Limiting for Database Protection

Use `maxChangeRateSeconds` to reduce load:
- High-frequency sensors: 1-5 seconds
- Medium-frequency sensors: 10-30 seconds
- Low-frequency monitoring: 60+ seconds

### Queue Management

Adjust `queueSize` based on message volume:
- Default: 10,000 messages
- High-volume: 50,000-100,000 messages
- Monitor `pathQueueSize` metric

## Troubleshooting

### Connection Issues

1. Verify Neo4j server is running and accessible
2. Check Bolt port (default: 7687)
3. Verify credentials
4. Review Neo4j server logs
5. Check firewall/network connectivity

### High Queue Size

If `pathQueueSize` keeps growing:
1. Increase `batchSize` to write faster
2. Enable rate limiting with `maxChangeRateSeconds`
3. Review topic filters to reduce message volume
4. Check Neo4j server performance

### Messages Not Written

1. Check `errors` metric for write failures
2. Verify Neo4j database permissions
3. Review MonsterMQ broker logs
4. Ensure topic filters are correct
5. Check if rate limiting is too aggressive

### Performance Issues

1. Increase `batchSize` for better throughput
2. Use rate limiting to reduce write frequency
3. Optimize Neo4j indexes
4. Monitor Neo4j server resources (CPU, memory, disk I/O)
5. Consider multiple Neo4j clients with different topic filters

## Best Practices

1. **Topic Organization** - Use consistent naming conventions for topics
2. **Rate Limiting** - Enable for high-frequency topics to prevent database overload
3. **Batch Sizing** - Balance between throughput and latency
4. **Queue Sizing** - Set based on peak message rates and batch processing time
5. **Index Optimization** - Create Neo4j indexes on frequently queried properties
6. **Monitoring** - Watch metrics to detect issues early
7. **Topic Filters** - Be specific to reduce unnecessary writes
