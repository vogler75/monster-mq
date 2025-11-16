# WinCC OA Integration

**High-Performance SCADA Data Transfer via SQL Queries**

MonsterMQ includes a native WinCC OA (Open Architecture) client that connects to Siemens WinCC OA SCADA systems using their GraphQL API over WebSocket, leveraging continuous SQL queries for efficient bulk data transfer.

## Key Features

- **Massive Scale** - Subscribe to millions of datapoints with minimal configuration
- **Continuous SQL Queries** - Leverage WinCC OA's `dpQueryConnectSingle` for efficient bulk updates
- **Real-time Streaming** - GraphQL subscription-based updates via WebSocket
- **Flexible Topic Mapping** - Transform WinCC OA datapoint names to MQTT topic hierarchies with regex support
- **Multiple Formats** - JSON with ISO timestamps, milliseconds, or raw values
- **Dynamic Column Support** - Support for any datapoint elements (value, timestamp, quality, etc.)
- **Topic Transformation** - Advanced datapoint name transformation with regex patterns

## Why WinCC OA SQL Queries?

The MQTT protocol doesn't support bulk messages, making it inefficient for transferring large numbers of topic value changes. MonsterMQ's WinCC OA client leverages WinCC OA's powerful **continuous SQL queries** (`dpQueryConnectSingle`), making it possible to **subscribe to 5 million tags with just a single SQL query**.

### Key Benefits

- **Efficient Bulk Transfer** - Process high-volume tag changes without MQTT per-message overhead
- **Single Query, Millions of Tags** - Use wildcards to subscribe to entire systems
- **Real-time Updates** - Continuous queries stream changes as they occur
- **Flexible Data Selection** - Choose which datapoint elements to retrieve (value, timestamp, quality, etc.)

## Configuration

WinCC OA clients are **not configured in YAML files**. Instead, they are managed through:

1. **Web Dashboard** - Navigate to "WinCC OA Clients" in the sidebar
2. **GraphQL API** - Use mutations to create/update/delete clients

**Example GraphQL Mutation:**

```graphql
mutation CreateWinCCOaClient {
  winCCOaClient {
    create(input: {
      name: "winccoa-plant1"
      namespace: "winccoa/plant1"
      nodeId: "local"
      enabled: true
      config: {
        graphqlEndpoint: "http://winccoa-server:4000/graphql"
        username: "admin"
        password: "password"
        messageFormat: JSON_ISO
        transformConfig: {
          removeSystemName: true
          convertDotToSlash: true
          convertUnderscoreToSlash: false
          regexPattern: null
          regexReplacement: null
        }
        addresses: [
          {
            query: "SELECT '_original.._value', '_original.._stime' FROM 'System1:*'"
            topic: "tags"
            description: "All datapoints in System1"
            answer: true
            retained: false
          },
          {
            query: "SELECT '_original.._value', '_original.._stime', '_original.._status64' FROM 'System1:Tank_*.'"
            topic: "tanks"
            description: "Tank datapoints with quality status"
            answer: false
            retained: true
          }
        ]
      }
    }) {
      success
      errors
      client { name }
    }
  }
}
```

WinCC OA client configurations are stored in the database and persist across restarts.

## Message Formats

- **JSON_ISO** - JSON with ISO 8601 timestamps
  ```json
  {"_original.._value": 42.5, "_original.._stime": "2025-01-16T10:30:00.123Z"}
  ```
- **JSON_MS** - JSON with millisecond epoch timestamps
  ```json
  {"_original.._value": 42.5, "_original.._stime": 1737023400123}
  ```
- **RAW_VALUE** - Raw value only (first column's value)
  ```
  42.5
  ```

## Address Configuration

Each address represents a WinCC OA continuous SQL query subscription.

### Address Fields

- **query** (required) - WinCC OA SQL query using `dpQueryConnectSingle` syntax
- **topic** (required) - MQTT topic prefix for publishing values
- **description** (optional) - Description of this subscription
- **answer** (optional, default: `false`) - Request initial values (answer row) from the query
- **retained** (optional, default: `false`) - Publish MQTT messages with retained flag

### SQL Query Examples

**Subscribe to all datapoints in a system:**
```sql
SELECT '_original.._value', '_original.._stime' FROM 'System1:*'
```

**Subscribe to specific datapoint pattern:**
```sql
SELECT '_original.._value', '_original.._stime' FROM 'System1:Tank_*.'
```

**Include quality status:**
```sql
SELECT '_original.._value', '_original.._stime', '_original.._status64' FROM 'System1:Sensor_*.'
```

**Multiple systems:**
```sql
SELECT '_original.._value', '_original.._stime' FROM '*:Production_*.'
```

### Column Selection

You can select any datapoint elements supported by WinCC OA:

- `_original.._value` - Current value
- `_original.._stime` - Timestamp
- `_original.._status64` - Quality status (64-bit)
- `_original.._manager` - Manager information
- Custom elements depending on your WinCC OA configuration

The selected columns will be included in the published MQTT message payload.

## Topic Transformation

WinCC OA datapoint names (e.g., `System1:Tank_01.Level`) are transformed to MQTT topics using configurable rules.

### Transformation Options

**removeSystemName** (default: `true`)
- Removes the system name prefix (e.g., `System1:`)
- `System1:Tank_01.Level` → `Tank_01.Level`

**convertDotToSlash** (default: `true`)
- Converts dots to slashes for MQTT topic hierarchy
- Trailing dots are automatically removed
- `Tank_01.Level.` → `Tank_01/Level`

**convertUnderscoreToSlash** (default: `false`)
- Converts underscores to slashes
- `Tank_01.Level` → `Tank/01.Level`

**regexPattern** and **regexReplacement** (optional)
- Apply custom regex transformations
- Example: Convert numbers to zero-padded format
  - Pattern: `([0-9]+)`
  - Replacement: `00$1`

### Transformation Example

**Input datapoint:** `System1:Tank_01.Level.`

**With default settings:**
```
removeSystemName: true
convertDotToSlash: true
convertUnderscoreToSlash: false
```
**Result:** `Tank_01/Level`

**Final MQTT topic:** `{namespace}/{topic}/{transformed}`
- Example: `winccoa/plant1/tags/Tank_01/Level`

## Topic Mapping

The final MQTT topic is composed as:

```
{namespace}/{topic}/{transformed_datapoint_name}
```

- **namespace** - Device namespace (e.g., `winccoa/plant1`)
- **topic** - Address-specific topic prefix (e.g., `tags`)
- **transformed_datapoint_name** - Transformed datapoint name

**Example:**
- Datapoint: `System1:Tank_01.Level.`
- Namespace: `winccoa/plant1`
- Topic: `tags`
- Transformed: `Tank_01/Level`
- **Final MQTT topic:** `winccoa/plant1/tags/Tank_01/Level`

## Authentication

WinCC OA clients support three authentication modes:

### 1. Username/Password (Login Mutation)
```graphql
config: {
  graphqlEndpoint: "http://winccoa-server:4000/graphql"
  username: "admin"
  password: "password"
}
```

The client performs a GraphQL login mutation to obtain a token.

### 2. Direct Token
```graphql
config: {
  graphqlEndpoint: "http://winccoa-server:4000/graphql"
  token: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

Use a pre-configured authentication token.

### 3. Anonymous Access
```graphql
config: {
  graphqlEndpoint: "http://winccoa-server:4000/graphql"
}
```

Omit username, password, and token for anonymous/open access.

## Connection Configuration

### graphqlEndpoint (required)
- HTTP(S) URL to WinCC OA GraphQL endpoint
- Example: `http://winccoa-server:4000/graphql`

### websocketEndpoint (optional)
- WebSocket URL for GraphQL subscriptions
- Defaults to `graphqlEndpoint` with `ws://` or `wss://` protocol
- Example: `ws://winccoa-server:4000/graphql`

### reconnectDelay (optional, default: 5000)
- Reconnection delay in milliseconds
- Minimum: 1000ms

### connectionTimeout (optional, default: 10000)
- Connection timeout in milliseconds
- Minimum: 1000ms

## Management via Web Dashboard

1. Navigate to **WinCC OA Clients** in the sidebar
2. Click **+ New WinCC OA Client**
3. Configure:
   - **Name** - Unique identifier
   - **Namespace** - MQTT topic prefix
   - **Node Assignment** - For clustering (assign to specific node or "local")
   - **GraphQL Endpoint** - WinCC OA server URL
   - **Credentials** - Username/password or token
   - **Message Format** - JSON_ISO, JSON_MS, or RAW_VALUE
   - **Transform Config** - Topic transformation rules
4. Add addresses (SQL queries)
   - **Query** - WinCC OA SQL query (SELECT ... FROM ...)
   - **Topic** - MQTT topic prefix for this query
   - **Description** - Optional description
   - **Answer** - Request initial values
   - **Retained** - Publish with retained flag
5. Save and enable

## Clustering Support

WinCC OA clients are cluster-aware:
- Each client is assigned to a specific node
- Only the assigned node runs the client
- Clients can be reassigned to different nodes via web dashboard or GraphQL
- Configuration is shared across the cluster via database
- Automatic failover if assigned node goes down (manual reassignment required)

## Metrics

Real-time metrics available via GraphQL:

```graphql
query {
  winCCOaClientMetrics(name: "winccoa-plant1") {
    device
    messagesInRate
    connected
    elapsedMs
  }
}
```

- **messagesInRate** - Messages received from WinCC OA per second
- **connected** - Connection status (true/false)
- **elapsedMs** - Time since last metrics reset

## WebSocket Protocol

The client uses the **graphql-transport-ws** subprotocol for WebSocket communication:

1. Connection initialization with `connection_init` message
2. Server responds with `connection_ack`
3. Client sends `subscribe` messages for each address
4. Server sends `next` messages with subscription data
5. Heartbeat via `ping`/`pong` messages

## Data Flow

1. **Connection** - Client connects to WinCC OA GraphQL WebSocket endpoint
2. **Authentication** - Login mutation or token-based authentication
3. **Subscription** - Client subscribes to configured SQL queries
4. **Data Reception** - WinCC OA sends continuous updates via WebSocket
5. **Transformation** - Datapoint names transformed to MQTT topics
6. **Publishing** - Values published to MQTT broker with configured QoS and retained flags

## Troubleshooting

### Connection Issues

1. Verify GraphQL endpoint URL is accessible
   ```bash
   curl http://winccoa-server:4000/graphql
   ```
2. Check credentials (username/password or token)
3. Ensure WebSocket connectivity (firewalls, proxies)
4. Review MonsterMQ logs for connection errors
5. Check WinCC OA server logs

### No Datapoint Values

1. Verify SQL query syntax is correct for WinCC OA
2. Test query in WinCC OA GEDI or control panel
3. Check datapoint names match wildcards in query
4. Ensure datapoints exist and are accessible
5. Verify user permissions for datapoints
6. Check that `answer: true` is set to receive initial values

### Topic Transformation Issues

1. Review transformation settings in client configuration
2. Check MonsterMQ logs for transformed topic names
3. Use MQTT client to subscribe to `{namespace}/#` and verify topics
4. Test regex patterns separately before applying
5. Ensure trailing dots/slashes don't create empty topic levels

### Performance Issues

1. Reduce number of datapoints per query (use more specific wildcards)
2. Limit selected columns to only needed elements
3. Adjust `messageFormat` to reduce payload size (use RAW_VALUE for simple values)
4. Monitor metrics via GraphQL to check message rates
5. Consider splitting large queries into multiple addresses
6. Check WinCC OA server performance and load

## Example Configurations

### Simple Temperature Monitoring

```graphql
addresses: [
  {
    query: "SELECT '_original.._value', '_original.._stime' FROM 'System1:Temperature_*'"
    topic: "sensors/temperature"
    description: "All temperature sensors"
    answer: true
    retained: false
  }
]
```

### Multi-System Production Monitoring

```graphql
addresses: [
  {
    query: "SELECT '_original.._value', '_original.._stime', '_original.._status64' FROM '*:Production_Line_*.'"
    topic: "production/lines"
    description: "All production lines across all systems"
    answer: true
    retained: true
  },
  {
    query: "SELECT '_original.._value', '_original.._stime' FROM '*:Alarm_*'"
    topic: "alarms"
    description: "All alarm datapoints"
    answer: false
    retained: false
  }
]
```

### Advanced Topic Transformation

```graphql
transformConfig: {
  removeSystemName: true
  convertDotToSlash: true
  convertUnderscoreToSlash: true
  regexPattern: "Line([0-9]+)"
  regexReplacement: "Line$1"
}
```

This configuration transforms:
- `System1:Production_Line_01.Status.`
- → `Production/Line/01/Status`

## Best Practices

1. **Use Wildcards Efficiently** - Balance between broad queries and performance
2. **Select Only Needed Columns** - Reduce payload size and processing overhead
3. **Enable Retained Messages** - For critical values that should persist
4. **Use Answer Flag Wisely** - Set `answer: true` to get initial values on connection
5. **Monitor Metrics** - Track message rates and connection status
6. **Test Queries First** - Validate SQL queries in WinCC OA before deploying
7. **Plan Topic Hierarchy** - Design topic transformation for your MQTT consumers
8. **Use Clustering** - Distribute clients across nodes for high availability
9. **Configure Reconnection** - Set appropriate reconnect delays for network conditions
10. **Log Analysis** - Enable fine logging for debugging, reduce for production

## Security Considerations

1. **Use HTTPS/WSS** - Secure GraphQL endpoint URLs in production
2. **Strong Passwords** - Use complex passwords for WinCC OA authentication
3. **Token Expiration** - Monitor token expiration and refresh as needed
4. **Network Isolation** - Run WinCC OA clients on trusted networks
5. **Access Control** - Limit WinCC OA user permissions to read-only access
6. **Audit Logging** - Monitor client activity via MonsterMQ and WinCC OA logs

## References

- [WinCC OA Documentation](https://www.winccoa.com/documentation/)
- [GraphQL Subscriptions](https://graphql.org/blog/subscriptions-in-graphql-and-relay/)
- [MQTT Topic Best Practices](https://www.hivemq.com/blog/mqtt-essentials-part-5-mqtt-topics-best-practices/)
