# MCP Server

MonsterMQ includes a Model Context Protocol (MCP) server that enables AI models to interact with MQTT data. The MCP server provides tools for querying archived messages, retrieving current values, and analyzing IoT data patterns.

## Overview

The MCP (Model Context Protocol) server allows AI assistants like Claude to:
- **Query Historical Data** - Access archived MQTT messages
- **Get Current Values** - Retrieve latest values for topics
- **Analyze Patterns** - Identify trends and anomalies in IoT data
- **Generate Insights** - Provide real-time analytics and predictions

## Quick Start

### Enable MCP Server

```yaml
# config.yaml
MCP:
  Enabled: true
  Port: 3000

# Required: Must have an archive group named "Default"
ArchiveGroups:
  - Name: Default
    Filter: "#"  # Archive all topics
    Enabled: true
```

### Connect with Claude Desktop

Add to Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "monstermq": {
      "command": "npx",
      "args": [
        "-y",
        "@modelcontextprotocol/server-monstermq"
      ],
      "env": {
        "MCP_PORT": "3000",
        "MCP_HOST": "localhost"
      }
    }
  }
}
```

### Direct MCP Connection

```bash
# Using MCP CLI
mcp connect http://localhost:3000

# Using WebSocket
wscat -c ws://localhost:3000/mcp
```

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  AI Model   │────▶│  MCP Server  │────▶│  MonsterMQ  │
│  (Claude)   │     │  Port 3000   │     │   Broker    │
└─────────────┘     └──────────────┘     └──────┬──────┘
                                                 │
                                          ┌──────┴──────┐
                                          │   Archive   │
                                          │  Database   │
                                          └─────────────┘
```

## MCP Tools

The MCP server exposes the following tools to AI models:

### 1. find-topics-by-name

Search for topics using name patterns with wildcard support.

```typescript
interface FindTopicsByNameParams {
  name: string;            // Name pattern to search for
  ignoreCase?: boolean;    // Case insensitive search (default: true)
  namespace?: string;      // Optional topic prefix filter
}

// Example usage by AI
{
  "tool": "find-topics-by-name",
  "params": {
    "name": "*temperature*",
    "ignoreCase": true,
    "namespace": "sensors"
  }
}
```

### 2. find-topics-by-description

Search for topics by matching their description text using regex patterns.

```typescript
interface FindTopicsByDescriptionParams {
  description: string;     // Regex pattern for description search
  ignoreCase?: boolean;    // Case insensitive search (default: true)
  namespace?: string;      // Optional topic prefix filter
}

// Example usage by AI
{
  "tool": "find-topics-by-description",
  "params": {
    "description": ".*temperature.*sensor.*",
    "ignoreCase": true
  }
}
```

### 3. get-topic-value

Get current/recent values for one or more MQTT topics.

```typescript
interface GetTopicValueParams {
  topics: string[];        // Array of exact topic names
}

// Example usage by AI
{
  "tool": "get-topic-value",
  "params": {
    "topics": ["sensors/temperature/room1", "sensors/humidity/room1"]
  }
}
```

### 4. query-message-archive

Query historical MQTT messages for a specific topic within a time range.

```typescript
interface QueryMessageArchiveParams {
  topic: string;           // Exact topic name
  startTime?: string;      // ISO 8601 timestamp
  endTime?: string;        // ISO 8601 timestamp
  limit?: number;          // Max messages (default: 100)
}

// Example usage by AI
{
  "tool": "query-message-archive",
  "params": {
    "topic": "sensors/temperature",
    "startTime": "2024-01-01T00:00:00Z",
    "endTime": "2024-01-01T12:00:00Z",
    "limit": 1000
  }
}
```

### 5. query-message-archive-by-sql

Execute PostgreSQL queries against historical MQTT data for advanced analysis.

```typescript
interface QueryMessageArchiveBySqlParams {
  sql: string;             // PostgreSQL query string
}

// Example usage by AI
{
  "tool": "query-message-archive-by-sql",
  "params": {
    "sql": "SELECT date_trunc('hour', time) as hour, AVG((payload_json->>'temperature')::numeric) as avg_temp FROM defaultarchive WHERE topic = 'sensors/temp' AND time >= NOW() - INTERVAL '24 hours' GROUP BY hour ORDER BY hour"
  }
}
```

## Configuration

### Basic Configuration

```yaml
MCP:
  Enabled: true
  Port: 3000
  Host: "0.0.0.0"  # Bind address
  Path: "/mcp"     # WebSocket path

# Archive configuration (required)
ArchiveGroups:
  - Name: Default
    Filter: "#"
    Enabled: true
    Store: POSTGRES  # Or MONGODB, CRATEDB
```

### Current Limitations

The MCP server implementation is basic and doesn't support advanced configuration options:

- **No authentication/security** - All requests are allowed
- **No rate limiting** - No built-in request throttling
- **No caching** - Queries hit the database directly
- **No custom timeouts** - Uses default Vert.x timeouts
- **Fixed port/host** - Configuration is minimal

The current implementation focuses on providing core MCP functionality for AI model integration.

## Use Cases

### 1. Anomaly Detection

AI models can detect anomalies in sensor data:

```python
# AI Assistant analyzing temperature data
async def detect_anomalies():
    # Get historical data
    messages = await mcp.call("query-message-archive", {
        "topic": "sensors/temperature",
        "startTime": "2024-01-01T00:00:00Z",
        "limit": 10000
    })

    # Analyze for anomalies
    values = [float(msg.payload) for msg in messages]
    mean = statistics.mean(values)
    stdev = statistics.stdev(values)

    anomalies = []
    for msg in messages:
        value = float(msg.payload)
        if abs(value - mean) > 3 * stdev:
            anomalies.append(msg)

    return anomalies
```

### 2. Predictive Maintenance

AI can predict equipment failures:

```javascript
// AI analyzing vibration patterns
const analyzeVibration = async () => {
  // First find vibration topics
  const topics = await mcp.call("find-topics-by-name", {
    name: "*vibration*",
    namespace: "machines"
  });

  // Get recent vibration data for each topic
  const data = await Promise.all(
    topics.map(topic =>
      mcp.call("query-message-archive", {
        topic: topic,
        startTime: new Date(Date.now() - 7*24*60*60*1000).toISOString(),
        limit: 1000
      })
    )
  );

  // Identify patterns indicating wear
  const patterns = detectWearPatterns(data);

  // Predict maintenance needs
  return predictMaintenanceSchedule(patterns);
};
```

### 3. Real-time Monitoring

AI provides intelligent monitoring:

```typescript
// AI monitoring production line
async function monitorProduction() {
  // Get current values from all stations
  const topics = ['production/station1/status', 'production/station2/status', 'production/station3/status'];

  const currentStatus = await mcp.call("get-topic-value", {
    topics: topics
  });

  // Analyze production flow
  const bottlenecks = identifyBottlenecks(currentStatus);
  const efficiency = calculateEfficiency(currentStatus);

  return {
    bottlenecks,
    efficiency,
    recommendations: generateOptimizations(currentStatus)
  };
}
```

### 4. Energy Optimization

AI optimizes energy consumption:

```python
# AI analyzing energy usage patterns
async def optimize_energy():
    # Get power consumption data using SQL for complex analysis
    power_data = await mcp.call("query-message-archive-by-sql", {
        "sql": """
        SELECT topic, time, (payload_json->>'consumption')::numeric as consumption
        FROM defaultarchive
        WHERE topic LIKE 'energy/%/consumption'
        AND time >= '2024-01-01T00:00:00Z'
        ORDER BY time
        """
    })

    # Identify peak usage patterns
    peaks = identify_peak_periods(power_data)

    # Generate optimization strategy
    strategy = {
        "shift_loads": calculate_load_shifting(peaks),
        "reduce_peaks": suggest_peak_reduction(peaks),
        "estimated_savings": calculate_savings(peaks)
    }

    return strategy
```

## Integration Examples

### Claude Desktop Integration

1. **Install MCP Server:**
```bash
npm install -g @modelcontextprotocol/server-monstermq
```

2. **Configure Claude Desktop:**
```json
{
  "mcpServers": {
    "monstermq-local": {
      "command": "mcp-monstermq",
      "args": ["--port", "3000", "--host", "localhost"]
    }
  }
}
```

3. **Use in Claude:**
```
User: "Analyze the temperature trends from the last 24 hours"

Claude: I'll analyze the temperature data from your MQTT broker.
[Uses GetArchivedMessages tool]
Based on the data, I can see that temperatures peaked at 2 PM...
```

### Python Client

```python
import asyncio
from mcp import Client

class MonsterMQAnalyzer:
    def __init__(self, host="localhost", port=3000):
        self.client = Client(f"ws://{host}:{port}/mcp")

    async def analyze_topic(self, topic, hours=24):
        # Get recent messages
        messages = await self.client.call("GetArchivedMessages", {
            "topic": topic,
            "from": (datetime.now() - timedelta(hours=hours)).isoformat(),
            "limit": 10000
        })

        # Perform analysis
        stats = self.calculate_statistics(messages)
        trends = self.identify_trends(messages)
        anomalies = self.detect_anomalies(messages)

        return {
            "statistics": stats,
            "trends": trends,
            "anomalies": anomalies
        }
```

### Node.js Client

```javascript
const { MCPClient } = require('@modelcontextprotocol/client');

class MQTTAnalyzer {
  constructor() {
    this.mcp = new MCPClient('ws://localhost:3000/mcp');
  }

  async analyzeDeviceHealth(deviceId) {
    // Get device metrics
    const metrics = await this.mcp.call('GetArchivedMessages', {
      topic: `devices/${deviceId}/+`,
      from: new Date(Date.now() - 24*60*60*1000).toISOString()
    });

    // Analyze health indicators
    const health = this.calculateHealthScore(metrics);
    const issues = this.identifyIssues(metrics);

    return { health, issues };
  }
}
```

## Security

### Authentication

```yaml
MCP:
  Authentication:
    Enabled: true
    Method: bearer  # or basic, oauth2
    Token: "${MCP_AUTH_TOKEN}"  # Environment variable
```

### TLS/SSL

```yaml
MCP:
  TLS:
    Enabled: true
    CertFile: "/path/to/cert.pem"
    KeyFile: "/path/to/key.pem"
    CAFile: "/path/to/ca.pem"
```

### Access Control

```yaml
MCP:
  ACL:
    Enabled: true
    Rules:
      - Tool: "GetArchivedMessages"
        Topics: ["public/+", "sensors/+"]
        Allow: true
      - Tool: "GetArchivedMessages"
        Topics: ["private/+"]
        Allow: false
```

## Performance Tuning

### Query Optimization

```yaml
MCP:
  QueryOptimization:
    # Parallel query execution
    ParallelQueries: true
    MaxParallel: 4

    # Query caching
    CacheEnabled: true
    CacheTTL: 300

    # Result limiting
    MaxResultSize: 10000
    DefaultLimit: 100
```

### Connection Pooling

```yaml
MCP:
  ConnectionPool:
    MinConnections: 2
    MaxConnections: 10
    IdleTimeout: 300000  # ms
    AcquireTimeout: 10000 # ms
```

## Monitoring

### Metrics

```yaml
MCP:
  Metrics:
    Enabled: true
    Endpoint: "/metrics"
    Include:
      - request_count
      - request_duration
      - active_connections
      - cache_hit_rate
      - error_rate
```

### Logging

```yaml
MCP:
  Logging:
    Level: INFO  # DEBUG, INFO, WARN, ERROR
    Format: json
    IncludeRequest: true
    IncludeResponse: false
    SlowQueryThreshold: 1000  # ms
```

### Health Check

```bash
# Check MCP server health
curl http://localhost:3000/health

# Response
{
  "status": "healthy",
  "version": "1.0.0",
  "uptime": 3600,
  "connections": 5,
  "archive_status": "connected"
}
```

## Troubleshooting

### Common Issues

1. **Connection refused:**
   - Verify MCP is enabled in config
   - Check port 3000 is not blocked
   - Ensure archive group "Default" exists

2. **No data returned:**
   - Verify archive is enabled and storing data
   - Check topic filters in archive configuration
   - Ensure time range includes data

3. **Timeout errors:**
   - Increase query timeout in configuration
   - Add indexes to archive database
   - Limit query result size

4. **Authentication failures:**
   - Verify token/credentials
   - Check authentication is configured correctly
   - Ensure client sends auth headers

### Debug Mode

```yaml
MCP:
  Debug:
    Enabled: true
    LogRequests: true
    LogResponses: true
    TraceSql: true
```

## Best Practices

1. **Use appropriate time ranges** - Query only needed data
2. **Implement caching** - Cache frequently accessed data
3. **Set rate limits** - Prevent overload from AI models
4. **Monitor performance** - Track query times and optimize
5. **Secure connections** - Use TLS in production
6. **Archive strategically** - Only archive needed topics
7. **Index properly** - Add database indexes for common queries
8. **Test AI interactions** - Validate AI tool usage patterns

## API Reference

### WebSocket Protocol

```javascript
// Connect
const ws = new WebSocket('ws://localhost:3000/mcp');

// Send request
ws.send(JSON.stringify({
  id: "123",
  method: "GetArchivedMessages",
  params: {
    topic: "sensors/+",
    limit: 100
  }
}));

// Receive response
ws.on('message', (data) => {
  const response = JSON.parse(data);
  if (response.id === "123") {
    console.log('Messages:', response.result);
  }
});
```

### Error Codes

| Code | Description |
|------|-------------|
| 1001 | Invalid parameters |
| 1002 | Authentication required |
| 1003 | Permission denied |
| 1004 | Topic not found |
| 1005 | Query timeout |
| 1006 | Archive not available |
| 1007 | Rate limit exceeded |