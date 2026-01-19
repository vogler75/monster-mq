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

MonsterMQ's MCP server uses HTTP transport (Streamable HTTP). Add to Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "monstermq": {
      "type": "url",
      "url": "http://localhost:3000/mcp"
    }
  }
}
```

### Direct MCP Connection

The MCP server endpoint is available at `http://<host>:<port>/mcp`:

- **POST /mcp** - Send JSON-RPC 2.0 requests and receive responses
- **GET /mcp** - Server-Sent Events (SSE) stream for real-time updates

```bash
# Example: Send initialize request
curl -X POST http://localhost:3000/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}'
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

## Topic Configuration and Metadata

### MCP Config Topic

MonsterMQ uses a special configuration topic suffix `<config>` to store metadata and descriptions for MQTT topics. This enables AI models to better understand the purpose and context of data streams.

#### How It Works

For any MQTT topic, you can publish a configuration message to `{topic}/<config>` containing metadata:

```bash
# Publish configuration for a temperature sensor
mosquitto_pub -t "sensors/temperature/room1/<config>" -m '{
  "description": "Temperature sensor in conference room 1",
  "unit": "°C",
  "type": "temperature",
  "location": "Conference Room 1",
  "range": {"min": 15, "max": 35}
}'

# Publish configuration for a humidity sensor
mosquitto_pub -t "sensors/humidity/office/<config>" -m '{
  "description": "Humidity monitoring for office environment",
  "unit": "%RH",
  "type": "humidity",
  "location": "Main Office",
  "critical_threshold": 80
}'
```

#### Configuration Format

The configuration message should be valid JSON and can contain any metadata fields:

```json
{
  "description": "Human-readable description of the topic",
  "unit": "Measurement unit (°C, %RH, ppm, etc.)",
  "type": "Data type or sensor category",
  "location": "Physical location description",
  "range": {"min": 0, "max": 100},
  "thresholds": {
    "warning": 75,
    "critical": 90
  },
  "tags": ["sensor", "environmental", "critical"],
  "owner": "maintenance-team",
  "installation_date": "2024-01-15"
}
```

#### MCP Integration

The MCP server uses these configuration topics to provide richer context to AI models:

1. **Enhanced Topic Discovery**: The `find-topics-by-description` tool searches through these configuration descriptions
2. **Contextual Information**: AI models receive topic metadata alongside data values
3. **Smart Filtering**: Configuration allows filtering by location, type, or other attributes

#### Best Practices

- **Use Descriptive Names**: Include clear, human-readable descriptions
- **Standardize Units**: Use consistent unit formats (°C, %RH, ppm)
- **Include Context**: Add location, installation date, and ownership information
- **Set Thresholds**: Define warning and critical thresholds for monitoring
- **Use Tags**: Add searchable tags for categorization

#### Example: Environmental Monitoring Setup

```bash
# Temperature sensors
mosquitto_pub -t "building/floor1/room101/temperature/<config>" -m '{
  "description": "Temperature sensor for server room environmental monitoring",
  "unit": "°C",
  "type": "temperature",
  "location": "Server Room 101, Floor 1",
  "critical_max": 25,
  "tags": ["environmental", "server-room", "critical"]
}'

# Humidity sensors
mosquitto_pub -t "building/floor1/room101/humidity/<config>" -m '{
  "description": "Humidity sensor for server room environmental monitoring",
  "unit": "%RH",
  "type": "humidity",
  "location": "Server Room 101, Floor 1",
  "critical_max": 60,
  "tags": ["environmental", "server-room", "critical"]
}'
```

This allows AI models to understand that these are critical environmental sensors in a server room and respond appropriately to threshold violations.