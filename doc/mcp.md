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