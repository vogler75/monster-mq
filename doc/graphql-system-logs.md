# GraphQL System Logs Subscription

The MonsterMQ GraphQL API provides a real-time subscription for streaming system logs with advanced filtering capabilities.

## Overview

The `systemLogs` subscription allows you to monitor broker logs in real-time via GraphQL WebSocket subscriptions. Logs arrive on a dedicated internal event-bus address and are filtered on the server side. MQTT log publishing is no longer used.

## Subscription Query

```graphql
subscription Logs($node: String = "+", $levels: [String!], $logger: String,
                  $thread: Long, $sourceClass: String, $sourceMethod: String,
                  $message: String) {
  systemLogs(node: $node, level: $levels, logger: $logger, thread: $thread,
             sourceClass: $sourceClass, sourceMethod: $sourceMethod, message: $message) {
    timestamp
    level
    logger
    message
    thread
    node
    sourceClass
    sourceMethod
    parameters
    exception { class message stackTrace }
  }
}
```

## Filter Parameters

### Node and Level Filters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `node` | String | `"+"` | Node identifier (use `+` for all nodes, or specific node ID) |
| `level` | [String!] | all levels | Log level (use `+` for all levels, or `INFO`, `WARNING`, `SEVERE`) |

### Field Filters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `logger` | String | null | Logger name (supports regex matching) |
| `thread` | Long | null | Thread ID (exact match) |
| `sourceClass` | String | null | Source class name (supports regex matching) |
| `sourceMethod` | String | null | Source method name (supports regex matching) |
| `message` | String | null | Log message (supports regex matching or substring) |

**Regex Support**: The `logger`, `sourceClass`, `sourceMethod`, and `message` filters support regular expressions. If the filter is a valid regex pattern, it will be used for matching; otherwise, it will perform exact match (or substring match for `message`).

## Response Fields

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | String | ISO 8601 timestamp of the log entry |
| `level` | String | Log level (`INFO`, `WARNING`, `SEVERE`) |
| `logger` | String | Logger name (typically class name) |
| `message` | String | Log message text |
| `thread` | Long | Thread ID where the log was generated |
| `node` | String | Node identifier where the log originated |
| `sourceClass` | String | Source class name (optional) |
| `sourceMethod` | String | Source method name (optional) |
| `parameters` | [String] | Log message parameters (optional) |
| `exception` | ExceptionInfo | Exception information if an error was logged (optional) |

### ExceptionInfo Type

| Field | Type | Description |
|-------|------|-------------|
| `class` | String | Exception class name |
| `message` | String | Exception message (optional) |
| `stackTrace` | String | Full stack trace |

## Examples

### Monitor All Logs

```graphql
subscription {
  systemLogs {
    timestamp
    level
    logger
    message
    node
  }
}
```

### Filter by Log Level

Monitor only warnings and severe errors:

```graphql
subscription {
  systemLogs(level: ["WARNING", "SEVERE"]) {
    timestamp
    level
    message
    exception {
      class
      message
      stackTrace
    }
  }
}
```

Or severe only:

```graphql
subscription {
  systemLogs(level: "SEVERE") {
    timestamp
    level
    logger
    message
    exception {
      class
      message
      stackTrace
    }
  }
}
```

### Filter by Logger (Regex)

Monitor logs from all GraphQL-related classes:

```graphql
subscription {
  systemLogs(logger: ".*graphql.*") {
    timestamp
    level
    logger
    message
  }
}
```

### Filter by Source Class (Regex)

Monitor logs from SessionHandler:

```graphql
subscription {
  systemLogs(sourceClass: ".*SessionHandler") {
    timestamp
    level
    sourceClass
    sourceMethod
    message
  }
}
```

### Filter by Message (Regex)

Monitor connection-related logs:

```graphql
subscription {
  systemLogs(message: ".*(connect|disconnect).*") {
    timestamp
    level
    message
    node
  }
}
```

### Filter by Specific Node

Monitor logs from a specific broker node:

```graphql
subscription {
  systemLogs(node: "broker-node-1") {
    timestamp
    level
    logger
    message
  }
}
```

### Combined Filters

Monitor severe errors from specific classes with exception details:

```graphql
subscription {
  systemLogs(
    level: "SEVERE"
    sourceClass: ".*(Session|Topic).*"
  ) {
    timestamp
    level
    logger
    message
    sourceClass
    sourceMethod
    exception {
      class
      message
      stackTrace
    }
  }
}
```

## Configuration Requirements

Enable system-wide capture and the bounded log buffer explicitly:

```yaml
Logging:
  Memory:
    Enabled: true
    Entries: 1000
```

Logger levels determine which records are generated. The buffer is in-memory and does not survive restarts.

## Technical Details

- **Transport**: GraphQL subscriptions use `graphql-transport-ws` at `/graphql` by default. See [GraphQL authentication limits](graphql.md#subscriptions) before exposing the WebSocket endpoint.
- **Message Format**: Logs are published to the dedicated internal syslog event-bus address and forwarded to subscribed GraphQL clients
- **Filtering**: Server-side filtering reduces network traffic by only sending matching log entries
- **Regex Performance**: Regex patterns are compiled once when the subscription is created
- **Backpressure**: Uses reactive streams with request/cancel support for flow control

## Related Documentation

- [GraphQL API](graphql.md)
- [MQTT Logging](mqtt-logging.md)
- [Clustering](clustering.md)
