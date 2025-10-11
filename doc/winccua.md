# WinCC Unified Integration

**Modern SCADA Integration via GraphQL**

MonsterMQ includes a WinCC Unified client that connects to Siemens WinCC Unified systems using their GraphQL API over WebSocket for real-time tag values and alarm notifications.

## Key Features

- **GraphQL Subscriptions** - Real-time tag value updates via WebSocket
- **Active Alarms** - Subscribe to alarm notifications with full alarm details
- **Quality Information** - Optional OPC UA quality data in tag values
- **Flexible Filtering** - Name filters with wildcards (e.g., `HMI_*`, `TANK_*`) for tag subscriptions
- **Topic Transformation** - Convert tag names to MQTT topic hierarchies with regex support
- **Multiple Formats** - JSON with ISO timestamps, milliseconds, or raw values

## Configuration

WinCC Unified clients are **not configured in YAML files**. Instead, they are managed through:

1. **Web Dashboard** - Navigate to "WinCC UA Clients" in the sidebar
2. **GraphQL API** - Use mutations to create/update/delete clients

**Example GraphQL Mutation:**

```graphql
mutation CreateWinCCUaClient {
  winCCUaClient {
    create(input: {
      name: "winccua-plant1"
      namespace: "winccua/plant1"
      nodeId: "local"
      enabled: true
      config: {
        graphqlEndpoint: "http://winccua-server:4000/graphql"
        username: "admin"
        password: "password"
        messageFormat: JSON_ISO
        addresses: [
          {
            type: TAG_VALUES
            topic: "tags"
            nameFilters: ["HMI_*", "TANK_*"]
            includeQuality: true
          },
          {
            type: ACTIVE_ALARMS
            topic: "alarms"
            systemNames: ["System1"]
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

WinCC Unified client configurations are stored in the database and persist across restarts.

## Message Formats

- **JSON_ISO** - JSON with ISO 8601 timestamps
- **JSON_MS** - JSON with millisecond epoch timestamps
- **RAW** - Raw value without formatting

## Address Types

### TAG_VALUES

Subscribe to real-time tag value changes:

- **nameFilters** - Array of name patterns with wildcards (e.g., `["HMI_*", "TANK_*"]`)
- **includeQuality** - Include OPC UA quality information in the payload (optional)
- **topic** - MQTT topic prefix for publishing values

### ACTIVE_ALARMS

Subscribe to active alarm notifications:

- **systemNames** - Array of alarm system names to filter
- **topic** - MQTT topic prefix for publishing alarms

## Topic Mapping

Tag names are automatically transformed to MQTT topics:

- **Default**: `{namespace}/{topic}/{tagName}`
- **Example**: `winccua/plant1/tags/HMI_Tank1_Level`

## Management via Web Dashboard

1. Navigate to **WinCC UA Clients** in the sidebar
2. Click **+ New WinCC UA Client**
3. Configure:
   - Name (unique identifier)
   - Namespace (MQTT topic prefix)
   - Node assignment (for clustering)
   - GraphQL endpoint URL
   - Credentials (username/password)
   - Message format
4. Add addresses (tag values and/or alarms)
5. Save and enable

## Clustering Support

WinCC UA clients are cluster-aware:
- Each client is assigned to a specific node
- Only the assigned node runs the client
- Clients can be reassigned to different nodes
- Configuration is shared across the cluster via database

## Metrics

Real-time metrics available via GraphQL:

- **messagesIn** - Messages received from WinCC Unified per second
- **connected** - Connection status
- **timestamp** - Last update time

## Troubleshooting

### Connection Issues

1. Verify GraphQL endpoint URL is accessible
2. Check credentials (username/password)
3. Ensure WebSocket connectivity
4. Review logs for connection errors

### No Tag Values

1. Verify name filters match actual tag names in WinCC Unified
2. Check that tags are configured for GraphQL subscriptions
3. Confirm the client is enabled and connected

### Alarm Subscription Issues

1. Verify system names match alarm systems in WinCC Unified
2. Check alarm system configuration
3. Ensure alarms are active and triggering
