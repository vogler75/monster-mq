# OPC UA Client Write & Read Support (Bidirectional) — Issue #95

## Context

Issue #95: The OPC UA client previously only supported reading (OPC UA → MQTT). This adds bidirectional support: fire & forget writes, request/response writes, and on-demand reads.

## Implemented Design

### Two Independent Feature Toggles

- **`enabled`** — Fire & forget writes via `{namespace}/write/{nodeId}`
- **`requestResponseEnabled`** — Request/response read & write via `{namespace}/request/{nodeId}` → `{namespace}/response/{nodeId}`

### Topic Patterns

**Fire & Forget Write** (no response):
```
Topic:   {namespace}/write/{nodeId}
Payload: {"value": 42.5}  or  {"value": 42, "dataType": "Int16"}
```

**Request/Response Write** (with status):
```
Publish:  {namespace}/request/{nodeId}   payload: {"value": 42.5}
Response: {namespace}/response/{nodeId}  payload: {"nodeId":"...","status":"Good","statusCode":0,"timestamp":"..."}
```

**Request/Response Read** (on-demand value):
```
Publish:  {namespace}/request/{nodeId}   payload: {} (empty or no payload)
Response: {namespace}/response/{nodeId}  payload: {"nodeId":"...","value":42.5,"status":"Good","statusCode":0,"timestamp":"..."}
```

**Batch** (mixed read/write):
```
Publish:  {namespace}/request
Payload:  [{"nodeId":"ns=2;i=1001"}, {"nodeId":"ns=2;i=1002","value":42.5}]
Response: [{"nodeId":"ns=2;i=1001","value":23.1,"status":"Good",...}, {"nodeId":"ns=2;i=1002","status":"Good",...}]
```

Rule: entries without `value` are reads, entries with `value` are writes.

## Files Modified

| File | Changes |
|------|---------|
| `broker/src/main/kotlin/stores/devices/OpcUaConfig.kt` | `OpcUaWriteConfig` with `enabled`, `requestResponseEnabled`, topic prefixes, QoS, timeout |
| `broker/src/main/kotlin/devices/opcua/OpcUaConnector.kt` | Write subscriptions, read/write handler, batch with mixed read+write, type conversion, response publishing |
| `broker/src/main/resources/schema-mutations.graphqls` | `OpcUaWriteConfigInput` |
| `broker/src/main/resources/schema-queries.graphqls` | `OpcUaWriteConfig` type |
| `broker/src/main/kotlin/graphql/OpcUaClientConfigMutations.kt` | Parse/persist writeConfig |
| `broker/src/main/kotlin/graphql/OpcUaClientConfigQueries.kt` | Return writeConfig |
| `dashboard/src/pages/opcua-device-detail.html` | Write config section, two enable checkboxes, edit address modal |
| `dashboard/src/js/opcua-device-detail.js` | Query/populate/save writeConfig, topic reference docs, address edit, toast fixes |
| `doc/opcua-client.md` | Full write/read documentation |

## Verification

1. **Build**: `cd broker && mvn clean package`
2. **Fire & forget write**: Enable `writeConfig.enabled`, publish to `{namespace}/write/ns=2;i=1001` with `{"value": 42.5}`
3. **Request/response write**: Enable `writeConfig.requestResponseEnabled`, subscribe to `{namespace}/response/#`, publish to `{namespace}/request/ns=2;i=1001` with `{"value": 42.5}`
4. **On-demand read**: Publish to `{namespace}/request/ns=2;i=1001` with `{}`, verify value returned
5. **Batch**: Publish mixed array to `{namespace}/request`, verify reads return values and writes return status
6. **Dashboard**: Verify both toggles, config fields, and topic reference display correctly
