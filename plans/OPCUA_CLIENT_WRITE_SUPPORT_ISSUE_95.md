# OPC UA Client Write Support (MQTT to OPC UA) — Issue #95

## Context

Issue #95: The OPC UA client currently only reads (OPC UA -> MQTT). This adds write support (MQTT -> OPC UA) to enable control, setpoints, and commands from MQTT clients to OPC UA servers.

**Design choices:**
- **Option A** (fire & forget): Publish to `{namespace}/write/{nodeId|browsePath}` — no response
- **Option C** (request/response): Publish to `{namespace}/request/write/{nodeId|browsePath}`, get response on `{namespace}/response/write/{nodeId|browsePath}`
- **Batch writes**: JSON array directly (not wrapped in `{"writes": [...]}`)

## Topic Design

### Single Write — Fire & Forget (Option A)
```
Topic:   opcua/plc01/write/ns=2;i=1001
Payload: {"value": 42.5}
         {"value": 42.5, "dataType": "Double"}
```
No response published. Simple and fast.

### Single Write — Request/Response (Option C)
```
Publish: opcua/plc01/request/write/ns=2;i=1001
Payload: {"value": 42.5}

Response on: opcua/plc01/response/write/ns=2;i=1001
Payload: {"nodeId": "ns=2;i=1001", "status": "Good", "statusCode": 0, "timestamp": "..."}
```

### Batch Write — Fire & Forget
```
Topic:   opcua/plc01/write
Payload: [{"nodeId": "ns=2;i=1001", "value": 42.5}, {"nodeId": "ns=2;i=1002", "value": true}]
```

### Batch Write — Request/Response
```
Publish: opcua/plc01/request/write
Payload: [{"nodeId": "ns=2;i=1001", "value": 42.5}, {"nodeId": "ns=2;i=1002", "value": true}]

Response on: opcua/plc01/response/write
Payload: [{"nodeId": "ns=2;i=1001", "status": "Good", ...}, {"nodeId": "ns=2;i=1002", "status": "Good", ...}]
```

## Implementation Plan

### Step 1: Add WriteConfig to Config Data Classes

**File:** `broker/src/main/kotlin/stores/devices/OpcUaConfig.kt`

Add `OpcUaWriteConfig` data class to `OpcUaConnectionConfig`:

```kotlin
data class OpcUaWriteConfig(
    val enabled: Boolean = false,
    val topicPrefix: String = "write",       // For fire&forget: {namespace}/{topicPrefix}/...
    val requestTopicPrefix: String = "request/write",  // For req/resp
    val responseTopicPrefix: String = "response/write", // For req/resp responses
    val qos: Int = 1,                        // QoS for write topic subscriptions
    val writeTimeout: Long = 5000            // Write operation timeout in ms
)
```

Add `writeConfig` field to `OpcUaConnectionConfig`. Update `fromJsonObject()`, `toJsonObject()`, `validate()`.

### Step 2: Implement Write Handler in OpcUaConnector

**File:** `broker/src/main/kotlin/devices/opcua/OpcUaConnector.kt`

Add write handling directly in the connector (no separate file needed), following the `MqttClientConnector` pattern:

1. **`setupWriteSubscriptions()`** — called after OPC UA connection is established:
   - Register EventBus consumer at `EventBusAddresses.Client.messages(writeClientId)` where `writeClientId = "opcua-write-${deviceConfig.name}"`
   - Subscribe to internal MQTT topics via `sessionHandler.subscribeInternalClient()`:
     - `{namespace}/{topicPrefix}/#` (fire & forget single + batch)
     - `{namespace}/{requestTopicPrefix}/#` (request/response single + batch)

2. **`handleWriteMessage(message: BrokerMessage)`** — dispatched from EventBus consumer:
   - Loop prevention: skip if `senderId == deviceConfig.name`
   - Determine if fire&forget or request/response based on topic prefix match
   - Parse topic to extract nodeId/browsePath (everything after the prefix)
   - Parse payload: if JSON array → batch write, if JSON object → single write
   - Call `executeOpcUaWrite()` or `executeOpcUaBatchWrite()`

3. **`executeOpcUaWrite(nodeIdStr, payload, isRequestResponse, originalTopic)`**:
   - Parse `nodeIdStr` to Milo `NodeId` (handle `ns=2;i=1001`, `ns=2;s=MyVar` formats)
   - Extract `value` from payload, optional `dataType` hint
   - Convert JSON value to OPC UA `Variant` using type conversion (reverse of read path)
   - Build `WriteValue(nodeId, AttributeId.Value, null, DataValue(variant))`
   - Call `client!!.write(listOf(writeValue))` with timeout
   - Increment `messagesOutCounter`
   - If request/response: publish response message with status

4. **`executeOpcUaBatchWrite(writes: JsonArray, isRequestResponse, originalTopic)`**:
   - Build list of `WriteValue` from each array element (each has `nodeId` + `value` + optional `dataType`)
   - Single `client!!.write(writeValues)` call (efficient batch)
   - If request/response: publish array of status results

5. **`convertJsonToVariant(value: Any?, dataTypeHint: String?)`** — type conversion:
   - If `dataType` hint provided: use it explicitly (Double, Float, Int32, Boolean, String, etc.)
   - If no hint: infer from JSON type:
     - JSON boolean → `Variant(Boolean)`
     - JSON integer → `Variant(Int32)` (or `Int64` if large)
     - JSON float → `Variant(Double)`
     - JSON string → `Variant(String)`
     - JSON null → skip/error

6. **`publishWriteResponse(originalTopic, nodeId, statusCode)`**:
   - Replace `request/write` prefix with `response/write` in topic
   - Build response JSON: `{"nodeId": "...", "status": "Good"|"Bad...", "statusCode": N, "timestamp": "..."}`
   - Publish via `sessionHandler.publishMessage()`

7. **`teardownWriteSubscriptions()`** — called on stop:
   - Unregister EventBus consumer
   - Unsubscribe internal client

### Step 3: GraphQL Schema Updates

**File:** `broker/src/main/resources/schema-mutations.graphqls`

Add input type:
```graphql
input OpcUaWriteConfigInput {
    enabled: Boolean
    topicPrefix: String
    requestTopicPrefix: String
    responseTopicPrefix: String
    qos: Int
    writeTimeout: Long
}
```

Add `writeConfig: OpcUaWriteConfigInput` to `OpcUaConnectionConfigInput`.

**File:** `broker/src/main/resources/schema-queries.graphqls`

Add query type:
```graphql
type OpcUaWriteConfig {
    enabled: Boolean!
    topicPrefix: String!
    requestTopicPrefix: String!
    responseTopicPrefix: String!
    qos: Int!
    writeTimeout: Long!
}
```

Add `writeConfig: OpcUaWriteConfig` to `OpcUaConnectionConfig`.

### Step 4: GraphQL Mutation Handler Updates

**File:** `broker/src/main/kotlin/graphql/OpcUaClientConfigMutations.kt`

- Parse `writeConfig` from input in `addOpcUaDevice()` and `updateOpcUaDevice()`
- Preserve existing writeConfig on update if not provided (same pattern as password handling)

**File:** `broker/src/main/kotlin/graphql/OpcUaClientConfigQueries.kt`

- Return `writeConfig` fields in query resolvers

### Step 5: Dashboard UI Updates

**Files:** Dashboard OPC UA device detail page

- Add "Write Configuration" section with toggle for enabled
- Show topic prefix fields, QoS, timeout
- Display computed write topics for reference

## Key Files to Modify

| File | Changes |
|------|---------|
| `broker/src/main/kotlin/stores/devices/OpcUaConfig.kt` | Add `OpcUaWriteConfig`, add to `OpcUaConnectionConfig` |
| `broker/src/main/kotlin/devices/opcua/OpcUaConnector.kt` | Add write subscription setup, handler, type conversion, response publishing |
| `broker/src/main/resources/schema-mutations.graphqls` | Add `OpcUaWriteConfigInput` |
| `broker/src/main/resources/schema-queries.graphqls` | Add `OpcUaWriteConfig` type |
| `broker/src/main/kotlin/graphql/OpcUaClientConfigMutations.kt` | Handle writeConfig in mutations |
| `broker/src/main/kotlin/graphql/OpcUaClientConfigQueries.kt` | Return writeConfig in queries |
| Dashboard pages (OPC UA device detail) | Write config UI section |

## Reusable Patterns

- **Internal MQTT subscription**: Follow `MqttClientConnector.setupPublishAddresses()` pattern (EventBus consumer + `subscribeInternalClient`)
- **Loop prevention**: Use `senderId` check (same as existing OPC UA read path)
- **Type conversion**: Reverse of `handleOpcUaValueChangeForAddress()` (lines 974-989 in OpcUaConnector.kt)
- **Milo write API**: `client.write(List<WriteValue>)` returns `CompletableFuture<List<StatusCode>>`
- **GraphQL config persistence**: Follow existing password-preservation pattern in mutations

## Verification

1. **Build**: `cd broker && mvn clean package`
2. **Manual test with fire & forget**:
   - Configure OPC UA device with `writeConfig.enabled = true`
   - Publish to `{namespace}/write/ns=2;i=1001` with `{"value": 42.5}`
   - Verify value changes on OPC UA server
3. **Manual test with request/response**:
   - Subscribe to `{namespace}/response/write/#`
   - Publish to `{namespace}/request/write/ns=2;i=1001` with `{"value": 42.5}`
   - Verify response message received with status
4. **Batch test**: Publish JSON array to `{namespace}/write` and verify all nodes written
5. **Dashboard**: Verify write config section appears and persists
