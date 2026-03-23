# OPC UA Client

MonsterMQ's OPC UA client extension (`broker/src/main/kotlin/devices/opcua`) maintains MQTT-facing subscriptions for external OPC UA servers. Each configured device results in its own `OpcUaConnector` verticle, deployed only on the cluster node specified in the device configuration. Connectors automatically reconnect when sessions fail and keep their certificate material isolated per device.

## GraphQL Entrypoints

Use the following mutations and queries (registered in `GraphQLServer.kt`) to manage client devices:

- `addOpcUaDevice(input: OpcUaDeviceInput!)` – create a new device and deploy its connector on the assigned node.
- `updateOpcUaDevice(name: String!, input: OpcUaDeviceInput!)` – replace the stored configuration; the connector is redeployed with the new settings.
- `deleteOpcUaDevice(name: String!)` – remove the device and tear down its connector.
- `toggleOpcUaDevice(name: String!, enabled: Boolean!)` – enable or disable an existing device without deleting it.
- `reassignOpcUaDevice(name: String!, nodeId: String!)` – move the device to a different cluster node.
- `addOpcUaAddress(deviceName: String!, input: OpcUaAddressInput!)` / `deleteOpcUaAddress(deviceName: String!, address: String!)` – manage static subscriptions on a device.
- Queries: `opcUaDevices`, `opcUaDevice`, `opcUaDevicesByNode`, and `clusterNodes` expose stored configurations (connection status is not currently tracked).

### Example – add a device

```graphql
mutation {
  addOpcUaDevice(input: {
    name: "plc01"
    namespace: "opcua/factory/plc01"
    nodeId: "node-a"
    enabled: true
    config: {
      endpointUrl: "opc.tcp://192.168.1.100:4840"
      securityPolicy: Basic256Sha256
      subscriptionSamplingInterval: 1000.0
      keepAliveFailuresAllowed: 3
      reconnectDelay: 5000
      addresses: [
        {
          address: "NodeId://ns=2;i=1001"
          topic: "temperature"
          publishMode: SEPARATE
        },
        {
          address: "BrowsePath://Objects/Factory/Line1/#"
          topic: "production"
          publishMode: SEPARATE
          removePath: true
        }
      ]
      certificateConfig: {
        securityDir: "security"
        applicationName: "MonsterMQ@factory"
        applicationUri: "urn:MonsterMQ:Factory:Client"
        createSelfSigned: true
        keystorePassword: "factory123"
        validateServerCertificate: true
        autoAcceptServerCertificates: false
      }
    }
  }) {
    success
    errors
  }
}
```

## Device Input Reference

| Field | Notes |
| ----- | ----- |
| `name` | Unique identifier (letters, numbers, `_`, `-`). Used for the device-specific keystore filename. |
| `namespace` | MQTT namespace prefix used when publishing OPC UA values (e.g. `opcua/factory/plc01`). |
| `nodeId` | Cluster node that runs the connector. Only the assigned node deploys the verticle. |
| `enabled` | Disabled devices remain stored but do not connect. |
| `config` | Nested `OpcUaConnectionConfigInput` described below. |

### Connection settings (`config`)

| Field | Default | Description |
| ----- | ------- | ----------- |
| `endpointUrl` | — | OPC UA endpoint URL, must start with `opc.tcp://`. |
| `updateEndpointUrl` | `true` | Automatically switch to the endpoint returned by the server if it differs from the configured URL. |
| `securityPolicy` | `None` | Accepted values: `None`, `Basic128Rsa15`, `Basic256`, `Basic256Sha256`, `Aes128_Sha256_RsaOaep`, `Aes256_Sha256_RsaPss`. Certificates are only loaded when the policy is not `None`. |
| `username` / `password` | `null` | Optional user credentials. When omitted the connector uses anonymous authentication. |
| `subscriptionSamplingInterval` | `0.0` | Requested sampling interval (ms) passed to the OPC UA subscription. |
| `keepAliveFailuresAllowed` | `3` | Number of missed keep-alives before the connector forces a reconnect. |
| `reconnectDelay` | `5000` | Delay (ms) between reconnect attempts. Must be ≥ 1000. |
| `connectionTimeout` | `10000` | OPC UA connect timeout in ms. |
| `requestTimeout` | `5000` | Service request timeout in ms. |
| `monitoringParameters` | buffer `100`, sampling `0.0`, discard `false` | Mirrors Milo's `MonitoringParameters` for each monitored item. |
| `certificateConfig` | — | See below. |

### Monitoring parameters

`monitoringParameters` is optional; omit it to use defaults. When provided, set:
- `bufferSize` – queue depth per monitored item (UInt32).
- `samplingInterval` – override sampling interval per item (ms).
- `discardOldest` – whether OPC UA should drop the oldest samples once the queue is full.

### Certificate configuration

Certificates are handled by `KeyStoreLoader` and stored under `certificateConfig.securityDir`:
- Keystore filename: `monstermq-opcua-client-{device-name}.pfx` (special characters outside `[A-Za-z0-9-]` are replaced with `_`).
- When `createSelfSigned` is `true` and the keystore is missing, a RSA-2048 self-signed certificate with alias `client-ai` is generated.
- Trust directories (`security/trusted-{device-name}/...`) are only created when `validateServerCertificate` is `true`.
- With `autoAcceptServerCertificates = true`, unknown server certificates are added to the trust list automatically; otherwise the connector refuses untrusted servers unless the trust store is empty (first connection is auto-saved).

### Address subscriptions

Every address must use the explicit scheme:
- `NodeId://ns=2;i=1001`
- `BrowsePath://Objects/Factory/Line1/#`

`publishMode` controls topic layout:
- `SEPARATE` (default) publishes values to `Namespace/Topic/[Path]`. For browse paths with wildcards, `removePath: true` removes the static prefix before distributing the wildcard tail.
- `SINGLE` publishes each value to the same `Namespace/Topic` without aggregating multiple values; the last write wins on that topic.

NodeId subscriptions ignore `removePath` and always publish to `Namespace/Topic`.

## MQTT Topics and Payload

Each OPC UA value becomes an MQTT message emitted on the namespace derived topic. The payload is JSON:

```json
{
  "value": 23.5,
  "timestamp": "2024-01-15T10:30:00.123Z",
  "status": 0
}
```

- `value` is typed (numbers stay numeric, booleans remain boolean; other types fall back to strings).
- `timestamp` is taken from the OPC UA `sourceTime` when available, otherwise `Instant.now()`.
- `status` contains the raw OPC UA `StatusCode` value.

## Writing to OPC UA (MQTT → OPC UA)

The connector supports writing values to OPC UA server nodes and reading values on demand. Both features are disabled by default and controlled independently via `writeConfig`.

### Configuration

Add a `writeConfig` block to the device's `config`:

```graphql
config: {
  endpointUrl: "opc.tcp://192.168.1.100:4840"
  # ... other settings ...
  writeConfig: {
    enabled: true                    # Fire & forget writes
    requestResponseEnabled: true     # Request/response (read + write)
    topicPrefix: "write"             # Fire & forget topic prefix
    requestTopicPrefix: "request"    # Request topic prefix
    responseTopicPrefix: "response"  # Response topic prefix
    qos: 1                           # Internal MQTT subscription QoS
    writeTimeout: 5000               # OPC UA write/read timeout (ms)
  }
}
```

| Field | Default | Description |
| ----- | ------- | ----------- |
| `enabled` | `false` | Enable fire & forget writes via `{namespace}/{topicPrefix}/{nodeId}`. |
| `requestResponseEnabled` | `false` | Enable request/response via `{namespace}/{requestTopicPrefix}/{nodeId}` with responses on `{namespace}/{responseTopicPrefix}/{nodeId}`. |
| `topicPrefix` | `write` | Topic prefix for fire & forget writes. |
| `requestTopicPrefix` | `request` | Topic prefix for request/response publish. |
| `responseTopicPrefix` | `response` | Topic prefix for responses. |
| `qos` | `1` | QoS for the connector's internal MQTT subscription to write/request topics. |
| `writeTimeout` | `5000` | Timeout in ms for OPC UA write and read service calls. |

### Fire & Forget Write

Publish to `{namespace}/{topicPrefix}/{nodeId}` — no response is sent.

```
Topic:   opcua/factory/plc01/write/ns=2;i=1001
Payload: {"value": 42.5}
```

With an explicit data type hint:

```
Payload: {"value": 42, "dataType": "Int16"}
```

### Request/Response Write

Publish to `{namespace}/{requestTopicPrefix}/{nodeId}` with a `value` field. A response with the write status is published on `{namespace}/{responseTopicPrefix}/{nodeId}`.

```
Publish:  opcua/factory/plc01/request/ns=2;i=1001
Payload:  {"value": 42.5}

Response: opcua/factory/plc01/response/ns=2;i=1001
Payload:  {"nodeId": "ns=2;i=1001", "status": "Good", "statusCode": 0, "timestamp": "2025-01-15T10:30:00Z"}
```

### Request/Response Read

Publish to `{namespace}/{requestTopicPrefix}/{nodeId}` with an empty payload (or `{}`). The current value is read from the OPC UA server and returned.

```
Publish:  opcua/factory/plc01/request/ns=2;i=1001
Payload:  {}

Response: opcua/factory/plc01/response/ns=2;i=1001
Payload:  {"nodeId": "ns=2;i=1001", "value": 42.5, "status": "Good", "statusCode": 0, "timestamp": "2025-01-15T10:30:00Z"}
```

### Batch Requests

Publish a JSON array to the base topic (without a nodeId). Each entry specifies a `nodeId` and optionally a `value`. Entries without `value` are reads, entries with `value` are writes.

```
Publish:  opcua/factory/plc01/request
Payload:  [
            {"nodeId": "ns=2;i=1001"},
            {"nodeId": "ns=2;i=1002", "value": 42.5},
            {"nodeId": "ns=2;i=1003", "value": true, "dataType": "Boolean"}
          ]

Response: opcua/factory/plc01/response
Payload:  [
            {"nodeId": "ns=2;i=1001", "value": 23.1, "status": "Good", "statusCode": 0, "timestamp": "..."},
            {"nodeId": "ns=2;i=1002", "status": "Good", "statusCode": 0, "timestamp": "..."},
            {"nodeId": "ns=2;i=1003", "status": "Good", "statusCode": 0, "timestamp": "..."}
          ]
```

Fire & forget batch writes (no reads) can use `{namespace}/{topicPrefix}` instead.

### Data Type Hints

When no `dataType` is provided, the type is inferred from the JSON value (numbers → Int32 or Double, booleans → Boolean, strings → String). For precise control, set `dataType` to one of:

`Boolean`, `Byte`, `SByte`, `UByte`, `Int16`, `UInt16`, `Int32`, `UInt32`, `Int64`, `UInt64`, `Float`, `Double`, `String`

## Operational Notes

- Connectors automatically resubscribe after reconnecting and respect `monitoringParameters` when creating Milo monitored items.
- Configuration changes are broadcast on the `opcua.device.config.changed` event bus address; only the assigned node responds by redeploying the connector.
- There is currently no persisted connection status exposed via GraphQL—monitor the broker logs (`opcua`) for handshake and reconnect messages.
- When using browse-path addresses with wildcards, verify that the OPC UA server grants browse permissions to the configured user; failed browses are logged and the connector retries during the next reconnect cycle.
- If you rely on per-topic retention, remember that `publishMode = SINGLE` still emits individual value messages; consumers should deduplicate by timestamp or last-value store.
