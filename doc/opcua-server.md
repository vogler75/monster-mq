# OPC UA Server

MonsterMQ can expose MQTT topics as OPC UA variables through the OPC UA server extension (`broker/src/main/kotlin/devices/opcuaserver`). Each stored server configuration spawns an `OpcUaServerInstance` on the selected cluster node, creates Milo namespaces/nodes, subscribes to the configured MQTT topics, and keeps values in sync in both directions.

## GraphQL Entrypoints

The CLI only wires the following resolvers at the moment:

- `createOpcUaServer(config: OpcUaServerConfigInput!)` – persist the configuration and start the server immediately when `enabled` is true.
- `startOpcUaServer(serverName: String!, nodeId: String)` – start an existing server on the target node (`nodeId` defaults to all nodes `"*"`).
- `stopOpcUaServer(serverName: String!, nodeId: String)` – stop the running instance.
- `deleteOpcUaServer(serverName: String!)` – shut down the server and remove the stored configuration.
- `addOpcUaServerAddress(serverName: String!, address: OpcUaServerAddressInput!)` – add or update an address mapping at runtime.
- `removeOpcUaServerAddress(serverName: String!, mqttTopic: String!)` – remove an address mapping and its node hierarchy.
- Queries `opcUaServers`, `opcUaServer`, and `opcUaServersByNode` return the persisted configuration plus cached runtime status when available.

### Example – create and map topics

```graphql
mutation {
  createOpcUaServer(config: {
    name: "factory-edge"
    namespace: "opcua/server/factory"
    nodeId: "node-a"
    enabled: true
    port: 4845
    path: "monstermq"
    namespaceIndex: 2
    namespaceUri: "urn:MonsterMQ:OpcUaServer:factory"
    bufferSize: 500
    updateInterval: 100
    security: {
      keystorePath: "server-keystore.jks"
      keystorePassword: "secret"
      certificateAlias: "server-cert"
      securityPolicies: ["None", "Basic256Sha256"]
      allowAnonymous: true
      allowUnencrypted: true
      createSelfSigned: true
      certificateDir: "security"
    }
  }) {
    success
    message
  }
}
```

```graphql
mutation {
  addOpcUaServerAddress(serverName: "factory-edge", address: {
    mqttTopic: "factory/line1/temperature"
    displayName: "Temperature"
    dataType: NUMERIC
    accessLevel: READ_ONLY
    unit: "degC"
  }) {
    success
    message
  }
}
```

## Configuration Reference

| Field | Default | Description |
| ----- | ------- | ----------- |
| `name` | — | Identifier for the server; used in keystore filenames and event-bus commands. |
| `namespace` | — | Logical MQTT namespace for internal routing. Typically mirrors the MQTT topic prefix. |
| `nodeId` | — | Target cluster node. Use `"*"` to start on all nodes. |
| `enabled` | `true` | Stores whether the server should start automatically. `createOpcUaServer` honours it immediately. |
| `port` | `4840` | TCP port the OPC UA endpoint binds to. |
| `path` | `"server"` | Endpoint path portion, exposed as `/path` (no leading slash in config). |
| `namespaceIndex` | `1` | Milo namespace index used for created nodes; must be ≥ 1. |
| `namespaceUri` | `"urn:MonsterMQ:OpcUaServer"` | Namespace URI advertised to clients. |
| `bufferSize` | `1000` | Maximum in-memory buffer for value updates before throttling. |
| `updateInterval` | `100` | Minimum milliseconds between writes applied to the same node. |

### Security block (`security`)

`OpcUaServerSecurity` governs endpoint security:
- `keystorePath` / `keystorePassword` / `certificateAlias` – keystore to read certificates from.
- `securityPolicies` – list of strings (`"None"`, `"Basic256Sha256"`, …) that become endpoints.
- `allowAnonymous` / `requireAuthentication` – controls whether Milo enforces MonsterMQ user authentication (currently only anonymous is wired).
- `allowUnencrypted` – permit unsecured endpoints when policy `None` is present.
- `certificateDir` – directory for keystores. The loader creates `monstermq-opcua-server-{name}.pfx` when `createSelfSigned` is `true`; otherwise the file must already exist.

Certificate generation mirrors the client side: RSA-2048, alias `opcua-server`, and subject alternative names for discovered hostnames and IPv4 addresses.

### Address mappings (`OpcUaServerAddressInput`)

| Field | Default | Description |
| ----- | ------- | ----------- |
| `mqttTopic` | — | Topic or wildcard (`+`, `#`) consumed from MQTT. Hierarchical folders are created from the topic segments. |
| `displayName` / `browseName` | derived from topic | Human-readable labels for the OPC UA node. `browseName` defaults to the last topic segment. |
| `description` | `null` | Optional description string added to the node metadata. |
| `dataType` | `TEXT` | Conversion profile: `BINARY`, `TEXT`, `NUMERIC`, `BOOLEAN`, `JSON`. Affects MQTT–OPC UA translation. |
| `accessLevel` | `READ_ONLY` | `READ_ONLY` exposes MQTT data as OPC UA variables; `READ_WRITE` also forwards client writes back to MQTT. |
| `unit` | `null` | Free-form engineering unit label stored on the node. |

## Data Flow

The server instance performs the following steps:

1. Loads/creates certificates based on the security configuration and builds Milo endpoints accordingly.
2. Starts an `OpcUaServerNamespace` and `OpcUaServerNodes`, using the configured `namespaceUri` and `namespaceIndex`.
3. Subscribes to every configured `mqttTopic` via the internal session handler. Wildcards (`#`, `+`) are honoured with simple prefix matching.
4. On inbound MQTT messages, converts the payload with `OpcUaDataConverter.mqttToOpcUa` and creates/updates a variable node beneath the namespace hierarchy. Each topic segment becomes a folder.
5. Tracks node update timestamps and enforces `updateInterval` to prevent excessive OPC UA traffic.
6. When an OPC UA client writes to a `READ_WRITE` node, the data is converted back to MQTT with `OpcUaDataConverter.opcUaToMqtt` and published using `sessionHandler.publishInternal`, avoiding echo loops by tagging the sender.

### JSON conversion profile

For `dataType = JSON`, the server expects/publishes payloads shaped as:

```json
{
  "value": 12.34,
  "timestamp": "2024-01-15T10:30:00.123Z",
  "status": 0
}
```

`timestamp` and `status` are optional on inbound MQTT messages; missing fields default to `now` and `GOOD` respectively.

## Operational Notes

- New address mappings appear immediately; the node manager creates folders and variables lazily when MQTT data arrives.
- Removing an address deletes the associated node and unsubscribes from the topic.
- `nodeId = "*"` broadcasts control commands to every cluster node; individual node IDs scope them to a single instance.
- Keep keystore passwords consistent when rotating certificates—`createSelfSigned: false` requires placing the `.pfx` in `security.certificateDir` ahead of time.
- Milo log output for endpoint negotiation and certificate validation is forwarded to the broker log; monitor entries tagged `OpcUaServer` during commissioning.
- The current GraphQL wiring does not expose bulk update/toggle mutations that exist in the schema file; rely on the helpers listed above.
