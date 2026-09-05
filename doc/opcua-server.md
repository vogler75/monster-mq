# OPC UA Server

MonsterMQ includes a built-in **OPC UA Server** that bridges MQTT and OPC UA protocols, enabling seamless integration between modern IoT systems and industrial automation.

## Overview

MonsterMQ can expose MQTT topics as OPC UA variables through the OPC UA server extension (`broker/src/main/kotlin/devices/opcuaserver`). Each stored server configuration spawns an `OpcUaServerInstance` on the selected cluster node, creates Milo namespaces/nodes, subscribes to the configured MQTT topics, and keeps values in sync in both directions.

### Key Features

- **MQTT-to-OPC UA Bridge** - Automatically creates OPC UA nodes from MQTT topics
- **Real-time Subscriptions** - OPC UA clients receive live updates when MQTT messages arrive
- **Multiple Data Types** - Support for TEXT, NUMERIC, BOOLEAN, BINARY, JSON, and UNWRAPPED_JSON data types
- **Hierarchical Structure** - MQTT topic paths become OPC UA folder hierarchies
- **Eclipse Milo Foundation** - Built on industry-standard Eclipse Milo OPC UA SDK
- **Web-based Configuration** - Configure address mappings through the GraphQL dashboard

### Quick Start

Default OPC UA server endpoint:
```
opc.tcp://localhost:4840/server
```

Create a server and add mappings using the step-by-step operations below.

### Data Type Mapping

| MQTT Payload | OPC UA Data Type | Example |
|-------------|------------------|---------|
| `"23.5"` | Double (NUMERIC) | Temperature sensor |
| `"true"` | Boolean (BOOLEAN) | Status flags |
| `"Hello World"` | String (TEXT) | Status messages |
| Raw payload bytes | ByteString (BINARY) | File transfers |
| `{"value":23.5}` | Double (JSON envelope) | Value with optional timestamp/status |

### Address Space Structure

MQTT topics are automatically mapped to OPC UA nodes:

```
MQTT Topic: sensors/temperature/room1
OPC UA Path: Objects/<configured namespace>/sensors/temperature/room1

MQTT Topic: factory/line1/status
OPC UA Path: Objects/<configured namespace>/factory/line1/status
```

### Real-time Updates

When MQTT messages are published, OPC UA subscribers receive immediate notifications:

1. **MQTT Message Published** → `sensors/temp1` with payload `"24.5"`
2. **OPC UA Node Updated** → `<configured namespace>/sensors/temp1` value becomes `24.5`
3. **Subscriptions Notified** → All OPC UA clients subscribed to the node receive the update

## Step-by-step Setup

### Step 1: Create an OPC UA Server

```graphql
mutation {
  opcUaServer {
    create(config: {
      name: "test-server"
      namespace: "opcua/server"
      nodeId: "local"
      enabled: true
      port: 4840
      path: "server"
      namespaceUri: "urn:MonsterMQ:OpcUaServer"
    }) {
      success
      message
    }
  }
}
```

### Step 2: Add Address Mappings

Map MQTT topics to OPC UA nodes:

```graphql
# Text data (read/write)
mutation {
  opcUaServer {
    addAddress(serverName: "test-server", address: {
      mqttTopic: "write/#"
      browseName: "write"
      dataType: TEXT
      accessLevel: READ_WRITE
    }) { success message }
  }
}
```

```graphql
# Numeric data (read/write)
mutation {
  opcUaServer {
    addAddress(serverName: "test-server", address: {
      mqttTopic: "float/#"
      browseName: "float"
      dataType: NUMERIC
      accessLevel: READ_WRITE
      unit: "units"
    }) { success message }
  }
}
```

```graphql
# Read-only sensor data
mutation {
  opcUaServer {
    addAddress(serverName: "test-server", address: {
      mqttTopic: "sensors/#"
      browseName: "sensors"
      dataType: NUMERIC
      accessLevel: READ_ONLY
    }) { success message }
  }
}
```

### Step 3: Check the running server

Address changes are applied through runtime commands. Check the mutation result
and broker logs before publishing test values. A restart is not normally required
for adding an address.

### Step 4: Create OPC UA Nodes via MQTT

OPC UA nodes are created dynamically when MQTT messages are published to mapped topics:

```python
import paho.mqtt.client as mqtt
import time

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.username_pw_set("Admin", "replace-with-your-password")
client.connect("localhost", 1883)
client.loop_start()

time.sleep(1)
client.publish("write/oee", "100").wait_for_publish()
client.publish("float/temperature", "23.5").wait_for_publish()

time.sleep(1)
client.loop_stop()
client.disconnect()
```

Or using mosquitto CLI:
```bash
mosquitto_pub -h localhost -p 1883 -u Admin -P "replace-with-your-password" -t "write/oee" -m "100"
mosquitto_pub -h localhost -p 1883 -u Admin -P "replace-with-your-password" -t "float/temperature" -m "23.5"
```

### Step 5: Connect an OPC UA Client

**Endpoint:** `opc.tcp://localhost:4840/server`

**Node ID format:**
- Variable nodes: `ns=<index>;s=<namespace>/<topic>:v` (for this example, `opcua/server/write/oee:v`)
- Folder nodes: `ns=<index>;s=<namespace>/<path>:o`. Resolve the actual namespace index from `namespaceUri`; do not hardcode `2`.

**Node path structure:**
```
Objects/
  opcua/server  (one root folder whose name is the configured namespace)
    write/
      oee  (value: "100")
    float/
      temperature  (value: 23.5)
```

**Python OPC UA client example:**
```python
from asyncua import Client
import asyncio

async def main():
    async with Client(url="opc.tcp://localhost:4840/server") as client:
        index = await client.get_namespace_index("urn:MonsterMQ:OpcUaServer")
        node = client.get_node(f"ns={index};s=opcua/server/write/oee:v")
        print(f"Current value: {await node.read_value()}")
        await node.write_value("999")
        print(f"New value: {await node.read_value()}")

asyncio.run(main())
```

## Management Commands

### Query server status

```graphql
query {
  opcUaServers(name: "test-server") {
    name
    enabled
    port
    path
    status { status endpointUrl }
    addresses {
      mqttTopic
      browseName
      dataType
      accessLevel
    }
  }
}
```

### List all servers

```graphql
query {
  opcUaServers {
    name
    enabled
    port
    status { status endpointUrl }
  }
}
```

### Update server configuration

```graphql
mutation {
  opcUaServer {
    update(name: "test-server", input: {
      name: "test-server"
      addresses: [{mqttTopic: "sensors/#", dataType: NUMERIC, accessLevel: READ_ONLY}]
      namespace: "opcua/server"
      nodeId: "local"
      enabled: true
      port: 4841
      path: "monstermq"
    }) { success errors }
  }
}
```

### Remove an address mapping

```graphql
mutation {
  opcUaServer {
    removeAddress(serverName: "test-server", mqttTopic: "write/#") { success message }
  }
}
```

### Enable/disable a server

```graphql
mutation {
  opcUaServer {
    toggle(name: "test-server", enabled: false) { success errors }
  }
}
```

### Delete a server

```graphql
mutation {
  opcUaServer {
    delete(serverName: "test-server") { success message }
  }
}
```

## Troubleshooting

### Server not starting

Check broker logs for:
```
[INFO] Starting OPC UA Server 'test-server' on port 4840
[INFO] OPC UA Server 'test-server' started successfully on port 4840
```

### Nodes not appearing

1. Ensure MQTT messages have been published to mapped topics — nodes are created lazily on first message.
2. Check topic patterns match (e.g. `write/#` matches `write/oee`).
3. Resolve the namespace index from the configured URI and include the configured namespace prefix in node IDs.
4. Use an OPC UA browser to explore the actual node structure.

### Connection timeout

- Verify port 4840 is not blocked by a firewall.
- Check server is running: `query { opcUaServers(name: "test-server") { status { status endpointUrl } } }`
- Test endpoint: `opc.tcp://localhost:4840/server`

## GraphQL Entrypoints

Operations are grouped under `opcUaServer`:

- `opcUaServer.create(config: OpcUaServerConfigInput!)` – persist the configuration and start the server immediately when `enabled` is true.
- `opcUaServer.start(serverName: String!, nodeId: String)` – start an existing server on the target node (`nodeId` defaults to `"*"`, which the command handler resolves to the current node).
- `opcUaServer.stop(serverName: String!, nodeId: String)` – stop the running instance.
- `opcUaServer.delete(serverName: String!)` – shut down the server and remove the stored configuration.
- `opcUaServer.addAddress(serverName: String!, address: OpcUaServerAddressInput!)` – add or update an address mapping at runtime.
- `opcUaServer.removeAddress(serverName: String!, mqttTopic: String!)` – remove an address mapping and its node hierarchy.
- Queries `opcUaServers(name:, node:)` return the persisted configuration plus cached runtime status when available.

For advanced settings such as `namespaceIndex`, buffering, and security, use
`opcUaServer.add(input: OpcUaServerInput!)` with an explicit `addresses` array.
The simplified `create(config:)` has a smaller input contract. See
[schema-mutations.graphqls](../broker/src/main/resources/schema-mutations.graphqls)
for the exact fields of both variants.

## Configuration Reference

| Field | Default | Description |
| ----- | ------- | ----------- |
| `name` | — | Identifier for the server; used in keystore filenames and event-bus commands. |
| `namespace` | — | Logical MQTT namespace for internal routing. Typically mirrors the MQTT topic prefix. |
| `nodeId` | — | Target cluster node. Use `"*"` to start on all nodes. |
| `enabled` | `true` | Stores whether the server should start automatically. `opcUaServer.create` honours it immediately. |
| `port` | `4840` | TCP port the OPC UA endpoint binds to. |
| `path` | `"server"` | Endpoint path portion, exposed as `/path` (no leading slash in config). |
| `namespaceIndex` | `1` | Stored legacy setting; runtime registers `namespaceUri` with Milo and uses the returned index. |
| `namespaceUri` | `"urn:MonsterMQ:OpcUaServer"` | Namespace URI advertised to clients. |
| `bufferSize` | `1000` | Stored setting; the current node update path does not implement a bounded update buffer using it. |
| `updateInterval` | `100` | Minimum milliseconds between writes applied to the same node. |

### Security block (`security`)

`OpcUaServerSecurity` governs endpoint security:
- `keystorePath` / `keystorePassword` / `certificateAlias` – keystore to read certificates from.
- `securityPolicies` – list of strings (`"None"`, `"Basic256Sha256"`, …) that become endpoints.
- `allowAnonymous` / `requireAuthentication` are stored settings, but the current server setup does not wire a MonsterMQ username identity validator. Do not treat these flags as enforced MQTT-user authentication.
- `allowUnencrypted` – permit unsecured endpoints when policy `None` is present.
- `certificateDir` – directory for keystores. The loader creates `monstermq-opcua-server-{name}.pfx` when `createSelfSigned` is `true`; otherwise the file must already exist.

Certificate generation mirrors the client side: RSA-2048, alias `opcua-server`, and subject alternative names for discovered hostnames and IPv4 addresses.

### Address mappings (`OpcUaServerAddressInput`)

| Field | Default | Description |
| ----- | ------- | ----------- |
| `mqttTopic` | — | Topic or wildcard (`+`, `#`) consumed from MQTT. Hierarchical folders are created from the topic segments. |
| `displayName` / `browseName` | derived from topic | Human-readable labels for the OPC UA node. `browseName` defaults to the last topic segment. |
| `description` | `null` | Optional description string added to the node metadata. |
| `dataType` | `TEXT` | Conversion profile: `BINARY`, `TEXT`, `NUMERIC`, `BOOLEAN`, `JSON`, `UNWRAPPED_JSON`. Affects MQTT–OPC UA translation. |
| `accessLevel` | `READ_ONLY` | `READ_ONLY` exposes MQTT data as OPC UA variables; `READ_WRITE` also forwards client writes back to MQTT. |
| `unit` | `null` | Free-form engineering unit label stored on the node. |

## Data Flow

The server instance performs the following steps:

1. Loads/creates certificates based on the security configuration and builds Milo endpoints accordingly.
2. Starts an `OpcUaServerNamespace` and `OpcUaServerNodes`, using the configured `namespaceUri` and the runtime index returned by Milo.
3. Subscribes to every configured `mqttTopic` via the internal session handler. Wildcards (`#`, `+`) use MQTT topic matching.
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
- A stored `nodeId = "*"` can deploy the configuration on every eligible node. Immediate start/stop/address commands resolve `"*"` to the current node; specify a concrete node where the operation supports it.
- Keep keystore passwords consistent when rotating certificates—`createSelfSigned: false` requires placing the `.pfx` in `security.certificateDir` ahead of time.
- Milo log output for endpoint negotiation and certificate validation is forwarded to the broker log; monitor entries tagged `OpcUaServer` during commissioning.
- `add`, `update`, and `toggle` return `success`, `errors`, and optional `server`; the simplified `create`, `start`, `stop`, `delete`, and address operations return `success` and `message`.
- `update` requires the server name and the full address list in its input; preserve existing mappings when changing only a port.

### Unwrapped JSON

`UNWRAPPED_JSON` exposes object fields as separate variables below the topic rather
than interpreting a `value`/`timestamp`/`status` envelope. Select it for structured
JSON objects and inspect the resulting address space with a browser; ordinary
`JSON` uses the envelope shown above.
