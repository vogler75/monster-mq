# OPC UA Server Setup via GraphQL

This guide shows how to configure and enable the MonsterMQ OPC UA server using GraphQL mutations.

## Prerequisites

- MonsterMQ broker running with GraphQL enabled (default: http://localhost:4000/graphql)
- GraphQL client or tool (curl, Postman, GraphQL Playground, etc.)

## Step 1: Create OPC UA Server

Create a new OPC UA server instance:

```graphql
mutation {
  opcUaServer {
    create(config: {
      name: "test-server"
      namespace: "opcua/server"
      nodeId: "*"
      enabled: true
      port: 4840
      path: "server"
      namespaceUri: "urn:MonsterMQ:OpcUaServer"
    }) {
      success
      message
      server {
        name
        port
        enabled
      }
    }
  }
}
```

**Parameters:**
- `name`: Unique server identifier
- `namespace`: MQTT namespace for internal communication
- `nodeId`: Target cluster node ("*" = all nodes)
- `enabled`: Auto-start the server
- `port`: OPC UA server port (default: 4840)
- `path`: Endpoint path (creates `opc.tcp://localhost:4840/server`)
- `namespaceUri`: OPC UA namespace URI

## Step 2: Add Address Mappings

Add MQTT topic → OPC UA node mappings:

### Text Data (e.g., write/# topics)

```graphql
mutation {
  opcUaServer {
    addAddress(serverName: "test-server", address: {
      mqttTopic: "write/#"
      browseName: "write"
      dataType: TEXT
      accessLevel: READ_WRITE
      description: "Writable text values"
    }) {
      success
      message
    }
  }
}
```

### Numeric Data (e.g., float/# topics)

```graphql
mutation {
  opcUaServer {
    addAddress(serverName: "test-server", address: {
      mqttTopic: "float/#"
      browseName: "float"
      dataType: NUMERIC
      accessLevel: READ_WRITE
      description: "Numeric sensor values"
      unit: "units"
    }) {
      success
      message
    }
  }
}
```

### Read-Only Data (e.g., sensors/# topics)

```graphql
mutation {
  opcUaServer {
    addAddress(serverName: "test-server", address: {
      mqttTopic: "sensors/#"
      browseName: "sensors"
      dataType: NUMERIC
      accessLevel: READ_ONLY
      description: "Read-only sensor data"
    }) {
      success
      message
    }
  }
}
```

**Data Types:**
- `TEXT` - String values
- `NUMERIC` - Double/Float values
- `BOOLEAN` - True/False
- `BINARY` - ByteString
- `JSON` - JSON objects (stored as string)

**Access Levels:**
- `READ_ONLY` - OPC UA clients can read but not write
- `READ_WRITE` - OPC UA clients can read and write
- `WRITE_ONLY` - OPC UA clients can write but not read

## Step 3: Restart Server (If Already Running)

After adding address mappings to an existing server, restart it to apply changes:

```graphql
mutation {
  opcUaServer {
    stop(serverName: "test-server") {
      success
      message
    }
  }
}
```

```graphql
mutation {
  opcUaServer {
    start(serverName: "test-server") {
      success
      message
    }
  }
}
```

## Step 4: Create OPC UA Nodes via MQTT

OPC UA nodes are created dynamically when MQTT messages are published to mapped topics:

### Using Python (paho-mqtt)

```python
import paho.mqtt.client as mqtt
import time

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.username_pw_set("admin", "public")
client.connect("localhost", 1883)
client.loop_start()

# Publish to create write/oee node
time.sleep(1)
result = client.publish("write/oee", "100")
result.wait_for_publish()
print("✓ Published to write/oee - OPC UA node created")

# Publish to create float/temperature node
result = client.publish("float/temperature", "23.5")
result.wait_for_publish()
print("✓ Published to float/temperature - OPC UA node created")

time.sleep(2)
client.loop_stop()
client.disconnect()
```

### Using MQTT CLI

```bash
# Create write/oee node
mosquitto_pub -h localhost -p 1883 -u admin -P public -t "write/oee" -m "100"

# Create float/temperature node
mosquitto_pub -h localhost -p 1883 -u admin -P public -t "float/temperature" -m "23.5"
```

## Step 5: Connect OPC UA Client

Once nodes are created, connect with any OPC UA client:

**Endpoint**: `opc.tcp://localhost:4840/server`

**Node Path Structure**:
```
Objects/
  opcua/
    server/
      write/
        oee (value: "100")
      float/
        temperature (value: 23.5)
```

**Node ID Format**:
- Variable nodes: `ns=2;s=<topic>:v` (e.g., `ns=2;s=write/oee:v`)
- Folder nodes: `ns=2;s=<topic>:o` (e.g., `ns=2;s=write:o`)

### Python OPC UA Client Example

```python
from asyncua import Client
import asyncio

async def main():
    client = Client(url="opc.tcp://localhost:4840/server")
    
    async with client:
        # Read value
        node = client.get_node("ns=2;s=write/oee:v")
        value = await node.read_value()
        print(f"Current value: {value}")
        
        # Write value
        await node.write_value("999")
        print("✓ Write successful")
        
        # Read back
        new_value = await node.read_value()
        print(f"New value: {new_value}")

asyncio.run(main())
```

## Management Commands

### Query Server Status

```graphql
query {
  opcUaServer(name: "test-server") {
    name
    enabled
    port
    path
    status
    addresses {
      mqttTopic
      browseName
      dataType
      accessLevel
    }
  }
}
```

### List All Servers

```graphql
query {
  opcUaServers {
    name
    enabled
    port
    status
  }
}
```

### Update Server Configuration

```graphql
mutation {
  opcUaServer {
    update(name: "test-server", input: {
      namespace: "opcua/server"
      nodeId: "*"
      enabled: true
      port: 4841
      path: "monstermq"
    }) {
      success
      message
    }
  }
}
```

### Remove Address Mapping

```graphql
mutation {
  opcUaServer {
    removeAddress(
      serverName: "test-server"
      mqttTopic: "write/#"
    ) {
      success
      message
    }
  }
}
```

### Toggle Server Enable/Disable

```graphql
mutation {
  opcUaServer {
    toggle(name: "test-server", enabled: false) {
      success
      message
    }
  }
}
```

### Delete Server

```graphql
mutation {
  opcUaServer {
    delete(serverName: "test-server") {
      success
      message
    }
  }
}
```

## PowerShell Helper Script

Save as `setup-opcua-server.ps1`:

```powershell
# Configure OPC UA server via GraphQL

$graphqlUrl = "http://localhost:4000/graphql"

function Invoke-GraphQL($query) {
    $body = @{query=$query} | ConvertTo-Json
    Invoke-RestMethod -Uri $graphqlUrl -Method Post -Body $body -ContentType 'application/json'
}

# Create server
Write-Host "Creating OPC UA server..." -ForegroundColor Cyan
$createQuery = @'
mutation {
  opcUaServer {
    create(config: {
      name: "test-server"
      namespace: "opcua/server"
      nodeId: "*"
      enabled: true
      port: 4840
      path: "server"
      namespaceUri: "urn:MonsterMQ:OpcUaServer"
    }) { success message }
  }
}
'@
Invoke-GraphQL $createQuery | Out-Null

# Add write/# mapping
Write-Host "Adding write/# mapping..." -ForegroundColor Cyan
$writeQuery = @'
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
'@
Invoke-GraphQL $writeQuery | Out-Null

# Add float/# mapping
Write-Host "Adding float/# mapping..." -ForegroundColor Cyan
$floatQuery = @'
mutation {
  opcUaServer {
    addAddress(serverName: "test-server", address: {
      mqttTopic: "float/#"
      browseName: "float"
      dataType: NUMERIC
      accessLevel: READ_WRITE
    }) { success message }
  }
}
'@
Invoke-GraphQL $floatQuery | Out-Null

Write-Host "`n✓ OPC UA server configured at opc.tcp://localhost:4840/server" -ForegroundColor Green
Write-Host "  Publish MQTT messages to create nodes dynamically`n" -ForegroundColor Yellow
```

## Troubleshooting

### Server not starting

Check broker logs for:
```
[INFO] Starting OPC UA Server 'test-server' on port 4840
[INFO] OPC UA Server 'test-server' started successfully on port 4840
```

### Address mappings not loading

After adding address mappings, restart the server:
```graphql
mutation {
  opcUaServer {
    stop(serverName: "test-server") { success }
  }
}
mutation {
  opcUaServer {
    start(serverName: "test-server") { success }
  }
}
```

Check logs for:
```
[INFO] Setting up X configured nodes for OPC UA server 'test-server'
```

### Nodes not appearing

1. Ensure MQTT messages are published to mapped topics
2. Check topic patterns match (e.g., `write/#` matches `write/oee`)
3. Verify namespace index is 2 in node IDs
4. Use OPC UA browser to explore actual node structure

### Connection timeout

- Verify port 4840 is not blocked by firewall
- Check server is running: query `opcUaServer(name: "test-server") { status }`
- Test with `opc.tcp://localhost:4840/server` endpoint

## See Also

- [OPC UA Server Documentation](../doc/opcua-server.md)
- [OPC UA Client Documentation](../doc/opcua-client.md)
- [GraphQL API Reference](../doc/graphql.md)
