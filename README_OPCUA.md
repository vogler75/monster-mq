# OPC UA Extension for MonsterMQ

This document describes the OPC UA extension for MonsterMQ, which provides seamless integration between OPC UA servers and the MQTT broker infrastructure.

## Overview

The OPC UA extension enables MonsterMQ to act as a bridge between OPC UA industrial automation systems and MQTT-based IoT architectures. It provides cluster-aware device management, automatic message archiving, and real-time data publishing from OPC UA servers to MQTT topics.

## Key Features

### 🔄 **Unified Message Publishing**
OPC UA messages use the same central publishing function as regular MQTT clients, ensuring:
- **Complete Message Archiving**: All OPC UA published values are automatically archived using the same mechanism as MQTT messages
- **No Code Duplication**: Both MQTT clients and OPC UA connectors use the shared `SessionHandler.publishMessage()` function
- **Sparkplug Integration**: OPC UA messages automatically participate in Sparkplug message expansion if configured
- **Cluster Distribution**: Messages are properly distributed across cluster nodes based on subscriptions

### 🏗️ **Cluster-Aware Architecture**
- **Node Assignment**: Each OPC UA device is assigned to a specific cluster node
- **Dynamic Deployment**: Connectors are automatically deployed/undeployed based on device configuration and node assignment
- **Failover Ready**: Infrastructure supports backup node assignment for future failover capabilities

### 📡 **Flexible Address Configuration**
- **NodeId Subscriptions**: Direct node monitoring using `NodeId://ns=2;i=16` format
- **BrowsePath Subscriptions**: Hierarchical browsing using `BrowsePath://Objects/Factory/#` format with wildcard support
- **Topic Modes**:
  - `SINGLE`: All values published to one topic
  - `SEPARATE`: Each node gets its own topic with configurable path processing

### 🔧 **Real-Time Configuration Management**
- **Hot Reconfiguration**: Device settings can be updated without broker restart
- **Address Management**: Add/remove OPC UA addresses dynamically
- **Enable/Disable**: Toggle devices on/off without losing configuration

## Architecture

### Core Components

1. **OpcUaExtension** (`devices/opcua/OpcUaExtension.kt`)
   - Main coordination verticle for OPC UA device management
   - Handles cluster-aware device deployment
   - Manages configuration changes via EventBus
   - Routes messages through central publishing function

2. **OpcUaConnector** (`devices/opcua/OpcUaConnector.kt`)
   - Individual device connection handler
   - Manages OPC UA client lifecycle
   - Handles subscriptions and value change notifications
   - Publishes values via central message bus

3. **Device Configuration** (`stores/DeviceConfig.kt`)
   - Device configuration data models
   - OPC UA connection parameters
   - Address subscription configurations
   - Validation and serialization logic

### Message Flow

```
OPC UA Server → OpcUaConnector → EventBus → OpcUaExtension → SessionHandler.publishMessage()
                                                                      ↓
Archive Storage ← MessageHandler ← SessionHandler ← Sparkplug Processing
```

### Central Publishing Integration

The key architectural improvement is the integration with MonsterMQ's central message publishing system:

```kotlin
// OPC UA messages now use the same path as MQTT clients
val sessionHandler = Monster.getSessionHandler()
sessionHandler?.publishMessage(mqttMessage)
```

This ensures OPC UA messages receive the same treatment as regular MQTT messages:
- Automatic archiving via `MessageHandler.saveMessage()`
- Cluster distribution based on topic subscriptions
- Sparkplug message expansion if configured
- Proper topic routing and subscription matching

## Configuration

### Device Configuration

Each OPC UA device requires configuration in the device store:

```json
{
  "name": "factory_plc01",
  "namespace": "opcua/factory",
  "nodeId": "cluster-node-1",
  "enabled": true,
  "config": {
    "endpointUrl": "opc.tcp://192.168.1.100:4840",
    "securityPolicy": "None",
    "subscriptionSamplingInterval": 1000.0,
    "addresses": [
      {
        "address": "BrowsePath://Objects/Factory/#",
        "topic": "sensors",
        "publishMode": "SEPARATE",
        "removePath": true
      },
      {
        "address": "NodeId://ns=2;i=16",
        "topic": "status",
        "publishMode": "SINGLE"
      }
    ]
  }
}
```

### Storage Requirements

OPC UA extension requires a `DeviceConfigStore` implementation. Supported store types:
- **PostgreSQL**: Full device configuration management
- **CrateDB**: Full device configuration management
- **MongoDB**: Full device configuration management

Configure via `ConfigStoreType` in `config.yaml`:

```yaml
ConfigStoreType: "postgres"  # or "cratedb", "mongodb"
```

## Topic Generation

### BrowsePath Subscriptions

For `BrowsePath://Objects/Factory/#` with namespace `opcua/factory` and topic `sensors`:

```
OPC UA Path: Objects/Factory/Line1/Temperature
MQTT Topic:  opcua/factory/sensors/Line1/Temperature
```

### NodeId Subscriptions

For `NodeId://ns=2;i=16` with namespace `opcua/factory` and topic `status`:

```
OPC UA Node: ns=2;i=16
MQTT Topic:  opcua/factory/status
```

## Management

### Adding Devices

Devices can be added via:
1. **GraphQL API**: Full CRUD operations through the management interface
2. **Direct Database**: Insert into device configuration tables
3. **Configuration Files**: Bulk import through DeviceConfigStore implementations

### Monitoring

- **Device Status**: Monitor connection status and message throughput
- **Message Archives**: All OPC UA values are archived alongside MQTT messages
- **Cluster Health**: Track device distribution across cluster nodes
- **Subscription Status**: Monitor OPC UA subscription health and reconnection attempts

### Error Handling

- **Connection Failures**: Automatic reconnection with configurable delays
- **Subscription Errors**: Individual address failures don't affect other subscriptions
- **Node Reassignment**: Graceful handling of cluster topology changes
- **Configuration Errors**: Validation prevents invalid device configurations

## Integration Benefits

By integrating OPC UA messages with MonsterMQ's central publishing system, the extension provides:

1. **Data Consistency**: OPC UA values are archived with the same reliability as MQTT messages
2. **Feature Parity**: OPC UA messages participate in all broker features (Sparkplug, clustering, archiving)
3. **Unified Monitoring**: Single interface for monitoring both MQTT and OPC UA message flows
4. **Simplified Architecture**: No duplicate code paths for message handling and storage
5. **Performance**: Direct function calls avoid EventBus overhead while maintaining proper separation of concerns

This architectural approach ensures that OPC UA integration feels native to MonsterMQ rather than being a bolt-on extension.