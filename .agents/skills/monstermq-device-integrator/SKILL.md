---
name: monstermq-device-integrator
description: >
  Guide for integrating new device types into the MonsterMQ broker. Use this skill whenever
  the user wants to add a new device connector, bridge, or protocol integration to MonsterMQ.
  This includes creating backend Kotlin verticles (Connector + Extension), GraphQL schema/resolvers,
  and dashboard UI pages. Trigger on requests like "add a new device type", "integrate Modbus",
  "create an OPC UA connector", "bridge protocol X to MQTT", "add a new client type",
  or any mention of connecting external systems/devices to MonsterMQ.
---

# Device Integration Skill for MonsterMQ

This skill provides step-by-step instructions for adding a new device connector, industrial protocol bridge, or database logger into MonsterMQ across the Kotlin backend, GraphQL API, and Vite dashboard.

---

## Architecture Blueprint & Core Reference

Read the primary specification guide at [`dev/plans/DEVICE_INTEGRATION.md`](file:///home/vogler/Workspace/monster-mq/dev/plans/DEVICE_INTEGRATION.md) — it contains the complete Extension + Connector architecture overview, code templates, and verification checklist.

### Reference Implementations by Complexity
- **Simple Bridge**: MQTT Client (`devices/mqttclient/`, `graphql/MqttClient*`, dashboard `mqtt-client*`)
- **Medium Protocol**: PLC4X (`devices/plc4x/`, `graphql/Plc4x*`, dashboard `plc4x-*`)
- **Complex Industrial**: WinCC Unified (`devices/winccua/`, `graphql/WinCCUa*`, dashboard `winccua-*`)

---

## End-to-End Implementation Order

Follow this 9-step sequence:

### 1. Configuration Data Class
**Location**: `broker/src/main/kotlin/stores/devices/YourDeviceConfig.kt`
- Define data class with `fromJsonObject()` companion and `toJsonObject()` method.
- Support address/tag mappings if the device bridges external points to MQTT topics.
- Include sensible defaults for timeouts, reconnect delays, and retry counts.

### 2. Connector Verticle (Per-Device Instance)
**Location**: `broker/src/main/kotlin/devices/yourdevice/YourDeviceConnector.kt`
- Extends Vert.x `AbstractVerticle`.
- Loads configuration from `config().getJsonObject("device")`.
- Implements connection lifecycle: connect, disconnect, exponential backoff reconnect.
- Publishes incoming messages to the message bus or Vert.x eventbus.
- Tracks metrics (`messagesInCounter`, `messagesOutCounter`, `isConnected`) and responds to eventbus queries.

### 3. Extension Verticle (Coordinator & Cluster Manager)
**Location**: `broker/src/main/kotlin/devices/yourdevice/YourDeviceExtension.kt`
- Manages connector verticles for this device type across the node.
- Cluster-aware: checks `clusterManager.isLocalNodeResponsible(device.id)`.
- Listens for configuration changes on `EventBusAddresses.Device.configChanged(TYPE)`.
- Deploys/undeploys connector verticles via `vertx.deployVerticle()`.

### 4. Register Verticle in Main (`Monster.kt`) & Feature Flag
**Location**: `broker/src/main/kotlin/Monster.kt` and `Features.kt`
- Declare top-level feature flag constant in `broker/src/main/kotlin/Features.kt` (`const val YourDevice = "YourDevice"`) and append to `Features.all`.
- Declare feature property under `properties.Features.properties` in `broker/yaml-json-schema.json`.
- Gate verticle deployment in `Monster.kt` (`if (Monster.isFeatureEnabled(Features.YourDevice)) { ... }`).
- Gate all GraphQL query and mutation resolver methods with `if (!Monster.isFeatureEnabled(Features.YourDevice))`.

### 5. GraphQL Schema Definition
**Location**: `broker/src/main/resources/` (`schema-types.graphqls`, `schema-queries.graphqls`, `schema-mutations.graphqls`)
- Add data types and status types in `schema-types.graphqls`.
- Add query endpoints in `schema-queries.graphqls` and mutation endpoints in `schema-mutations.graphqls`.
- Always keep query output `Type` models separate from mutation input `InputType` models.

### 6. GraphQL Resolvers
**Location**: `broker/src/main/kotlin/graphql/YourDeviceConfigQueries.kt` and `YourDeviceConfigMutations.kt`
- Gate resolver methods with `if (!Monster.isFeatureEnabled(Features.YourDevice))`.
- Query resolver fetches live metrics via EventBus.
- Mutation resolver creates/updates/deletes device configuration via `IDeviceConfigStore`.
- Preserve existing passwords on update if not provided in input.

### 7. Dashboard List Page
**Location**: `dashboard/src/pages/yourdevice-clients.html` + `src/js/yourdevice-clients.js`
- Build using **List Page Shape** (header, metric-cards, table with status indicators and action buttons).
- Use `window.graphqlClient` for queries and `window.ui` for notifications.

### 8. Dashboard Detail Page
**Location**: `dashboard/src/pages/yourdevice-client-detail.html` + `src/js/yourdevice-client-detail.js`
- Build using **Detail Page Shape** (breadcrumb header, section-cards, form controls).
- Read ID from URL params (`new URLSearchParams(window.location.search).get('id')`).
- Handle both Create mode and Edit mode without clearing existing passwords.

### 9. Dashboard Sidebar Navigation
**Location**: `dashboard/src/js/sidebar.js`
- Add menu item to `getMenuConfig()` under the `Bridging` section with `feature: 'YourDevice'`.

---

## Verification Checklist

After implementation, verify:
- [ ] Backend verticle starts cleanly when enabled via feature flag.
- [ ] GraphQL query returns active device configuration and live metrics.
- [ ] GraphQL mutation correctly creates, updates, and deletes devices.
- [ ] Dashboard list and detail views function properly without CSS component overrides.
- [ ] Reconnection logic handles external device disconnects smoothly.
