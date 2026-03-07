---
name: device-integrator
description: >
  Guide for integrating new device types into the MonsterMQ broker. Use this skill whenever
  the user wants to add a new device connector, bridge, or protocol integration to MonsterMQ.
  This includes creating backend Kotlin verticles (Connector + Extension), GraphQL schema/resolvers,
  and dashboard UI pages. Trigger on requests like "add a new device type", "integrate Modbus",
  "create an OPC UA connector", "bridge protocol X to MQTT", "add a new client type",
  or any mention of connecting external systems/devices to MonsterMQ.
---

# Device Integration Skill for MonsterMQ

You are helping a developer integrate a new device type into the MonsterMQ broker.
This is a full-stack task spanning Kotlin backend, GraphQL API, and JavaScript dashboard.

## Before You Start

Read the comprehensive guide at `plans/DEVICE_INTEGRATION.md` — it contains the complete
architecture overview, code templates, and checklist. Use it as your primary reference.

Then study one existing implementation as a pattern. Good references by complexity:

- **Simplest**: MQTT Client (`devices/mqttclient/`, `graphql/MqttClient*`, dashboard `mqtt-client*`)
- **Medium**: PLC4X (`devices/plc4x/`, `graphql/Plc4x*`, dashboard `plc4x-*`)
- **Full-featured**: WinCC Unified (`devices/winccua/`, `graphql/WinCCUa*`, dashboard `winccua-*`)

## Implementation Order

Follow this sequence — each step builds on the previous:

### 1. Configuration Data Classes
**Location**: `broker/src/main/kotlin/stores/devices/YourDeviceConfig.kt`

- Create data class with `fromJsonObject()` companion and `toJsonObject()` method
- Include an address/mapping list if the device maps external points to MQTT topics
- Add sensible defaults for optional fields (reconnectDelay, timeouts, etc.)
- Look at existing configs in `stores/devices/` for the pattern

### 2. Connector Verticle
**Location**: `broker/src/main/kotlin/devices/yourdevice/YourDeviceConnector.kt`

- Extends `AbstractVerticle`
- Loads config from `config().getJsonObject("device")`
- Implements connection lifecycle: connect, disconnect, reconnect with backoff
- Publishes incoming data as `BrokerMessage` to the extension's eventbus address
- Tracks metrics: `messagesInCounter`, `messagesOutCounter`, `isConnected`
- Responds to metrics queries on `"${Extension.ADDRESS_DEVICE_METRICS}.${deviceConfig.id}"`

If the device needs to **receive MQTT messages** (commands, subscriptions):
- Use `SessionHandler.subscribeInternalClient()` pattern
- Register consumer on `EventBusAddresses.Client.messages(clientId)`
- Handle both `BrokerMessage` and `BulkClientMessage` types
- Unsubscribe and unregister on stop

### 3. Extension Verticle
**Location**: `broker/src/main/kotlin/devices/yourdevice/YourDeviceExtension.kt`

- Manages lifecycle of all connectors of this type
- Cluster-aware: uses `clusterManager.isLocalNodeResponsible(device.id)`
- Listens for config changes on `EventBusAddresses.Device.configChanged(TYPE)`
- Deploys/undeploys connector verticles via `vertx.deployVerticle()`
- Subscribes to the value publish address via `messageBus`

### 4. Register in Monster.kt
**Location**: `broker/src/main/kotlin/Monster.kt`

- Deploy extension verticle in the devices initialization block
- Follow existing pattern: check if devices are enabled, deploy, log

### 5. GraphQL Schema
**Location**: `broker/src/main/resources/schema-types.graphqls` (types), `schema-queries.graphqls` (queries), `schema-mutations.graphqls` (mutations)

- Define output types (for queries) and input types (for mutations) — always separate
- Add metrics type with `messagesIn`, `messagesOut`, `connected`
- Add query for live metrics
- Add create/update mutations

### 6. GraphQL Resolvers
**Location**: `broker/src/main/kotlin/graphql/YourDeviceConfigQueries.kt` and `YourDeviceConfigMutations.kt`

- Query resolver: fetch metrics via eventbus request with timeout
- Mutation resolvers: create/update device configs via `IDeviceConfigStore`
- Register in `GraphQLServer.kt`
- Preserve existing password on update if not provided

### 7. Dashboard List Page
**Location**: `broker/src/main/resources/dashboard/pages/yourdevice-clients.html` + `js/yourdevice-clients.js`

- Follow the standard page template structure:
  - Head: Inter font, `monster-theme.css`, `storage.js`, `graphql-client.js`, `log-viewer.js`, `sidebar.js`
  - Body: sidebar placeholder `<aside class="sidebar" id="sidebar"><nav id="sidebar-nav"></nav></aside>`
  - Main content wrapper: `<div class="main-content" id="main-content">`
- Table with columns: Name, Namespace, Status (connected indicator), Messages In/Out, Actions
- Auto-refresh every 30 seconds
- Use `GraphQLDashboardClient` for queries (instance at `window.graphqlClient`)

### 8. Dashboard Detail Page
**Location**: `broker/src/main/resources/dashboard/pages/yourdevice-client-detail.html` + `js/yourdevice-client-detail.js`

- Form for create/update with all config fields
- Dynamic address list management (add/remove rows)
- Load existing config when editing (id from URL query param)
- Preserve password field behavior (don't clear on update)

### 9. Add to Sidebar Menu
**Location**: `broker/src/main/resources/dashboard/js/sidebar.js`

- Add entry to the `menuConfig` array in the `renderMenu()` method
- Place in the "Bridging" section
- Choose an appropriate SVG icon

## Key Patterns to Follow

### Topic Naming
```
{namespace}/{function}/{device}/{datapoint}
```
Keep under 5 levels deep.

### Logging Levels
```kotlin
logger.severe("...")   // Errors
logger.warning("...")  // Warnings
logger.info("...")     // Important events
logger.fine("...")     // Debug details
```

### Error Handling
Always use Vert.X Future pattern with `.onSuccess`/`.onFailure`. Schedule reconnect on failure.

### Metrics
- Use `AtomicLong` counters for thread safety
- Always handle null metrics in frontend (device may be offline)
- Return null from GraphQL resolver if eventbus request times out

## Common Pitfalls

Read the "Common UI and GraphQL Integration Pitfalls" section in `plans/DEVICE_INTEGRATION.md`.
Key issues: password preservation on update, separate type vs input types, null metrics handling,
always sending complete address list on update.

## Verification Checklist

After implementation, verify:
- [ ] Device deploys successfully (check logs)
- [ ] Metrics are collected and visible in dashboard
- [ ] Create/update/delete works via dashboard
- [ ] Reconnection works when external device restarts
- [ ] Cluster-aware: device runs on correct node
- [ ] Clean shutdown: timers cancelled, connections closed
