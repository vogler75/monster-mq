# Device Integration Guide for MonsterMQ Broker

This guide provides a comprehensive step-by-step process for integrating a new device type into MonsterMQ. It covers all layers from backend Kotlin classes to frontend dashboard UI.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Step-by-Step Integration Process](#step-by-step-integration-process)
3. [Backend Implementation](#backend-implementation)
4. [GraphQL API Layer](#graphql-api-layer)
5. [Frontend Dashboard Integration](#frontend-dashboard-integration)
6. [Testing and Validation](#testing-and-validation)
7. [Common Patterns and Best Practices](#common-patterns-and-best-practices)

---

## Architecture Overview

MonsterMQ uses a layered architecture for device integrations:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Frontend Dashboard (JS)                       │
│  - List View (e.g., mqtt-clients.js)                            │
│  - Detail View (e.g., mqtt-client-detail.js)                    │
└────────────────────────┬────────────────────────────────────────┘
                         │ GraphQL over HTTP
┌────────────────────────▼────────────────────────────────────────┐
│                      GraphQL API Layer                           │
│  - Schema Definitions (schema.graphqls)                          │
│  - Query Resolvers (*ConfigQueries.kt)                          │
│  - Mutation Resolvers (*ConfigMutations.kt)                     │
└────────────────────────┬────────────────────────────────────────┘
                         │ Function Calls
┌────────────────────────▼────────────────────────────────────────┐
│                    Device Configuration Storage                  │
│  - IDeviceConfigStore Interface                                  │
│  - PostgreSQL / CrateDB / MongoDB / SQLite Implementations      │
└────────────────────────┬────────────────────────────────────────┘
                         │ CRUD Operations
┌────────────────────────▼────────────────────────────────────────┐
│                      Backend Device Layer                        │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Extension (Coordinator Verticle)                        │   │
│  │  - Cluster-aware device management                       │   │
│  │  - Listens to config changes via EventBus               │   │
│  │  - Deploys/undeploys connector verticles                │   │
│  └───────────────────────┬──────────────────────────────────┘   │
│                          │                                       │
│  ┌───────────────────────▼──────────────────────────────────┐   │
│  │  Connector (per-device Verticle)                         │   │
│  │  - Manages single device connection                      │   │
│  │  - Handles data flow (incoming/outgoing)                │   │
│  │  - Implements device-specific protocol                  │   │
│  └──────────────────────────────────────────────────────────┘   │
└──────────────────────┬──────────────────────┬───────────────────┘
                       │                      │
              ┌────────▼─────────┐   ┌───────▼────────┐
              │  MQTT Message    │   │  External      │
              │  Bus (Internal)  │   │  Device/System │
              └──────────────────┘   └────────────────┘
```

### Key Components

1. **DeviceConfig**: Generic configuration model stored in database
2. **Extension**: Cluster-aware coordinator that manages device lifecycle
3. **Connector**: Per-device verticle handling actual device communication
4. **GraphQL API**: Type-safe API for configuration management
5. **Dashboard UI**: Web interface for administration

---

## Step-by-Step Integration Process

### Phase 1: Planning

**Before writing code, define:**

1. **Device Type Identifier**: Unique string (e.g., `"MQTT-Client"`, `"WinCCUA-Client"`)
2. **Configuration Schema**: What parameters does the device need?
   - Connection details (hostname, port, credentials)
   - Operational settings (timeouts, buffer sizes)
   - Address mappings (topics, subscriptions, data points)
3. **Data Flow**: How does data flow in/out?
   - Incoming: Device → Broker (publish to MQTT topics)
   - Outgoing: Broker → Device (subscribe to MQTT topics)
4. **Metrics**: What should be tracked? (messages in/out, connection status, errors)

### Phase 2: Backend Implementation

**Order of implementation:**

1. Configuration data classes
2. Connector verticle
3. Extension verticle
4. Integration with Monster.kt

### Phase 3: GraphQL API

**Order of implementation:**

1. Schema types and inputs
2. Query resolvers
3. Mutation resolvers
4. Register with GraphQLServer.kt

### Phase 4: Frontend Dashboard

**Order of implementation:**

1. List view page (JavaScript)
2. Detail view page (JavaScript)
3. Register routes in sidebar
4. Add navigation links

---

## Backend Implementation

### 1. Configuration Data Classes

**Location**: `src/main/kotlin/stores/devices/YourDeviceConfig.kt`

**Purpose**: Type-safe configuration models

**Example** (based on WinCC UA):

```kotlin
package at.rocworks.stores.devices

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/**
 * Configuration for WinCC Unified client connection
 */
data class WinCCUaConnectionConfig(
    val graphqlEndpoint: String,           // GraphQL API URL
    val username: String,                  // Authentication username
    val password: String,                  // Authentication password
    val messageFormat: String = FORMAT_JSON_ISO,  // Output format
    val reconnectDelay: Long = 5000,       // Reconnection delay (ms)
    val addresses: List<WinCCUaAddress> = emptyList(),  // Subscriptions
    val transformConfig: WinCCUaTransformConfig = WinCCUaTransformConfig()
) {
    companion object {
        // Message format constants
        const val FORMAT_JSON_ISO = "json-iso"
        const val FORMAT_JSON_MS = "json-ms"
        const val FORMAT_RAW_VALUE = "raw"

        /**
         * Parse from JsonObject (from database or GraphQL)
         */
        fun fromJsonObject(json: JsonObject): WinCCUaConnectionConfig {
            val addressesArray = json.getJsonArray("addresses", JsonArray())
            val addresses = addressesArray.map {
                WinCCUaAddress.fromJsonObject(it as JsonObject)
            }

            val transformJson = json.getJsonObject("transformConfig")
                ?: JsonObject()
            val transformConfig = WinCCUaTransformConfig.fromJsonObject(transformJson)

            return WinCCUaConnectionConfig(
                graphqlEndpoint = json.getString("graphqlEndpoint"),
                username = json.getString("username"),
                password = json.getString("password"),
                messageFormat = json.getString("messageFormat", FORMAT_JSON_ISO),
                reconnectDelay = json.getLong("reconnectDelay", 5000L),
                addresses = addresses,
                transformConfig = transformConfig
            )
        }
    }

    /**
     * Convert to JsonObject (for storage)
     */
    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("graphqlEndpoint", graphqlEndpoint)
            .put("username", username)
            .put("password", password)
            .put("messageFormat", messageFormat)
            .put("reconnectDelay", reconnectDelay)
            .put("addresses", JsonArray(addresses.map { it.toJsonObject() }))
            .put("transformConfig", transformConfig.toJsonObject())
    }
}

/**
 * Address configuration for subscriptions
 */
data class WinCCUaAddress(
    val type: WinCCUaAddressType,      // TAG_VALUES or ACTIVE_ALARMS
    val topic: String,                 // MQTT topic for publishing
    val description: String? = null,
    val retained: Boolean = false,
    val nameFilters: List<String>? = null,    // For tag name filtering (e.g., ["HMI_*", "TANK_*"])
    val systemNames: List<String>? = null,    // For alarm filtering
    val filterString: String? = null          // For tag filtering
) {
    companion object {
        fun fromJsonObject(json: JsonObject): WinCCUaAddress {
            // Handle nameFilters (new format) or convert from browseArguments (legacy)
            val nameFiltersList = json.getJsonArray("nameFilters")?.map { it.toString() }
                ?: json.getJsonObject("browseArguments")?.getString("filter")?.let { listOf(it) }

            return WinCCUaAddress(
                type = WinCCUaAddressType.valueOf(json.getString("type")),
                topic = json.getString("topic"),
                description = json.getString("description"),
                retained = json.getBoolean("retained", false),
                nameFilters = nameFiltersList,
                systemNames = json.getJsonArray("systemNames")?.map { it.toString() },
                filterString = json.getString("filterString")
            )
        }
    }

    fun toJsonObject(): JsonObject {
        val json = JsonObject()
            .put("type", type.name)
            .put("topic", topic)
            .put("retained", retained)

        description?.let { json.put("description", it) }
        nameFilters?.let { json.put("nameFilters", JsonArray(it)) }
        systemNames?.let { json.put("systemNames", JsonArray(it)) }
        filterString?.let { json.put("filterString", it) }

        return json
    }
}

enum class WinCCUaAddressType {
    TAG_VALUES,      // Subscribe to tag value changes
    ACTIVE_ALARMS    // Subscribe to alarm notifications
}

/**
 * Topic transformation configuration
 */
data class WinCCUaTransformConfig(
    val replacePattern: String? = null,   // Regex pattern to replace
    val replaceWith: String? = null,      // Replacement string
    val toLowerCase: Boolean = false,     // Convert to lowercase
    val toUpperCase: Boolean = false      // Convert to uppercase
) {
    companion object {
        fun fromJsonObject(json: JsonObject): WinCCUaTransformConfig {
            return WinCCUaTransformConfig(
                replacePattern = json.getString("replacePattern"),
                replaceWith = json.getString("replaceWith"),
                toLowerCase = json.getBoolean("toLowerCase", false),
                toUpperCase = json.getBoolean("toUpperCase", false)
            )
        }
    }

    fun toJsonObject(): JsonObject {
        val json = JsonObject()
            .put("toLowerCase", toLowerCase)
            .put("toUpperCase", toUpperCase)

        replacePattern?.let { json.put("replacePattern", it) }
        replaceWith?.let { json.put("replaceWith", it) }

        return json
    }

    /**
     * Apply transformations to tag name
     */
    fun transformTagNameToTopic(tagName: String): String {
        var result = tagName

        // Apply regex replacement
        if (replacePattern != null && replaceWith != null) {
            result = result.replace(Regex(replacePattern), replaceWith)
        }

        // Apply case transformations
        if (toLowerCase) result = result.lowercase()
        if (toUpperCase) result = result.uppercase()

        return result
    }
}
```

**Key Points**:
- Companion object with `fromJsonObject()` for parsing
- `toJsonObject()` for serialization
- Validation in constructor or separate method
- Sensible defaults for optional parameters

### 2. Connector Verticle

**Location**: `src/main/kotlin/devices/yourdevice/YourDeviceConnector.kt`

**Purpose**: Manages single device connection and data flow

**Template Structure**:

```kotlin
package at.rocworks.devices.yourdevice

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.devices.YourDeviceConnectionConfig
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger

/**
 * Connector for managing a single YourDevice connection
 */
class YourDeviceConnector : AbstractVerticle() {

    private val logger = Logger.getLogger(this::class.java.name)

    // Configuration loaded from config()
    private lateinit var deviceConfig: DeviceConfig
    private lateinit var yourDeviceConfig: YourDeviceConnectionConfig

    // Connection state
    private var isConnected = false
    private var reconnectTimerId: Long? = null

    // Metrics
    private val messagesInCounter = AtomicLong(0)
    private val messagesOutCounter = AtomicLong(0)

    /**
     * Verticle startup
     */
    override fun start(startPromise: Promise<Void>) {
        try {
            // 1. Load configuration
            val deviceJson = config().getJsonObject("device")
            deviceConfig = DeviceConfig.fromJsonObject(deviceJson)
            yourDeviceConfig = YourDeviceConnectionConfig.fromJsonObject(deviceConfig.config)

            logger.info("Starting connector for device: ${deviceConfig.name}")

            // 2. Setup EventBus consumers for metrics
            setupEventBusConsumers()

            // 3. Initialize connection
            connect()
                .onSuccess {
                    logger.info("Connector started successfully for ${deviceConfig.name}")
                    startPromise.complete()
                }
                .onFailure { error ->
                    logger.severe("Failed to start connector: ${error.message}")
                    // Schedule reconnect
                    scheduleReconnect()
                    startPromise.complete() // Don't fail startup
                }

        } catch (e: Exception) {
            logger.severe("Error starting connector: ${e.message}")
            e.printStackTrace()
            startPromise.fail(e)
        }
    }

    /**
     * Verticle shutdown
     */
    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Stopping connector for device: ${deviceConfig.name}")

        // Cancel reconnect timer
        reconnectTimerId?.let { vertx.cancelTimer(it) }

        // Disconnect from device
        disconnect()
            .onComplete {
                logger.info("Connector stopped for ${deviceConfig.name}")
                stopPromise.complete()
            }
    }

    /**
     * Connect to external device/system
     */
    private fun connect(): Future<Void> {
        val promise = Promise.promise<Void>()

        // TODO: Implement device-specific connection logic
        // Example: HTTP client, WebSocket, TCP socket, etc.

        // Example structure:
        // 1. Create client (HTTP, WebSocket, etc.)
        // 2. Authenticate if needed
        // 3. Setup subscriptions/listeners
        // 4. Mark as connected

        logger.info("Connecting to ${yourDeviceConfig.hostname}:${yourDeviceConfig.port}")

        // Placeholder - replace with actual implementation
        isConnected = true
        promise.complete()

        return promise.future()
    }

    /**
     * Disconnect from device
     */
    private fun disconnect(): Future<Void> {
        val promise = Promise.promise<Void>()

        if (!isConnected) {
            promise.complete()
            return promise.future()
        }

        // TODO: Implement device-specific disconnection
        // Example: Close connections, unsubscribe, cleanup

        isConnected = false
        promise.complete()

        return promise.future()
    }

    /**
     * Schedule automatic reconnection
     */
    private fun scheduleReconnect() {
        reconnectTimerId?.let { vertx.cancelTimer(it) }

        reconnectTimerId = vertx.setTimer(yourDeviceConfig.reconnectDelay) {
            logger.info("Attempting reconnection for ${deviceConfig.name}")
            connect()
                .onFailure {
                    logger.warning("Reconnection failed, will retry")
                    scheduleReconnect()
                }
        }
    }

    /**
     * Handle incoming data from device
     * Publish to local MQTT bus
     */
    private fun handleIncomingData(topic: String, payload: ByteArray) {
        try {
            // Construct MQTT topic
            val mqttTopic = "${deviceConfig.namespace}/$topic"

            // Create broker message
            val message = at.rocworks.data.BrokerMessage(
                messageId = 0,
                topicName = mqttTopic,
                payload = payload,
                qosLevel = 0,
                isRetain = false,
                isDup = false,
                isQueued = false,
                clientId = "yourdevice-${deviceConfig.name}"
            )

            // Publish to MQTT bus
            vertx.eventBus().publish(
                YourDeviceExtension.ADDRESS_VALUE_PUBLISH,
                message
            )

            // Update metrics
            messagesInCounter.incrementAndGet()

            logger.fine("Published to MQTT: $mqttTopic")

        } catch (e: Exception) {
            logger.severe("Error handling incoming data: ${e.message}")
        }
    }

    /**
     * Handle outgoing data to device
     * Subscribe to local MQTT topics and forward
     */
    private fun handleOutgoingData(topic: String, payload: ByteArray) {
        try {
            // TODO: Send data to device
            // Example: HTTP POST, WebSocket send, etc.

            messagesOutCounter.incrementAndGet()

        } catch (e: Exception) {
            logger.severe("Error handling outgoing data: ${e.message}")
        }
    }

    /**
     * Setup EventBus consumers for metrics and control
     */
    private fun setupEventBusConsumers() {
        // Metrics consumer
        vertx.eventBus().consumer<JsonObject>(
            YourDeviceExtension.connectorMetrics(deviceConfig.name)
        ) { message ->
            val metrics = JsonObject()
                .put("messagesIn", messagesInCounter.getAndSet(0))
                .put("messagesOut", messagesOutCounter.getAndSet(0))
                .put("connected", isConnected)
                .put("timestamp", System.currentTimeMillis())

            message.reply(metrics)
        }
    }
}
```

**Key Responsibilities**:
- Manage device connection lifecycle
- Handle authentication and reconnection
- Transform device data → MQTT messages (incoming)
- Transform MQTT messages → device commands (outgoing)
- Track metrics (messages in/out, connection status)
- Respond to EventBus queries for metrics

### 3. Extension Verticle

**Location**: `src/main/kotlin/devices/yourdevice/YourDeviceExtension.kt`

**Purpose**: Cluster-aware coordinator for all devices of this type

**Template Structure**:

```kotlin
package at.rocworks.devices.yourdevice

import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.handlers.SessionHandler
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

/**
 * Extension for managing YourDevice client devices
 * Handles device lifecycle across the cluster
 */
class YourDeviceExtension(
    private val deviceStore: IDeviceConfigStore,
    private val sessionHandler: SessionHandler,
    private val nodeId: String
) : AbstractVerticle() {

    private val logger = Logger.getLogger(this::class.java.name)

    // Track active device deployments
    private val activeDeployments = ConcurrentHashMap<String, String>() // deviceName -> deploymentId

    companion object {
        // EventBus addresses
        const val ADDRESS_DEVICE_CONFIG_CHANGED = "yourdevice.device.config.changed"
        const val ADDRESS_VALUE_PUBLISH = "yourdevice.value.publish"

        fun connectorMetrics(deviceName: String) = "yourdevice.connector.$deviceName.metrics"
    }

    override fun start(startPromise: Promise<Void>) {
        logger.info("Starting YourDevice Extension on node: $nodeId")

        // 1. Setup EventBus consumers
        setupEventBusConsumers()

        // 2. Load and deploy devices for this node
        loadAndDeployDevices()
            .onSuccess {
                logger.info("YourDevice Extension started successfully")
                startPromise.complete()
            }
            .onFailure { error ->
                logger.severe("Failed to start extension: ${error.message}")
                startPromise.fail(error)
            }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Stopping YourDevice Extension")

        // Undeploy all active connectors
        val futures = activeDeployments.values.map { deploymentId ->
            val promise = Promise.promise<Void>()
            vertx.undeploy(deploymentId) { result ->
                if (result.succeeded()) {
                    promise.complete()
                } else {
                    promise.fail(result.cause())
                }
            }
            promise.future()
        }

        Future.all(futures)
            .onComplete {
                activeDeployments.clear()
                logger.info("YourDevice Extension stopped")
                stopPromise.complete()
            }
    }

    /**
     * Load devices assigned to this node and deploy connectors
     */
    private fun loadAndDeployDevices(): Future<Void> {
        val promise = Promise.promise<Void>()

        deviceStore.getEnabledDevicesByNode(nodeId)
            .onSuccess { devices ->
                val yourDevices = devices.filter {
                    it.type == DeviceConfig.DEVICE_TYPE_YOURDEVICE_CLIENT
                }

                logger.info("Found ${yourDevices.size} YourDevice clients for node $nodeId")

                // Deploy each device
                val deployFutures = yourDevices.map { deployConnectorForDevice(it) }

                Future.all(deployFutures)
                    .onSuccess {
                        logger.info("All devices deployed successfully")
                        promise.complete()
                    }
                    .onFailure { error ->
                        logger.warning("Some devices failed to deploy: ${error.message}")
                        promise.complete() // Don't fail extension startup
                    }
            }
            .onFailure { error ->
                logger.severe("Failed to load devices: ${error.message}")
                promise.fail(error)
            }

        return promise.future()
    }

    /**
     * Deploy connector verticle for a device
     */
    private fun deployConnectorForDevice(device: DeviceConfig): Future<String> {
        val promise = Promise.promise<String>()

        logger.info("Deploying connector for device: ${device.name}")

        // Prepare deployment configuration
        val config = JsonObject()
            .put("device", device.toJsonObject())

        val options = DeploymentOptions()
            .setConfig(config)
            .setWorker(false)

        // Deploy verticle
        vertx.deployVerticle(
            YourDeviceConnector::class.java.name,
            options
        ) { result ->
            if (result.succeeded()) {
                val deploymentId = result.result()
                activeDeployments[device.name] = deploymentId
                logger.info("Deployed connector for ${device.name}: $deploymentId")
                promise.complete(deploymentId)
            } else {
                logger.severe("Failed to deploy connector for ${device.name}: ${result.cause().message}")
                promise.fail(result.cause())
            }
        }

        return promise.future()
    }

    /**
     * Undeploy connector verticle for a device
     */
    private fun undeployConnectorForDevice(deviceName: String): Future<Void> {
        val promise = Promise.promise<Void>()

        val deploymentId = activeDeployments.remove(deviceName)
        if (deploymentId == null) {
            logger.warning("No active deployment found for device: $deviceName")
            promise.complete()
            return promise.future()
        }

        logger.info("Undeploying connector for device: $deviceName")

        vertx.undeploy(deploymentId) { result ->
            if (result.succeeded()) {
                logger.info("Undeployed connector for $deviceName")
                promise.complete()
            } else {
                logger.severe("Failed to undeploy connector: ${result.cause().message}")
                promise.fail(result.cause())
            }
        }

        return promise.future()
    }

    /**
     * Setup EventBus consumers for configuration changes
     */
    private fun setupEventBusConsumers() {
        // Listen for configuration changes
        vertx.eventBus().consumer<JsonObject>(ADDRESS_DEVICE_CONFIG_CHANGED) { message ->
            handleDeviceConfigChange(message.body())
        }

        // Forward device data to MQTT bus
        vertx.eventBus().consumer<at.rocworks.data.BrokerMessage>(ADDRESS_VALUE_PUBLISH) { message ->
            sessionHandler.publishMessage(message.body())
        }
    }

    /**
     * Handle device configuration change notifications
     */
    private fun handleDeviceConfigChange(changeData: JsonObject) {
        val operation = changeData.getString("operation")
        val deviceName = changeData.getString("deviceName")
        val deviceJson = changeData.getJsonObject("device")

        logger.info("Received config change: $operation for device $deviceName")

        when (operation) {
            "add", "update", "toggle" -> {
                val device = DeviceConfig.fromJsonObject(deviceJson)

                // Only handle if assigned to this node
                if (device.nodeId != nodeId) {
                    logger.fine("Device $deviceName assigned to different node: ${device.nodeId}")
                    return
                }

                // Redeploy device
                undeployConnectorForDevice(deviceName)
                    .onComplete {
                        if (device.enabled) {
                            deployConnectorForDevice(device)
                        }
                    }
            }

            "delete" -> {
                undeployConnectorForDevice(deviceName)
            }

            "reassign" -> {
                val device = DeviceConfig.fromJsonObject(deviceJson)

                // If moved away from this node, undeploy
                if (device.nodeId != nodeId) {
                    undeployConnectorForDevice(deviceName)
                }
                // If moved to this node, deploy
                else if (device.enabled) {
                    deployConnectorForDevice(device)
                }
            }

            "addAddress", "deleteAddress" -> {
                // Redeploy to pick up address changes
                val device = DeviceConfig.fromJsonObject(deviceJson)
                if (device.nodeId == nodeId && device.enabled) {
                    undeployConnectorForDevice(deviceName)
                        .onComplete {
                            deployConnectorForDevice(device)
                        }
                }
            }
        }
    }
}
```

**Key Responsibilities**:
- Load devices assigned to current cluster node
- Deploy/undeploy connector verticles
- Listen to configuration changes via EventBus
- Forward device data to MQTT SessionHandler
- Handle cluster node reassignment

### 4. Integration with Monster.kt

**Location**: `src/main/kotlin/Monster.kt`

**Add device constant**:

```kotlin
// In DeviceConfig.kt
companion object {
    const val DEVICE_TYPE_YOURDEVICE_CLIENT = "YourDevice-Client"
    // ... other device types
}
```

**Deploy extension in Monster.kt** (around line 200-300):

```kotlin
// YourDevice Client Extension
if (config.getJsonObject("yourdevice")?.getBoolean("enabled") == true) {
    logger.info("Starting YourDevice client extension...")
    val yourDeviceExtension = YourDeviceExtension(
        deviceStore = deviceConfigStore,
        sessionHandler = sessionHandler,
        nodeId = nodeId
    )

    vertx.deployVerticle(yourDeviceExtension)
        .onSuccess {
            logger.info("YourDevice client extension deployed")
        }
        .onFailure { error ->
            logger.severe("Failed to deploy YourDevice extension: ${error.message}")
        }
}
```

**Update config.yaml**:

```yaml
yourdevice:
  enabled: true
```

---

## GraphQL API Layer

### 1. Schema Definitions

**Location**: `src/main/resources/schema.graphqls`

**Add types, queries, mutations, and inputs**:

```graphql
# ============================================================================
# YourDevice Client Types
# ============================================================================

type YourDeviceClient {
    name: String!
    namespace: String!
    nodeId: String!
    config: YourDeviceConnectionConfig!
    enabled: Boolean!
    createdAt: String!
    updatedAt: String!
    isOnCurrentNode: Boolean!
    metrics: YourDeviceClientMetrics
}

type YourDeviceConnectionConfig {
    hostname: String!
    port: Int!
    username: String
    reconnectDelay: Long!
    addresses: [YourDeviceAddress!]!
}

type YourDeviceAddress {
    type: YourDeviceAddressType!
    topic: String!
    description: String
    retained: Boolean!
}

enum YourDeviceAddressType {
    SUBSCRIPTION
    COMMAND
}

type YourDeviceClientMetrics {
    messagesIn: Float!
    messagesOut: Float!
    connected: Boolean!
    timestamp: String!
}

type YourDeviceClientResult {
    success: Boolean!
    errors: [String!]
    client: YourDeviceClient
}

# ============================================================================
# YourDevice Client Queries
# ============================================================================

extend type Query {
    yourDeviceClients: [YourDeviceClient!]!
    yourDeviceClient(name: String!): YourDeviceClient
    yourDeviceClientsByNode(nodeId: String!): [YourDeviceClient!]!
}

# ============================================================================
# YourDevice Client Mutations
# ============================================================================

type YourDeviceClientMutations {
    create(input: YourDeviceClientInput!): YourDeviceClientResult!
    update(name: String!, input: YourDeviceClientInput!): YourDeviceClientResult!
    delete(name: String!): Boolean!
    start(name: String!): YourDeviceClientResult!
    stop(name: String!): YourDeviceClientResult!
    toggle(name: String!, enabled: Boolean!): YourDeviceClientResult!
    reassign(name: String!, nodeId: String!): YourDeviceClientResult!
    addAddress(deviceName: String!, input: YourDeviceAddressInput!): YourDeviceClientResult!
    deleteAddress(deviceName: String!, topic: String!): YourDeviceClientResult!
}

extend type Mutation {
    yourDeviceClient: YourDeviceClientMutations!
}

# ============================================================================
# YourDevice Client Inputs
# ============================================================================

input YourDeviceClientInput {
    name: String!
    namespace: String!
    nodeId: String!
    enabled: Boolean = true
    config: YourDeviceConnectionConfigInput!
}

input YourDeviceConnectionConfigInput {
    hostname: String!
    port: Int = 443
    username: String
    password: String
    reconnectDelay: Long = 5000
}

input YourDeviceAddressInput {
    type: YourDeviceAddressType!
    topic: String!
    description: String
    retained: Boolean = false
}
```

**Placement**: Add after existing device types (MQTT, OPC UA, etc.), before User Management section

### 2. Query Resolvers

**Location**: `src/main/kotlin/graphql/YourDeviceClientConfigQueries.kt`

```kotlin
package at.rocworks.graphql

import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.YourDeviceConnectionConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture

class YourDeviceClientConfigQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {

    /**
     * Query all YourDevice clients
     */
    fun yourDeviceClients(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any>>>()

            deviceStore.getAllDevices()
                .onSuccess { devices ->
                    val yourDeviceClients = devices
                        .filter { it.type == DeviceConfig.DEVICE_TYPE_YOURDEVICE_CLIENT }
                        .map { deviceToMap(it) }

                    future.complete(yourDeviceClients)
                }
                .onFailure { error ->
                    future.completeExceptionally(error)
                }

            future
        }
    }

    /**
     * Query single YourDevice client by name
     */
    fun yourDeviceClient(): DataFetcher<CompletableFuture<Map<String, Any>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>?>()
            val name: String = env.getArgument("name")

            deviceStore.getDevice(name)
                .onSuccess { device ->
                    if (device == null || device.type != DeviceConfig.DEVICE_TYPE_YOURDEVICE_CLIENT) {
                        future.complete(null)
                    } else {
                        future.complete(deviceToMap(device))
                    }
                }
                .onFailure { error ->
                    future.completeExceptionally(error)
                }

            future
        }
    }

    /**
     * Query YourDevice clients by cluster node
     */
    fun yourDeviceClientsByNode(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any>>>()
            val nodeId: String = env.getArgument("nodeId")

            deviceStore.getDevicesByNode(nodeId)
                .onSuccess { devices ->
                    val yourDeviceClients = devices
                        .filter { it.type == DeviceConfig.DEVICE_TYPE_YOURDEVICE_CLIENT }
                        .map { deviceToMap(it) }

                    future.complete(yourDeviceClients)
                }
                .onFailure { error ->
                    future.completeExceptionally(error)
                }

            future
        }
    }

    /**
     * Convert DeviceConfig to Map for GraphQL
     */
    private fun deviceToMap(device: DeviceConfig): Map<String, Any> {
        val config = YourDeviceConnectionConfig.fromJsonObject(device.config)

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "enabled" to device.enabled,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString(),
            "isOnCurrentNode" to (device.nodeId == System.getProperty("node.id", "node1")),
            "config" to mapOf(
                "hostname" to config.hostname,
                "port" to config.port,
                "username" to (config.username ?: ""),
                "reconnectDelay" to config.reconnectDelay,
                "addresses" to config.addresses.map { address ->
                    mapOf(
                        "type" to address.type.name,
                        "topic" to address.topic,
                        "description" to (address.description ?: ""),
                        "retained" to address.retained
                    )
                }
            )
        )
    }
}
```

### 3. Mutation Resolvers

**Location**: `src/main/kotlin/graphql/YourDeviceClientConfigMutations.kt`

```kotlin
package at.rocworks.graphql

import at.rocworks.devices.yourdevice.YourDeviceExtension
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.*
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture

class YourDeviceClientConfigMutations(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {

    /**
     * Create new YourDevice client
     */
    fun createYourDeviceClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            val input: Map<String, Any> = env.getArgument("input")

            try {
                // Parse and validate input
                val request = parseDeviceConfigRequest(input)
                val validationErrors = request.validate()

                if (validationErrors.isNotEmpty()) {
                    future.complete(mapOf(
                        "success" to false,
                        "errors" to validationErrors,
                        "client" to null
                    ))
                    return@DataFetcher future
                }

                // Check if name already exists
                deviceStore.getDevice(request.name)
                    .compose { existingDevice ->
                        if (existingDevice != null) {
                            throw IllegalArgumentException("Device with name '${request.name}' already exists")
                        }

                        // Check if namespace is in use
                        deviceStore.isNamespaceInUse(request.namespace, null)
                    }
                    .compose { namespaceInUse ->
                        if (namespaceInUse) {
                            throw IllegalArgumentException("Namespace '${request.namespace}' is already in use")
                        }

                        // Create device config
                        val device = request.toDeviceConfig()
                        deviceStore.saveDevice(device)
                    }
                    .onSuccess { savedDevice ->
                        // Notify extension
                        notifyDeviceConfigChange("add", savedDevice)

                        // Return success
                        future.complete(mapOf(
                            "success" to true,
                            "errors" to emptyList<String>(),
                            "client" to deviceToMap(savedDevice)
                        ))
                    }
                    .onFailure { error ->
                        future.complete(mapOf(
                            "success" to false,
                            "errors" to listOf(error.message ?: "Unknown error"),
                            "client" to null
                        ))
                    }

            } catch (e: Exception) {
                future.complete(mapOf(
                    "success" to false,
                    "errors" to listOf(e.message ?: "Failed to create client"),
                    "client" to null
                ))
            }

            future
        }
    }

    /**
     * Update existing YourDevice client
     */
    fun updateYourDeviceClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            val name: String = env.getArgument("name")
            val input: Map<String, Any> = env.getArgument("input")

            try {
                val request = parseDeviceConfigRequest(input)
                val validationErrors = request.validate()

                if (validationErrors.isNotEmpty()) {
                    future.complete(mapOf(
                        "success" to false,
                        "errors" to validationErrors,
                        "client" to null
                    ))
                    return@DataFetcher future
                }

                // Get existing device
                deviceStore.getDevice(name)
                    .compose { existingDevice ->
                        if (existingDevice == null) {
                            throw IllegalArgumentException("Device '$name' not found")
                        }

                        // Check namespace conflict (exclude current device)
                        deviceStore.isNamespaceInUse(request.namespace, name)
                            .map { inUse ->
                                if (inUse && request.namespace != existingDevice.namespace) {
                                    throw IllegalArgumentException("Namespace '${request.namespace}' is already in use")
                                }
                                existingDevice
                            }
                    }
                    .compose { existingDevice ->
                        // Update device
                        val updatedDevice = existingDevice.copy(
                            namespace = request.namespace,
                            nodeId = request.nodeId,
                            config = request.config.toJsonObject(),
                            enabled = request.enabled,
                            updatedAt = Instant.now()
                        )

                        deviceStore.saveDevice(updatedDevice)
                    }
                    .onSuccess { updatedDevice ->
                        // Notify extension
                        notifyDeviceConfigChange("update", updatedDevice)

                        future.complete(mapOf(
                            "success" to true,
                            "errors" to emptyList<String>(),
                            "client" to deviceToMap(updatedDevice)
                        ))
                    }
                    .onFailure { error ->
                        future.complete(mapOf(
                            "success" to false,
                            "errors" to listOf(error.message ?: "Unknown error"),
                            "client" to null
                        ))
                    }

            } catch (e: Exception) {
                future.complete(mapOf(
                    "success" to false,
                    "errors" to listOf(e.message ?: "Failed to update client"),
                    "client" to null
                ))
            }

            future
        }
    }

    /**
     * Delete YourDevice client
     */
    fun deleteYourDeviceClient(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Boolean>()
            val name: String = env.getArgument("name")

            deviceStore.deleteDevice(name)
                .onSuccess { deleted ->
                    if (deleted) {
                        // Notify extension
                        val changeData = JsonObject()
                            .put("operation", "delete")
                            .put("deviceName", name)

                        vertx.eventBus().publish(
                            YourDeviceExtension.ADDRESS_DEVICE_CONFIG_CHANGED,
                            changeData
                        )
                    }
                    future.complete(deleted)
                }
                .onFailure { error ->
                    future.completeExceptionally(error)
                }

            future
        }
    }

    /**
     * Toggle YourDevice client enabled/disabled
     */
    fun toggleYourDeviceClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            val name: String = env.getArgument("name")
            val enabled: Boolean = env.getArgument("enabled")

            deviceStore.toggleDevice(name, enabled)
                .onSuccess { device ->
                    if (device == null) {
                        future.complete(mapOf(
                            "success" to false,
                            "errors" to listOf("Device '$name' not found"),
                            "client" to null
                        ))
                    } else {
                        // Notify extension
                        notifyDeviceConfigChange("toggle", device)

                        future.complete(mapOf(
                            "success" to true,
                            "errors" to emptyList<String>(),
                            "client" to deviceToMap(device)
                        ))
                    }
                }
                .onFailure { error ->
                    future.complete(mapOf(
                        "success" to false,
                        "errors" to listOf(error.message ?: "Unknown error"),
                        "client" to null
                    ))
                }

            future
        }
    }

    /**
     * Reassign device to different cluster node
     */
    fun reassignYourDeviceClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            val name: String = env.getArgument("name")
            val nodeId: String = env.getArgument("nodeId")

            deviceStore.reassignDevice(name, nodeId)
                .onSuccess { device ->
                    if (device == null) {
                        future.complete(mapOf(
                            "success" to false,
                            "errors" to listOf("Device '$name' not found"),
                            "client" to null
                        ))
                    } else {
                        // Notify extension
                        notifyDeviceConfigChange("reassign", device)

                        future.complete(mapOf(
                            "success" to true,
                            "errors" to emptyList<String>(),
                            "client" to deviceToMap(device)
                        ))
                    }
                }
                .onFailure { error ->
                    future.complete(mapOf(
                        "success" to false,
                        "errors" to listOf(error.message ?: "Unknown error"),
                        "client" to null
                    ))
                }

            future
        }
    }

    /**
     * Add address to YourDevice client
     */
    fun addYourDeviceClientAddress(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            val deviceName: String = env.getArgument("deviceName")
            val addressInput: Map<String, Any> = env.getArgument("input")

            try {
                // Parse address
                val address = parseAddressInput(addressInput)

                // Get device
                deviceStore.getDevice(deviceName)
                    .compose { device ->
                        if (device == null) {
                            throw IllegalArgumentException("Device '$deviceName' not found")
                        }

                        // Parse config and add address
                        val config = YourDeviceConnectionConfig.fromJsonObject(device.config)
                        val updatedAddresses = config.addresses + address
                        val updatedConfig = config.copy(addresses = updatedAddresses)

                        // Save updated device
                        val updatedDevice = device.copy(
                            config = updatedConfig.toJsonObject(),
                            updatedAt = Instant.now()
                        )

                        deviceStore.saveDevice(updatedDevice)
                    }
                    .onSuccess { updatedDevice ->
                        // Notify extension
                        notifyDeviceConfigChange("addAddress", updatedDevice)

                        future.complete(mapOf(
                            "success" to true,
                            "errors" to emptyList<String>(),
                            "client" to deviceToMap(updatedDevice)
                        ))
                    }
                    .onFailure { error ->
                        future.complete(mapOf(
                            "success" to false,
                            "errors" to listOf(error.message ?: "Unknown error"),
                            "client" to null
                        ))
                    }

            } catch (e: Exception) {
                future.complete(mapOf(
                    "success" to false,
                    "errors" to listOf(e.message ?: "Failed to add address"),
                    "client" to null
                ))
            }

            future
        }
    }

    // Helper methods

    private fun parseDeviceConfigRequest(input: Map<String, Any>): YourDeviceConfigRequest {
        val name = input["name"] as String
        val namespace = input["namespace"] as String
        val nodeId = input["nodeId"] as String
        val enabled = input["enabled"] as? Boolean ?: true
        val configInput = input["config"] as Map<String, Any>

        val config = parseConnectionConfig(configInput)

        return YourDeviceConfigRequest(name, namespace, nodeId, enabled, config)
    }

    private fun parseConnectionConfig(input: Map<String, Any>): YourDeviceConnectionConfig {
        val hostname = input["hostname"] as String
        val port = (input["port"] as? Int) ?: 443
        val username = input["username"] as? String
        val password = input["password"] as? String
        val reconnectDelay = (input["reconnectDelay"] as? Number)?.toLong() ?: 5000L

        return YourDeviceConnectionConfig(
            hostname = hostname,
            port = port,
            username = username,
            password = password,
            reconnectDelay = reconnectDelay,
            addresses = emptyList() // Addresses added separately
        )
    }

    private fun parseAddressInput(input: Map<String, Any>): YourDeviceAddress {
        val type = YourDeviceAddressType.valueOf(input["type"] as String)
        val topic = input["topic"] as String
        val description = input["description"] as? String
        val retained = input["retained"] as? Boolean ?: false

        return YourDeviceAddress(type, topic, description, retained)
    }

    private fun notifyDeviceConfigChange(operation: String, device: DeviceConfig) {
        val changeData = JsonObject()
            .put("operation", operation)
            .put("deviceName", device.name)
            .put("device", device.toJsonObject())

        vertx.eventBus().publish(
            YourDeviceExtension.ADDRESS_DEVICE_CONFIG_CHANGED,
            changeData
        )
    }

    private fun deviceToMap(device: DeviceConfig): Map<String, Any> {
        // Use same logic as YourDeviceClientConfigQueries
        val queries = YourDeviceClientConfigQueries(vertx, deviceStore)
        return queries.deviceToMap(device)
    }
}

// Request models
data class YourDeviceConfigRequest(
    val name: String,
    val namespace: String,
    val nodeId: String,
    val enabled: Boolean,
    val config: YourDeviceConnectionConfig
) {
    fun validate(): List<String> {
        val errors = mutableListOf<String>()

        if (name.isBlank()) errors.add("Name cannot be empty")
        if (namespace.isBlank()) errors.add("Namespace cannot be empty")
        if (nodeId.isBlank()) errors.add("Node ID cannot be empty")
        if (config.hostname.isBlank()) errors.add("Hostname cannot be empty")
        if (config.port !in 1..65535) errors.add("Port must be between 1 and 65535")

        return errors
    }

    fun toDeviceConfig(): DeviceConfig {
        return DeviceConfig(
            name = name,
            namespace = namespace,
            nodeId = nodeId,
            config = config.toJsonObject(),
            enabled = enabled,
            type = DeviceConfig.DEVICE_TYPE_YOURDEVICE_CLIENT,
            createdAt = Instant.now(),
            updatedAt = Instant.now()
        )
    }
}
```

### 4. Register with GraphQLServer

**Location**: `src/main/kotlin/graphql/GraphQLServer.kt`

**Add initialization** (around line 100-150):

```kotlin
// YourDevice Client resolvers
val yourDeviceClientQueries = YourDeviceClientConfigQueries(vertx, deviceConfigStore)
val yourDeviceClientMutations = YourDeviceClientConfigMutations(vertx, deviceConfigStore)
```

**Register queries** (around line 200-250):

```kotlin
.type("Query") { builder ->
    builder
        // ... existing queries ...
        .dataFetcher("yourDeviceClients", yourDeviceClientQueries.yourDeviceClients())
        .dataFetcher("yourDeviceClient", yourDeviceClientQueries.yourDeviceClient())
        .dataFetcher("yourDeviceClientsByNode", yourDeviceClientQueries.yourDeviceClientsByNode())
}
```

**Register mutations** (around line 300-400):

```kotlin
.type("Mutation") { builder ->
    builder
        // ... existing mutations ...
        .dataFetcher("yourDeviceClient") { _ -> emptyMap<String, Any>() }
}

// YourDevice Client Mutations type
.type("YourDeviceClientMutations") { builder ->
    builder
        .dataFetcher("create", yourDeviceClientMutations.createYourDeviceClient())
        .dataFetcher("update", yourDeviceClientMutations.updateYourDeviceClient())
        .dataFetcher("delete", yourDeviceClientMutations.deleteYourDeviceClient())
        .dataFetcher("toggle", yourDeviceClientMutations.toggleYourDeviceClient())
        .dataFetcher("reassign", yourDeviceClientMutations.reassignYourDeviceClient())
        .dataFetcher("addAddress", yourDeviceClientMutations.addYourDeviceClientAddress())
        .dataFetcher("deleteAddress", yourDeviceClientMutations.deleteYourDeviceClientAddress())
}
```

---

## Frontend Dashboard Integration

### 1. List View Page

**Location**: `src/main/resources/dashboard/js/yourdevice-clients.js`

**Template based on mqtt-clients.js**:

```javascript
/**
 * YourDevice Client Manager
 * Manages list of YourDevice client devices
 */

class YourDeviceClientManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
        this.clients = [];
        this.clusterNodes = [];
        this.deleteClientName = null;
    }

    async init() {
        console.log('Initializing YourDevice Client Manager');

        // Load cluster nodes for dropdown
        await this.loadClusterNodes();

        // Load clients
        await this.loadClients();

        // Setup auto-refresh (every 30 seconds)
        setInterval(() => this.loadClients(), 30000);

        // Setup event listeners
        this.setupEventListeners();
    }

    async loadClusterNodes() {
        try {
            const query = `
                query GetBrokers {
                    brokers {
                        nodeId
                    }
                }
            `;

            const data = await this.client.query(query);
            this.clusterNodes = data.brokers.map(b => b.nodeId);

            // Populate node select dropdowns
            const nodeSelects = document.querySelectorAll('#addNodeId, #editNodeId');
            nodeSelects.forEach(select => {
                select.innerHTML = this.clusterNodes.map(nodeId =>
                    `<option value="${nodeId}">${nodeId}</option>`
                ).join('');
            });

        } catch (error) {
            console.error('Error loading cluster nodes:', error);
            showToast('Error loading cluster nodes', 'error');
        }
    }

    async loadClients() {
        try {
            const query = `
                query GetYourDeviceClients {
                    yourDeviceClients {
                        name
                        namespace
                        nodeId
                        enabled
                        isOnCurrentNode
                        createdAt
                        updatedAt
                        config {
                            hostname
                            port
                            username
                            reconnectDelay
                            addresses {
                                type
                                topic
                                description
                                retained
                            }
                        }
                        metrics {
                            messagesIn
                            messagesOut
                            connected
                        }
                    }
                }
            `;

            const data = await this.client.query(query);
            this.clients = data.yourDeviceClients;

            // Update metrics and render
            this.updateMetrics();
            this.renderClientsTable();

        } catch (error) {
            console.error('Error loading clients:', error);
            showToast('Error loading YourDevice clients', 'error');
        }
    }

    updateMetrics() {
        const total = this.clients.length;
        const enabled = this.clients.filter(c => c.enabled).length;
        const onCurrentNode = this.clients.filter(c => c.isOnCurrentNode).length;
        const totalAddresses = this.clients.reduce((sum, c) => sum + c.config.addresses.length, 0);

        document.getElementById('totalClients').textContent = total;
        document.getElementById('enabledClients').textContent = enabled;
        document.getElementById('currentNodeClients').textContent = onCurrentNode;
        document.getElementById('totalAddresses').textContent = totalAddresses;
    }

    renderClientsTable() {
        const tbody = document.querySelector('#clientsTable tbody');

        if (this.clients.length === 0) {
            tbody.innerHTML = '<tr><td colspan="8" class="text-center">No YourDevice clients configured</td></tr>';
            return;
        }

        tbody.innerHTML = this.clients.map(client => {
            const statusBadge = client.enabled
                ? '<span class="badge bg-success">Enabled</span>'
                : '<span class="badge bg-secondary">Disabled</span>';

            const nodeBadge = client.isOnCurrentNode
                ? `<span class="badge bg-primary">${client.nodeId}</span>`
                : `<span class="badge bg-secondary">${client.nodeId}</span>`;

            const connectionUrl = `${client.config.hostname}:${client.config.port}`;
            const addressCount = client.config.addresses.length;

            const messagesIn = client.metrics?.messagesIn?.toFixed(2) || '0.00';
            const messagesOut = client.metrics?.messagesOut?.toFixed(2) || '0.00';
            const connected = client.metrics?.connected
                ? '<span class="badge bg-success">Connected</span>'
                : '<span class="badge bg-danger">Disconnected</span>';

            return `
                <tr>
                    <td><strong>${client.name}</strong></td>
                    <td>${connectionUrl}</td>
                    <td><code>${client.namespace}</code></td>
                    <td>${nodeBadge}</td>
                    <td>${statusBadge}</td>
                    <td>${addressCount}</td>
                    <td>
                        ${messagesIn} / ${messagesOut}<br>
                        ${connected}
                    </td>
                    <td>
                        <button class="btn btn-sm btn-primary" onclick="yourDeviceManager.viewClient('${client.name}')">
                            <i class="bi bi-eye"></i> View
                        </button>
                        <button class="btn btn-sm btn-${client.enabled ? 'warning' : 'success'}"
                                onclick="yourDeviceManager.toggleClient('${client.name}', ${!client.enabled})">
                            <i class="bi bi-${client.enabled ? 'pause' : 'play'}-fill"></i>
                        </button>
                        <button class="btn btn-sm btn-danger" onclick="yourDeviceManager.confirmDelete('${client.name}')">
                            <i class="bi bi-trash"></i>
                        </button>
                    </td>
                </tr>
            `;
        }).join('');
    }

    setupEventListeners() {
        // Add client form submission
        document.getElementById('addClientForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            await this.addClient();
        });

        // Confirm delete
        document.getElementById('confirmDeleteBtn').addEventListener('click', async () => {
            if (this.deleteClientName) {
                await this.deleteClient(this.deleteClientName);
                this.deleteClientName = null;
            }
        });
    }

    async addClient() {
        try {
            const formData = {
                name: document.getElementById('addName').value,
                namespace: document.getElementById('addNamespace').value,
                nodeId: document.getElementById('addNodeId').value,
                enabled: document.getElementById('addEnabled').checked,
                config: {
                    hostname: document.getElementById('addHostname').value,
                    port: parseInt(document.getElementById('addPort').value),
                    username: document.getElementById('addUsername').value || null,
                    password: document.getElementById('addPassword').value || null,
                    reconnectDelay: parseInt(document.getElementById('addReconnectDelay').value)
                }
            };

            const mutation = `
                mutation CreateYourDeviceClient($input: YourDeviceClientInput!) {
                    yourDeviceClient {
                        create(input: $input) {
                            success
                            errors
                            client {
                                name
                            }
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, { input: formData });

            if (result.yourDeviceClient.create.success) {
                showToast('YourDevice client created successfully', 'success');
                bootstrap.Modal.getInstance(document.getElementById('addClientModal')).hide();
                document.getElementById('addClientForm').reset();
                await this.loadClients();
            } else {
                const errors = result.yourDeviceClient.create.errors.join(', ');
                showToast(`Failed to create client: ${errors}`, 'error');
            }

        } catch (error) {
            console.error('Error creating client:', error);
            showToast('Error creating client', 'error');
        }
    }

    async toggleClient(name, enabled) {
        try {
            const mutation = `
                mutation ToggleYourDeviceClient($name: String!, $enabled: Boolean!) {
                    yourDeviceClient {
                        toggle(name: $name, enabled: $enabled) {
                            success
                            errors
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, { name, enabled });

            if (result.yourDeviceClient.toggle.success) {
                showToast(`Client ${enabled ? 'enabled' : 'disabled'} successfully`, 'success');
                await this.loadClients();
            } else {
                const errors = result.yourDeviceClient.toggle.errors.join(', ');
                showToast(`Failed to toggle client: ${errors}`, 'error');
            }

        } catch (error) {
            console.error('Error toggling client:', error);
            showToast('Error toggling client', 'error');
        }
    }

    confirmDelete(name) {
        this.deleteClientName = name;
        const modal = new bootstrap.Modal(document.getElementById('deleteConfirmModal'));
        document.getElementById('deleteClientName').textContent = name;
        modal.show();
    }

    async deleteClient(name) {
        try {
            const mutation = `
                mutation DeleteYourDeviceClient($name: String!) {
                    yourDeviceClient {
                        delete(name: $name)
                    }
                }
            `;

            const result = await this.client.query(mutation, { name });

            if (result.yourDeviceClient.delete) {
                showToast('Client deleted successfully', 'success');
                bootstrap.Modal.getInstance(document.getElementById('deleteConfirmModal')).hide();
                await this.loadClients();
            } else {
                showToast('Failed to delete client', 'error');
            }

        } catch (error) {
            console.error('Error deleting client:', error);
            showToast('Error deleting client', 'error');
        }
    }

    viewClient(name) {
        window.location.href = `yourdevice-client-detail.html?name=${encodeURIComponent(name)}`;
    }
}

// Initialize on page load
let yourDeviceManager;
document.addEventListener('DOMContentLoaded', () => {
    yourDeviceManager = new YourDeviceClientManager();
    yourDeviceManager.init();
});

// Utility function for toast notifications
function showToast(message, type = 'info') {
    // Use your toast implementation
    console.log(`[${type}] ${message}`);
}
```

**Create HTML page** at `src/main/resources/dashboard/yourdevice-clients.html`:

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>YourDevice Clients - MonsterMQ</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.0/font/bootstrap-icons.css" rel="stylesheet">
    <link href="css/dashboard.css" rel="stylesheet">
</head>
<body>
    <div id="sidebar-placeholder"></div>

    <main class="main-content">
        <div class="container-fluid">
            <div class="d-flex justify-content-between align-items-center mb-4">
                <h1>YourDevice Clients</h1>
                <button class="btn btn-primary" data-bs-toggle="modal" data-bs-target="#addClientModal">
                    <i class="bi bi-plus-circle"></i> Add Client
                </button>
            </div>

            <!-- Metrics Cards -->
            <div class="row mb-4">
                <div class="col-md-3">
                    <div class="card">
                        <div class="card-body">
                            <h6 class="card-subtitle mb-2 text-muted">Total Clients</h6>
                            <h2 class="card-title" id="totalClients">0</h2>
                        </div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="card">
                        <div class="card-body">
                            <h6 class="card-subtitle mb-2 text-muted">Enabled</h6>
                            <h2 class="card-title" id="enabledClients">0</h2>
                        </div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="card">
                        <div class="card-body">
                            <h6 class="card-subtitle mb-2 text-muted">On Current Node</h6>
                            <h2 class="card-title" id="currentNodeClients">0</h2>
                        </div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="card">
                        <div class="card-body">
                            <h6 class="card-subtitle mb-2 text-muted">Total Addresses</h6>
                            <h2 class="card-title" id="totalAddresses">0</h2>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Clients Table -->
            <div class="card">
                <div class="card-body">
                    <table class="table table-hover" id="clientsTable">
                        <thead>
                            <tr>
                                <th>Name</th>
                                <th>Connection</th>
                                <th>Namespace</th>
                                <th>Node</th>
                                <th>Status</th>
                                <th>Addresses</th>
                                <th>Messages (In/Out)</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr><td colspan="8" class="text-center">Loading...</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </main>

    <!-- Add Client Modal -->
    <div class="modal fade" id="addClientModal" tabindex="-1">
        <div class="modal-dialog modal-lg">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title">Add YourDevice Client</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">
                    <form id="addClientForm">
                        <div class="mb-3">
                            <label for="addName" class="form-label">Name *</label>
                            <input type="text" class="form-control" id="addName" required>
                        </div>

                        <div class="mb-3">
                            <label for="addNamespace" class="form-label">Namespace *</label>
                            <input type="text" class="form-control" id="addNamespace" required
                                   placeholder="e.g., plant1/yourdevice">
                        </div>

                        <div class="mb-3">
                            <label for="addNodeId" class="form-label">Cluster Node *</label>
                            <select class="form-select" id="addNodeId" required></select>
                        </div>

                        <div class="mb-3 form-check">
                            <input type="checkbox" class="form-check-input" id="addEnabled" checked>
                            <label class="form-check-label" for="addEnabled">Enabled</label>
                        </div>

                        <h6>Connection Settings</h6>
                        <hr>

                        <div class="row">
                            <div class="col-md-8">
                                <div class="mb-3">
                                    <label for="addHostname" class="form-label">Hostname *</label>
                                    <input type="text" class="form-control" id="addHostname" required>
                                </div>
                            </div>
                            <div class="col-md-4">
                                <div class="mb-3">
                                    <label for="addPort" class="form-label">Port *</label>
                                    <input type="number" class="form-control" id="addPort" value="443" required>
                                </div>
                            </div>
                        </div>

                        <div class="row">
                            <div class="col-md-6">
                                <div class="mb-3">
                                    <label for="addUsername" class="form-label">Username</label>
                                    <input type="text" class="form-control" id="addUsername">
                                </div>
                            </div>
                            <div class="col-md-6">
                                <div class="mb-3">
                                    <label for="addPassword" class="form-label">Password</label>
                                    <input type="password" class="form-control" id="addPassword">
                                </div>
                            </div>
                        </div>

                        <div class="mb-3">
                            <label for="addReconnectDelay" class="form-label">Reconnect Delay (ms)</label>
                            <input type="number" class="form-control" id="addReconnectDelay" value="5000">
                        </div>

                        <button type="submit" class="btn btn-primary">Create Client</button>
                    </form>
                </div>
            </div>
        </div>
    </div>

    <!-- Delete Confirmation Modal -->
    <div class="modal fade" id="deleteConfirmModal" tabindex="-1">
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title">Confirm Delete</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">
                    Are you sure you want to delete client <strong id="deleteClientName"></strong>?
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                    <button type="button" class="btn btn-danger" id="confirmDeleteBtn">Delete</button>
                </div>
            </div>
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script src="js/graphql-client.js"></script>
    <script src="js/sidebar.js"></script>
    <script src="js/yourdevice-clients.js"></script>
</body>
</html>
```

### 2. Detail View Page

**Location**: `src/main/resources/dashboard/js/yourdevice-client-detail.js`

**Template based on mqtt-client-detail.js** (abbreviated for brevity):

```javascript
class YourDeviceClientDetailManager {
    constructor() {
        this.client = new GraphQLDashboardClient('/graphql');
        this.clientName = new URLSearchParams(window.location.search).get('name');
        this.clientData = null;
        this.clusterNodes = [];
        this.deleteAddressTopic = null;
    }

    async init() {
        if (!this.clientName) {
            showToast('No client name provided', 'error');
            window.location.href = 'yourdevice-clients.html';
            return;
        }

        await this.loadClusterNodes();
        await this.loadClientData();

        // Auto-refresh every 30 seconds
        setInterval(() => this.loadClientData(), 30000);

        this.setupEventListeners();
    }

    async loadClientData() {
        try {
            const query = `
                query GetYourDeviceClient($name: String!) {
                    yourDeviceClient(name: $name) {
                        name
                        namespace
                        nodeId
                        enabled
                        createdAt
                        updatedAt
                        isOnCurrentNode
                        config {
                            hostname
                            port
                            username
                            reconnectDelay
                            addresses {
                                type
                                topic
                                description
                                retained
                            }
                        }
                        metrics {
                            messagesIn
                            messagesOut
                            connected
                        }
                    }
                }
            `;

            const data = await this.client.query(query, { name: this.clientName });

            if (!data.yourDeviceClient) {
                showToast('Client not found', 'error');
                window.location.href = 'yourdevice-clients.html';
                return;
            }

            this.clientData = data.yourDeviceClient;
            this.renderClientInfo();
            this.renderAddressesList();

        } catch (error) {
            console.error('Error loading client:', error);
            showToast('Error loading client data', 'error');
        }
    }

    renderClientInfo() {
        document.getElementById('clientName').textContent = this.clientData.name;
        document.getElementById('clientNamespace').textContent = this.clientData.namespace;
        document.getElementById('clientNodeId').textContent = this.clientData.nodeId;

        const statusBadge = this.clientData.enabled
            ? '<span class="badge bg-success">Enabled</span>'
            : '<span class="badge bg-secondary">Disabled</span>';
        document.getElementById('clientStatus').innerHTML = statusBadge;

        // Connection details
        document.getElementById('clientHostname').textContent = this.clientData.config.hostname;
        document.getElementById('clientPort').textContent = this.clientData.config.port;
        document.getElementById('clientUsername').textContent = this.clientData.config.username || 'Not set';
        document.getElementById('clientReconnectDelay').textContent = this.clientData.config.reconnectDelay;

        // Metrics
        if (this.clientData.metrics) {
            document.getElementById('messagesIn').textContent =
                this.clientData.metrics.messagesIn?.toFixed(2) || '0.00';
            document.getElementById('messagesOut').textContent =
                this.clientData.metrics.messagesOut?.toFixed(2) || '0.00';

            const connectionBadge = this.clientData.metrics.connected
                ? '<span class="badge bg-success">Connected</span>'
                : '<span class="badge bg-danger">Disconnected</span>';
            document.getElementById('connectionStatus').innerHTML = connectionBadge;
        }
    }

    renderAddressesList() {
        const tbody = document.querySelector('#addressesTable tbody');
        const addresses = this.clientData.config.addresses;

        if (addresses.length === 0) {
            tbody.innerHTML = '<tr><td colspan="4" class="text-center">No addresses configured</td></tr>';
            return;
        }

        tbody.innerHTML = addresses.map(addr => {
            const typeBadge = addr.type === 'SUBSCRIPTION'
                ? '<span class="badge bg-info">⬇️ SUBSCRIPTION</span>'
                : '<span class="badge bg-warning">⬆️ COMMAND</span>';

            const retainedBadge = addr.retained
                ? '<span class="badge bg-secondary">Retained</span>'
                : '';

            return `
                <tr>
                    <td>${typeBadge}</td>
                    <td><code>${addr.topic}</code></td>
                    <td>${addr.description || '-'} ${retainedBadge}</td>
                    <td>
                        <button class="btn btn-sm btn-danger"
                                onclick="detailManager.confirmDeleteAddress('${addr.topic}')">
                            <i class="bi bi-trash"></i>
                        </button>
                    </td>
                </tr>
            `;
        }).join('');
    }

    async addAddress() {
        try {
            const addressData = {
                type: document.getElementById('addAddressType').value,
                topic: document.getElementById('addAddressTopic').value,
                description: document.getElementById('addAddressDescription').value || null,
                retained: document.getElementById('addAddressRetained').checked
            };

            const mutation = `
                mutation AddYourDeviceClientAddress($deviceName: String!, $input: YourDeviceAddressInput!) {
                    yourDeviceClient {
                        addAddress(deviceName: $deviceName, input: $input) {
                            success
                            errors
                        }
                    }
                }
            `;

            const result = await this.client.query(mutation, {
                deviceName: this.clientName,
                input: addressData
            });

            if (result.yourDeviceClient.addAddress.success) {
                showToast('Address added successfully', 'success');
                bootstrap.Modal.getInstance(document.getElementById('addAddressModal')).hide();
                document.getElementById('addAddressForm').reset();
                await this.loadClientData();
            } else {
                const errors = result.yourDeviceClient.addAddress.errors.join(', ');
                showToast(`Failed to add address: ${errors}`, 'error');
            }

        } catch (error) {
            console.error('Error adding address:', error);
            showToast('Error adding address', 'error');
        }
    }

    // ... similar methods for update, toggle, delete
}

// Initialize on page load
let detailManager;
document.addEventListener('DOMContentLoaded', () => {
    detailManager = new YourDeviceClientDetailManager();
    detailManager.init();
});
```

### 3. Register in Sidebar Navigation

**Location**: `src/main/resources/dashboard/js/sidebar.js`

**Add menu item**:

```javascript
const sidebarHTML = `
    <nav id="sidebar" class="sidebar">
        <!-- ... existing menu items ... -->

        <div class="nav-section">Integrations</div>
        <ul class="nav flex-column">
            <!-- MQTT Clients -->
            <li class="nav-item">
                <a class="nav-link" href="mqtt-clients.html">
                    <i class="bi bi-arrow-left-right"></i> MQTT Clients
                </a>
            </li>

            <!-- YourDevice Clients -->
            <li class="nav-item">
                <a class="nav-link" href="yourdevice-clients.html">
                    <i class="bi bi-device-hdd"></i> YourDevice Clients
                </a>
            </li>

            <!-- ... other integrations ... -->
        </ul>
    </nav>
`;
```

---

## Testing and Validation

### 1. Unit Testing

**Test configuration parsing**:

```kotlin
@Test
fun testConnectionConfigParsing() {
    val json = JsonObject()
        .put("hostname", "localhost")
        .put("port", 8080)
        .put("username", "testuser")
        .put("reconnectDelay", 10000)

    val config = YourDeviceConnectionConfig.fromJsonObject(json)

    assertEquals("localhost", config.hostname)
    assertEquals(8080, config.port)
    assertEquals("testuser", config.username)
    assertEquals(10000L, config.reconnectDelay)
}
```

### 2. Integration Testing

**Test flow**:

1. Create device via GraphQL mutation
2. Verify device stored in database
3. Verify extension deploys connector
4. Verify connector connects to device
5. Verify data flows through MQTT bus
6. Update device configuration
7. Verify redeploy happens
8. Delete device
9. Verify cleanup

### 3. Manual Testing Checklist

**Dashboard UI**:
- [ ] List page loads and displays clients
- [ ] Can create new client with valid configuration
- [ ] Form validation works (required fields, port range)
- [ ] Can toggle client enabled/disabled
- [ ] Can delete client with confirmation
- [ ] Detail page loads for existing client
- [ ] Can update client configuration
- [ ] Can add addresses
- [ ] Can delete addresses
- [ ] Metrics update automatically

**Backend**:
- [ ] Connector deploys successfully on node startup
- [ ] Connector connects to external device
- [ ] Incoming data published to MQTT topics correctly
- [ ] Outgoing MQTT subscriptions forward to device
- [ ] Reconnection works after connection loss
- [ ] Metrics tracked correctly
- [ ] Cluster reassignment works
- [ ] Configuration changes trigger redeploy

---

## Common Patterns and Best Practices

### 1. Configuration Management

**Use sensible defaults**:
```kotlin
data class YourDeviceConnectionConfig(
    val hostname: String,
    val port: Int = 443,                    // Default port
    val reconnectDelay: Long = 5000,        // 5 seconds
    val timeout: Long = 30000               // 30 seconds
)
```

**Validate in constructor or separate method**:
```kotlin
init {
    require(hostname.isNotBlank()) { "Hostname cannot be blank" }
    require(port in 1..65535) { "Port must be between 1 and 65535" }
    require(reconnectDelay > 0) { "Reconnect delay must be positive" }
}
```

### 2. Error Handling

**Always handle failures gracefully**:
```kotlin
connect()
    .onSuccess {
        logger.info("Connected successfully")
    }
    .onFailure { error ->
        logger.warning("Connection failed: ${error.message}")
        scheduleReconnect()  // Don't crash, retry
    }
```

**Provide user-friendly error messages**:
```kotlin
future.complete(mapOf(
    "success" to false,
    "errors" to listOf("Unable to connect to ${config.hostname}:${config.port}. Please check the hostname and port."),
    "client" to null
))
```

### 3. Metrics

**Use atomic counters for thread safety**:
```kotlin
private val messagesInCounter = AtomicLong(0)
private val messagesOutCounter = AtomicLong(0)

// Increment
messagesInCounter.incrementAndGet()

// Read and reset (for rate calculation)
val rate = messagesInCounter.getAndSet(0)
```

**Calculate rates, not totals**:
```kotlin
// Good: messages per second
val metrics = JsonObject()
    .put("messagesIn", messagesInCounter.getAndSet(0) / intervalSeconds)

// Bad: cumulative total
val metrics = JsonObject()
    .put("messagesIn", messagesInCounter.get())  // Grows forever
```

### 4. EventBus Communication

**Use constants for addresses**:
```kotlin
companion object {
    const val ADDRESS_DEVICE_CONFIG_CHANGED = "yourdevice.device.config.changed"
    const val ADDRESS_VALUE_PUBLISH = "yourdevice.value.publish"

    fun connectorMetrics(deviceName: String) = "yourdevice.connector.$deviceName.metrics"
}
```

**Structured message format**:
```kotlin
val changeData = JsonObject()
    .put("operation", "add")           // What happened
    .put("deviceName", device.name)    // Which device
    .put("device", device.toJsonObject())  // Full data

vertx.eventBus().publish(ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
```

### 5. Topic Naming

**Use hierarchical namespaces**:
```
{namespace}/{function}/{device}/{datapoint}

Examples:
plant1/yourdevice/sensor1/temperature
plant1/yourdevice/sensor1/pressure
plant1/yourdevice/actuator1/status
```

**Avoid deep nesting** (keep it under 5 levels):
```
Good:  plant1/sensors/temperature/room1
Bad:   enterprise/region/plant/building/floor/room/sensor/type/temperature
```

### 6. Logging

**Use appropriate log levels**:
```kotlin
logger.severe("Failed to connect")           // Errors
logger.warning("Connection lost, retrying")  // Warnings
logger.info("Client started successfully")   // Important events
logger.fine("Published message to topic")    // Debug details
```

**Include context**:
```kotlin
logger.info("Starting connector for device: ${deviceConfig.name}")
logger.warning("Connection failed for ${deviceConfig.name}: ${error.message}")
```

### 7. Documentation

**Document public APIs**:
```kotlin
/**
 * Connects to the YourDevice system and establishes subscriptions.
 *
 * @return Future that completes when connection is established
 * @throws ConnectionException if authentication fails
 */
fun connect(): Future<Void>
```

**Add inline comments for complex logic**:
```kotlin
// Transform tag name to MQTT topic using configured rules
// 1. Apply regex replacement if configured
// 2. Apply case transformation
// 3. Prepend namespace
val mqttTopic = transformTagToTopic(tagName)
```

---

## Metrics Implementation Guide

### Overview

MonsterMQ tracks metrics at multiple levels:
- **Per-device metrics**: Individual device connection status, message rates
- **Broker-level aggregated metrics**: Cluster-wide totals across all devices of a type
- **Historical metrics**: Stored in database for time-series analysis

### Metrics Architecture

```
┌────────────────────────────────────────────────────────────────┐
│  Device Connector (per-device)                                 │
│  - Tracks messagesInCounter, messagesOutCounter                │
│  - Exposes metrics via EventBus consumer                       │
│  - Returns: messagesInRate, connected status                   │
└────────────────────┬───────────────────────────────────────────┘
                     │
                     │ EventBus request/reply
                     ▼
┌────────────────────────────────────────────────────────────────┐
│  MetricsHandler (periodic collection - every minute)           │
│  - Queries all devices via EventBus                            │
│  - Aggregates into totals (e.g., winCCUaClientIn)              │
│  - Stores per-device metrics: storeWinCCUaClientMetrics()      │
│  - Stores aggregated broker metrics: storeBrokerMetrics()      │
└────────────────────┬───────────────────────────────────────────┘
                     │
                     │ Store operations
                     ▼
┌────────────────────────────────────────────────────────────────┐
│  IMetricsStore Interface + Database Implementations            │
│  - PostgreSQL, CrateDB, MongoDB, SQLite                        │
│  - Stores metrics as JSONB with timestamp                      │
│  - MetricKind enum identifies metric type                      │
└────────────────────┬───────────────────────────────────────────┘
                     │
                     │ GraphQL queries
                     ▼
┌────────────────────────────────────────────────────────────────┐
│  MetricsResolver (GraphQL field resolvers)                     │
│  - Fetches from database OR live from EventBus                 │
│  - Applies rounding to 2 decimal places                        │
│  - Returns data to GraphQL clients                             │
└────────────────────────────────────────────────────────────────┘
```

### Step-by-Step Metrics Implementation

**IMPORTANT**: When adding metrics for a new device type, you MUST update ALL of the following locations. Missing even one will result in metrics not appearing in queries.

#### 1. Add Metrics Data Class

**Location**: `src/main/kotlin/graphql/Models.kt`

```kotlin
// Add device-specific metrics data class
data class YourDeviceClientMetrics(
    val messagesIn: Double,
    val messagesOut: Double = 0.0,  // Optional if device doesn't send out
    val connected: Boolean = false,
    val timestamp: String
)
```

**Add field to BrokerMetrics** (around line 145):

```kotlin
data class BrokerMetrics(
    // ... existing fields ...
    val winCCOaClientIn: Double = 0.0,
    val winCCUaClientIn: Double = 0.0,  // ← Add your device type here
    val yourDeviceClientIn: Double = 0.0,  // NEW
    val timestamp: String
)
```

#### 2. Update Connector to Track Metrics

**Location**: `src/main/kotlin/devices/yourdevice/YourDeviceConnector.kt`

```kotlin
class YourDeviceConnector : AbstractVerticle() {
    // Metrics counters
    private val messagesInCounter = AtomicLong(0)
    private val messagesOutCounter = AtomicLong(0)
    private var lastMetricsReset = System.currentTimeMillis()

    // Connection state
    private var isConnected = false

    private fun setupMetricsEndpoint() {
        val addr = YourDeviceExtension.connectorMetrics(deviceConfig.name)

        vertx.eventBus().consumer<JsonObject>(addr) { msg ->
            try {
                val now = System.currentTimeMillis()
                val elapsedMs = now - lastMetricsReset
                val elapsedSec = if (elapsedMs > 0) elapsedMs / 1000.0 else 1.0

                // Get counter values and reset
                val inCount = messagesInCounter.getAndSet(0)
                val outCount = messagesOutCounter.getAndSet(0)
                lastMetricsReset = now

                val json = JsonObject()
                    .put("device", deviceConfig.name)
                    .put("messagesInRate", inCount / elapsedSec)
                    .put("messagesOutRate", outCount / elapsedSec)
                    .put("connected", isConnected)  // IMPORTANT: Include connection status
                    .put("elapsedMs", elapsedMs)

                msg.reply(json)
            } catch (e: Exception) {
                msg.fail(500, e.message)
            }
        }
    }

    // Increment counters when processing messages
    private fun handleIncomingData(...) {
        messagesInCounter.incrementAndGet()  // ← Increment here
        // ... publish to MQTT ...
    }
}
```

#### 3. Add EventBus Addresses

**Location**: `src/main/kotlin/bus/EventBusAddresses.kt`

```kotlin
object EventBusAddresses {
    object YourDeviceBridge {
        const val CONNECTORS_LIST = "yourdevice.connectors.list"
        fun connectorMetrics(deviceName: String) = "yourdevice.connector.$deviceName.metrics"
    }
}
```

**Location**: `src/main/kotlin/devices/yourdevice/YourDeviceExtension.kt`

```kotlin
companion object {
    const val CONNECTORS_LIST = "yourdevice.connectors.list"
    fun connectorMetrics(deviceName: String) = "yourdevice.connector.$deviceName.metrics"
}

// Add consumer to list all devices
private fun setupEventBusConsumers() {
    vertx.eventBus().consumer<JsonObject>(CONNECTORS_LIST) { msg ->
        val devices = activeDeployments.keys.toList()
        msg.reply(JsonObject().put("devices", JsonArray(devices)))
    }
}
```

#### 4. Add MetricKind Enum Value

**Location**: `src/main/kotlin/stores/IMetricsStore.kt`

```kotlin
enum class MetricKind {
    BROKER, SESSION, MQTTCLIENT, KAFKACLIENT,
    WINCCOACLIENT, WINCCUACLIENT,
    YOURDEVICECLIENT,  // ← Add your device type
    OPCUADEVICE, ARCHIVEGROUP;

    fun toDbString(): String = when (this) {
        BROKER -> "broker"
        SESSION -> "session"
        MQTTCLIENT -> "mqtt"
        KAFKACLIENT -> "kafka"
        WINCCOACLIENT -> "winccoa"
        WINCCUACLIENT -> "winccua"
        YOURDEVICECLIENT -> "yourdevice"  // ← Add mapping
        OPCUADEVICE -> "opcua"
        ARCHIVEGROUP -> "archive"
    }
}
```

#### 5. Add Store Methods to Interface

**Location**: `src/main/kotlin/stores/IMetricsStore.kt`

```kotlin
interface IMetricsStore {
    // ... existing methods ...

    // YourDevice Client Metrics
    fun storeYourDeviceClientMetrics(
        timestamp: Instant,
        clientName: String,
        metrics: at.rocworks.extensions.graphql.YourDeviceClientMetrics
    ): Future<Void>

    fun getYourDeviceClientMetricsList(
        clientName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<List<at.rocworks.extensions.graphql.YourDeviceClientMetrics>>
}
```

#### 6. Implement Store Methods in ALL Database Implementations

**CRITICAL**: You MUST implement these methods in ALL four database stores:

**PostgreSQL** (`stores/dbs/postgres/MetricsStorePostgres.kt`):

```kotlin
override fun storeYourDeviceClientMetrics(
    timestamp: Instant,
    clientName: String,
    metrics: at.rocworks.extensions.graphql.YourDeviceClientMetrics
): Future<Void> =
    storeMetrics(MetricKind.YOURDEVICECLIENT, timestamp, clientName, yourDeviceClientMetricsToJson(metrics))

override fun getYourDeviceClientMetricsList(
    clientName: String,
    from: Instant?,
    to: Instant?,
    lastMinutes: Int?
): Future<List<at.rocworks.extensions.graphql.YourDeviceClientMetrics>> =
    getMetricsHistory(MetricKind.YOURDEVICECLIENT, clientName, from, to, lastMinutes, Int.MAX_VALUE)
        .map { list -> list.map { jsonToYourDeviceClientMetrics(it.second) } }

private fun yourDeviceClientMetricsToJson(m: at.rocworks.extensions.graphql.YourDeviceClientMetrics) = JsonObject()
    .put("messagesIn", m.messagesIn)
    .put("messagesOut", m.messagesOut)
    .put("connected", m.connected)
    .put("timestamp", m.timestamp)

private fun jsonToYourDeviceClientMetrics(j: JsonObject) =
    if (j.isEmpty)
        at.rocworks.extensions.graphql.YourDeviceClientMetrics(0.0, 0.0, false, TimestampConverter.currentTimeIsoString())
    else
        at.rocworks.extensions.graphql.YourDeviceClientMetrics(
            messagesIn = j.getDouble("messagesIn", 0.0),
            messagesOut = j.getDouble("messagesOut", 0.0),
            connected = j.getBoolean("connected", false),
            timestamp = j.getString("timestamp") ?: TimestampConverter.currentTimeIsoString()
        )
```

**Repeat for CrateDB, MongoDB, SQLite** using the exact same pattern.

#### 7. Update Broker Metrics Serialization in ALL Stores

**CRITICAL STEP - Often Missed**: Update broker metrics JSON serialization/deserialization in ALL four stores:

**PostgreSQL** (`stores/dbs/postgres/MetricsStorePostgres.kt`):

```kotlin
private fun brokerMetricsToJson(m: BrokerMetrics) = JsonObject()
    .put("messagesIn", m.messagesIn)
    // ... all existing fields ...
    .put("winCCOaClientIn", m.winCCOaClientIn)
    .put("winCCUaClientIn", m.winCCUaClientIn)
    .put("yourDeviceClientIn", m.yourDeviceClientIn)  // ← Add here
    .put("timestamp", m.timestamp)

private fun jsonToBrokerMetrics(j: JsonObject) = if (j.isEmpty) BrokerMetrics(
    messagesIn = 0.0,
    // ... all fields with defaults ...
    winCCOaClientIn = 0.0,
    winCCUaClientIn = 0.0,
    yourDeviceClientIn = 0.0,  // ← Add here
    timestamp = TimestampConverter.currentTimeIsoString()
) else BrokerMetrics(
    messagesIn = j.getDouble("messagesIn", 0.0),
    // ... all existing fields ...
    winCCOaClientIn = j.getDouble("winCCOaClientIn", 0.0),
    winCCUaClientIn = j.getDouble("winCCUaClientIn", 0.0),
    yourDeviceClientIn = j.getDouble("yourDeviceClientIn", 0.0),  // ← Add here
    timestamp = j.getString("timestamp") ?: TimestampConverter.currentTimeIsoString()
)
```

**Repeat for CrateDB, MongoDB, SQLite**.

#### 8. Add Metrics Collection to MetricsHandler

**Location**: `src/main/kotlin/handlers/MetricsHandler.kt`

Add variables at the beginning of `collectAndStoreMetrics()`:

```kotlin
var yourDeviceClientInTotal = 0.0
var yourDeviceDone = false
```

Update `tryAssembleAndStore()` condition:

```kotlin
fun tryAssembleAndStore() {
    if (brokerDone && bridgeDone && opcUaDone && kafkaDone &&
        winCCOaDone && winCCUaDone && yourDeviceDone &&  // ← Add here
        archiveDone && nodeMetrics != null) {
```

Add collection block (following the pattern of other devices):

```kotlin
// YourDevice Client metrics aggregation
val yourDeviceListAddress = EventBusAddresses.YourDeviceBridge.CONNECTORS_LIST
vertx.eventBus().request<JsonObject>(yourDeviceListAddress, JsonObject()).onComplete { listReply ->
    if (listReply.succeeded()) {
        val body = listReply.result().body()
        val devices = body.getJsonArray("devices") ?: JsonArray()
        logger.fine("YourDevice metrics aggregation: found ${devices.size()} devices")

        if (devices.isEmpty) {
            yourDeviceDone = true
            tryAssembleAndStore()
        } else {
            var remaining = devices.size()
            devices.forEach { d ->
                val deviceName = d as String
                val mAddr = EventBusAddresses.YourDeviceBridge.connectorMetrics(deviceName)
                vertx.eventBus().request<JsonObject>(mAddr, JsonObject()).onComplete { mReply ->
                    if (mReply.succeeded()) {
                        try {
                            val m = mReply.result().body()
                            val inRate = m.getDouble("messagesInRate", 0.0)
                            val outRate = m.getDouble("messagesOutRate", 0.0)
                            val connected = m.getBoolean("connected", false)

                            logger.fine("YourDevice client '$deviceName' metrics: in=$inRate out=$outRate connected=$connected")
                            yourDeviceClientInTotal += inRate

                            // Store individual client metrics
                            val clientMetrics = at.rocworks.extensions.graphql.YourDeviceClientMetrics(
                                messagesIn = inRate,
                                messagesOut = outRate,
                                connected = connected,
                                timestamp = TimestampConverter.instantToIsoString(timestamp)
                            )
                            metricsStore.storeYourDeviceClientMetrics(timestamp, deviceName, clientMetrics)
                        } catch (e: Exception) {
                            logger.warning("Error processing YourDevice metrics for $deviceName: ${e.message}")
                        }
                    } else {
                        logger.fine("No metrics for YourDevice client $deviceName: ${mReply.cause()?.message}")
                    }

                    remaining -= 1
                    if (remaining == 0) {
                        logger.fine("YourDevice metrics collection complete. Total: $yourDeviceClientInTotal")
                        yourDeviceDone = true
                        tryAssembleAndStore()
                    }
                }
            }
        }
    } else {
        logger.fine("Could not retrieve YourDevice connector list: ${listReply.cause()?.message}")
        yourDeviceDone = true
        tryAssembleAndStore()
    }
}
```

Add to BrokerMetrics construction:

```kotlin
val brokerMetrics = BrokerMetrics(
    // ... existing fields ...
    winCCOaClientIn = winCCOaClientInTotal,
    winCCUaClientIn = winCCUaClientInTotal,
    yourDeviceClientIn = yourDeviceClientInTotal,  // ← Add here
    timestamp = TimestampConverter.instantToIsoString(timestamp)
)
```

Update log message:

```kotlin
logger.fine("Stored aggregated broker metrics (...winCCUaIn=$winCCUaClientInTotal yourDeviceIn=$yourDeviceClientInTotal) for nodeId: $nodeId")
```

#### 9. Add GraphQL Resolvers

**Location**: `src/main/kotlin/graphql/MetricsResolver.kt`

**Add device metrics resolver with live fallback**:

```kotlin
fun yourDeviceClientMetrics(): DataFetcher<CompletableFuture<List<YourDeviceClientMetrics>>> {
    return DataFetcher { env ->
        val future = CompletableFuture<List<YourDeviceClientMetrics>>()
        val client = env.getSource<Map<String, Any>>()
        val clientName = client?.get("name") as? String

        if (clientName == null) {
            future.complete(listOf(YourDeviceClientMetrics(0.0, 0.0, false, TimestampConverter.currentTimeIsoString())))
            return@DataFetcher future
        }

        fun fetchLive() {
            // Fallback to live metrics if no stored data
            val addr = at.rocworks.bus.EventBusAddresses.YourDeviceBridge.connectorMetrics(clientName)
            vertx.eventBus().request<JsonObject>(addr, JsonObject()).onComplete { reply ->
                if (reply.succeeded()) {
                    val body = reply.result().body()
                    val inRate = body.getDouble("messagesInRate", 0.0)
                    val outRate = body.getDouble("messagesOutRate", 0.0)
                    val connected = body.getBoolean("connected", false)
                    future.complete(listOf(YourDeviceClientMetrics(
                        round2(inRate), round2(outRate), connected,
                        TimestampConverter.currentTimeIsoString()
                    )))
                } else {
                    future.complete(listOf(YourDeviceClientMetrics(0.0, 0.0, false, TimestampConverter.currentTimeIsoString())))
                }
            }
        }

        if (metricsStore != null) {
            metricsStore.getYourDeviceClientMetricsList(clientName, null, null, 1).onComplete { result ->
                if (result.succeeded() && result.result().isNotEmpty()) {
                    val m = result.result().first()
                    future.complete(listOf(YourDeviceClientMetrics(
                        round2(m.messagesIn), round2(m.messagesOut),
                        m.connected, m.timestamp
                    )))
                } else {
                    fetchLive()  // ← CRITICAL: Fallback to live if no stored data
                }
            }
        } else {
            fetchLive()
        }

        future
    }
}

fun yourDeviceClientMetricsHistory(): DataFetcher<CompletableFuture<List<YourDeviceClientMetrics>>> {
    return DataFetcher { env ->
        val future = CompletableFuture<List<YourDeviceClientMetrics>>()
        val client = env.getSource<Map<String, Any>>()
        val clientName = client?.get("name") as? String

        if (clientName == null) {
            future.complete(emptyList())
            return@DataFetcher future
        }

        val from = env.getArgument<String?>("from")
        val to = env.getArgument<String?>("to")
        val lastMinutes = env.getArgument<Int?>("lastMinutes")

        if (metricsStore != null) {
            val fromInstant = from?.let { Instant.parse(it) }
            val toInstant = to?.let { Instant.parse(it) }

            metricsStore.getYourDeviceClientMetricsList(clientName, fromInstant, toInstant, lastMinutes)
                .onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result().map {
                            YourDeviceClientMetrics(
                                round2(it.messagesIn),
                                round2(it.messagesOut),
                                it.connected,
                                it.timestamp
                            )
                        })
                    } else {
                        logger.warning("Failed to get historical metrics: ${result.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
        } else {
            future.complete(emptyList())
        }

        future
    }
}
```

**CRITICAL - Update `getCurrentBrokerMetrics()`** (around line 654):

```kotlin
private fun getCurrentBrokerMetrics(nodeId: String, callback: (BrokerMetrics) -> Unit) {
    fun roundBrokerMetrics(bm: BrokerMetrics): BrokerMetrics {
        return BrokerMetrics(
            // ... all existing fields ...
            winCCOaClientIn = round2(bm.winCCOaClientIn),
            winCCUaClientIn = round2(bm.winCCUaClientIn),
            yourDeviceClientIn = round2(bm.yourDeviceClientIn),  // ← MUST ADD HERE
            timestamp = bm.timestamp
        )
    }
    // ... rest of method
}
```

**CRITICAL - Update `getLiveBrokerMetrics()`** (around line 695):

Add to ALL three BrokerMetrics constructions in this function:

```kotlin
private fun getLiveBrokerMetrics(nodeId: String, callback: (BrokerMetrics) -> Unit) {
    // ... existing code ...

    BrokerMetrics(
        // ... all existing fields ...
        winCCOaClientIn = 0.0,
        winCCUaClientIn = 0.0,
        yourDeviceClientIn = 0.0,  // ← Add to success case
        timestamp = TimestampConverter.currentTimeIsoString()
    )

    // Also add to the two error case BrokerMetrics constructions
}
```

**Update default values in `brokerMetrics()` resolver** (around line 765):

```kotlin
callback(BrokerMetrics(
    // ... defaults ...
    winCCUaClientIn = 0.0,
    yourDeviceClientIn = 0.0,  // ← Add here
    timestamp = TimestampConverter.currentTimeIsoString()
))
```

#### 10. Register Resolvers in GraphQLServer

**Location**: `src/main/kotlin/graphql/GraphQLServer.kt`

```kotlin
.type("YourDeviceClient") { builder ->
    builder
        .dataFetcher("metrics", metricsResolver.yourDeviceClientMetrics())
        .dataFetcher("metricsHistory", metricsResolver.yourDeviceClientMetricsHistory())
}
```

#### 11. Update GraphQL Schema

**Location**: `src/main/resources/schema.graphqls`

**Add to BrokerMetrics type**:

```graphql
type BrokerMetrics {
    # ... existing fields ...
    winCCOaClientIn: Float!
    winCCUaClientIn: Float!
    yourDeviceClientIn: Float!  # ← Add here
    timestamp: String!
}
```

**Add device metrics type**:

```graphql
type YourDeviceClientMetrics {
    messagesIn: Float!
    messagesOut: Float!
    connected: Boolean!
    timestamp: String!
}
```

**Add to device type**:

```graphql
type YourDeviceClient {
    name: String!
    # ... other fields ...
    metrics: [YourDeviceClientMetrics!]!
    metricsHistory(from: String, to: String, lastMinutes: Int): [YourDeviceClientMetrics!]!
}
```

### Common Pitfalls and Solutions

#### Pitfall 1: Metrics Show as 0 in GraphQL Query

**Symptoms**:
- Data is in database
- `metricsIn` shows 0 in queries

**Causes**:
1. Missing field in `roundBrokerMetrics()` in MetricsResolver.kt
2. Missing field in `getLiveBrokerMetrics()` fallback cases
3. Missing field in broker metrics JSON serialization/deserialization

**Solution**:
Search for ALL instances of `BrokerMetrics(` construction and ensure your new field is included.

```bash
# Find all BrokerMetrics constructions
grep -n "BrokerMetrics(" MetricsResolver.kt
grep -n "BrokerMetrics(" stores/dbs/*/MetricsStore*.kt
```

#### Pitfall 2: Metrics Not Collected from Devices

**Symptoms**:
- No data in database at all
- No log messages about metrics collection

**Causes**:
1. Forgot to add device to `tryAssembleAndStore()` condition
2. Missing EventBus `CONNECTORS_LIST` consumer in Extension
3. Wrong EventBus address format

**Solution**:
- Check MetricsHandler logs for "found N devices"
- Verify Extension has `CONNECTORS_LIST` consumer
- Test EventBus address manually: `vertx.eventBus().request<JsonObject>("yourdevice.connectors.list", JsonObject())`

#### Pitfall 3: Compilation Errors After Adding Metrics

**Symptoms**:
- Build fails with "not abstract and does not implement abstract members"

**Causes**:
- Added methods to IMetricsStore but didn't implement in all 4 stores

**Solution**:
ALWAYS implement in ALL FOUR stores:
- PostgreSQL: `stores/dbs/postgres/MetricsStorePostgres.kt`
- CrateDB: `stores/dbs/cratedb/MetricsStoreCrateDB.kt`
- MongoDB: `stores/dbs/mongodb/MetricsStoreMongoDB.kt`
- SQLite: `stores/dbs/sqlite/MetricsStoreSQLite.kt`

#### Pitfall 4: Live Metrics Work but Historical Doesn't

**Symptoms**:
- Current metrics show correct values
- `metricsHistory` returns empty array

**Causes**:
- MetricsHandler not actually calling `storeYourDeviceClientMetrics()`
- Database serialization/deserialization incorrect
- MetricKind enum missing or wrong

**Solution**:
- Check database table for stored metrics: `SELECT * FROM metrics WHERE kind = 'yourdevice' ORDER BY timestamp DESC LIMIT 10;`
- Verify MetricKind.toDbString() returns correct value
- Check logs for "Stored aggregated broker metrics"

### Metrics Implementation Checklist

When adding metrics for a new device type, verify ALL of these:

**Backend Core**:
- [ ] Data class added to Models.kt
- [ ] Field added to BrokerMetrics in Models.kt
- [ ] Connector tracks counters (AtomicLong)
- [ ] Connector exposes EventBus metrics endpoint
- [ ] Connector returns `connected` status in metrics response
- [ ] Extension has CONNECTORS_LIST EventBus consumer
- [ ] EventBus addresses defined in EventBusAddresses.kt

**Metrics Storage**:
- [ ] MetricKind enum value added
- [ ] MetricKind.toDbString() mapping added
- [ ] IMetricsStore interface methods added (store + get)
- [ ] PostgreSQL implementation (both methods + JSON helpers)
- [ ] CrateDB implementation (both methods + JSON helpers)
- [ ] MongoDB implementation (both methods + JSON helpers)
- [ ] SQLite implementation (both methods + JSON helpers)

**Broker Metrics Serialization** (CRITICAL):
- [ ] PostgreSQL: brokerMetricsToJson() includes new field
- [ ] PostgreSQL: jsonToBrokerMetrics() includes new field (empty case)
- [ ] PostgreSQL: jsonToBrokerMetrics() includes new field (populated case)
- [ ] CrateDB: Same 3 updates
- [ ] MongoDB: Same 3 updates
- [ ] SQLite: Same 3 updates

**Metrics Collection**:
- [ ] MetricsHandler: variable for total added
- [ ] MetricsHandler: variable for done flag added
- [ ] MetricsHandler: tryAssembleAndStore() condition updated
- [ ] MetricsHandler: collection block added
- [ ] MetricsHandler: BrokerMetrics construction includes field
- [ ] MetricsHandler: log message includes field

**GraphQL Resolvers** (CRITICAL):
- [ ] yourDeviceClientMetrics() resolver with live fallback
- [ ] yourDeviceClientMetricsHistory() resolver
- [ ] getCurrentBrokerMetrics() roundBrokerMetrics() includes field
- [ ] getLiveBrokerMetrics() success case includes field
- [ ] getLiveBrokerMetrics() error case 1 includes field
- [ ] getLiveBrokerMetrics() error case 2 includes field
- [ ] brokerMetrics() default case includes field

**GraphQL Schema & Registration**:
- [ ] Device metrics type defined
- [ ] BrokerMetrics type includes field
- [ ] Device type has metrics field
- [ ] Device type has metricsHistory field
- [ ] Resolvers registered in GraphQLServer.kt

**Testing**:
- [ ] Build compiles without errors
- [ ] Connector deploys and connects
- [ ] EventBus metrics endpoint responds
- [ ] Metrics appear in database after 1 minute
- [ ] GraphQL query returns current metrics
- [ ] GraphQL query returns historical metrics
- [ ] Broker metrics show aggregated total

### Testing Metrics Implementation

**1. Test EventBus Metrics Endpoint**:

```kotlin
// In Vert.x code or test
vertx.eventBus().request<JsonObject>(
    "yourdevice.connector.device1.metrics",
    JsonObject()
).onComplete { reply ->
    if (reply.succeeded()) {
        val metrics = reply.result().body()
        println("messagesInRate: ${metrics.getDouble("messagesInRate")}")
        println("connected: ${metrics.getBoolean("connected")}")
    }
}
```

**2. Test Metrics Collection**:

Wait 1 minute after starting broker, then check database:

```sql
-- Check device metrics
SELECT * FROM metrics
WHERE kind = 'yourdevice'
ORDER BY timestamp DESC
LIMIT 10;

-- Check broker metrics includes your device
SELECT
    timestamp,
    data->>'yourDeviceClientIn' as your_device_in,
    data->>'winCCUaClientIn' as winccua_in
FROM metrics
WHERE kind = 'broker'
ORDER BY timestamp DESC
LIMIT 5;
```

**3. Test GraphQL Queries**:

```graphql
# Test device metrics
query {
  yourDeviceClients {
    name
    metrics {
      messagesIn
      messagesOut
      connected
      timestamp
    }
  }
}

# Test broker aggregated metrics
query {
  brokers {
    nodeId
    metrics {
      yourDeviceClientIn
      winCCUaClientIn
      timestamp
    }
  }
}

# Test historical metrics
query {
  yourDeviceClient(name: "device1") {
    metricsHistory(lastMinutes: 60) {
      messagesIn
      connected
      timestamp
    }
  }
}
```

---

## Summary Checklist

**When integrating a new device type, complete these steps**:

### Backend (Kotlin)
- [ ] Create configuration data classes with `fromJsonObject()` and `toJsonObject()`
- [ ] Implement Connector verticle extending `AbstractVerticle`
- [ ] Implement Extension verticle for cluster-aware management
- [ ] Add device type constant to `DeviceConfig`
- [ ] Deploy extension in `Monster.kt`
- [ ] Update `config.yaml` with device section

### GraphQL API
- [ ] Define types in `schema.graphqls` (Device, Config, Address, Metrics, Result)
- [ ] Define enum types for address modes/types
- [ ] Add queries (`devices`, `device`, `devicesByNode`)
- [ ] Create grouped mutations type with operations (create, update, delete, toggle, reassign, addAddress, deleteAddress)
- [ ] Define input types for all mutations
- [ ] Implement `*ConfigQueries.kt` resolver class
- [ ] Implement `*ConfigMutations.kt` resolver class
- [ ] Register resolvers in `GraphQLServer.kt`

### Frontend Dashboard
- [ ] Create list view JavaScript (`yourdevice-clients.js`)
- [ ] Create list view HTML page (`yourdevice-clients.html`)
- [ ] Create detail view JavaScript (`yourdevice-client-detail.js`)
- [ ] Create detail view HTML page (`yourdevice-client-detail.html`)
- [ ] Add navigation links in `sidebar.js`
- [ ] Test all CRUD operations in UI
- [ ] Test metrics display and auto-refresh

### Testing
- [ ] Unit test configuration parsing
- [ ] Integration test full flow (create → deploy → data flow → delete)
- [ ] Manual test all UI operations
- [ ] Test cluster node reassignment
- [ ] Test reconnection on connection loss
- [ ] Test configuration updates trigger redeploy

---

## Common UI and GraphQL Integration Pitfalls

When implementing the frontend dashboard and GraphQL API layer, several common issues can arise. This section documents real issues encountered during development and how to avoid them.

### 1. Addresses Management in Client Updates

**Problem**: Including `addresses` in the config object when updating a client causes GraphQL validation errors.

**Root Cause**: The backend manages addresses separately through dedicated `addAddress` and `deleteAddress` mutations. During updates, addresses are automatically preserved from the existing configuration (see `WinCCUaClientConfigMutations.kt:222`).

**Solution**: Never include `addresses` in the config when saving a client:

```javascript
// ❌ WRONG - causes GraphQL validation error
const clientData = {
    name: name,
    namespace: namespace,
    nodeId: nodeId,
    enabled: enabled,
    config: {
        graphqlEndpoint: endpoint,
        username: username,
        password: password,
        addresses: this.addresses  // ❌ Don't send this
    }
}

// ✅ CORRECT - addresses managed separately
const clientData = {
    name: name,
    namespace: namespace,
    nodeId: nodeId,
    enabled: enabled,
    config: {
        graphqlEndpoint: endpoint,
        username: username,
        password: password
        // addresses are automatically preserved by backend
    }
}
```

**Backend Code Reference** (`WinCCUaClientConfigMutations.kt:216-225`):
```kotlin
// Parse existing config from JsonObject
val existingConfig = WinCCUaConnectionConfig.fromJsonObject(existingDevice.config)
val requestConfig = WinCCUaConnectionConfig.fromJsonObject(request.config)

// Update device (preserve existing addresses and passwords)
val newConfig = requestConfig.copy(
    addresses = existingConfig.addresses,  // ✅ Preserved automatically
    password = requestConfig.password.ifBlank { existingConfig.password }
)
```

### 2. Password Preservation in Updates

**Problem**: When editing a client, forcing users to re-enter the password is bad UX. But how do you preserve existing passwords?

**Solution**: The backend uses `ifBlank` to preserve passwords. Send an **empty string** (not null) when you don't want to change the password:

```javascript
// In edit mode
function getPasswordForSave() {
    if (this.isNewMode) {
        // New client - password required
        const password = document.getElementById('client-password').value;
        if (!password) throw new Error('Password is required');
        return password;
    }

    // Edit mode - check "Change Password" checkbox
    const changePasswordCheckbox = document.getElementById('change-password-checkbox');
    if (changePasswordCheckbox && changePasswordCheckbox.checked) {
        const password = document.getElementById('client-password').value;
        if (!password) throw new Error('Please enter a new password');
        return password;  // ✅ Send new password
    }

    return '';  // ✅ Empty string preserves existing password
}
```

**UI Pattern**:
```html
<div style="display: flex; justify-content: space-between; align-items: center;">
    <label for="client-password">Password</label>
    <div class="checkbox-group" id="change-password-group" style="display: none;">
        <input type="checkbox" id="change-password-checkbox">
        <label for="change-password-checkbox">Change Password</label>
    </div>
</div>
<input type="password" id="client-password" disabled placeholder="••••••••">
```

**JavaScript to toggle password field**:
```javascript
if (!this.isNewClient) {
    changePasswordGroup.style.display = 'block';
    passwordField.disabled = true;
    passwordField.required = false;
    passwordField.placeholder = '••••••••';

    changePasswordCheckbox.addEventListener('change', (e) => {
        passwordField.disabled = !e.target.checked;
        passwordField.required = e.target.checked;
        passwordField.placeholder = e.target.checked ? 'Enter new password' : '••••••••';
        if (!e.target.checked) {
            passwordField.value = '';
        }
    });
}
```

### 3. GraphQL Query Field Mismatches

**Problem**: Frontend JavaScript queries fields that don't exist in the GraphQL schema, causing validation errors.

**Example Error**:
```
Field 'removeSystemName' in type 'WinCCUaTransformConfig' is undefined
Field 'query' in type 'WinCCUaAddress' is undefined
```

**Root Cause**: Copying code from similar integrations without checking schema differences. WinCC UA schema is different from WinCC OA schema.

**Solution**: Always verify field names against the actual GraphQL schema (`schema.graphqls`) before writing queries:

```javascript
// ❌ WRONG - copied from WinCC OA
const query = `
    query {
        winCCUaClient(name: $name) {
            config {
                token  // ❌ Doesn't exist in WinCC UA schema
                transformConfig {
                    removeSystemName  // ❌ Doesn't exist
                }
                addresses {
                    query  // ❌ Called 'nameFilters' in WinCC UA
                }
            }
        }
    }
`

// ✅ CORRECT - matches schema.graphqls
const query = `
    query {
        winCCUaClient(name: $name) {
            config {
                username  // ✅ Exists in WinCC UA
                transformConfig {
                    convertDotToSlash  // ✅ Correct field name
                }
                addresses {
                    nameFilters  // ✅ Correct field name
                }
            }
        }
    }
`
```

### 4. Array Field Handling: nameFilters vs browseArguments

**Problem**: Converting from complex JSON objects to simple string arrays requires careful UI and backend handling.

**Example**: WinCC UA originally used `browseArguments: JsonObject` but was simplified to `nameFilters: [String!]`.

**Frontend Conversion**:
```javascript
// Parse comma-separated input into array
const nameFiltersValue = document.getElementById('address-browse-arguments').value.trim();
addressData.nameFilters = nameFiltersValue
    ? nameFiltersValue.split(',').map(f => f.trim()).filter(f => f.length > 0)
    : null;

// Example: "HMI_*, TANK_*, System.Tag1" → ["HMI_*", "TANK_*", "System.Tag1"]
```

**Display Array in UI**:
```javascript
// Show array as comma-separated string
if (address.nameFilters && address.nameFilters.length > 0) {
    optionsBadges.push(`Filters: ${address.nameFilters.join(', ')}`);
}
```

**Backend Backward Compatibility** (`WinCCUaConfig.kt`):
```kotlin
// Support old format during migration
val nameFiltersList = json.getJsonArray("nameFilters")?.map { it.toString() }
    ?: json.getJsonObject("browseArguments")?.getString("filter")?.let { listOf(it) }
```

### 5. GraphQL Type vs Input Type Confusion

**Problem**: There are two versions of most types: the **type** (for queries) and the **input** (for mutations). They may have different fields.

**Example**:
```graphql
# Type - returned from queries (may include computed fields)
type WinCCUaAddress {
    type: WinCCUaAddressType!
    topic: String!
    nameFilters: [String!]
    # May include additional read-only fields
}

# Input - sent to mutations (only writeable fields)
input WinCCUaAddressInput {
    type: WinCCUaAddressType!
    topic: String!
    nameFilters: [String!]
    # No read-only fields like 'id' or computed values
}
```

**Solution**: Always check both type and input definitions when adding new fields. Update both if the field should be writable.

### 6. Null vs Empty String vs Undefined in GraphQL

**Problem**: JavaScript has three "empty" values but GraphQL only understands null and defined values.

**Best Practices**:
```javascript
// For optional strings - use null for "not set"
websocketEndpoint: document.getElementById('websocket').value.trim() || null

// For arrays - use null for "not provided", [] for "empty list"
nameFilters: filters.length > 0 ? filters : null  // ✅ null = not set
nameFilters: []  // ✅ empty array = explicitly no filters

// For numbers - send actual values or use schema defaults
reconnectDelay: parseInt(value) || undefined  // ❌ undefined becomes null
reconnectDelay: parseInt(value)  // ✅ relies on schema default if invalid
```

### 7. Live Metrics Fallback Pattern

**Problem**: Newly created clients have no stored metrics yet, so GraphQL queries return empty results.

**Solution**: Implement a fallback to fetch live metrics from EventBus when database has no data:

```kotlin
fun winCCUaClientMetrics(): DataFetcher<CompletableFuture<List<WinCCUaClientMetrics>>> {
    return DataFetcher { env ->
        val future = CompletableFuture<List<WinCCUaClientMetrics>>()
        val clientName = env.getSource<Map<String, Any>>()["name"] as String

        // Try database first
        metricsStore.getWinCCUaClientMetricsList(clientName, 1).onComplete { result ->
            if (result.succeeded() && result.result().isNotEmpty()) {
                future.complete(result.result())  // ✅ Return stored metrics
            } else {
                // ✅ Fallback: fetch live from EventBus
                fetchLiveMetrics(clientName, future)
            }
        }

        future
    }
}

private fun fetchLiveMetrics(clientName: String, future: CompletableFuture<...>) {
    val addr = EventBusAddresses.WinCCUaBridge.connectorMetrics(clientName)
    vertx.eventBus().request<JsonObject>(addr, JsonObject()).onComplete { reply ->
        if (reply.succeeded()) {
            val body = reply.result().body()
            val metrics = WinCCUaClientMetrics(
                messagesIn = body.getDouble("messagesInRate", 0.0),
                connected = body.getBoolean("connected", false),
                timestamp = TimestampConverter.currentTimeIsoString()
            )
            future.complete(listOf(metrics))
        } else {
            future.complete(emptyList())  // ✅ Graceful fallback
        }
    }
}
```

### Summary: Quick Checklist for UI Development

When implementing a new device client UI:

- [ ] **Don't send addresses in config** - use dedicated addAddress/deleteAddress mutations
- [ ] **Use empty string for unchanged passwords** - backend uses `ifBlank` to preserve
- [ ] **Verify all GraphQL field names** against schema.graphqls before querying
- [ ] **Handle array fields properly** - parse comma-separated input, display joined strings
- [ ] **Update both type and input** in GraphQL schema when adding fields
- [ ] **Use null for optional fields** - don't send undefined
- [ ] **Implement live metrics fallback** - query EventBus when database is empty
- [ ] **Test in both create and edit modes** - different validation rules apply
- [ ] **Check browser console for GraphQL errors** - they show exact field mismatches

---

## Example: WinCC Unified Integration

This guide was created based on the actual WinCC Unified client integration. Here are the key files for reference:

**Backend**:
- `src/main/kotlin/devices/winccua/WinCCUaConnector.kt` - Connector with GraphQL/WebSocket client
- `src/main/kotlin/devices/winccua/WinCCUaExtension.kt` - Extension coordinator
- `src/main/kotlin/stores/devices/WinCCUaConfig.kt` - Configuration models

**GraphQL**:
- `src/main/resources/schema.graphqls` (lines 2075-2262) - WinCC UA types and mutations
- `src/main/kotlin/graphql/WinCCUaClientConfigQueries.kt` - Query resolvers
- `src/main/kotlin/graphql/WinCCUaClientConfigMutations.kt` - Mutation resolvers

**Frontend**:
- `src/main/resources/dashboard/js/winccua-clients.js` - List view
- `src/main/resources/dashboard/js/winccua-client-detail.js` - Detail view
- `src/main/resources/dashboard/winccua-clients.html` - List page
- `src/main/resources/dashboard/winccua-client-detail.html` - Detail page

By following this guide, you can integrate any new device type into MonsterMQ with a consistent architecture and user experience.