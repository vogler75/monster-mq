# Device Integration Guide for MonsterMQ Broker

Quick reference guide for integrating new device types into MonsterMQ. For detailed examples, refer to implementation files: WinCCOA, WinCCUA, PLC4X, SparkplugB.

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

```
┌─────────────────────────────────────────────────────────┐
│           Frontend Dashboard (JS)                        │
│  - List View | Detail View                              │
└────────────────────┬────────────────────────────────────┘
                     │ GraphQL/HTTP
┌────────────────────▼────────────────────────────────────┐
│           GraphQL API Layer                              │
│  - Schema | Query/Mutation Resolvers                    │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│      Device Configuration Storage (IDeviceConfigStore)  │
│  - PostgreSQL | CrateDB | MongoDB | SQLite              │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│           Backend Device Layer                           │
│  Extension (cluster-aware) → Connector (per-device)     │
└────────┬────────────────────┬────────────────────────────┘
         │                    │
    MQTT Bus          External Device/System
```

### Key Components

1. **DeviceConfig**: Generic configuration model
2. **Extension**: Cluster-aware coordinator managing device lifecycle
3. **Connector**: Per-device verticle handling communication
4. **GraphQL API**: Type-safe configuration management
5. **Dashboard UI**: Web administration interface

---

## Step-by-Step Integration Process

### Planning Phase

Define before coding:

1. **Device Type Identifier**: Unique string (e.g., `"MQTT-Client"`)
2. **Configuration Schema**: Connection details, settings, address mappings
3. **Data Flow**: Incoming (Device → MQTT) / Outgoing (MQTT → Device)
4. **Metrics**: Messages in/out, connection status, errors

### Implementation Order

**Backend**: Config classes → Connector → Extension → Monster.kt integration
**GraphQL**: Schema → Queries → Mutations → Register in GraphQLServer.kt
**Frontend**: List view → Detail view → Sidebar routes

---

## Backend Implementation

### 1. Configuration Data Classes

**Location**: `src/main/kotlin/stores/devices/YourDeviceConfig.kt`

```kotlin
data class YourDeviceConfig(
    val hostname: String,
    val port: Int,
    val username: String,
    val password: String,
    val reconnectDelay: Long = 5000,
    val addresses: List<YourDeviceAddress> = emptyList()
) {
    companion object {
        fun fromJsonObject(json: JsonObject): YourDeviceConfig {
            // Parse configuration
        }
    }

    fun toJsonObject(): JsonObject {
        // Serialize configuration
    }
}

data class YourDeviceAddress(
    val topic: String,
    val description: String? = null,
    val retained: Boolean = false
) {
    companion object {
        fun fromJsonObject(json: JsonObject): YourDeviceAddress { /* ... */ }
    }
    fun toJsonObject(): JsonObject { /* ... */ }
}
```

**Key points**: Companion `fromJsonObject()`, `toJsonObject()` method, sensible defaults

### 2. Connector Verticle

**Location**: `src/main/kotlin/devices/yourdevice/YourDeviceConnector.kt`

```kotlin
class YourDeviceConnector : AbstractVerticle() {
    private lateinit var deviceConfig: DeviceConfig
    private lateinit var yourDeviceConfig: YourDeviceConfig
    private var isConnected = false
    private val messagesInCounter = AtomicLong(0)
    private val messagesOutCounter = AtomicLong(0)

    override fun start(startPromise: Promise<Void>) {
        // Load configuration
        val deviceJson = config().getJsonObject("device")
        deviceConfig = DeviceConfig.fromJsonObject(deviceJson)
        yourDeviceConfig = YourDeviceConfig.fromJsonObject(deviceConfig.config)

        setupEventBusConsumers()
        connect()
            .onSuccess { startPromise.complete() }
            .onFailure {
                scheduleReconnect()
                startPromise.complete()
            }
    }

    override fun stop(stopPromise: Promise<Void>) {
        reconnectTimerId?.let { vertx.cancelTimer(it) }
        disconnect().onComplete { stopPromise.complete() }
    }

    private fun connect(): Future<Void> {
        // TODO: Device-specific connection logic
    }

    private fun handleIncomingData(topic: String, payload: ByteArray) {
        val mqttTopic = "${deviceConfig.namespace}/$topic"
        val message = BrokerMessage(
            messageId = 0,
            topicName = mqttTopic,
            payload = payload,
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = "yourdevice-${deviceConfig.name}"
        )
        vertx.eventBus().publish(YourDeviceExtension.ADDRESS_VALUE_PUBLISH, message)
        messagesInCounter.incrementAndGet()
    }

    private fun setupEventBusConsumers() {
        // Metrics queries
        vertx.eventBus().consumer<JsonObject>(
            "${YourDeviceExtension.ADDRESS_DEVICE_METRICS}.${deviceConfig.id}"
        ) { message ->
            message.reply(JsonObject()
                .put("messagesIn", messagesInCounter.get())
                .put("messagesOut", messagesOutCounter.get())
                .put("connected", isConnected))
        }
    }
}
```

### 3. Extension Verticle

**Location**: `src/main/kotlin/devices/yourdevice/YourDeviceExtension.kt`

```kotlin
class YourDeviceExtension(
    private val deviceStore: IDeviceConfigStore,
    private val clusterManager: ClusterManager,
    private val messageBus: IMessageBus
) : AbstractVerticle() {

    companion object {
        const val TYPE = "YourDevice-Client"
        const val ADDRESS_VALUE_PUBLISH = "device.yourdevice.value.publish"
        const val ADDRESS_DEVICE_METRICS = "device.yourdevice.metrics"
    }

    private val deployedDevices = ConcurrentHashMap<String, String>()

    override fun start(startPromise: Promise<Void>) {
        // Setup config change listener
        vertx.eventBus().consumer<JsonObject>(
            EventBusAddresses.Device.configChanged(TYPE)
        ) { message ->
            val event = DeviceConfigEvent.fromJson(message.body())
            handleConfigChange(event)
        }

        // Subscribe to MQTT messages from connectors
        messageBus.subscribe(ADDRESS_VALUE_PUBLISH) { message ->
            val brokerMessage = message as BrokerMessage
            messageBus.publish(brokerMessage)
        }

        // Load and deploy existing devices
        deviceStore.getByType(TYPE)
            .onSuccess { devices ->
                devices.forEach { deployDevice(it) }
                startPromise.complete()
            }
            .onFailure { startPromise.fail(it) }
    }

    private fun handleConfigChange(event: DeviceConfigEvent) {
        when (event.operation) {
            "CREATE", "UPDATE" -> {
                if (shouldManageDevice(event.deviceConfig)) {
                    deployDevice(event.deviceConfig)
                }
            }
            "DELETE" -> undeployDevice(event.deviceConfig.id)
        }
    }

    private fun deployDevice(device: DeviceConfig): Future<Void> {
        val promise = Promise.promise<Void>()

        // Undeploy if already exists
        deployedDevices[device.id]?.let { deploymentId ->
            vertx.undeploy(deploymentId)
        }

        // Deploy connector
        val options = DeploymentOptions().setConfig(
            JsonObject().put("device", device.toJsonObject())
        )

        vertx.deployVerticle(YourDeviceConnector::class.java.name, options)
            .onSuccess { deploymentId ->
                deployedDevices[device.id] = deploymentId
                logger.info("Deployed device: ${device.name}")
                promise.complete()
            }
            .onFailure { promise.fail(it) }

        return promise.future()
    }

    private fun undeployDevice(deviceId: String): Future<Void> {
        val promise = Promise.promise<Void>()
        deployedDevices.remove(deviceId)?.let { deploymentId ->
            vertx.undeploy(deploymentId)
                .onSuccess {
                    logger.info("Undeployed device: $deviceId")
                    promise.complete()
                }
                .onFailure { promise.fail(it) }
        } ?: promise.complete()
        return promise.future()
    }

    private fun shouldManageDevice(device: DeviceConfig): Boolean {
        return clusterManager.isLocalNodeResponsible(device.id)
    }
}
```

### 4. Integration with Monster.kt

**Location**: `src/main/kotlin/Monster.kt`

Add to initialization:

```kotlin
// In Monster.kt start() method
if (config.devices?.enabled == true) {
    // ... existing device extensions ...

    vertx.deployVerticle(
        YourDeviceExtension(deviceStore, clusterManager, messageBus)
    ).await()
    logger.info("YourDevice extension deployed")
}
```

---

## GraphQL API Layer

### 1. Schema Definitions

**Location**: `src/main/resources/schema.graphqls`

```graphql
# Types (read operations)
type YourDeviceConfig {
    hostname: String!
    port: Int!
    username: String!
    reconnectDelay: Long!
    addresses: [YourDeviceAddress!]!
}

type YourDeviceAddress {
    topic: String!
    description: String
    retained: Boolean!
}

type YourDeviceMetrics {
    messagesIn: Long!
    messagesOut: Long!
    connected: Boolean!
}

# Inputs (write operations)
input YourDeviceConfigInput {
    hostname: String!
    port: Int!
    username: String!
    password: String!
    reconnectDelay: Long
    addresses: [YourDeviceAddressInput!]
}

input YourDeviceAddressInput {
    topic: String!
    description: String
    retained: Boolean
}

# Queries
extend type Query {
    yourDeviceLiveMetrics(deviceId: ID!): YourDeviceMetrics
}

# Mutations
extend type Mutation {
    createYourDeviceConfig(
        name: String!
        namespace: String!
        enabled: Boolean!
        config: YourDeviceConfigInput!
    ): DeviceConfig!

    updateYourDeviceConfig(
        id: ID!
        name: String
        namespace: String
        enabled: Boolean
        config: YourDeviceConfigInput
    ): DeviceConfig!
}
```

**Important**: Use separate types for queries (`Type`) and mutations (`TypeInput`)

### 2. Query Resolvers

**Location**: `src/main/kotlin/graphql/YourDeviceConfigQueries.kt`

```kotlin
class YourDeviceConfigQueries(private val vertx: Vertx) {

    fun yourDeviceLiveMetrics(env: DataFetchingEnvironment): CompletableFuture<JsonObject?> {
        val deviceId = env.getArgument<String>("deviceId")
        val promise = Promise.promise<JsonObject?>()

        vertx.eventBus().request<JsonObject>(
            "${YourDeviceExtension.ADDRESS_DEVICE_METRICS}.$deviceId",
            JsonObject(),
            DeliveryOptions().setSendTimeout(5000)
        ).onSuccess { reply ->
            promise.complete(reply.body())
        }.onFailure {
            promise.complete(null) // Return null if device not responding
        }

        return promise.future().toCompletionStage().toCompletableFuture()
    }
}
```

### 3. Mutation Resolvers

**Location**: `src/main/kotlin/graphql/YourDeviceConfigMutations.kt`

```kotlin
class YourDeviceConfigMutations(
    private val deviceStore: IDeviceConfigStore,
    private val vertx: Vertx
) {
    private val logger = Logger.getLogger(this::class.java.name)

    fun createYourDeviceConfig(env: DataFetchingEnvironment): CompletableFuture<DeviceConfig> {
        val name = env.getArgument<String>("name")
        val namespace = env.getArgument<String>("namespace")
        val enabled = env.getArgument<Boolean>("enabled")
        val configInput = env.getArgument<Map<String, Any>>("config")

        val config = parseConfigInput(configInput)

        val deviceConfig = DeviceConfig(
            id = UUID.randomUUID().toString(),
            name = name,
            namespace = namespace,
            type = YourDeviceExtension.TYPE,
            enabled = enabled,
            config = config.toJsonObject()
        )

        return deviceStore.create(deviceConfig)
            .onSuccess { notifyConfigChange("CREATE", it) }
            .toCompletionStage().toCompletableFuture()
    }

    fun updateYourDeviceConfig(env: DataFetchingEnvironment): CompletableFuture<DeviceConfig> {
        val id = env.getArgument<String>("id")

        return deviceStore.get(id)
            .compose { existing ->
                val updated = existing.copy(
                    name = env.getArgument<String?>("name") ?: existing.name,
                    namespace = env.getArgument<String?>("namespace") ?: existing.namespace,
                    enabled = env.getArgument<Boolean?>("enabled") ?: existing.enabled,
                    config = env.getArgument<Map<String, Any>?>("config")?.let {
                        parseConfigInput(it).toJsonObject()
                    } ?: existing.config
                )
                deviceStore.update(updated)
                    .onSuccess { notifyConfigChange("UPDATE", it) }
            }
            .toCompletionStage().toCompletableFuture()
    }

    private fun parseConfigInput(input: Map<String, Any>): YourDeviceConfig {
        val addresses = (input["addresses"] as? List<Map<String, Any>>)?.map { addr ->
            YourDeviceAddress(
                topic = addr["topic"] as String,
                description = addr["description"] as? String,
                retained = addr["retained"] as? Boolean ?: false
            )
        } ?: emptyList()

        return YourDeviceConfig(
            hostname = input["hostname"] as String,
            port = input["port"] as Int,
            username = input["username"] as String,
            password = input["password"] as String,
            reconnectDelay = (input["reconnectDelay"] as? Number)?.toLong() ?: 5000,
            addresses = addresses
        )
    }

    private fun notifyConfigChange(operation: String, device: DeviceConfig) {
        val event = DeviceConfigEvent(operation, device)
        vertx.eventBus().publish(
            EventBusAddresses.Device.configChanged(device.type),
            event.toJson()
        )
    }
}
```

### 4. Register with GraphQLServer

**Location**: `src/main/kotlin/GraphQLServer.kt`

```kotlin
// In buildSchema() method
val queries = YourDeviceConfigQueries(vertx)
val mutations = YourDeviceConfigMutations(deviceStore, vertx)

val wiring = RuntimeWiring.newRuntimeWiring()
    .type("Query") { builder ->
        builder.dataFetcher("yourDeviceLiveMetrics", queries::yourDeviceLiveMetrics)
    }
    .type("Mutation") { builder ->
        builder
            .dataFetcher("createYourDeviceConfig", mutations::createYourDeviceConfig)
            .dataFetcher("updateYourDeviceConfig", mutations::updateYourDeviceConfig)
    }
    .build()
```

---

## Frontend Dashboard Integration

### 1. List View Page

**Location**: `src/main/resources/dashboard/pages/yourdevice-clients.html`

```html
<!DOCTYPE html>
<html>
<head>
    <title>YourDevice Clients</title>
    <link rel="stylesheet" href="../assets/style.css">
</head>
<body>
    <div id="content">
        <h1>YourDevice Clients</h1>
        <button id="createBtn">Create Client</button>
        <table id="clientsTable">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Namespace</th>
                    <th>Hostname</th>
                    <th>Status</th>
                    <th>Messages In</th>
                    <th>Messages Out</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody id="clientsTableBody"></tbody>
        </table>
    </div>
    <script src="../js/common.js"></script>
    <script src="../js/yourdevice-clients.js"></script>
</body>
</html>
```

**Location**: `src/main/resources/dashboard/js/yourdevice-clients.js`

```javascript
async function loadClients() {
    const query = `query {
        devices(type: "YourDevice-Client") {
            id name namespace enabled config
        }
    }`;
    const result = await graphqlQuery(query);
    const clients = result.devices || [];

    const tbody = document.getElementById('clientsTableBody');
    tbody.innerHTML = '';

    for (const client of clients) {
        const config = JSON.parse(client.config);
        const metrics = await fetchMetrics(client.id);

        const row = tbody.insertRow();
        row.innerHTML = `
            <td><a href="yourdevice-client-detail.html?id=${client.id}">${client.name}</a></td>
            <td>${client.namespace}</td>
            <td>${config.hostname}:${config.port}</td>
            <td>${metrics?.connected ? '🟢 Connected' : '🔴 Disconnected'}</td>
            <td>${metrics?.messagesIn || 0}</td>
            <td>${metrics?.messagesOut || 0}</td>
            <td>
                <button onclick="editClient('${client.id}')">Edit</button>
                <button onclick="deleteClient('${client.id}')">Delete</button>
            </td>
        `;
    }
}

async function fetchMetrics(deviceId) {
    const query = `query { yourDeviceLiveMetrics(deviceId: "${deviceId}") {
        messagesIn messagesOut connected
    }}`;
    const result = await graphqlQuery(query);
    return result.yourDeviceLiveMetrics;
}

async function deleteClient(id) {
    if (!confirm('Delete this client?')) return;
    const mutation = `mutation { deleteDeviceConfig(id: "${id}") }`;
    await graphqlQuery(mutation);
    loadClients();
}

document.getElementById('createBtn').addEventListener('click', () => {
    window.location.href = 'yourdevice-client-detail.html';
});

loadClients();
setInterval(loadClients, 5000); // Auto-refresh every 5 seconds
```

### 2. Detail View Page

**Location**: `src/main/resources/dashboard/pages/yourdevice-client-detail.html`

```html
<!DOCTYPE html>
<html>
<head>
    <title>YourDevice Client Details</title>
    <link rel="stylesheet" href="../assets/style.css">
</head>
<body>
    <div id="content">
        <h1 id="pageTitle">YourDevice Client</h1>
        <form id="clientForm">
            <label>Name: <input type="text" id="name" required></label>
            <label>Namespace: <input type="text" id="namespace" required></label>
            <label>Enabled: <input type="checkbox" id="enabled"></label>

            <h3>Connection</h3>
            <label>Hostname: <input type="text" id="hostname" required></label>
            <label>Port: <input type="number" id="port" required></label>
            <label>Username: <input type="text" id="username" required></label>
            <label>Password: <input type="password" id="password"></label>

            <h3>Addresses</h3>
            <button type="button" id="addAddressBtn">Add Address</button>
            <div id="addressesList"></div>

            <button type="submit">Save</button>
            <button type="button" onclick="history.back()">Cancel</button>
        </form>
    </div>
    <script src="../js/common.js"></script>
    <script src="../js/yourdevice-client-detail.js"></script>
</body>
</html>
```

**Location**: `src/main/resources/dashboard/js/yourdevice-client-detail.js`

```javascript
let clientId = null;
let addresses = [];

async function loadClient() {
    const params = new URLSearchParams(window.location.search);
    clientId = params.get('id');

    if (clientId) {
        const query = `query { device(id: "${clientId}") {
            id name namespace enabled config
        }}`;
        const result = await graphqlQuery(query);
        const client = result.device;
        const config = JSON.parse(client.config);

        document.getElementById('name').value = client.name;
        document.getElementById('namespace').value = client.namespace;
        document.getElementById('enabled').checked = client.enabled;
        document.getElementById('hostname').value = config.hostname;
        document.getElementById('port').value = config.port;
        document.getElementById('username').value = config.username;
        addresses = config.addresses || [];
        renderAddresses();
    }
}

function renderAddresses() {
    const container = document.getElementById('addressesList');
    container.innerHTML = addresses.map((addr, i) => `
        <div class="address-item">
            <input type="text" value="${addr.topic}"
                onchange="addresses[${i}].topic = this.value" placeholder="Topic">
            <input type="text" value="${addr.description || ''}"
                onchange="addresses[${i}].description = this.value" placeholder="Description">
            <label><input type="checkbox" ${addr.retained ? 'checked' : ''}
                onchange="addresses[${i}].retained = this.checked"> Retained</label>
            <button type="button" onclick="removeAddress(${i})">Remove</button>
        </div>
    `).join('');
}

function removeAddress(index) {
    addresses.splice(index, 1);
    renderAddresses();
}

document.getElementById('addAddressBtn').addEventListener('click', () => {
    addresses.push({ topic: '', description: '', retained: false });
    renderAddresses();
});

document.getElementById('clientForm').addEventListener('submit', async (e) => {
    e.preventDefault();

    const config = {
        hostname: document.getElementById('hostname').value,
        port: parseInt(document.getElementById('port').value),
        username: document.getElementById('username').value,
        password: document.getElementById('password').value || undefined,
        addresses: addresses
    };

    const mutation = clientId
        ? `mutation {
            updateYourDeviceConfig(
                id: "${clientId}"
                name: "${document.getElementById('name').value}"
                namespace: "${document.getElementById('namespace').value}"
                enabled: ${document.getElementById('enabled').checked}
                config: ${JSON.stringify(config).replace(/"/g, '\\"')}
            ) { id }
        }`
        : `mutation {
            createYourDeviceConfig(
                name: "${document.getElementById('name').value}"
                namespace: "${document.getElementById('namespace').value}"
                enabled: ${document.getElementById('enabled').checked}
                config: ${JSON.stringify(config).replace(/"/g, '\\"')}
            ) { id }
        }`;

    await graphqlQuery(mutation);
    window.location.href = 'yourdevice-clients.html';
});

loadClient();
```

### 3. Register Routes

**Location**: `src/main/resources/dashboard/components/sidebar.html`

```html
<li><a href="pages/yourdevice-clients.html">YourDevice Clients</a></li>
```

---

## Testing and Validation

### Backend Tests

```kotlin
class YourDeviceConnectorTest {
    @Test
    fun testConnect() {
        // Test connection logic
    }

    @Test
    fun testMessagePublish() {
        // Test MQTT publishing
    }

    @Test
    fun testReconnection() {
        // Test auto-reconnect
    }
}
```

### GraphQL Tests

```bash
# Test queries
curl -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ devices(type: \"YourDevice-Client\") { id name } }"}'

# Test mutations
curl -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "mutation { createYourDeviceConfig(...) { id } }"}'
```

### Integration Tests

1. Create device via dashboard
2. Verify deployment in logs
3. Check MQTT messages published
4. Test metrics collection
5. Test update/delete operations

---

## Common Patterns and Best Practices

### 1. Error Handling

```kotlin
connect()
    .onSuccess { logger.info("Connected") }
    .onFailure { error ->
        logger.severe("Connection failed: ${error.message}")
        scheduleReconnect()
    }
```

### 2. Resource Cleanup

```kotlin
override fun stop(stopPromise: Promise<Void>) {
    // Cancel timers
    timerId?.let { vertx.cancelTimer(it) }

    // Close connections
    disconnect()
        .onComplete { stopPromise.complete() }
}
```

### 3. Configuration Validation

```kotlin
init {
    require(port in 1..65535) { "Invalid port" }
    require(hostname.isNotBlank()) { "Hostname required" }
}
```

### 4. Cluster Awareness

```kotlin
private fun shouldManageDevice(device: DeviceConfig): Boolean {
    return clusterManager.isLocalNodeResponsible(device.id)
}
```

### 5. Internal MQTT Subscriptions

When your device needs to **subscribe to MQTT topics** (e.g., to receive commands or listen for specific messages), use the **SessionHandler internal client subscription pattern**. This treats your device as a regular MQTT subscriber internally, ensuring proper routing, wildcards, and QoS handling.

#### Step 1: Register EventBus consumer for receiving messages

```kotlin
class YourDeviceConnector : AbstractVerticle() {
    private var internalClientId: String? = null

    private fun subscribeToMqttTopics(): Future<Void> {
        val promise = Promise.promise<Void>()
        val sessionHandler = Monster.getSessionHandler()

        if (sessionHandler == null) {
            promise.fail("SessionHandler not available")
            return promise.future()
        }

        // Create unique internal client ID
        internalClientId = "yourdevice-${deviceConfig.name}"

        // Register EventBus consumer to receive MQTT messages
        // Handles both individual and bulk messages
        vertx.eventBus().consumer<Any>(EventBusAddresses.Client.messages(internalClientId!!)) { busMessage ->
            try {
                val messages = when (val body = busMessage.body()) {
                    is BrokerMessage -> listOf(body)
                    is BulkClientMessage -> body.messages
                    else -> {
                        logger.warning("Unknown message type: ${body?.javaClass?.simpleName}")
                        emptyList()
                    }
                }
                messages.forEach { message ->
                    handleMqttMessage(message)
                }
            } catch (e: Exception) {
                logger.warning("Error processing MQTT message: ${e.message}")
            }
        }

        // Subscribe to MQTT topic via SessionHandler
        val topicFilter = "${deviceConfig.namespace}/commands/#"  // Supports wildcards
        val qos = 0  // QoS level (0, 1, or 2)

        logger.info("Internal subscription for client '$internalClientId' to topic '$topicFilter' with QoS $qos")
        sessionHandler.subscribeInternalClient(internalClientId!!, topicFilter, qos)

        promise.complete()
        return promise.future()
    }

    private fun handleMqttMessage(message: BrokerMessage) {
        logger.info("Received MQTT message: ${message.topicName} = ${String(message.payload)}")
        // Process the message...
    }
}
```

#### Step 2: Unsubscribe on stop

```kotlin
override fun stop(stopPromise: Promise<Void>) {
    internalClientId?.let { clientId ->
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler != null) {
            // Unsubscribe from all topics
            val topicFilter = "${deviceConfig.namespace}/commands/#"
            sessionHandler.unsubscribeInternalClient(clientId, topicFilter)

            // Unregister the internal client
            sessionHandler.unregisterInternalClient(clientId)

            logger.info("Unsubscribed internal client '$clientId'")
        }
    }
    stopPromise.complete()
}
```

#### Key Points

- **Use `EventBusAddresses.Client.messages(clientId)`** - Standard address pattern for MQTT message delivery
- **Support both message types** - Handle `BrokerMessage` (single) and `BulkClientMessage` (batch)
- **Wildcards supported** - Use `+` (single level) and `#` (multi-level) in topic filters
- **QoS respected** - Messages are delivered according to the requested QoS level
- **Clean shutdown** - Always unsubscribe and unregister on stop

#### Example Use Cases

- **SparkplugB Decoder**: Subscribes to `spBv1.0/#` to receive all SparkplugB messages
- **PLC4X Connector**: Subscribes to command topics for writing values to PLCs
- **Flow Engine**: Subscribes to trigger topics to start flow executions

### 6. Topic Naming

Use hierarchical namespaces:
```
{namespace}/{function}/{device}/{datapoint}

Examples:
plant1/yourdevice/sensor1/temperature
plant1/yourdevice/actuator1/status
```

Avoid deep nesting (keep under 5 levels).

### 7. Logging

```kotlin
logger.severe("Failed to connect")           // Errors
logger.warning("Connection lost, retrying")  // Warnings
logger.info("Client started successfully")   // Important events
logger.fine("Published message to topic")    // Debug details
```

---

## Metrics Implementation Guide

### Backend Metrics Collection

```kotlin
class YourDeviceConnector : AbstractVerticle() {
    private val messagesInCounter = AtomicLong(0)
    private val messagesOutCounter = AtomicLong(0)
    private var isConnected = false

    private fun setupEventBusConsumers() {
        vertx.eventBus().consumer<JsonObject>(
            "${YourDeviceExtension.ADDRESS_DEVICE_METRICS}.${deviceConfig.id}"
        ) { message ->
            message.reply(JsonObject()
                .put("messagesIn", messagesInCounter.get())
                .put("messagesOut", messagesOutCounter.get())
                .put("connected", isConnected))
        }
    }
}
```

### GraphQL Schema

```graphql
type YourDeviceMetrics {
    messagesIn: Long!
    messagesOut: Long!
    connected: Boolean!
}

extend type Query {
    yourDeviceLiveMetrics(deviceId: ID!): YourDeviceMetrics
}
```

### GraphQL Query Resolver

```kotlin
fun yourDeviceLiveMetrics(env: DataFetchingEnvironment): CompletableFuture<JsonObject?> {
    val deviceId = env.getArgument<String>("deviceId")
    val promise = Promise.promise<JsonObject?>()

    vertx.eventBus().request<JsonObject>(
        "${YourDeviceExtension.ADDRESS_DEVICE_METRICS}.$deviceId",
        JsonObject(),
        DeliveryOptions().setSendTimeout(5000)
    ).onSuccess { reply ->
        promise.complete(reply.body())
    }.onFailure {
        promise.complete(null) // Return null if device offline
    }

    return promise.future().toCompletionStage().toCompletableFuture()
}
```

### Frontend Integration

```javascript
async function fetchMetrics(deviceId) {
    const query = `query {
        yourDeviceLiveMetrics(deviceId: "${deviceId}") {
            messagesIn messagesOut connected
        }
    }`;
    const result = await graphqlQuery(query);
    return result.yourDeviceLiveMetrics; // May be null
}

async function loadClients() {
    // ...
    for (const client of clients) {
        const metrics = await fetchMetrics(client.id);
        row.innerHTML = `
            <td>${metrics?.connected ? '🟢' : '🔴'}</td>
            <td>${metrics?.messagesIn || 0}</td>
            <td>${metrics?.messagesOut || 0}</td>
        `;
    }
}
```

### Common Pitfalls

**Metrics not collected**: Ensure `setupEventBusConsumers()` called in `start()`

**Compilation errors**: Import `java.util.concurrent.atomic.AtomicLong`

**Historical metrics don't work**: Use archive groups with topic filters for persistence

---

## Summary Checklist

### Backend (Kotlin)
- [ ] Config data classes with `fromJsonObject()` / `toJsonObject()`
- [ ] Connector verticle with connection lifecycle
- [ ] Extension verticle with cluster awareness
- [ ] Integration in Monster.kt
- [ ] Metrics collection via EventBus

### GraphQL API
- [ ] Schema types and inputs
- [ ] Query resolvers (including metrics)
- [ ] Mutation resolvers (create/update/delete)
- [ ] Register in GraphQLServer.kt

### Frontend Dashboard
- [ ] List view (HTML + JS)
- [ ] Detail view (HTML + JS)
- [ ] Sidebar navigation
- [ ] Metrics display with null handling

### Testing
- [ ] Unit tests for config parsing
- [ ] Integration test for deployment
- [ ] MQTT message flow verification
- [ ] GraphQL API testing
- [ ] Dashboard manual testing

---

## Common UI and GraphQL Integration Pitfalls

### 1. Addresses Management in Client Updates

**Problem**: When updating a client with addresses, GraphQL input doesn't preserve existing addresses if password is omitted.

**Solution**: Always send complete address list in updates:

```javascript
// ✓ Correct
const mutation = `mutation {
    updateYourDeviceConfig(
        id: "${clientId}"
        config: {
            addresses: ${JSON.stringify(addresses)}
            password: "${password || currentPassword}"
        }
    ) { id }
}`;
```

### 2. Password Preservation

**Problem**: Password gets cleared on update if not provided.

**Solution**: In mutation resolver, preserve existing password if not provided:

```kotlin
fun updateYourDeviceConfig(env: DataFetchingEnvironment): CompletableFuture<DeviceConfig> {
    return deviceStore.get(id).compose { existing ->
        val existingConfig = YourDeviceConfig.fromJsonObject(existing.config)
        val newPassword = configInput["password"] as? String
            ?: existingConfig.password // Preserve existing
        // ...
    }
}
```

### 3. GraphQL Query Field Mismatches

**Problem**: Frontend queries field that doesn't exist in schema.

**Solution**: Ensure schema matches data:

```graphql
# Schema must match what resolver returns
type YourDeviceAddress {
    topic: String!
    description: String  # Nullable if optional
}
```

### 4. Array Field Handling

**Problem**: Inconsistent field names (e.g., `nameFilters` vs `browseArguments`).

**Solution**: Support backward compatibility in parsing:

```kotlin
val nameFiltersList = json.getJsonArray("nameFilters")?.map { it.toString() }
    ?: json.getJsonObject("browseArguments")?.getString("filter")?.let { listOf(it) }
```

### 5. GraphQL Type vs Input Type Confusion

**Problem**: Using output type in mutation input.

**Solution**: Always use separate `Input` types for mutations:

```graphql
type YourDeviceConfig { ... }       # For queries
input YourDeviceConfigInput { ... } # For mutations
```

### 6. Null vs Empty String

**Problem**: Frontend sends empty strings instead of null.

**Solution**: Normalize in frontend:

```javascript
password: document.getElementById('password').value || undefined
```

### 7. Live Metrics Fallback Pattern

**Problem**: Metrics query fails if device offline, breaking UI.

**Solution**: Return null and handle in frontend:

```javascript
const metrics = await fetchMetrics(client.id);
// Use optional chaining
const status = metrics?.connected ? 'Online' : 'Offline';
```

---

## Example: WinCC Unified Integration

For complete implementation examples, see:

- **Backend**: `src/main/kotlin/devices/winccua/`
  - `WinCCUaConnector.kt` - Connection handling
  - `WinCCUaExtension.kt` - Extension management
- **Config**: `src/main/kotlin/stores/devices/WinCCUaConnectionConfig.kt`
- **GraphQL**:
  - `src/main/kotlin/graphql/WinCCUaConfigQueries.kt`
  - `src/main/kotlin/graphql/WinCCUaConfigMutations.kt`
  - `src/main/resources/schema.graphqls` (WinCCUA types)
- **Frontend**:
  - `src/main/resources/dashboard/pages/winccua-clients.html`
  - `src/main/resources/dashboard/js/winccua-clients.js`
  - `src/main/resources/dashboard/pages/winccua-client-detail.html`
  - `src/main/resources/dashboard/js/winccua-client-detail.js`

Other reference implementations:
- **MQTT Client**: `src/main/kotlin/devices/mqtt/`
- **PLC4X**: `src/main/kotlin/devices/plc4x/`
- **SparkplugB**: `src/main/kotlin/extensions/SparkplugExtension.kt`
