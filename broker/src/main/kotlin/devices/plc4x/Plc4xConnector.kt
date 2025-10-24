package at.rocworks.devices.plc4x

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.data.TopicTree
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.devices.Plc4xConnectionConfig
import at.rocworks.stores.devices.Plc4xAddress
import at.rocworks.stores.devices.Plc4xAddressMode
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import org.apache.plc4x.java.api.PlcConnection
import org.apache.plc4x.java.api.PlcDriverManager
import org.apache.plc4x.java.api.messages.PlcReadRequest
import org.apache.plc4x.java.api.messages.PlcReadResponse
import org.apache.plc4x.java.api.messages.PlcWriteRequest
import org.apache.plc4x.java.api.messages.PlcWriteResponse
import org.apache.plc4x.java.api.types.PlcResponseCode
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

/**
 * PLC4X Connector - Handles connection to a single PLC device
 *
 * Responsibilities:
 * - Maintains PLC4X connection using PlcDriverManager
 * - Polls configured addresses at specified intervals (READ and READ_WRITE modes)
 * - Subscribes to MQTT topics and writes to PLC (WRITE and READ_WRITE modes)
 * - Applies value transformations (scaling, offset) and deadband filtering
 * - Publishes PLC values as MQTT messages via EventBus
 * - Handles reconnection and error recovery
 * - Tracks metrics (messagesIn for PLC->MQTT, messagesOut for MQTT->PLC, connected status)
 * - Loop prevention for READ_WRITE mode
 *
 * Supports multiple industrial protocols through PLC4X:
 * - Siemens S7, Modbus TCP/RTU/ASCII, Beckhoff ADS, BACnet, KNXnet/IP
 * - Allen-Bradley EtherNet/IP, CANopen, Firmata, and more
 */
class Plc4xConnector : AbstractVerticle() {

    // Metrics counters (PLC -> MQTT is messagesIn, MQTT -> PLC is messagesOut)
    private val messagesInCounter = java.util.concurrent.atomic.AtomicLong(0)
    private val messagesOutCounter = java.util.concurrent.atomic.AtomicLong(0)
    private var lastMetricsReset = System.currentTimeMillis()

    private val logger: Logger = Utils.getLogger(this::class.java)

    // Device configuration
    private lateinit var deviceConfig: DeviceConfig
    private lateinit var plc4xConfig: Plc4xConnectionConfig

    // PLC4X connection
    private var connection: PlcConnection? = null
    private var driverManager: PlcDriverManager? = null

    // Connection state
    private var isConnected = false
    private var isReconnecting = false
    private var reconnectTimerId: Long? = null
    private var pollingTimerId: Long? = null

    // Last values for deadband filtering, publish-on-change, and loop prevention
    private val lastNumericValues = ConcurrentHashMap<String, Number>() // address.name -> last published numeric value
    private val lastRawValues = ConcurrentHashMap<String, Any>() // address.name -> last published value (any type)
    private val lastReadValues = ConcurrentHashMap<String, Any>() // address.name -> last value READ from PLC (for loop prevention)

    // Track subscribed MQTT topics for WRITE and READ_WRITE modes
    private val subscribedTopics = ConcurrentHashMap<String, Plc4xAddress>() // mqttTopic -> address

    override fun start(startPromise: Promise<Void>) {
        try {
            // Load device configuration
            val deviceJson = config().getJsonObject("device")
            deviceConfig = DeviceConfig.fromJsonObject(deviceJson)
            plc4xConfig = Plc4xConnectionConfig.fromJsonObject(deviceConfig.config)

            logger.info("Starting Plc4xConnector for device: ${deviceConfig.name} (protocol: ${plc4xConfig.protocol})")

            // Validate configuration
            val validationErrors = plc4xConfig.validate()
            if (validationErrors.isNotEmpty()) {
                val errorMsg = "Configuration validation failed: ${validationErrors.joinToString(", ")}"
                logger.severe(errorMsg)
                startPromise.fail(Exception(errorMsg))
                return
            }

            // Initialize PLC4X driver manager
            driverManager = PlcDriverManager.getDefault()

            // Register metrics endpoint
            setupMetricsEndpoint()

            // Start connector successfully regardless of initial connection status
            logger.info("Plc4xConnector for device ${deviceConfig.name} started successfully")
            startPromise.complete()

            // Setup MQTT subscriptions for WRITE and READ_WRITE modes
            setupMqttSubscriptions()

            // Attempt initial connection in the background - if it fails, reconnection will be scheduled
            connectToPlc()
                .compose { setupPolling() }
                .onComplete { result ->
                    if (result.succeeded()) {
                        logger.info("Initial PLC connection successful for device ${deviceConfig.name}")
                    } else {
                        logger.warning("Initial PLC connection failed for device ${deviceConfig.name}: ${result.cause()?.message}. Will retry automatically.")
                    }
                }

        } catch (e: Exception) {
            logger.severe("Exception during Plc4xConnector startup: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Stopping Plc4xConnector for device: ${deviceConfig.name}")

        // Cancel any pending timers
        reconnectTimerId?.let { timerId ->
            vertx.cancelTimer(timerId)
            reconnectTimerId = null
            logger.info("Cancelled pending reconnection timer for device ${deviceConfig.name}")
        }

        pollingTimerId?.let { timerId ->
            vertx.cancelTimer(timerId)
            pollingTimerId = null
            logger.info("Cancelled polling timer for device ${deviceConfig.name}")
        }

        // Unsubscribe from all MQTT topics
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler != null) {
            val clientId = "plc4x-connector-${deviceConfig.name}"
            subscribedTopics.forEach { (topic, address) ->
                sessionHandler.unsubscribeInternalClient(clientId, topic)
                logger.info("Unsubscribed from MQTT topic '$topic' for address ${address.name}")
            }
            subscribedTopics.clear()
            // Unregister the client
            sessionHandler.unregisterInternalClient(clientId)
        } else {
            subscribedTopics.clear()
        }

        disconnectFromPlc()
            .onComplete { result ->
                if (result.succeeded()) {
                    logger.info("Plc4xConnector for device ${deviceConfig.name} stopped successfully")
                } else {
                    logger.warning("Error during Plc4xConnector shutdown: ${result.cause()?.message}")
                }
                stopPromise.complete()
            }
    }

    private fun setupMetricsEndpoint() {
        val addr = EventBusAddresses.Plc4xBridge.connectorMetrics(deviceConfig.name)
        vertx.eventBus().consumer<JsonObject>(addr) { msg ->
            try {
                val now = System.currentTimeMillis()
                val elapsedMs = now - lastMetricsReset
                val elapsedSec = if (elapsedMs > 0) elapsedMs / 1000.0 else 1.0
                val inCount = messagesInCounter.getAndSet(0)
                val outCount = messagesOutCounter.getAndSet(0)
                lastMetricsReset = now
                val json = JsonObject()
                    .put("device", deviceConfig.name)
                    .put("messagesInRate", inCount / elapsedSec)
                    .put("messagesOutRate", outCount / elapsedSec)
                    .put("elapsedMs", elapsedMs)
                    .put("connected", isConnected)
                msg.reply(json)
            } catch (e: Exception) {
                msg.fail(500, e.message)
            }
        }
        logger.info("Registered PLC4X metrics endpoint for device ${deviceConfig.name} at address $addr")
    }

    private fun connectToPlc(): Future<Void> {
        val promise = Promise.promise<Void>()

        if (isConnected || isReconnecting) {
            promise.complete()
            return promise.future()
        }

        isReconnecting = true

        vertx.executeBlocking<Void> {
            try {
                logger.info("Connecting to PLC: ${plc4xConfig.connectionString}")

                // PLC4X automatically selects the driver based on connection string prefix
                // Examples: "s7://192.168.1.10", "modbus-tcp://192.168.1.20:502", "ads://192.168.1.30"
                connection = driverManager!!.getConnectionManager().getConnection(plc4xConfig.connectionString)

                if (connection != null && connection!!.isConnected) {
                    isConnected = true
                    isReconnecting = false
                    logger.info("Connected to PLC: ${plc4xConfig.connectionString} (protocol: ${plc4xConfig.protocol})")
                    null // Return null for Void
                } else {
                    isReconnecting = false
                    logger.severe("Failed to connect to PLC: connection is null or not connected")
                    scheduleReconnection()
                    throw Exception("Failed to connect to PLC: connection is null or not connected")
                }

            } catch (e: Exception) {
                isReconnecting = false
                logger.severe("Exception during PLC connection: ${e.message}")
                scheduleReconnection()
                throw e
            }
        }.onComplete { result ->
            if (result.succeeded()) {
                promise.complete()
            } else {
                promise.fail(result.cause())
            }
        }

        return promise.future()
    }

    private fun disconnectFromPlc(): Future<Void> {
        val promise = Promise.promise<Void>()

        // Cancel any pending timers
        reconnectTimerId?.let { timerId ->
            vertx.cancelTimer(timerId)
            reconnectTimerId = null
        }

        pollingTimerId?.let { timerId ->
            vertx.cancelTimer(timerId)
            pollingTimerId = null
        }

        if (connection != null && connection!!.isConnected) {
            vertx.executeBlocking<Void> {
                try {
                    connection!!.close()
                    isConnected = false
                    isReconnecting = false
                    connection = null
                    lastNumericValues.clear()
                    lastRawValues.clear()
                    lastReadValues.clear()
                    logger.info("Disconnected from PLC: ${plc4xConfig.connectionString}")
                    null // Return null for Void
                } catch (e: Exception) {
                    logger.warning("Error disconnecting from PLC: ${e.message}")
                    null // Complete anyway
                }
            }.onComplete { result ->
                promise.complete()
            }
        } else {
            isConnected = false
            isReconnecting = false
            connection = null
            lastNumericValues.clear()
            lastRawValues.clear()
            lastReadValues.clear()
            promise.complete()
        }

        return promise.future()
    }

    private fun scheduleReconnection() {
        if (!isReconnecting) {
            // Cancel any existing reconnection timer
            reconnectTimerId?.let { timerId ->
                vertx.cancelTimer(timerId)
            }

            // Schedule new reconnection attempt
            reconnectTimerId = vertx.setTimer(plc4xConfig.reconnectDelay) {
                reconnectTimerId = null
                if (!isConnected) {
                    logger.info("Attempting to reconnect to PLC for device ${deviceConfig.name}...")
                    connectToPlc()
                        .compose { setupPolling() }
                        .onComplete { result ->
                            if (result.failed()) {
                                logger.warning("Reconnection failed for device ${deviceConfig.name}: ${result.cause()?.message}")
                            }
                        }
                }
            }
            logger.info("Scheduled reconnection for device ${deviceConfig.name} in ${plc4xConfig.reconnectDelay}ms")
        }
    }

    private fun setupPolling(): Future<Void> {
        val promise = Promise.promise<Void>()

        if (!plc4xConfig.enabled) {
            logger.info("Polling disabled for device ${deviceConfig.name}")
            promise.complete()
            return promise.future()
        }

        if (plc4xConfig.addresses.isEmpty()) {
            logger.info("No addresses configured for device ${deviceConfig.name}")
            promise.complete()
            return promise.future()
        }

        logger.info("Setting up polling for ${plc4xConfig.addresses.size} addresses at ${plc4xConfig.pollingInterval}ms interval")

        // Start periodic polling
        pollingTimerId = vertx.setPeriodic(plc4xConfig.pollingInterval) {
            if (isConnected) {
                pollAddresses()
            }
        }

        // Do initial poll
        pollAddresses()

        promise.complete()
        return promise.future()
    }

    private fun pollAddresses() {
        if (!isConnected || connection == null) {
            return
        }

        // Only poll addresses in READ or READ_WRITE mode (not WRITE-only)
        val enabledAddresses = plc4xConfig.addresses.filter {
            it.enabled && (it.mode == Plc4xAddressMode.READ || it.mode == Plc4xAddressMode.READ_WRITE)
        }
        if (enabledAddresses.isEmpty()) {
            return
        }

        vertx.executeBlocking<Unit> {
            try {
                // Create read request builder
                val requestBuilder = connection!!.readRequestBuilder()

                // Add all enabled addresses to the request
                enabledAddresses.forEach { address ->
                    requestBuilder.addTagAddress(address.name, address.address)
                }

                // Build and execute read request
                val readRequest = requestBuilder.build()
                val responseFuture = readRequest.execute()

                // Wait for response
                val response = responseFuture.get()

                // Process response for each address
                enabledAddresses.forEach { address ->
                    try {
                        val responseCode = response.getResponseCode(address.name)
                        if (responseCode == PlcResponseCode.OK) {
                            val value = response.getObject(address.name)
                            if (value != null) {
                                handleValueChange(address, value)
                            }
                        } else {
                            logger.warning("Failed to read address ${address.name}: $responseCode")
                        }
                    } catch (e: Exception) {
                        logger.warning("Error processing address ${address.name}: ${e.message}")
                    }
                }

                Unit // Return Unit

            } catch (e: Exception) {
                logger.severe("Error polling addresses: ${e.message}")
                // Connection might be lost - schedule reconnection
                isConnected = false
                scheduleReconnection()
                throw e
            }
        }
    }

    private fun handleValueChange(address: Plc4xAddress, rawValue: Any) {
        try {
            // Track the last value READ from PLC for loop prevention in READ_WRITE mode
            lastReadValues[address.name] = rawValue

            // Convert to Number if possible for transformation and deadband
            val numericValue = when (rawValue) {
                is Number -> rawValue
                is Boolean -> if (rawValue) 1 else 0
                is String -> rawValue.toDoubleOrNull()
                else -> null
            }

            // Apply value transformation if numeric
            val transformedValue = if (numericValue != null && (address.scalingFactor != null || address.offset != null)) {
                address.transformValue(numericValue)
            } else {
                rawValue
            }

            // Check deadband for numeric values (only if deadband is configured)
            if (numericValue != null && address.deadband != null) {
                val lastValue = lastNumericValues[address.name]
                if (lastValue != null && !address.exceedsDeadband(lastValue, numericValue)) {
                    // Value change within deadband - don't publish
                    logger.finest("Value change within deadband for ${address.name}: $lastValue -> $numericValue")
                    return
                }
            }

            // Check publish-on-change: only publish if value actually changed
            if (address.publishOnChange) {
                val lastValue = lastRawValues[address.name]
                if (lastValue != null && lastValue == rawValue) {
                    // Value hasn't changed - don't publish
                    logger.finest("Value unchanged for ${address.name}, skipping publish: $rawValue")
                    return
                }
            }

            // Update last values
            if (numericValue != null) {
                lastNumericValues[address.name] = numericValue
            }
            lastRawValues[address.name] = rawValue

            // Create MQTT message payload
            val payload = JsonObject()
                .put("value", transformedValue)
                .put("timestamp", Instant.now().toString())
                .put("device", deviceConfig.name)
                .put("address", address.name)

            // Generate MQTT topic - use configured topic or default namespace/address pattern
            val mqttTopic = if (address.topic.isNotBlank()) {
                address.topic
            } else {
                "${deviceConfig.namespace}/${address.name}"
            }

            // Publish to MQTT via EventBus
            val mqttMessage = BrokerMessage(
                messageId = 0,
                topicName = mqttTopic,
                payload = payload.encode().toByteArray(),
                qosLevel = address.qos,
                isRetain = address.retained,
                isDup = false,
                isQueued = false,
                clientId = "plc4x-connector-${deviceConfig.name}"
            )

            // Send to PLC4X extension for proper message bus publishing
            vertx.eventBus().publish(Plc4xExtension.ADDRESS_PLC4X_VALUE_PUBLISH, mqttMessage)

            messagesInCounter.incrementAndGet()
            logger.fine { "Published PLC value: $mqttTopic = $transformedValue (from ${address.address})" }

        } catch (e: Exception) {
            logger.severe("Error handling value change for address ${address.name}: ${e.message}")
        }
    }

    /**
     * Setup MQTT subscriptions for addresses in WRITE or READ_WRITE mode
     */
    private fun setupMqttSubscriptions() {
        // Find all addresses that need MQTT subscriptions (WRITE or READ_WRITE mode)
        val writeAddresses = plc4xConfig.addresses.filter {
            it.enabled && (it.mode == Plc4xAddressMode.WRITE || it.mode == Plc4xAddressMode.READ_WRITE)
        }

        if (writeAddresses.isEmpty()) {
            logger.info("No addresses configured for WRITE mode for device ${deviceConfig.name}")
            return
        }

        // Subscribe to MQTT topics using SessionHandler
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler != null) {
            val clientId = "plc4x-connector-${deviceConfig.name}"

            // Register eventBus consumer for this PLC4X connector (handles both individual and bulk messages)
            vertx.eventBus().consumer<Any>(at.rocworks.bus.EventBusAddresses.Client.messages(clientId)) { busMessage ->
                try {
                    val messages = when (val body = busMessage.body()) {
                        is BrokerMessage -> listOf(body)
                        is at.rocworks.data.BulkClientMessage -> body.messages
                        else -> {
                            logger.warning("Unknown message type: ${body?.javaClass?.simpleName}")
                            emptyList()
                        }
                    }
                    messages.forEach { message ->
                        // Find the address config for this topic
                        val address = subscribedTopics.values.find { addr ->
                            val topic = if (addr.topic.isNotBlank()) addr.topic else "${deviceConfig.namespace}/${addr.name}"
                            TopicTree.matches(topic, message.topicName)
                        }
                        if (address != null) {
                            handleMqttMessage(address, message)
                        }
                    }
                } catch (e: Exception) {
                    logger.warning("Error processing MQTT message: ${e.message}")
                }
            }

            writeAddresses.forEach { address ->
                // Generate MQTT topic - use configured topic or default namespace/address pattern
                val mqttTopic = if (address.topic.isNotBlank()) {
                    address.topic
                } else {
                    "${deviceConfig.namespace}/${address.name}"
                }

                val qos = address.qos

                logger.info("Internal subscription for PLC4X client '$clientId' to MQTT topic '$mqttTopic' with QoS $qos (mode: ${address.mode})")

                sessionHandler.subscribeInternalClient(clientId, mqttTopic, qos)

                subscribedTopics[mqttTopic] = address
            }
        } else {
            logger.severe("SessionHandler not available for MQTT subscriptions")
        }
    }

    /**
     * Handle incoming MQTT message and write value to PLC
     */
    private fun handleMqttMessage(address: Plc4xAddress, message: BrokerMessage) {
        try {
            if (!isConnected || connection == null) {
                logger.warning("Cannot write to PLC - not connected (address: ${address.name})")
                return
            }

            // Parse the MQTT payload to extract the value
            val payloadString = String(message.payload)
            val value = try {
                val json = JsonObject(payloadString)
                json.getValue("value")
            } catch (e: Exception) {
                // If not JSON, try to parse as plain value
                payloadString.toDoubleOrNull() ?: payloadString
            }

            // Loop prevention for READ_WRITE mode
            if (address.mode == Plc4xAddressMode.READ_WRITE) {
                val lastReadValue = lastReadValues[address.name]
                if (lastReadValue != null && lastReadValue == value) {
                    // This value came from our own read - don't write it back to avoid loop
                    logger.finest("Skipping PLC write for ${address.name} - value matches last read (loop prevention): $value")
                    return
                }
            }

            // Convert value to appropriate type for PLC
            val plcValue = when (value) {
                is Number -> {
                    // Apply reverse transformation (undo scaling and offset)
                    if (address.scalingFactor != null || address.offset != null) {
                        address.reverseTransformValue(value)
                    } else {
                        value
                    }
                }
                is String -> value.toDoubleOrNull() ?: value
                else -> value
            }

            // Write to PLC using blocking call
            vertx.executeBlocking<Unit> {
                try {
                    // Create write request
                    val requestBuilder = connection!!.writeRequestBuilder()
                    requestBuilder.addTagAddress(address.name, address.address, plcValue)

                    // Build and execute write request
                    val writeRequest = requestBuilder.build()
                    val responseFuture = writeRequest.execute()

                    // Wait for response
                    val response = responseFuture.get()

                    // Check response
                    val responseCode = response.getResponseCode(address.name)
                    if (responseCode == PlcResponseCode.OK) {
                        messagesOutCounter.incrementAndGet()
                        logger.fine { "Successfully wrote value to PLC: ${address.address} = $plcValue (from MQTT topic ${message.topicName})" }
                    } else {
                        logger.warning("Failed to write to PLC address ${address.name}: $responseCode")
                    }

                    Unit // Return Unit

                } catch (e: Exception) {
                    logger.severe("Error writing to PLC address ${address.name}: ${e.message}")
                    throw e
                }
            }.onComplete { result ->
                if (result.failed()) {
                    // Connection might be lost - schedule reconnection
                    if (isConnected) {
                        isConnected = false
                        scheduleReconnection()
                    }
                }
            }

        } catch (e: Exception) {
            logger.severe("Error handling MQTT message for address ${address.name}: ${e.message}")
        }
    }
}
