package at.rocworks.devices.winccoa

import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.bus.EventBusAddresses
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.WinCCOaAddress
import at.rocworks.stores.WinCCOaConnectionConfig
import at.rocworks.stores.WinCCOaTransformConfig
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.WebSocket
import io.vertx.core.http.WebSocketClient
import io.vertx.core.http.WebSocketConnectOptions
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger

/**
 * WinCC OA Connector - Handles connection to WinCC OA GraphQL server
 *
 * Responsibilities:
 * - Maintains WebSocket connection to WinCC OA GraphQL server
 * - Handles authentication (login mutation or token)
 * - Manages GraphQL subscriptions (dpQueryConnectSingle)
 * - Transforms datapoint names to MQTT topics with caching
 * - Publishes WinCC OA value changes as MQTT messages
 * - Handles reconnection and error recovery
 */
class WinCCOaConnector : AbstractVerticle() {

    // Metrics counters
    private val messagesInCounter = AtomicLong(0)
    private var lastMetricsReset = System.currentTimeMillis()

    private val logger: Logger = Utils.getLogger(this::class.java)

    // Device configuration
    private lateinit var deviceConfig: DeviceConfig
    private lateinit var winCCOaConfig: WinCCOaConnectionConfig

    // HTTP and WebSocket clients
    private lateinit var webClient: WebClient
    private var wsClient: WebSocketClient? = null
    private var webSocket: WebSocket? = null

    // Authentication
    private var authToken: String? = null

    // Connection state
    private var isConnected = false
    private var isReconnecting = false
    private var reconnectTimerId: Long? = null

    // Subscription tracking
    private val activeSubscriptions = ConcurrentHashMap<String, WinCCOaAddress>() // subscriptionId -> address
    private var nextSubscriptionId = 1

    // Topic transformation cache (dpName -> mqttTopic)
    private val topicCache = ConcurrentHashMap<String, String>()

    override fun start(startPromise: Promise<Void>) {
        try {
            // Load device configuration
            val deviceJson = config().getJsonObject("device")
            deviceConfig = DeviceConfig.fromJsonObject(deviceJson)
            winCCOaConfig = WinCCOaConnectionConfig.fromJsonObject(deviceConfig.config)

            logger.info("Starting WinCCOaConnector for device: ${deviceConfig.name}")

            // Validate configuration
            val validationErrors = winCCOaConfig.validate()
            if (validationErrors.isNotEmpty()) {
                val errorMsg = "Configuration validation failed: ${validationErrors.joinToString(", ")}"
                logger.severe(errorMsg)
                startPromise.fail(errorMsg)
                return
            }

            // Initialize HTTP client for REST API calls
            val webClientOptions = WebClientOptions()
                .setConnectTimeout(winCCOaConfig.connectionTimeout.toInt())

            webClient = WebClient.create(vertx, webClientOptions)

            // Initialize WebSocket client (Vert.x 5)
            wsClient = vertx.createWebSocketClient()

            // Register metrics endpoint
            setupMetricsEndpoint()

            startPromise.complete()

            // Attempt initial connection in the background
            authenticate()
                .compose { connectWebSocket() }
                .compose { setupSubscriptions() }
                .onComplete { result ->
                    if (result.succeeded()) {
                        logger.info("Initial WinCC OA connection successful for device ${deviceConfig.name}")
                    } else {
                        logger.warning("Initial WinCC OA connection failed for device ${deviceConfig.name}: ${result.cause()?.message}. Will retry automatically.")
                    }
                }

        } catch (e: Exception) {
            logger.severe("Exception during WinCCOaConnector startup: ${e.message}")
            e.printStackTrace()
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Stopping WinCCOaConnector for device: ${deviceConfig.name}")

        // Cancel any pending reconnection timer
        reconnectTimerId?.let { timerId ->
            vertx.cancelTimer(timerId)
            reconnectTimerId = null
        }

        disconnectWebSocket()
            .onComplete {
                webClient.close()
                wsClient?.close()
                stopPromise.complete()
            }
    }

    private fun setupMetricsEndpoint() {
        val addr = EventBusAddresses.WinCCOaBridge.connectorMetrics(deviceConfig.name)
        vertx.eventBus().consumer<JsonObject>(addr) { msg ->
            try {
                val now = System.currentTimeMillis()
                val elapsedMs = now - lastMetricsReset
                val elapsedSec = if (elapsedMs > 0) elapsedMs / 1000.0 else 1.0
                val inCount = messagesInCounter.getAndSet(0)
                lastMetricsReset = now
                val json = JsonObject()
                    .put("device", deviceConfig.name)
                    .put("messagesInRate", inCount / elapsedSec)
                    .put("elapsedMs", elapsedMs)
                msg.reply(json)
            } catch (e: Exception) {
                msg.fail(500, e.message)
            }
        }
        logger.info("Registered WinCC OA metrics endpoint for device ${deviceConfig.name} at address $addr")
    }

    /**
     * Authenticate with WinCC OA GraphQL server
     * Either uses configured token, performs login mutation, or skips authentication for anonymous access
     */
    private fun authenticate(): Future<Void> {
        val promise = Promise.promise<Void>()

        // If token is provided, use it directly
        if (winCCOaConfig.token != null) {
            authToken = winCCOaConfig.token
            logger.info("Using pre-configured token for device ${deviceConfig.name}")
            promise.complete()
            return promise.future()
        }

        // If no username/password, skip authentication (anonymous access)
        if (winCCOaConfig.username == null || winCCOaConfig.password == null) {
            authToken = null
            logger.info("No authentication credentials provided - using anonymous access for device ${deviceConfig.name}")
            promise.complete()
            return promise.future()
        }

        // Otherwise, perform login mutation
        logger.info("Authenticating with WinCC OA server for device ${deviceConfig.name}")

        val loginMutation = """
            mutation {
                login(username: "${winCCOaConfig.username}", password: "${winCCOaConfig.password}") {
                    token
                    expiresAt
                }
            }
        """.trimIndent()

        val requestBody = JsonObject()
            .put("query", loginMutation)

        webClient.postAbs(winCCOaConfig.graphqlEndpoint)
            .putHeader("Content-Type", "application/json")
            .sendJsonObject(requestBody)
            .onSuccess { response ->
                if (response.statusCode() == 200) {
                    val body = response.bodyAsJsonObject()
                    val data = body.getJsonObject("data")
                    val errors = body.getJsonArray("errors")

                    if (errors != null && errors.size() > 0) {
                        val errorMsg = "Login failed: ${errors.encode()}"
                        logger.severe(errorMsg)
                        promise.fail(errorMsg)
                    } else if (data != null) {
                        val loginData = data.getJsonObject("login")
                        authToken = loginData.getString("token")
                        val expiresAt = loginData.getString("expiresAt")
                        logger.info("Successfully authenticated with WinCC OA server. Token expires at: $expiresAt")
                        promise.complete()
                    } else {
                        promise.fail("Invalid login response")
                    }
                } else {
                    promise.fail("Login request failed with status ${response.statusCode()}")
                }
            }
            .onFailure { error ->
                logger.severe("Authentication failed for device ${deviceConfig.name}: ${error.message}")
                promise.fail(error)
            }

        return promise.future()
    }

    /**
     * Connect to WinCC OA GraphQL WebSocket for subscriptions
     */
    private fun connectWebSocket(): Future<Void> {
        val promise = Promise.promise<Void>()

        if (isConnected || isReconnecting) {
            promise.complete()
            return promise.future()
        }

        if (wsClient == null) {
            promise.fail("WebSocket client not initialized")
            return promise.future()
        }

        isReconnecting = true

        val wsEndpoint = winCCOaConfig.getWebSocketEndpoint()
        logger.info("Connecting to WinCC OA WebSocket: $wsEndpoint (authenticated: ${authToken != null})")

        // Parse WebSocket URL
        val uri = java.net.URI(wsEndpoint)
        val host = uri.host
        val port = if (uri.port > 0) uri.port else if (uri.scheme == "wss") 443 else 80
        val path = if (uri.path.isNullOrEmpty()) "/" else uri.path
        val ssl = uri.scheme == "wss"

        // Create WebSocket connection using Vert.x 5 API
        // Use graphql-transport-ws subprotocol (modern GraphQL over WebSocket protocol)
        val connectOptions = WebSocketConnectOptions()
            .setHost(host)
            .setPort(port)
            .setURI(path)
            .setSsl(ssl)
            .addSubProtocol("graphql-transport-ws")

        wsClient!!.connect(connectOptions)
            .onSuccess { ws: WebSocket ->
                webSocket = ws
                isConnected = true
                isReconnecting = false

                val subProtocol = ws.subProtocol()
                logger.info("Connected to WinCC OA WebSocket for device ${deviceConfig.name}, negotiated subprotocol: $subProtocol")

                // Setup message handler FIRST before sending any messages
                ws.textMessageHandler { message: String ->
                    handleWebSocketMessage(message)
                }

                // Setup binary message handler for debugging
                ws.binaryMessageHandler { buffer ->
                    logger.fine("Received binary WebSocket message for device ${deviceConfig.name}: ${buffer.toString()}")
                }

                // Setup close handler
                ws.closeHandler { _: Void? ->
                    logger.warning("WebSocket connection closed for device ${deviceConfig.name}")
                    isConnected = false
                    webSocket = null
                    scheduleReconnection()
                }

                // Setup exception handler
                ws.exceptionHandler { error: Throwable ->
                    logger.severe("WebSocket error for device ${deviceConfig.name}: ${error.message}")
                    error.printStackTrace()
                    isConnected = false
                    webSocket = null
                    scheduleReconnection()
                }

                // NOW initialize GraphQL subscription protocol (after handlers are set up)
                initializeGraphQLWebSocket(ws)

                promise.complete()
            }
            .onFailure { error: Throwable ->
                isReconnecting = false
                logger.severe("Failed to connect to WinCC OA WebSocket: ${error.message}")
                scheduleReconnection()
                promise.fail(error)
            }

        return promise.future()
    }

    /**
     * Initialize GraphQL WebSocket protocol (graphql-ws)
     * Sends connection_init message with authentication (if configured)
     */
    private fun initializeGraphQLWebSocket(ws: WebSocket) {
        val payload = JsonObject()

        // Only add Authorization header if we have a token
        if (authToken != null) {
            payload.put("Authorization", "Bearer $authToken")
        }

        val initMessage = JsonObject()
            .put("type", "connection_init")
            .put("payload", payload)

        val messageStr = initMessage.encode()
        logger.fine("Sending connection_init message for device ${deviceConfig.name}: $messageStr")
        ws.writeTextMessage(messageStr)
    }

    /**
     * Handle incoming WebSocket messages
     */
    private fun handleWebSocketMessage(message: String) {
        try {
            logger.fine("Received WebSocket message for device ${deviceConfig.name}: $message")
            val json = JsonObject(message)
            val type = json.getString("type")

            when (type) {
                "connection_ack" -> {
                    logger.info("WebSocket connection acknowledged for device ${deviceConfig.name}")
                }
                "next", "data" -> {
                    // Handle both graphql-ws "next" and subscriptions-transport-ws "data"
                    handleSubscriptionData(json)
                }
                "error" -> {
                    val payload = json.getValue("payload")
                    logger.severe("GraphQL subscription error for device ${deviceConfig.name}: $payload")
                }
                "complete" -> {
                    val id = json.getString("id")
                    logger.info("Subscription $id completed for device ${deviceConfig.name}")
                    activeSubscriptions.remove(id)
                }
                "connection_error" -> {
                    val payload = json.getValue("payload")
                    logger.severe("WebSocket connection error for device ${deviceConfig.name}: $payload")
                }
                "ka" -> {
                    // Keep-alive message (subscriptions-transport-ws)
                    logger.fine("Received keep-alive message for device ${deviceConfig.name}")
                }
                "ping" -> {
                    // Ping message (graphql-ws) - should respond with pong
                    val pongMessage = JsonObject().put("type", "pong")
                    webSocket?.writeTextMessage(pongMessage.encode())
                    logger.fine("Received ping, sent pong for device ${deviceConfig.name}")
                }
                else -> {
                    logger.warning("Received unknown WebSocket message type '$type' for device ${deviceConfig.name}: $message")
                }
            }
        } catch (e: Exception) {
            logger.severe("Error handling WebSocket message for device ${deviceConfig.name}: ${e.message} - Message: $message")
            e.printStackTrace()
        }
    }

    /**
     * Handle subscription data from dpQueryConnectSingle
     */
    private fun handleSubscriptionData(message: JsonObject) {
        try {
            val id = message.getString("id")
            val payload = message.getJsonObject("payload")

            if (payload == null) {
                logger.warning("No payload in subscription message. Full message: ${message.encode()}")
                return
            }

            val data = payload.getJsonObject("data")
            if (data == null) {
                logger.warning("No data field in payload. Full message: ${message.encode()}")
                return
            }

            val dpQueryData = data.getJsonObject("dpQueryConnectSingle")
            if (dpQueryData == null) {
                logger.warning("No dpQueryConnectSingle data in subscription response. Full message: ${message.encode()}")
                return
            }

            val values = dpQueryData.getJsonArray("values")
            val error = dpQueryData.getValue("error")

            if (error != null) {
                logger.warning("Subscription error: $error")
                return
            }

            if (values == null || values.size() < 2) {
                return // No data or only header row
            }

            val address = activeSubscriptions[id]
            if (address == null) {
                logger.warning("Received data for unknown subscription: $id")
                return
            }

            // Parse values array
            // First row is header: ["", ":_original.._value", ":_original.._stime"]
            // Subsequent rows are data: ["System1:Test_000001.1", 1, "2025-10-06T18:03:05.984Z"]
            val header = values.getJsonArray(0)

            for (i in 1 until values.size()) {
                val row = values.getJsonArray(i)
                if (row.size() >= 3) {
                    val dpName = row.getString(0)
                    val value = row.getValue(1)
                    val stime = row.getString(2)

                    publishValue(address, dpName, value, stime)
                }
            }

        } catch (e: Exception) {
            logger.severe("Error handling subscription data: ${e.message}. Full message: ${message.encode()}")
            e.printStackTrace()
        }
    }

    /**
     * Publish value to MQTT broker with topic transformation
     */
    private fun publishValue(address: WinCCOaAddress, dpName: String, value: Any?, stime: String) {
        try {
            // Get or compute MQTT topic from cache
            val mqttTopic = topicCache.computeIfAbsent(dpName) { name ->
                val transformed = winCCOaConfig.transformConfig.transformDpNameToTopic(name)
                "${deviceConfig.namespace}/${address.topic}/$transformed"
            }

            // Format message based on configuration
            val payload = formatMessage(value, stime)

            // Publish to MQTT broker
            val mqttMessage = BrokerMessage(
                messageId = 0,
                topicName = mqttTopic,
                payload = payload,
                qosLevel = 0,
                isRetain = address.retained,
                isDup = false,
                isQueued = false,
                clientId = "winccoa-connector-${deviceConfig.name}"
            )

            vertx.eventBus().publish(WinCCOaExtension.ADDRESS_WINCCOA_VALUE_PUBLISH, mqttMessage)

            messagesInCounter.incrementAndGet()
            logger.fine("Published WinCC OA value: $mqttTopic = $value")

        } catch (e: Exception) {
            logger.severe("Error publishing value for $dpName: ${e.message}")
        }
    }

    /**
     * Format message according to configured format
     */
    private fun formatMessage(value: Any?, stime: String): ByteArray {
        return when (winCCOaConfig.messageFormat) {
            WinCCOaConnectionConfig.FORMAT_JSON_ISO -> {
                val json = JsonObject()
                    .put("value", value)
                    .put("time", stime)
                json.encode().toByteArray()
            }
            WinCCOaConnectionConfig.FORMAT_JSON_MS -> {
                val timestamp = try {
                    Instant.parse(stime).toEpochMilli()
                } catch (e: Exception) {
                    System.currentTimeMillis()
                }
                val json = JsonObject()
                    .put("value", value)
                    .put("time", timestamp)
                json.encode().toByteArray()
            }
            WinCCOaConnectionConfig.FORMAT_RAW_VALUE -> {
                // Handle BLOB type (Buffer data)
                if (value is JsonObject && value.getString("type") == "Buffer") {
                    val dataArray = value.getJsonArray("data")
                    val bytes = ByteArray(dataArray.size())
                    for (i in 0 until dataArray.size()) {
                        bytes[i] = dataArray.getInteger(i).toByte()
                    }
                    bytes
                } else {
                    // Plain value as string
                    value.toString().toByteArray()
                }
            }
            else -> {
                // Default to JSON_ISO
                val json = JsonObject()
                    .put("value", value)
                    .put("time", stime)
                json.encode().toByteArray()
            }
        }
    }

    /**
     * Setup all configured subscriptions
     */
    private fun setupSubscriptions(): Future<Void> {
        val promise = Promise.promise<Void>()

        if (webSocket == null || !isConnected) {
            promise.fail("WebSocket not connected")
            return promise.future()
        }

        val addresses = winCCOaConfig.addresses
        if (addresses.isEmpty()) {
            logger.info("No addresses configured for device ${deviceConfig.name}")
            promise.complete()
            return promise.future()
        }

        logger.info("Setting up ${addresses.size} subscriptions for device ${deviceConfig.name}")

        addresses.forEach { address ->
            subscribeToAddress(address)
        }

        promise.complete()
        return promise.future()
    }

    /**
     * Subscribe to a specific address (GraphQL query)
     */
    private fun subscribeToAddress(address: WinCCOaAddress) {
        if (webSocket == null || !isConnected) {
            logger.warning("Cannot subscribe - WebSocket not connected")
            return
        }

        val subscriptionId = "${deviceConfig.name}-sub-${nextSubscriptionId++}"

        val subscription = """
            subscription {
                dpQueryConnectSingle(query: "${address.query.replace("\"", "\\\"")}", answer: ${address.answer}) {
                    values
                    type
                    error
                }
            }
        """.trimIndent()

        val subscribeMessage = JsonObject()
            .put("id", subscriptionId)
            .put("type", "subscribe")
            .put("payload", JsonObject()
                .put("query", subscription)
            )

        val messageStr = subscribeMessage.encode()
        logger.fine("Sending subscribe message for device ${deviceConfig.name}: $messageStr")
        webSocket?.writeTextMessage(messageStr)
        activeSubscriptions[subscriptionId] = address

        logger.info("Subscribed to WinCC OA query: ${address.query} (ID: $subscriptionId)")
    }

    /**
     * Disconnect from WebSocket
     */
    private fun disconnectWebSocket(): Future<Void> {
        val promise = Promise.promise<Void>()

        // Cancel any pending reconnection timer
        reconnectTimerId?.let { timerId ->
            vertx.cancelTimer(timerId)
            reconnectTimerId = null
        }

        if (webSocket != null) {
            webSocket?.close()
            webSocket = null
        }

        isConnected = false
        isReconnecting = false
        activeSubscriptions.clear()
        nextSubscriptionId = 1

        promise.complete()
        return promise.future()
    }

    /**
     * Schedule reconnection attempt
     */
    private fun scheduleReconnection() {
        if (!isReconnecting) {
            // Cancel any existing reconnection timer
            reconnectTimerId?.let { timerId ->
                vertx.cancelTimer(timerId)
            }

            // Schedule new reconnection attempt
            reconnectTimerId = vertx.setTimer(winCCOaConfig.reconnectDelay) {
                reconnectTimerId = null
                if (!isConnected) {
                    logger.info("Attempting to reconnect to WinCC OA server for device ${deviceConfig.name}...")
                    authenticate()
                        .compose { connectWebSocket() }
                        .compose { setupSubscriptions() }
                        .onComplete { result ->
                            if (result.failed()) {
                                logger.warning("Reconnection failed for device ${deviceConfig.name}: ${result.cause()?.message}")
                            }
                        }
                }
            }
            logger.info("Scheduled reconnection for device ${deviceConfig.name} in ${winCCOaConfig.reconnectDelay}ms")
        }
    }
}
