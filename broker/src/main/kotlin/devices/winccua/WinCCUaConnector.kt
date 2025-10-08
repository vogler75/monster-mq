package at.rocworks.devices.winccua

import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.devices.WinCCUaAddress
import at.rocworks.stores.devices.WinCCUaAddressType
import at.rocworks.stores.devices.WinCCUaConnectionConfig
import at.rocworks.stores.devices.WinCCUaTransformConfig
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
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
 * WinCC Unified Connector - Handles connection to WinCC Unified GraphQL server
 *
 * Responsibilities:
 * - Maintains WebSocket connection to WinCC Unified GraphQL server
 * - Handles authentication (login mutation with username/password to get bearer token)
 * - Manages GraphQL subscriptions (tagValues and activeAlarms)
 * - For tagValues: uses browse query to get tag list before subscribing
 * - Transforms tag names to MQTT topics with caching
 * - Publishes WinCC Unified value changes as MQTT messages
 * - Handles reconnection and error recovery
 */
class WinCCUaConnector : AbstractVerticle() {

    // Metrics counters
    private val messagesInCounter = AtomicLong(0)
    private var lastMetricsReset = System.currentTimeMillis()

    private val logger: Logger = Utils.getLogger(this::class.java)

    // Device configuration
    private lateinit var deviceConfig: DeviceConfig
    private lateinit var winCCUaConfig: WinCCUaConnectionConfig

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
    private var isStopping = false

    // Subscription tracking
    private val activeSubscriptions = ConcurrentHashMap<String, WinCCUaAddress>() // subscriptionId -> address
    private var nextSubscriptionId = 1

    // Tag list cache (per address)
    private val tagListCache = ConcurrentHashMap<WinCCUaAddress, List<String>>() // address -> tag list

    // Topic transformation cache (tagName -> mqttTopic)
    private val topicCache = ConcurrentHashMap<String, String>()

    override fun start(startPromise: Promise<Void>) {
        try {
            // Load device configuration
            val deviceJson = config().getJsonObject("device")
            deviceConfig = DeviceConfig.fromJsonObject(deviceJson)
            winCCUaConfig = WinCCUaConnectionConfig.fromJsonObject(deviceConfig.config)

            logger.info("Starting WinCCUaConnector for device: ${deviceConfig.name}")

            // Validate configuration
            val validationErrors = winCCUaConfig.validate()
            if (validationErrors.isNotEmpty()) {
                val errorMsg = "Configuration validation failed: ${validationErrors.joinToString(", ")}"
                logger.severe(errorMsg)
                startPromise.fail(errorMsg)
                return
            }

            // Initialize HTTP client for REST API calls
            val webClientOptions = WebClientOptions()
                .setConnectTimeout(winCCUaConfig.connectionTimeout.toInt())

            webClient = WebClient.create(vertx, webClientOptions)

            // Initialize WebSocket client
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
                        logger.info("Initial WinCC Unified connection successful for device ${deviceConfig.name}")
                    } else {
                        logger.warning("Initial WinCC Unified connection failed for device ${deviceConfig.name}: ${result.cause()?.message}. Will retry automatically.")
                    }
                }

        } catch (e: Exception) {
            logger.severe("Exception during WinCCUaConnector startup: ${e.message}")
            e.printStackTrace()
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Stopping WinCCUaConnector for device: ${deviceConfig.name}")

        // Set stopping flag to prevent reconnection attempts
        isStopping = true

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
        val addr = at.rocworks.bus.EventBusAddresses.WinCCUaBridge.connectorMetrics(deviceConfig.name)
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
                    .put("connected", isConnected)
                    .put("elapsedMs", elapsedMs)
                msg.reply(json)
            } catch (e: Exception) {
                msg.fail(500, e.message)
            }
        }
        logger.info("Registered WinCC Unified metrics endpoint for device ${deviceConfig.name} at address $addr")
    }

    /**
     * Authenticate with WinCC Unified GraphQL server
     * Performs login mutation with username and password to get bearer token
     */
    private fun authenticate(): Future<Void> {
        val promise = Promise.promise<Void>()

        // Don't authenticate if stopping
        if (isStopping) {
            promise.fail("Connector is stopping")
            return promise.future()
        }

        logger.info("Authenticating with WinCC Unified server for device ${deviceConfig.name}")
        logger.info("GraphQL endpoint: ${winCCUaConfig.graphqlEndpoint}")

        val loginMutation = """
            mutation {
                login(username: "${winCCUaConfig.username}", password: "${winCCUaConfig.password}") {
                    token
                    expires
                }
            }
        """.trimIndent()

        logger.fine("Login mutation: $loginMutation")

        val requestBody = JsonObject()
            .put("query", loginMutation)

        logger.fine("Request body: ${requestBody.encode()}")

        webClient.postAbs(winCCUaConfig.graphqlEndpoint)
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
                        val expires = loginData.getString("expires")
                        logger.info("Successfully authenticated with WinCC Unified server. Token expires at: $expires")
                        promise.complete()
                    } else {
                        promise.fail("Invalid login response")
                    }
                } else {
                    val responseBody = response.bodyAsString() ?: "(empty body)"
                    val errorMsg = "Login request failed with status ${response.statusCode()}. Response: $responseBody"
                    logger.warning(errorMsg)
                    promise.fail(errorMsg)
                }
            }
            .onFailure { error ->
                logger.severe("Authentication failed for device ${deviceConfig.name}: ${error.message}")
                promise.fail(error)
            }

        return promise.future()
    }

    /**
     * Connect to WinCC Unified GraphQL WebSocket for subscriptions
     */
    private fun connectWebSocket(): Future<Void> {
        val promise = Promise.promise<Void>()

        // Don't connect if stopping
        if (isStopping) {
            promise.fail("Connector is stopping")
            return promise.future()
        }

        if (isConnected || isReconnecting) {
            promise.complete()
            return promise.future()
        }

        if (wsClient == null) {
            promise.fail("WebSocket client not initialized")
            return promise.future()
        }

        if (authToken == null) {
            promise.fail("No authentication token available")
            return promise.future()
        }

        isReconnecting = true

        val wsEndpoint = winCCUaConfig.getWebSocketEndpoint()
        logger.info("Connecting to WinCC Unified WebSocket: $wsEndpoint")

        // Parse WebSocket URL
        val uri = java.net.URI(wsEndpoint)
        val host = uri.host
        val port = if (uri.port > 0) uri.port else if (uri.scheme == "wss") 443 else 80
        val path = if (uri.path.isNullOrEmpty()) "/" else uri.path
        val ssl = uri.scheme == "wss"

        // Create WebSocket connection using graphql-transport-ws subprotocol
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
                logger.info("Connected to WinCC Unified WebSocket for device ${deviceConfig.name}, negotiated subprotocol: $subProtocol")

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

                // Initialize GraphQL subscription protocol with authentication
                initializeGraphQLWebSocket(ws)

                promise.complete()
            }
            .onFailure { error: Throwable ->
                isReconnecting = false
                logger.severe("Failed to connect to WinCC Unified WebSocket: ${error.message}")
                scheduleReconnection()
                promise.fail(error)
            }

        return promise.future()
    }

    /**
     * Initialize GraphQL WebSocket protocol (graphql-transport-ws)
     * Sends connection_init message with Authorization Bearer token
     */
    private fun initializeGraphQLWebSocket(ws: WebSocket) {
        val payload = JsonObject()
            .put("Authorization", "Bearer $authToken")

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
                    // Keep-alive message
                    logger.fine("Received keep-alive message for device ${deviceConfig.name}")
                }
                "ping" -> {
                    // Ping message - should respond with pong
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
     * Handle subscription data from tagValues or activeAlarms
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

            val address = activeSubscriptions[id]
            if (address == null) {
                logger.warning("Received data for unknown subscription: $id")
                return
            }

            // Handle based on address type
            when (address.type) {
                WinCCUaAddressType.TAG_VALUES -> handleTagValuesData(data, address)
                WinCCUaAddressType.ACTIVE_ALARMS -> handleActiveAlarmsData(data, address)
            }

        } catch (e: Exception) {
            logger.severe("Error handling subscription data: ${e.message}. Full message: ${message.encode()}")
            e.printStackTrace()
        }
    }

    /**
     * Handle tagValues subscription data
     */
    private fun handleTagValuesData(data: JsonObject, address: WinCCUaAddress) {
        val tagValuesData = data.getJsonObject("tagValues")
        if (tagValuesData == null) {
            logger.warning("No tagValues data in subscription response")
            return
        }

        // Parse TagValueNotification - new structure
        val tagName = tagValuesData.getString("name")
        val valueObj = tagValuesData.getJsonObject("value")
        val errorObj = tagValuesData.getJsonObject("error")

        // Check for actual errors (error code != 0 means error)
        if (errorObj != null) {
            val errorCode = errorObj.getString("code")
            val errorDesc = errorObj.getString("description")
            // Error code "0" means success, not an error
            if (errorCode != "0") {
                logger.warning("Error in tagValues for tag $tagName: $errorCode - $errorDesc")
                return
            }
        }

        // Extract value and timestamp from value object
        val value = valueObj?.getValue("value")
        val timestamp = valueObj?.getString("timestamp")

        if (tagName != null && value != null) {
            publishTagValue(address, tagName, value, timestamp)
        }
    }

    /**
     * Handle activeAlarms subscription data
     */
    private fun handleActiveAlarmsData(data: JsonObject, address: WinCCUaAddress) {
        val activeAlarmsData = data.getJsonObject("activeAlarms")
        if (activeAlarmsData == null) {
            logger.warning("No activeAlarms data in subscription response")
            return
        }

        // Publish the complete alarm notification with all fields
        val alarmName = activeAlarmsData.getString("name")
            ?: activeAlarmsData.getString("path")

        if (alarmName != null) {
            publishAlarm(address, activeAlarmsData)
        } else {
            logger.warning("Alarm notification has no name or path, skipping")
        }
    }

    /**
     * Publish tag value to MQTT broker with topic transformation
     */
    private fun publishTagValue(address: WinCCUaAddress, tagName: String, value: Any?, timestamp: String?) {
        try {
            // Get or compute MQTT topic from cache
            val mqttTopic = topicCache.computeIfAbsent(tagName) { name ->
                val transformed = winCCUaConfig.transformConfig.transformTagNameToTopic(name)
                "${deviceConfig.namespace}/${address.topic}/$transformed"
            }

            // Format message based on configuration
            val payload = formatTagMessage(value, timestamp)

            // Publish to MQTT broker
            val mqttMessage = BrokerMessage(
                messageId = 0,
                topicName = mqttTopic,
                payload = payload,
                qosLevel = 0,
                isRetain = address.retained,
                isDup = false,
                isQueued = false,
                clientId = "winccua-connector-${deviceConfig.name}"
            )

            vertx.eventBus().publish(WinCCUaExtension.ADDRESS_WINCCUA_VALUE_PUBLISH, mqttMessage)

            messagesInCounter.incrementAndGet()
            logger.fine("Published WinCC Unified tag value: $mqttTopic = $value")

        } catch (e: Exception) {
            logger.severe("Error publishing tag value for $tagName: ${e.message}")
        }
    }

    /**
     * Publish alarm to MQTT broker
     */
    private fun publishAlarm(address: WinCCUaAddress, alarmData: JsonObject) {
        try {
            // Use alarm name for topic (canonical name of the configured alarm)
            // If name not available, use path (full hierarchical name) or fall back to "unknown"
            val alarmName = alarmData.getString("name")
                ?: alarmData.getString("path")
                ?: "unknown"

            val mqttTopic = "${deviceConfig.namespace}/${address.topic}/$alarmName"

            // Publish alarm as JSON
            val payload = alarmData.encode().toByteArray()

            val mqttMessage = BrokerMessage(
                messageId = 0,
                topicName = mqttTopic,
                payload = payload,
                qosLevel = 0,
                isRetain = address.retained,
                isDup = false,
                isQueued = false,
                clientId = "winccua-connector-${deviceConfig.name}"
            )

            vertx.eventBus().publish(WinCCUaExtension.ADDRESS_WINCCUA_VALUE_PUBLISH, mqttMessage)

            messagesInCounter.incrementAndGet()
            logger.fine("Published WinCC Unified alarm: $mqttTopic")

        } catch (e: Exception) {
            logger.severe("Error publishing alarm: ${e.message}")
        }
    }

    /**
     * Format tag message according to configured format
     */
    private fun formatTagMessage(value: Any?, timestamp: String?): ByteArray {
        return when (winCCUaConfig.messageFormat) {
            WinCCUaConnectionConfig.FORMAT_JSON_ISO -> {
                val json = JsonObject()
                    .put("value", value)
                if (timestamp != null) json.put("time", timestamp)
                json.encode().toByteArray()
            }
            WinCCUaConnectionConfig.FORMAT_JSON_MS -> {
                val timestampMs = try {
                    if (timestamp != null) Instant.parse(timestamp).toEpochMilli() else System.currentTimeMillis()
                } catch (e: Exception) {
                    System.currentTimeMillis()
                }
                val json = JsonObject()
                    .put("value", value)
                    .put("time", timestampMs)
                json.encode().toByteArray()
            }
            WinCCUaConnectionConfig.FORMAT_RAW_VALUE -> {
                // Just the plain value as string
                value.toString().toByteArray()
            }
            else -> {
                // Default to JSON_ISO
                val json = JsonObject()
                    .put("value", value)
                if (timestamp != null) json.put("time", timestamp)
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

        val addresses = winCCUaConfig.addresses
        if (addresses.isEmpty()) {
            logger.info("No addresses configured for device ${deviceConfig.name}")
            promise.complete()
            return promise.future()
        }

        logger.info("Setting up ${addresses.size} subscriptions for device ${deviceConfig.name}")

        // Setup each address (may need to browse first for TAG_VALUES)
        val setupFutures = addresses.map { address ->
            when (address.type) {
                WinCCUaAddressType.TAG_VALUES -> setupTagValuesSubscription(address)
                WinCCUaAddressType.ACTIVE_ALARMS -> setupActiveAlarmsSubscription(address)
            }
        }

        Future.all<Void>(setupFutures as List<Future<Void>>)
            .onComplete { result ->
                if (result.succeeded()) {
                    logger.info("All subscriptions set up successfully for device ${deviceConfig.name}")
                    promise.complete()
                } else {
                    logger.warning("Some subscriptions failed to set up: ${result.cause()?.message}")
                    promise.complete() // Continue anyway
                }
            }

        return promise.future()
    }

    /**
     * Setup tagValues subscription (with browse query first)
     */
    private fun setupTagValuesSubscription(address: WinCCUaAddress): Future<Void> {
        val promise = Promise.promise<Void>()

        // First, execute browse query to get tag list
        executeBrowseQuery(address)
            .onComplete { browseResult ->
                if (browseResult.succeeded()) {
                    val tagList = browseResult.result()
                    tagListCache[address] = tagList
                    logger.info("Browse query returned ${tagList.size} tags for address ${address.topic}")

                    // Now subscribe to tagValues with the tag list
                    subscribeToTagValues(address, tagList)
                    promise.complete()
                } else {
                    logger.severe("Failed to execute browse query: ${browseResult.cause()?.message}")
                    promise.fail(browseResult.cause())
                }
            }

        return promise.future()
    }

    /**
     * Execute browse query to get list of tags
     */
    private fun executeBrowseQuery(address: WinCCUaAddress): Future<List<String>> {
        val promise = Promise.promise<List<String>>()

        val nameFilters = address.nameFilters
        if (nameFilters == null || nameFilters.isEmpty()) {
            promise.fail("No name filters provided")
            return promise.future()
        }

        // Build nameFilters array for GraphQL query
        val nameFiltersArray = nameFilters.joinToString(", ") { "\"$it\"" }
        val nameFiltersGraphQL = "[$nameFiltersArray]"

        val browseQuery = """
            query {
                browse(nameFilters: $nameFiltersGraphQL) {
                    name
                }
            }
        """.trimIndent()

        val requestBody = JsonObject()
            .put("query", browseQuery)

        logger.info("Executing browse query for address ${address.topic}")
        logger.fine("Browse query: $browseQuery")
        logger.fine("Request body: ${requestBody.encode()}")

        webClient.postAbs(winCCUaConfig.graphqlEndpoint)
            .putHeader("Content-Type", "application/json")
            .putHeader("Authorization", "Bearer $authToken")
            .sendJsonObject(requestBody)
            .onSuccess { response ->
                if (response.statusCode() == 200) {
                    val body = response.bodyAsJsonObject()
                    val data = body.getJsonObject("data")
                    val errors = body.getJsonArray("errors")

                    if (errors != null && errors.size() > 0) {
                        val errorMsg = "Browse query failed: ${errors.encode()}"
                        logger.severe(errorMsg)
                        logger.severe("Query was: $browseQuery")
                        promise.fail(errorMsg)
                    } else if (data != null) {
                        val browseArray = data.getJsonArray("browse")
                        val tagList = browseArray?.map { it as JsonObject }?.map { it.getString("name") } ?: emptyList()
                        logger.fine("Browse query result: ${tagList.size} tags")
                        promise.complete(tagList)
                    } else {
                        promise.fail("Invalid browse query response")
                    }
                } else {
                    val responseBody = response.bodyAsString() ?: "(empty body)"
                    val errorMsg = "Browse query failed with status ${response.statusCode()}. Response: $responseBody"
                    logger.severe(errorMsg)
                    logger.severe("Query was: $browseQuery")
                    promise.fail(errorMsg)
                }
            }
            .onFailure { error ->
                logger.severe("Browse query failed: ${error.message}")
                promise.fail(error)
            }

        return promise.future()
    }

    /**
     * Subscribe to tagValues with list of tags
     */
    private fun subscribeToTagValues(address: WinCCUaAddress, tags: List<String>) {
        if (webSocket == null || !isConnected) {
            logger.warning("Cannot subscribe - WebSocket not connected")
            return
        }

        val subscriptionId = "${deviceConfig.name}-tagvalues-${nextSubscriptionId++}"

        // Build tags array for GraphQL
        val tagsJsonArray = JsonArray(tags)

        val subscription = """
            subscription {
                tagValues(names: ${tagsJsonArray.encode()}) {
                    name
                    value {
                        value
                        timestamp
                    }
                    error {
                        code
                        description
                    }
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
        logger.fine("Sending tagValues subscribe message for device ${deviceConfig.name}: $messageStr")
        webSocket?.writeTextMessage(messageStr)
        activeSubscriptions[subscriptionId] = address

        logger.info("Subscribed to ${tags.size} tags for address ${address.topic} (ID: $subscriptionId)")
    }

    /**
     * Setup activeAlarms subscription
     */
    private fun setupActiveAlarmsSubscription(address: WinCCUaAddress): Future<Void> {
        if (webSocket == null || !isConnected) {
            return Future.failedFuture("WebSocket not connected")
        }

        val subscriptionId = "${deviceConfig.name}-alarms-${nextSubscriptionId++}"

        // Build subscription with optional filters
        val filtersBuilder = StringBuilder()
        val filters = mutableListOf<String>()

        if (address.systemNames != null && address.systemNames.isNotEmpty()) {
            val systemNamesArray = JsonArray(address.systemNames)
            filters.add("systemNames: ${systemNamesArray.encode()}")
        }

        if (address.filterString != null) {
            filters.add("filterString: \"${address.filterString}\"")
        }

        val filtersStr = if (filters.isNotEmpty()) "(${filters.joinToString(", ")})" else ""

        val subscription = """
            subscription {
                activeAlarms$filtersStr {
                    name
                    instanceID
                    alarmGroupID
                    raiseTime
                    acknowledgmentTime
                    clearTime
                    resetTime
                    modificationTime
                    state
                    textColor
                    backColor
                    flashing
                    languages
                    alarmClassName
                    alarmClassSymbol
                    alarmClassID
                    stateMachine
                    priority
                    alarmParameterValues
                    alarmType
                    eventText
                    infoText
                    alarmText1
                    alarmText2
                    alarmText3
                    alarmText4
                    alarmText5
                    alarmText6
                    alarmText7
                    alarmText8
                    alarmText9
                    stateText
                    origin
                    area
                    changeReason
                    connectionName
                    valueLimit
                    sourceType
                    suppressionState
                    hostName
                    userName
                    value
                    valueQuality {
                        quality
                        subStatus
                        limit
                        extendedSubStatus
                        sourceQuality
                        sourceTime
                        timeCorrected
                    }
                    quality {
                        quality
                        subStatus
                        limit
                        extendedSubStatus
                        sourceQuality
                        sourceTime
                        timeCorrected
                    }
                    invalidFlags {
                        invalidConfiguration
                        invalidTimestamp
                        invalidAlarmParameter
                        invalidEventText
                    }
                    deadBand
                    producer
                    duration
                    durationIso
                    sourceID
                    systemSeverity
                    loopInAlarm
                    loopInAlarmParameterValues
                    path
                    userResponse
                    notificationReason
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
        logger.fine("Sending activeAlarms subscribe message for device ${deviceConfig.name}: $messageStr")
        webSocket?.writeTextMessage(messageStr)
        activeSubscriptions[subscriptionId] = address

        logger.info("Subscribed to activeAlarms for address ${address.topic} (ID: $subscriptionId)")

        return Future.succeededFuture()
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
        // Don't schedule reconnection if stopping or already reconnecting
        if (isStopping || isReconnecting) {
            return
        }

        // Cancel any existing reconnection timer
        reconnectTimerId?.let { timerId ->
            vertx.cancelTimer(timerId)
        }

        // Schedule new reconnection attempt
        reconnectTimerId = vertx.setTimer(winCCUaConfig.reconnectDelay) {
            reconnectTimerId = null
            // Check again if we're stopping before attempting reconnection
            if (!isConnected && !isStopping) {
                logger.info("Attempting to reconnect to WinCC Unified server for device ${deviceConfig.name}...")
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
        logger.info("Scheduled reconnection for device ${deviceConfig.name} in ${winCCUaConfig.reconnectDelay}ms")
    }
}
