package at.rocworks.devices.i3xclient

import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.devices.I3xAddress
import at.rocworks.stores.devices.I3xConnectionConfig
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.http.HttpClient
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.http.HttpClientResponse
import io.vertx.core.http.HttpMethod
import io.vertx.core.http.RequestOptions
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.HttpRequest
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import java.net.URI
import java.util.Base64
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger

/**
 * i3X Client Connector Verticle.
 *
 * Responsibilities:
 * - Establishes HTTP/SSE connection to a remote or local i3X v1 server.
 * - Handles authentication (Basic Auth, Bearer Token, Custom Headers).
 * - Creates subscription via `POST /subscriptions`.
 * - Registers object element IDs via `POST /subscriptions/register`.
 * - Streams live updates via Server-Sent Events (`GET /subscriptions/stream`).
 * - Maps incoming object updates to MQTT topics and formats payloads.
 * - Publishes updates to the broker's message bus.
 * - Handles reconnections with exponential backoff and error recovery.
 * - Exposes throughput metrics and connection status via EventBus.
 */
class I3xClientConnector : AbstractVerticle() {

    private val logger: Logger = Utils.getLogger(this::class.java)

    // Device configuration
    private lateinit var deviceConfig: DeviceConfig
    private lateinit var i3xConfig: I3xConnectionConfig

    // Clients
    private lateinit var webClient: WebClient
    private var httpClient: HttpClient? = null
    private var activeStreamResponse: HttpClientResponse? = null

    // State
    private var currentSubscriptionId: String? = null
    private var isConnected = false
    private var isConnecting = false
    private var isStopping = false
    private var reconnectTimerId: Long? = null

    // Metrics
    private val messagesInCounter = AtomicLong(0)
    private var lastMetricsReset = System.currentTimeMillis()

    override fun start(startPromise: Promise<Void>) {
        try {
            val deviceJson = config().getJsonObject("device")
            deviceConfig = DeviceConfig.fromJsonObject(deviceJson)
            i3xConfig = I3xConnectionConfig.fromJsonObject(deviceConfig.config)

            logger.info("Starting I3xClientConnector for device: ${deviceConfig.name}")

            val validationErrors = i3xConfig.validate()
            if (validationErrors.isNotEmpty()) {
                val msg = "Validation failed for i3X Client '${deviceConfig.name}': ${validationErrors.joinToString(", ")}"
                logger.severe(msg)
                startPromise.fail(msg)
                return
            }

            val webClientOptions = WebClientOptions()
                .setConnectTimeout(i3xConfig.connectionTimeout.toInt())
                .setTrustAll(true)
            webClient = WebClient.create(vertx, webClientOptions)

            setupMetricsEndpoint()
            startPromise.complete()

            // Initiate connection in the background
            connect()

        } catch (e: Exception) {
            logger.severe("Exception during I3xClientConnector startup: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Stopping I3xClientConnector for device: ${deviceConfig.name}")
        isStopping = true

        reconnectTimerId?.let {
            vertx.cancelTimer(it)
            reconnectTimerId = null
        }

        disconnectCurrentStream()
        webClient.close()
        httpClient?.close()
        stopPromise.complete()
    }

    private fun setupMetricsEndpoint() {
        val addr = EventBusAddresses.I3xBridge.connectorMetrics(deviceConfig.name)
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
    }

    // -------------------------------------------------------------------------
    //  Connection Lifecycle
    // -------------------------------------------------------------------------

    private fun connect() {
        if (isStopping || isConnected || isConnecting) return
        isConnecting = true

        logger.info("Connecting i3X Client '${deviceConfig.name}' to ${i3xConfig.url}...")

        createSubscription()
            .compose { subId ->
                currentSubscriptionId = subId
                registerAddresses(subId)
            }
            .compose {
                openSseStream(currentSubscriptionId!!)
            }
            .onSuccess {
                isConnecting = false
                isConnected = true
                logger.info("Successfully established i3X SSE stream for '${deviceConfig.name}' (subId=$currentSubscriptionId)")
            }
            .onFailure { err ->
                isConnecting = false
                isConnected = false
                logger.warning("i3X connection failed for '${deviceConfig.name}': ${err.message}. Retrying in ${i3xConfig.reconnectDelay}ms...")
                scheduleReconnect()
            }
    }

    private fun scheduleReconnect() {
        if (isStopping || reconnectTimerId != null) return
        disconnectCurrentStream()

        reconnectTimerId = vertx.setTimer(i3xConfig.reconnectDelay) {
            reconnectTimerId = null
            if (!isStopping) {
                connect()
            }
        }
    }

    private fun disconnectCurrentStream() {
        isConnected = false
        try {
            activeStreamResponse = null
            httpClient?.close()
            httpClient = null
        } catch (_: Exception) {}
    }

    // -------------------------------------------------------------------------
    //  Auth & HTTP Helpers
    // -------------------------------------------------------------------------

    private fun applyAuth(req: HttpRequest<*>) {
        when (i3xConfig.authType) {
            I3xConnectionConfig.AUTH_TYPE_BASIC -> {
                if (!i3xConfig.username.isNullOrEmpty()) {
                    val credentials = "${i3xConfig.username}:${i3xConfig.password ?: ""}"
                    val encoded = Base64.getEncoder().encodeToString(credentials.toByteArray())
                    req.putHeader("Authorization", "Basic $encoded")
                }
            }
            I3xConnectionConfig.AUTH_TYPE_BEARER -> {
                if (!i3xConfig.token.isNullOrEmpty()) {
                    req.putHeader("Authorization", "Bearer ${i3xConfig.token}")
                }
            }
        }
        for (h in i3xConfig.headers) {
            if (h.key.isNotBlank()) {
                req.putHeader(h.key, h.value)
            }
        }
    }

    private fun applyAuthToRequestOptions(reqOptions: RequestOptions) {
        when (i3xConfig.authType) {
            I3xConnectionConfig.AUTH_TYPE_BASIC -> {
                if (!i3xConfig.username.isNullOrEmpty()) {
                    val credentials = "${i3xConfig.username}:${i3xConfig.password ?: ""}"
                    val encoded = Base64.getEncoder().encodeToString(credentials.toByteArray())
                    reqOptions.addHeader("Authorization", "Basic $encoded")
                }
            }
            I3xConnectionConfig.AUTH_TYPE_BEARER -> {
                if (!i3xConfig.token.isNullOrEmpty()) {
                    reqOptions.addHeader("Authorization", "Bearer ${i3xConfig.token}")
                }
            }
        }
        for (h in i3xConfig.headers) {
            if (h.key.isNotBlank()) {
                reqOptions.addHeader(h.key, h.value)
            }
        }
    }

    // -------------------------------------------------------------------------
    //  Subscription Protocol: Create, Register, Stream
    // -------------------------------------------------------------------------

    private fun createSubscription(): Future<String> {
        val promise = Promise.promise<String>()
        val endpoint = "${i3xConfig.normalizedBaseUrl()}/subscriptions"

        val body = JsonObject()
            .put("clientId", i3xConfig.clientId)
            .put("displayName", deviceConfig.name)

        val req = webClient.postAbs(endpoint)
            .putHeader("Content-Type", "application/json")
        applyAuth(req)

        req.sendJsonObject(body)
            .onSuccess { resp ->
                if (resp.statusCode() == 200) {
                    val json = resp.bodyAsJsonObject()
                    val result = json.getJsonObject("result")
                    val subId = result?.getString("subscriptionId")
                    if (!subId.isNullOrBlank()) {
                        logger.fine("Created i3X subscription: $subId")
                        promise.complete(subId)
                    } else {
                        promise.fail("Response did not contain valid subscriptionId: ${resp.bodyAsString()}")
                    }
                } else {
                    promise.fail("POST $endpoint returned HTTP ${resp.statusCode()}: ${resp.bodyAsString()}")
                }
            }
            .onFailure { err ->
                promise.fail(err)
            }

        return promise.future()
    }

    private fun registerAddresses(subscriptionId: String): Future<Void> {
        val addresses = i3xConfig.addresses
        if (addresses.isEmpty()) {
            logger.info("No addresses configured for i3X device '${deviceConfig.name}'")
            return Future.succeededFuture()
        }

        // Group by maxDepth to send optimized bulk registrations
        val grouped = addresses.groupBy { it.maxDepth }
        val futures = grouped.map { (maxDepth, addrs) ->
            val elementIds = addrs.map { it.elementId.trim().trim('/') }.filter { it.isNotEmpty() }.distinct()
            if (elementIds.isEmpty()) return@map Future.succeededFuture<Void>()

            val promise = Promise.promise<Void>()
            val endpoint = "${i3xConfig.normalizedBaseUrl()}/subscriptions/register"
            val body = JsonObject()
                .put("clientId", i3xConfig.clientId)
                .put("subscriptionId", subscriptionId)
                .put("elementIds", JsonArray(elementIds))
                .put("maxDepth", maxDepth)

            val req = webClient.postAbs(endpoint).putHeader("Content-Type", "application/json")
            applyAuth(req)

            req.sendJsonObject(body)
                .onSuccess { resp ->
                    if (resp.statusCode() == 200) {
                        logger.fine("Registered ${elementIds.size} elements (maxDepth=$maxDepth) for subId=$subscriptionId")
                        promise.complete()
                    } else {
                        promise.fail("POST $endpoint returned HTTP ${resp.statusCode()}: ${resp.bodyAsString()}")
                    }
                }
                .onFailure { err ->
                    promise.fail(err)
                }

            promise.future()
        }

        return Future.all(futures).mapEmpty()
    }

    private fun openSseStream(subscriptionId: String): Future<Void> {
        val promise = Promise.promise<Void>()

        val uri = URI(i3xConfig.normalizedBaseUrl())
        val isSsl = uri.scheme.equals("https", ignoreCase = true)
        val host = uri.host ?: "localhost"
        val port = if (uri.port > 0) uri.port else (if (isSsl) 443 else 80)
        val basePath = uri.path.orEmpty().trimEnd('/')

        val clientOptions = HttpClientOptions()
            .setConnectTimeout(i3xConfig.connectionTimeout.toInt())
            .setSsl(isSsl)
            .setTrustAll(true)
            .setKeepAlive(true)

        val client = vertx.createHttpClient(clientOptions)
        httpClient = client

        // Primary method: POST /subscriptions/stream with JSON payload { "clientId": ..., "subscriptionId": ... }
        val streamPathPost = "$basePath/subscriptions/stream"
        val bodyJson = JsonObject()
            .put("clientId", i3xConfig.clientId)
            .put("subscriptionId", subscriptionId)
        val bodyBuffer = bodyJson.toBuffer()

        val reqOptionsPost = RequestOptions()
            .setMethod(HttpMethod.POST)
            .setHost(host)
            .setPort(port)
            .setURI(streamPathPost)
            .setSsl(isSsl)
            .addHeader("Content-Type", "application/json")
            .addHeader("Accept", "text/event-stream")
            .addHeader("Cache-Control", "no-cache")

        applyAuthToRequestOptions(reqOptionsPost)

        client.request(reqOptionsPost)
            .compose { req -> req.send(bodyBuffer) }
            .onSuccess { resp ->
                if (resp.statusCode() == 200) {
                    activeStreamResponse = resp
                    setupSseReader(resp)
                    promise.complete()
                } else if (resp.statusCode() == 405) {
                    logger.fine("POST /subscriptions/stream returned HTTP 405, attempting GET fallback")
                    openSseStreamGetFallback(subscriptionId, host, port, basePath, isSsl, promise)
                } else {
                    val msg = "SSE stream request failed with HTTP ${resp.statusCode()}: ${resp.statusMessage()}"
                    logger.warning(msg)
                    promise.fail(msg)
                }
            }
            .onFailure { err ->
                promise.fail(err)
            }

        return promise.future()
    }

    private fun openSseStreamGetFallback(
        subscriptionId: String,
        host: String,
        port: Int,
        basePath: String,
        isSsl: Boolean,
        promise: Promise<Void>
    ) {
        val client = httpClient ?: return promise.fail("HttpClient is null")
        val streamPathGet = "$basePath/subscriptions/stream?subscriptionId=$subscriptionId&clientId=${i3xConfig.clientId}"

        val reqOptionsGet = RequestOptions()
            .setMethod(HttpMethod.GET)
            .setHost(host)
            .setPort(port)
            .setURI(streamPathGet)
            .setSsl(isSsl)
            .addHeader("Accept", "text/event-stream")
            .addHeader("Cache-Control", "no-cache")

        applyAuthToRequestOptions(reqOptionsGet)

        client.request(reqOptionsGet)
            .compose { req -> req.send() }
            .onSuccess { resp ->
                if (resp.statusCode() == 200) {
                    activeStreamResponse = resp
                    setupSseReader(resp)
                    promise.complete()
                } else {
                    val msg = "SSE stream GET fallback failed with HTTP ${resp.statusCode()}: ${resp.statusMessage()}"
                    logger.warning(msg)
                    promise.fail(msg)
                }
            }
            .onFailure { err ->
                promise.fail(err)
            }
    }

    private fun setupSseReader(resp: HttpClientResponse) {
        val lineBuffer = StringBuilder()
        var currentEvent = "message"
        val dataBuffer = StringBuilder()

        resp.handler { chunk ->
            lineBuffer.append(chunk.toString(Charsets.UTF_8))
            while (true) {
                val newlineIdx = lineBuffer.indexOf("\n")
                if (newlineIdx < 0) break

                var line = lineBuffer.substring(0, newlineIdx)
                lineBuffer.delete(0, newlineIdx + 1)
                if (line.endsWith("\r")) line = line.substring(0, line.length - 1)

                if (line.isEmpty()) {
                    // End of SSE event block
                    if (dataBuffer.isNotEmpty()) {
                        handleSseEvent(currentEvent, dataBuffer.toString())
                        dataBuffer.setLength(0)
                        currentEvent = "message"
                    }
                } else if (line.startsWith(":")) {
                    // SSE comment / heartbeat
                    logger.finest("SSE comment from '${deviceConfig.name}': $line")
                } else if (line.startsWith("event:")) {
                    currentEvent = line.substring("event:".length).trim()
                } else if (line.startsWith("data:")) {
                    if (dataBuffer.isNotEmpty()) dataBuffer.append("\n")
                    dataBuffer.append(line.substring("data:".length).trim())
                }
            }
        }

        resp.endHandler {
            if (isConnected) {
                logger.warning("i3X SSE stream disconnected for device '${deviceConfig.name}'")
                isConnected = false
                scheduleReconnect()
            }
        }

        resp.exceptionHandler { err ->
            if (isConnected) {
                logger.warning("i3X SSE stream error for device '${deviceConfig.name}': ${err.message}")
                isConnected = false
                scheduleReconnect()
            }
        }
    }

    // -------------------------------------------------------------------------
    //  Event Processing & MQTT Publishing
    // -------------------------------------------------------------------------

    private fun handleSseEvent(event: String, data: String) {
        val trimmed = data.trim()
        if (trimmed.isEmpty()) return

        try {
            logger.fine("Received i3X event '$event': $trimmed")
            if (trimmed.startsWith("[")) {
                val array = JsonArray(trimmed)
                for (i in 0 until array.size()) {
                    val item = array.getValue(i)
                    if (item is JsonObject) {
                        processBatchOrUpdate(item)
                    }
                }
            } else if (trimmed.startsWith("{")) {
                val json = JsonObject(trimmed)
                processBatchOrUpdate(json)
            } else {
                logger.warning("Unrecognized SSE event payload format from '${deviceConfig.name}': $trimmed")
            }
        } catch (e: Exception) {
            logger.warning("Failed to parse i3X event data: ${e.message}")
        }
    }

    private fun processBatchOrUpdate(json: JsonObject) {
        val updatesArray = json.getJsonArray("updates")
        if (updatesArray != null) {
            for (i in 0 until updatesArray.size()) {
                val update = updatesArray.getJsonObject(i) ?: continue
                processUpdate(update)
            }
        } else if (json.containsKey("elementId")) {
            processUpdate(json)
        }
    }

    private fun processUpdate(update: JsonObject) {
        val elementId = update.getString("elementId") ?: return
        var value = update.getValue("value")
        var quality = update.getString("quality")
        var timestamp = update.getString("timestamp")

        if (value is JsonObject && (value.containsKey("value") || value.containsKey("quality") || value.containsKey("timestamp"))) {
            if (quality == null) quality = value.getString("quality")
            if (timestamp == null) timestamp = value.getString("timestamp")
            value = value.getValue("value")
        }
        if (quality == null) quality = "Good"

        val matchingAddress = findMatchingAddress(elementId)
        if (matchingAddress == null) {
            logger.fine("No address matching elementId '$elementId' in device '${deviceConfig.name}'")
            return
        }

        val topic = I3xPublisher.resolveTopic(deviceConfig.namespace, matchingAddress, elementId)
        val payload = I3xPublisher.formatPayload(value, quality, timestamp, matchingAddress.messageFormat)
        val brokerMessage = I3xPublisher.buildBrokerMessage(
            deviceName = deviceConfig.name,
            topic = topic,
            payload = payload,
            retained = matchingAddress.retained,
            qos = matchingAddress.qos
        )

        vertx.eventBus().publish(I3xClientExtension.ADDRESS_I3X_VALUE_PUBLISH, brokerMessage)
        messagesInCounter.incrementAndGet()
        logger.fine("Forwarded i3X update: $elementId -> $topic")
    }

    private fun findMatchingAddress(elementId: String): I3xAddress? {
        val cleanElement = elementId.trim().trim('/')
        // Exact match first
        val exact = i3xConfig.addresses.firstOrNull { it.elementId.trim().trim('/') == cleanElement }
        if (exact != null) return exact

        // Prefix match for recursive/subtree subscriptions (maxDepth == 0 or maxDepth > 1)
        return i3xConfig.addresses
            .filter { addr ->
                val prefix = addr.elementId.trim().trim('/')
                (addr.maxDepth == 0 || addr.maxDepth > 1) && cleanElement.startsWith("$prefix/")
            }
            .maxByOrNull { it.elementId.length }
    }
}
