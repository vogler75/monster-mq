package at.rocworks.devices.winccua

import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.devices.WinCCUaAddress
import at.rocworks.stores.devices.WinCCUaAddressType
import at.rocworks.stores.devices.WinCCUaConnectionConfig
import io.vertx.core.AbstractVerticle
import io.vertx.core.Context
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.channels.CompletionHandler
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger

/**
 * WinCC Unified Connector — Open Pipe variant.
 *
 * Talks to WinCC Unified Runtime via the local Open Pipe interface (`\\.\pipe\HmiRuntime` on
 * Windows, `/tmp/HmiRuntime` on Linux) using the expert (JSON) syntax. See
 * `dev/WinCC_Unified_OpenPipe_Reference.md` for the protocol.
 *
 * Differences vs. [WinCCUaConnector] (GraphQL):
 *  - No HTTP/WebSocket; a single duplex named pipe. Authentication is implicit via OS group
 *    membership (`SIMATIC HMI` on Windows, `industrial` on Linux).
 *  - Uses [AsynchronousFileChannel] (Windows overlapped I/O / FILE_FLAG_OVERLAPPED) so that
 *    reads and writes on the same handle are truly independent and never deadlock — the same
 *    reason Node.js `net.createConnection` works on named pipes while `RandomAccessFile`
 *    (synchronous I/O) deadlocks.
 *  - Browsing is paged via repeated `BrowseTags` `Next` requests. When the server considers the
 *    browse done it closes the browse session; any subsequent Next returns `ErrorBrowseTags`
 *    ("Your browse request has been expired") — treated as browse complete, not an error.
 *  - All requests/responses are correlated via `ClientCookie`.
 *
 * Downstream behavior — topic transformation, message format, MQTT envelope, metrics — is
 * identical to the GraphQL connector and shared via [WinCCUaPublisher].
 */
class WinCCUaPipeConnector : AbstractVerticle() {

    private val logger: Logger = Utils.getLogger(this::class.java)

    // Metrics
    private val messagesInCounter = AtomicLong(0)
    private var lastMetricsReset = System.currentTimeMillis()

    // Device config
    private lateinit var deviceConfig: DeviceConfig
    private lateinit var winCCUaConfig: WinCCUaConnectionConfig

    // Async pipe channel + line accumulator for splitting incoming bytes into lines
    private var pipeChannel: AsynchronousFileChannel? = null
    private val lineAccumulator = StringBuilder()

    // State (mutated only from the verticle context)
    private var isConnected = false
    private var isStopping = false
    private var reconnectTimerId: Long? = null
    private lateinit var verticleContext: Context

    // Subscription tracking — keyed by ClientCookie
    private val activeSubscriptions = ConcurrentHashMap<String, WinCCUaAddress>()
    private var nextCookieSeq = 1L

    // In-flight browse state — keyed by browse ClientCookie
    private class BrowseState(
        val address: WinCCUaAddress,
        val promise: Promise<List<String>>
    ) {
        val accumulated: MutableList<String> = mutableListOf()
    }
    private val pendingBrowses = ConcurrentHashMap<String, BrowseState>()

    // Topic transformation cache
    private val topicCache = ConcurrentHashMap<String, String>()

    override fun start(startPromise: Promise<Void>) {
        try {
            verticleContext = context

            val deviceJson = config().getJsonObject("device")
            deviceConfig = DeviceConfig.fromJsonObject(deviceJson)
            winCCUaConfig = WinCCUaConnectionConfig.fromJsonObject(deviceConfig.config)

            logger.info("Starting WinCCUaPipeConnector for device: ${deviceConfig.name}")
            logger.info("Open Pipe path: ${winCCUaConfig.resolvePipePath()} (process must be a member of 'SIMATIC HMI' on Windows / 'industrial' on Linux)")

            val validationErrors = winCCUaConfig.validate()
            if (validationErrors.isNotEmpty()) {
                val msg = "Configuration validation failed: ${validationErrors.joinToString(", ")}"
                logger.severe(msg)
                startPromise.fail(msg)
                return
            }

            setupMetricsEndpoint()
            startPromise.complete()

            openPipe()
                .compose { setupSubscriptions() }
                .onComplete { result ->
                    if (result.succeeded()) {
                        logger.info("Initial WinCC Unified Open Pipe connection successful for device ${deviceConfig.name}")
                    } else {
                        logger.warning("Initial Open Pipe connection failed for device ${deviceConfig.name}: ${result.cause()?.message}. Will retry automatically.")
                    }
                }

        } catch (e: Exception) {
            logger.severe("Exception during WinCCUaPipeConnector startup: ${e.message}")
            e.printStackTrace()
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Stopping WinCCUaPipeConnector for device: ${deviceConfig.name}")
        isStopping = true
        reconnectTimerId?.let { vertx.cancelTimer(it) }
        reconnectTimerId = null
        isConnected = false
        teardownState()
        closePipeChannelAsync().onComplete { stopPromise.complete() }
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
     * Open the named pipe as an [AsynchronousFileChannel] (overlapped I/O on Windows).
     * The channel open itself is done on a worker thread since [Path.of] + channel open can
     * do a stat/connect on the path. Once open, [startAsyncRead] is called on the event loop.
     */
    private fun openPipe(): Future<Void> {
        val promise = Promise.promise<Void>()
        if (isStopping) { promise.fail("Connector is stopping"); return promise.future() }
        if (isConnected) { promise.complete(); return promise.future() }

        val path = winCCUaConfig.resolvePipePath()

        vertx.executeBlocking(Callable<Void> {
            val channel = AsynchronousFileChannel.open(
                Path.of(path),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
            )
            pipeChannel = channel
            null
        })
        .onSuccess {
            isConnected = true
            lineAccumulator.clear()
            logger.info("Open Pipe connected for device ${deviceConfig.name} at $path")
            startAsyncRead()
            promise.complete()
        }
        .onFailure { error ->
            logger.severe("Failed to open pipe at $path for device ${deviceConfig.name}: ${error.message}")
            scheduleReconnection()
            promise.fail(error)
        }

        return promise.future()
    }

    /**
     * Post one non-blocking read on the async channel. When bytes arrive the CompletionHandler
     * dispatches them to the event loop for line splitting, then immediately posts the next read.
     * There is exactly one pending read at a time.
     *
     * Using overlapped I/O (AsynchronousFileChannel) allows a concurrent async write to be
     * in-flight on the same pipe handle simultaneously — the OS kernel handles the two
     * directions independently, matching exactly what Node.js `net.createConnection` does.
     *
     * The position argument is always 0; for named pipes the OS ignores it.
     */
    private fun startAsyncRead() {
        val channel = pipeChannel ?: return
        val buf = ByteBuffer.allocate(8192)
        channel.read(buf, 0L, buf, object : CompletionHandler<Int, ByteBuffer> {
            override fun completed(result: Int, attachment: ByteBuffer) {
                if (result > 0) {
                    attachment.flip()
                    val bytes = ByteArray(result)
                    attachment.get(bytes)
                    val str = bytes.toString(Charsets.UTF_8)
                    verticleContext.runOnContext { processReadBytes(str) }
                }
                when {
                    isStopping || !isConnected -> { /* shutting down — don't post another read */ }
                    result >= 0 -> { attachment.clear(); startAsyncRead() }
                    else -> verticleContext.runOnContext { onConnectionLost("EOF") }
                }
            }
            override fun failed(exc: Throwable, attachment: ByteBuffer) {
                if (!isStopping && isConnected) {
                    logger.warning("Open Pipe read failed for device ${deviceConfig.name}: ${exc.message}")
                    verticleContext.runOnContext { onConnectionLost(exc.message ?: "read error") }
                }
            }
        })
    }

    private fun processReadBytes(str: String) {
        for (ch in str) {
            if (ch == '\n') {
                val line = lineAccumulator.toString().trimEnd('\r')
                lineAccumulator.clear()
                if (line.isNotEmpty()) handlePipeMessage(line)
            } else {
                lineAccumulator.append(ch)
            }
        }
    }

    private fun onConnectionLost(reason: String) {
        if (!isConnected) return
        isConnected = false
        logger.warning("Open Pipe connection lost for device ${deviceConfig.name}: $reason")
        teardownState()
        closePipeChannelAsync()
        scheduleReconnection()
    }

    private fun teardownState() {
        lineAccumulator.clear()
        pendingBrowses.values.forEach { it.promise.tryFail("Pipe connection closed") }
        pendingBrowses.clear()
        activeSubscriptions.clear()
    }

    private fun closePipeChannelAsync(): Future<Void> {
        val ch = pipeChannel ?: return Future.succeededFuture()
        pipeChannel = null
        return vertx.executeBlocking(Callable<Void> {
            try { ch.close() } catch (_: Exception) {}
            null
        })
    }

    private fun scheduleReconnection() {
        if (isStopping) return
        reconnectTimerId?.let { vertx.cancelTimer(it) }
        reconnectTimerId = vertx.setTimer(winCCUaConfig.reconnectDelay) {
            reconnectTimerId = null
            if (isStopping || isConnected) return@setTimer
            logger.info("Attempting to reconnect Open Pipe for device ${deviceConfig.name}...")
            openPipe()
                .compose { setupSubscriptions() }
                .onComplete { result ->
                    if (result.failed()) {
                        logger.warning("Open Pipe reconnection failed for device ${deviceConfig.name}: ${result.cause()?.message}")
                    }
                }
        }
        logger.info("Scheduled Open Pipe reconnection for device ${deviceConfig.name} in ${winCCUaConfig.reconnectDelay}ms")
    }

    /**
     * Send one expert-syntax JSON message terminated by `\n`. Uses overlapped I/O so the write
     * never blocks the event loop or a worker thread, and can proceed concurrently with a
     * pending read on the same channel.
     *
     * The position argument is always 0; named pipes ignore it in the overlapped structure.
     */
    private fun sendCommand(json: JsonObject): Future<Boolean> {
        val channel = pipeChannel ?: return Future.succeededFuture(false)
        val encoded = json.encode()
        logger.info("Open Pipe TX [${deviceConfig.name}]: $encoded")
        val bytes = ByteBuffer.wrap((encoded + "\n").toByteArray(Charsets.UTF_8))
        val promise = Promise.promise<Boolean>()
        channel.write(bytes, 0L, null, object : CompletionHandler<Int, Void?> {
            override fun completed(result: Int, attachment: Void?) {
                logger.info("Open Pipe TX flushed [${deviceConfig.name}] ($result bytes)")
                verticleContext.runOnContext { promise.tryComplete(true) }
            }
            override fun failed(exc: Throwable, attachment: Void?) {
                logger.warning("Open Pipe write failed for device ${deviceConfig.name}: ${exc.message}")
                verticleContext.runOnContext {
                    promise.tryComplete(false)
                    if (!isStopping) onConnectionLost(exc.message ?: "write error")
                }
            }
        })
        return promise.future()
    }

    private fun nextCookie(prefix: String): String = "${deviceConfig.name}-$prefix-${nextCookieSeq++}"

    /* ---------------------- Subscription setup ---------------------- */

    private fun setupSubscriptions(): Future<Void> {
        if (!isConnected) return Future.failedFuture("Pipe not connected")
        val addresses = winCCUaConfig.addresses
        if (addresses.isEmpty()) {
            logger.info("No addresses configured for device ${deviceConfig.name}")
            return Future.succeededFuture()
        }
        logger.info("Setting up ${addresses.size} subscriptions for device ${deviceConfig.name}")

        val futures = addresses.map { address ->
            when (address.type) {
                WinCCUaAddressType.TAG_VALUES -> setupTagValuesSubscription(address)
                WinCCUaAddressType.ACTIVE_ALARMS -> setupActiveAlarmsSubscription(address)
            }
        }
        val promise = Promise.promise<Void>()
        Future.all<Void>(futures).onComplete { result ->
            if (result.succeeded()) {
                logger.info("All Open Pipe subscriptions set up for device ${deviceConfig.name}")
                promise.complete()
            } else {
                logger.warning("Some Open Pipe subscriptions failed: ${result.cause()?.message}")
                promise.complete() // Continue anyway, matching GraphQL connector behavior
            }
        }
        return promise.future()
    }

    private fun setupTagValuesSubscription(address: WinCCUaAddress): Future<Void> {
        val promise = Promise.promise<Void>()
        executeBrowse(address)
            .onComplete { browseResult ->
                if (browseResult.succeeded()) {
                    val tagList = browseResult.result()
                    logger.info("Open Pipe BrowseTags returned ${tagList.size} tags for address ${address.topic}")
                    if (tagList.isNotEmpty()) {
                        subscribeToTags(address, tagList)
                    } else {
                        logger.warning("BrowseTags returned no tags for address ${address.topic} — nothing to subscribe to")
                    }
                    promise.complete()
                } else {
                    logger.severe("Open Pipe BrowseTags failed: ${browseResult.cause()?.message}")
                    promise.fail(browseResult.cause())
                }
            }
        return promise.future()
    }

    /**
     * Issue a paged BrowseTags. Responses are routed back via [handleBrowseTagsResponse], which
     * sends Next requests until the server returns an empty page or closes the browse session
     * (signalled by ErrorBrowseTags, treated as complete in [handleErrorMessage]).
     */
    private fun executeBrowse(address: WinCCUaAddress): Future<List<String>> {
        val promise = Promise.promise<List<String>>()
        val nameFilters = address.nameFilters
        if (nameFilters.isNullOrEmpty()) {
            promise.fail("No name filters provided for address ${address.topic}")
            return promise.future()
        }

        val cookie = nextCookie("browse")
        pendingBrowses[cookie] = BrowseState(address, promise)

        // BrowseTags accepts a single Filter. Use the first; warn if more are configured.
        val filter = nameFilters.first()
        if (nameFilters.size > 1) {
            logger.warning("Open Pipe BrowseTags accepts a single Filter; using first '$filter' and ignoring others for address ${address.topic}")
        }

        val params = JsonObject().put("Filter", filter)
        if (address.systemNames != null && address.systemNames.isNotEmpty()) {
            params.put("SystemNames", JsonArray(address.systemNames))
        }

        val request = JsonObject()
            .put("Message", "BrowseTags")
            .put("Params", params)
            .put("ClientCookie", cookie)

        sendCommand(request).onComplete { result ->
            if (result.failed() || result.result() != true) {
                pendingBrowses.remove(cookie)
                promise.tryFail("Failed to send BrowseTags request")
            }
        }
        return promise.future()
    }

    private fun subscribeToTags(address: WinCCUaAddress, tags: List<String>) {
        if (address.pipeTagMode == WinCCUaAddress.PIPE_TAG_MODE_SINGLE) {
            // One SubscribeTag per tag: RT notifies only for that tag when it changes.
            // Each cookie maps independently; the tag name comes from the notification payload.
            tags.forEach { tagName ->
                val cookie = nextCookie("tagvalues")
                val request = JsonObject()
                    .put("Message", "SubscribeTag")
                    .put("Params", JsonObject().put("Tags", JsonArray(listOf(tagName))))
                    .put("ClientCookie", cookie)
                activeSubscriptions[cookie] = address
                sendCommand(request).onComplete { result ->
                    if (result.failed() || result.result() != true) activeSubscriptions.remove(cookie)
                }
            }
            logger.info("Open Pipe SubscribeTag SINGLE: ${tags.size} individual subscriptions for address ${address.topic}")
        } else {
            // BULK: one SubscribeTag with all tags; RT fires with all values on any change.
            val cookie = nextCookie("tagvalues")
            val request = JsonObject()
                .put("Message", "SubscribeTag")
                .put("Params", JsonObject().put("Tags", JsonArray(tags)))
                .put("ClientCookie", cookie)
            activeSubscriptions[cookie] = address
            sendCommand(request).onComplete { result ->
                if (result.failed() || result.result() != true) {
                    activeSubscriptions.remove(cookie)
                } else {
                    logger.info("Open Pipe SubscribeTag BULK: ${tags.size} tags on address ${address.topic} (cookie: $cookie)")
                }
            }
        }
    }

    private fun setupActiveAlarmsSubscription(address: WinCCUaAddress): Future<Void> {
        val cookie = nextCookie("alarms")
        val params = JsonObject()
        if (address.systemNames != null && address.systemNames.isNotEmpty()) {
            params.put("SystemNames", JsonArray(address.systemNames))
        }
        if (address.filterString != null) {
            params.put("Filter", address.filterString)
        }
        val request = JsonObject()
            .put("Message", "SubscribeAlarm")
            .put("Params", params)
            .put("ClientCookie", cookie)
        activeSubscriptions[cookie] = address
        val promise = Promise.promise<Void>()
        sendCommand(request).onComplete { result ->
            if (result.failed() || result.result() != true) {
                activeSubscriptions.remove(cookie)
                promise.fail("Failed to send SubscribeAlarm")
            } else {
                logger.info("Open Pipe SubscribeAlarm for address ${address.topic} (cookie: $cookie)")
                promise.complete()
            }
        }
        return promise.future()
    }

    /* ---------------------- Incoming message dispatch ---------------------- */

    private fun handlePipeMessage(line: String) {
        try {
            logger.fine { "Open Pipe RX [${deviceConfig.name}]: $line" }
            val json = JsonObject(line)
            when (val message = json.getString("Message")) {
                "NotifyBrowseTags" -> handleBrowseTagsResponse(json)
                "NotifySubscribeTag", "NotifyReadTag" -> handleTagNotification(json)
                "NotifySubscribeAlarm", "NotifyReadAlarm" -> handleAlarmNotification(json)
                "NotifyUnsubscribeTag", "NotifyUnsubscribeAlarm" -> {
                    val cookie = json.getString("ClientCookie")
                    activeSubscriptions.remove(cookie)
                }
                null -> logger.warning("Open Pipe message without Message field: $line")
                else -> {
                    if (message.startsWith("Error")) {
                        handleErrorMessage(json, message)
                    } else {
                        logger.fine { "Open Pipe unhandled message '$message' for device ${deviceConfig.name}" }
                    }
                }
            }
        } catch (e: Exception) {
            logger.severe("Error handling Open Pipe message for device ${deviceConfig.name}: ${e.message}. Line: $line")
        }
    }

    private fun handleBrowseTagsResponse(json: JsonObject) {
        val cookie = json.getString("ClientCookie") ?: return
        val state = pendingBrowses[cookie] ?: run {
            logger.warning("Open Pipe NotifyBrowseTags for unknown browse cookie: $cookie")
            return
        }
        val params = json.getJsonObject("Params")
        val tagsArray = params?.getJsonArray("Tags")
        val pageSize = tagsArray?.size() ?: 0

        if (pageSize == 0) {
            pendingBrowses.remove(cookie)
            state.promise.tryComplete(state.accumulated.toList())
            return
        }

        for (i in 0 until tagsArray!!.size()) {
            val item = tagsArray.getJsonObject(i)
            val name = item.getString("Name") ?: item.getString("name") ?: continue
            state.accumulated.add(name)
        }

        // Request next page
        val nextRequest = JsonObject()
            .put("Message", "BrowseTags")
            .put("Params", "Next")
            .put("ClientCookie", cookie)
        sendCommand(nextRequest).onComplete { result ->
            if (result.failed() || result.result() != true) {
                pendingBrowses.remove(cookie)
                state.promise.tryFail("Failed to send BrowseTags Next")
            }
        }
    }

    private fun handleTagNotification(json: JsonObject) {
        val cookie = json.getString("ClientCookie") ?: return
        val address = activeSubscriptions[cookie] ?: run {
            logger.warning("Open Pipe tag notification for unknown subscription cookie: $cookie")
            return
        }

        if (address.pipeTagMode == WinCCUaAddress.PIPE_TAG_MODE_BULK) {
            // BULK: one subscription for all tags. RT sends all tag values on every change.
            // Publish only the Tags array (Params.Tags) to the single configured topic.
            val tags = json.getJsonObject("Params")?.getJsonArray("Tags") ?: return
            val topic = "${deviceConfig.namespace}/${address.topic}"
            val msg = WinCCUaPublisher.buildTagBrokerMessage(
                deviceConfig.name, topic, tags.encode().toByteArray(), address.retained
            )
            vertx.eventBus().publish(WinCCUaExtension.ADDRESS_WINCCUA_VALUE_PUBLISH, msg)
            messagesInCounter.incrementAndGet()
            logger.fine { "Open Pipe BULK published to $topic" }
            return
        }

        // PER_TAG: each subscription covers exactly one tag, so RT only fires when that tag
        // changes. Publish each tag in the notification to its individual topic/<tagName>.
        val tagsArray = json.getJsonObject("Params")?.getJsonArray("Tags") ?: return
        for (i in 0 until tagsArray.size()) {
            val tagJson = tagsArray.getJsonObject(i)
            val errorCode = tagJson.getValue("ErrorCode")
            val errorIsZero = when (errorCode) {
                null -> true
                is Number -> errorCode.toLong() == 0L
                is String -> errorCode == "0" || errorCode.isEmpty()
                else -> false
            }
            if (!errorIsZero) {
                val errorDesc = tagJson.getString("ErrorDescription") ?: ""
                logger.warning("Open Pipe tag error for ${tagJson.getString("Name")}: $errorCode $errorDesc")
                continue
            }

            val name = tagJson.getString("Name") ?: continue
            val value = tagJson.getValue("Value")
            val timestamp = tagJson.getString("TimeStamp")
            val qualityStr = tagJson.getString("Quality")
            val qualityCode = tagJson.getString("QualityCode")
            val qualityJson = if (qualityStr != null || qualityCode != null) {
                JsonObject().apply {
                    if (qualityStr != null) put("quality", qualityStr)
                    if (qualityCode != null) put("qualityCode", qualityCode)
                }
            } else null

            publishTagValue(address, name, value, timestamp, qualityJson)
        }
    }

    private fun handleAlarmNotification(json: JsonObject) {
        val cookie = json.getString("ClientCookie") ?: return
        val address = activeSubscriptions[cookie] ?: run {
            logger.warning("Open Pipe alarm notification for unknown subscription cookie: $cookie")
            return
        }
        val params = json.getJsonObject("Params") ?: json.getJsonObject("params") ?: return
        val alarmsArray = params.getJsonArray("Alarms") ?: return
        for (i in 0 until alarmsArray.size()) {
            val alarmJson = alarmsArray.getJsonObject(i) ?: continue
            publishAlarm(address, alarmJson)
        }
    }

    private fun handleErrorMessage(json: JsonObject, message: String) {
        val cookie = json.getString("ClientCookie")
        val errorCode = json.getValue("ErrorCode")
        val errorDesc = json.getString("ErrorDescription") ?: ""

        // When the server considers a browse done it closes the browse session. A subsequent
        // Next returns ErrorBrowseTags ("Your browse request has been expired"). This happens
        // when the last page contained fewer tags than the page size — treat as browse complete.
        if (message == "ErrorBrowseTags" && cookie != null) {
            val state = pendingBrowses.remove(cookie)
            if (state != null) {
                logger.info("Open Pipe ErrorBrowseTags for cookie $cookie ($errorDesc) — browse complete with ${state.accumulated.size} accumulated tags")
                state.promise.tryComplete(state.accumulated.toList())
                return
            }
        }

        logger.severe("Open Pipe $message for device ${deviceConfig.name} (cookie=$cookie, code=$errorCode): $errorDesc")
        if (cookie != null) {
            pendingBrowses.remove(cookie)?.promise?.tryFail("$message: $errorDesc")
        }
    }

    /* ---------------------- Publish (shared with GraphQL connector) ---------------------- */

    private fun publishTagValue(address: WinCCUaAddress, tagName: String, value: Any?, timestamp: String?, quality: JsonObject?) {
        try {
            val mqttTopic = WinCCUaPublisher.resolveTopic(
                deviceConfig.namespace, address.topic, tagName, winCCUaConfig.transformConfig, topicCache
            )
            val payload = WinCCUaPublisher.formatTagPayload(winCCUaConfig.messageFormat, value, timestamp, quality)
            val msg = WinCCUaPublisher.buildTagBrokerMessage(deviceConfig.name, mqttTopic, payload, address.retained)
            vertx.eventBus().publish(WinCCUaExtension.ADDRESS_WINCCUA_VALUE_PUBLISH, msg)
            messagesInCounter.incrementAndGet()
            logger.fine { "Open Pipe published tag: $mqttTopic = $value" }
        } catch (e: Exception) {
            logger.severe("Error publishing tag value for $tagName: ${e.message}")
        }
    }

    private fun publishAlarm(address: WinCCUaAddress, alarmData: JsonObject) {
        try {
            val alarmName = WinCCUaPublisher.resolveAlarmName(alarmData) ?: "unknown"
            val mqttTopic = WinCCUaPublisher.buildAlarmTopic(deviceConfig.namespace, address.topic, alarmName)
            val msg = WinCCUaPublisher.buildAlarmBrokerMessage(deviceConfig.name, mqttTopic, alarmData, address.retained)
            vertx.eventBus().publish(WinCCUaExtension.ADDRESS_WINCCUA_VALUE_PUBLISH, msg)
            messagesInCounter.incrementAndGet()
            logger.fine { "Open Pipe published alarm: $mqttTopic" }
        } catch (e: Exception) {
            logger.severe("Error publishing alarm: ${e.message}")
        }
    }
}
