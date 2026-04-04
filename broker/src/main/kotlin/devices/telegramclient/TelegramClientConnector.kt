package at.rocworks.devices.telegramclient

import at.rocworks.Const
import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.devices.TelegramClientConfig
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Level
import java.util.logging.Logger

/**
 * TelegramClientConnector
 *
 * Per-device bidirectional bridge between the local MQTT broker and Telegram Bot API.
 *
 * Topic structure (namespace = device.namespace):
 *   {namespace}/chats        — retained JSON array of registered chat IDs
 *   {namespace}/in/{chatId}  — incoming text messages from Telegram
 *   {namespace}/out/{chatId} — publish here to send a message to a Telegram chat
 *
 * Bot commands handled:
 *   /start — registers the chat (adds chatId to retained chats list)
 *   /stop  — unregisters the chat (removes chatId from retained chats list)
 *
 * Allowed users: if [TelegramClientConfig.allowedUsers] is non-empty, only messages
 * from those Telegram usernames are accepted. Others receive a rejection reply.
 */
class TelegramClientConnector : AbstractVerticle() {
    private val logger: Logger = Utils.getLogger(this::class.java)

    private lateinit var device: DeviceConfig
    private lateinit var cfg: TelegramClientConfig

    private var webClient: WebClient? = null
    private var pollingTimerId: Long? = null
    private var reconnectTimerId: Long? = null
    private var isConnected = false
    private var updateOffset: Long = 0

    /** Active chats that have /start-ed. Maps chatId → user info JSON. Published as retained on {namespace}/chats. */
    private val registeredChats = ConcurrentHashMap<Long, JsonObject>()

    // Metrics
    private val messagesIn = AtomicLong(0)   // Telegram → MQTT
    private val messagesOut = AtomicLong(0)  // MQTT → Telegram
    private val errors = AtomicLong(0)
    private var lastMetricsReset = System.currentTimeMillis()

    private val internalClientId get() = "telegramclient-${device.name}"
    private val namespace get() = device.namespace

    companion object {
        private const val TELEGRAM_API_HOST = "api.telegram.org"
        private const val TELEGRAM_API_PORT = 443
    }

    // ── Lifecycle ────────────────────────────────────────────────────────

    override fun start(startPromise: Promise<Void>) {
        try {
            val deviceJson = config().getJsonObject("device")
            device = DeviceConfig.fromJsonObject(deviceJson)
            cfg = TelegramClientConfig.fromJson(device.config)

            val validationErrors = cfg.validate()
            if (validationErrors.isNotEmpty()) {
                startPromise.fail("TelegramClient config errors: ${validationErrors.joinToString(", ")}")
                return
            }

            logger.info("Starting TelegramClientConnector for device '${device.name}' namespace='$namespace'")

            val options = WebClientOptions()
                .setSsl(true)
                .setDefaultHost(TELEGRAM_API_HOST)
                .setDefaultPort(TELEGRAM_API_PORT)
                .setTrustAll(true)
                .setConnectTimeout(10000)

            if (!cfg.proxyHost.isNullOrBlank() && cfg.proxyPort != null) {
                options.setProxyOptions(
                    io.vertx.core.net.ProxyOptions()
                        .setHost(cfg.proxyHost!!)
                        .setPort(cfg.proxyPort!!)
                )
            }

            webClient = WebClient.create(vertx, options)

            // Set up outbound: subscribe to {namespace}/out/#
            setupOutboundSubscription()

            // Register metrics endpoint
            setupMetricsEndpoint()

            startPromise.complete()

            // Verify bot token, then start polling
            verifyBotAndStartPolling()
        } catch (e: Exception) {
            logger.log(Level.SEVERE, "Failed to start TelegramClientConnector: ${e.message}", e)
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Stopping TelegramClientConnector for device '${device.name}'")

        pollingTimerId?.let { vertx.cancelTimer(it); pollingTimerId = null }
        reconnectTimerId?.let { vertx.cancelTimer(it); reconnectTimerId = null }

        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler != null) {
            sessionHandler.unsubscribeInternalClient(internalClientId, "$namespace/out/#")
            sessionHandler.unregisterInternalClient(internalClientId)
        }

        webClient?.close()
        webClient = null
        isConnected = false

        logger.info("TelegramClientConnector '${device.name}' stopped")
        stopPromise.complete()
    }

    // ── Bot verification and polling ─────────────────────────────────────

    private fun verifyBotAndStartPolling() {
        val client = webClient ?: return
        client.get("/bot${cfg.botToken}/getMe")
            .send()
            .onSuccess { response ->
                val body = response.bodyAsJsonObject()
                if (body.getBoolean("ok", false)) {
                    val botUser = body.getJsonObject("result")
                    logger.info("Telegram bot verified for '${device.name}': @${botUser.getString("username")}")
                    isConnected = true
                    pollUpdates()
                } else {
                    logger.warning("Telegram bot verification failed for '${device.name}': ${body.getString("description")}")
                    isConnected = false
                    scheduleReconnect()
                }
            }
            .onFailure { err ->
                logger.warning("Failed to verify Telegram bot for '${device.name}': ${err.message}")
                isConnected = false
                scheduleReconnect()
            }
    }

    private fun scheduleReconnect() {
        if (reconnectTimerId != null) return
        reconnectTimerId = vertx.setTimer(cfg.reconnectDelayMs) {
            reconnectTimerId = null
            if (!isConnected) {
                logger.info("Retrying Telegram bot verification for device '${device.name}'…")
                verifyBotAndStartPolling()
            }
        }
    }

    // ── Inbound: Telegram → MQTT (long polling) ─────────────────────────

    private fun pollUpdates() {
        if (!isConnected) return
        val client = webClient ?: return

        client.get("/bot${cfg.botToken}/getUpdates")
            .addQueryParam("offset", updateOffset.toString())
            .addQueryParam("timeout", cfg.pollingTimeoutSeconds.toString())
            .timeout((cfg.pollingTimeoutSeconds + 10) * 1000L)
            .send()
            .onSuccess { response ->
                try {
                    val body = response.bodyAsJsonObject()
                    if (body.getBoolean("ok", false)) {
                        val results = body.getJsonArray("result")
                        if (results != null && results.size() > 0) {
                            for (i in 0 until results.size()) {
                                val update = results.getJsonObject(i)
                                processUpdate(update)
                                val updateId = update.getLong("update_id", 0)
                                if (updateId >= updateOffset) {
                                    updateOffset = updateId + 1
                                }
                            }
                        }
                    } else {
                        logger.warning("getUpdates failed for '${device.name}': ${body.getString("description")}")
                        errors.incrementAndGet()
                    }
                } catch (e: Exception) {
                    logger.warning("Error processing Telegram updates for '${device.name}': ${e.message}")
                    errors.incrementAndGet()
                }

                if (isConnected) {
                    pollingTimerId = vertx.setTimer(100) { pollUpdates() }
                }
            }
            .onFailure { err ->
                logger.warning("Telegram polling error for '${device.name}': ${err.message}")
                errors.incrementAndGet()
                if (isConnected) {
                    pollingTimerId = vertx.setTimer(cfg.reconnectDelayMs) { pollUpdates() }
                }
            }
    }

    private fun processUpdate(update: JsonObject) {
        val message = update.getJsonObject("message") ?: return
        val chat = message.getJsonObject("chat") ?: return
        val chatId = chat.getLong("id") ?: return
        val from = message.getJsonObject("from")
        val username = from?.getString("username")
        val text = message.getString("text") ?: return

        // Check allowed users
        if (!cfg.isUserAllowed(username)) {
            logger.fine { "Telegram message from unauthorized user '${username}' in chat $chatId — ignored" }
            sendTelegramMessage(chatId, "Sorry, you are not authorized to use this bot.")
            return
        }

        // Handle bot commands
        when {
            text.startsWith("/start") -> {
                handleStartCommand(chatId, username, from, chat)
                return
            }
            text.startsWith("/stop") -> {
                handleStopCommand(chatId, username)
                return
            }
        }

        // Regular message → publish to {namespace}/in/{chatId}
        val payload = JsonObject()
            .put("chatId", chatId)
            .put("from", username ?: from?.getString("first_name") ?: "unknown")
            .put("text", text)
            .put("messageId", message.getLong("message_id", 0))
            .put("date", message.getLong("date", 0))

        val mqttTopic = "$namespace/in/$chatId"
        val brokerMsg = BrokerMessage(
            messageId = 0,
            topicName = mqttTopic,
            payload = payload.encode().toByteArray(),
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = internalClientId,
            senderId = internalClientId
        )
        Monster.getSessionHandler()?.publishMessage(brokerMsg)
        messagesIn.incrementAndGet()
        logger.fine { "Telegram→MQTT chat=$chatId → topic='$mqttTopic'" }
    }

    // ── Bot commands: /start and /stop ───────────────────────────────────

    private fun handleStartCommand(chatId: Long, username: String?, from: JsonObject?, chat: JsonObject?) {
        val isNew = !registeredChats.containsKey(chatId)
        val chatInfo = JsonObject()
            .put("chatId", chatId)
            .put("username", username)
            .put("firstName", from?.getString("first_name"))
            .put("lastName", from?.getString("last_name"))
            .put("languageCode", from?.getString("language_code"))
            .put("chatType", chat?.getString("type") ?: "private")
            .put("chatTitle", chat?.getString("title"))  // for groups/channels
            .put("registeredAt", java.time.Instant.now().toString())
        registeredChats[chatId] = chatInfo
        publishChatsRetained()
        val displayName = username ?: from?.getString("first_name") ?: "there"
        if (isNew) {
            logger.info("Chat $chatId registered for device '${device.name}' (user: $displayName)")
            sendTelegramMessage(chatId, "Welcome, $displayName! This chat is now registered.")
        } else {
            sendTelegramMessage(chatId, "This chat is already registered. User info updated.")
        }
    }

    private fun handleStopCommand(chatId: Long, username: String?) {
        val removed = registeredChats.remove(chatId)
        publishChatsRetained()
        if (removed != null) {
            logger.info("Chat $chatId unregistered for device '${device.name}' (user: $username)")
            sendTelegramMessage(chatId, "This chat has been unregistered. Send /start to re-register.")
        } else {
            sendTelegramMessage(chatId, "This chat was not registered.")
        }
    }

    /** Publish the current registered chats as a retained JSON array on {namespace}/chats. */
    private fun publishChatsRetained() {
        val chatArray = JsonArray(registeredChats.values.toList())
        val brokerMsg = BrokerMessage(
            messageId = 0,
            topicName = "$namespace/chats",
            payload = chatArray.encode().toByteArray(),
            qosLevel = 1,
            isRetain = true,
            isDup = false,
            isQueued = false,
            clientId = internalClientId,
            senderId = internalClientId
        )
        Monster.getSessionHandler()?.publishMessage(brokerMsg)
        logger.fine { "Published retained chats for '${device.name}': $chatArray" }
    }

    // ── Outbound: MQTT → Telegram ────────────────────────────────────────

    /**
     * Subscribe to {namespace}/out/# so that any message published to
     * {namespace}/out/{chatId} is forwarded to that Telegram chat.
     */
    private fun setupOutboundSubscription() {
        val outboundTopic = "$namespace/out/#"

        vertx.eventBus().consumer<Any>(EventBusAddresses.Client.messages(internalClientId)) { busMsg ->
            try {
                when (val body = busMsg.body()) {
                    is BrokerMessage -> handleOutboundMessage(body)
                    is at.rocworks.data.BulkClientMessage -> body.messages.forEach { handleOutboundMessage(it) }
                    else -> logger.warning("Unknown message type from EventBus: ${body?.javaClass?.simpleName}")
                }
            } catch (e: Exception) {
                errors.incrementAndGet()
                logger.warning("Error processing outbound message for '${device.name}': ${e.message}")
            }
        }

        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler != null) {
            logger.info("Internal MQTT subscription for outbound Telegram '$internalClientId' → topic '$outboundTopic'")
            sessionHandler.subscribeInternalClient(internalClientId, outboundTopic, 0)
        } else {
            logger.severe("SessionHandler not available for outbound Telegram subscriptions")
        }
    }

    private fun handleOutboundMessage(msg: BrokerMessage) {
        // Loop prevention
        if (msg.senderId == internalClientId || msg.clientId == internalClientId) return

        if (!isConnected) {
            logger.fine { "Telegram not connected, dropping outbound message on '${msg.topicName}'" }
            return
        }

        // Extract chatId from topic: {namespace}/out/{chatId}
        val prefix = "$namespace/out/"
        if (!msg.topicName.startsWith(prefix)) return
        val chatIdStr = msg.topicName.substring(prefix.length)
        val chatId = chatIdStr.toLongOrNull()
        if (chatId == null) {
            logger.warning("Invalid chatId in outbound topic '${msg.topicName}'")
            errors.incrementAndGet()
            return
        }

        val text = String(msg.payload)
        sendTelegramMessage(chatId, text)
    }

    private fun sendTelegramMessage(chatId: Long, text: String) {
        val client = webClient ?: return

        val body = JsonObject()
            .put("chat_id", chatId)
            .put("text", text)

        if (cfg.parseMode != "Text") {
            body.put("parse_mode", cfg.parseMode)
        }

        client.post("/bot${cfg.botToken}/sendMessage")
            .sendJsonObject(body)
            .onSuccess { response ->
                val result = response.bodyAsJsonObject()
                if (result.getBoolean("ok", false)) {
                    messagesOut.incrementAndGet()
                    logger.fine { "MQTT→Telegram chat=$chatId" }
                } else {
                    errors.incrementAndGet()
                    logger.warning("Telegram sendMessage failed for '${device.name}': ${result.getString("description")}")
                }
            }
            .onFailure { err ->
                errors.incrementAndGet()
                logger.warning("Failed to send Telegram message for '${device.name}': ${err.message}")
            }
    }

    // ── Metrics endpoint ─────────────────────────────────────────────────

    private fun setupMetricsEndpoint() {
        val addr = EventBusAddresses.TelegramBridge.connectorMetrics(device.name)
        vertx.eventBus().consumer<JsonObject>(addr) { msg ->
            try {
                val now = System.currentTimeMillis()
                val elapsedMs = now - lastMetricsReset
                val elapsedSec = if (elapsedMs > 0) elapsedMs / 1000.0 else 1.0
                val inCount = messagesIn.getAndSet(0)
                val outCount = messagesOut.getAndSet(0)
                val errCount = errors.get()
                lastMetricsReset = now
                msg.reply(
                    JsonObject()
                        .put("device", device.name)
                        .put("messagesInRate", inCount / elapsedSec)
                        .put("messagesOutRate", outCount / elapsedSec)
                        .put("errors", errCount)
                        .put("elapsedMs", elapsedMs)
                        .put("connected", isConnected)
                        .put("registeredChats", registeredChats.size)
                )
            } catch (e: Exception) {
                msg.fail(500, e.message)
            }
        }
    }
}
