package at.rocworks.devices.natsclient

import at.rocworks.Const
import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.devices.NatsClientAddress
import at.rocworks.stores.devices.NatsClientConfig
import io.nats.client.Connection
import io.nats.client.Dispatcher
import io.nats.client.JetStream
import io.nats.client.JetStreamSubscription
import io.nats.client.Nats
import io.nats.client.PushSubscribeOptions
import io.nats.client.api.ConsumerConfiguration
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.util.concurrent.Callable
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Level
import java.util.logging.Logger

/**
 * NatsClientConnector
 *
 * Per-device bidirectional bridge between the local MQTT broker and a NATS server.
 *
 * Outbound (MQTT → NATS): Subscribes to local MQTT topics as an internal client and
 *   publishes received [BrokerMessage]s to the mapped NATS subject.
 *
 * Inbound  (NATS → MQTT): Creates a NATS dispatcher (or JetStream push-subscriber) for
 *   each SUBSCRIBE address and republishes arriving messages to the local MQTT broker.
 */
class NatsClientConnector : AbstractVerticle() {
    private val logger: Logger = Utils.getLogger(this::class.java)

    private lateinit var device: DeviceConfig
    private lateinit var cfg: NatsClientConfig

    // NATS connection state
    private var natsConnection: Connection? = null
    private var jetStream: JetStream? = null
    private val dispatchers = CopyOnWriteArrayList<Dispatcher>()
    private val jetStreamSubscriptions = CopyOnWriteArrayList<JetStreamSubscription>()

    private var isConnected = false
    private var reconnectTimerId: Long? = null

    // Metrics
    private val messagesIn = AtomicLong(0)   // NATS → MQTT
    private val messagesOut = AtomicLong(0)  // MQTT → NATS
    private val errors = AtomicLong(0)
    private var lastMetricsReset = System.currentTimeMillis()

    // Internal MQTT client identity for the outbound direction
    private val internalClientId get() = "natsclient-${device.name}"

    // ── Lifecycle ────────────────────────────────────────────────────────

    override fun start(startPromise: Promise<Void>) {
        try {
            val deviceJson = config().getJsonObject("device")
            device = DeviceConfig.fromJsonObject(deviceJson)
            cfg = NatsClientConfig.fromJson(device.config)

            val validationErrors = cfg.validate()
            if (validationErrors.isNotEmpty()) {
                startPromise.fail("NatsClient config errors: ${validationErrors.joinToString(", ")}")
                return
            }

            logger.info("Starting NatsClientConnector for device '${device.name}' servers=${cfg.servers}")

            // Set up outbound direction (MQTT → NATS) immediately so messages queue while we connect
            setupOutboundSubscriptions()

            // Register metrics endpoint
            setupMetricsEndpoint()

            // Start promise completes immediately; NATS connection happens in the background
            startPromise.complete()

            // Connect to NATS asynchronously (blocking)
            vertx.executeBlocking(Callable {
                try {
                    connectToNats()
                } catch (e: Exception) {
                    logger.warning("Initial NATS connection failed for '${device.name}': ${e.message}. Will retry.")
                }
                null
            })
        } catch (e: Exception) {
            logger.log(Level.SEVERE, "Failed to start NatsClientConnector: ${e.message}", e)
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Stopping NatsClientConnector for device '${device.name}'")

        // Cancel any pending reconnect timer
        reconnectTimerId?.let { vertx.cancelTimer(it) ; reconnectTimerId = null }

        // Unsubscribe internal MQTT client
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler != null) {
            cfg.addresses.filter { it.isPublish() }.forEach { addr ->
                sessionHandler.unsubscribeInternalClient(internalClientId, addr.mqttTopic)
            }
            sessionHandler.unregisterInternalClient(internalClientId)
        }

        // Close NATS connection in background
        vertx.executeBlocking(Callable {
            disconnectFromNats()
            null
        }).onComplete {
            logger.info("NatsClientConnector '${device.name}' stopped")
            stopPromise.complete()
        }
    }

    // ── NATS connection management ───────────────────────────────────────

    private fun connectToNats() {
        try {
            logger.info("Connecting to NATS for device '${device.name}': ${cfg.servers}")

            val options = cfg.buildOptions()
            val conn = Nats.connect(options)
            natsConnection = conn
            isConnected = true

            if (cfg.useJetStream) {
                jetStream = conn.jetStream()
            }

            // Ensure we reconnect when the connection drops
            // jnats fires a connection status change when disconnected
            vertx.runOnContext {
                logger.info("Connected to NATS for device '${device.name}'")
                setupInboundSubscriptions()
            }
        } catch (e: Exception) {
            logger.log(Level.WARNING, "NATS connection failed for '${device.name}': ${e.message}")
            isConnected = false
            natsConnection = null
            scheduleReconnect()
        }
    }

    private fun disconnectFromNats() {
        reconnectTimerId?.let { vertx.cancelTimer(it) ; reconnectTimerId = null }
        try {
            dispatchers.forEach { runCatching { natsConnection?.closeDispatcher(it) } }
            dispatchers.clear()
            jetStreamSubscriptions.forEach { runCatching { it.unsubscribe() } }
            jetStreamSubscriptions.clear()
            natsConnection?.close()
        } catch (e: Exception) {
            logger.warning("Error closing NATS connection for '${device.name}': ${e.message}")
        } finally {
            natsConnection = null
            jetStream = null
            isConnected = false
        }
    }

    private fun scheduleReconnect() {
        if (reconnectTimerId != null) return
        reconnectTimerId = vertx.setTimer(cfg.reconnectDelayMs) {
            reconnectTimerId = null
            if (!isConnected) {
                logger.info("Retrying NATS connection for device '${device.name}'…")
                vertx.executeBlocking(Callable { connectToNats(); null })
            }
        }
    }

    // ── Inbound direction: NATS → MQTT ───────────────────────────────────

    /**
     * Called on the Vert.X event loop after a successful NATS connect.
     * Sets up NATS subscriptions (dispatcher or JetStream) for every SUBSCRIBE address.
     */
    private fun setupInboundSubscriptions() {
        val subscribeAddresses = cfg.addresses.filter { it.isSubscribe() }
        if (subscribeAddresses.isEmpty()) return

        val conn = natsConnection ?: return

        subscribeAddresses.forEach { addr ->
            try {
                if (cfg.useJetStream && jetStream != null) {
                    setupJetStreamSubscription(addr, conn)
                } else {
                    setupCoreNatsSubscription(addr, conn)
                }
                logger.info("Subscribed to NATS subject '${addr.natsSubject}' for device '${device.name}'")
            } catch (e: Exception) {
                errors.incrementAndGet()
                logger.log(Level.SEVERE, "Failed to subscribe to '${addr.natsSubject}' for '${device.name}': ${e.message}", e)
            }
        }
    }

    private fun setupCoreNatsSubscription(addr: NatsClientAddress, conn: Connection) {
        val dispatcher = conn.createDispatcher { msg ->
            try {
                val mqttTopic = addr.natsToMqttTopic(msg.subject)
                val brokerMsg = BrokerMessage(
                    messageId = 0,
                    topicName = mqttTopic,
                    payload = msg.data ?: ByteArray(0),
                    qosLevel = addr.qos,
                    isRetain = false,
                    isDup = false,
                    isQueued = false,
                    clientId = internalClientId,
                    senderId = internalClientId
                )
                Monster.getSessionHandler()?.publishMessage(brokerMsg)
                messagesIn.incrementAndGet()
            } catch (e: Exception) {
                errors.incrementAndGet()
                logger.warning("Error forwarding NATS→MQTT for '${device.name}': ${e.message}")
            }
        }
        dispatcher.subscribe(addr.natsSubject)
        dispatchers.add(dispatcher)
    }

    private fun setupJetStreamSubscription(addr: NatsClientAddress, conn: Connection) {
        val js = jetStream ?: return

        val consumerConfigBuilder = ConsumerConfiguration.builder()
            .filterSubject(addr.natsSubject)

        if (!cfg.consumerDurableName.isNullOrBlank()) {
            consumerConfigBuilder.durable(cfg.consumerDurableName)
        }

        val pushOptions = PushSubscribeOptions.builder()
            .stream(cfg.streamName)
            .configuration(consumerConfigBuilder.build())
            .build()

        // MessageHandler for JetStream messages – passed explicitly to js.subscribe()
        val handler = io.nats.client.MessageHandler { msg ->
            try {
                val mqttTopic = addr.natsToMqttTopic(msg.subject)
                val brokerMsg = BrokerMessage(
                    messageId = 0,
                    topicName = mqttTopic,
                    payload = msg.data ?: ByteArray(0),
                    qosLevel = addr.qos,
                    isRetain = false,
                    isDup = false,
                    isQueued = false,
                    clientId = internalClientId,
                    senderId = internalClientId
                )
                Monster.getSessionHandler()?.publishMessage(brokerMsg)
                msg.ack()
                messagesIn.incrementAndGet()
            } catch (e: Exception) {
                errors.incrementAndGet()
                logger.warning("Error forwarding JetStream→MQTT for '${device.name}': ${e.message}")
                msg.nak()
            }
        }

        // Create dispatcher without a default handler; pass handler explicitly to js.subscribe()
        val dispatcher = conn.createDispatcher()
        val sub = js.subscribe(addr.natsSubject, dispatcher, handler, true, pushOptions)
        jetStreamSubscriptions.add(sub)
        dispatchers.add(dispatcher)
    }

    // ── Outbound direction: MQTT → NATS ──────────────────────────────────

    /**
     * Registers an internal MQTT subscription for every PUBLISH address so that
     * local MQTT messages are forwarded to NATS.
     */
    private fun setupOutboundSubscriptions() {
        val publishAddresses = cfg.addresses.filter { it.isPublish() }
        if (publishAddresses.isEmpty()) return

        // Consume local MQTT messages via the internal client's EventBus address
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

        // Register internal subscriptions with the broker session handler
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler != null) {
            publishAddresses.forEach { addr ->
                logger.info("Internal MQTT subscription for outbound bridge '$internalClientId' → topic '${addr.mqttTopic}'")
                sessionHandler.subscribeInternalClient(internalClientId, addr.mqttTopic, addr.qos)
            }
        } else {
            logger.severe("SessionHandler not available for outbound NATS subscriptions")
        }
    }

    private fun handleOutboundMessage(msg: BrokerMessage) {
        logger.finer { "Outbound message for '${device.name}': topic='${msg.topicName}' sender='${msg.senderId}' client='${msg.clientId}'" }

        // Loop prevention: skip messages we published ourselves
        if (msg.senderId == internalClientId || msg.clientId == internalClientId) return

        val conn = natsConnection
        if (conn == null || conn.status != Connection.Status.CONNECTED) {
            logger.fine { "NATS not connected, dropping outbound message on '${msg.topicName}'" }
            return
        }

        // Find the best matching PUBLISH address
        val matchingAddr = cfg.addresses
            .filter { it.isPublish() }
            .firstOrNull { addr -> mqttTopicMatchesFilter(msg.topicName, addr.mqttTopic) }
            ?: return

        try {
            val natsSubject = matchingAddr.mqttToNatsSubject(msg.topicName)

            if (cfg.useJetStream && jetStream != null) {
                jetStream!!.publish(natsSubject, msg.payload)
            } else {
                conn.publish(natsSubject, msg.payload)
            }
            messagesOut.incrementAndGet()
            logger.fine { "MQTT→NATS '${msg.topicName}' → '$natsSubject'" }
        } catch (e: Exception) {
            errors.incrementAndGet()
            logger.warning("Failed to publish to NATS subject for '${device.name}': ${e.message}")
        }
    }

    // ── Metrics endpoint ─────────────────────────────────────────────────

    private fun setupMetricsEndpoint() {
        val addr = EventBusAddresses.NatsBridge.connectorMetrics(device.name)
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
                )
            } catch (e: Exception) {
                msg.fail(500, e.message)
            }
        }
    }

    // ── Utilities ────────────────────────────────────────────────────────

    /**
     * MQTT topic filter matching (supports + and # wildcards).
     */
    private fun mqttTopicMatchesFilter(topic: String, filter: String): Boolean {
        if (filter == "#") return true
        val topicParts = topic.split("/")
        val filterParts = filter.split("/")
        return matchParts(topicParts, filterParts, 0, 0)
    }

    private fun matchParts(topic: List<String>, filter: List<String>, ti: Int, fi: Int): Boolean {
        if (fi == filter.size) return ti == topic.size
        val fp = filter[fi]
        return when {
            fp == "#" -> true
            ti == topic.size -> false
            fp == "+" || fp == topic[ti] -> matchParts(topic, filter, ti + 1, fi + 1)
            else -> false
        }
    }
}
