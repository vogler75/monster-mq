package at.rocworks.devices.mqttclient

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.queue.IMessageQueue
import at.rocworks.queue.MessageQueueDisk
import at.rocworks.queue.MessageQueueMemory
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.devices.MqttClientAddress
import at.rocworks.stores.devices.MqttClientConnectionConfig
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import org.eclipse.paho.client.mqttv3.*
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import java.util.concurrent.Callable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import java.util.logging.Logger
import javax.net.ssl.HostnameVerifier
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLSocketFactory
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager
import java.security.cert.X509Certificate
import org.eclipse.paho.client.mqttv3.MqttMessage as PahoMqttMessage

/**
 * MQTT Client Connector - Handles connection to a single remote MQTT broker
 *
 * Responsibilities:
 * - Maintains MQTT client connection to remote broker
 * - Handles subscriptions (bring data IN from remote)
 * - Handles publications (push data OUT to remote)
 * - Topic transformation with removePath logic
 * - Reconnection and error recovery
 */
class MqttClientConnector : AbstractVerticle() {

    // Metrics counters
    private val messagesInCounter = java.util.concurrent.atomic.AtomicLong(0) // remote -> local
    private val messagesOutCounter = java.util.concurrent.atomic.AtomicLong(0) // local -> remote
    private val messagesLoopSkippedCounter = java.util.concurrent.atomic.AtomicLong(0) // skipped due to loop prevention
    private val messagesQueuedCounter = java.util.concurrent.atomic.AtomicLong(0)
    private val bufferedPublishFailuresCounter = java.util.concurrent.atomic.AtomicLong(0)
    private var lastMetricsReset = System.currentTimeMillis()


    private val logger: Logger = Utils.getLogger(this::class.java)

    // Device configuration
    private lateinit var deviceConfig: DeviceConfig
    private lateinit var mqttConfig: MqttClientConnectionConfig

    // Paho MQTT client
    @Volatile
    private var client: MqttAsyncClient? = null

    // Connection state
    @Volatile
    private var isConnected = false
    @Volatile
    private var isReconnecting = false
    private var reconnectTimerId: Long? = null

    // Optional local-to-remote bridge buffer
    private var messageQueue: IMessageQueue? = null
    private val queueWriterStop = AtomicBoolean(false)
    private var queueWriterThread: Thread? = null

    // Track subscribed addresses
    private val subscribedAddresses = ConcurrentHashMap<String, MqttClientAddress>() // remoteTopic -> address

    // Track publish addresses
    private val publishAddresses = ConcurrentHashMap<String, MqttClientAddress>() // localTopic pattern -> address

    // EventBus consumer registration
    private var eventBusConsumerRegistration: io.vertx.core.eventbus.MessageConsumer<BrokerMessage>? = null

    override fun start(startPromise: Promise<Void>) {
        try {
            // Load device configuration
            val deviceJson = config().getJsonObject("device")
            deviceConfig = DeviceConfig.fromJsonObject(deviceJson)
            mqttConfig = MqttClientConnectionConfig.fromJsonObject(deviceConfig.config)

            logger.info("Starting MqttClientConnector for device: ${deviceConfig.name}")

            // Validate configuration
            val validationErrors = mqttConfig.validate()
            if (validationErrors.isNotEmpty()) {
                val errorMsg = "Invalid MQTT client configuration: ${validationErrors.joinToString(", ")}"
                logger.severe(errorMsg)
                startPromise.fail(errorMsg)
                return
            }

            // Start connector successfully regardless of initial connection status
            logger.info("MqttClientConnector for device ${deviceConfig.name} started successfully")

            initializeMessageBuffer()

            // Setup publish addresses (subscribe to local MQTT bus)
            setupPublishAddresses()

            // Start promise is completed immediately
            startPromise.complete()

            // Register metrics endpoint
            setupMetricsEndpoint()

            // Attempt initial connection in the background
            vertx.executeBlocking(Callable {
                try {
                    connectToRemoteBroker()
                } catch (e: Exception) {
                    logger.warning("Initial MQTT connection failed for device ${deviceConfig.name}: ${e.message}. Will retry automatically.")
                }
                null
            })

        } catch (e: Exception) {
            logger.severe("Exception during MqttClientConnector startup: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Stopping MqttClientConnector for device: ${deviceConfig.name}")

        // Cancel any pending reconnection timer
        reconnectTimerId?.let { timerId ->
            vertx.cancelTimer(timerId)
            reconnectTimerId = null
            logger.info("Cancelled pending reconnection timer for device ${deviceConfig.name}")
        }

        queueWriterStop.set(true)
        queueWriterThread?.join(5000)

        // Unsubscribe from internal topics
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler != null) {
            val clientId = "mqttclient-${deviceConfig.name}"
            publishAddresses.values.forEach { address ->
                val topicFilter = address.localTopic
                logger.info("Unsubscribing internal client '$clientId' from local topic '$topicFilter'")
                sessionHandler.unsubscribeInternalClient(clientId, topicFilter)
            }
            // Unregister the client
            sessionHandler.unregisterInternalClient(clientId)
        }

        // Disconnect from remote broker
        vertx.executeBlocking(Callable {
            try {
                disconnectFromRemoteBroker()
            } catch (e: Exception) {
                logger.warning("Error during disconnect: ${e.message}")
            }
            null
        }).onComplete {
            try {
                messageQueue?.close()
            } catch (e: Exception) {
                logger.warning("Error closing MQTT bridge buffer queue: ${e.message}")
            }
            logger.info("MqttClientConnector for device ${deviceConfig.name} stopped")
            stopPromise.complete()
        }
    }

    private fun initializeMessageBuffer() {
        if (!mqttConfig.bufferEnabled) {
            logger.info("MQTT bridge message buffering is disabled for device ${deviceConfig.name}")
            return
        }

        val blockSize = minOf(100, maxOf(1, mqttConfig.bufferSize))
        messageQueue = if (mqttConfig.persistBuffer) {
            MessageQueueDisk(
                queueName = "mqtt-bridge",
                deviceName = deviceConfig.name,
                logger = logger,
                queueSize = mqttConfig.bufferSize,
                blockSize = blockSize,
                pollTimeout = 100L,
                diskPath = "./buffer/mqttbridge"
            )
        } else {
            MessageQueueMemory(
                logger = logger,
                queueSize = mqttConfig.bufferSize,
                blockSize = blockSize,
                pollTimeout = 100L
            )
        }

        startQueueWriter()
        logger.info(
            "MQTT bridge message buffering enabled for device ${deviceConfig.name}: " +
                "type=${if (mqttConfig.persistBuffer) "DISK" else "MEMORY"}, size=${mqttConfig.bufferSize}"
        )
    }

    private fun startQueueWriter() {
        queueWriterStop.set(false)
        queueWriterThread = thread(start = true, name = "mqtt-bridge-buffer-${deviceConfig.name}") {
            while (!queueWriterStop.get()) {
                try {
                    drainMessageBuffer()
                } catch (e: Exception) {
                    logger.warning("Error draining MQTT bridge buffer for device ${deviceConfig.name}: ${e.message}")
                    Thread.sleep(1000)
                }
            }
        }
    }

    private fun drainMessageBuffer() {
        val queue = messageQueue ?: return
        val messagesToPublish = mutableListOf<BrokerMessage>()
        val blockSize = queue.pollBlock { message ->
            messagesToPublish.add(message)
        }

        if (blockSize == 0) {
            Thread.sleep(10)
            return
        }

        if (!isConnected || client == null) {
            Thread.sleep(1000)
            return
        }

        var allPublished = true
        publishLoop@ for (localMessage in messagesToPublish) {
            val matches = publishAddresses.values.filter { address ->
                MqttTopicTransformer.matchesLocalPattern(localMessage.topicName, address.localTopic)
            }

            if (matches.isEmpty()) {
                logger.warning("Dropping buffered MQTT bridge message for ${localMessage.topicName}: no publish address matches anymore")
                continue
            }

            for (address in matches) {
                val remoteTopic = MqttTopicTransformer.localToRemote(localMessage.topicName, address)
                if (remoteTopic == null) {
                    logger.warning("Dropping buffered MQTT bridge message for ${localMessage.topicName}: cannot transform topic")
                    continue
                }

                val published = publishToRemoteBrokerAwait(remoteTopic, localMessage, address)
                if (!published) {
                    allPublished = false
                    bufferedPublishFailuresCounter.incrementAndGet()
                    break@publishLoop
                }
            }
        }

        if (allPublished) {
            queue.pollCommit()
        } else {
            Thread.sleep(1000)
        }
    }

    private fun connectToRemoteBroker() {
        if (isConnected || isReconnecting) {
            return
        }

        isReconnecting = true

        try {
            logger.info("Connecting to remote MQTT broker: ${mqttConfig.brokerUrl}")

            // Create Paho MQTT client
            val persistence = MemoryPersistence()
            client = MqttAsyncClient(mqttConfig.brokerUrl, mqttConfig.clientId, persistence)

            // Configure connection options
            val connOpts = MqttConnectOptions().apply {
                isCleanSession = mqttConfig.cleanSession
                keepAliveInterval = mqttConfig.keepAlive
                connectionTimeout = (mqttConfig.connectionTimeout / 1000).toInt()
                isAutomaticReconnect = false // We handle reconnection manually

                // Set credentials if provided
                if (mqttConfig.username != null) {
                    userName = mqttConfig.username
                    password = mqttConfig.password?.toCharArray() ?: charArrayOf()
                }

                // Set SSL/TLS for secure protocols (ssl:// and wss://)
                val protocol = mqttConfig.getProtocol()
                if (protocol == MqttClientConnectionConfig.PROTOCOL_SSL ||
                    protocol == MqttClientConnectionConfig.PROTOCOL_WSS) {
                    if (mqttConfig.sslVerifyCertificate) {
                        // Use default SSL socket factory with certificate verification
                        socketFactory = SSLSocketFactory.getDefault() as SSLSocketFactory
                    } else {
                        // Disable certificate and hostname verification (INSECURE!)
                        logger.warning("SSL certificate verification is DISABLED for ${deviceConfig.name}. This is not recommended for production!")
                        socketFactory = createTrustAllSslSocketFactory()
                        sslHostnameVerifier = HostnameVerifier { _, _ -> true }
                    }
                }
            }

            // Set callback for connection events
            client!!.setCallback(object : MqttCallback {
                override fun connectionLost(cause: Throwable?) {
                    vertx.runOnContext {
                        logger.warning("Connection lost to remote broker ${deviceConfig.name}: ${cause?.message}")
                        isConnected = false
                        scheduleReconnection()
                    }
                }

                override fun messageArrived(topic: String, message: PahoMqttMessage) {
                    // Per-subscription IMqttMessageListener handles routing; this is a no-op fallback
                }

                override fun deliveryComplete(token: IMqttDeliveryToken?) {
                    // Not used for our purposes
                }
            })

            // Connect
            client!!.connect(connOpts, null, object : IMqttActionListener {
                override fun onSuccess(asyncActionToken: IMqttToken?) {
                    vertx.runOnContext {
                        isConnected = true
                        isReconnecting = false
                        logger.info("Connected to remote MQTT broker: ${mqttConfig.brokerUrl}")

                        // Setup subscriptions
                        setupSubscriptions()
                    }
                }

                override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
                    vertx.runOnContext {
                        isReconnecting = false
                        logger.severe("Failed to connect to remote MQTT broker: ${exception?.message}")
                        logger.severe("Exception details: ${exception?.toString()}")
                        exception?.printStackTrace()
                        scheduleReconnection()
                    }
                }
            })

        } catch (e: Exception) {
            isReconnecting = false
            logger.severe("Exception during MQTT connection: ${e.message}")
            scheduleReconnection()
        }
    }

    private fun disconnectFromRemoteBroker() {
        // Cancel any pending reconnection timer
        reconnectTimerId?.let { timerId ->
            vertx.cancelTimer(timerId)
            reconnectTimerId = null
        }

        if (client != null && isConnected) {
            try {
                client!!.disconnect()
                isConnected = false
                isReconnecting = false
                client = null
                subscribedAddresses.clear()
                logger.info("Disconnected from remote broker for device ${deviceConfig.name}")
            } catch (e: Exception) {
                logger.warning("Error disconnecting from remote broker: ${e.message}")
            }
        } else {
            isConnected = false
            isReconnecting = false
            client = null
            subscribedAddresses.clear()
        }
    }

    private fun scheduleReconnection() {
        if (!isReconnecting && reconnectTimerId == null) {
            reconnectTimerId = vertx.setTimer(mqttConfig.reconnectDelay) {
                reconnectTimerId = null
                if (!isConnected) {
                    logger.info("Attempting to reconnect to remote MQTT broker for device ${deviceConfig.name}...")
                    vertx.executeBlocking(Callable {
                        try {
                            connectToRemoteBroker()
                        } catch (e: Exception) {
                            logger.warning("Reconnection attempt failed: ${e.message}")
                        }
                        null
                    })
                }
            }
            logger.info("Scheduled reconnection for device ${deviceConfig.name} in ${mqttConfig.reconnectDelay}ms")
        }
    }

    private fun setupSubscriptions() {
        // Setup all SUBSCRIBE mode addresses
        val subscribeAddresses = mqttConfig.addresses.filter { it.isSubscribe() }

        if (subscribeAddresses.isEmpty()) {
            logger.info("No subscribe addresses configured for device ${deviceConfig.name}")
            return
        }

        logger.info("Setting up ${subscribeAddresses.size} subscriptions for device ${deviceConfig.name}")

        try {
            subscribeAddresses.forEach { address ->
                val topic = address.remoteTopic
                val qos = address.qos

                client!!.subscribe(topic, qos, null, object : IMqttActionListener {
                    override fun onSuccess(asyncActionToken: IMqttToken?) {
                        vertx.runOnContext {
                            subscribedAddresses[topic] = address
                            logger.info("Subscribed to remote topic: $topic with QoS $qos for device ${deviceConfig.name}")
                        }
                    }

                    override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
                        vertx.runOnContext {
                            logger.severe("Failed to subscribe to remote topic $topic: ${exception?.message}")
                        }
                    }
                }, IMqttMessageListener { receivedTopic, message ->
                    vertx.runOnContext {
                        publishRemoteMessageLocally(receivedTopic, message, address)
                    }
                })
            }
        } catch (e: Exception) {
            logger.severe("Error setting up subscriptions: ${e.message}")
        }
    }

    private fun setupPublishAddresses() {
        // Setup all PUBLISH mode addresses
        val publishAddrs = mqttConfig.addresses.filter { it.isPublish() }

        if (publishAddrs.isEmpty()) {
            logger.info("No publish addresses configured for device ${deviceConfig.name}")
            return
        }

        logger.info("Setting up ${publishAddrs.size} publish addresses for device ${deviceConfig.name}")

        // Store publish addresses for matching
        publishAddrs.forEach { address ->
            publishAddresses[address.localTopic] = address
        }

        // Register eventBus consumer for this MQTT client (handles both individual and bulk messages)
        val clientId = "mqttclient-${deviceConfig.name}"
        vertx.eventBus().consumer<Any>(EventBusAddresses.Client.messages(clientId)) { busMessage ->
            try {
                when (val body = busMessage.body()) {
                    is BrokerMessage -> handleLocalMqttMessage(body)
                    is at.rocworks.data.BulkClientMessage -> body.messages.forEach { handleLocalMqttMessage(it) }
                    else -> logger.warning("Unknown message type: ${body?.javaClass?.simpleName}")
                }
            } catch (e: Exception) {
                logger.warning("Error processing local MQTT message: ${e.message}")
            }
        }

        // Subscribe to local topics using internal subscription mechanism
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler != null) {
            publishAddrs.forEach { address ->
                val topicFilter = address.localTopic
                val qos = address.qos
                logger.info("Internal subscription for MQTT client '$clientId' to local topic '$topicFilter' with QoS $qos")

                sessionHandler.subscribeInternalClient(clientId, topicFilter, qos)
            }
        } else {
            logger.severe("SessionHandler not available for internal subscriptions")
        }
    }

    private fun publishRemoteMessageLocally(topic: String, message: PahoMqttMessage, address: MqttClientAddress) {
        try {
            val localTopic = MqttTopicTransformer.remoteToLocal(topic, address)
            val clientId = "mqttclient-${deviceConfig.name}"
            val localMessage = BrokerMessage(
                messageId = 0,
                topicName = localTopic,
                payload = message.payload,
                qosLevel = message.qos,
                isRetain = message.isRetained,
                isDup = message.isDuplicate,
                isQueued = false,
                clientId = clientId,
                senderId = clientId
            )
            val sessionHandler = Monster.getSessionHandler()
            if (sessionHandler != null) {
                sessionHandler.publishMessage(localMessage)
                messagesInCounter.incrementAndGet()
                logger.fine { "Forwarded remote message from $topic to local topic $localTopic" }
            } else {
                logger.warning("SessionHandler not available for message publishing")
            }
        } catch (e: Exception) {
            logger.severe("Error handling remote message: ${e.message}")
        }
    }

    private fun handleLocalMqttMessage(localMessage: BrokerMessage) {
        try {
            // LOOP PREVENTION: Skip if message originated from this bridge and loop prevention is enabled
            if (mqttConfig.loopPrevention) {
                val bridgeClientId = "mqttclient-${deviceConfig.name}"
                if (localMessage.senderId == bridgeClientId || localMessage.clientId == bridgeClientId) {
                    logger.fine { "Skipping message from ${localMessage.topicName} - originated from bridge itself (clientId: $bridgeClientId)" }
                    messagesLoopSkippedCounter.incrementAndGet()
                    return
                }
            }

            // Check each publish address to see if this message matches
            publishAddresses.values.forEach { address ->
                if (MqttTopicTransformer.matchesLocalPattern(localMessage.topicName, address.localTopic)) {
                    // Transform topic from local to remote
                    val remoteTopic = MqttTopicTransformer.localToRemote(localMessage.topicName, address)

                    if (remoteTopic != null) {
                        if (mqttConfig.bufferEnabled) {
                            enqueueLocalMessage(localMessage)
                            return
                        }
                        publishToRemoteBroker(remoteTopic, localMessage, address)
                    }
                }
            }
        } catch (e: Exception) {
            logger.severe("Error handling local MQTT message: ${e.message}")
        }
    }

    private fun enqueueLocalMessage(localMessage: BrokerMessage) {
        val queue = messageQueue
        if (queue == null) {
            logger.warning("MQTT bridge buffering enabled but queue is not initialized; dropping ${localMessage.topicName}")
            return
        }

        queue.add(localMessage)
        messagesQueuedCounter.incrementAndGet()
        logger.finest { "Queued local MQTT message ${localMessage.topicName} for remote bridge ${deviceConfig.name}" }
    }

    private fun publishToRemoteBroker(remoteTopic: String, localMessage: BrokerMessage, address: MqttClientAddress) {
        if (!isConnected || client == null) {
            logger.fine { "Not connected to remote broker, skipping publish to $remoteTopic" }
            return
        }

        try {
            // Use configured QoS from address, but respect the message QoS if it's lower
            val effectiveQos = minOf(address.qos, localMessage.qosLevel)

            val message = PahoMqttMessage(localMessage.payload).apply {
                qos = effectiveQos
                isRetained = localMessage.isRetain
            }

            client!!.publish(remoteTopic, message, null, object : IMqttActionListener {
                override fun onSuccess(asyncActionToken: IMqttToken?) {
                    messagesOutCounter.incrementAndGet()
                    logger.fine { "Published message to remote topic: $remoteTopic with QoS $effectiveQos" }
                }

                override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
                    logger.warning("Failed to publish to remote topic $remoteTopic: ${exception?.message}")
                }
            })
        } catch (e: Exception) {
            logger.severe("Error publishing to remote broker: ${e.message}")
        }
    }

    private fun publishToRemoteBrokerAwait(remoteTopic: String, localMessage: BrokerMessage, address: MqttClientAddress): Boolean {
        if (!isConnected || client == null) {
            logger.fine { "Not connected to remote broker, retaining buffered publish to $remoteTopic" }
            return false
        }

        return try {
            val effectiveQos = minOf(address.qos, localMessage.qosLevel)
            val message = PahoMqttMessage(localMessage.payload).apply {
                qos = effectiveQos
                isRetained = localMessage.isRetain
            }
            val future = CompletableFuture<Boolean>()
            client!!.publish(remoteTopic, message, null, object : IMqttActionListener {
                override fun onSuccess(asyncActionToken: IMqttToken?) {
                    messagesOutCounter.incrementAndGet()
                    logger.fine { "Published buffered message to remote topic: $remoteTopic with QoS $effectiveQos" }
                    future.complete(true)
                }

                override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
                    logger.warning("Failed to publish buffered message to remote topic $remoteTopic: ${exception?.message}")
                    future.complete(false)
                }
            })
            future.get(30, TimeUnit.SECONDS)
        } catch (e: Exception) {
            logger.warning("Error publishing buffered message to remote broker: ${e.message}")
            false
        }
    }

    private fun setupMetricsEndpoint() {
        val addr = EventBusAddresses.MqttBridge.connectorMetrics(deviceConfig.name)
        vertx.eventBus().consumer<io.vertx.core.json.JsonObject>(addr) { msg ->
            try {
                val now = System.currentTimeMillis()
                val elapsedMs = now - lastMetricsReset
                val elapsedSec = if (elapsedMs > 0) elapsedMs / 1000.0 else 1.0
                val inCount = messagesInCounter.getAndSet(0)
                val outCount = messagesOutCounter.getAndSet(0)
                val loopSkippedCount = messagesLoopSkippedCounter.getAndSet(0)
                val queuedCount = messagesQueuedCounter.getAndSet(0)
                val publishFailureCount = bufferedPublishFailuresCounter.getAndSet(0)
                lastMetricsReset = now
                val json = io.vertx.core.json.JsonObject()
                    .put("device", deviceConfig.name)
                    .put("messagesInRate", inCount / elapsedSec)
                    .put("messagesOutRate", outCount / elapsedSec)
                    .put("messagesLoopSkipped", loopSkippedCount)
                    .put("messagesQueued", queuedCount)
                    .put("bufferedPublishFailures", publishFailureCount)
                    .put("bufferEnabled", mqttConfig.bufferEnabled)
                    .put("bufferPersisted", mqttConfig.persistBuffer)
                    .put("bufferSize", messageQueue?.getSize() ?: 0)
                    .put("bufferCapacity", messageQueue?.getCapacity() ?: 0)
                    .put("bufferFull", messageQueue?.isQueueFull() ?: false)
                    .put("elapsedMs", elapsedMs)
                msg.reply(json)
            } catch (e: Exception) {
                msg.fail(500, e.message)
            }
        }
        logger.info("Registered metrics endpoint for device ${deviceConfig.name} at address $addr")
    }

    /**
     * Creates an SSL socket factory that trusts all certificates and disables hostname verification (INSECURE)
     * This should only be used for development/testing with self-signed certificates
     */
    private fun createTrustAllSslSocketFactory(): SSLSocketFactory {
        // Create a custom X509ExtendedTrustManager instead of X509TrustManager to prevent wrapping
        val trustAllCerts = arrayOf<TrustManager>(object : javax.net.ssl.X509ExtendedTrustManager() {
            override fun checkClientTrusted(chain: Array<X509Certificate>, authType: String) {}
            override fun checkServerTrusted(chain: Array<X509Certificate>, authType: String) {}
            override fun checkClientTrusted(chain: Array<X509Certificate>, authType: String, socket: java.net.Socket) {}
            override fun checkServerTrusted(chain: Array<X509Certificate>, authType: String, socket: java.net.Socket) {}
            override fun checkClientTrusted(chain: Array<X509Certificate>, authType: String, engine: javax.net.ssl.SSLEngine) {}
            override fun checkServerTrusted(chain: Array<X509Certificate>, authType: String, engine: javax.net.ssl.SSLEngine) {}
            override fun getAcceptedIssuers(): Array<X509Certificate> = arrayOf()
        })

        val sslContext = SSLContext.getInstance("TLS")
        sslContext.init(null, trustAllCerts, java.security.SecureRandom())

        // Wrap the factory to disable endpoint identification on all created sockets
        val delegate = sslContext.socketFactory
        return object : SSLSocketFactory() {
            override fun getDefaultCipherSuites() = delegate.defaultCipherSuites
            override fun getSupportedCipherSuites() = delegate.supportedCipherSuites

            private fun configureSocket(socket: java.net.Socket): java.net.Socket {
                if (socket is javax.net.ssl.SSLSocket) {
                    val params = socket.sslParameters
                    params.endpointIdentificationAlgorithm = null
                    socket.sslParameters = params
                    logger.finer("Configured SSL socket with disabled endpoint identification")
                }
                return socket
            }

            override fun createSocket(): java.net.Socket {
                return configureSocket(delegate.createSocket())
            }

            override fun createSocket(s: java.net.Socket?, host: String?, port: Int, autoClose: Boolean): java.net.Socket {
                return configureSocket(delegate.createSocket(s, host, port, autoClose))
            }

            override fun createSocket(host: String?, port: Int) =
                configureSocket(delegate.createSocket(host, port))

            override fun createSocket(host: String?, port: Int, localHost: java.net.InetAddress?, localPort: Int) =
                configureSocket(delegate.createSocket(host, port, localHost, localPort))

            override fun createSocket(host: java.net.InetAddress?, port: Int) =
                configureSocket(delegate.createSocket(host, port))

            override fun createSocket(address: java.net.InetAddress?, port: Int, localAddress: java.net.InetAddress?, localPort: Int) =
                configureSocket(delegate.createSocket(address, port, localAddress, localPort))
        }
    }
}
