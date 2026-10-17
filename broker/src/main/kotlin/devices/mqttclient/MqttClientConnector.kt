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
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.MqttMessage as PahoMqttMessage
import org.eclipse.paho.mqttv5.client.MqttAsyncClient as PahoMqtt5AsyncClient
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions as PahoMqtt5ConnectionOptions
import org.eclipse.paho.mqttv5.client.DisconnectedBufferOptions as PahoMqtt5BufferOptions
import org.eclipse.paho.mqttv5.client.persist.MqttDefaultFilePersistence as PahoMqtt5FilePersistence
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence as PahoMqtt5MemoryPersistence
import org.eclipse.paho.mqttv5.common.MqttMessage as PahoMqtt5Message
import org.eclipse.paho.mqttv5.common.MqttSubscription as PahoMqtt5Subscription
import org.eclipse.paho.mqttv5.common.packet.MqttProperties as PahoMqtt5Properties
import org.eclipse.paho.mqttv5.common.packet.UserProperty as PahoMqtt5UserProperty
import org.eclipse.paho.mqttv5.client.MqttActionListener as PahoMqtt5ActionListener
import org.eclipse.paho.mqttv5.client.IMqttMessageListener as PahoMqtt5MessageListener
import org.eclipse.paho.mqttv5.client.IMqttToken as PahoMqtt5Token
import org.eclipse.paho.mqttv5.client.MqttCallback as PahoMqtt5Callback
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse as PahoMqtt5DisconnectResponse
import java.time.Instant
import java.util.concurrent.Callable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import java.util.logging.Logger
import javax.net.ssl.HostnameVerifier
import javax.net.ssl.SSLSocketFactory

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

    // Effective protocol version: starts at mqttConfig.protocolVersion (default 4), falls back to 4 if 5 fails on v3 broker
    @Volatile
    private var effectiveProtocolVersion: Int = 4
    private val fallbackTriggered = AtomicBoolean(false)
    private val sawV3Connack = AtomicBoolean(false)
    @Volatile
    private var remoteMaximumPacketSize: Long? = null

    // Paho MQTT clients
    @Volatile
    private var client3: MqttAsyncClient? = null
    @Volatile
    private var client5: PahoMqtt5AsyncClient? = null

    // Connection state
    @Volatile
    private var isConnected = false
    @Volatile
    private var isReconnecting = false
    private val isStopped = AtomicBoolean(false)
    private var reconnectTimerId: Long? = null

    // Custom TLS setup (CA, client certificate, ALPN, SNI); null uses the JVM default
    private var sslSocketFactory: SSLSocketFactory? = null

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
        isStopped.set(false)
        try {
            // Load device configuration
            val deviceJson = config().getJsonObject("device")
            deviceConfig = DeviceConfig.fromJsonObject(deviceJson)
            mqttConfig = MqttClientConnectionConfig.fromJsonObject(deviceConfig.config)
            effectiveProtocolVersion = mqttConfig.protocolVersion
            fallbackTriggered.set(false)
            sawV3Connack.set(false)

            logger.info("Starting MqttClientConnector for device: ${deviceConfig.name} (protocolVersion $effectiveProtocolVersion)")

            // Validate configuration
            val validationErrors = mqttConfig.validate()
            if (validationErrors.isNotEmpty()) {
                val errorMsg = "Invalid MQTT client configuration: ${validationErrors.joinToString(", ")}"
                logger.severe(errorMsg)
                startPromise.fail(errorMsg)
                return
            }

            val protocol = mqttConfig.getProtocol()
            if (protocol == MqttClientConnectionConfig.PROTOCOL_SSL || protocol == MqttClientConnectionConfig.PROTOCOL_WSS) {
                try {
                    sslSocketFactory = MqttClientTls.buildSocketFactory(mqttConfig)
                } catch (e: MqttClientTlsException) {
                    val errorMsg = "Invalid MQTT client TLS configuration: ${e.message}"
                    logger.severe(errorMsg)
                    startPromise.fail(errorMsg)
                    return
                }
                if (!mqttConfig.sslVerifyCertificate) {
                    logger.warning("SSL certificate verification is DISABLED for ${deviceConfig.name}. This is not recommended for production!")
                }
                if (mqttConfig.tlsClientCertPath != null) {
                    logger.info("Using client certificate ${mqttConfig.tlsClientCertPath} for device ${deviceConfig.name}")
                }
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
                if (isStopped.get()) return@Callable null
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
        isStopped.set(true)
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

    private fun useMonsterBuffer(): Boolean =
        mqttConfig.bufferEnabled && mqttConfig.bufferImplementation == MqttClientConnectionConfig.BUFFER_IMPLEMENTATION_MONSTER

    private fun usePahoBuffer(): Boolean =
        mqttConfig.bufferEnabled && mqttConfig.bufferImplementation == MqttClientConnectionConfig.BUFFER_IMPLEMENTATION_PAHO

    private fun initializeMessageBuffer() {
        if (!useMonsterBuffer()) {
            logger.info(
                "MQTT bridge Monster queue buffering is disabled for device ${deviceConfig.name} " +
                    "(bufferEnabled=${mqttConfig.bufferEnabled}, implementation=${mqttConfig.bufferImplementation})"
            )
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
                "implementation=MONSTER, type=${if (mqttConfig.persistBuffer) "DISK" else "MEMORY"}, size=${mqttConfig.bufferSize}"
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

        if (!isConnected || (client3 == null && client5 == null)) {
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
        if (isStopped.get() || isConnected || isReconnecting) {
            return
        }

        isReconnecting = true

        try {
            logger.info("Connecting to remote MQTT broker: ${mqttConfig.brokerUrl} (protocolVersion $effectiveProtocolVersion)")

            if (effectiveProtocolVersion == 5) {
                connectMqtt5()
            } else {
                connectMqtt3()
            }
        } catch (e: Exception) {
            isReconnecting = false
            logger.severe("Exception during MQTT connection: ${e.message}")
            if (!isStopped.get()) {
                scheduleReconnection()
            }
        }
    }

    private fun connectMqtt3() {
        // Create Paho MQTT client once so built-in buffered messages survive reconnect attempts.
        if (client3 == null) {
            val persistence = if (usePahoBuffer() && mqttConfig.persistBuffer) {
                MqttDefaultFilePersistence("./buffer/mqttbridge/paho")
            } else {
                MemoryPersistence()
            }
            client3 = MqttAsyncClient(mqttConfig.brokerUrl, mqttConfig.clientId, persistence)
        }

        if (isStopped.get()) {
            isReconnecting = false
            disconnectFromRemoteBroker()
            return
        }

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
                socketFactory = sslSocketFactory ?: (SSLSocketFactory.getDefault() as SSLSocketFactory)
                if (!mqttConfig.sslVerifyCertificate) {
                    sslHostnameVerifier = HostnameVerifier { _, _ -> true }
                }
            }
        }

        if (usePahoBuffer()) {
            val bufferOpts = DisconnectedBufferOptions().apply {
                isBufferEnabled = true
                bufferSize = mqttConfig.bufferSize
                isPersistBuffer = mqttConfig.persistBuffer
                isDeleteOldestMessages = mqttConfig.deleteOldestMessages
            }
            client3!!.setBufferOpts(bufferOpts)
            logger.info(
                "Configured Paho disconnected buffer for device ${deviceConfig.name}: " +
                    "size=${mqttConfig.bufferSize}, persisted=${mqttConfig.persistBuffer}, deleteOldest=${mqttConfig.deleteOldestMessages}"
            )
        }

        // Set callback for connection events
        client3!!.setCallback(object : MqttCallback {
            override fun connectionLost(cause: Throwable?) {
                if (isStopped.get()) return
                vertx.runOnContext {
                    if (isStopped.get()) return@runOnContext
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
        client3!!.connect(connOpts, null, object : IMqttActionListener {
            override fun onSuccess(asyncActionToken: IMqttToken?) {
                vertx.runOnContext {
                    if (isStopped.get()) {
                        isReconnecting = false
                        disconnectFromRemoteBroker()
                        return@runOnContext
                    }
                    isConnected = true
                    isReconnecting = false
                    logger.info("Connected to remote MQTT broker: ${mqttConfig.brokerUrl}")

                    // Setup subscriptions
                    setupSubscriptions3()
                }
            }

            override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
                vertx.runOnContext {
                    if (isStopped.get()) {
                        isReconnecting = false
                        return@runOnContext
                    }
                    isReconnecting = false
                    logger.severe("Failed to connect to remote MQTT broker: ${exception?.message}")
                    logger.severe("Exception details: ${exception?.toString()}")
                    exception?.printStackTrace()
                    scheduleReconnection()
                }
            }
        })
    }

    private fun connectMqtt5() {
        sawV3Connack.set(false)
        remoteMaximumPacketSize = null

        if (client5 == null) {
            val persistence = if (usePahoBuffer() && mqttConfig.persistBuffer) {
                PahoMqtt5FilePersistence("./buffer/mqttbridge/paho/v5")
            } else {
                PahoMqtt5MemoryPersistence()
            }
            client5 = PahoMqtt5AsyncClient(mqttConfig.brokerUrl, mqttConfig.clientId, persistence)
        }

        if (isStopped.get()) {
            isReconnecting = false
            disconnectFromRemoteBroker()
            return
        }

        val connOpts = PahoMqtt5ConnectionOptions().apply {
            if (mqttConfig.cleanSession) {
                isCleanStart = true
                sessionExpiryInterval = mqttConfig.sessionExpiryInterval ?: 0L
            } else {
                isCleanStart = false
                sessionExpiryInterval = mqttConfig.sessionExpiryInterval ?: 0xFFFFFFFFL
            }
            keepAliveInterval = mqttConfig.keepAlive
            connectionTimeout = (mqttConfig.connectionTimeout / 1000).toInt()
            isAutomaticReconnect = false

            if (mqttConfig.username != null) {
                userName = mqttConfig.username
                password = mqttConfig.password?.toByteArray(Charsets.UTF_8) ?: ByteArray(0)
            }

            mqttConfig.receiveMaximum?.let { receiveMaximum = it }
            mqttConfig.maximumPacketSize?.let { maximumPacketSize = it }
            mqttConfig.topicAliasMaximum?.let { topicAliasMaximum = it }

            val protocol = mqttConfig.getProtocol()
            if (protocol == MqttClientConnectionConfig.PROTOCOL_SSL || protocol == MqttClientConnectionConfig.PROTOCOL_WSS) {
                val baseSslFactory = sslSocketFactory ?: (SSLSocketFactory.getDefault() as SSLSocketFactory)
                socketFactory = SniffingSslSocketFactory(baseSslFactory) { head ->
                    if (isV3Connack(head)) sawV3Connack.set(true)
                }
                if (!mqttConfig.sslVerifyCertificate) {
                    sslHostnameVerifier = HostnameVerifier { _, _ -> true }
                }
            } else {
                socketFactory = SniffingSocketFactory(javax.net.SocketFactory.getDefault()) { head ->
                    if (isV3Connack(head)) sawV3Connack.set(true)
                }
            }
        }

        if (usePahoBuffer()) {
            val bufferOpts = PahoMqtt5BufferOptions().apply {
                isBufferEnabled = true
                bufferSize = mqttConfig.bufferSize
                isPersistBuffer = mqttConfig.persistBuffer
                isDeleteOldestMessages = mqttConfig.deleteOldestMessages
            }
            client5!!.setBufferOpts(bufferOpts)
            logger.info(
                "Configured Paho disconnected buffer (MQTT 5) for device ${deviceConfig.name}: " +
                    "size=${mqttConfig.bufferSize}, persisted=${mqttConfig.persistBuffer}, deleteOldest=${mqttConfig.deleteOldestMessages}"
            )
        }

        client5!!.setCallback(object : PahoMqtt5Callback {
            override fun disconnected(disconnectResponse: PahoMqtt5DisconnectResponse?) {
                if (isStopped.get()) return
                vertx.runOnContext {
                    if (isStopped.get()) return@runOnContext
                    val reason = disconnectResponse?.reasonString ?: disconnectResponse?.exception?.message ?: "unknown"
                    logger.warning("Connection lost to remote broker ${deviceConfig.name}: $reason")
                    isConnected = false
                    scheduleReconnection()
                }
            }

            override fun mqttErrorOccurred(exception: org.eclipse.paho.mqttv5.common.MqttException?) {
                logger.warning("MQTT error occurred for device ${deviceConfig.name}: ${exception?.message}")
            }

            override fun messageArrived(topic: String?, message: PahoMqtt5Message?) {}

            override fun deliveryComplete(token: PahoMqtt5Token?) {}

            override fun connectComplete(reconnect: Boolean, serverURI: String?) {}

            override fun authPacketArrived(reasonCode: Int, properties: PahoMqtt5Properties?) {}
        })

        client5!!.connect(connOpts, null, object : PahoMqtt5ActionListener {
            override fun onSuccess(asyncActionToken: PahoMqtt5Token?) {
                vertx.runOnContext {
                    if (isStopped.get()) {
                        isReconnecting = false
                        disconnectFromRemoteBroker()
                        return@runOnContext
                    }
                    sawV3Connack.set(false)
                    remoteMaximumPacketSize = asyncActionToken?.responseProperties?.maximumPacketSize
                    isConnected = true
                    isReconnecting = false
                    logger.info("Connected to remote MQTT broker (MQTT 5): ${mqttConfig.brokerUrl}")
                    setupSubscriptions5()
                }
            }

            override fun onFailure(asyncActionToken: PahoMqtt5Token?, exception: Throwable?) {
                vertx.runOnContext {
                    if (isStopped.get()) {
                        isReconnecting = false
                        return@runOnContext
                    }
                    isReconnecting = false

                    val isV3 = sawV3Connack.get() || isUnsupportedProtocolVersion(exception)
                    if (isV3 && effectiveProtocolVersion == 5 && fallbackTriggered.compareAndSet(false, true)) {
                        val warning = "bridge remote does not support MQTT 5; using MQTT 3.1.1 (MQTT 5 properties, inbound retain flag and No Local are not available)"
                        if (usePahoBuffer()) {
                            logger.warning("$warning for device ${deviceConfig.name}: messages queued in the MQTT 5 PAHO buffer before the first connect are dropped")
                        } else {
                            logger.warning("$warning for device ${deviceConfig.name}")
                        }
                        effectiveProtocolVersion = 4
                        vertx.executeBlocking(Callable {
                            try {
                                client5?.close(true)
                            } catch (e: Exception) {
                                logger.fine { "Error closing v5 client during fallback: ${e.message}" }
                            }
                            client5 = null
                            null
                        }).onComplete {
                            vertx.executeBlocking(Callable {
                                if (isStopped.get()) return@Callable null
                                try {
                                    connectToRemoteBroker()
                                } catch (e: Exception) {
                                    logger.warning("Fallback connection to MQTT 3.1.1 failed for device ${deviceConfig.name}: ${e.message}. Will retry automatically.")
                                    scheduleReconnection()
                                }
                                null
                            })
                        }
                        return@runOnContext
                    }

                    logger.severe("Failed to connect to remote MQTT broker: ${exception?.message}")
                    logger.severe("Exception details: ${exception?.toString()}")
                    exception?.printStackTrace()
                    scheduleReconnection()
                }
            }
        })
    }

    private fun disconnectFromRemoteBroker() {
        // Cancel any pending reconnection timer
        reconnectTimerId?.let { timerId ->
            vertx.cancelTimer(timerId)
            reconnectTimerId = null
        }

        val c3 = client3
        client3 = null
        val c5 = client5
        client5 = null
        isConnected = false
        isReconnecting = false
        subscribedAddresses.clear()

        if (c3 != null) {
            try {
                c3.setCallback(null)
            } catch (e: Exception) {
                logger.warning("Error clearing MQTT callback for device ${deviceConfig.name}: ${e.message}")
            }

            try {
                if (c3.isConnected) {
                    c3.disconnectForcibly(1000, 2000)
                    logger.info("Disconnected from remote broker (MQTT 3.1.1) for device ${deviceConfig.name}")
                }
            } catch (e: Exception) {
                logger.warning("Error disconnecting from remote broker: ${e.message}")
            }

            try {
                c3.close(true)
                logger.fine("Closed MQTT 3.1.1 client for device ${deviceConfig.name}")
            } catch (e: Exception) {
                logger.warning("Error closing MQTT client: ${e.message}")
            }
        }

        if (c5 != null) {
            try {
                c5.setCallback(null)
            } catch (e: Exception) {
                logger.warning("Error clearing MQTT 5 callback for device ${deviceConfig.name}: ${e.message}")
            }

            try {
                if (c5.isConnected) {
                    c5.disconnectForcibly(1000, 2000, false)
                    logger.info("Disconnected from remote broker (MQTT 5) for device ${deviceConfig.name}")
                }
            } catch (e: Exception) {
                logger.warning("Error disconnecting from remote broker: ${e.message}")
            }

            try {
                c5.close(true)
                logger.fine("Closed MQTT 5 client for device ${deviceConfig.name}")
            } catch (e: Exception) {
                logger.warning("Error closing MQTT 5 client: ${e.message}")
            }
        }
    }

    private fun scheduleReconnection() {
        if (isStopped.get()) return
        if (!isReconnecting && reconnectTimerId == null) {
            reconnectTimerId = vertx.setTimer(mqttConfig.reconnectDelay) {
                reconnectTimerId = null
                if (isStopped.get()) return@setTimer
                if (!isConnected) {
                    logger.info("Attempting to reconnect to remote MQTT broker for device ${deviceConfig.name}...")
                    vertx.executeBlocking(Callable {
                        if (isStopped.get()) return@Callable null
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

    private fun setupSubscriptions3() {
        if (isStopped.get()) return
        val subscribeAddresses = mqttConfig.addresses.filter { it.isSubscribe() }

        if (subscribeAddresses.isEmpty()) {
            logger.info("No subscribe addresses configured for device ${deviceConfig.name}")
            return
        }

        logger.info("Setting up ${subscribeAddresses.size} subscriptions (MQTT 3.1.1) for device ${deviceConfig.name}")

        try {
            subscribeAddresses.forEach { address ->
                val topic = address.remoteTopic
                val qos = subscriptionQos(address)

                client3!!.subscribe(topic, qos, null, object : IMqttActionListener {
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
                        publishRemoteMessageLocally3(receivedTopic, message, address)
                    }
                })
            }
        } catch (e: Exception) {
            logger.severe("Error setting up subscriptions: ${e.message}")
        }
    }

    private fun setupSubscriptions5() {
        if (isStopped.get()) return
        val subscribeAddresses = mqttConfig.addresses.filter { it.isSubscribe() }

        if (subscribeAddresses.isEmpty()) {
            logger.info("No subscribe addresses configured for device ${deviceConfig.name}")
            return
        }

        logger.info("Setting up ${subscribeAddresses.size} subscriptions (MQTT 5) for device ${deviceConfig.name}")

        try {
            subscribeAddresses.forEach { address ->
                val topic = address.remoteTopic
                val qos = subscriptionQos(address)
                val subscription = PahoMqtt5Subscription(topic, qos).apply {
                    isNoLocal = !MqttTopicTransformer.isSharedSubscription(topic)
                    isRetainAsPublished = true
                }

                client5!!.subscribe(arrayOf(subscription), null, object : PahoMqtt5ActionListener {
                    override fun onSuccess(asyncActionToken: PahoMqtt5Token?) {
                        vertx.runOnContext {
                            subscribedAddresses[topic] = address
                            logger.info("Subscribed to remote topic: $topic with QoS $qos for device ${deviceConfig.name}")
                        }
                    }

                    override fun onFailure(asyncActionToken: PahoMqtt5Token?, exception: Throwable?) {
                        vertx.runOnContext {
                            logger.severe("Failed to subscribe to remote topic $topic: ${exception?.message}")
                        }
                    }
                }, arrayOf(PahoMqtt5MessageListener { receivedTopic, message ->
                    vertx.runOnContext {
                        publishRemoteMessageLocally5(receivedTopic, message, address)
                    }
                }), PahoMqtt5Properties())
            }
        } catch (e: Exception) {
            logger.severe("Error setting up subscriptions (MQTT 5): ${e.message}")
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
                val qos = subscriptionQos(address)
                logger.info("Internal subscription for MQTT client '$clientId' to local topic '$topicFilter' with QoS $qos")

                sessionHandler.subscribeInternalClient(clientId, topicFilter, qos)
            }
        } else {
            logger.severe("SessionHandler not available for internal subscriptions")
        }
    }

    private fun publishRemoteMessageLocally3(topic: String, message: PahoMqttMessage, address: MqttClientAddress) {
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
            forwardLocalMessage(localMessage, topic, localTopic)
        } catch (e: Exception) {
            logger.severe("Error handling remote message (MQTT 3.1.1): ${e.message}")
        }
    }

    private fun publishRemoteMessageLocally5(topic: String, message: PahoMqtt5Message, address: MqttClientAddress) {
        try {
            val localTopic = MqttTopicTransformer.remoteToLocal(topic, address)
            val clientId = "mqttclient-${deviceConfig.name}"
            val props = message.properties
            val userProps = props?.userProperties?.takeIf { it.isNotEmpty() }?.let { list ->
                val map = LinkedHashMap<String, String>()
                list.forEach { prop -> map[prop.key] = prop.value }
                map
            }

            val localMessage = BrokerMessage(
                messageId = 0,
                topicName = localTopic,
                payload = message.payload,
                qosLevel = message.qos,
                isRetain = message.isRetained,
                isDup = message.isDuplicate,
                isQueued = false,
                clientId = clientId,
                senderId = clientId,
                time = Instant.now(),
                messageExpiryInterval = props?.messageExpiryInterval,
                payloadFormatIndicator = if (props != null && props.payloadFormat) 1 else null,
                contentType = props?.contentType,
                responseTopic = props?.responseTopic,
                correlationData = props?.correlationData,
                userProperties = userProps
            )
            forwardLocalMessage(localMessage, topic, localTopic)
        } catch (e: Exception) {
            logger.severe("Error handling remote message (MQTT 5): ${e.message}")
        }
    }

    private fun forwardLocalMessage(localMessage: BrokerMessage, topic: String, localTopic: String) {
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler != null) {
            sessionHandler.publishMessage(localMessage)
            messagesInCounter.incrementAndGet()
            logger.fine { "Forwarded remote message from $topic to local topic $localTopic" }
        } else {
            logger.warning("SessionHandler not available for message publishing")
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

            // PeerLink gate: Skip replicas if bridgeOutbound is disabled (default)
            if (localMessage.peer != null && !(at.rocworks.Monster.getPeerLinkManager()?.config?.receive?.bridgeOutbound ?: false)) {
                return
            }

            // Check each publish address to see if this message matches
            publishAddresses.values.forEach { address ->
                if (MqttTopicTransformer.matchesLocalPattern(localMessage.topicName, address.localTopic)) {
                    // Transform topic from local to remote
                    val remoteTopic = MqttTopicTransformer.localToRemote(localMessage.topicName, address)

                    if (remoteTopic != null) {
                        if (useMonsterBuffer()) {
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

    private fun calculateRemainingExpiry(localMessage: BrokerMessage): Pair<Long?, Boolean> {
        val expiry = localMessage.messageExpiryInterval ?: return null to true
        val ageSeconds = java.time.Duration.between(localMessage.time, Instant.now()).seconds
        val remaining = expiry - maxOf(0L, ageSeconds)
        return if (remaining <= 0) {
            null to false // expired!
        } else {
            remaining to true
        }
    }

    private fun publishToRemoteBroker(remoteTopic: String, localMessage: BrokerMessage, address: MqttClientAddress) {
        if (!isConnected && !usePahoBuffer()) {
            logger.fine { "Not connected to remote broker, skipping publish to $remoteTopic" }
            return
        }

        val (remainingExp, isLive) = calculateRemainingExpiry(localMessage)
        if (!isLive) {
            logger.fine { "Dropping expired MQTT bridge message for $remoteTopic" }
            return
        }

        val limit = remoteMaximumPacketSize
        if (limit != null && limit > 0 && localMessage.payload.size > limit) {
            logger.warning("Dropping bridge message for $remoteTopic: payload size ${localMessage.payload.size} exceeds remote maximum packet size $limit")
            return
        }

        if (effectiveProtocolVersion == 5) {
            val c5 = client5 ?: return
            try {
                val effectiveQos = effectivePublishQos(address, localMessage)
                val retain = localMessage.isRetain
                val message = PahoMqtt5Message(localMessage.payload).apply {
                    qos = effectiveQos
                    isRetained = retain
                    properties = PahoMqtt5Properties().apply {
                        localMessage.contentType?.let { contentType = it }
                        localMessage.responseTopic?.let { responseTopic = it }
                        localMessage.correlationData?.let { correlationData = it }
                        remainingExp?.let { messageExpiryInterval = it }
                        if (localMessage.payloadFormatIndicator == 1) {
                            payloadFormat = true
                        }
                        localMessage.userProperties?.takeIf { it.isNotEmpty() }?.let { map ->
                            userProperties = map.map { (k, v) -> PahoMqtt5UserProperty(k, v) }
                        }
                    }
                }

                c5.publish(remoteTopic, message, null, object : PahoMqtt5ActionListener {
                    override fun onSuccess(asyncActionToken: PahoMqtt5Token?) {
                        messagesOutCounter.incrementAndGet()
                        logger.fine { "Published message to remote topic: $remoteTopic with QoS $effectiveQos" }
                    }

                    override fun onFailure(asyncActionToken: PahoMqtt5Token?, exception: Throwable?) {
                        if (exception is org.eclipse.paho.mqttv5.common.MqttException && exception.reasonCode >= 0x80) {
                            logger.warning("Remote broker refused publish to $remoteTopic (reason 0x${Integer.toHexString(exception.reasonCode)}); dropping message")
                        } else {
                            logger.warning("Failed to publish to remote topic $remoteTopic: ${exception?.message}")
                        }
                    }
                })
            } catch (e: Exception) {
                logger.severe("Error publishing to remote broker (MQTT 5): ${e.message}")
            }
        } else {
            val c3 = client3 ?: return
            try {
                val effectiveQos = effectivePublishQos(address, localMessage)
                val message = PahoMqttMessage(localMessage.payload).apply {
                    qos = effectiveQos
                    isRetained = localMessage.isRetain
                }

                c3.publish(remoteTopic, message, null, object : IMqttActionListener {
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
    }

    private fun publishToRemoteBrokerAwait(remoteTopic: String, localMessage: BrokerMessage, address: MqttClientAddress): Boolean {
        if (!isConnected) {
            logger.fine { "Not connected to remote broker, retaining buffered publish to $remoteTopic" }
            return false
        }

        val (remainingExp, isLive) = calculateRemainingExpiry(localMessage)
        if (!isLive) {
            logger.fine { "Dropping expired buffered MQTT bridge message for $remoteTopic" }
            return true // count as handled
        }

        val limit = remoteMaximumPacketSize
        if (limit != null && limit > 0 && localMessage.payload.size > limit) {
            logger.warning("Dropping buffered bridge message for $remoteTopic: payload size ${localMessage.payload.size} exceeds remote maximum packet size $limit")
            return true // count as handled, retrying would block buffer forever
        }

        if (effectiveProtocolVersion == 5) {
            val c5 = client5 ?: return false
            return try {
                val effectiveQos = effectivePublishQos(address, localMessage)
                val retain = localMessage.isRetain
                val message = PahoMqtt5Message(localMessage.payload).apply {
                    qos = effectiveQos
                    isRetained = retain
                    properties = PahoMqtt5Properties().apply {
                        localMessage.contentType?.let { contentType = it }
                        localMessage.responseTopic?.let { responseTopic = it }
                        localMessage.correlationData?.let { correlationData = it }
                        remainingExp?.let { messageExpiryInterval = it }
                        if (localMessage.payloadFormatIndicator == 1) {
                            payloadFormat = true
                        }
                        localMessage.userProperties?.takeIf { it.isNotEmpty() }?.let { map ->
                            userProperties = map.map { (k, v) -> PahoMqtt5UserProperty(k, v) }
                        }
                    }
                }
                val future = CompletableFuture<Boolean>()
                c5.publish(remoteTopic, message, null, object : PahoMqtt5ActionListener {
                    override fun onSuccess(asyncActionToken: PahoMqtt5Token?) {
                        messagesOutCounter.incrementAndGet()
                        logger.fine { "Published buffered message to remote topic: $remoteTopic with QoS $effectiveQos" }
                        future.complete(true)
                    }

                    override fun onFailure(asyncActionToken: PahoMqtt5Token?, exception: Throwable?) {
                        if (exception is org.eclipse.paho.mqttv5.common.MqttException && exception.reasonCode >= 0x80) {
                            logger.warning("Remote broker refused publish to $remoteTopic (reason 0x${Integer.toHexString(exception.reasonCode)}); dropping message")
                            future.complete(true)
                        } else {
                            logger.warning("Failed to publish buffered message to remote topic $remoteTopic: ${exception?.message}")
                            future.complete(false)
                        }
                    }
                })
                future.get(30, TimeUnit.SECONDS)
            } catch (e: Exception) {
                logger.warning("Error publishing buffered message to remote broker: ${e.message}")
                false
            }
        } else {
            val c3 = client3 ?: return false
            return try {
                val effectiveQos = effectivePublishQos(address, localMessage)
                val message = PahoMqttMessage(localMessage.payload).apply {
                    qos = effectiveQos
                    isRetained = localMessage.isRetain
                }
                val future = CompletableFuture<Boolean>()
                c3.publish(remoteTopic, message, null, object : IMqttActionListener {
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
    }

    private fun subscriptionQos(address: MqttClientAddress): Int =
        if (address.qos == MqttClientAddress.QOS_KEEP_ORIGIN) 2 else address.qos

    private fun effectivePublishQos(address: MqttClientAddress, localMessage: BrokerMessage): Int =
        if (address.qos == MqttClientAddress.QOS_KEEP_ORIGIN) localMessage.qosLevel else minOf(address.qos, localMessage.qosLevel)

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
                    .put("bufferImplementation", mqttConfig.bufferImplementation)
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
}
