package at.rocworks.devices.mqttclient

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.MqttClientAddress
import at.rocworks.stores.MqttClientConnectionConfig
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import org.eclipse.paho.client.mqttv3.*
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger
import javax.net.ssl.SSLSocketFactory
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

    private val logger: Logger = Utils.getLogger(this::class.java)

    // Device configuration
    private lateinit var deviceConfig: DeviceConfig
    private lateinit var mqttConfig: MqttClientConnectionConfig

    // Paho MQTT client
    private var client: MqttAsyncClient? = null

    // Connection state
    private var isConnected = false
    private var isReconnecting = false
    private var reconnectTimerId: Long? = null

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

            // Setup publish addresses (subscribe to local MQTT bus)
            setupPublishAddresses()

            // Start promise is completed immediately
            startPromise.complete()

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

        // Unsubscribe from internal topics
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler != null) {
            publishAddresses.values.forEach { address ->
                val clientId = "mqttclient-${deviceConfig.name}"
                val topicFilter = address.localTopic
                logger.info("Unsubscribing internal client '$clientId' from local topic '$topicFilter'")
                sessionHandler.unsubscribeInternal(clientId, topicFilter)
            }
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
            logger.info("MqttClientConnector for device ${deviceConfig.name} stopped")
            stopPromise.complete()
        }
    }

    private fun connectToRemoteBroker() {
        if (isConnected || isReconnecting) {
            return
        }

        isReconnecting = true

        try {
            logger.info("Connecting to remote MQTT broker: ${mqttConfig.getBrokerUrl()}")

            // Create Paho MQTT client
            val persistence = MemoryPersistence()
            client = MqttAsyncClient(mqttConfig.getBrokerUrl(), mqttConfig.clientId, persistence)

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

                // Set SSL/TLS for secure protocols
                if (mqttConfig.protocol == MqttClientConnectionConfig.PROTOCOL_TCPS ||
                    mqttConfig.protocol == MqttClientConnectionConfig.PROTOCOL_WSS) {
                    socketFactory = SSLSocketFactory.getDefault() as SSLSocketFactory
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
                    vertx.runOnContext {
                        handleRemoteMessageArrived(topic, message)
                    }
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
                        logger.info("Connected to remote MQTT broker: ${mqttConfig.getBrokerUrl()}")

                        // Setup subscriptions
                        setupSubscriptions()
                    }
                }

                override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
                    vertx.runOnContext {
                        isReconnecting = false
                        logger.severe("Failed to connect to remote MQTT broker: ${exception?.message}")
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
                val qos = 0 // QoS 0 for now

                client!!.subscribe(topic, qos, null, object : IMqttActionListener {
                    override fun onSuccess(asyncActionToken: IMqttToken?) {
                        vertx.runOnContext {
                            subscribedAddresses[topic] = address
                            logger.info("Subscribed to remote topic: $topic for device ${deviceConfig.name}")
                        }
                    }

                    override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
                        vertx.runOnContext {
                            logger.severe("Failed to subscribe to remote topic $topic: ${exception?.message}")
                        }
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

        // Subscribe to local topics using internal subscription mechanism
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler != null) {
            publishAddrs.forEach { address ->
                val clientId = "mqttclient-${deviceConfig.name}"
                val topicFilter = address.localTopic
                logger.info("Internal subscription for MQTT client '$clientId' to local topic '$topicFilter'")

                sessionHandler.subscribeInternal(clientId, topicFilter, 0) { message ->
                    handleLocalMqttMessage(message)
                }
            }
        } else {
            logger.severe("SessionHandler not available for internal subscriptions")
        }
    }

    private fun handleRemoteMessageArrived(topic: String, message: PahoMqttMessage) {
        try {
            logger.fine("Received message from remote broker on topic: $topic")

            // Find matching subscribe address
            val matchingAddress = subscribedAddresses.values.find { address ->
                MqttTopicTransformer.matchesRemotePattern(topic, address.remoteTopic)
            }

            if (matchingAddress == null) {
                logger.fine("No matching address for remote topic: $topic")
                return
            }

            // Transform topic from remote to local
            val localTopic = MqttTopicTransformer.remoteToLocal(topic, matchingAddress)

            // Create BrokerMessage for local broker
            // Set sender to our clientId to prevent loop (won't be delivered back to us)
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
                sender = clientId
            )

            // Publish to local MQTT through SessionHandler to ensure proper archiving
            val sessionHandler = Monster.getSessionHandler()
            if (sessionHandler != null) {
                sessionHandler.publishMessage(localMessage)
                logger.fine("Forwarded remote message from $topic to local topic $localTopic")
            } else {
                logger.warning("SessionHandler not available for message publishing")
            }

        } catch (e: Exception) {
            logger.severe("Error handling remote message: ${e.message}")
        }
    }

    private fun handleLocalMqttMessage(localMessage: BrokerMessage) {
        try {
            // Check each publish address to see if this message matches
            publishAddresses.values.forEach { address ->
                if (MqttTopicTransformer.matchesLocalPattern(localMessage.topicName, address.localTopic)) {
                    // Transform topic from local to remote
                    val remoteTopic = MqttTopicTransformer.localToRemote(localMessage.topicName, address)

                    if (remoteTopic != null) {
                        publishToRemoteBroker(remoteTopic, localMessage, address)
                    }
                }
            }
        } catch (e: Exception) {
            logger.severe("Error handling local MQTT message: ${e.message}")
        }
    }

    private fun publishToRemoteBroker(remoteTopic: String, localMessage: BrokerMessage, address: MqttClientAddress) {
        if (!isConnected || client == null) {
            logger.fine("Not connected to remote broker, skipping publish to $remoteTopic")
            return
        }

        try {
            val message = PahoMqttMessage(localMessage.payload).apply {
                qos = localMessage.qosLevel
                isRetained = localMessage.isRetain
            }

            client!!.publish(remoteTopic, message, null, object : IMqttActionListener {
                override fun onSuccess(asyncActionToken: IMqttToken?) {
                    logger.fine("Published message to remote topic: $remoteTopic")
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