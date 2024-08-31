package at.rocworks

import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttTopicName
import io.netty.handler.codec.mqtt.MqttConnectReturnCode
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.Vertx
import io.vertx.mqtt.MqttEndpoint
import io.vertx.mqtt.messages.MqttPublishMessage
import io.vertx.mqtt.messages.MqttSubscribeMessage
import io.vertx.mqtt.messages.MqttUnsubscribeMessage
import java.util.concurrent.ArrayBlockingQueue
import java.util.logging.Logger

class MqttClient(private val distributor: Distributor): AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    @Volatile
    private var endpoint: MqttEndpoint? = null

    @Volatile
    private var connected: Boolean = false

    private val messageQueue = ArrayBlockingQueue<MqttMessage>(10000) // TODO: configurable

    private var messageQueueError: Boolean = false

    private var lastMessageId: Int = 0
    private fun getNextMessageId() = if (lastMessageId == 65535) {
        lastMessageId = 0
        0
    } else ++lastMessageId

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    fun getClientId() = deploymentID()

    companion object {
        private val logger = Logger.getLogger(this::class.simpleName)
        private val clients: HashMap<String, MqttClient> = hashMapOf()

        fun deployEndpoint(vertx: Vertx, endpoint: MqttEndpoint, distributor: Distributor) {
            val clientId = endpoint.clientIdentifier()
            clients[clientId]?.let { client ->
                logger.info("Client [${endpoint.clientIdentifier()}] Redeploy existing session.")
                if (endpoint.isCleanSession) client.cleanSession()
                client.startEndpoint(endpoint)
            } ?: run {
                logger.info("Client [${endpoint.clientIdentifier()}] Deploy a new session.")
                val client = MqttClient(distributor)
                vertx.deployVerticle(client).onComplete {
                    clients[clientId] = client
                    client.startEndpoint(endpoint)
                }
            }
        }

        fun undeployEndpoint(vertx: Vertx, endpoint: MqttEndpoint) {
            val clientId = endpoint.clientIdentifier()
            logger.info("Remove client [${endpoint.clientIdentifier()}]")
            clients[clientId]?.let { client ->
                vertx.undeploy(client.deploymentID()).onComplete {
                    clients.remove(clientId)
                }
            }
        }

        fun getClientAddress(clientId: String) = "${Const.CLIENT_NAMESPACE}/${clientId}"

        fun sendMessageToClient(vertx: Vertx, clientId: String, message: MqttMessage) {
            vertx.eventBus().publish(getClientAddress(clientId), message)
        }
    }

    override fun start() {
        vertx.eventBus().consumer(getClientAddress(getClientId())) {
            consumeMessage(it.body())
        }
    }

    fun startEndpoint(endpoint: MqttEndpoint) {
        logger.info("Client [${endpoint.clientIdentifier()}] Request to connect. Clean session [${endpoint.isCleanSession}] protocol [${endpoint.protocolVersion()}]")
        // protocolVersion: 3=MQTTv31, 4=MQTTv311, 5=MQTTv5
        if (endpoint.protocolVersion()==5) {
            logger.warning("Client [${endpoint.clientIdentifier()}] Protocol version 5 not yet supported. Closing session.")
            endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_PROTOCOL_ERROR)
            endpoint.close()
            undeployEndpoint(vertx, endpoint)
        } else {
            endpoint.exceptionHandler { exceptionHandler(endpoint, it) }
            endpoint.pingHandler { pingHandler(endpoint) }
            endpoint.subscribeHandler { subscribeHandler(endpoint, it) }
            endpoint.unsubscribeHandler { unsubscribeHandler(endpoint, it) }

            endpoint.publishHandler { publishHandler(endpoint, it) }
            endpoint.publishReleaseHandler { publishReleaseHandler(endpoint, it) }

            endpoint.publishAcknowledgeHandler { publishAcknowledgeHandler(endpoint, it) }
            endpoint.publishReceivedHandler { publishedReceivedHandler(endpoint, it) }
            endpoint.publishCompletionHandler { publishCompletionHandler(endpoint, it) }

            endpoint.disconnectHandler { disconnectHandler(endpoint) }
            endpoint.closeHandler { closeHandler(endpoint) }

            endpoint.accept(endpoint.isCleanSession) // TODO: check if we have an existing session

            logger.info("Client [${endpoint.clientIdentifier()}] Queued messages: ${messageQueue.size}")
            messageQueue.forEach { it.publish(endpoint, getNextMessageId()) }
            messageQueue.clear()

            this.endpoint = endpoint
            this.connected = true
        }
    }

    private fun stopEndpoint(endpoint: MqttEndpoint) {
        this.connected = false

        if (endpoint.isCleanSession) {
            logger.info("Client [${endpoint.clientIdentifier()}] Undeploy client, it is a clean session.")
            cleanSession()
            undeployEndpoint(vertx, endpoint)
        } else {
            logger.info("Client [${endpoint.clientIdentifier()}] Pause client, it is not a clean session.")
        }
    }

    private fun cleanSession() {
        distributor.cleanSessionRequest(this)
        messageQueue.clear()
    }

    private fun exceptionHandler(endpoint: MqttEndpoint, throwable: Throwable) {
        logger.severe("Client [${endpoint.clientIdentifier()}] Exception: ${throwable.message}")
        closeConnection(endpoint)
    }

    private fun pingHandler(endpoint: MqttEndpoint) {
    }

    private fun subscribeHandler(endpoint: MqttEndpoint, subscribe: MqttSubscribeMessage) {
        val xs = subscribe.topicSubscriptions().map {
            it.qualityOfService()
        }

        // Acknowledge the subscriptions
        endpoint.subscribeAcknowledge(subscribe.messageId(), xs)

        // Subscribe
        subscribe.topicSubscriptions().forEach { subscription ->
            logger.info("Client [${endpoint.clientIdentifier()}] Subscription for [${subscription.topicName()}] with QoS ${subscription.qualityOfService()}")
            distributor.subscribeRequest(this, MqttTopicName(subscription.topicName()))
        }
    }

    private fun consumeMessage(message: MqttMessage) {
        this.endpoint?.let { endpoint ->
            if (this.connected) {
                message.publish(endpoint, getNextMessageId())
                logger.finest { "Client [${endpoint.clientIdentifier()}] Delivered message [${message.messageId}] for topic [${message.topicName}]" }
            } else {
                try {
                    messageQueue.add(message)
                    if (messageQueueError) messageQueueError = false
                    logger.finest { "Client [${endpoint.clientIdentifier()}] Queued message [${message.messageId}] for topic [${message.topicName}]" }
                } catch (e: IllegalStateException) {
                    if (!messageQueueError) {
                        messageQueueError = true
                        logger.warning("Client [${endpoint.clientIdentifier()}] Error adding message to queue: [${e.message}]")
                    }
                }
            }
        }
    }

    private fun unsubscribeHandler(endpoint: MqttEndpoint, unsubscribe: MqttUnsubscribeMessage) {
        unsubscribe.topics().forEach { topicName ->
            logger.info("Client [${endpoint.clientIdentifier()}] Unsubscribe for [${topicName}]}")
            distributor.unsubscribeRequest(this, MqttTopicName(topicName))
        }
        endpoint.unsubscribeAcknowledge(unsubscribe.messageId())
    }

    // Receiving messages from client (publish)

    private fun publishHandler(endpoint: MqttEndpoint, message: MqttPublishMessage) {
        logger.finest { "Client [${endpoint.clientIdentifier()}] Published message [${message.messageId()}] for [${message.topicName()}] with QoS ${message.qosLevel()}" }
        endpoint.apply {
            // Handle QoS levels
            if (message.qosLevel() == MqttQoS.AT_LEAST_ONCE) { // Level 1
                logger.finest { "Client [${endpoint.clientIdentifier()}] Sending acknowledge for id [${message.messageId()}]" }
                publishAcknowledge(message.messageId())
            } else if (message.qosLevel() == MqttQoS.EXACTLY_ONCE) { // Level 2
                logger.finest { "Client [${endpoint.clientIdentifier()}] Sending received for id [${message.messageId()}]" }
                publishReceived(message.messageId())
            }
        }
        distributor.publishMessage(MqttMessage(message))
    }

    private fun publishReleaseHandler(endpoint: MqttEndpoint, id: Int) {
        logger.finest { "Client [${endpoint.clientIdentifier()}] Got publish release id [$id]"}
        endpoint.publishComplete(id) // Publish QoS 2
    }

    // Sending messages to client (subscribe)

    private fun publishAcknowledgeHandler(endpoint: MqttEndpoint, id: Int) {
        logger.finest { "Client [${endpoint.clientIdentifier()}] Got sending acknowledge id [$id]" }
    }

    private fun publishedReceivedHandler(endpoint: MqttEndpoint, id: Int) {
        logger.finest { "Client [${endpoint.clientIdentifier()}] Got sending received id [$id], now sending release to client." }
        endpoint.publishRelease(id)
    }

    private fun publishCompletionHandler(endpoint: MqttEndpoint, id: Int) {
        logger.finest { "Client [${endpoint.clientIdentifier()}] Got sending complete id [$id]" }
    }

    private fun sendLastWill(endpoint: MqttEndpoint) {
        logger.info("Client [${endpoint.clientIdentifier()}] Sending Last-Will message.")
        endpoint.will()?.let { will ->
            if (will.isWillFlag) {
                distributor.publishMessage(MqttMessage(will))
            }
        }
    }

    private fun disconnectHandler(endpoint: MqttEndpoint) {
        logger.info("Client [${endpoint.clientIdentifier()}] Disconnect received.")
        if (this.connected) stopEndpoint(endpoint)
    }

    private fun closeHandler(endpoint: MqttEndpoint) {
        logger.info("Client [${endpoint.clientIdentifier()}] Close received.")
        closeConnection(endpoint)
    }

    private fun closeConnection(endpoint: MqttEndpoint) {
        if (this.connected) { // if there was no disconnect before
            sendLastWill(endpoint)
            stopEndpoint(endpoint)
        }
    }
}