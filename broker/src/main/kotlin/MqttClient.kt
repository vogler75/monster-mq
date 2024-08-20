package at.rocworks

import at.rocworks.data.MqttClientId
import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttTopicName
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.Vertx
import io.vertx.mqtt.MqttEndpoint
import io.vertx.mqtt.messages.MqttPublishMessage
import io.vertx.mqtt.messages.MqttSubscribeMessage
import io.vertx.mqtt.messages.MqttUnsubscribeMessage
import java.util.concurrent.ArrayBlockingQueue
import java.util.logging.Level
import java.util.logging.Logger

class MqttClient(private val distributor: Distributor): AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    @Volatile
    private var endpoint: MqttEndpoint? = null

    @Volatile
    private var connected: Boolean = false

    private val messageQueue = ArrayBlockingQueue<MqttMessage>(10000) // TODO: configurable

    init {
        logger.level = Level.INFO
    }

    fun getClientId() = MqttClientId(deploymentID())
    fun getDistributorId(): String = distributor.deploymentID()

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

        fun getClientAddress(clientId: MqttClientId) = "${Const.CLIENT_NAMESPACE}/${clientId.identifier}"

        fun sendMessageToClient(vertx: Vertx, clientId: MqttClientId, message: MqttMessage) {
            vertx.eventBus().publish(getClientAddress(clientId), message)
        }
    }

    override fun start() {
        vertx.eventBus().consumer(getClientAddress(getClientId())) {
            consumeMessage(it.body())
        }
    }

    fun startEndpoint(endpoint: MqttEndpoint) {
        logger.info("Client [${endpoint.clientIdentifier()}] Request to connect, clean session is [${endpoint.isCleanSession}]")
        endpoint.exceptionHandler { exceptionHandler(endpoint, it) }
        endpoint.subscribeHandler { subscribeHandler(endpoint, it) }
        endpoint.unsubscribeHandler { unsubscribeHandler(endpoint, it) }
        endpoint.publishHandler { publishHandler(endpoint, it) }
        endpoint.publishReleaseHandler { publishReleaseHandler(endpoint, it) }
        endpoint.disconnectHandler { disconnectHandler(endpoint) }
        endpoint.closeHandler { closeHandler(endpoint) }
        endpoint.accept(endpoint.isCleanSession)

        logger.info("Client [${endpoint.clientIdentifier()}] Queued messages: ${messageQueue.size}")
        messageQueue.forEach { it.publish(endpoint) }
        messageQueue.clear()

        this.endpoint = endpoint
        this.connected = true
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
    }

    private fun subscribeHandler(endpoint: MqttEndpoint, subscribe: MqttSubscribeMessage) {
        val xs = subscribe.topicSubscriptions().map {
            it.qualityOfService() // TODO: check access control lists
        }

        // Acknowledge the subscriptions
        endpoint.subscribeAcknowledge(subscribe.messageId(), xs)

        // Subscribe
        subscribe.topicSubscriptions().forEach { subscription ->
            logger.info("Client [${endpoint.clientIdentifier()}] Subscription for [${subscription.topicName()}] with QoS ${subscription.qualityOfService()}")
            distributor.subscribeRequest(this, MqttTopicName(subscription.topicName()))
        }
    }

    private var queueAddError: Boolean = false
    private fun consumeMessage(message: MqttMessage) {
        this.endpoint?.let { endpoint ->
            if (this.connected) {
                message.publish(endpoint)
                logger.finest { "Client [${endpoint.clientIdentifier()}] Delivered message for topic [${message.topicName}]" }
            } else {
                try {
                    messageQueue.add(message)
                    if (queueAddError) queueAddError = false
                    logger.finest { "Client [${endpoint.clientIdentifier()}] Queued message for topic [${message.topicName}]" }
                } catch (e: IllegalStateException) {
                    if (!queueAddError) {
                        queueAddError = true
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

    private fun publishHandler(endpoint: MqttEndpoint, message: MqttPublishMessage) {
        logger.finest { "Client [${endpoint.clientIdentifier()}] Received message [${message.topicName()}] with QoS ${message.qosLevel()}" }

        distributor.publishMessage(MqttMessage(message))

        endpoint.apply {
            // Handle QoS levels
            if (message.qosLevel() == MqttQoS.AT_LEAST_ONCE) {
                publishAcknowledge(message.messageId())
            } else if (message.qosLevel() == MqttQoS.EXACTLY_ONCE) {
                publishReceived(message.messageId())
            }
        }
    }

    private fun sendLastWill(endpoint: MqttEndpoint) {
        logger.info("Client [${endpoint.clientIdentifier()}] Sending Last-Will message.")
        endpoint.will()?.let { will ->
            if (will.isWillFlag) {
                distributor.publishMessage(MqttMessage(will))
            }
        }
    }

    private fun publishReleaseHandler(endpoint: MqttEndpoint, messageId: Int) {
        endpoint.publishComplete(messageId)
    }

    private fun disconnectHandler(endpoint: MqttEndpoint) {
        logger.info("Client [${endpoint.clientIdentifier()}] Disconnect received.")
        if (this.connected) stopEndpoint(endpoint)
    }

    private fun closeHandler(endpoint: MqttEndpoint) {
        logger.info("Client [${endpoint.clientIdentifier()}] Close received.")
        if (this.connected) { // if there was no disconnect before
            sendLastWill(endpoint)
            stopEndpoint(endpoint)
        }
    }
}