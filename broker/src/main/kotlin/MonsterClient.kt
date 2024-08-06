package at.rocworks

import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.Vertx
import io.vertx.mqtt.MqttEndpoint
import io.vertx.mqtt.messages.MqttPublishMessage
import io.vertx.mqtt.messages.MqttSubscribeMessage
import io.vertx.mqtt.messages.MqttUnsubscribeMessage
import io.vertx.mqtt.messages.impl.MqttPublishMessageImpl
import java.util.concurrent.ArrayBlockingQueue
import java.util.logging.Level
import java.util.logging.Logger

class MonsterClient(private val server: MonsterServer): AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    private var endpoint: MqttEndpoint? = null

    private val messageQueue = ArrayBlockingQueue<MqttPublishMessageImpl>(10000) // TODO: configurable

    @Volatile private var pauseClient: Boolean = false
    private var connected = false

    private fun getClientBusAddr() = Const.getClientBusAddr(this.deploymentID())

    companion object {
        private val logger = Logger.getLogger(this.javaClass.simpleName)
        private val clients: HashMap<String, MonsterClient> = hashMapOf()

        fun endpointHandler(vertx: Vertx, endpoint: MqttEndpoint, server: MonsterServer) {
            clients[endpoint.clientIdentifier()]?.let { client ->
                logger.info("Existing client [${endpoint.clientIdentifier()}]")
                if (endpoint.isCleanSession) client.cleanSession()
                client.startEndpoint(endpoint)
            } ?: run {
                logger.info("New client [${endpoint.clientIdentifier()}]")
                val client = MonsterClient(server)
                vertx.deployVerticle(client)
                clients[endpoint.clientIdentifier()] = client
                client.startEndpoint(endpoint)
            }
        }

        fun removeEndpoint(vertx: Vertx, endpoint: MqttEndpoint) {
            logger.info("Remove client [${endpoint.clientIdentifier()}]")
            val client : MonsterClient? = clients[endpoint.clientIdentifier()]
            if (client != null) {
                clients.remove(endpoint.clientIdentifier())
                vertx.undeploy(client.deploymentID())
            }
        }
    }

    init {
        logger.level = Level.INFO
    }

    fun startEndpoint(endpoint: MqttEndpoint) {
        logger.info("Client [${endpoint.clientIdentifier()}] request to connect, clean session = ${endpoint.isCleanSession}")
        this.endpoint = endpoint
        endpoint.subscribeHandler(::subscribeHandler)
        endpoint.unsubscribeHandler(::unsubscribeHandler)
        endpoint.publishHandler(::publishHandler)
        endpoint.publishReleaseHandler(::publishReleaseHandler)
        endpoint.disconnectHandler { disconnectHandler() }
        endpoint.closeHandler { closeHandler() }
        endpoint.accept(endpoint.isCleanSession)
        connected = true

        vertx.eventBus().consumer<MqttPublishMessageImpl>(getClientBusAddr()) {
            consumeMessage(it.body())
        }

        if (this.pauseClient) {
            logger.info("Send queue size ${messageQueue.size}")
            messageQueue.forEach {
                endpoint.publish(it.topicName(), it.payload(), it.qosLevel(), it.isDup, it.isRetain)
            }
            messageQueue.clear()
            this.pauseClient = false
        }
    }

    private fun stopEndpoint() {
        if (!connected) return
        else connected = false
        endpoint?.let { endpoint ->
            logger.info("Stop client [${endpoint.clientIdentifier()}]")
            if (endpoint.isCleanSession) {
                logger.info("Undeploy client [${endpoint.clientIdentifier()}]")
                cleanSession()
                removeEndpoint(vertx, endpoint)
            } else {
                logger.info("Pause client [${endpoint.clientIdentifier()}]")
                pauseClient = true
            }
        }
    }

    fun cleanSession() {
        server.distributor.cleanSessionRequest(this) {}
        messageQueue.clear()
    }

    private fun subscribeHandler(subscribe: MqttSubscribeMessage) {
        // Acknowledge the subscriptions
        endpoint?.subscribeAcknowledge(subscribe.messageId(), subscribe.topicSubscriptions().map { it.qualityOfService() })

        // Subscribe
        subscribe.topicSubscriptions().forEach { subscription ->
            logger.info("Subscription for [${subscription.topicName()}] with QoS ${subscription.qualityOfService()}")
            server.distributor.subscribeRequest(this, subscription.topicName()) { }
        }
    }

    private fun consumeMessage(message: MqttPublishMessageImpl) {
        if (!this.pauseClient) {
            this.endpoint?.publish(
                message.topicName(),
                message.payload(),
                message.qosLevel(),
                message.isDup,
                message.isRetain
            )
            logger.finest { "Delivered message to [${endpoint?.clientIdentifier()}] topic [${message.topicName()}]" }
        } else {
            try {
                messageQueue.add(message)
                logger.finest { "Queued message for [${endpoint?.clientIdentifier()}] topic [${message.topicName()}]" }
            } catch (e: IllegalStateException) {
                logger.warning(e.message)
            }
        }
    }

    private fun unsubscribeHandler(unsubscribe: MqttUnsubscribeMessage) {
        unsubscribe.topics().forEach { topicName ->
            server.distributor.unsubscribeRequest(this, topicName) { }
        }
    }

    private fun publishHandler(message: MqttPublishMessage) {
        logger.finest { "Received message [${message.topicName()}] with QoS ${message.qosLevel()}" }

        server.distributor.publishMessage(message)

        endpoint?.apply {
            // Handle QoS levels
            if (message.qosLevel() == MqttQoS.AT_LEAST_ONCE) {
                publishAcknowledge(message.messageId())
            } else if (message.qosLevel() == MqttQoS.EXACTLY_ONCE) {
                publishReceived(message.messageId())
            }
        }
    }

    private fun publishReleaseHandler(messageId: Int) {
        endpoint?.publishComplete(messageId)
    }

    private fun disconnectHandler() {
        logger.info("Disconnect received [${endpoint?.clientIdentifier()}]")
        stopEndpoint()
    }

    private fun closeHandler() {
        logger.info("Close received [${endpoint?.clientIdentifier()}]")
        stopEndpoint()
    }
}