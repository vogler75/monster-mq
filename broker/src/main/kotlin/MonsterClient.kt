package at.rocworks

import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.mqtt.MqttEndpoint
import io.vertx.mqtt.messages.MqttPublishMessage
import io.vertx.mqtt.messages.MqttSubscribeMessage
import io.vertx.mqtt.messages.MqttUnsubscribeMessage
import java.util.concurrent.ArrayBlockingQueue
import java.util.logging.Logger

class MonsterClient: AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    private var endpoint: MqttEndpoint? = null
    private val subscriptions = mutableMapOf<String, MessageConsumer<Buffer>>()
    private val messageQueue = ArrayBlockingQueue<Pair<String, Buffer>>(10000) // TODO: configurable

    @Volatile private var pauseClient: Boolean = false

    companion object {
        private val logger = Logger.getLogger(this.javaClass.simpleName)
        private val clients: HashMap<String, MonsterClient> = hashMapOf()

        fun endpointHandler(vertx: Vertx, endpoint: MqttEndpoint) {
            val client : MonsterClient? = clients[endpoint.clientIdentifier()]
            if (client != null) {
                logger.info("Existing client [${endpoint.clientIdentifier()}]")
                if (endpoint.isCleanSession) client.cleanSession()
                client.startEndpoint(endpoint)
            } else {
                logger.info("New client [${endpoint.clientIdentifier()}]")
                val client = MonsterClient()
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

    fun startEndpoint(endpoint: MqttEndpoint) {
        logger.info("Client [${endpoint.clientIdentifier()}] request to connect, clean session = ${endpoint.isCleanSession}")
        this.endpoint = endpoint
        endpoint.subscribeHandler(::subscribeHandler)
        endpoint.unsubscribeHandler(::unsubscribeHandler)
        endpoint.publishHandler(::publishHandler)
        endpoint.publishReleaseHandler(::publishReleaseHandler)
        endpoint.disconnectHandler { closeHandler(1) }
        endpoint.closeHandler { closeHandler(2) }
        endpoint.accept(endpoint.isCleanSession)
        if (this.pauseClient) {
            logger.info("Send queue size ${messageQueue.size}")
            messageQueue.forEach {
                endpoint.publish(it.first, it.second, MqttQoS.AT_LEAST_ONCE, false, false)
            }
            messageQueue.clear()
            this.pauseClient = false
        }
    }

    private fun stopEndpoint() {
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
        subscriptions.forEach { it.value.unregister() }
        subscriptions.clear()
        messageQueue.clear()
    }

    private fun subscribeHandler(subscribe: MqttSubscribeMessage) {
        subscribe.topicSubscriptions().forEach {
            logger.info("Subscription for [${it.topicName()}] with QoS ${it.qualityOfService()}")
        }

        // Acknowledge the subscriptions
        endpoint?.subscribeAcknowledge(subscribe.messageId(), subscribe.topicSubscriptions().map { it.qualityOfService() })

        subscribe.topicSubscriptions().forEach { subscription ->
            val address = subscription.topicName()
            if (!subscriptions.contains(address)) {
                val consumer = vertx.eventBus().consumer<Buffer>(address) { message ->
                    val payload = message.body()
                    if (!this.pauseClient) {
                        this.endpoint?.publish(address, payload, MqttQoS.AT_LEAST_ONCE, false, false)
                        logger.info("Delivered message to [${endpoint?.clientIdentifier()}] topic [$address]")
                    } else {
                        logger.info("Queued message for [${endpoint?.clientIdentifier()}] topic [$address]")
                        messageQueue.add(address to payload)
                    }
                }
                subscriptions[address] = consumer
            }
        }
    }

    private fun unsubscribeHandler(unsubscribe: MqttUnsubscribeMessage) {
        unsubscribe.topics().forEach { address ->
            subscriptions[address]?.unregister()
            subscriptions.remove(address)
        }
    }

    private fun publishHandler(message: MqttPublishMessage) {
        logger.info("Received message [${message.topicName()}] with QoS ${message.qosLevel()}")

        // Publish the message to the Vert.x event bus
        val address: String = message.topicName()
        val payload: Buffer = message.payload()
        vertx.eventBus().publish(address, payload)

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

    private fun closeHandler(handler: Int) {
        logger.info("Close received [${endpoint?.clientIdentifier()}] [$handler].")
        stopEndpoint()
    }
}