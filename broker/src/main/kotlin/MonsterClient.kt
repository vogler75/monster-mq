package at.rocworks

import io.netty.handler.codec.mqtt.MqttProperties
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
    private var connected = false

    private val messageQueue = ArrayBlockingQueue<MqttPublishMessageImpl>(10000) // TODO: configurable

    @Volatile
    private var pauseClient: Boolean = false

    private lateinit var clientBusAddr: String

    init {
        logger.level = Level.INFO
    }
    
    fun clientId() = endpoint?.clientIdentifier() ?: ""

    companion object {
        private val logger = Logger.getLogger(this::class.simpleName)
        private val clients: HashMap<String, MonsterClient> = hashMapOf()

        fun endpointHandler(vertx: Vertx, endpoint: MqttEndpoint, server: MonsterServer) {
            clients[endpoint.clientIdentifier()]?.let { client ->
                logger.info("Existing client [${endpoint.clientIdentifier()}]")
                if (endpoint.isCleanSession) client.cleanSession()
                client.startEndpoint(endpoint)
            } ?: run {
                logger.info("New client [${endpoint.clientIdentifier()}]")
                val client = MonsterClient(server)
                vertx.deployVerticle(client).onComplete {
                    clients[endpoint.clientIdentifier()] = client
                    client.startEndpoint(endpoint)
                }
            }
        }

        fun removeEndpoint(vertx: Vertx, endpoint: MqttEndpoint) {
            logger.info("Remove client [${endpoint.clientIdentifier()}]")
            clients[endpoint.clientIdentifier()]?.let { client ->
                vertx.undeploy(client.deploymentID()).onComplete {
                    clients.remove(endpoint.clientIdentifier())
                }
            }
        }
    }

    override fun start() {
        clientBusAddr = Const.getClientBusAddr(this.deploymentID())
    }

    fun startEndpoint(endpoint: MqttEndpoint) {
        logger.info("Client [${endpoint.clientIdentifier()}] request to connect, clean session = ${endpoint.isCleanSession}")
        this.endpoint = endpoint
        endpoint.exceptionHandler(::exceptionHandler)
        endpoint.subscribeHandler(::subscribeHandler)
        endpoint.unsubscribeHandler(::unsubscribeHandler)
        endpoint.publishHandler(::publishHandler)
        endpoint.publishReleaseHandler(::publishReleaseHandler)
        endpoint.disconnectHandler { disconnectHandler() }
        endpoint.closeHandler { closeHandler() }
        endpoint.accept(endpoint.isCleanSession)
        connected = true

        vertx.eventBus().consumer(clientBusAddr) {
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
        connected = false
        endpoint?.let { endpoint ->
            if (endpoint.isCleanSession) {
                logger.info("Client [${clientId()}] undeploy.")
                cleanSession()
                removeEndpoint(vertx, endpoint)
            } else {
                logger.info("Client [${clientId()}] paused.")
                pauseClient = true
            }
        }
    }

    private fun cleanSession() {
        server.distributor.cleanSessionRequest(this) {}
        messageQueue.clear()
    }

    private fun exceptionHandler(throwable: Throwable) {
        logger.severe("Client [${clientId()}] Exception: ${throwable.message}")
    }

    private fun subscribeHandler(subscribe: MqttSubscribeMessage) {
        // Acknowledge the subscriptions
        endpoint?.subscribeAcknowledge(subscribe.messageId(), subscribe.topicSubscriptions().map { it.qualityOfService() })

        // Subscribe
        subscribe.topicSubscriptions().forEach { subscription ->
            logger.info("Client [${clientId()}] Subscription for [${subscription.topicName()}] with QoS ${subscription.qualityOfService()}")
            server.distributor.subscribeRequest(this, subscription.topicName()) { }
        }
    }

    private var queueAddError: Boolean = false
    private fun consumeMessage(message: MqttPublishMessageImpl) {
        if (!this.pauseClient) {
            this.endpoint?.publish(
                message.topicName(),
                message.payload(),
                message.qosLevel(),
                message.isDup,
                message.isRetain
            )
            logger.finest { "Client [${clientId()}] Delivered message to [${clientId()}] topic [${message.topicName()}]" }
        } else {
            try {
                messageQueue.add(message)
                if (queueAddError) queueAddError = false
                logger.finest { "Client [${clientId()}] Queued message for topic [${message.topicName()}]" }
            } catch (e: IllegalStateException) {
                if (!queueAddError) {
                    queueAddError = true
                    logger.warning("Client [${clientId()}] Error adding message to queue: [${e.message}]")
                }
            }
        }
    }

    private fun unsubscribeHandler(unsubscribe: MqttUnsubscribeMessage) {
        unsubscribe.topics().forEach { topicName ->
            logger.info("Client [${clientId()}] Unsubscribe for [${topicName}]}")
            server.distributor.unsubscribeRequest(this, topicName) { }
        }
    }

    private fun publishHandler(message: MqttPublishMessage) {
        logger.finest { "Client [${clientId()}] Received message [${message.topicName()}] with QoS ${message.qosLevel()}" }

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

    private fun sendLastWill() {
        logger.info("Client [${clientId()}] Sending Last-Will message.")
        endpoint?.will()?.let { will ->
            if (will.isWillFlag) {
                val message = MqttPublishMessageImpl(
                    0,
                    MqttQoS.valueOf(will.willQos),
                    false,
                    will.isWillRetain,
                    will.willTopic,
                    Const.bufferToByteBuf(will.willMessage),
                    MqttProperties()
                )
                server.distributor.publishMessage(message)
            }
        }
    }

    private fun publishReleaseHandler(messageId: Int) {
        endpoint?.publishComplete(messageId)
    }

    private fun disconnectHandler() {
        logger.info("Client [${clientId()}] disconnect received.")
        if (connected) stopEndpoint()
    }

    private fun closeHandler() {
        logger.info("Client [${clientId()}] close received.")
        if (connected) { // if there was no disconnect before
            sendLastWill()
            stopEndpoint()
        }
    }
}