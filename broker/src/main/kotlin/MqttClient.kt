package at.rocworks

import at.rocworks.data.MqttMessage
import io.netty.handler.codec.mqtt.MqttConnectReturnCode
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.Vertx
import io.vertx.core.eventbus.Message
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.json.JsonObject
import io.vertx.mqtt.MqttEndpoint
import io.vertx.mqtt.messages.MqttPublishMessage
import io.vertx.mqtt.messages.MqttSubscribeMessage
import io.vertx.mqtt.messages.MqttUnsubscribeMessage
import java.util.logging.Logger

class MqttClient(private val distributor: Distributor): AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    @Volatile
    private var endpoint: MqttEndpoint? = null

    @Volatile
    private var connected: Boolean = false

    private var messageConsumer: MessageConsumer<MqttMessage>? = null
    private var commandConsumer: MessageConsumer<JsonObject>? = null

    private var lastMessageId: Int = 0
    private fun getNextMessageId() = if (lastMessageId == 65535) {
        lastMessageId = 0
        0
    } else ++lastMessageId

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    fun getClientId(): String = endpoint?.clientIdentifier() ?: throw(Exception("MqttClient::getClientId called without endpoint!"))// deploymentID()

    companion object {
        private val logger = Logger.getLogger(this::class.simpleName)
        private val clients: HashMap<String, MqttClient> = hashMapOf()

        fun deployEndpoint(vertx: Vertx, endpoint: MqttEndpoint, distributor: Distributor) {
            val clientId = endpoint.clientIdentifier()
            clients[clientId]?.let { client ->
                logger.info("Client [${clientId}] Redeploy existing session for [${endpoint.remoteAddress()}].")
                client.startEndpoint(endpoint)
            } ?: run {
                logger.info("Client [${clientId}] Deploy a new session for [${endpoint.remoteAddress()}].")
                val client = MqttClient(distributor)
                vertx.deployVerticle(client).onComplete {
                    clients[clientId] = client
                    client.startEndpoint(endpoint)
                }
            }
        }

        fun undeployEndpoint(vertx: Vertx, endpoint: MqttEndpoint) {
            val clientId = endpoint.clientIdentifier()
            logger.info("Remove client [${clientId}]")
            clients[clientId]?.let { client ->
                vertx.undeploy(client.deploymentID()).onComplete {
                    clients.remove(clientId)
                }
            }
        }

        private fun getClientAddress(clientId: String) = "${Const.CLIENT_NAMESPACE}/${clientId}"
        fun getClientAddressMessages(clientId: String) = "${getClientAddress(clientId)}/m"
        fun getClientAddressCommands(clientId: String) = "${getClientAddress(clientId)}/c"

        fun sendMessageToClient(vertx: Vertx, clientId: String, message: MqttMessage) {
            vertx.eventBus().publish(getClientAddressMessages(clientId), message)
        }

        fun sendCommandToClient(vertx: Vertx, clientId: String, command: JsonObject) {
            vertx.eventBus().publish(getClientAddressCommands(clientId), command)
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
            this.endpoint = endpoint

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

            if (messageConsumer==null) {
                messageConsumer = vertx.eventBus().consumer(getClientAddressMessages(endpoint.clientIdentifier())) {
                    consumeMessage(it)
                }
                commandConsumer = vertx.eventBus().consumer(getClientAddressCommands(endpoint.clientIdentifier())) {
                    consumeCommand(it)
                }
            }

            if (endpoint.isCleanSession) {
                // Clean and remove any existing session state
                distributor.sessionHandler.removeClient(getClientId())
            } else {
                // Publish queued messages
                distributor.sessionHandler.dequeueMessages(endpoint.clientIdentifier()) { message ->
                    logger.finest { "Client [${endpoint.clientIdentifier()}] Dequeued message [${message.messageId}] for topic [${message.topicName}]" }
                    message.publish(endpoint, getNextMessageId())
                }
            }
            distributor.sessionHandler.putClient(endpoint.clientIdentifier(), endpoint.isCleanSession, true)
            this.connected = true
        }
    }

    private fun stopEndpoint(endpoint: MqttEndpoint) {
        this.connected = false
        if (endpoint.isCleanSession) {
            logger.info("Client [${endpoint.clientIdentifier()}] Remove client, it is a clean session.")
            distributor.sessionHandler.removeClient(getClientId())
        } else {
            logger.info("Client [${endpoint.clientIdentifier()}] Pause client, it is not a clean session.")
            distributor.sessionHandler.pauseClient(getClientId())
        }
        undeployEndpoint(vertx, endpoint)
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
            distributor.subscribeRequest(this, subscription.topicName())
        }
    }

    private fun consumeMessage(message: Message<MqttMessage>) {
        this.endpoint?.let { endpoint ->
            val mqttMessage = message.body()
            if (this.connected) {
                mqttMessage.publish(endpoint, getNextMessageId())
                logger.finest { "Client [${endpoint.clientIdentifier()}] Delivered message [${mqttMessage.messageId}] for topic [${mqttMessage.topicName}]" }
            }
        }
    }

    private fun consumeCommand(message: Message<JsonObject>) {
        this.endpoint?.let { endpoint ->
            val command = message.body()
            when (val key = command.getString(Const.COMMAND_KEY)) {
                Const.COMMAND_STATUS -> {
                    logger.info("Client [${endpoint.clientIdentifier()}] Status command received.")
                    message.reply(JsonObject().put("Connected", connected))
                }
                else -> {
                    logger.warning("Client [${endpoint.clientIdentifier()}] Received unknown command [$key].")
                }
            }
        }
    }

    private fun unsubscribeHandler(endpoint: MqttEndpoint, unsubscribe: MqttUnsubscribeMessage) {
        unsubscribe.topics().forEach { topicName ->
            logger.info("Client [${endpoint.clientIdentifier()}] Unsubscribe for [${topicName}]}")
            distributor.unsubscribeRequest(this, topicName)
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