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
import java.time.Instant

class MqttClient(private val distributor: Distributor): AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    @Volatile
    private var endpoint: MqttEndpoint? = null

    @Volatile
    private var connected: Boolean = false

    private var deployed: Boolean = false
    private var lastPing: Instant = Instant.MIN

    private var nextMessageId: Int = 0
    private fun getNextMessageId(): Int = ++nextMessageId

    private data class InFlightMessage(
        val message: MqttMessage,
        var stage: Int = 1,
        var lastTryTime: Instant = Instant.now(),
        var retryCount: Int = 0
    )

    private val inFlightMessagesRcv = mutableMapOf<Int, InFlightMessage>() // messageId
    private val inFlightMessagesSnd : ArrayDeque<InFlightMessage> = ArrayDeque()

    private val busConsumers = mutableListOf<MessageConsumer<*>>()

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    fun getClientId(): String = endpoint?.clientIdentifier() ?: throw(Exception("MqttClient::getClientId called without endpoint!"))// deploymentID()

    companion object {
        const val MAX_IN_FLIGHT_MESSAGES = 1000 // TODO make this configurable

        private val logger = Utils.getLogger(this::class.java)
        private val clients: HashMap<String, MqttClient> = hashMapOf()

        fun deployEndpoint(vertx: Vertx, endpoint: MqttEndpoint, distributor: Distributor) {
            val clientId = endpoint.clientIdentifier()
            clients[clientId]?.let { client ->
                logger.info("Client [${clientId}] Redeploy existing session for [${endpoint.remoteAddress()}] [${Utils.getCurrentFunctionName()}]")
                client.startEndpoint(endpoint)
            } ?: run {
                logger.info("Client [${clientId}] Deploy a new session for [${endpoint.remoteAddress()}] [${Utils.getCurrentFunctionName()}]")
                val client = MqttClient(distributor)
                vertx.deployVerticle(client).onComplete {
                    clients[clientId] = client
                    client.startEndpoint(endpoint)
                }
            }
        }

        fun undeployEndpoint(vertx: Vertx, endpoint: MqttEndpoint) {
            val clientId = endpoint.clientIdentifier()
            logger.info("Remove client [${clientId}] [${Utils.getCurrentFunctionName()}]")
            clients[clientId]?.let { client ->
                logger.info("Client [${clientId}] Undeploy deployment id [${client.deploymentID()}] [${Utils.getCurrentFunctionName()}]")
                vertx.undeploy(client.deploymentID()).onComplete {
                    logger.info("Client [${clientId}] Undeployed [${Utils.getCurrentFunctionName()}]")
                    clients.remove(clientId)
                }
            }
        }

        private fun getAddress(clientId: String) = "${Const.GLOBAL_CLIENT_NAMESPACE}/${clientId}"
        fun getCommandAddress(clientId: String) = "${getAddress(clientId)}/c"
        fun getMessages0Address(clientId: String) = "${getAddress(clientId)}/m0"
        fun getMessages1Address(clientId: String) = "${getAddress(clientId)}/m1"
        fun getMessages2Address(clientId: String) = "${getAddress(clientId)}/m2"
    }

    override fun start() {
        vertx.setPeriodic(1000) { receivingInFlightMessagesPeriodicCheck() }
        vertx.setPeriodic(1000) { sendingInFlightMessagesPeriodicCheck() }
    }

    override fun stop() {
        logger.info("Client [${endpoint?.clientIdentifier()}] Stop [${Utils.getCurrentFunctionName()}] deployment id [${this.deploymentID()}]")
        busConsumers.forEach { it.unregister() }
    }

    fun startEndpoint(endpoint: MqttEndpoint) {
        logger.info("Client [${endpoint.clientIdentifier()}] Request to connect. Clean session [${endpoint.isCleanSession}] protocol [${endpoint.protocolVersion()}] [${Utils.getCurrentFunctionName()}]")
        // protocolVersion: 3=MQTTv31, 4=MQTTv311, 5=MQTTv5
        if (endpoint.protocolVersion()==5) {
            logger.warning("Client [${endpoint.clientIdentifier()}] Protocol version 5 not yet supported. Closing session [${Utils.getCurrentFunctionName()}]")
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

            if (!deployed) {
                deployed = true
                busConsumers.add(vertx.eventBus().consumer(getCommandAddress(endpoint.clientIdentifier()), ::consumeCommand))
                busConsumers.add(vertx.eventBus().consumer(getMessages0Address(endpoint.clientIdentifier()), ::consumeMessageQoS0))
                busConsumers.add(vertx.eventBus().consumer(getMessages1Address(endpoint.clientIdentifier()), ::consumeMessageQoS1))
                busConsumers.add(vertx.eventBus().consumer(getMessages2Address(endpoint.clientIdentifier()), ::consumeMessageQoS2))
            }

            if (endpoint.isCleanSession) {
                // Clean and remove any existing session state
                distributor.sessionHandler.delClient(getClientId())
            } else {
                // Publish queued messages
                distributor.sessionHandler.dequeueMessages(endpoint.clientIdentifier()) { message ->
                    logger.finest { "Client [${endpoint.clientIdentifier()}] Dequeued message [${message.messageId}] for topic [${message.topicName}] [${Utils.getCurrentFunctionName()}]" }
                    publishMessage(endpoint, message)
                }
            }

            this.connected = true
            distributor.sessionHandler.setClient(endpoint.clientIdentifier(), endpoint.isCleanSession, true)
        }
    }

    private fun stopEndpoint(endpoint: MqttEndpoint) {
        this.connected = false
        if (endpoint.isCleanSession) {
            logger.info("Client [${endpoint.clientIdentifier()}] Remove client, it is a clean session [${Utils.getCurrentFunctionName()}]")
            distributor.sessionHandler.delClient(getClientId())
        } else {
            logger.info("Client [${endpoint.clientIdentifier()}] Pause client, it is not a clean session [${Utils.getCurrentFunctionName()}]")
            distributor.sessionHandler.pauseClient(getClientId())
        }
        undeployEndpoint(vertx, endpoint)
    }

    private fun exceptionHandler(endpoint: MqttEndpoint, throwable: Throwable) {
        logger.severe("Client [${endpoint.clientIdentifier()}] Exception: ${throwable.message} [${Utils.getCurrentFunctionName()}]")
        closeConnection(endpoint)
    }

    private fun pingHandler(endpoint: MqttEndpoint) {
        lastPing = Instant.now()
        endpoint.pong()
    }

    private fun subscribeHandler(endpoint: MqttEndpoint, subscribe: MqttSubscribeMessage) {
        // Acknowledge the subscriptions
        val acknowledge = subscribe.topicSubscriptions().map { it.qualityOfService() }
        endpoint.subscribeAcknowledge(subscribe.messageId(), acknowledge)

        // Subscribe
        subscribe.topicSubscriptions().forEach { subscription ->
            logger.info("Client [${endpoint.clientIdentifier()}] Subscription for [${subscription.topicName()}] with QoS ${subscription.qualityOfService()} [${Utils.getCurrentFunctionName()}]")
            distributor.subscribeRequest(this, subscription.topicName(), subscription.qualityOfService())
        }
    }

    private fun consumeCommand(message: Message<JsonObject>) {
        this.endpoint?.let { endpoint ->
            val command = message.body()
            when (val key = command.getString(Const.COMMAND_KEY)) {
                Const.COMMAND_STATUS -> {
                    logger.info("Client [${endpoint.clientIdentifier()}] Status command received [${Utils.getCurrentFunctionName()}]")
                    message.reply(JsonObject().put("Connected", connected))
                }
                else -> {
                    logger.warning("Client [${endpoint.clientIdentifier()}] Received unknown command [$key] [${Utils.getCurrentFunctionName()}]")
                }
            }
        }
    }

    private fun unsubscribeHandler(endpoint: MqttEndpoint, unsubscribe: MqttUnsubscribeMessage) {
        unsubscribe.topics().forEach { topicName ->
            logger.info("Client [${endpoint.clientIdentifier()}] Unsubscribe for [${topicName}]} [${Utils.getCurrentFunctionName()}]")
            distributor.unsubscribeRequest(this, topicName)
        }
        endpoint.unsubscribeAcknowledge(unsubscribe.messageId())
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Receiving messages from client (publish)
    // -----------------------------------------------------------------------------------------------------------------

    private fun publishHandler(endpoint: MqttEndpoint, message: MqttPublishMessage) {
        logger.finest { "Client [${endpoint.clientIdentifier()}] Publish: message [${message.messageId()}] for [${message.topicName()}] with QoS ${message.qosLevel()} [${Utils.getCurrentFunctionName()}]" }
        endpoint.apply {
            // Handle QoS levels
            when (message.qosLevel()) {
                MqttQoS.AT_MOST_ONCE -> { // Level 0
                    logger.finest { "Client [${endpoint.clientIdentifier()}] Publish: no acknowledge needed [${Utils.getCurrentFunctionName()}]" }
                    distributor.publishMessage(MqttMessage(message))
                }
                MqttQoS.AT_LEAST_ONCE -> { // Level 1
                    logger.finest { "Client [${endpoint.clientIdentifier()}] Publish: sending acknowledge for id [${message.messageId()}] [${Utils.getCurrentFunctionName()}]" }
                    distributor.publishMessage(MqttMessage(message))
                    // TODO: check the result of the publishMessage and send the acknowledge only if the message was delivered
                    publishAcknowledge(message.messageId())
                }
                MqttQoS.EXACTLY_ONCE -> { // Level 2
                    logger.finest { "Client [${endpoint.clientIdentifier()}] Publish: sending received for id [${message.messageId()}] [${Utils.getCurrentFunctionName()}]" }
                    publishReceived(message.messageId())
                    inFlightMessagesRcv[message.messageId()] = InFlightMessage(MqttMessage(message))
                }
                else -> {
                    logger.warning { "Client [${endpoint.clientIdentifier()}] Publish: unknown QoS level [${message.qosLevel()}] [${Utils.getCurrentFunctionName()}]" }
                }
            }
        }
    }

    private fun publishReleaseHandler(endpoint: MqttEndpoint, id: Int) {
        inFlightMessagesRcv[id]?.let { inFlightMessage ->
            logger.finest { "Client [${endpoint.clientIdentifier()}] Publish: got publish release id [$id], now sending complete to client [${Utils.getCurrentFunctionName()}]"}
            endpoint.publishComplete(id)
            distributor.publishMessage(inFlightMessage.message)
            inFlightMessagesRcv.remove(id)
        } ?: run {
            logger.warning { "Client [${endpoint.clientIdentifier()}] Publish: got publish release for unknown id [$id] [${Utils.getCurrentFunctionName()}]"}
        }
    }

    private fun receivingInFlightMessagesPeriodicCheck() {
        inFlightMessagesRcv.forEach { (id, inFlightMessage) ->
            if (inFlightMessage.retryCount < Const.QOS2_RETRY_COUNT) {
                if (inFlightMessage.lastTryTime.plusSeconds(Const.QOS2_RETRY_INTERVAL).isBefore(Instant.now())) {
                    logger.finest { "Client [${endpoint?.clientIdentifier()}] Publish: retry message [${id}] for topic [${inFlightMessage.message.topicName}] [${Utils.getCurrentFunctionName()}]" }
                    inFlightMessage.lastTryTime = Instant.now()
                    inFlightMessage.retryCount++
                    endpoint?.publishReceived(id)
                }
            } else {
                logger.warning { "Client [${endpoint?.clientIdentifier()}] Publish: Message [${id}] for topic [${inFlightMessage.message.topicName}] not delivered  [${Utils.getCurrentFunctionName()}]" }
                inFlightMessagesRcv.remove(id)
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Sending messages to client (subscribe)
    // -----------------------------------------------------------------------------------------------------------------

    private fun consumeMessageQoS0(busMessage: Message<MqttMessage>) {
        this.endpoint?.let { endpoint ->
            val message = busMessage.body().cloneWithNewMessageId(0)
            if (this.connected) {
                publishMessage(endpoint, message)
                logger.finest { "Client [${endpoint.clientIdentifier()}] QoS [0] message [${message.messageId}] for topic [${message.topicName}] delivered  [${Utils.getCurrentFunctionName()}]" }
            } else {
                logger.warning { "Client [${endpoint.clientIdentifier()}] QoS [0] message [${message.messageId}] for topic [${message.topicName}] not delivered, client not connected. [${Utils.getCurrentFunctionName()}]" }
            }
        }
    }

    private fun publishMessage(endpoint: MqttEndpoint, message: MqttMessage) {
        if (message.qosLevel == 0) {
            message.publish(endpoint)
        } else {
            if (inFlightMessagesSnd.size > MAX_IN_FLIGHT_MESSAGES) {
                logger.warning { "Client [${endpoint.clientIdentifier()}] QoS [${message.qosLevel}] message [${message.messageId}] for topic [${message.topicName}] not delivered, queue is full [${Utils.getCurrentFunctionName()}]" }
            } else {
                inFlightMessagesSnd.addLast(InFlightMessage(message))
                if (inFlightMessagesSnd.size == 1) {
                    message.publish(endpoint)
                    logger.finest { "Client [${endpoint.clientIdentifier()}] QoS [${message.qosLevel}] message [${message.messageId}] for topic [${message.topicName}] delivered [${Utils.getCurrentFunctionName()}]" }
                } else {
                    logger.finest { "Client [${endpoint.clientIdentifier()}] QoS [${message.qosLevel}] message [${message.messageId}] for topic [${message.topicName}] queued [${Utils.getCurrentFunctionName()}]" }
                }
            }
        }
    }

    private fun publishMessageCheckNext(endpoint: MqttEndpoint) {
        if (inFlightMessagesSnd.size > 0) {
            inFlightMessagesSnd.first().message.publish(endpoint)
            logger.finest { "Client [${endpoint.clientIdentifier()}] Subscribe: next message [${inFlightMessagesSnd.first().message.messageId}] from queue delivered [${Utils.getCurrentFunctionName()}]" }
        }
    }

    private fun consumeMessageQoS1(busMessage: Message<MqttMessage>) {
        this.endpoint?.let { endpoint ->
            val message = busMessage.body().cloneWithNewMessageId(getNextMessageId())
            if (this.connected) {
                publishMessage(endpoint, message)
            } else {
                logger.finest { "Client [${endpoint.clientIdentifier()}] QoS [1] message [${message.messageId}] for topic [${message.topicName}] not delivered, client not connected [${Utils.getCurrentFunctionName()}]" }
            }
        }
    }

    private fun publishAcknowledgeHandler(endpoint: MqttEndpoint, id: Int) { // QoS 1
        try {
            inFlightMessagesSnd.first().let { inFlightMessage ->
                if (inFlightMessage.message.messageId == id) {
                    logger.finest { "Client [${endpoint.clientIdentifier()}] Subscribe: got acknowledge id [$id] [${Utils.getCurrentFunctionName()}]" }
                    inFlightMessagesSnd.removeFirst()
                    publishMessageCheckNext(endpoint)
                } else {
                    logger.warning { "Client [${endpoint.clientIdentifier()}] Subscribe: got acknowledge id [$id] but expected [${inFlightMessage.message.messageId}] [${Utils.getCurrentFunctionName()}]" }
                }
            }
        } catch (e: NoSuchElementException) {
            logger.warning { "Client [${endpoint.clientIdentifier()}] Subscribe: got acknowledge id [$id] but no message in queue [${Utils.getCurrentFunctionName()}]" }
        }
    }

    private fun consumeMessageQoS2(busMessage: Message<MqttMessage>) {
        this.endpoint?.let { endpoint ->
            val message = busMessage.body().cloneWithNewMessageId(getNextMessageId())
            if (this.connected) {
                publishMessage(endpoint, message)
            } else {
                logger.finest { "Client [${endpoint.clientIdentifier()}] QoS [2] message [${message.messageId}] for topic [${message.topicName}] not delivered, client not connected [${Utils.getCurrentFunctionName()}]" }
            }
        }
    }

    private fun publishedReceivedHandler(endpoint: MqttEndpoint, id: Int) { // QoS 2
        try {
            inFlightMessagesSnd.first().let { inFlightMessage ->
                if (inFlightMessage.message.messageId == id) {
                    logger.finest { "Client [${endpoint.clientIdentifier()}] Subscribe: got received id [$id], now sending release to client [${Utils.getCurrentFunctionName()}]" }
                    inFlightMessage.stage = 2
                    endpoint.publishRelease(id)
                } else {
                    logger.warning { "Client [${endpoint.clientIdentifier()}] Subscribe: got received id [$id] but expected [${inFlightMessage.message.messageId}] [${Utils.getCurrentFunctionName()}]" }
                }
            }
        } catch (e: NoSuchElementException) {
            logger.warning { "Client [${endpoint.clientIdentifier()}] Subscribe: got received id [$id] but no message in queue [${Utils.getCurrentFunctionName()}]" }
        }
    }

    private fun publishCompletionHandler(endpoint: MqttEndpoint, id: Int) { // QoS 2
        try {
            inFlightMessagesSnd.first().let { inFlightMessage ->
                if (inFlightMessage.message.messageId == id) {
                    logger.finest { "Client [${endpoint.clientIdentifier()}] Subscribe: got complete id [$id] [${Utils.getCurrentFunctionName()}]" }
                    inFlightMessagesSnd.removeFirst()
                    publishMessageCheckNext(endpoint)
                } else {
                    logger.warning { "Client [${endpoint.clientIdentifier()}] Subscribe: got complete id [$id] but expected [${inFlightMessage.message.messageId}] [${Utils.getCurrentFunctionName()}]" }
                }
            }
        } catch (e: NoSuchElementException) {
            logger.warning { "Client [${endpoint.clientIdentifier()}] Subscribe: got complete id [$id] but no message in queue [${Utils.getCurrentFunctionName()}]" }
        }
    }

    private fun sendingInFlightMessagesPeriodicCheck() {
        try {
            inFlightMessagesSnd.last().let { inFlightMessage ->
                if (inFlightMessage.retryCount < Const.QOS2_RETRY_COUNT) {
                    if (inFlightMessage.lastTryTime.plusSeconds(Const.QOS2_RETRY_INTERVAL).isBefore(Instant.now())) {
                        logger.finest { "Client [${endpoint?.clientIdentifier()}] Subscribe: retry message [${inFlightMessage.message.messageId}] stage [${inFlightMessage.stage}] for topic [${inFlightMessage.message.topicName}] [${Utils.getCurrentFunctionName()}]" }
                        inFlightMessage.lastTryTime = Instant.now()
                        inFlightMessage.retryCount++
                        when (inFlightMessage.stage) {
                            1 -> endpoint?.let { inFlightMessage.message.publish(it) }
                            2 -> endpoint?.publishReceived(inFlightMessage.message.messageId)
                            else -> logger.warning { "Client [${endpoint?.clientIdentifier()}] Subscribe: unknown stage [${inFlightMessage.stage}] for message [${inFlightMessage.message.messageId}] [${Utils.getCurrentFunctionName()}]" }
                        }
                    } else {
                        // nothing
                    }
                } else {
                    logger.warning { "Client [${endpoint?.clientIdentifier()}] Subscribe: Message [${inFlightMessage.message.messageId}] for topic [${inFlightMessage.message.topicName}] not delivered [${Utils.getCurrentFunctionName()}]" }
                    inFlightMessagesSnd.removeLast()
                }
            }
        } catch (e: NoSuchElementException) {
            // no messages in queue
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Last will
    // -----------------------------------------------------------------------------------------------------------------

    private fun sendLastWill(endpoint: MqttEndpoint) {
        logger.info("Client [${endpoint.clientIdentifier()}] Sending Last-Will message [${Utils.getCurrentFunctionName()}]")
        endpoint.will()?.let { will ->
            if (will.isWillFlag) {
                distributor.publishMessage(MqttMessage(will))
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Disconnect handling
    // -----------------------------------------------------------------------------------------------------------------

    private fun disconnectHandler(endpoint: MqttEndpoint) {
        logger.info("Client [${endpoint.clientIdentifier()}] Disconnect received [${Utils.getCurrentFunctionName()}]")
        if (this.connected) stopEndpoint(endpoint)
    }

    private fun closeHandler(endpoint: MqttEndpoint) {
        logger.info("Client [${endpoint.clientIdentifier()}] Close received [${Utils.getCurrentFunctionName()}]")
        closeConnection(endpoint)
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Close connection
    // -----------------------------------------------------------------------------------------------------------------

    private fun closeConnection(endpoint: MqttEndpoint) {
        if (this.connected) { // if there was no disconnect before
            sendLastWill(endpoint)
            stopEndpoint(endpoint)
        }
    }
}