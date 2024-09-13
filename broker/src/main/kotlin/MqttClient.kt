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

class MqttClient(
    private val endpoint: MqttEndpoint,
    private val distributor: Distributor
): AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    @Volatile
    private var connected: Boolean = false

    private var deployed: Boolean = false
    private var lastPing: Instant = Instant.MIN

    private var nextMessageId: Int = 0
    private fun getNextMessageId(): Int = if (nextMessageId==Int.MAX_VALUE) {
        nextMessageId=1
        nextMessageId
    } else ++nextMessageId

    private data class InFlightMessage(
        val message: MqttMessage,
        var stage: Int = 1,
        var lastTryTime: Instant = Instant.now(),
        var retryCount: Int = 0
    )

    private val inFlightMessagesRcv = mutableMapOf<Int, InFlightMessage>() // messageId
    private val inFlightMessagesSnd : ArrayDeque<InFlightMessage> = ArrayDeque()

    private val busConsumers = mutableListOf<MessageConsumer<*>>()

    // create a getter for the client id
    val clientId: String
        get() = endpoint.clientIdentifier()

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    companion object {
        const val MAX_IN_FLIGHT_MESSAGES = 100_000 // TODO make this configurable

        private val logger = Utils.getLogger(this::class.java)

        fun deployEndpoint(vertx: Vertx, endpoint: MqttEndpoint, distributor: Distributor) {
            val clientId = endpoint.clientIdentifier()
            logger.info("Client [${clientId}] Deploy a new session for [${endpoint.remoteAddress()}] [${Utils.getCurrentFunctionName()}]")
            // TODO: check if the client is already connected (cluster wide)
            val client = MqttClient(endpoint, distributor)
            vertx.deployVerticle(client).onComplete {
                client.startEndpoint()
            }
        }

        fun undeployEndpoint(vertx: Vertx, deploymentID: String) {
            logger.info("DeploymentID [$deploymentID] undeploy [${Utils.getCurrentFunctionName()}]")
            vertx.undeploy(deploymentID).onComplete {
                logger.info("DeploymentID [$deploymentID] undeployed [${Utils.getCurrentFunctionName()}]")
            }
        }

        private fun getBaseAddress(clientId: String) = "${Const.GLOBAL_CLIENT_NAMESPACE}/${clientId}"
        fun getCommandAddress(clientId: String) = "${getBaseAddress(clientId)}/c"
        fun getMessages0Address(clientId: String) = "${getBaseAddress(clientId)}/m0"
        fun getMessages1Address(clientId: String) = "${getBaseAddress(clientId)}/m1"
        fun getMessages2Address(clientId: String) = "${getBaseAddress(clientId)}/m2"
    }

    override fun start() {
        vertx.setPeriodic(1000) { receivingInFlightMessagesPeriodicCheck() }
        vertx.setPeriodic(1000) { sendingInFlightMessagesPeriodicCheck() }
    }

    override fun stop() {
        logger.info("Client [${clientId}] Stop [${Utils.getCurrentFunctionName()}] ")
        busConsumers.forEach { it.unregister() }
    }

    fun startEndpoint() {
        logger.info("Client [$clientId] Request to connect. Clean session [${endpoint.isCleanSession}] protocol [${endpoint.protocolVersion()}] [${Utils.getCurrentFunctionName()}]")
        // protocolVersion: 3=MQTTv31, 4=MQTTv311, 5=MQTTv5
        if (endpoint.protocolVersion()==5) {
            logger.warning("Client [$clientId] Protocol version 5 not yet supported. Closing session [${Utils.getCurrentFunctionName()}]")
            endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_PROTOCOL_ERROR)
            endpoint.close()
            undeployEndpoint(vertx, this.deploymentID())
        } else {
            endpoint.exceptionHandler(::exceptionHandler)
            endpoint.pingHandler() { pingHandler() }
            endpoint.subscribeHandler(::subscribeHandler)
            endpoint.unsubscribeHandler(::unsubscribeHandler)
            endpoint.publishHandler(::publishHandler)
            endpoint.publishReleaseHandler(::publishReleaseHandler)

            endpoint.publishAcknowledgeHandler(::publishAcknowledgeHandler)
            endpoint.publishReceivedHandler(::publishedReceivedHandler)
            endpoint.publishCompletionHandler(::publishCompletionHandler)

            endpoint.disconnectHandler { disconnectHandler() }
            endpoint.closeHandler { closeHandler() }

            if (!deployed) {
                deployed = true
                busConsumers.add(vertx.eventBus().consumer(getCommandAddress(clientId), ::consumeCommand))
                busConsumers.add(vertx.eventBus().consumer(getMessages0Address(clientId), ::consumeMessageQoS0))
                busConsumers.add(vertx.eventBus().consumer(getMessages1Address(clientId), ::consumeMessageQoS1))
                busConsumers.add(vertx.eventBus().consumer(getMessages2Address(clientId), ::consumeMessageQoS2))
            }

            if (endpoint.isCleanSession) {
                distributor.sessionHandler.delClient(clientId) // Clean and remove any existing session state
                endpoint.accept(false) // false... session not present because of clean session requested
            } else {
                // Check if session was already present or if it was the first connect
                val sessionPresent = distributor.sessionHandler.isPresent(clientId)
                endpoint.accept(sessionPresent) // TODO: check if we have an existing session

                // Publish queued messages
                distributor.sessionHandler.dequeueMessages(clientId) { message ->
                    logger.finest { "Client [$clientId] Dequeued message [${message.messageId}] for topic [${message.topicName}] [${Utils.getCurrentFunctionName()}]" }
                    publishMessage(message.cloneWithNewMessageId(getNextMessageId()))
                }
            }

            this.connected = true
            distributor.sessionHandler.setClient(clientId, endpoint.isCleanSession, true)
            distributor.sessionHandler.setLastWill(clientId, endpoint.will())
        }
    }

    private fun stopEndpoint() {
        if (endpoint.isCleanSession) {
            logger.info("Client [$clientId] Remove client, it is a clean session [${Utils.getCurrentFunctionName()}]")
            distributor.sessionHandler.delClient(clientId)
        } else {
            logger.info("Client [$clientId] Pause client, it is not a clean session [${Utils.getCurrentFunctionName()}]")
            distributor.sessionHandler.pauseClient(clientId)
        }
        undeployEndpoint(vertx, this.deploymentID())
    }

    private fun exceptionHandler(throwable: Throwable) {
        logger.severe("Client [$clientId] Exception: ${throwable.message} [${Utils.getCurrentFunctionName()}]")
        closeConnection()
    }

    private fun pingHandler() {
        lastPing = Instant.now()
        endpoint.pong()
    }

    private fun subscribeHandler(subscribe: MqttSubscribeMessage) {
        // Acknowledge the subscriptions
        val acknowledge = subscribe.topicSubscriptions().map { it.qualityOfService() }
        endpoint.subscribeAcknowledge(subscribe.messageId(), acknowledge)

        // Subscribe
        subscribe.topicSubscriptions().forEach { subscription ->
            logger.info("Client [$clientId] Subscription for [${subscription.topicName()}] with QoS ${subscription.qualityOfService()} [${Utils.getCurrentFunctionName()}]")
            distributor.subscribeRequest(this, subscription.topicName(), subscription.qualityOfService())
        }
    }

    private fun consumeCommand(message: Message<JsonObject>) {
        val command = message.body()
        when (val key = command.getString(Const.COMMAND_KEY)) {
            Const.COMMAND_STATUS -> {
                logger.info("Client [$clientId] Status command received [${Utils.getCurrentFunctionName()}]")
                message.reply(JsonObject().put("Connected", connected))
            }
            Const.COMMAND_DISCONNECT -> {
                logger.info("Client [$clientId] Disconnect command received [${Utils.getCurrentFunctionName()}]")
                closeConnection()
                message.reply(JsonObject().put("Connected", false))
            }
            else -> {
                logger.warning("Client [$clientId] Received unknown command [$key] [${Utils.getCurrentFunctionName()}]")
                message.reply(JsonObject().put("Error", "Received unknown command [$key]"))
            }
        }
    }

    private fun unsubscribeHandler(unsubscribe: MqttUnsubscribeMessage) {
        unsubscribe.topics().forEach { topicName ->
            logger.info("Client [$clientId] Unsubscribe for [${topicName}]} [${Utils.getCurrentFunctionName()}]")
            distributor.unsubscribeRequest(this, topicName)
        }
        endpoint.unsubscribeAcknowledge(unsubscribe.messageId())
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Receiving messages from client (publish)
    // -----------------------------------------------------------------------------------------------------------------

    private fun publishHandler(message: MqttPublishMessage) {
        logger.finest { "Client [$clientId] Publish: message [${message.messageId()}] for [${message.topicName()}] with QoS ${message.qosLevel()} [${Utils.getCurrentFunctionName()}]" }
        // Handle QoS levels
        when (message.qosLevel()) {
            MqttQoS.AT_MOST_ONCE -> { // Level 0
                logger.finest { "Client [$clientId] Publish: no acknowledge needed [${Utils.getCurrentFunctionName()}]" }
                distributor.publishMessage(MqttMessage(message))
            }
            MqttQoS.AT_LEAST_ONCE -> { // Level 1
                logger.finest { "Client [$clientId] Publish: sending acknowledge for id [${message.messageId()}] [${Utils.getCurrentFunctionName()}]" }
                distributor.publishMessage(MqttMessage(message))
                // TODO: check the result of the publishMessage and send the acknowledge only if the message was delivered
                endpoint.publishAcknowledge(message.messageId())
            }
            MqttQoS.EXACTLY_ONCE -> { // Level 2
                logger.finest { "Client [$clientId] Publish: sending received for id [${message.messageId()}] [${Utils.getCurrentFunctionName()}]" }
                endpoint.publishReceived(message.messageId())
                inFlightMessagesRcv[message.messageId()] = InFlightMessage(MqttMessage(message))
            }
            else -> {
                logger.warning { "Client [$clientId] Publish: unknown QoS level [${message.qosLevel()}] [${Utils.getCurrentFunctionName()}]" }
            }
        }
    }

    private fun publishReleaseHandler(id: Int) {
        inFlightMessagesRcv[id]?.let { inFlightMessage ->
            logger.finest { "Client [$clientId] Publish: got publish release id [$id], now sending complete to client [${Utils.getCurrentFunctionName()}]"}
            endpoint.publishComplete(id)
            distributor.publishMessage(inFlightMessage.message)
            inFlightMessagesRcv.remove(id)
        } ?: run {
            logger.warning { "Client [$clientId] Publish: got publish release for unknown id [$id] [${Utils.getCurrentFunctionName()}]"}
        }
    }

    private fun receivingInFlightMessagesPeriodicCheck() {
        inFlightMessagesRcv.forEach { (id, inFlightMessage) ->
            if (inFlightMessage.retryCount < Const.QOS2_RETRY_COUNT) {
                if (inFlightMessage.lastTryTime.plusSeconds(Const.QOS2_RETRY_INTERVAL).isBefore(Instant.now())) {
                    logger.finest { "Client [$clientId] Publish: retry message [${id}] for topic [${inFlightMessage.message.topicName}] [${Utils.getCurrentFunctionName()}]" }
                    inFlightMessage.lastTryTime = Instant.now()
                    inFlightMessage.retryCount++
                    endpoint.publishReceived(id)
                }
            } else {
                logger.warning { "Client [$clientId] Publish: Message [${id}] for topic [${inFlightMessage.message.topicName}] not delivered  [${Utils.getCurrentFunctionName()}]" }
                inFlightMessagesRcv.remove(id)
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Sending messages to client (subscribe)
    // -----------------------------------------------------------------------------------------------------------------

    private fun consumeMessageQoS0(busMessage: Message<MqttMessage>) {
        val message = busMessage.body().cloneWithNewMessageId(0)
        if (this.connected) {
            publishMessage(message)
            logger.finest { "Client [$clientId] QoS [0] message [${message.messageId}] for topic [${message.topicName}] delivered  [${Utils.getCurrentFunctionName()}]" }
        } else {
            logger.warning { "Client [$clientId] QoS [0] message [${message.messageId}] for topic [${message.topicName}] not delivered, client not connected. [${Utils.getCurrentFunctionName()}]" }
        }
    }

    private fun publishMessage(message: MqttMessage) {
        if (message.qosLevel == 0) {
            message.publish(endpoint)
        } else {
            if (inFlightMessagesSnd.size > MAX_IN_FLIGHT_MESSAGES) {
                logger.warning { "Client [$clientId] QoS [${message.qosLevel}] message [${message.messageId}] for topic [${message.topicName}] not delivered, queue is full [${Utils.getCurrentFunctionName()}]" }
                // TODO: message must be removed from message store (queued messages)
            } else {
                inFlightMessagesSnd.addLast(InFlightMessage(message))
                if (inFlightMessagesSnd.size == 1) {
                    message.publish(endpoint)
                    logger.finest { "Client [$clientId] QoS [${message.qosLevel}] message [${message.messageId}] for topic [${message.topicName}] delivered [${Utils.getCurrentFunctionName()}]" }
                } else {
                    logger.finest { "Client [$clientId] QoS [${message.qosLevel}] message [${message.messageId}] for topic [${message.topicName}] queued [${Utils.getCurrentFunctionName()}]" }
                }
            }
        }
    }

    private fun publishMessageCheckNext() {
        if (inFlightMessagesSnd.size > 0) {
            inFlightMessagesSnd.first().message.publish(endpoint)
            logger.finest { "Client [$clientId] Subscribe: next message [${inFlightMessagesSnd.first().message.messageId}] from queue delivered [${Utils.getCurrentFunctionName()}]" }
        }
    }

    private fun publishMessageCompleted(message: MqttMessage) {
        inFlightMessagesSnd.removeFirst()
        distributor.publishMessageCompleted(clientId, message)
    }

    private fun consumeMessageQoS1(busMessage: Message<MqttMessage>) {
        val message = busMessage.body().cloneWithNewMessageId(getNextMessageId())
        if (this.connected) {
            publishMessage(message)
        } else {
            logger.finest { "Client [$clientId] QoS [1] message [${message.messageId}] for topic [${message.topicName}] not delivered, client not connected [${Utils.getCurrentFunctionName()}]" }
        }
    }

    private fun publishAcknowledgeHandler(id: Int) { // QoS 1
        try {
            inFlightMessagesSnd.first().let { inFlightMessage ->
                if (inFlightMessage.message.messageId == id) {
                    logger.finest { "Client [$clientId] Subscribe: got acknowledge id [$id] [${Utils.getCurrentFunctionName()}]" }
                    publishMessageCompleted(inFlightMessage.message)
                    publishMessageCheckNext()
                } else {
                    logger.warning { "Client [$clientId] Subscribe: got acknowledge id [$id] but expected [${inFlightMessage.message.messageId}] [${Utils.getCurrentFunctionName()}]" }
                }
            }
        } catch (e: NoSuchElementException) {
            logger.warning { "Client [$clientId] Subscribe: got acknowledge id [$id] but no message in queue [${Utils.getCurrentFunctionName()}]" }
        }
    }

    private fun consumeMessageQoS2(busMessage: Message<MqttMessage>) {
        val message = busMessage.body().cloneWithNewMessageId(getNextMessageId())
        if (this.connected) {
            publishMessage(message)
        } else {
            logger.finest { "Client [$clientId] QoS [2] message [${message.messageId}] for topic [${message.topicName}] not delivered, client not connected [${Utils.getCurrentFunctionName()}]" }
        }
    }

    private fun publishedReceivedHandler(id: Int) { // QoS 2
        try {
            inFlightMessagesSnd.first().let { inFlightMessage ->
                if (inFlightMessage.message.messageId == id) {
                    logger.finest { "Client [$clientId] Subscribe: got received id [$id], now sending release to client [${Utils.getCurrentFunctionName()}]" }
                    inFlightMessage.stage = 2
                    endpoint.publishRelease(id)
                } else {
                    logger.warning { "Client [$clientId] Subscribe: got received id [$id] but expected [${inFlightMessage.message.messageId}] [${Utils.getCurrentFunctionName()}]" }
                }
            }
        } catch (e: NoSuchElementException) {
            logger.warning { "Client [$clientId] Subscribe: got received id [$id] but no message in queue [${Utils.getCurrentFunctionName()}]" }
        }
    }

    private fun publishCompletionHandler(id: Int) { // QoS 2
        try {
            inFlightMessagesSnd.first().let { inFlightMessage ->
                if (inFlightMessage.message.messageId == id) {
                    logger.finest { "Client [$clientId] Subscribe: got complete id [$id] [${Utils.getCurrentFunctionName()}]" }
                    publishMessageCompleted(inFlightMessage.message)
                    publishMessageCheckNext()
                } else {
                    logger.warning { "Client [$clientId] Subscribe: got complete id [$id] but expected [${inFlightMessage.message.messageId}] [${Utils.getCurrentFunctionName()}]" }
                }
            }
        } catch (e: NoSuchElementException) {
            logger.warning { "Client [$clientId] Subscribe: got complete id [$id] but no message in queue [${Utils.getCurrentFunctionName()}]" }
        }
    }

    private fun sendingInFlightMessagesPeriodicCheck() {
        try {
            inFlightMessagesSnd.last().let { inFlightMessage ->
                if (inFlightMessage.retryCount < Const.QOS2_RETRY_COUNT) {
                    if (inFlightMessage.lastTryTime.plusSeconds(Const.QOS2_RETRY_INTERVAL).isBefore(Instant.now())) {
                        logger.finest { "Client [$clientId] Subscribe: retry message [${inFlightMessage.message.messageId}] stage [${inFlightMessage.stage}] for topic [${inFlightMessage.message.topicName}] [${Utils.getCurrentFunctionName()}]" }
                        inFlightMessage.lastTryTime = Instant.now()
                        inFlightMessage.retryCount++
                        when (inFlightMessage.stage) {
                            1 -> endpoint.let { inFlightMessage.message.publish(it) }
                            2 -> endpoint.publishReceived(inFlightMessage.message.messageId)
                            else -> logger.warning { "Client [$clientId] Subscribe: unknown stage [${inFlightMessage.stage}] for message [${inFlightMessage.message.messageId}] [${Utils.getCurrentFunctionName()}]" }
                        }
                    } else {
                        // nothing
                    }
                } else {
                    logger.warning { "Client [$clientId] Subscribe: Message [${inFlightMessage.message.messageId}] for topic [${inFlightMessage.message.topicName}] not delivered [${Utils.getCurrentFunctionName()}]" }
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

    private fun sendLastWill() {
        logger.info("Client [$clientId] Sending Last-Will message [${Utils.getCurrentFunctionName()}]")
        endpoint.will()?.let { will ->
            if (will.isWillFlag) {
                distributor.publishMessage(MqttMessage(will))
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Disconnect handling
    // -----------------------------------------------------------------------------------------------------------------

    private fun disconnectHandler() {
        logger.info("Client [$clientId] Disconnect received [${Utils.getCurrentFunctionName()}]")
        if (this.connected) {
            stopEndpoint()
            this.connected=false
        }
    }

    private fun closeHandler() {
        logger.info("Client [$clientId] Close received [${Utils.getCurrentFunctionName()}]")
        closeConnection()
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Close connection
    // -----------------------------------------------------------------------------------------------------------------

    private fun closeConnection() {
        logger.info("Client [$clientId] Close connection [${Utils.getCurrentFunctionName()}]")
        if (this.connected) { // if there was no disconnect before
            sendLastWill()
            stopEndpoint()
            this.connected=false
        }
    }
}