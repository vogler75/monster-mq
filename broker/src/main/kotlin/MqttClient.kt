package at.rocworks

import at.rocworks.data.MqttMessage
import at.rocworks.handlers.SessionHandler
import io.netty.handler.codec.mqtt.MqttConnectReturnCode
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.eventbus.Message
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.json.JsonObject
import io.vertx.mqtt.MqttEndpoint
import io.vertx.mqtt.messages.MqttPublishMessage
import io.vertx.mqtt.messages.MqttSubscribeMessage
import io.vertx.mqtt.messages.MqttUnsubscribeMessage
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque

class MqttClient(
    private val endpoint: MqttEndpoint,
    private val sessionHandler: SessionHandler,
): AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    private var deployed: Boolean = false
    private var ready: Boolean = false
    private var lastPing: Instant = Instant.MIN
    private var gracefulDisconnected: Boolean = false
    private var lastStatisticsMessage: String = ""

    private var nextMessageId: Int = 0
    private fun getNextMessageId(): Int = if (nextMessageId==65535) {
        nextMessageId=1
        nextMessageId
    } else ++nextMessageId

    private data class InFlightMessage(
        val message: MqttMessage,
        var stage: Int = 1,
        var lastTryTime: Instant = Instant.now(),
        var retryCount: Int = 0
    )

    private val inFlightMessagesRcv = ConcurrentHashMap<Int, InFlightMessage>() // messageId TODO: is concurrent needed?
    private val inFlightMessagesSnd : ConcurrentLinkedDeque<InFlightMessage> = ConcurrentLinkedDeque() // TODO: is concurrent needed?

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

        fun deployEndpoint(vertx: Vertx, endpoint: MqttEndpoint, sessionHandler: SessionHandler) {
            val clientId = endpoint.clientIdentifier()
            logger.fine("Client [${clientId}] Deploy a new session for [${endpoint.remoteAddress()}] [${Utils.getCurrentFunctionName()}]")
            // TODO: check if the client is already connected (cluster wide)
            val client = MqttClient(endpoint, sessionHandler)
            vertx.deployVerticle(client).onComplete {
                client.startEndpoint()
            }
        }

        fun undeployEndpoint(vertx: Vertx, deploymentID: String) {
            logger.fine("DeploymentID [$deploymentID] undeploy [${Utils.getCurrentFunctionName()}]")
            vertx.undeploy(deploymentID).onComplete {
                logger.fine("DeploymentID [$deploymentID] undeployed [${Utils.getCurrentFunctionName()}]")
            }
        }

        private fun getBaseAddress(clientId: String) = "${Const.GLOBAL_CLIENT_NAMESPACE}/${clientId}"

        fun getCommandAddress(clientId: String) = "${getBaseAddress(clientId)}/C"
        fun getMessagesAddress(clientId: String) = "${getBaseAddress(clientId)}/M"
    }

    override fun start() {
        vertx.setPeriodic(1000) { receivingInFlightMessagesPeriodicCheck() }
        vertx.setPeriodic(1000) { sendingInFlightMessagesPeriodicCheck() }
        vertx.setPeriodic(1000) { publishStatistics() }
    }

    override fun stop() {
        logger.fine("Client [${clientId}] Stop [${Utils.getCurrentFunctionName()}] ")
        busConsumers.forEach { it.unregister() }
    }

    fun startEndpoint() {
        logger.info("Client [$clientId] Request to connect. Clean session [${endpoint.isCleanSession}] protocol [${endpoint.protocolVersion()}] [${Utils.getCurrentFunctionName()}]")
        // protocolVersion: 3=MQTTv31, 4=MQTTv311, 5=MQTTv5
        if (endpoint.protocolVersion()==5) {
            logger.warning("Client [$clientId] Protocol version 5 not yet supported. Closing session [${Utils.getCurrentFunctionName()}]")
            // print all connectProperties
            endpoint.connectProperties().listAll().forEach() { p ->
                logger.info("Property [${p.propertyId()}] = ${p.value()}")
            }
            rejectAndCloseEndpoint(MqttConnectReturnCode.CONNECTION_REFUSED_PROTOCOL_ERROR)
        } else {
            endpoint.exceptionHandler(::exceptionHandler)
            endpoint.pingHandler { pingHandler() }
            endpoint.subscribeHandler(::subscribeHandler)
            endpoint.unsubscribeHandler(::unsubscribeHandler)
            endpoint.publishHandler(::publishHandler)
            endpoint.publishReleaseHandler(::publishReleaseHandler)

            endpoint.publishAcknowledgeHandler(::publishAcknowledgeHandler)
            endpoint.publishReceivedHandler(::publishedReceivedHandler)
            endpoint.publishCompletionHandler(::publishCompletionHandler)

            endpoint.disconnectHandler { disconnectHandler() }
            endpoint.closeHandler { closeHandler() }

            // Message bus consumers
            if (!deployed) {
                deployed = true
                busConsumers.add(vertx.eventBus().consumer(getCommandAddress(clientId), ::consumeCommand))
                busConsumers.add(vertx.eventBus().consumer(getMessagesAddress(clientId), ::consumeMessage))
            } else {
                logger.severe("Client [$clientId] Already deployed [${Utils.getCurrentFunctionName()}]")
            }

            // Set last will
            sessionHandler.setLastWill(clientId, endpoint.will())

            fun finishClientStartup(present: Boolean) {
                // Accept connection
                endpoint.accept(present)

                // Set client to connected
                val information = JsonObject()
                information.put("RemoteAddress", endpoint.remoteAddress().toString())
                information.put("LocalAddress", endpoint.localAddress().toString())
                information.put("ProtocolVersion", endpoint.protocolVersion())
                information.put("SSL", endpoint.isSsl)
                information.put("AutoKeepAlive", endpoint.isAutoKeepAlive)
                information.put("KeepAliveTimeSeconds", endpoint.keepAliveTimeSeconds())
                sessionHandler.setClient(clientId, endpoint.isCleanSession, information).onComplete {
                    logger.fine("Dequeue messages for client [$clientId] [${Utils.getCurrentFunctionName()}]")
                    sessionHandler.dequeueMessages(clientId) { m ->
                        logger.finest { "Client [$clientId] Dequeued message [${m.messageId}] for topic [${m.topicName}] [${Utils.getCurrentFunctionName()}]" }
                        publishMessage(m.cloneWithNewMessageId(getNextMessageId())) // TODO: if qos is >0 then all messages are put in the inflight queue
                        endpoint.isConnected // continue as long as the client is connected
                    }.onComplete {
                        if (endpoint.isConnected) { // if the client is still connected after the message queue
                            ready = true
                            sessionHandler.onlineClient(clientId)
                        }
                    }
                }
            }

            // Accept connection
            if (endpoint.isCleanSession) {
                sessionHandler.delClient(clientId).onComplete { // Clean and remove any existing session state
                    finishClientStartup(false) // false... session not present because of clean session requested
                }.onFailure {
                    logger.severe("Client [$clientId] Error: ${it.message} [${Utils.getCurrentFunctionName()}]")
                    rejectAndCloseEndpoint(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE)
                }
            } else {
                // Check if session was already present or if it was the first connect
                sessionHandler.isPresent(clientId).onComplete { present ->
                    finishClientStartup(present.result())
                }.onFailure {
                    logger.severe("Client [$clientId] Error: ${it.message} [${Utils.getCurrentFunctionName()}]")
                    rejectAndCloseEndpoint(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE)
                }
            }
        }
    }

    private fun rejectAndCloseEndpoint(code: MqttConnectReturnCode) {
        endpoint.reject(code)
        if (endpoint.isConnected)
            endpoint.close()
        undeployEndpoint(vertx, this.deploymentID())
    }

    private fun stopEndpoint() {
        if (endpoint.isCleanSession) {
            logger.fine("Client [$clientId] Remove client, it is a clean session [${Utils.getCurrentFunctionName()}]")
            sessionHandler.delClient(clientId)
        } else {
            logger.fine("Client [$clientId] Pause client, it is not a clean session [${Utils.getCurrentFunctionName()}]")
            sessionHandler.pauseClient(clientId)
        }
        undeployEndpoint(vertx, this.deploymentID())
    }

    private fun exceptionHandler(throwable: Throwable) {
        logger.severe("Client [$clientId] Exception: ${throwable.message} [${Utils.getCurrentFunctionName()}]")
        //closeConnection()
    }

    private fun pingHandler() {
        lastPing = Instant.now()
        //endpoint.pong() // A java clients dies when pong is sent
    }

    private fun subscribeHandler(subscribe: MqttSubscribeMessage) {
        // Acknowledge the subscriptions
        val acknowledge = subscribe.topicSubscriptions().map { it.qualityOfService() }
        endpoint.subscribeAcknowledge(subscribe.messageId(), acknowledge)

        // Subscribe
        subscribe.topicSubscriptions().forEach { subscription ->
            logger.fine("Client [$clientId] Subscription for [${subscription.topicName()}] with QoS ${subscription.qualityOfService()} [${Utils.getCurrentFunctionName()}]")
            sessionHandler.subscribeRequest(this, subscription.topicName(), subscription.qualityOfService())
        }
    }

    private fun consumeCommand(message: Message<JsonObject>) {
        val command = message.body()
        when (val key = command.getString(Const.COMMAND_KEY)) {
            Const.COMMAND_STATUS -> {
                logger.info("Client [$clientId] Status command received [${Utils.getCurrentFunctionName()}]")
                message.reply(JsonObject().put("Connected", endpoint.isConnected))
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
            logger.fine("Client [$clientId] Unsubscribe for [${topicName}]} [${Utils.getCurrentFunctionName()}]")
            sessionHandler.unsubscribeRequest(this, topicName)
        }
        endpoint.unsubscribeAcknowledge(unsubscribe.messageId())
    }

    private fun publishStatistics() {
        val data = JsonObject()
            .put("Connected", endpoint.isConnected)
            .put("LastPing", lastPing.toString())
            .put("InFlightMessagesRcv", inFlightMessagesRcv.size)
            .put("InFlightMessagesSnd", inFlightMessagesSnd.size)
        val payload = data.encode()
        if (lastStatisticsMessage != payload) {
            lastStatisticsMessage = payload
            val message = MqttMessage(clientId, "\$SYS/clients/${clientId}/statistics", payload)
            sessionHandler.publishMessage(message)
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Receiving messages from client (publish)
    // -----------------------------------------------------------------------------------------------------------------

    private fun publishHandler(message: MqttPublishMessage) {
        logger.finest { "Client [$clientId] Publish: message [${message.messageId()}] for [${message.topicName()}] with QoS ${message.qosLevel()} [${Utils.getCurrentFunctionName()}]" }
        // Handle QoS levels
        if (message.topicName().startsWith(Const.SYS_TOPIC_NAME)) {
            logger.warning { "Client [$clientId] Publish: message for system topic [${message.topicName()}] not allowed! [${Utils.getCurrentFunctionName()}]" }
        }
        else
        when (message.qosLevel()) {
            MqttQoS.AT_MOST_ONCE -> { // Level 0
                logger.finest { "Client [$clientId] Publish: no acknowledge needed [${Utils.getCurrentFunctionName()}]" }
                sessionHandler.publishMessage(MqttMessage(clientId, message))
            }
            MqttQoS.AT_LEAST_ONCE -> { // Level 1
                logger.finest { "Client [$clientId] Publish: sending acknowledge for id [${message.messageId()}] [${Utils.getCurrentFunctionName()}]" }
                sessionHandler.publishMessage(MqttMessage(clientId, message))
                // TODO: check the result of the publishMessage and send the acknowledge only if the message was delivered
                endpoint.publishAcknowledge(message.messageId())
            }
            MqttQoS.EXACTLY_ONCE -> { // Level 2
                logger.finest { "Client [$clientId] Publish: sending received for id [${message.messageId()}] [${Utils.getCurrentFunctionName()}]" }
                endpoint.publishReceived(message.messageId())
                inFlightMessagesRcv[message.messageId()] = InFlightMessage(MqttMessage(clientId, message))
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
            sessionHandler.publishMessage(inFlightMessage.message)
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

    private fun consumeMessage(busMessage: Message<MqttMessage>) {
        if (!ready) {
            busMessage.reply(false)
        }
        else when (busMessage.body().qosLevel) {
            0 -> consumeMessageQoS0(busMessage)
            1 -> consumeMessageQoS1(busMessage)
            2 -> consumeMessageQoS2(busMessage)
            else -> {
                logger.warning { "Client [$clientId] Subscribe: unknown QoS level [${busMessage.body().qosLevel}] [${Utils.getCurrentFunctionName()}]" }
            }
        }
    }

    private fun consumeMessageQoS0(busMessage: Message<MqttMessage>) {
        val message = busMessage.body().cloneWithNewMessageId(0)
        if (endpoint.isConnected) {
            publishMessage(message)
            logger.finest { "Client [$clientId] QoS [0] message [${message.messageId}] for topic [${message.topicName}] delivered  [${Utils.getCurrentFunctionName()}]" }
        } else {
            logger.finest { "Client [$clientId] QoS [0] message [${message.messageId}] for topic [${message.topicName}] not delivered, client not connected. [${Utils.getCurrentFunctionName()}]" }
        }
    }

    private fun publishMessage(message: MqttMessage) {
        if (!endpoint.isConnected) {
            logger.finest("Client [$clientId] QoS [${message.qosLevel}] message [${message.messageId}] for topic [${message.topicName}] not delivered, client not connected [${Utils.getCurrentFunctionName()}]")
        } else {
            if (message.qosLevel == 0) {
                message.publishToEndpoint(endpoint)
            } else {
                if (inFlightMessagesSnd.size > MAX_IN_FLIGHT_MESSAGES) {
                    logger.warning { "Client [$clientId] QoS [${message.qosLevel}] message [${message.messageId}] for topic [${message.topicName}] not delivered, queue is full [${Utils.getCurrentFunctionName()}]" }
                    // TODO: message must be removed from message store (queued messages)
                } else {
                    inFlightMessagesSnd.addLast(InFlightMessage(message))
                    if (inFlightMessagesSnd.size == 1) {
                        message.publishToEndpoint(endpoint)
                        logger.finest { "Client [$clientId] QoS [${message.qosLevel}] message [${message.messageId}] for topic [${message.topicName}] delivered [${Utils.getCurrentFunctionName()}]" }
                    } else {
                        logger.finest { "Client [$clientId] QoS [${message.qosLevel}] message [${message.messageId}] for topic [${message.topicName}] queued [${Utils.getCurrentFunctionName()}]" }
                    }
                }
            }
        }
    }

    private fun publishMessageCheckNext() {
        if (inFlightMessagesSnd.isNotEmpty()) {
            inFlightMessagesSnd.first().message.publishToEndpoint(endpoint)
            logger.finest { "Client [$clientId] Subscribe: next message [${inFlightMessagesSnd.first().message.messageId}] from queue delivered [${Utils.getCurrentFunctionName()}]" }
        }
    }

    private fun publishMessageCompleted(message: MqttMessage) {
        inFlightMessagesSnd.removeFirst()
        if (message.isQueued) sessionHandler.removeMessage(clientId, message.messageUuid)
    }

    private fun consumeMessageQoS1(busMessage: Message<MqttMessage>) {
        val message = busMessage.body().cloneWithNewMessageId(getNextMessageId())
        if (endpoint.isConnected) {
            publishMessage(message)
            busMessage.reply(true)
        } else {
            logger.finest { "Client [$clientId] QoS [1] message [${message.messageId}] for topic [${message.topicName}] not delivered, client not connected [${Utils.getCurrentFunctionName()}]" }
            busMessage.reply(false)
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
        if (endpoint.isConnected) {
            publishMessage(message)
            busMessage.reply(true)
        } else {
            logger.finest { "Client [$clientId] QoS [2] message [${message.messageId}] for topic [${message.topicName}] not delivered, client not connected [${Utils.getCurrentFunctionName()}]" }
            busMessage.reply(false)
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
                            1 -> endpoint.let { inFlightMessage.message.publishToEndpoint(it) } // TODO: set isDup to true
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
        endpoint.will()?.let { will ->
            if (will.isWillFlag) {
                logger.fine("Client [$clientId] Sending Last-Will message [${Utils.getCurrentFunctionName()}]")
                sessionHandler.publishMessage(MqttMessage(clientId, will))
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Disconnect handling
    // -----------------------------------------------------------------------------------------------------------------

    private fun disconnectHandler() {
        // Graceful disconnect by the client, closeHandler is also called after this
        logger.fine("Client [$clientId] Graceful disconnect [${endpoint.isConnected}] [${Utils.getCurrentFunctionName()}]")
        gracefulDisconnected = true
    }

    private fun closeHandler() {
        logger.fine("Client [$clientId] Close received [${endpoint.isConnected}] [${Utils.getCurrentFunctionName()}]")
        closeConnection()
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Close connection
    // -----------------------------------------------------------------------------------------------------------------

    private fun closeConnection() {
        logger.info("Client [$clientId] Close connection [${endpoint.isConnected}] [${Utils.getCurrentFunctionName()}]")
        if (!gracefulDisconnected) { // if there was no disconnect before
            sendLastWill()
        }
        stopEndpoint()
    }
}