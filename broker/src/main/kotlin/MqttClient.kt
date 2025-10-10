package at.rocworks

import at.rocworks.bus.EventBusAddresses
import at.rocworks.auth.UserManager
import at.rocworks.data.BrokerMessage
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
    private val userManager: UserManager
): AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    private var deployed: Boolean = false
    private var ready: Boolean = false
    private var lastPing: Instant = Instant.MIN
    private var gracefulDisconnected: Boolean = false
    private var lastStatisticsMessage: String = ""

    // Rate limiting - snapshot values for periodic check
    private var lastMessagesIn: Long = 0
    private var lastMessagesOut: Long = 0

    private var nextMessageId: Int = 0
    private fun getNextMessageId(): Int = if (nextMessageId==65535) {
        nextMessageId=1
        nextMessageId
    } else ++nextMessageId

    private data class InFlightMessage(
        val message: BrokerMessage,
        var stage: Int = 1,
        var lastTryTime: Instant = Instant.now(),
        var retryCount: Int = 0
    )

    private val inFlightMessagesRcv = ConcurrentHashMap<Int, InFlightMessage>() // messageId TODO: is concurrent needed?
    private val inFlightMessagesSnd : ConcurrentLinkedDeque<InFlightMessage> = ConcurrentLinkedDeque() // TODO: is concurrent needed?

    private val busConsumers = mutableListOf<MessageConsumer<*>>()
    
    // Authenticated user (null if not authenticated or auth disabled)
    private var authenticatedUser: at.rocworks.data.User? = null


    // create a getter for the client id
    val clientId: String
        get() = endpoint.clientIdentifier()

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    companion object {
        const val MAX_IN_FLIGHT_MESSAGES = 100_000 // TODO make this configurable

        private val logger = Utils.getLogger(this::class.java)


        fun deployEndpoint(vertx: Vertx, endpoint: MqttEndpoint, sessionHandler: SessionHandler, userManager: UserManager) {
            val clientId = endpoint.clientIdentifier()
            logger.fine { "Client [${clientId}] Deploy a new session for [${endpoint.remoteAddress()}] [${Utils.getCurrentFunctionName()}]" }
            // TODO: check if the client is already connected (cluster wide)
            val client = MqttClient(endpoint, sessionHandler, userManager)
            vertx.deployVerticle(client).onComplete {
                client.startEndpoint()
            }
        }

        fun undeployEndpoint(vertx: Vertx, deploymentID: String) {
            logger.fine { "DeploymentID [$deploymentID] undeploy [${Utils.getCurrentFunctionName()}]" }
            vertx.undeploy(deploymentID).onComplete {
                logger.fine { "DeploymentID [$deploymentID] undeployed [${Utils.getCurrentFunctionName()}]" }
            }
        }


        fun getCommandAddress(clientId: String) = EventBusAddresses.Client.commands(clientId)
        fun getMessagesAddress(clientId: String) = EventBusAddresses.Client.messages(clientId)
    }

    override fun start() {
        vertx.setPeriodic(1000) { receivingInFlightMessagesPeriodicCheck() }
        vertx.setPeriodic(1000) { sendingInFlightMessagesPeriodicCheck() }
        vertx.setPeriodic(1000) { checkRateLimits() }
    }

    override fun stop() {
        logger.fine { "Client [${clientId}] Stop [${Utils.getCurrentFunctionName()}] " }
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
                information.put("clientAddress", endpoint.remoteAddress().toString())
                information.put("sessionExpiryInterval", endpoint.keepAliveTimeSeconds())
                sessionHandler.setClient(clientId, endpoint.isCleanSession, information).onComplete {
                    logger.fine { "Dequeue messages for client [$clientId] [${Utils.getCurrentFunctionName()}]" }
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

            // Authentication check
            if (userManager.isUserManagementEnabled()) {
                val username = endpoint.auth()?.username
                val password = endpoint.auth()?.password
                
                if (username == null || password == null) {
                    // No credentials provided - treat as anonymous user
                    logger.info("Client [$clientId] No credentials provided, using anonymous access")
                    
                    // Retrieve the Anonymous user from UserManager to ensure proper permissions
                    vertx.executeBlocking(java.util.concurrent.Callable<at.rocworks.data.User?> {
                        try {
                            kotlinx.coroutines.runBlocking {
                                userManager.getUser(at.rocworks.Const.ANONYMOUS_USER)
                            }
                        } catch (e: Exception) {
                            logger.warning("Client [$clientId] Error getting Anonymous user: ${e.message}")
                            // Create a default Anonymous user if retrieval fails
                            at.rocworks.data.User(
                                username = at.rocworks.Const.ANONYMOUS_USER,
                                passwordHash = "",
                                enabled = true,
                                canSubscribe = true,
                                canPublish = true,
                                isAdmin = false
                            )
                        }
                    }).onComplete { result ->
                        if (result.succeeded() && result.result() != null) {
                            authenticatedUser = result.result()
                            logger.fine { "Client [$clientId] Using Anonymous user for unauthenticated access" }
                            proceedWithConnection()
                        } else {
                            // If we can't get the Anonymous user, create a default one
                            authenticatedUser = at.rocworks.data.User(
                                username = at.rocworks.Const.ANONYMOUS_USER,
                                passwordHash = "",
                                enabled = true,
                                canSubscribe = true,
                                canPublish = true,
                                isAdmin = false
                            )
                            logger.warning("Client [$clientId] Using default Anonymous user (could not retrieve from store)")
                            proceedWithConnection()
                        }
                    }
                    return
                }
                
                // Now authenticate() returns Future<User?>, so we handle it directly
                userManager.authenticate(username, password).onComplete { authResult ->
                    if (authResult.succeeded() && authResult.result() != null) {
                        authenticatedUser = authResult.result()
                        logger.info("Client [$clientId] Authentication successful for user [$username]")
                        proceedWithConnection()
                    } else {
                        if (authResult.failed()) {
                            logger.warning("Client [$clientId] Authentication error: ${authResult.cause()?.message}")
                        } else {
                            logger.warning("Client [$clientId] Authentication failed for user [$username]")
                        }
                        rejectAndCloseEndpoint(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED)
                    }
                }
            } else {
                // Authentication disabled, proceed normally
                proceedWithConnection()
            }
        }
    }

    private fun proceedWithConnection() {
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
                logger.fine { "Dequeue messages for client [$clientId] [${Utils.getCurrentFunctionName()}]" }
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

    private fun rejectAndCloseEndpoint(code: MqttConnectReturnCode) {
        endpoint.reject(code)
        if (endpoint.isConnected)
            endpoint.close()
        undeployEndpoint(vertx, this.deploymentID())
    }

    private fun stopEndpoint() {
        if (endpoint.isCleanSession) {
            logger.fine { "Client [$clientId] Remove client, it is a clean session [${Utils.getCurrentFunctionName()}]" }
            sessionHandler.delClient(clientId)
        } else {
            logger.fine { "Client [$clientId] Pause client, it is not a clean session [${Utils.getCurrentFunctionName()}]" }
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
        val username = authenticatedUser?.username ?: at.rocworks.Const.ANONYMOUS_USER

        val acknowledgements = mutableListOf<MqttQoS>()

        subscribe.topicSubscriptions().forEach { subscription ->
            val topic = subscription.topicName()
            var allowed = true

            // Root wildcard policy
            if (topic == "#" && !Monster.allowRootWildcardSubscription()) {
                allowed = false
                logger.warning("Client [$clientId] Root wildcard subscription '#' rejected (AllowRootWildcardSubscription=false)")
            }

            // ACL check (only if still allowed so far and user management enabled)
            if (allowed && userManager.isUserManagementEnabled()) {
                if (!userManager.canSubscribe(username, topic)) {
                    allowed = false
                    logger.warning("Client [$clientId] Subscription DENIED for [$topic] - user [$username] lacks permission")
                }
            }

            // Build acknowledgement list
            acknowledgements.add(if (allowed) subscription.qualityOfService() else MqttQoS.FAILURE)

            // Forward allowed subscriptions to SessionHandler
            if (allowed) {
                logger.fine { "Client [$clientId] Subscription ALLOWED for [$topic] with QoS ${subscription.qualityOfService()}" }
                sessionHandler.subscribeRequest(this, topic, subscription.qualityOfService())
            } else {
                logger.fine { "Client [$clientId] Subscription REJECTED for [$topic]" }
            }
        }

        // Send SUBACK with per-subscription results
        endpoint.subscribeAcknowledge(subscribe.messageId(), acknowledgements)
    }

    private fun consumeCommand(message: Message<JsonObject>) {
        val command = message.body()
        when (val key = command.getString(Const.COMMAND_KEY)) {
            Const.COMMAND_STATUS -> {
                logger.info("Client [$clientId] Status command received [${Utils.getCurrentFunctionName()}]")
                message.reply(JsonObject().put("Connected", endpoint.isConnected))
            }
            Const.COMMAND_STATISTICS -> {
                logger.finest("Client [$clientId] Statistics command received [${Utils.getCurrentFunctionName()}]")
                message.reply(getConnectionStatistics())
            }
            Const.COMMAND_DISCONNECT -> {
                val reason = command.getString("Reason")
                logger.info("Client [$clientId] Disconnect command received" + (reason?.let { ": $it" } ?: "") + " [${Utils.getCurrentFunctionName()}]")
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
            logger.fine { "Client [$clientId] Unsubscribe for [${topicName}]} [${Utils.getCurrentFunctionName()}]" }
            sessionHandler.unsubscribeRequest(this, topicName)
        }
        endpoint.unsubscribeAcknowledge(unsubscribe.messageId())
    }

    /**
     * Get connection statistics for this client
     * Used by SessionHandler for unified metrics publishing
     */
    fun getConnectionStatistics(): JsonObject {
        return JsonObject()
            .put("connected", endpoint.isConnected)
            .put("lastPing", lastPing.toString())
            .put("inFlightMessagesRcv", inFlightMessagesRcv.size)
            .put("inFlightMessagesSnd", inFlightMessagesSnd.size)
    }

    private fun checkRateLimits() {
        val maxPublishRate = Monster.getMaxPublishRate()
        val maxSubscribeRate = Monster.getMaxSubscribeRate()

        // Skip if both unlimited
        if (maxPublishRate == 0 && maxSubscribeRate == 0) return

        // Get current metrics from SessionHandler
        val metrics = sessionHandler.getClientMetrics(clientId) ?: return

        val currentMessagesIn = metrics.messagesIn.get()
        val currentMessagesOut = metrics.messagesOut.get()

        // Calculate deltas since last check (approximately 1 second ago)
        val deltaIn = currentMessagesIn - lastMessagesIn
        val deltaOut = currentMessagesOut - lastMessagesOut

        // Check publish rate limit
        if (maxPublishRate > 0 && deltaIn > maxPublishRate) {
            logger.warning("Client [$clientId] Publish rate limit exceeded: $deltaIn > $maxPublishRate msg/s - disconnecting")
            sessionHandler.disconnectClient(clientId, "Publish rate limit exceeded")
            return
        }

        // Check subscribe rate limit
        if (maxSubscribeRate > 0 && deltaOut > maxSubscribeRate) {
            logger.warning("Client [$clientId] Subscribe rate limit exceeded: $deltaOut > $maxSubscribeRate msg/s - disconnecting")
            sessionHandler.disconnectClient(clientId, "Subscribe rate limit exceeded")
            return
        }

        // Update snapshot values for next check
        lastMessagesIn = currentMessagesIn
        lastMessagesOut = currentMessagesOut
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Receiving messages from client (publish)
    // -----------------------------------------------------------------------------------------------------------------

    private fun publishHandler(message: MqttPublishMessage) {
        logger.finest { "Client [$clientId] Publish: message [${message.messageId()}] for [${message.topicName()}] with QoS ${message.qosLevel()} [${Utils.getCurrentFunctionName()}]" }

        // Increment messages received from client
        sessionHandler.incrementMessagesIn(clientId)
        
        val topicName = message.topicName()
        
        // Check system topic restrictions
        if (topicName.startsWith(Const.SYS_TOPIC_NAME)) {
            logger.warning { "Client [$clientId] Publish: message for system topic [$topicName] not allowed! [${Utils.getCurrentFunctionName()}]" }
            return
        }
        
        // Check ACL permissions
        val username = authenticatedUser?.username ?: at.rocworks.Const.ANONYMOUS_USER
        val canPublish = if (userManager.isUserManagementEnabled()) {
            userManager.canPublish(username, topicName)
        } else {
            true // Allow all if user management is disabled
        }
        
        if (!canPublish) {
            logger.warning("Client [$clientId] Publish DENIED for [$topicName] - user [$username] lacks permission")
            
            // Disconnect client if configured
            if (userManager.shouldDisconnectOnUnauthorized()) {
                logger.warning("Client [$clientId] Disconnecting due to unauthorized publish attempt: $topicName")
            sessionHandler.disconnectClient(clientId, "Unauthorized publish to $topicName")
            return
            }
            
            // Otherwise, just ignore the message (silent drop)
            return
        }
        
        logger.finest { "Client [$clientId] Publish ALLOWED for [$topicName] - user [$username]" }

        // Handle QoS levels
        when (message.qosLevel()) {
            MqttQoS.AT_MOST_ONCE -> { // Level 0
                logger.finest { "Client [$clientId] Publish: no acknowledge needed [${Utils.getCurrentFunctionName()}]" }
                sessionHandler.publishMessage(BrokerMessage(clientId, message))
            }
            MqttQoS.AT_LEAST_ONCE -> { // Level 1
                logger.finest { "Client [$clientId] Publish: sending acknowledge for id [${message.messageId()}] [${Utils.getCurrentFunctionName()}]" }
                sessionHandler.publishMessage(BrokerMessage(clientId, message))
                // TODO: check the result of the publishMessage and send the acknowledge only if the message was delivered
                endpoint.publishAcknowledge(message.messageId())
            }
            MqttQoS.EXACTLY_ONCE -> { // Level 2
                logger.finest { "Client [$clientId] Publish: sending received for id [${message.messageId()}] [${Utils.getCurrentFunctionName()}]" }
                endpoint.publishReceived(message.messageId())
                inFlightMessagesRcv[message.messageId()] = InFlightMessage(BrokerMessage(clientId, message))
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

    private fun consumeMessage(busMessage: Message<BrokerMessage>) {
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

    private fun consumeMessageQoS0(busMessage: Message<BrokerMessage>) {
        val message = busMessage.body().cloneWithNewMessageId(0)
        if (endpoint.isConnected) {
            publishMessage(message)
            logger.finest { "Client [$clientId] QoS [0] message [${message.messageId}] for topic [${message.topicName}] delivered  [${Utils.getCurrentFunctionName()}]" }
        } else {
            logger.finest { "Client [$clientId] QoS [0] message [${message.messageId}] for topic [${message.topicName}] not delivered, client not connected. [${Utils.getCurrentFunctionName()}]" }
        }
    }

    private fun publishMessage(message: BrokerMessage) {
        if (!endpoint.isConnected) {
            logger.finest("Client [$clientId] QoS [${message.qosLevel}] message [${message.messageId}] for topic [${message.topicName}] not delivered, client not connected [${Utils.getCurrentFunctionName()}]")
        } else {
            // Increment messages sent to client
            sessionHandler.incrementMessagesOut(clientId)

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

    private fun publishMessageCompleted(message: BrokerMessage) {
        inFlightMessagesSnd.removeFirst()
        if (message.isQueued) sessionHandler.removeMessage(clientId, message.messageUuid)
    }

    private fun consumeMessageQoS1(busMessage: Message<BrokerMessage>) {
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

    private fun consumeMessageQoS2(busMessage: Message<BrokerMessage>) {
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
                logger.fine { "Client [$clientId] Sending Last-Will message [${Utils.getCurrentFunctionName()}]" }
                sessionHandler.publishMessage(BrokerMessage(clientId, will))
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Disconnect handling
    // -----------------------------------------------------------------------------------------------------------------

    private fun disconnectHandler() {
        // Graceful disconnect by the client, closeHandler is also called after this
        logger.fine { "Client [$clientId] Graceful disconnect [${endpoint.isConnected}] [${Utils.getCurrentFunctionName()}]" }
        gracefulDisconnected = true
    }

    private fun closeHandler() {
        logger.fine { "Client [$clientId] Close received [${endpoint.isConnected}] [${Utils.getCurrentFunctionName()}]" }
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
        if (endpoint.isConnected) {
            logger.fine { "Client [$clientId] Send close  [${Utils.getCurrentFunctionName()}]" }
            endpoint.close()
        }
        stopEndpoint()
    }
}