package at.rocworks

import at.rocworks.bus.EventBusAddresses
import at.rocworks.auth.UserManager
import at.rocworks.data.BrokerMessage
import at.rocworks.data.BulkClientMessage
import at.rocworks.handlers.SessionHandler
import io.netty.handler.codec.mqtt.MqttConnectReturnCode
import io.netty.handler.codec.mqtt.MqttProperties
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
import io.vertx.mqtt.messages.codes.MqttPubAckReasonCode
import io.vertx.mqtt.messages.codes.MqttSubAckReasonCode
import io.vertx.mqtt.messages.codes.MqttUnsubAckReasonCode
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

    // Queue-first state machine for QoS 1+ message delivery
    private var isProcessingQueue = false
    private var triggerPending = false

    // Message cache for bulk fetching (reduces database queries)
    private val messageCache = mutableListOf<BrokerMessage>()
    private val messageCacheLock = Any()
    private val MESSAGE_CACHE_SIZE = 1000

    private val busConsumers = mutableListOf<MessageConsumer<*>>()
    
    // Authenticated user (null if not authenticated or auth disabled)
    private var authenticatedUser: at.rocworks.data.User? = null
    
    // MQTT v5.0 Topic Aliases (Phase 4)
    // Session-specific: Map<aliasId, topicName>
    // Cleared on disconnect (not persistent)
    private val topicAliases = mutableMapOf<Int, String>()
    
    // MQTT v5.0 Flow Control (Phase 8)
    // Client's Receive Maximum - limit for outstanding QoS 1/2 messages
    private var clientReceiveMaximum = 65535  // Default per MQTT v5 spec
    
    // MQTT v5.0 Will Delay Interval (Phase 8)
    // Delay in seconds before publishing Last Will message
    private var willDelayInterval: Long = 0L  // Default: no delay
    private var willDelayTimerId: Long? = null  // Timer ID for cancellation
    
    // Effective clean session flag (considers MQTT v5 session expiry interval)
    // For MQTT v5: sessionExpiry == 0 means clean, > 0 means persistent
    // For MQTT v3.1.1: uses endpoint.isCleanSession
    private var effectiveCleanSession: Boolean = true


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
        fun getQueueTriggerAddress(clientId: String) = EventBusAddresses.Client.queueTrigger(clientId)
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

    /**
     * Called when a trigger is received indicating that a message is available in the queue.
     * Part of the queue-first delivery mechanism for QoS 1+ persistent session clients.
     */
    private fun onMessageAvailable() {
        if (!ready || !endpoint.isConnected) {
            logger.finest { "Client [$clientId] Trigger received but not ready or not connected [${Utils.getCurrentFunctionName()}]" }
            return
        }

        if (isProcessingQueue) {
            // Already processing - remember to check again after current batch
            triggerPending = true
            logger.finest { "Client [$clientId] Trigger received while processing, setting pending flag [${Utils.getCurrentFunctionName()}]" }
            return
        }

        startQueueProcessing()
    }

    /**
     * Start processing the message queue for this client.
     */
    private fun startQueueProcessing() {
        isProcessingQueue = true
        processNextMessage()
    }

    /**
     * Process the next message from the queue.
     * Uses a message cache to reduce database queries - fetches messages in bulk.
     * On PUBACK/PUBCOMP, this is called again to process the next message.
     */
    private fun processNextMessage() {
        if (!endpoint.isConnected) {
            isProcessingQueue = false
            triggerPending = false
            clearMessageCache()
            return
        }

        fetchNextMessageFromCacheOrDb().onComplete { result ->
            if (result.failed()) {
                logger.warning { "Client [$clientId] Error fetching next pending message: ${result.cause()?.message} [${Utils.getCurrentFunctionName()}]" }
                isProcessingQueue = false
                return@onComplete
            }

            val msg = result.result()
            if (msg == null) {
                // Queue empty
                if (triggerPending) {
                    triggerPending = false
                    logger.finest { "Client [$clientId] Queue empty but trigger pending, checking again [${Utils.getCurrentFunctionName()}]" }
                    processNextMessage()  // Try once more
                } else {
                    logger.finest { "Client [$clientId] Queue empty, going idle [${Utils.getCurrentFunctionName()}]" }
                    isProcessingQueue = false  // Go idle
                }
                return@onComplete
            }

            // Messages are already marked in-flight during bulk fetch (fetchNextMessageFromCacheOrDb)
            // Clone with new message ID and publish immediately
            val msgWithId = msg.cloneWithNewMessageId(getNextMessageId())
            logger.finest { "Client [$clientId] Publishing queued message [${msgWithId.messageId}] for topic [${msgWithId.topicName}] [${Utils.getCurrentFunctionName()}]" }
            publishMessage(msgWithId)
        }
    }

    /**
     * Fetch the next message from local cache, or fetch a batch from the database if cache is empty.
     */
    private fun fetchNextMessageFromCacheOrDb(): Future<BrokerMessage?> {
        // Check cache first (synchronized for thread safety)
        synchronized(messageCacheLock) {
            if (messageCache.isNotEmpty()) {
                return Future.succeededFuture(messageCache.removeFirst())
            }
        }

        // Cache empty - fetch a batch from database
        return sessionHandler.fetchPendingMessages(clientId, MESSAGE_CACHE_SIZE).map { messages ->
            if (messages.isEmpty()) {
                null
            } else {
                // Mark all fetched messages as in-flight at once
                val uuids = messages.map { it.messageUuid }
                sessionHandler.markMessagesInFlight(clientId, uuids)

                // Put remaining messages in cache (all except first)
                synchronized(messageCacheLock) {
                    if (messages.size > 1) {
                        messageCache.addAll(messages.drop(1))
                    }
                }
                logger.fine { "Client [$clientId] Fetched ${messages.size} messages from database, cached ${messages.size - 1} [${Utils.getCurrentFunctionName()}]" }
                messages.first()
            }
        }
    }

    /**
     * Clear the message cache (called on disconnect/reconnect).
     */
    private fun clearMessageCache() {
        synchronized(messageCacheLock) {
            messageCache.clear()
        }
    }

    fun startEndpoint() {
        logger.info("Client [$clientId] Request to connect. Clean session [${endpoint.isCleanSession}] protocol [${endpoint.protocolVersion()}] [${Utils.getCurrentFunctionName()}]")
        // protocolVersion: 3=MQTTv31, 4=MQTTv311, 5=MQTTv5
        val isMqtt5 = endpoint.protocolVersion() == 5
        
        // Parse MQTT5 properties from CONNECT packet
        var mqtt5SessionExpiryInterval = 0L
        var mqtt5ReceiveMaximum = 65535
        var mqtt5MaximumPacketSize = 268435456L
        var mqtt5TopicAliasMaximum = 0
        
        if (isMqtt5) {
            logger.info("Client [$clientId] MQTT 5.0 connection accepted")
            // Parse MQTT5 CONNECT properties
            val props = endpoint.connectProperties()
            logger.info("Client [$clientId] CONNECT properties count: ${props.listAll().size}")
            props.listAll().forEach { p ->
                logger.info("Client [$clientId] CONNECT Property [${p.propertyId()}] = ${p.value()}")
                when (p.propertyId()) {
                    17 -> mqtt5SessionExpiryInterval = (p.value() as? Number)?.toLong() ?: 0L  // Session Expiry Interval
                    24 -> willDelayInterval = (p.value() as? Number)?.toLong() ?: 0L  // Will Delay Interval
                    33 -> mqtt5ReceiveMaximum = (p.value() as? Number)?.toInt() ?: 65535  // Receive Maximum
                    39 -> mqtt5MaximumPacketSize = (p.value() as? Number)?.toLong() ?: 268435456L  // Maximum Packet Size
                    34 -> mqtt5TopicAliasMaximum = (p.value() as? Number)?.toInt() ?: 0  // Topic Alias Maximum
                    else -> logger.fine("MQTT5 Property [${p.propertyId()}] = ${p.value()}")
                }
            }
            logger.info("Client [$clientId] MQTT5 properties: sessionExpiry=$mqtt5SessionExpiryInterval, receiveMax=$mqtt5ReceiveMaximum, maxPacketSize=$mqtt5MaximumPacketSize, willDelay=$willDelayInterval")
            
            // Store client's Receive Maximum for flow control (Phase 8)
            clientReceiveMaximum = mqtt5ReceiveMaximum
        }
        
        // For MQTT v5, clean session is determined by session expiry interval (0 = clean, > 0 = persistent)
        effectiveCleanSession = if (isMqtt5) {
            mqtt5SessionExpiryInterval == 0L
        } else {
            endpoint.isCleanSession
        }
        
        run {
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
                // Register with Any type to handle both BrokerMessage and BulkClientMessage
                busConsumers.add(vertx.eventBus().consumer<Any>(getMessagesAddress(clientId)) { busMessage ->
                    handleBusMessage(busMessage)
                })
                // Queue trigger handler for queue-first delivery (QoS 1+ persistent sessions)
                busConsumers.add(vertx.eventBus().consumer<String>(getQueueTriggerAddress(clientId)) { _ ->
                    onMessageAvailable()
                })
            } else {
                logger.severe("Client [$clientId] Already deployed [${Utils.getCurrentFunctionName()}]")
            }

            // Set last will
            sessionHandler.setLastWill(clientId, endpoint.will())

            fun finishClientStartup(present: Boolean) {
                // Cancel any pending Will Delay timer (client reconnected before will was published)
                willDelayTimerId?.let { timerId ->
                    vertx.cancelTimer(timerId)
                    willDelayTimerId = null
                    logger.info("Client [$clientId] Reconnected - canceling pending Will Delay timer")
                }
                
                // Accept connection
                if (isMqtt5) {
                    // MQTT v5.0: Send CONNACK with properties (Phase 7)
                    val connackProps = MqttProperties()
                    
                    // Session Expiry Interval (17) - Echo back or override if needed
                    connackProps.add(MqttProperties.IntegerProperty(17, mqtt5SessionExpiryInterval.toInt()))
                    
                    // Assigned Client Identifier (18) - Only if client provided empty ID
                    // Note: Vert.x automatically assigns an ID, so we check if it was auto-generated
                    if (clientId.startsWith("auto-")) {
                        connackProps.add(MqttProperties.StringProperty(18, clientId))
                    }
                    
                    // Server Keep Alive (19) - Override client's keep-alive if needed
                    // Use the endpoint's negotiated keep-alive value
                    val serverKeepAlive = endpoint.keepAliveTimeSeconds()
                    if (serverKeepAlive > 0) {
                        connackProps.add(MqttProperties.IntegerProperty(19, serverKeepAlive))
                    }
                    
                    // Receive Maximum (33) - Server's limit for outstanding QoS 1/2 messages
                    connackProps.add(MqttProperties.IntegerProperty(33, 100))
                    
                    // Maximum QoS (36) - Server supports QoS 0, 1, and 2
                    connackProps.add(MqttProperties.IntegerProperty(36, 2))
                    
                    // Retain Available (37) - Server supports retained messages
                    connackProps.add(MqttProperties.IntegerProperty(37, 1))  // 1 = available
                    
                    // Maximum Packet Size (39) - Server's maximum packet size
                    connackProps.add(MqttProperties.IntegerProperty(39, 268435455))  // Max allowed by MQTT v5
                    
                    // Topic Alias Maximum (34) - Server's limit for topic aliases (Phase 4)
                    connackProps.add(MqttProperties.IntegerProperty(34, 10))
                    
                    // Wildcard Subscription Available (40)
                    connackProps.add(MqttProperties.IntegerProperty(40, 1))  // 1 = available
                    
                    // Subscription Identifier Available (41)
                    connackProps.add(MqttProperties.IntegerProperty(41, 0))  // 0 = not supported yet
                    
                    // Shared Subscription Available (42)
                    connackProps.add(MqttProperties.IntegerProperty(42, 1))  // 1 = available
                    
                    endpoint.accept(present, connackProps)
                    logger.info("Client [$clientId] MQTT5 CONNACK sent with server properties")
                } else {
                    // MQTT v3.1.1: Simple accept
                    endpoint.accept(present)
                }

                // Set client to connected
                val information = JsonObject()
                information.put("RemoteAddress", endpoint.remoteAddress().toString())
                information.put("LocalAddress", endpoint.localAddress().toString())
                information.put("ProtocolVersion", endpoint.protocolVersion())
                information.put("SSL", endpoint.isSsl)
                information.put("AutoKeepAlive", endpoint.isAutoKeepAlive)
                information.put("KeepAliveTimeSeconds", endpoint.keepAliveTimeSeconds())
                information.put("clientAddress", endpoint.remoteAddress().toString())
                information.put("sessionExpiryInterval", if (isMqtt5) mqtt5SessionExpiryInterval else endpoint.keepAliveTimeSeconds().toLong())
                sessionHandler.setClient(clientId, effectiveCleanSession, information).onComplete {
                    if (endpoint.isConnected) {
                        // Now safe to mark as ready for new messages
                        ready = true
                        sessionHandler.onlineClient(clientId)

                        // For persistent sessions, reset any stale in-flight messages and send trigger
                        // This handles the case where previous connection died with messages in-flight
                        if (!effectiveCleanSession) {
                            sessionHandler.resetInFlightMessages(clientId).onComplete {
                                logger.fine { "Client [$clientId] Reset in-flight messages and sending initial queue trigger [${Utils.getCurrentFunctionName()}]" }
                                sessionHandler.sendMessageAvailableTrigger(clientId)
                            }
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
        val isMqtt5 = endpoint.protocolVersion() == 5
        
        // Parse MQTT5 properties for CONNACK (if not already parsed)
        var mqtt5SessionExpiryInterval = 0L
        if (isMqtt5) {
            val props = endpoint.connectProperties()
            props.listAll().forEach { p ->
                if (p.propertyId() == 17) {
                    mqtt5SessionExpiryInterval = (p.value() as? Number)?.toLong() ?: 0L
                }
            }
        }
        
        fun finishClientStartup(present: Boolean) {
            // Accept connection
            if (isMqtt5) {
                // MQTT v5.0: Send CONNACK with properties (Phase 7)
                val connackProps = MqttProperties()
                
                // Session Expiry Interval (17) - Echo back or override if needed
                connackProps.add(MqttProperties.IntegerProperty(17, mqtt5SessionExpiryInterval.toInt()))
                
                // Assigned Client Identifier (18) - Only if client provided empty ID
                // Note: Vert.x automatically assigns an ID, so we check if it was auto-generated
                if (clientId.startsWith("auto-")) {
                    connackProps.add(MqttProperties.StringProperty(18, clientId))
                }
                
                // Server Keep Alive (19) - Override client's keep-alive if needed
                // Use the endpoint's negotiated keep-alive value
                val serverKeepAlive = endpoint.keepAliveTimeSeconds()
                if (serverKeepAlive > 0) {
                    connackProps.add(MqttProperties.IntegerProperty(19, serverKeepAlive))
                }
                
                // Receive Maximum (33) - Server's limit for outstanding QoS 1/2 messages
                connackProps.add(MqttProperties.IntegerProperty(33, 100))
                
                // Maximum QoS (36) - Server supports QoS 0, 1, and 2
                connackProps.add(MqttProperties.IntegerProperty(36, 2))
                
                // Retain Available (37) - Server supports retained messages
                connackProps.add(MqttProperties.IntegerProperty(37, 1))  // 1 = available
                
                // Maximum Packet Size (39) - Server's maximum packet size
                connackProps.add(MqttProperties.IntegerProperty(39, 268435455))  // Max allowed by MQTT v5
                
                // Topic Alias Maximum (34) - Server's limit for topic aliases (Phase 4)
                connackProps.add(MqttProperties.IntegerProperty(34, 10))
                
                // Wildcard Subscription Available (40)
                connackProps.add(MqttProperties.IntegerProperty(40, 1))  // 1 = available
                
                // Subscription Identifier Available (41)
                connackProps.add(MqttProperties.IntegerProperty(41, 0))  // 0 = not supported yet
                
                // Shared Subscription Available (42)
                connackProps.add(MqttProperties.IntegerProperty(42, 1))  // 1 = available
                
                endpoint.accept(present, connackProps)
                logger.info("Client [$clientId] MQTT5 CONNACK sent with server properties")
            } else {
                // MQTT v3.1.1: Simple accept
                endpoint.accept(present)
            }

            // Set client to connected
            val information = JsonObject()
            information.put("RemoteAddress", endpoint.remoteAddress().toString())
            information.put("LocalAddress", endpoint.localAddress().toString())
            information.put("ProtocolVersion", endpoint.protocolVersion())
            information.put("SSL", endpoint.isSsl)
            information.put("AutoKeepAlive", endpoint.isAutoKeepAlive)
            information.put("KeepAliveTimeSeconds", endpoint.keepAliveTimeSeconds())
            sessionHandler.setClient(clientId, effectiveCleanSession, information).onComplete {
                if (endpoint.isConnected) {
                    // Now safe to mark as ready for new messages
                    ready = true
                    sessionHandler.onlineClient(clientId)

                    // For persistent sessions, reset any stale in-flight messages and send trigger
                    // This handles the case where previous connection died with messages in-flight
                    if (!effectiveCleanSession) {
                        sessionHandler.resetInFlightMessages(clientId).onComplete {
                            logger.fine { "Client [$clientId] Reset in-flight messages and sending initial queue trigger [${Utils.getCurrentFunctionName()}]" }
                            sessionHandler.sendMessageAvailableTrigger(clientId)
                        }
                    }
                }
            }
        }

        // Accept connection
        if (effectiveCleanSession) {
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
        // Reset queue processing state
        isProcessingQueue = false
        triggerPending = false
        clearMessageCache()

        // Clear topic aliases (MQTT v5.0 Phase 4)
        // Topic aliases are session-specific but NOT persistent across disconnects
        if (topicAliases.isNotEmpty()) {
            logger.fine { "Client [$clientId] Clearing ${topicAliases.size} topic aliases on disconnect" }
            topicAliases.clear()
        }

        if (effectiveCleanSession) {
            logger.fine { "Client [$clientId] Remove client, it is a clean session [${Utils.getCurrentFunctionName()}]" }
            sessionHandler.delClient(clientId)
        } else {
            logger.fine { "Client [$clientId] Pause client, it is not a clean session [${Utils.getCurrentFunctionName()}]" }
            // Note: in-flight messages will be reset when client reconnects
            sessionHandler.pauseClient(clientId)
        }
        undeployEndpoint(vertx, this.deploymentID())
    }

    private fun exceptionHandler(throwable: Throwable) {
        logger.severe("Client [$clientId] Exception: ${throwable.message} [${Utils.getCurrentFunctionName()}]")
        closeConnection()
    }

    private fun pingHandler() {
        lastPing = Instant.now()
        //endpoint.pong() // A java clients dies when pong is sent
    }

    private fun subscribeHandler(subscribe: MqttSubscribeMessage) {
        val username = authenticatedUser?.username ?: at.rocworks.Const.ANONYMOUS_USER
        val protocolVersion = endpoint.protocolVersion()

        // For MQTT v5: use reason codes; for v3.1.1: use QoS values
        if (protocolVersion == 5) {
            // MQTT v5.0: Use MqttSubAckReasonCode
            val reasonCodes = mutableListOf<MqttSubAckReasonCode>()

            subscribe.topicSubscriptions().forEach { subscription ->
                val topic = subscription.topicName()
                var allowed = true
                var reasonCode: MqttSubAckReasonCode
                
                // MQTT v5 subscription options - access via subscriptionOption()
                val subOption = subscription.subscriptionOption()
                val noLocal = subOption?.isNoLocal ?: false  // Don't send back messages this client published
                val retainHandling = subOption?.retainHandling()?.value() ?: 0  // 0=send retained, 1=send if new, 2=never send

                // Root wildcard policy
                if (topic == "#" && !Monster.allowRootWildcardSubscription()) {
                    allowed = false
                    reasonCode = MqttSubAckReasonCode.TOPIC_FILTER_INVALID
                    logger.warning("Client [$clientId] Root wildcard subscription '#' rejected (AllowRootWildcardSubscription=false)")
                } else if (allowed && userManager.isUserManagementEnabled() && !userManager.canSubscribe(username, topic)) {
                    // ACL check
                    allowed = false
                    reasonCode = MqttSubAckReasonCode.NOT_AUTHORIZED
                    logger.warning("Client [$clientId] Subscription DENIED for [$topic] - user [$username] lacks permission")
                } else {
                    // Subscription allowed - return granted QoS
                    reasonCode = MqttSubAckReasonCode.qosGranted(subscription.qualityOfService())
                }

                reasonCodes.add(reasonCode)

                // Forward allowed subscriptions to SessionHandler
                if (allowed) {
                    logger.fine { "Client [$clientId] Subscription ALLOWED for [$topic] with QoS ${subscription.qualityOfService()} noLocal=$noLocal retainHandling=$retainHandling" }
                    sessionHandler.subscribeRequest(this, topic, subscription.qualityOfService(), noLocal, retainHandling)
                } else {
                    logger.fine { "Client [$clientId] Subscription REJECTED for [$topic] - reason: $reasonCode" }
                }
            }

            // Send MQTT v5 SUBACK with reason codes
            endpoint.subscribeAcknowledge(subscribe.messageId(), reasonCodes, MqttProperties.NO_PROPERTIES)
        } else {
            // MQTT v3.1.1: Use QoS values (backward compatibility)
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

            // Send MQTT v3.1.1 SUBACK with QoS values
            endpoint.subscribeAcknowledge(subscribe.messageId(), acknowledgements)
        }
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
        val protocolVersion = endpoint.protocolVersion()

        if (protocolVersion == 5) {
            // MQTT v5.0: Use reason codes for each topic
            val reasonCodes = mutableListOf<MqttUnsubAckReasonCode>()

            unsubscribe.topics().forEach { topicName ->
                logger.fine { "Client [$clientId] Unsubscribe for [${topicName}] [${Utils.getCurrentFunctionName()}]" }
                sessionHandler.unsubscribeRequest(this, topicName)
                // Assuming unsubscribe is always successful (no validation in current impl)
                reasonCodes.add(MqttUnsubAckReasonCode.SUCCESS)
            }

            // Send MQTT v5 UNSUBACK with reason codes
            endpoint.unsubscribeAcknowledge(unsubscribe.messageId(), reasonCodes, MqttProperties.NO_PROPERTIES)
        } else {
            // MQTT v3.1.1: Simple acknowledgement
            unsubscribe.topics().forEach { topicName ->
                logger.fine { "Client [$clientId] Unsubscribe for [${topicName}] [${Utils.getCurrentFunctionName()}]" }
                sessionHandler.unsubscribeRequest(this, topicName)
            }
            endpoint.unsubscribeAcknowledge(unsubscribe.messageId())
        }
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
        
        var topicName = message.topicName()
        
        // MQTT v5.0: Handle Topic Alias (Phase 4)
        if (endpoint.protocolVersion() == 5) {
            val props = message.properties()
            val topicAliasProperty = props?.getProperty(35)  // Property ID 35 = Topic Alias
            
            if (topicAliasProperty != null) {
                val topicAlias = (topicAliasProperty.value() as? Number)?.toInt()
                
                if (topicAlias != null && topicAlias > 0) {
                    if (topicName.isEmpty()) {
                        // Client is using existing alias - resolve topic name
                        val resolvedTopic = topicAliases[topicAlias]
                        if (resolvedTopic != null) {
                            topicName = resolvedTopic
                            logger.fine { "Client [$clientId] Resolved Topic Alias $topicAlias → $topicName" }
                        } else {
                            logger.warning("Client [$clientId] Topic Alias $topicAlias not found - ignoring message")
                            return
                        }
                    } else {
                        // Client is establishing new alias mapping
                        // Validate alias is within limit (server's limit is 10)
                        if (topicAlias > 10) {
                            logger.warning("Client [$clientId] Topic Alias $topicAlias exceeds maximum (10) - disconnecting")
                            sessionHandler.disconnectClient(clientId, "Topic Alias exceeds maximum")
                            return
                        }
                        
                        // Store alias mapping
                        topicAliases[topicAlias] = topicName
                        logger.fine { "Client [$clientId] Registered Topic Alias $topicAlias ← $topicName" }
                    }
                }
            }
        }
        
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
                sessionHandler.publishMessage(BrokerMessage(clientId, message, topicName))
            }
            MqttQoS.AT_LEAST_ONCE -> { // Level 1
                logger.finest { "Client [$clientId] Publish: sending acknowledge for id [${message.messageId()}] [${Utils.getCurrentFunctionName()}]" }
                sessionHandler.publishMessage(BrokerMessage(clientId, message, topicName))
                // TODO: check the result of the publishMessage and send the acknowledge only if the message was delivered
                
                // Send PUBACK with MQTT v5 reason code if protocol version is 5
                if (endpoint.protocolVersion() == 5) {
                    endpoint.publishAcknowledge(message.messageId(), MqttPubAckReasonCode.SUCCESS, MqttProperties.NO_PROPERTIES)
                } else {
                    endpoint.publishAcknowledge(message.messageId())
                }
            }
            MqttQoS.EXACTLY_ONCE -> { // Level 2
                logger.finest { "Client [$clientId] Publish: sending received for id [${message.messageId()}] [${Utils.getCurrentFunctionName()}]" }
                endpoint.publishReceived(message.messageId())
                inFlightMessagesRcv[message.messageId()] = InFlightMessage(BrokerMessage(clientId, message, topicName))
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

    /**
     * Route incoming bus message to appropriate handler based on type.
     * Handles both individual BrokerMessage and bulk BulkClientMessage.
     */
    private fun handleBusMessage(busMessage: io.vertx.core.eventbus.Message<Any>) {
        when (val body = busMessage.body()) {
            is BrokerMessage -> {
                // Create a properly typed message wrapper
                @Suppress("UNCHECKED_CAST")
                val brokerMessage = busMessage as io.vertx.core.eventbus.Message<BrokerMessage>
                consumeMessage(brokerMessage)
            }
            is BulkClientMessage -> {
                logger.finest { "Client [$clientId] Received bulk message with [${body.messages.size}] messages [${Utils.getCurrentFunctionName()}]" }
                body.messages.forEach { message ->
                    // Process each message as if it came individually
                    processBulkMessageItem(message)
                }
            }
            else -> {
                logger.warning { "Client [$clientId] Received unknown message type: ${body?.javaClass?.simpleName} [${Utils.getCurrentFunctionName()}]" }
            }
        }
    }

    /**
     * Process a single message from a bulk message batch.
     * Mirrors the logic from consumeMessage but without the bus message reply.
     */
    private fun processBulkMessageItem(message: BrokerMessage) {
        if (!ready) {
            return
        }
        when (message.qosLevel) {
            0 -> {
                val msg = message.cloneWithNewMessageId(0)
                if (endpoint.isConnected) {
                    publishMessage(msg)
                    logger.finest { "Client [$clientId] QoS [0] message [${msg.messageId}] for topic [${msg.topicName}] delivered [${Utils.getCurrentFunctionName()}]" }
                } else {
                    logger.finest { "Client [$clientId] QoS [0] message [${msg.messageId}] for topic [${msg.topicName}] not delivered, client not connected [${Utils.getCurrentFunctionName()}]" }
                }
            }
            1 -> {
                val msg = message.cloneWithNewMessageId(getNextMessageId())
                if (endpoint.isConnected) {
                    publishMessage(msg)
                    logger.finest { "Client [$clientId] QoS [1] message [${msg.messageId}] for topic [${msg.topicName}] delivered [${Utils.getCurrentFunctionName()}]" }
                } else {
                    logger.finest { "Client [$clientId] QoS [1] message [${msg.messageId}] for topic [${msg.topicName}] not delivered, client not connected [${Utils.getCurrentFunctionName()}]" }
                }
            }
            2 -> {
                val msg = message.cloneWithNewMessageId(getNextMessageId())
                if (endpoint.isConnected) {
                    publishMessage(msg)
                    logger.finest { "Client [$clientId] QoS [2] message [${msg.messageId}] for topic [${msg.topicName}] delivered [${Utils.getCurrentFunctionName()}]" }
                } else {
                    logger.finest { "Client [$clientId] QoS [2] message [${msg.messageId}] for topic [${msg.topicName}] not delivered, client not connected [${Utils.getCurrentFunctionName()}]" }
                }
            }
            else -> {
                logger.warning { "Client [$clientId] Subscribe: unknown QoS level [${message.qosLevel}] [${Utils.getCurrentFunctionName()}]" }
            }
        }
    }

    private fun consumeMessage(busMessage: Message<BrokerMessage>) {
        if (!ready) {
            busMessage.reply(false)
            return
        }

        when (busMessage.body().qosLevel) {
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
            // Increment messages sent to client ONLY when actually publishing to endpoint
            if (message.qosLevel == 0) {
                sessionHandler.incrementMessagesOut(clientId)
                message.publishToEndpoint(endpoint)
            } else {
                // MQTT v5.0 Flow Control (Phase 8): Enforce client's Receive Maximum
                val maxInFlight = if (endpoint.protocolVersion() == 5) {
                    clientReceiveMaximum
                } else {
                    MAX_IN_FLIGHT_MESSAGES
                }
                
                if (inFlightMessagesSnd.size >= maxInFlight) {
                    logger.warning { "Client [$clientId] QoS [${message.qosLevel}] message [${message.messageId}] for topic [${message.topicName}] not delivered, Receive Maximum limit reached (${inFlightMessagesSnd.size}/$maxInFlight) [${Utils.getCurrentFunctionName()}]" }
                    // TODO: message must be removed from message store (queued messages)
                } else {
                    inFlightMessagesSnd.addLast(InFlightMessage(message))
                    if (inFlightMessagesSnd.size == 1) {
                        sessionHandler.incrementMessagesOut(clientId)
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
            val msg = inFlightMessagesSnd.first().message
            sessionHandler.incrementMessagesOut(clientId)
            msg.publishToEndpoint(endpoint)
            logger.finest { "Client [$clientId] Subscribe: next message [${msg.messageId}] from queue delivered [${Utils.getCurrentFunctionName()}]" }
        }
    }

    private fun publishMessageCompleted(message: BrokerMessage) {
        inFlightMessagesSnd.removeFirst()
        if (message.isQueued) sessionHandler.markMessageDelivered(clientId, message.messageUuid)
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
                    val wasQueued = inFlightMessage.message.isQueued
                    publishMessageCompleted(inFlightMessage.message)
                    // For queue-first delivery: fetch next from DB queue
                    if (wasQueued && isProcessingQueue) {
                        processNextMessage()
                    } else {
                        publishMessageCheckNext()
                    }
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
                    val wasQueued = inFlightMessage.message.isQueued
                    publishMessageCompleted(inFlightMessage.message)
                    // For queue-first delivery: fetch next from DB queue
                    if (wasQueued && isProcessingQueue) {
                        processNextMessage()
                    } else {
                        publishMessageCheckNext()
                    }
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
                // MQTT v5: Honor Will Delay Interval (Property 24)
                if (willDelayInterval > 0 && endpoint.protocolVersion() == 5) {
                    logger.info("Client [$clientId] Will Delay Interval: ${willDelayInterval}s - scheduling Last Will")
                    willDelayTimerId = vertx.setTimer(willDelayInterval * 1000) {
                        logger.fine { "Client [$clientId] Will Delay expired - publishing Last Will [${Utils.getCurrentFunctionName()}]" }
                        sessionHandler.publishMessage(BrokerMessage(clientId, will))
                        willDelayTimerId = null
                    }
                } else {
                    // No delay - publish immediately
                    logger.fine { "Client [$clientId] Sending Last-Will message [${Utils.getCurrentFunctionName()}]" }
                    sessionHandler.publishMessage(BrokerMessage(clientId, will))
                }
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