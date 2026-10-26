package at.rocworks

import at.rocworks.bus.EventBusAddresses
import at.rocworks.auth.UserManager
import at.rocworks.data.BrokerMessage
import at.rocworks.data.BulkClientMessage
import at.rocworks.schema.TopicSchemaPolicyCache
import at.rocworks.handlers.Backpressure
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
import io.vertx.mqtt.messages.codes.MqttPubCompReasonCode
import io.vertx.mqtt.messages.codes.MqttDisconnectReasonCode
import io.vertx.mqtt.messages.codes.MqttSubAckReasonCode
import io.vertx.mqtt.messages.codes.MqttUnsubAckReasonCode
import io.vertx.mqtt.messages.MqttDisconnectMessage
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

class MqttClient(
    private val endpoint: MqttEndpoint,
    private val sessionHandler: SessionHandler,
    private val userManager: UserManager,
    // When true, a verified TLS client certificate's Common Name is used as the client's
    // authenticated identity, skipping password checks. Defaults to false so existing
    // setups are unaffected.
    private val useIdentityAsUsername: Boolean = false,
    // When true, a client certificate whose Common Name has no user account yet gets one
    // created automatically with default, non-admin permissions. When false, such a client
    // is rejected and accounts must be created up front.
    private val autoCreateUser: Boolean = false
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

    private data class InFlightMessage(
        val message: BrokerMessage,
        var stage: Int = 1,
        var lastTryTime: Long = System.currentTimeMillis(), // epoch millis, avoids an Instant per message
        var retryCount: Int = 0
    )

    private val inFlightMessagesRcv = ConcurrentHashMap<Int, InFlightMessage>() // messageId TODO: is concurrent needed?

    // Outbound QoS 1/2 state. Endpoint and bus handlers run on the connection's event loop, the
    // periodic retry check on the verticle's context, so access is guarded by outboundLock.
    private val outboundLock = Any()
    // Sent and awaiting PUBACK/PUBREC/PUBCOMP, keyed by packet id, in send order
    private val inFlightMessagesSnd = LinkedHashMap<Int, InFlightMessage>()
    // Waiting for a free slot in the in-flight window, without packet id yet
    private val pendingMessagesSnd = ArrayDeque<BrokerMessage>()
    private var nextMessageId: Int = 0

    // Caller holds outboundLock. The in-flight window is capped at 65535, so a free id always exists.
    private fun nextFreeMessageId(): Int {
        do {
            nextMessageId = if (nextMessageId >= 65535) 1 else nextMessageId + 1
        } while (inFlightMessagesSnd.containsKey(nextMessageId))
        return nextMessageId
    }

    private fun maxInFlight(): Int =
        (if (endpoint.protocolVersion() == 5) clientReceiveMaximum else Monster.getMaxInFlightMessages()).coerceIn(1, 65535)

    private fun inFlightCount(): Int = synchronized(outboundLock) { inFlightMessagesSnd.size }

    // Queue-first state machine for QoS 1+ message delivery
    private var isProcessingQueue = false
    private var triggerPending = false
    private var isFetchingQueueMessage = false

    // Message cache for bulk fetching (reduces database queries)
    private val messageCache = mutableListOf<BrokerMessage>()
    private val messageCacheLock = Any()
    private val messageCacheSize = Monster.getMessageCacheSize()

    private val busConsumers = mutableListOf<MessageConsumer<*>>()

    // Underlying socket, for write queue state and pausing reads (null if not accessible)
    private val socket: io.vertx.core.net.NetSocket? = try {
        endpointConnField.get(endpoint) as? io.vertx.core.net.NetSocket
    } catch (e: Exception) {
        null
    }

    // QoS 0 messages waiting while the socket write queue is full, bounded by
    // MqttTcpServer.MaxQueuedQoS0Messages. Guarded by outboundLock; only one thread sends from it.
    private val pendingQoS0 = ArrayDeque<BrokerMessage>()
    private var flushingQoS0 = false
    private var qos0DroppedSinceWarning = 0L
    private var qos0LastDropWarning = 0L

    // Reading from the socket is paused because of broker backpressure (connection context only)
    private var readPaused = false
    
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
    
    private val instanceId: String = java.util.UUID.randomUUID().toString()
    private val isMqtt5: Boolean get() = endpoint.protocolVersion() == 5

    private var cleanStartRequested: Boolean = true
    private var sessionExpiryInterval: Long = 0L

    // Effective clean session flag for backward compatibility
    private val effectiveCleanSession: Boolean
        get() = if (isMqtt5) sessionExpiryInterval == 0L else endpoint.isCleanSession

    private var connectionAccepted: Boolean = false

    // Outbound Receive Maximum saturation tracking
    private var receiveMaximumWarningActive: Boolean = false
    private var receiveMaximumWarningSince: Instant? = null
    private var receiveMaximumSuppressedWarnings: Long = 0L
    private var receiveMaximumLastSampledMessageId: Int? = null
    private var receiveMaximumLastSampledTopic: String? = null
    private var receiveMaximumLastSampledQos: Int? = null
    private var receiveMaximumLastSampledInFlightObserved: Int? = null

    private val pendingEvents = mutableListOf<() -> Unit>()

    private fun drainPendingEvents() {
        if (pendingEvents.isNotEmpty()) {
            logger.info("Client [$clientId] Session is ready, draining ${pendingEvents.size} pending queued events")
            val events = ArrayList(pendingEvents)
            pendingEvents.clear()
            events.forEach { it.invoke() }
        }
    }

    // create a getter for the client id
    val clientId: String
        get() = endpoint.clientIdentifier()


    companion object {
        private val logger = Utils.getLogger(this::class.java)

        private const val RETRY_INTERVAL_MS = Const.QOS2_RETRY_INTERVAL * 1000


        // Vert.x writes every PUBLISH with writeAndFlush (one socket syscall per message) when the
        // write is not triggered by a read on the same channel. FlushConsolidationHandler coalesces
        // the flushes of one event loop task into a single syscall.
        private val endpointConnField by lazy {
            io.vertx.mqtt.impl.MqttEndpointImpl::class.java.getDeclaredField("conn").apply { isAccessible = true }
        }

        private fun installFlushConsolidation(endpoint: MqttEndpoint) {
            try {
                val conn = endpointConnField.get(endpoint) as io.vertx.core.internal.net.StreamChannelInternal
                val pipeline = conn.channelHandlerContext().pipeline()
                if (pipeline.get("flushConsolidation") == null) {
                    pipeline.addFirst("flushConsolidation", io.netty.handler.flush.FlushConsolidationHandler(256, true))
                }
            } catch (e: Exception) {
                logger.fine { "Flush consolidation not installed: ${e.message}" }
            }
        }

        fun deployEndpoint(vertx: Vertx, endpoint: MqttEndpoint, sessionHandler: SessionHandler, userManager: UserManager, useIdentityAsUsername: Boolean = false, autoCreateUser: Boolean = false) {
            val clientId = endpoint.clientIdentifier()
            installFlushConsolidation(endpoint)
            logger.fine { "Client [${clientId}] Deploy a new session for [${endpoint.remoteAddress()}] [${Utils.getCurrentFunctionName()}]" }

            fun doDeploy() {
                val client = MqttClient(endpoint, sessionHandler, userManager, useIdentityAsUsername, autoCreateUser)
                vertx.deployVerticle(client).onComplete {
                    client.startEndpoint()
                }
            }

            if (clientId.isNotEmpty() && sessionHandler.isConnected(clientId)) {
                logger.info("Client [$clientId] Already connected - disconnecting existing session before deploying new connection")
                val payload = JsonObject().put(Const.COMMAND_KEY, Const.COMMAND_DISCONNECT).put("Reason", "Duplicate client ID connected")
                vertx.eventBus().request<JsonObject>(getCommandAddress(clientId), payload).onComplete { _ ->
                    vertx.setTimer(50) {
                        doDeploy()
                    }
                }
            } else {
                doDeploy()
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

        val maxInFlight = maxInFlight()
        val inFlight = inFlightCount()
        if (inFlight >= maxInFlight) {
            logger.finest { "Client [$clientId] Queue processing paused: outstanding in-flight ($inFlight) >= max ($maxInFlight)" }
            return
        }

        if (isFetchingQueueMessage) {
            logger.finest { "Client [$clientId] Already fetching next message, skipping duplicate processing" }
            return
        }

        isFetchingQueueMessage = true
        fetchNextMessageFromCacheOrDb().onComplete { result ->
            isFetchingQueueMessage = false
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
                    vertx.runOnContext { processNextMessage() }
                } else {
                    logger.finest { "Client [$clientId] Queue empty, going idle [${Utils.getCurrentFunctionName()}]" }
                    isProcessingQueue = false  // Go idle
                }
                return@onComplete
            }

            // Messages are already marked in-flight during bulk fetch (fetchNextMessageFromCacheOrDb)
            // publishMessage assigns the packet id
            logger.finest { "Client [$clientId] Publishing queued message [${msg.messageUuid}] for topic [${msg.topicName}] [${Utils.getCurrentFunctionName()}]" }
            publishMessage(msg)

            // Continue processing the next message on the event loop context to fill the window
            vertx.runOnContext { processNextMessage() }
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

        // Cache empty - fetch a batch from database (atomically marked in-flight)
        return sessionHandler.fetchAndLockPendingMessages(clientId, messageCacheSize).map { messages ->
            if (messages.isEmpty()) {
                null
            } else {
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
        val cid = clientId
        if (cid == "inline" || cid.startsWith("peerlink:")) {
            logger.warning("Client [$cid] Connection refused: reserved client identifier")
            rejectAndCloseEndpoint(if (isMqtt5) MqttConnectReturnCode.CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID else MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED)
            return
        }
        logger.info("Client [$clientId] Request to connect. Clean session [${endpoint.isCleanSession}] protocol [${endpoint.protocolVersion()}] [${Utils.getCurrentFunctionName()}]")
        cleanStartRequested = endpoint.isCleanSession

        // Parse MQTT5 properties from CONNECT packet
        var mqtt5ReceiveMaximum = 65535
        var mqtt5MaximumPacketSize = 268435456L
        var mqtt5TopicAliasMaximum = 0
        var mqtt5AuthMethod: String? = null
        var mqtt5AuthData: ByteArray? = null

        sessionExpiryInterval = if (isMqtt5) 0L else if (cleanStartRequested) 0L else Long.MAX_VALUE

        if (isMqtt5) {
            logger.info("Client [$clientId] MQTT 5.0 connection accepted")
            // Parse MQTT5 CONNECT properties
            val props = endpoint.connectProperties()
            val mqtt5PropertyNames = mapOf(
                17 to "SessionExpiryInterval",
                21 to "AuthenticationMethod",
                22 to "AuthenticationData",
                23 to "RequestProblemInformation",
                24 to "WillDelayInterval",
                25 to "RequestResponseInformation",
                33 to "ReceiveMaximum",
                34 to "TopicAliasMaximum",
                38 to "UserProperty",
                39 to "MaximumPacketSize"
            )
            props.listAll().forEach { p ->
                val name = mqtt5PropertyNames[p.propertyId()] ?: "Property[${p.propertyId()}]"
                val value = p.value()
                val valueStr = when (value) {
                    is io.netty.handler.codec.mqtt.MqttProperties.StringPair -> "${value.key}=${value.value}"
                    is List<*> -> value.joinToString(", ") { item ->
                        if (item is io.netty.handler.codec.mqtt.MqttProperties.StringPair) "${item.key}=${item.value}" else item.toString()
                    }
                    else -> value.toString()
                }
                logger.fine("Client [$clientId] CONNECT $name: $valueStr")
                when (p.propertyId()) {
                    17 -> sessionExpiryInterval = (value as? Number)?.toLong() ?: 0L
                    21 -> mqtt5AuthMethod = value as? String
                    22 -> mqtt5AuthData = value as? ByteArray
                    24 -> willDelayInterval = (value as? Number)?.toLong() ?: 0L
                    33 -> mqtt5ReceiveMaximum = (value as? Number)?.toInt() ?: 65535
                    39 -> mqtt5MaximumPacketSize = (value as? Number)?.toLong() ?: 268435456L
                    34 -> mqtt5TopicAliasMaximum = (value as? Number)?.toInt() ?: 0
                }
            }
            logger.info("Client [$clientId] MQTT5 properties: sessionExpiry=$sessionExpiryInterval, receiveMax=$mqtt5ReceiveMaximum, maxPacketSize=$mqtt5MaximumPacketSize, willDelay=$willDelayInterval")

            if (mqtt5AuthMethod != null) {
                logger.info("Client [$clientId] MQTT5 enhanced authentication requested: method=$mqtt5AuthMethod, dataLength=${mqtt5AuthData?.size ?: 0}")
            }

            // Store client's Receive Maximum for flow control (Phase 8)
            clientReceiveMaximum = mqtt5ReceiveMaximum
        }

        sessionHandler.cancelSessionExpiry(clientId)
        sessionHandler.setActiveClientInstance(clientId, instanceId)

        run {
            endpoint.exceptionHandler(::exceptionHandler)
            socket?.drainHandler { flushQoS0() }
            endpoint.pingHandler { pingHandler() }
            endpoint.subscribeHandler(::subscribeHandler)
            endpoint.unsubscribeHandler(::unsubscribeHandler)
            endpoint.publishHandler(::publishHandler)
            endpoint.publishReleaseHandler(::publishReleaseHandler)

            endpoint.publishAcknowledgeHandler(::publishAcknowledgeHandler)
            endpoint.publishReceivedHandler(::publishedReceivedHandler)
            endpoint.publishCompletionHandler(::publishCompletionHandler)

            endpoint.disconnectMessageHandler { disconnectMsg ->
                if (isMqtt5) {
                    val props = disconnectMsg.properties()
                    val prop = props.getProperty(17)
                    if (prop != null) {
                        val newInterval = (prop.value() as? Number)?.toLong() ?: 0L
                        sessionExpiryInterval = newInterval
                        logger.info("Client [$clientId] Updated sessionExpiryInterval from DISCONNECT packet: $sessionExpiryInterval")
                    }
                }
                disconnectHandler()
            }
            endpoint.disconnectHandler { disconnectHandler() }
            endpoint.closeHandler { closeHandler() }

            // Message bus consumers
            if (!deployed) {
                deployed = true
                busConsumers.add(vertx.eventBus().consumer(getCommandAddress(clientId), ::consumeCommand))
                // Register with Any type to handle both BrokerMessage and BulkClientMessage
                val messageConsumer = vertx.eventBus().consumer<Any>(getMessagesAddress(clientId)) { busMessage ->
                    handleBusMessage(busMessage)
                }
                // Messages still queued for this consumer when the client disconnects are dropped;
                // they are QoS 0 or clean-session messages (persistent sessions get QoS 1/2 through
                // the queue store). Without a discard handler Vert.x logs a warning for every one.
                (messageConsumer as? io.vertx.core.eventbus.impl.MessageConsumerImpl<Any>)?.discardHandler { busMessage ->
                    logger.finest { "Client [$clientId] Discarding message after disconnect [${(busMessage.body() as? BrokerMessage)?.topicName}]" }
                }
                busConsumers.add(messageConsumer)
                // Queue trigger handler for queue-first delivery (QoS 1+ persistent sessions)
                busConsumers.add(vertx.eventBus().consumer<String>(getQueueTriggerAddress(clientId)) { _ ->
                    onMessageAvailable()
                })
            } else {
                logger.severe("Client [$clientId] Already deployed [${Utils.getCurrentFunctionName()}]")
            }

            // Set last will
            sessionHandler.setLastWill(clientId, endpoint.will())

            // Authentication check
            if (userManager.isUserManagementEnabled()) {
                // If enabled, a verified TLS client certificate's Common Name identifies the
                // client - the certificate was already validated against the trusted CA during
                // the TLS handshake, so it replaces the password. The user account still governs
                // permissions and ACL rules, exactly like a password-authenticated user.
                if (useIdentityAsUsername) {
                    val certUsername = extractPeerCertificateCommonName()
                    if (certUsername != null) {
                        val existingUser = userManager.getUser(certUsername)
                        if (existingUser != null) {
                            if (!existingUser.enabled) {
                                logger.warning("Client [$clientId] Certificate user [$certUsername] is disabled - rejecting connection")
                                rejectAndCloseEndpoint(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED)
                                return
                            }
                            authenticatedUser = existingUser
                            logger.info("Client [$clientId] Authenticated via TLS client certificate as [$certUsername]")
                            proceedWithConnection()
                            return
                        }

                        if (!autoCreateUser) {
                            logger.warning("Client [$clientId] No user account for certificate Common Name [$certUsername] - rejecting connection")
                            rejectAndCloseEndpoint(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED)
                            return
                        }

                        // Create the account with default, non-admin permissions. The password is a
                        // random value that is never stored anywhere else, so the account cannot be
                        // used for password authentication - the certificate remains the credential.
                        userManager.createUser(
                            username = certUsername,
                            password = java.util.UUID.randomUUID().toString(),
                            enabled = true,
                            canSubscribe = true,
                            canPublish = true,
                            isAdmin = false
                        ).onComplete { created ->
                            val newUser = if (created.succeeded() && created.result()) userManager.getUser(certUsername) else null
                            if (newUser != null) {
                                authenticatedUser = newUser
                                logger.info("Client [$clientId] Created user [$certUsername] from client certificate and authenticated")
                                proceedWithConnection()
                            } else {
                                logger.warning("Client [$clientId] Could not create user [$certUsername] from client certificate - rejecting connection")
                                rejectAndCloseEndpoint(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED)
                            }
                        }
                        return
                    }
                }

                // Check for MQTT v5.0 enhanced authentication first
                if (isMqtt5 && mqtt5AuthMethod != null) {
                    // Enhanced authentication requested
                    logger.info("Client [$clientId] MQTT v5.0 enhanced authentication: method=$mqtt5AuthMethod")
                    
                    // TODO: Implement enhanced authentication flow when Vert.x MQTT adds AUTH packet support
                    // For now, log the attempt and fall back to basic authentication
                    logger.warning("Client [$clientId] Enhanced authentication ($mqtt5AuthMethod) not yet supported - AUTH packet handling requires Vert.x MQTT API enhancement")
                    logger.info("Client [$clientId] Falling back to basic username/password authentication")
                    
                    // Fall through to basic authentication
                }
                
                val username = endpoint.auth()?.username
                val password = endpoint.auth()?.password
                
                if (username.isNullOrEmpty()) {
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
                            null
                        }
                    }).onComplete { result ->
                        val anonymousUser = result.result()
                        if (anonymousUser != null && anonymousUser.enabled) {
                            authenticatedUser = anonymousUser
                            logger.fine { "Client [$clientId] Using Anonymous user for unauthenticated access" }
                            proceedWithConnection()
                        } else {
                            logger.warning("Client [$clientId] Anonymous user is disabled or not found - rejecting connection")
                            rejectAndCloseEndpoint(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED)
                        }
                    }
                    return
                }
                
                // Now authenticate() returns Future<User?>, so we handle it directly
                userManager.authenticate(username, password ?: "").onComplete { authResult ->
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

    // Extracts the Common Name from the client's TLS certificate, if one was presented.
    // Returns null (rather than throwing) whenever no usable client certificate is
    // available - e.g. ClientAuth is REQUEST and the client didn't offer one, or the
    // connection isn't TLS at all. That's the normal/expected case, not an error.
    private fun extractPeerCertificateCommonName(): String? {
        if (!endpoint.isSsl) return null
        return try {
            val cert = endpoint.sslSession()?.peerCertificates?.firstOrNull() as? java.security.cert.X509Certificate
                ?: return null
            val ldapName = javax.naming.ldap.LdapName(cert.subjectX500Principal.name)
            val commonName = ldapName.rdns.firstOrNull { it.type.equals("CN", ignoreCase = true) }?.value?.toString()?.trim()
            if (commonName.isNullOrBlank()) null else commonName
        } catch (e: javax.net.ssl.SSLPeerUnverifiedException) {
            null
        } catch (e: Exception) {
            logger.warning("Client [$clientId] Failed to read Common Name from client certificate: ${e.message}")
            null
        }
    }

    private fun proceedWithConnection() {
        val isPersistent = if (isMqtt5) sessionExpiryInterval > 0L else !cleanStartRequested
        val cleanSession = !isPersistent

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
                connackProps.add(MqttProperties.IntegerProperty(17, sessionExpiryInterval.toInt()))
                
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
                connackProps.add(MqttProperties.IntegerProperty(33, Monster.getServerReceiveMaximum()))
                
                // Maximum QoS (36) - Omitted: per MQTT 5.0 §3.2.2.3.4, valid values are 0 or 1 only.
                // When absent, the client may use QoS 2. Including value 2 is a protocol error.
                
                // Retain Available (37) - Server supports retained messages
                connackProps.add(MqttProperties.IntegerProperty(37, 1))  // 1 = available
                
                // Maximum Packet Size (39) - Server's maximum packet size
                connackProps.add(MqttProperties.IntegerProperty(39, 268435455))  // Max allowed by MQTT v5
                
                // Topic Alias Maximum (34) - Server's limit for topic aliases (Phase 4)
                connackProps.add(MqttProperties.IntegerProperty(34, 10))
                
                // Wildcard Subscription Available (40)
                connackProps.add(MqttProperties.IntegerProperty(40, 1))  // 1 = available
                
                // Subscription Identifier Available (41)
                connackProps.add(MqttProperties.IntegerProperty(41, 1))  // 1 = available
                
                // Shared Subscription Available (42)
                connackProps.add(MqttProperties.IntegerProperty(42, 0))  // 0 = not supported yet
                
                connectionAccepted = true
                endpoint.accept(present, connackProps)
                logger.info("Client [$clientId] MQTT5 CONNACK sent with server properties (sessionPresent=$present, expiry=$sessionExpiryInterval)")
            } else {
                // MQTT v3.1.1: Simple accept
                connectionAccepted = true
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
            information.put("sessionExpiryInterval", if (isMqtt5) sessionExpiryInterval else endpoint.keepAliveTimeSeconds().toLong())
            sessionHandler.setClient(clientId, cleanSession, information).onComplete {
                if (endpoint.isConnected) {
                    // Now safe to mark as ready for new messages
                    ready = true
                    sessionHandler.onlineClient(clientId)
                    drainPendingEvents()

                    // For persistent sessions, reset any stale in-flight messages and send trigger
                    // This handles the case where previous connection died with messages in-flight
                    if (isPersistent) {
                        sessionHandler.resetInFlightMessages(clientId).onComplete {
                            logger.fine { "Client [$clientId] Reset in-flight messages and sending initial queue trigger [${Utils.getCurrentFunctionName()}]" }
                            sessionHandler.sendMessageAvailableTrigger(clientId)
                        }
                    }
                }
            }
        }

        // Accept connection
        if (cleanStartRequested) {
            sessionHandler.delClient(clientId).onComplete { // Clean and remove any existing session state
                finishClientStartup(false) // false... session not present because of clean start requested
            }.onFailure {
                logger.severe("Client [$clientId] Error: ${it.message} [${Utils.getCurrentFunctionName()}]")
                rejectAndCloseEndpoint(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE)
            }
        } else {
            // Check if session was already present or if it was the first connect
            sessionHandler.isPresent(clientId).onComplete { present ->
                finishClientStartup(present.result() ?: false)
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
        isFetchingQueueMessage = false
        clearMessageCache()
        synchronized(outboundLock) {
            pendingMessagesSnd.clear()
            pendingQoS0.clear()
        }

        if (receiveMaximumWarningActive) {
            val since = receiveMaximumWarningSince
            val durationStr = if (since != null) {
                val duration = java.time.Duration.between(since, Instant.now())
                "${duration.toMillis() / 1000.0}s"
            } else {
                "unknown"
            }
            logger.warning { "Client [$clientId] Disconnected while saturated at Receive Maximum limit. Saturation duration: $durationStr. Suppressed $receiveMaximumSuppressedWarnings warnings. Last blocked topic: $receiveMaximumLastSampledTopic (QoS $receiveMaximumLastSampledQos, MsgId $receiveMaximumLastSampledMessageId)." }
        }

        // Clear topic aliases (MQTT v5.0 Phase 4)
        // Topic aliases are session-specific but NOT persistent across disconnects
        if (topicAliases.isNotEmpty()) {
            logger.fine { "Client [$clientId] Clearing ${topicAliases.size} topic aliases on disconnect" }
            topicAliases.clear()
        }

        val isCurrent = sessionHandler.isActiveClientInstance(clientId, instanceId)
        if (isCurrent) {
            sessionHandler.clearActiveClientInstance(clientId, instanceId)
            val isPersistent = if (isMqtt5) sessionExpiryInterval > 0L else !cleanStartRequested
            if (!isPersistent || sessionExpiryInterval == 0L) {
                logger.fine { "Client [$clientId] Remove client, non-persistent session or session expiry 0 [${Utils.getCurrentFunctionName()}]" }
                sessionHandler.delClient(clientId)
            } else {
                logger.fine { "Client [$clientId] Pause client, persistent session [${Utils.getCurrentFunctionName()}]" }
                sessionHandler.pauseClient(clientId)
                if (isMqtt5 && sessionExpiryInterval > 0L && sessionExpiryInterval < 0xFFFFFFFFL) {
                    sessionHandler.scheduleSessionExpiry(clientId, sessionExpiryInterval)
                }
            }
        } else {
            logger.info("Client [$clientId] Superseded by newer connection instance - skipping session cleanup")
        }
        undeployEndpoint(vertx, this.deploymentID())
    }

    private fun exceptionHandler(throwable: Throwable) {
        val msg = throwable.message ?: ""
        val localizedMsg = throwable.localizedMessage ?: msg
        val isCommonNetworkError = throwable is java.io.IOException ||
                msg.contains("connection reset", ignoreCase = true) ||
                msg.contains("broken pipe", ignoreCase = true) ||
                msg.contains("connection timed out", ignoreCase = true)

        if (isCommonNetworkError) {
            logger.warning("Client [$clientId] connection disconnected abruptly: $localizedMsg")
        } else {
            logger.severe("Client [$clientId] Exception: $msg [${Utils.getCurrentFunctionName()}]")
        }
        closeConnection()
    }

    private fun pingHandler() {
        if (!ready) {
            logger.fine { "Client [$clientId] Connection not ready, queuing PINGREQ" }
            pendingEvents.add { pingHandler() }
            return
        }
        lastPing = Instant.now()
        //endpoint.pong() // A java clients dies when pong is sent
    }

    private fun subscribeHandler(subscribe: MqttSubscribeMessage) {
        if (!ready) {
            logger.info("Client [$clientId] Connection not ready, queuing SUBSCRIBE for message id [${subscribe.messageId()}]")
            pendingEvents.add { subscribeHandler(subscribe) }
            return
        }
        val username = authenticatedUser?.username ?: at.rocworks.Const.ANONYMOUS_USER
        val protocolVersion = endpoint.protocolVersion()

        // For MQTT v5: use reason codes; for v3.1.1: use QoS values
        if (protocolVersion == 5) {
            // MQTT v5.0: Use MqttSubAckReasonCode
            val reasonCodeFutures = mutableListOf<Future<MqttSubAckReasonCode>>()
            val messageId = subscribe.messageId()

            // Subscription Identifier (11): at most one per SUBSCRIBE, applies to all its filters
            val subscriptionIdProperty = subscribe.properties()
                ?.getProperty(11) as? MqttProperties.IntegerProperty
            val subscriptionId = subscriptionIdProperty?.value() ?: 0
            if (subscriptionIdProperty != null && subscriptionId == 0) {
                // MQTT 5.0 §3.8.2.1.2: a Subscription Identifier of 0 is a Protocol Error
                logger.warning("Client [$clientId] SUBSCRIBE with Subscription Identifier 0 - protocol error, disconnecting")
                endpoint.disconnect(MqttDisconnectReasonCode.PROTOCOL_ERROR, MqttProperties.NO_PROPERTIES)
                closeConnection()
                return
            }

            subscribe.topicSubscriptions().forEach { subscription ->
                val topic = subscription.topicName()
                var allowed = true
                var reasonCode: MqttSubAckReasonCode
                
                // MQTT v5 subscription options - access via subscriptionOption()
                val subOption = subscription.subscriptionOption()
                val noLocal = subOption?.isNoLocal ?: false  // Don't send back messages this client published
                val retainHandling = subOption?.retainHandling()?.value() ?: 0  // 0=send retained, 1=send if new, 2=never send
                val retainAsPublished = subOption?.isRetainAsPublished ?: false  // Preserve original retain flag

                // Root wildcard policy
                if (topic == "#" && !Monster.allowRootWildcardSubscription()) {
                    allowed = false
                    reasonCode = MqttSubAckReasonCode.TOPIC_FILTER_INVALID
                    logger.warning("Client [$clientId] Root wildcard subscription '#' rejected (AllowRootWildcardSubscription=false)")
                } else if (userManager.isUserManagementEnabled()) {
                    val isWildcard = topic.contains('+') || topic.contains('#')
                    if (!Monster.aclCheckOnSubscription() && isWildcard) {
                        // Delivery-time ACL mode: skip ACL rule check for wildcard subscriptions,
                        // but still enforce the user-level canSubscribe flag
                        val user = userManager.getUser(username)
                        if (user != null && !user.canSubscribe) {
                            allowed = false
                            reasonCode = MqttSubAckReasonCode.NOT_AUTHORIZED
                            logger.warning("Client [$clientId] Subscription DENIED for [$topic] - user [$username] subscribe permission disabled")
                        } else {
                            reasonCode = MqttSubAckReasonCode.qosGranted(subscription.qualityOfService())
                        }
                    } else if (!userManager.canSubscribe(username, topic, clientId)) {
                        // ACL check
                        allowed = false
                        reasonCode = MqttSubAckReasonCode.NOT_AUTHORIZED
                        logger.warning("Client [$clientId] Subscription DENIED for [$topic] - user [$username] lacks permission")
                    } else {
                        reasonCode = MqttSubAckReasonCode.qosGranted(subscription.qualityOfService())
                    }
                } else {
                    // Subscription allowed - return granted QoS
                    reasonCode = MqttSubAckReasonCode.qosGranted(subscription.qualityOfService())
                }

                // Forward allowed subscriptions to SessionHandler
                if (allowed) {
                    logger.fine { "Client [$clientId] Subscription ALLOWED for [$topic] with QoS ${subscription.qualityOfService()} noLocal=$noLocal retainHandling=$retainHandling retainAsPublished=$retainAsPublished subscriptionId=$subscriptionId" }
                    val subFuture = sessionHandler.subscribeRequest(this, topic, subscription.qualityOfService(), noLocal, retainHandling, retainAsPublished, subscriptionId)
                        .map { success ->
                            if (success) reasonCode else MqttSubAckReasonCode.UNSPECIFIED_ERROR
                        }
                        .recover { error ->
                            logger.warning("Client [$clientId] Subscription request failed for [$topic]: ${error.message}")
                            Future.succeededFuture(MqttSubAckReasonCode.UNSPECIFIED_ERROR)
                        }
                    reasonCodeFutures.add(subFuture)
                } else {
                    logger.fine { "Client [$clientId] Subscription REJECTED for [$topic] - reason: $reasonCode" }
                    reasonCodeFutures.add(Future.succeededFuture(reasonCode))
                }
            }

            Future.all(reasonCodeFutures).onComplete {
                val reasonCodes = reasonCodeFutures.map { future ->
                    if (future.succeeded()) future.result() else MqttSubAckReasonCode.UNSPECIFIED_ERROR
                }
                endpoint.subscribeAcknowledge(messageId, reasonCodes, MqttProperties.NO_PROPERTIES)
            }
        } else {
            // MQTT v3.1.1: Use QoS values (backward compatibility)
            val acknowledgementFutures = mutableListOf<Future<MqttQoS>>()
            val messageId = subscribe.messageId()

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
                    val isWildcard = topic.contains('+') || topic.contains('#')
                    if (!Monster.aclCheckOnSubscription() && isWildcard) {
                        // Delivery-time ACL mode: skip ACL rule check for wildcard subscriptions,
                        // but still enforce the user-level canSubscribe flag
                        val user = userManager.getUser(username)
                        if (user != null && !user.canSubscribe) {
                            allowed = false
                            logger.warning("Client [$clientId] Subscription DENIED for [$topic] - user [$username] subscribe permission disabled")
                        }
                    } else if (!userManager.canSubscribe(username, topic, clientId)) {
                        allowed = false
                        logger.warning("Client [$clientId] Subscription DENIED for [$topic] - user [$username] lacks permission")
                    }
                }

                // Forward allowed subscriptions to SessionHandler
                if (allowed) {
                    logger.fine { "Client [$clientId] Subscription ALLOWED for [$topic] with QoS ${subscription.qualityOfService()}" }
                    val subFuture = sessionHandler.subscribeRequest(this, topic, subscription.qualityOfService())
                        .map { success ->
                            if (success) subscription.qualityOfService() else MqttQoS.FAILURE
                        }
                        .recover { error ->
                            logger.warning("Client [$clientId] Subscription request failed for [$topic]: ${error.message}")
                            Future.succeededFuture(MqttQoS.FAILURE)
                        }
                    acknowledgementFutures.add(subFuture)
                } else {
                    logger.fine { "Client [$clientId] Subscription REJECTED for [$topic]" }
                    acknowledgementFutures.add(Future.succeededFuture(MqttQoS.FAILURE))
                }
            }

            Future.all(acknowledgementFutures).onComplete {
                val acknowledgements = acknowledgementFutures.map { future ->
                    if (future.succeeded()) future.result() else MqttQoS.FAILURE
                }
                endpoint.subscribeAcknowledge(messageId, acknowledgements)
            }
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
        if (!ready) {
            logger.info("Client [$clientId] Connection not ready, queuing UNSUBSCRIBE for message id [${unsubscribe.messageId()}]")
            pendingEvents.add { unsubscribeHandler(unsubscribe) }
            return
        }
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

    private fun updateSessionHandlerInFlight() {
        sessionHandler.updateInFlightCounts(clientId, inFlightCount(), inFlightMessagesRcv.size)
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
            .put("inFlightMessagesSnd", inFlightCount())
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
        if (Backpressure.isSaturated()) pauseReading()
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

        // Construct BrokerMessage immediately (copies payload bytes synchronously on Event Loop thread)
        val msg = BrokerMessage(clientId, message, topicName)

        if (!ready) {
            logger.fine { "Client [$clientId] Connection not ready, queuing PUBLISH for message id [${msg.messageId}]" }
            pendingEvents.add { processPublishMessage(msg) }
            return
        }

        processPublishMessage(msg)
    }

    private fun processPublishMessage(msg: BrokerMessage) {
        logger.finest { "Client [$clientId] Publish: message [${msg.messageId}] for [${msg.topicName}] with QoS ${msg.qosLevel} [${Utils.getCurrentFunctionName()}]" }

        // Increment messages received from client
        sessionHandler.incrementMessagesIn(clientId)
        
        val topicName = msg.topicName

        // Check system topic restrictions
        if (topicName.startsWith(Const.SYS_TOPIC_NAME)) {
            logger.warning { "Client [$clientId] Publish: message for system topic [$topicName] not allowed! [${Utils.getCurrentFunctionName()}]" }
            return
        }

        // MQTT spec §3.3.2.1: Topic Name in PUBLISH MUST NOT contain wildcard characters '+' or '#'
        if (topicName.contains('+') || topicName.contains('#')) {
            logger.warning { "Client [$clientId] Publish: topic [$topicName] contains wildcard characters - protocol violation [${Utils.getCurrentFunctionName()}]" }
            sessionHandler.disconnectClient(clientId, "Publish topic contains wildcard characters")
            return
        }
        
        // Check ACL permissions
        val username = authenticatedUser?.username ?: at.rocworks.Const.ANONYMOUS_USER
        val canPublish = if (userManager.isUserManagementEnabled()) {
            userManager.canPublish(username, topicName, clientId)
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

        // Validate against Topic Schema Namespace (if any)
        val schemaPolicyCache = TopicSchemaPolicyCache.getInstance()
        if (schemaPolicyCache != null) {
            val nsEntry = schemaPolicyCache.matchNamespace(topicName)
            logger.finer { "Client [$clientId] Schema check for [$topicName]: ${if (nsEntry != null) "matched namespace '${nsEntry.namespaceName}'" else "no matching namespace"}" }
            if (nsEntry != null) {
                val payload = msg.getPayloadAsString()
                val result = nsEntry.validator.validate(payload)
                if (!result.valid) {
                    when (result.errorCategory) {
                        "PARSE_ERROR" -> schemaPolicyCache.parseErrorCount.incrementAndGet()
                        "SCHEMA_ERROR" -> schemaPolicyCache.schemaErrorCount.incrementAndGet()
                    }
                    schemaPolicyCache.rejectedCount.incrementAndGet()
                    logger.warning("Client [$clientId] Publish REJECTED by namespace '${nsEntry.namespaceName}' (schema '${nsEntry.schemaPolicyName}') for [$topicName]: ${result.errorDetail}")
                    // For MQTT v5, send PUBACK with error reason code
                    if (endpoint.protocolVersion() == 5 && msg.qosLevel == 1) {
                        endpoint.publishAcknowledge(msg.messageId, MqttPubAckReasonCode.PAYLOAD_FORMAT_INVALID, MqttProperties.NO_PROPERTIES)
                    }
                    return
                }
                schemaPolicyCache.validatedCount.incrementAndGet()
            }
        }

        // Handle QoS levels
        when (msg.qosLevel) {
            0 -> { // Level 0
                logger.finest { "Client [$clientId] Publish: no acknowledge needed [${Utils.getCurrentFunctionName()}]" }
                sessionHandler.publishMessage(msg)
            }
            1 -> { // Level 1
                logger.finest { "Client [$clientId] Publish: sending acknowledge for id [${msg.messageId}] [${Utils.getCurrentFunctionName()}]" }
                sessionHandler.publishMessage(msg)
                
                // Send PUBACK with MQTT v5 reason code if protocol version is 5
                if (endpoint.protocolVersion() == 5) {
                    endpoint.publishAcknowledge(msg.messageId, MqttPubAckReasonCode.SUCCESS, MqttProperties.NO_PROPERTIES)
                } else {
                    endpoint.publishAcknowledge(msg.messageId)
                }
            }
            2 -> { // Level 2
                logger.finest { "Client [$clientId] Publish: sending received for id [${msg.messageId}] [${Utils.getCurrentFunctionName()}]" }
                endpoint.publishReceived(msg.messageId)
                inFlightMessagesRcv[msg.messageId] = InFlightMessage(msg)
                sessionHandler.addIncomingQos2Message(clientId, msg.messageId, msg)
                updateSessionHandlerInFlight()
            }
            else -> {
                logger.warning { "Client [$clientId] Publish: unknown QoS level [${msg.qosLevel}] [${Utils.getCurrentFunctionName()}]" }
            }
        }
    }

    private fun publishReleaseHandler(id: Int) {
        if (!ready) {
            logger.info("Client [$clientId] Connection not ready, queuing PUBREL id [$id]")
            pendingEvents.add { publishReleaseHandler(id) }
            return
        }
        val inFlightMessage = inFlightMessagesRcv.remove(id)
        val msg = inFlightMessage?.message ?: sessionHandler.getIncomingQos2Message(clientId, id)
        if (msg != null) {
            logger.finest { "Client [$clientId] Publish: got publish release id [$id], now sending complete to client [${Utils.getCurrentFunctionName()}]"}
            sessionHandler.completeIncomingQos2Message(clientId, id)
            endpoint.publishComplete(id)
            sessionHandler.publishMessage(msg)
            updateSessionHandlerInFlight()
        } else if (sessionHandler.isIncomingQos2Completed(clientId, id)) {
            // Already processed and completed, send duplicate PUBCOMP
            logger.fine { "Client [$clientId] Duplicate PUBREL received for already completed id [$id] - sending PUBCOMP" }
            endpoint.publishComplete(id)
        } else {
            logger.warning { "Client [$clientId] Publish: got publish release for unknown id [$id] [${Utils.getCurrentFunctionName()}]"}
            if (endpoint.protocolVersion() == 5) {
                endpoint.publishComplete(id, MqttPubCompReasonCode.PACKET_IDENTIFIER_NOT_FOUND, MqttProperties.NO_PROPERTIES)
            } else {
                endpoint.publishComplete(id)
            }
        }
    }

    private fun receivingInFlightMessagesPeriodicCheck() {
        inFlightMessagesRcv.forEach { (id, inFlightMessage) ->
            if (inFlightMessage.retryCount < Const.QOS2_RETRY_COUNT) {
                val now = System.currentTimeMillis()
                if (inFlightMessage.lastTryTime + RETRY_INTERVAL_MS < now) {
                    inFlightMessage.lastTryTime = now
                    inFlightMessage.retryCount++
                    // MQTT 5 §4.4: no in-session retransmission
                    if (endpoint.protocolVersion() != 5) {
                        logger.finest { "Client [$clientId] Publish: retry message [${id}] for topic [${inFlightMessage.message.topicName}] [${Utils.getCurrentFunctionName()}]" }
                        endpoint.publishReceived(id)
                    }
                }
            } else {
                logger.warning { "Client [$clientId] Publish: Message [${id}] for topic [${inFlightMessage.message.topicName}] not delivered  [${Utils.getCurrentFunctionName()}]" }
                inFlightMessagesRcv.remove(id)
                updateSessionHandlerInFlight()
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
            0, 1, 2 -> {
                // QoS 0 gets packet id 0, QoS 1/2 get their packet id in publishMessage
                val msg = if (message.qosLevel == 0) message.cloneWithNewMessageId(0) else message
                if (endpoint.isConnected) {
                    publishMessage(msg)
                    logger.finest { "Client [$clientId] QoS [${msg.qosLevel}] message for topic [${msg.topicName}] delivered [${Utils.getCurrentFunctionName()}]" }
                } else {
                    logger.finest { "Client [$clientId] QoS [${msg.qosLevel}] message for topic [${msg.topicName}] not delivered, client not connected [${Utils.getCurrentFunctionName()}]" }
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
            // Delivery-time ACL filtering: check the concrete topic before delivering,
            // so wildcard subscriptions overlapping a deny rule and rules changed
            // after subscribing are enforced (same as the Go edge broker)
            val username = authenticatedUser?.username ?: Const.ANONYMOUS_USER
            if (userManager.isUserManagementEnabled() && !userManager.isAdmin(username)) {
                if (!userManager.canSubscribe(username, message.topicName, clientId)) {
                    logger.finest { "Client [$clientId] Message for topic [${message.topicName}] dropped by delivery-time ACL filter for user [$username]" }
                    return
                }
            }

            // Increment messages sent to client ONLY when actually publishing to endpoint
            if (message.qosLevel == 0) {
                publishQoS0(message)
            } else {
                // MQTT v5.0 Flow Control (Phase 8): Enforce client's Receive Maximum.
                // Messages beyond the window wait in the pending queue (keeping their order)
                // and are sent when PUBACK/PUBCOMP frees a slot.
                val maxInFlight = maxInFlight()
                var messageToSend: BrokerMessage? = null
                synchronized(outboundLock) {
                    if (inFlightMessagesSnd.size < maxInFlight && pendingMessagesSnd.isEmpty()) {
                        messageToSend = addInFlightMessage(message)
                    } else if (pendingMessagesSnd.size < Monster.getMaxQueuedMessages()) {
                        pendingMessagesSnd.addLast(message)
                        logger.finest { "Client [$clientId] QoS [${message.qosLevel}] message for topic [${message.topicName}] queued, in-flight window full [${Utils.getCurrentFunctionName()}]" }
                    } else {
                        receiveMaximumLastSampledMessageId = message.messageId
                        receiveMaximumLastSampledTopic = message.topicName
                        receiveMaximumLastSampledQos = message.qosLevel
                        receiveMaximumLastSampledInFlightObserved = inFlightMessagesSnd.size

                        if (!receiveMaximumWarningActive) {
                            receiveMaximumWarningActive = true
                            receiveMaximumWarningSince = Instant.now()
                            receiveMaximumSuppressedWarnings = 0L
                            logger.warning { "Client [$clientId] QoS [${message.qosLevel}] message for topic [${message.topicName}] not delivered, in-flight window full (${inFlightMessagesSnd.size}/$maxInFlight) and pending queue full (${pendingMessagesSnd.size}/${Monster.getMaxQueuedMessages()}) [${Utils.getCurrentFunctionName()}]" }
                        } else {
                            receiveMaximumSuppressedWarnings++
                        }
                    }
                }

                messageToSend?.let { msg ->
                    updateSessionHandlerInFlight()
                    sessionHandler.incrementMessagesOut(clientId)
                    publishInFlightToEndpoint(msg)
                    logger.finest { "Client [$clientId] QoS [${msg.qosLevel}] message [${msg.messageId}] for topic [${msg.topicName}] delivered [${Utils.getCurrentFunctionName()}]" }
                }
            }
        }
    }

    // Caller holds outboundLock
    private fun addInFlightMessage(message: BrokerMessage): BrokerMessage {
        val msg = message.cloneWithNewMessageId(nextFreeMessageId())
        inFlightMessagesSnd[msg.messageId] = InFlightMessage(msg)
        return msg
    }

    // The DUP flag of the incoming PUBLISH is not propagated to subscribers (MQTT 3.1.1/5 §3.3.1.1),
    // it is only set when the broker re-sends a QoS 1/2 message itself.
    private fun publishToEndpoint(message: BrokerMessage, dup: Boolean = false): Future<Int> {
        val subscriptionIds = if (endpoint.protocolVersion() == 5) {
            sessionHandler.getSubscriptionIdentifiers(clientId, message.topicName)
        } else {
            emptyList()
        }
        return message.publishToEndpoint(endpoint, subscriptionIdentifiers = subscriptionIds, dup = dup)
    }

    /**
     * Send an in-flight QoS 1/2 message. The retry timer starts when the packet has actually been
     * written to the socket, not when it was queued for writing, so a backlogged write queue does
     * not trigger premature retries.
     */
    private fun publishInFlightToEndpoint(message: BrokerMessage, dup: Boolean = false) {
        publishToEndpoint(message, dup).onSuccess {
            synchronized(outboundLock) {
                inFlightMessagesSnd[message.messageId]?.let { if (it.message === message) it.lastTryTime = System.currentTimeMillis() }
            }
        }
    }

    /**
     * Send a QoS 0 message. While the socket write queue is full, messages are buffered up to
     * MaxQueuedQoS0Messages and then dropped (oldest or newest, MqttTcpServer.QoS0OverflowPolicy),
     * so a slow subscriber cannot make the broker buffer without limit.
     */
    private fun publishQoS0(message: BrokerMessage) {
        val limit = Monster.getMaxQueuedQoS0Messages()
        val s = socket
        if (limit <= 0 || s == null) {
            sessionHandler.incrementMessagesOut(clientId)
            publishToEndpoint(message)
            return
        }
        var sendDirectly = false
        var dropped = false
        synchronized(outboundLock) {
            if (pendingQoS0.isEmpty() && !flushingQoS0 && !s.writeQueueFull()) {
                sendDirectly = true
            } else if (pendingQoS0.size < limit) {
                pendingQoS0.addLast(message)
            } else {
                dropped = true
                if (Monster.isQoS0DropOldest()) {
                    pendingQoS0.removeFirst()
                    pendingQoS0.addLast(message)
                }
            }
        }
        when {
            sendDirectly -> {
                sessionHandler.incrementMessagesOut(clientId)
                publishToEndpoint(message)
            }
            dropped -> qos0MessageDropped()
            // The write queue may have drained between the check and the enqueue
            !s.writeQueueFull() -> flushQoS0()
        }
    }

    private fun qos0MessageDropped() {
        sessionHandler.incrementMessagesDropped(clientId)
        val now = System.currentTimeMillis()
        val count = synchronized(outboundLock) {
            qos0DroppedSinceWarning++
            if (now - qos0LastDropWarning < 10_000) return
            qos0LastDropWarning = now
            qos0DroppedSinceWarning.also { qos0DroppedSinceWarning = 0 }
        }
        logger.warning { "Client [$clientId] slow subscriber, [$count] QoS 0 message(s) dropped, QoS 0 queue full (${Monster.getMaxQueuedQoS0Messages()}) [${Utils.getCurrentFunctionName()}]" }
    }

    /**
     * Send buffered QoS 0 messages while the socket accepts writes. Called by the socket's
     * drain handler and the periodic check.
     */
    private fun flushQoS0() {
        val s = socket ?: return
        synchronized(outboundLock) {
            if (flushingQoS0 || pendingQoS0.isEmpty()) return
            flushingQoS0 = true
        }
        try {
            while (endpoint.isConnected && !s.writeQueueFull()) {
                val message = synchronized(outboundLock) { pendingQoS0.removeFirstOrNull() } ?: break
                sessionHandler.incrementMessagesOut(clientId)
                publishToEndpoint(message)
            }
        } finally {
            synchronized(outboundLock) { flushingQoS0 = false }
        }
    }

    /**
     * Stop reading from this client while the broker is saturated, so TCP flow control slows
     * the publisher down. Reading resumes when the broker recovers, or after MaxPauseMs at the
     * latest so that keep-alive and acknowledgements are not held back for too long.
     */
    private fun pauseReading() {
        val s = socket ?: return
        if (readPaused) return
        readPaused = true
        Backpressure.pauseCount.incrementAndGet()
        s.pause()
        val start = System.currentTimeMillis()
        fun check() {
            if (endpoint.isConnected && Backpressure.isSaturated() && System.currentTimeMillis() - start < Backpressure.maxPauseMs) {
                vertx.setTimer(20) { check() }
            } else {
                readPaused = false
                s.resume()
            }
        }
        vertx.setTimer(20) { check() }
    }

    /**
     * Move pending messages into the in-flight window as long as it has free slots.
     */
    private fun sendPendingMessages() {
        val maxInFlight = maxInFlight()
        var messagesToSend: MutableList<BrokerMessage>? = null
        synchronized(outboundLock) {
            while (pendingMessagesSnd.isNotEmpty() && inFlightMessagesSnd.size < maxInFlight) {
                val msg = addInFlightMessage(pendingMessagesSnd.removeFirst())
                (messagesToSend ?: mutableListOf<BrokerMessage>().also { messagesToSend = it }).add(msg)
            }
            checkReceiveMaximumRecovery()
        }

        messagesToSend?.let { messages ->
            updateSessionHandlerInFlight()
            messages.forEach { msg ->
                sessionHandler.incrementMessagesOut(clientId)
                publishInFlightToEndpoint(msg)
                logger.finest { "Client [$clientId] Subscribe: pending message [${msg.messageId}] delivered [${Utils.getCurrentFunctionName()}]" }
            }
        }
    }

    // Caller holds outboundLock
    private fun checkReceiveMaximumRecovery() {
        if (receiveMaximumWarningActive && pendingMessagesSnd.isEmpty()) {
            val since = receiveMaximumWarningSince
            val durationStr = if (since != null) {
                val duration = java.time.Duration.between(since, Instant.now())
                "${duration.toMillis() / 1000.0}s"
            } else {
                "unknown"
            }
            logger.info { "Client [$clientId] Pending queue recovered (queue size is 0). Saturated for $durationStr. Suppressed $receiveMaximumSuppressedWarnings warnings. Last dropped topic: $receiveMaximumLastSampledTopic (QoS $receiveMaximumLastSampledQos)." }

            receiveMaximumWarningActive = false
            receiveMaximumWarningSince = null
            receiveMaximumSuppressedWarnings = 0L
            receiveMaximumLastSampledMessageId = null
            receiveMaximumLastSampledTopic = null
            receiveMaximumLastSampledQos = null
            receiveMaximumLastSampledInFlightObserved = null
        }
    }

    /**
     * Complete an outbound QoS 1/2 message (PUBACK or PUBCOMP received).
     */
    private fun publishMessageCompleted(id: Int, ackName: String) {
        val inFlightMessage = synchronized(outboundLock) { inFlightMessagesSnd.remove(id) }
        if (inFlightMessage == null) {
            logger.fine { "Client [$clientId] $ackName: got id [$id] but no matching message in in-flight queue [${Utils.getCurrentFunctionName()}]" }
            return
        }
        logger.finest { "Client [$clientId] $ackName: got id [$id] [${Utils.getCurrentFunctionName()}]" }
        updateSessionHandlerInFlight()

        val message = inFlightMessage.message
        if (message.isQueued) {
            sessionHandler.removeMessage(clientId, message.messageUuid)
        }

        sendPendingMessages()
        if (message.isQueued && isProcessingQueue) {
            processNextMessage()
        }
    }

    private fun consumeMessageQoS1(busMessage: Message<BrokerMessage>) {
        val message = busMessage.body()
        if (endpoint.isConnected) {
            publishMessage(message)
            busMessage.reply(true)
        } else {
            logger.finest { "Client [$clientId] QoS [1] message for topic [${message.topicName}] not delivered, client not connected [${Utils.getCurrentFunctionName()}]" }
            busMessage.reply(false)
        }
    }

    private fun publishAcknowledgeHandler(id: Int) { // QoS 1
        if (!ready) {
            logger.info("Client [$clientId] Connection not ready, queuing PUBACK id [$id]")
            pendingEvents.add { publishAcknowledgeHandler(id) }
            return
        }
        publishMessageCompleted(id, "PUBACK")
    }

    private fun consumeMessageQoS2(busMessage: Message<BrokerMessage>) {
        val message = busMessage.body()
        if (endpoint.isConnected) {
            publishMessage(message)
            busMessage.reply(true)
        } else {
            logger.finest { "Client [$clientId] QoS [2] message for topic [${message.topicName}] not delivered, client not connected [${Utils.getCurrentFunctionName()}]" }
            busMessage.reply(false)
        }
    }

    private fun publishedReceivedHandler(id: Int) { // QoS 2
        if (!ready) {
            logger.info("Client [$clientId] Connection not ready, queuing PUBREC id [$id]")
            pendingEvents.add { publishedReceivedHandler(id) }
            return
        }

        val found = synchronized(outboundLock) {
            inFlightMessagesSnd[id]?.let { it.stage = 2; true } ?: false
        }

        if (found) {
            logger.finest { "Client [$clientId] PUBREC: got received id [$id], now sending release to client [${Utils.getCurrentFunctionName()}]" }
            endpoint.publishRelease(id)
        } else {
            logger.fine { "Client [$clientId] PUBREC: got received id [$id] but no matching message in in-flight queue [${Utils.getCurrentFunctionName()}]" }
        }
    }

    private fun publishCompletionHandler(id: Int) { // QoS 2
        if (!ready) {
            logger.info("Client [$clientId] Connection not ready, queuing PUBCOMP id [$id]")
            pendingEvents.add { publishCompletionHandler(id) }
            return
        }
        publishMessageCompleted(id, "PUBCOMP")
    }

    private fun sendingInFlightMessagesPeriodicCheck() {
        flushQoS0() // fallback in case a drain was missed
        val messagesToRetry = mutableListOf<Pair<InFlightMessage, Int>>()
        var removed = false
        val now = System.currentTimeMillis()
        val mqtt5 = endpoint.protocolVersion() == 5

        synchronized(outboundLock) {
            val iterator = inFlightMessagesSnd.values.iterator()
            while (iterator.hasNext()) {
                val inFlightMessage = iterator.next()
                if (inFlightMessage.retryCount < Const.QOS2_RETRY_COUNT) {
                    if (inFlightMessage.lastTryTime + RETRY_INTERVAL_MS < now) {
                        inFlightMessage.lastTryTime = now
                        inFlightMessage.retryCount++
                        // MQTT 5 §4.4: PUBLISH/PUBREL are re-sent only on reconnect, never while the
                        // connection is alive. The retry count still expires unacknowledged messages.
                        if (!mqtt5) {
                            val action = when (inFlightMessage.stage) {
                                1 -> 1
                                2 -> 2
                                else -> 3
                            }
                            messagesToRetry.add(Pair(inFlightMessage, action))
                        }
                    }
                } else {
                    // Max retries reached: remove it
                    iterator.remove()
                    removed = true
                    messagesToRetry.add(Pair(inFlightMessage, 4))
                }
            }
        }

        // Execute actions outside lock
        messagesToRetry.forEach { (inFlightMessage, retryAction) ->
            when (retryAction) {
                1 -> {
                    logger.finest { "Client [$clientId] PUBACK/PUBREC: retry message [${inFlightMessage.message.messageId}] stage [1] for topic [${inFlightMessage.message.topicName}] [${Utils.getCurrentFunctionName()}]" }
                    publishInFlightToEndpoint(inFlightMessage.message, dup = true)
                }
                2 -> {
                    logger.finest { "Client [$clientId] PUBACK/PUBREC: retry message [${inFlightMessage.message.messageId}] stage [2] for topic [${inFlightMessage.message.topicName}] [${Utils.getCurrentFunctionName()}]" }
                    endpoint.publishRelease(inFlightMessage.message.messageId)
                }
                3 -> {
                    logger.warning { "Client [$clientId] PUBACK/PUBREC: unknown stage [${inFlightMessage.stage}] for message [${inFlightMessage.message.messageId}] [${Utils.getCurrentFunctionName()}]" }
                }
                4 -> {
                    logger.warning { "Client [$clientId] PUBACK/PUBREC: Message [${inFlightMessage.message.messageId}] for topic [${inFlightMessage.message.topicName}] not delivered [${Utils.getCurrentFunctionName()}]" }
                }
            }
        }

        if (removed) {
            updateSessionHandlerInFlight()
            sendPendingMessages()
        }

        // Periodic wakeup fallback: if the queue processing is idle but the client is connected,
        // ready, and has no active messages in flight, trigger a wakeup to fetch any expired/recovered locked messages!
        if (!isProcessingQueue && ready && endpoint.isConnected && inFlightCount() == 0) {
            vertx.runOnContext { onMessageAvailable() }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Last will
    // -----------------------------------------------------------------------------------------------------------------

    private fun sendLastWill() {
        if (!connectionAccepted) {
            logger.fine { "Client [$clientId] Connection was not accepted - skipping Last Will" }
            return
        }
        endpoint.will()?.let { will ->
            if (will.isWillFlag) {
                val msg = BrokerMessage(clientId, will)
                val topicName = msg.topicName

                // System topic restriction
                if (topicName.startsWith(Const.SYS_TOPIC_NAME)) {
                    logger.warning("Client [$clientId] Last Will for system topic [$topicName] not allowed!")
                    return
                }

                // MQTT spec §3.3.2.1: MUST NOT contain wildcards
                if (topicName.contains('+') || topicName.contains('#')) {
                    logger.warning("Client [$clientId] Last Will topic [$topicName] contains wildcards!")
                    return
                }

                // ACL check
                val username = authenticatedUser?.username ?: Const.ANONYMOUS_USER
                val canPublish = if (userManager.isUserManagementEnabled()) {
                    userManager.canPublish(username, topicName, clientId)
                } else {
                    true
                }
                if (!canPublish) {
                    logger.warning("Client [$clientId] Last Will publish DENIED for [$topicName] - user [$username] lacks permission")
                    return
                }

                // Topic schema policy validation
                val schemaPolicyCache = TopicSchemaPolicyCache.getInstance()
                if (schemaPolicyCache != null) {
                    val nsEntry = schemaPolicyCache.matchNamespace(topicName)
                    if (nsEntry != null) {
                        val result = nsEntry.validator.validate(msg.getPayloadAsString())
                        if (!result.valid) {
                            logger.warning("Client [$clientId] Last Will publish REJECTED by schema for [$topicName]: ${result.errorDetail}")
                            return
                        }
                    }
                }

                // MQTT v5: Honor Will Delay Interval (Property 24)
                if (willDelayInterval > 0 && endpoint.protocolVersion() == 5) {
                    logger.info("Client [$clientId] Will Delay Interval: ${willDelayInterval}s - scheduling Last Will")
                    willDelayTimerId = vertx.setTimer(willDelayInterval * 1000) {
                        logger.fine { "Client [$clientId] Will Delay expired - publishing Last Will [${Utils.getCurrentFunctionName()}]" }
                        sessionHandler.publishMessage(msg)
                        willDelayTimerId = null
                    }
                } else {
                    // No delay - publish immediately
                    logger.fine { "Client [$clientId] Sending Last-Will message [${Utils.getCurrentFunctionName()}]" }
                    sessionHandler.publishMessage(msg)
                }
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Disconnect handling
    // -----------------------------------------------------------------------------------------------------------------

    private fun disconnectHandler() {
        // Graceful disconnect by the client, closeHandler is also called after this
        val protocolVersion = endpoint.protocolVersion()
        if (protocolVersion == 5) {
            logger.info("Client [$clientId] MQTT v5.0 DISCONNECT packet received (graceful disconnect)")
        } else {
            logger.fine { "Client [$clientId] MQTT v3.1.1 graceful disconnect [${endpoint.isConnected}]" }
        }
        gracefulDisconnected = true
    }

    private fun closeHandler() {
        if (!gracefulDisconnected) {
            logger.info("Client [$clientId] Unexpected close (no DISCONNECT packet received) [connected=${endpoint.isConnected}]")
        } else {
            logger.fine { "Client [$clientId] Close received after graceful disconnect [${endpoint.isConnected}]" }
        }
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
        if (connectionAccepted && pendingEvents.isNotEmpty()) {
            logger.info("Client [$clientId] Connection closing, draining ${pendingEvents.size} pending queued events before session teardown")
            val events = ArrayList(pendingEvents)
            pendingEvents.clear()
            events.forEach { it.invoke() }
        }
        if (endpoint.isConnected) {
            logger.fine { "Client [$clientId] Send close  [${Utils.getCurrentFunctionName()}]" }
            endpoint.close()
        }
        stopEndpoint()
        sessionHandler.updateInFlightCounts(clientId, 0, 0)
    }
}
