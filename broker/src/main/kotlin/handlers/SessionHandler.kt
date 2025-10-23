package at.rocworks.handlers

import at.rocworks.data.TopicTree

import at.rocworks.Const
import at.rocworks.bus.EventBusAddresses
import at.rocworks.Monster
import at.rocworks.MqttClient
import at.rocworks.Utils
import at.rocworks.bus.IMessageBus
import at.rocworks.extensions.ApiService
import at.rocworks.cluster.DataReplicator
import at.rocworks.cluster.SetMapReplicator
import at.rocworks.data.*
import at.rocworks.stores.ISessionStoreAsync
import java.util.concurrent.locks.ReentrantLock
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.mqtt.MqttWill
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

open class SessionHandler(
    private val sessionStore: ISessionStoreAsync,
    private val messageBus: IMessageBus,
    private val messageHandler: MessageHandler,
    private val enqueueMessages: Boolean
): AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    // Dual-index subscription manager: exact (O(1)) + wildcard (O(depth))
    // This replaces the single TopicTree for better performance with large subscription counts
    private val subscriptionManager = SubscriptionManager()
    private val clientStatus = ConcurrentHashMap<String, ClientStatus>() // ClientId + Status

    // Distributed client-to-node mapping using ClusterDataReplicator
    private lateinit var clientNodeMapping: DataReplicator<String> // ClientId -> NodeId

    // Track which nodes have subscriptions for each topic using ClusterSetMapReplicator
    private lateinit var topicNodeMapping: SetMapReplicator // TopicFilter -> Set<NodeId>

    // Metrics tracking
    private val clientMetrics = ConcurrentHashMap<String, SessionMetrics>() // ClientId -> Metrics
    private val clientDetails = ConcurrentHashMap<String, ClientDetails>() // ClientId -> Session details

    // Message bus metrics (inter-node communication)
    private val messageBusOut = AtomicLong(0) // Messages sent to other nodes
    private val messageBusIn = AtomicLong(0)  // Messages received from other nodes

    // Timestamp tracking for rate calculations
    private var lastMetricsResetTime = System.currentTimeMillis()
    private val messageBusLastResetTime = AtomicLong(System.currentTimeMillis())

    private val subAddQueue: ArrayBlockingQueue<MqttSubscription> = ArrayBlockingQueue(Monster.getSubscriptionQueueSize())
    private val subDelQueue: ArrayBlockingQueue<MqttSubscription> = ArrayBlockingQueue(Monster.getSubscriptionQueueSize())

    private val msgAddQueue: ArrayBlockingQueue<Pair<BrokerMessage, List<String>>> = ArrayBlockingQueue(Monster.getMessageQueueSize())
    private val msgDelQueue: ArrayBlockingQueue<Pair<String, String>> = ArrayBlockingQueue(Monster.getMessageQueueSize())

    // Use unified EventBus addresses
    private val subscriptionAddAddress = EventBusAddresses.Cluster.SUBSCRIPTION_ADD
    private val subscriptionDelAddress = EventBusAddresses.Cluster.SUBSCRIPTION_DELETE
    private val clientStatusAddress = EventBusAddresses.Cluster.CLIENT_STATUS
    private val clientMappingAddress = EventBusAddresses.Cluster.CLIENT_NODE_MAPPING
    private val topicMappingAddress = EventBusAddresses.Cluster.TOPIC_NODE_MAPPING
    private fun nodeMessageAddress(nodeId: String) = EventBusAddresses.Node.messages(nodeId)
    private fun localNodeMessageAddress() = nodeMessageAddress(Monster.getClusterNodeId(vertx))

    private val sparkplugHandler = Monster.getSparkplugExtension()

    private val inFlightMessages = HashMap<String, ArrayBlockingQueue<BrokerMessage>>()

    // Bulk messaging buffers
    private data class BulkMessageBuffer(
        val messages: ArrayBlockingQueue<BrokerMessage>,
        var lastFlushTime: Long = System.currentTimeMillis(),
        val lock: ReentrantLock = ReentrantLock()
    )

    private val bulkMessagingEnabled = Monster.isBulkMessagingEnabled()
    private val bulkMessagingTimeoutMs = Monster.getBulkMessagingTimeoutMs()
    private val bulkMessagingBulkSize = Monster.getBulkMessagingBulkSize()

    private val clientBulkBuffer = ConcurrentHashMap<String, BulkMessageBuffer>()
    private val nodeBulkBuffer = ConcurrentHashMap<String, BulkMessageBuffer>()
    private var bulkFlushTimerId: Long = -1
    private var bulkMessagingMetricsTimerId: Long = -1

    // For bulk messaging per-second rate calculations
    private var lastBulkMessagingMetricsTime = System.currentTimeMillis()
    private val bulkMessagingClientsFlushed = AtomicLong(0)
    private val bulkMessagingNodesFlushed = AtomicLong(0)
    private var lastBulkMessagingClientsFlushed = 0L
    private var lastBulkMessagingNodesFlushed = 0L

    // Publish bulk processing (topic grouping + worker threads)
    private data class PublishBulkBuffer(
        val messages: ArrayBlockingQueue<BrokerMessage>,
        var lastFlushTime: Long = System.currentTimeMillis(),
        val lock: ReentrantLock = ReentrantLock()
    )

    private val publishBulkProcessingEnabled = Monster.isPublishBulkProcessingEnabled()
    private val publishBulkTimeoutMs = Monster.getPublishBulkTimeoutMs()
    private val publishBulkSize = Monster.getPublishBulkSize()
    private val publishWorkerThreads = Monster.getPublishWorkerThreads()

    private val publishBulkBuffer = PublishBulkBuffer(ArrayBlockingQueue(publishBulkSize * 2))
    private var publishBatchFlushTimerId: Long = -1
    private var publishWorkerPool: PublishWorkerPool? = null
    private var publishWorkerLogTimerId: Long = -1

    // Message listeners for external subscribers (e.g., GraphQL subscriptions)
    // Map: listenerId -> (topic filters, callback function)
    private val messageListeners = ConcurrentHashMap<String, Pair<List<String>, (BrokerMessage) -> Unit>>()

    // Track subscription cleanup for GraphQL listeners (listenerId -> topic filters)
    private val graphqlListenerTopics = ConcurrentHashMap<String, List<String>>()

    private fun commandAddress() = EventBusAddresses.Node.commands(deploymentID())
    private fun metricsAddress() = EventBusAddresses.Node.metrics(Monster.getClusterNodeId(vertx))
    // REMOVED: messageAddress() - no longer using broadcast message bus

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    companion object {
        // Use global command key from Const to ensure interoperability with clients (e.g., MqttClient.consumeCommand)
        const val COMMAND_SUBSCRIBE = "S"
        const val COMMAND_UNSUBSCRIBE = "U"
    }

    enum class ClientStatus {
        UNKNOWN, CREATED, ONLINE, PAUSED, DELETE
    }

    data class SessionMetrics(
        val messagesIn: AtomicLong = AtomicLong(0),
        val messagesOut: AtomicLong = AtomicLong(0),
        var lastResetTime: Long = System.currentTimeMillis()
    )

    data class ClientDetails(
        val nodeId: String,
        val clientAddress: String?,
        val cleanSession: Boolean,
        val sessionExpiryInterval: Int?,
        val information: String?
    )

    override fun start(startPromise: Promise<Void>) {
        logger.info("Start session handler...")

        // Initialize cluster data replicators
        clientNodeMapping = DataReplicator(vertx, clientMappingAddress, null, "ClientNodeMapping")
        topicNodeMapping = SetMapReplicator(vertx, topicMappingAddress, "TopicNodeMapping")

        // Register codec for client-node mapping (still needed for compatibility with HealthHandler if needed)
        vertx.eventBus().registerDefaultCodec(ClientNodeMapping::class.java, ClientNodeMappingCodec())

        vertx.eventBus().consumer<MqttSubscription>(subscriptionAddAddress) { message ->
            val subscription = message.body()
            // Add to subscription manager (routes to exact or wildcard index)
            subscriptionManager.subscribe(subscription.clientId, subscription.topicName, subscription.qos.value())

            // Track topic subscriptions by node for targeted publishing (cluster replication)
            val nodeId = clientNodeMapping.get(subscription.clientId) ?: Monster.getClusterNodeId(vertx)
            topicNodeMapping.addToSet(subscription.topicName, nodeId)
            logger.finest { "Added topic subscription [${subscription.topicName}] for node [${nodeId}]" }
        }

        vertx.eventBus().consumer<MqttSubscription>(subscriptionDelAddress) { message ->
            val subscription = message.body()
            // Remove from subscription manager
            subscriptionManager.unsubscribe(subscription.clientId, subscription.topicName)

            // Clean up topic-node mapping if no more clients on this node for this topic
            val nodeId = clientNodeMapping.get(subscription.clientId) ?: Monster.getClusterNodeId(vertx)
            val remainingClientsOnNode = subscriptionManager.findAllSubscribers(subscription.topicName)
                .any { (clientId, _) -> clientNodeMapping.get(clientId) == nodeId }

            if (!remainingClientsOnNode) {
                topicNodeMapping.removeFromSet(subscription.topicName, nodeId)
                logger.finest { "Removed topic subscription [${subscription.topicName}] for node [${nodeId}]" }
            }
        }

        // Log the replication mode
        if (Monster.isClustered()) {
            logger.info("Using ClusterDataReplicator for distributed data synchronization in cluster mode")
        } else {
            logger.info("Using ClusterDataReplicator in local mode (no replication)")
        }

        vertx.eventBus().consumer<JsonObject>(clientStatusAddress) { message ->
            logger.fine { "Client status [${message.body()}]" }
            val clientId = message.body().getString("ClientId", "")
            val status = ClientStatus.valueOf(message.body().getString("Status", ""))
            val deliveryOptions = DeliveryOptions(JsonObject().put("NodeId", Monster.getClusterNodeId(vertx)))

            when (status) {
                ClientStatus.CREATED -> {
                    clientStatus[clientId] = ClientStatus.CREATED
                    message.reply(true, deliveryOptions)
                }
                ClientStatus.ONLINE -> {
                    inFlightMessages[clientId]?.let { messages ->
                        logger.fine { "Publishing [${messages.count()}] in-flight messages to client [${clientId}] [${Utils.getCurrentFunctionName()}]" }
                        messages.forEach { message -> sendMessageToClient(clientId, message)}
                    }
                    inFlightMessages.remove(clientId)
                    clientStatus[clientId] = ClientStatus.ONLINE
                    message.reply(true, deliveryOptions)
                }
                ClientStatus.PAUSED -> {
                    clientStatus[clientId] = ClientStatus.PAUSED
                    message.reply(true, deliveryOptions)
                }
                ClientStatus.DELETE -> {
                    clientStatus.remove(clientId)
                    message.reply(true, deliveryOptions)
                }
                else -> {
                    logger.warning("Unknown client status [${message.body()}] [${Utils.getCurrentFunctionName()}]")
                    message.reply(false, deliveryOptions)
                }
            }
        }

        vertx.eventBus().consumer<JsonObject>(commandAddress()) { message ->
            message.body()?.let { payload ->
                logger.finest { "Received request [${payload}] [${Utils.getCurrentFunctionName()}]" }
                when (payload.getString(Const.COMMAND_KEY)) {
                    COMMAND_SUBSCRIBE -> subscribeCommand(message)
                    COMMAND_UNSUBSCRIBE -> unsubscribeCommand(message)
                    else -> logger.warning("Unknown command [${payload}] [${Utils.getCurrentFunctionName()}]")
                }
            }
        }

        // Metrics query handler
        vertx.eventBus().consumer<JsonObject>(metricsAddress()) { message ->
            val metrics = JsonObject()
            var totalMessagesIn = 0L
            var totalMessagesOut = 0L

            clientMetrics.values.forEach { sessionMetrics ->
                totalMessagesIn += sessionMetrics.messagesIn.get()
                totalMessagesOut += sessionMetrics.messagesOut.get()
            }

            val subStats = subscriptionManager.getStats()
            metrics.put("messagesIn", totalMessagesIn)
                   .put("messagesOut", totalMessagesOut)
                   .put("nodeSessionCount", clientMetrics.size)
                   .put("messageBusIn", messageBusIn.get())
                   .put("messageBusOut", messageBusOut.get())
                   .put("topicIndexSize", subStats.totalExactSubscriptions + subStats.totalWildcardSubscriptions)
                   .put("exactTopics", subStats.totalExactTopics)
                   .put("exactSubscriptions", subStats.totalExactSubscriptions)
                   .put("wildcardPatterns", subStats.totalWildcardPatterns)
                   .put("clientNodeMappingSize", clientNodeMapping.size())
                   .put("topicNodeMappingSize", topicNodeMapping.size())

            message.reply(metrics)
        }

        // Metrics and reset handler - returns current values and resets counters to 0
        vertx.eventBus().consumer<JsonObject>(EventBusAddresses.Node.metricsAndReset(Monster.getClusterNodeId(vertx))) { message ->
            val currentTime = System.currentTimeMillis()
            val metrics = JsonObject()
            var totalMessagesIn = 0L
            var totalMessagesOut = 0L
            var totalMessagesInRate = 0.0
            var totalMessagesOutRate = 0.0
            val sessionMetricsArray = io.vertx.core.json.JsonArray()

            // Collect all client metrics with connection statistics
            val clientStatsFutures = mutableListOf<Future<Void>>()

            clientMetrics.forEach { (clientId, sessionMetrics) ->
                val inCount = sessionMetrics.messagesIn.getAndSet(0)
                val outCount = sessionMetrics.messagesOut.getAndSet(0)

                // Calculate duration since last reset for this client
                val duration = (currentTime - sessionMetrics.lastResetTime) / 1000.0 // seconds
                val inRate = if (duration > 0) kotlin.math.round(inCount / duration) else 0.0
                val outRate = if (duration > 0) kotlin.math.round(outCount / duration) else 0.0

                // Update last reset time
                sessionMetrics.lastResetTime = currentTime

                totalMessagesIn += inCount
                totalMessagesOut += outCount
                totalMessagesInRate += inRate
                totalMessagesOutRate += outRate

                // Build client metrics object
                val clientMetricsJson = JsonObject()
                    .put("clientId", clientId)
                    .put("messagesIn", inCount)
                    .put("messagesOut", outCount)
                    .put("messagesInRate", inRate)
                    .put("messagesOutRate", outRate)

                // Try to get connection statistics from the client (if it's an MQTT client)
                val statsPromise = Promise.promise<Void>()
                val statsRequest = JsonObject().put(Const.COMMAND_KEY, Const.COMMAND_STATISTICS)
                vertx.eventBus().request<JsonObject>(
                    MqttClient.getCommandAddress(clientId),
                    statsRequest,
                    io.vertx.core.eventbus.DeliveryOptions().setSendTimeout(100)
                ).onComplete { statsResult ->
                    if (statsResult.succeeded()) {
                        val stats = statsResult.result().body()
                        clientMetricsJson
                            .put("connected", stats.getBoolean("connected", false))
                            .put("lastPing", stats.getString("lastPing", ""))
                            .put("inFlightMessagesRcv", stats.getInteger("inFlightMessagesRcv", 0))
                            .put("inFlightMessagesSnd", stats.getInteger("inFlightMessagesSnd", 0))
                    }
                    // Add metrics regardless of success or failure
                    sessionMetricsArray.add(clientMetricsJson)
                    statsPromise.complete()
                }

                clientStatsFutures.add(statsPromise.future())
            }

            // Wait for all connection statistics to be collected, then reply
            Future.all<Void>(clientStatsFutures).onComplete { _ ->
                // Calculate message bus rates
                val messageBusInCount = messageBusIn.getAndSet(0)
                val messageBusOutCount = messageBusOut.getAndSet(0)
                val lastBusResetTime = messageBusLastResetTime.getAndSet(currentTime)
                val busDuration = (currentTime - lastBusResetTime) / 1000.0 // seconds
                val messageBusInRate = if (busDuration > 0) kotlin.math.round(messageBusInCount / busDuration) else 0.0
                val messageBusOutRate = if (busDuration > 0) kotlin.math.round(messageBusOutCount / busDuration) else 0.0

                val subStats = subscriptionManager.getStats()
                metrics.put("messagesIn", totalMessagesIn)
                       .put("messagesOut", totalMessagesOut)
                       .put("messagesInRate", totalMessagesInRate)
                       .put("messagesOutRate", totalMessagesOutRate)
                       .put("nodeSessionCount", clientMetrics.size)
                       .put("messageBusIn", messageBusInCount)
                       .put("messageBusOut", messageBusOutCount)
                       .put("messageBusInRate", messageBusInRate)
                       .put("messageBusOutRate", messageBusOutRate)
                       .put("topicIndexSize", subStats.totalExactSubscriptions + subStats.totalWildcardSubscriptions)
                       .put("exactTopics", subStats.totalExactTopics)
                       .put("exactSubscriptions", subStats.totalExactSubscriptions)
                       .put("wildcardPatterns", subStats.totalWildcardPatterns)
                       .put("clientNodeMappingSize", clientNodeMapping.size())
                       .put("topicNodeMappingSize", topicNodeMapping.size())
                       .put("sessionMetrics", sessionMetricsArray)
                message.reply(metrics)
            }
        }

        // Individual session metrics handler - non-destructive
        vertx.eventBus().consumer<JsonObject>(EventBusAddresses.Node.sessionMetrics(Monster.getClusterNodeId(vertx), "*")) { message ->
            val requestedClientId = message.headers().get("clientId")

            if (requestedClientId != null) {
                val sessionMetrics = clientMetrics[requestedClientId]
                if (sessionMetrics != null) {
                    val response = JsonObject()
                        .put("clientId", requestedClientId)
                        .put("messagesIn", sessionMetrics.messagesIn.get())
                        .put("messagesOut", sessionMetrics.messagesOut.get())
                        .put("found", true)
                    message.reply(response)
                } else {
                    message.reply(JsonObject().put("found", false))
                }
            } else {
                message.fail(400, "Missing clientId header")
            }
        }

        // Individual session details handler - non-destructive
        vertx.eventBus().consumer<JsonObject>(EventBusAddresses.Node.sessionDetails(Monster.getClusterNodeId(vertx), "*")) { message ->
            val requestedClientId = message.headers().get("clientId")

            if (requestedClientId != null) {
                val clientDetails = getClientDetails(requestedClientId)
                val clientMetrics = getClientMetrics(requestedClientId)
                val clientStatus = getClientStatus(requestedClientId)

                if (clientDetails != null) {
                    val response = JsonObject()
                        .put("clientId", requestedClientId)
                        .put("nodeId", clientDetails.nodeId)
                        .put("clientAddress", clientDetails.clientAddress)
                        .put("cleanSession", clientDetails.cleanSession)
                        .put("sessionExpiryInterval", clientDetails.sessionExpiryInterval ?: 0)
                        .put("connected", clientStatus == ClientStatus.ONLINE)
                        .put("information", clientDetails.information)
                        .put("found", true)
                    message.reply(response)
                } else {
                    message.reply(JsonObject().put("found", false))
                }
            } else {
                message.fail(400, "Missing clientId header")
            }
        }

        queueWorkerThread("SubAddQueue", subAddQueue, 1000, sessionStore::addSubscriptions)
        queueWorkerThread("SubDelQueue", subDelQueue, 1000, sessionStore::delSubscriptions)

        queueWorkerThread("MsgAddQueue", msgAddQueue, 1000, sessionStore::enqueueMessages)
        queueWorkerThread("MsgDelQueue", msgDelQueue, 1000, sessionStore::removeMessages)

        // Only subscribe to message bus if it's Kafka (external source)
        // Internal Vert.x message bus broadcast is no longer used - we use targeted messaging
        val f0 = if (messageBus is at.rocworks.bus.MessageBusKafka) {
            logger.info("Subscribing to Kafka message bus for external messages [${Utils.getCurrentFunctionName()}]")
            messageBus.subscribeToMessageBus { message ->
                // Messages from Kafka should use targeted distribution
                publishMessage(message)
            }
        } else {
            logger.info("Skipping internal message bus subscription - using targeted messaging only [${Utils.getCurrentFunctionName()}]")
            Future.succeededFuture()
        }

        // Subscribe to node-specific message address for targeted messages
        // Handles both individual BrokerMessage and bulk BulkNodeMessage
        vertx.eventBus().consumer<Any>(localNodeMessageAddress()) { message ->
            when (val payload = message.body()) {
                is BrokerMessage -> {
                    messageBusIn.incrementAndGet()
                    logger.finest { "Received targeted message [${payload.topicName}] [${Utils.getCurrentFunctionName()}]" }
                    processMessageForLocalClients(payload)
                }
                is BulkNodeMessage -> {
                    messageBusIn.addAndGet(payload.messages.size.toLong())
                    logger.finest { "Received bulk message with [${payload.messages.size}] messages [${Utils.getCurrentFunctionName()}]" }
                    payload.messages.forEach { brokerMessage ->
                        processMessageForLocalClients(brokerMessage)
                    }
                }
                else -> {
                    logger.warning("Unknown message type in node consumer: ${payload?.javaClass?.simpleName} [${Utils.getCurrentFunctionName()}]")
                }
            }
        }

        // Subscribe to broadcast messages (e.g., system logs, metrics)
        vertx.eventBus().consumer<BrokerMessage>(EventBusAddresses.Cluster.BROADCAST) { message ->
            message.body()?.let { payload ->
                try {
                    // Process broadcast messages for local subscribers - no logging here to prevent loops
                    processMessageForLocalClients(payload)
                } catch (e: Exception) {
                    // Silently ignore errors to prevent logging loops
                }
            }
        }

        // Start bulk message flusher if enabled
        if (bulkMessagingEnabled) {
            logger.info("Starting bulk message flusher with timeout=${bulkMessagingTimeoutMs}ms, bulkSize=$bulkMessagingBulkSize")
            bulkFlushTimerId = vertx.setPeriodic(bulkMessagingTimeoutMs) {
                flushBulkBuffers()
            }

            // Start periodic bulk messaging metrics publishing (every 1 second)
            bulkMessagingMetricsTimerId = vertx.setPeriodic(1_000) {
                publishBulkMessagingMetrics()
            }
        }

        // Start publish bulk processing if enabled
        if (publishBulkProcessingEnabled) {
            logger.info("Starting publish bulk processor with timeout=${publishBulkTimeoutMs}ms, bulkSize=$publishBulkSize, workers=$publishWorkerThreads")

            // Initialize and start worker pool
            publishWorkerPool = PublishWorkerPool(vertx, this, publishWorkerThreads)
            publishWorkerPool!!.start()

            // Register shutdown hook for graceful worker shutdown
            Runtime.getRuntime().addShutdownHook(Thread({
                logger.info("Shutdown hook: initiated, closing worker pool")
                try {
                    publishWorkerPool?.shutdown()
                    logger.info("Shutdown hook: worker pool closed successfully")
                } catch (e: Exception) {
                    logger.severe("Shutdown hook: error closing worker pool: ${e.message}")
                }
            }, "SessionHandler-ShutdownHook"))

            // Start periodic batch flusher
            publishBatchFlushTimerId = vertx.setPeriodic(publishBulkTimeoutMs) {
                flushPublishBatch()
            }

            // Start periodic worker load logging and metrics publishing (every 1 second)
            publishWorkerLogTimerId = vertx.setPeriodic(1_000) {
                publishWorkerPool?.logWorkerLoad()
                val nodeName = Monster.getClusterNodeId(vertx)
                publishWorkerPool?.publishMetrics(nodeName)
            }
        }

        logger.info("Loading all sessions and their subscriptions [${Utils.getCurrentFunctionName()}]")
        val f1 = sessionStore.iterateAllSessions { clientId, nodeId, connected, cleanSession ->
            val localNodeId = Monster.getClusterNodeId(vertx)

            if (connected) {
                if (nodeId != localNodeId) {
                    // Client connected to another node - add to cluster mapping
                    clientNodeMapping.put(clientId, nodeId)
                    logger.finest { "Loaded connected client [${clientId}] on node [${nodeId}]" }
                } else {
                    // Client connected to this node but we're restarting - mark as paused for now
                    // It will reconnect and update the status properly
                    clientStatus[clientId] = ClientStatus.PAUSED
                    logger.finest { "Loaded local client [${clientId}] as paused (node restart)" }
                }
            } else {
                // Client is offline
                if (!cleanSession) {
                    // Persistent session - mark as paused so messages can be queued
                    clientStatus[clientId] = ClientStatus.PAUSED
                    logger.finest { "Loaded offline persistent client [${clientId}]" }
                }
                // Clean sessions that are offline are ignored (will be cleaned up by purge)
            }
        }

        logger.info("Loading all subscriptions [${Utils.getCurrentFunctionName()}]")
        val f2 = sessionStore.iterateSubscriptions { topicName, clientId, qos ->
            // Add to subscription manager (routes to exact or wildcard index)
            subscriptionManager.subscribe(clientId, topicName, qos)

            // Build topic-to-node mapping based on where client is located (cluster replication)
            val nodeId = clientNodeMapping.get(clientId) ?: Monster.getClusterNodeId(vertx)
            topicNodeMapping.addToSet(topicName, nodeId)
            logger.finest { "Loaded subscription [${topicName}] for client [${clientId}] on node [${nodeId}]" }
        }

        Future.all(f0, f1, f2).onComplete {
            if (it.succeeded()) {
                logger.info("Session handler ready [${Utils.getCurrentFunctionName()}]")
                startPromise.complete()
            } else {
                startPromise.fail(it.cause())
            }
        }
    }


    private fun <T> queueWorkerThread(
        name: String,
        queue: ArrayBlockingQueue<T>,
        blockSize: Int,
        execute: (block: List<T>)->Future<Void>
    ) /*= thread(start = true)*/ {
        logger.fine { "Start [$name] loop" }
        val block = arrayListOf<T>()
        var lastCheckTime = System.currentTimeMillis()

        fun loop() {
            vertx.executeBlocking(Callable {
                queue.poll(100, TimeUnit.MILLISECONDS)?.let { item ->
                    block.add(item)
                    while (queue.poll()?.let(block::add) != null && block.size < blockSize) {
                        // nothing to do here
                    }
                }
            }).onComplete {
                if (block.isNotEmpty()) {
                    execute(block).onComplete {
                        block.clear()
                        vertx.runOnContext { loop() }
                    }
                } else {
                    vertx.runOnContext { loop() }
                }

                val currentTime = System.currentTimeMillis()
                if (currentTime - lastCheckTime >= 1000 && queue.size > 1000) { // TODO: configurable
                    logger.warning("Queue [$name] size [${queue.size}] [${Utils.getCurrentFunctionName()}]")
                    lastCheckTime = currentTime
                }
            }
        }
        loop()
    }

    fun getClientStatus(clientId: String): ClientStatus = clientStatus[clientId] ?: ClientStatus.UNKNOWN

    // Forcefully disconnect a client via command dispatch
    fun disconnectClient(clientId: String, reason: String? = null) {
        val payload = JsonObject().put(Const.COMMAND_KEY, Const.COMMAND_DISCONNECT)
        reason?.let { payload.put("Reason", it) }
        logger.warning("Disconnecting client [$clientId] via SessionHandler" + (reason?.let { ": $it" } ?: ""))
        vertx.eventBus().send(MqttClient.getCommandAddress(clientId), payload)
    }

    // Metrics tracking methods
    fun incrementMessagesIn(clientId: String) {
        clientMetrics[clientId]?.messagesIn?.incrementAndGet()
    }

    fun incrementMessagesOut(clientId: String) {
        clientMetrics[clientId]?.messagesOut?.incrementAndGet()
    }

    fun getClientMetrics(clientId: String): SessionMetrics? = clientMetrics[clientId]

    fun getAllClientMetrics(): Map<String, SessionMetrics> = clientMetrics.toMap()

    fun getAllClientMetricsAndReset(): Map<String, at.rocworks.extensions.graphql.SessionMetrics> {
        val currentTime = System.currentTimeMillis()
        return clientMetrics.mapValues { (clientId, sessionMetrics) ->
            val inCount = sessionMetrics.messagesIn.getAndSet(0)
            val outCount = sessionMetrics.messagesOut.getAndSet(0)
            
            // Calculate duration since last reset
            val duration = (currentTime - sessionMetrics.lastResetTime) / 1000.0 // seconds
            val inRate = if (duration > 0) kotlin.math.round(inCount / duration) else 0.0
            val outRate = if (duration > 0) kotlin.math.round(outCount / duration) else 0.0
            
            // Debug logging to understand rate calculation
            logger.fine { "Client [$clientId]: inCount=$inCount, outCount=$outCount, duration=$duration, inRate=$inRate, outRate=$outRate" }
            
            // Update last reset time
            sessionMetrics.lastResetTime = currentTime
            
            at.rocworks.extensions.graphql.SessionMetrics(
                messagesIn = inRate,
                messagesOut = outRate,
                timestamp = at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
            )
        }
    }

    fun getClientMetricsWithRate(clientId: String): at.rocworks.extensions.graphql.SessionMetrics? {
        val sessionMetrics = clientMetrics[clientId] ?: return null
        val currentTime = System.currentTimeMillis()
        
        // Get counts without resetting (for individual client queries)
        val inCount = sessionMetrics.messagesIn.get()
        val outCount = sessionMetrics.messagesOut.get()
        
        // Calculate duration since last reset
        val duration = (currentTime - sessionMetrics.lastResetTime) / 1000.0 // seconds
        val inRate = if (duration > 0) kotlin.math.round(inCount / duration) else 0.0
        val outRate = if (duration > 0) kotlin.math.round(outCount / duration) else 0.0
        
        return at.rocworks.extensions.graphql.SessionMetrics(
            messagesIn = inRate,
            messagesOut = outRate,
            timestamp = at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
        )
    }

    fun getClientDetails(clientId: String): ClientDetails? = clientDetails[clientId]

    fun getSessionCount(): Int = clientMetrics.size

    fun getTopicIndexSize(): Int {
        val stats = subscriptionManager.getStats()
        return stats.totalExactSubscriptions + stats.totalWildcardSubscriptions
    }

    fun getClientNodeMappingSize(): Int = clientNodeMapping.size()

    fun getTopicNodeMappingSize(): Int = topicNodeMapping.size()

    fun getMessageBusOutCount(): Long = messageBusOut.get()

    fun getMessageBusInCount(): Long = messageBusIn.get()

    /**
     * Register a message listener for external subscribers (e.g., GraphQL subscriptions).
     * Creates virtual MQTT subscriptions so messages are routed correctly in clustered environments.
     * The listener will receive all messages matching the specified topic filters.
     *
     * @param listenerId Unique identifier for this listener
     * @param topicFilters List of MQTT topic filters (supports + and # wildcards)
     * @param callback Function called for each matching message
     */
    fun registerMessageListener(
        listenerId: String,
        topicFilters: List<String>,
        callback: (BrokerMessage) -> Unit
    ) {
        messageListeners[listenerId] = Pair(topicFilters, callback)
        graphqlListenerTopics[listenerId] = topicFilters

        // Create virtual MQTT subscriptions for cluster-aware routing
        // This ensures that in clustered environments, messages published on other nodes
        // are routed to this node where the GraphQL subscriber is listening
        val virtualClientId = "graphql-$listenerId"
        topicFilters.forEach { topicFilter ->
            val subscription = MqttSubscription(
                clientId = virtualClientId,
                topicName = topicFilter,
                qos = io.netty.handler.codec.mqtt.MqttQoS.AT_LEAST_ONCE
            )
            // This publishes to subscriptionAddAddress which is consumed by all nodes,
            // updating their topicNodeMapping so they know where to route messages
            addSubscription(subscription)
        }

        logger.fine { "Registered message listener [$listenerId] for topic filters: $topicFilters" }
    }

    /**
     * Unregister a previously registered message listener and clean up virtual subscriptions.
     *
     * @param listenerId The listener to remove
     */
    fun unregisterMessageListener(listenerId: String) {
        messageListeners.remove(listenerId)

        // Remove virtual MQTT subscriptions
        val topicFilters = graphqlListenerTopics.remove(listenerId) ?: return
        val virtualClientId = "graphql-$listenerId"

        topicFilters.forEach { topicName ->
            val subscription = MqttSubscription(
                clientId = virtualClientId,
                topicName = topicName,
                qos = io.netty.handler.codec.mqtt.MqttQoS.AT_LEAST_ONCE
            )
            // This publishes to subscriptionDelAddress which is consumed by all nodes,
            // cleaning up their topicNodeMapping
            delSubscription(subscription)
        }

        logger.fine { "Unregistered message listener [$listenerId]" }
    }

    fun setClient(clientId: String, cleanSession: Boolean, information: JsonObject): Future<Void> {
        logger.fine { "Set client [$clientId] clean session [$cleanSession] information [$information]" }

        // Initialize metrics and client details
        clientMetrics[clientId] = SessionMetrics()
        val nodeId = Monster.getClusterNodeId(vertx)
        clientDetails[clientId] = ClientDetails(
            nodeId = nodeId,
            clientAddress = information.getString("RemoteAddress"),
            cleanSession = cleanSession,
            sessionExpiryInterval = information.getInteger("sessionExpiryInterval"),
            information = information.encode()
        )

        // Set client-to-node mapping using replicator
        clientNodeMapping.put(clientId, nodeId)

        val payload = JsonObject().put("ClientId", clientId).put("Status", ClientStatus.CREATED)
        val f1 = sessionStore.setClient(clientId, nodeId, cleanSession, true, information)
        return if (Monster.isClustered()) {
            val fx = Monster.getClusterNodeIds(vertx).map {
                vertx.eventBus().request<Boolean>(clientStatusAddress, payload)
            }
            Future.all<Any>(listOf(f1)+fx as List<Future<*>>).mapEmpty()

        } else {
            val f2 = vertx.eventBus().request<Boolean>(clientStatusAddress, payload)
            Future.all(f1, f2).mapEmpty()
        }
    }

    fun onlineClient(clientId: String): Future<Void> {
        val payload = JsonObject().put("ClientId", clientId).put("Status", ClientStatus.ONLINE)
        return if (Monster.isClustered()) {
            val fx = Monster.getClusterNodeIds(vertx).map {
                vertx.eventBus().request<Boolean>(clientStatusAddress, payload)
            }
            Future.all<Any>(fx as List<Future<*>>).mapEmpty()
        } else {
            vertx.eventBus().request<Boolean>(clientStatusAddress, payload).mapEmpty()
        }
    }

    fun pauseClient(clientId: String): Future<Void> {
        val payload = JsonObject().put("ClientId", clientId).put("Status", ClientStatus.PAUSED)
        val f1 = sessionStore.setConnected(clientId, false)
        if (Monster.isClustered()) {
            val fx = Monster.getClusterNodeIds(vertx).map {
                vertx.eventBus().request<Boolean>(clientStatusAddress, payload)
            }
            return Future.all<Any>(listOf(f1)+fx as List<Future<*>>).mapEmpty()
        } else {
            val f2 = vertx.eventBus().request<Boolean>(clientStatusAddress, payload)
            return Future.all(f1, f2).mapEmpty()
        }
    }

    fun delClient(clientId: String): Future<Void> {
        // Clean up metrics and client details
        clientMetrics.remove(clientId)
        val clientDetail = clientDetails.remove(clientId)

        // Remove client from node mapping using replicator
        clientNodeMapping.remove(clientId)

        val payload = JsonObject().put("ClientId", clientId).put("Status", ClientStatus.DELETE)
        vertx.eventBus().publish(clientStatusAddress, payload)
        return sessionStore.delClient(clientId) { subscription ->
            logger.finest { "Delete subscription [$subscription]" }
            vertx.eventBus().publish(subscriptionDelAddress, subscription)
        }
    }

    fun setLastWill(clientId: String, will: MqttWill): Future<Void> {
        if (will.isWillFlag) {
            val message = BrokerMessage(clientId, will)
            return sessionStore.setLastWill(clientId, message)
        } else {
            return sessionStore.setLastWill(clientId, null)
        }
    }

    fun isPresent(clientId: String): Future<Boolean> = sessionStore.isPresent(clientId)

    private fun enqueueMessage(message: BrokerMessage, clientIds: List<String>) {
        if (enqueueMessages) {
            try {
                msgAddQueue.add(Pair(message, clientIds))
            } catch (e: IllegalStateException) {
                logger.severe("CRITICAL: Message queue overflow! Queue is full (${msgAddQueue.size}/${msgAddQueue.remainingCapacity() + msgAddQueue.size}). Message [${message.topicName}] to ${clientIds.size} clients will be LOST. Increase 'Queues.MessageQueueSize' in config.yaml")
            }
        }
    }

    fun dequeueMessages(clientId: String, callback: (BrokerMessage)->Boolean) = sessionStore.dequeueMessages(clientId, callback)

    fun removeMessage(clientId: String, messageUuid: String) {
        try {
            msgDelQueue.add(Pair(clientId, messageUuid))
        } catch (e: IllegalStateException) {
            logger.severe("CRITICAL: Message delete queue overflow! Queue is full (${msgDelQueue.size}/${msgDelQueue.remainingCapacity() + msgDelQueue.size}). Message removal for client [${clientId}] msg [${messageUuid}] will be LOST. Increase 'Queues.MessageQueueSize' in config.yaml")
        }
    }

    private fun addSubscription(subscription: MqttSubscription) {
        vertx.eventBus().publish(subscriptionAddAddress, subscription)
        try {
            subAddQueue.add(subscription)
        } catch (e: IllegalStateException) {
            logger.severe("CRITICAL: Subscription queue overflow! Queue is full (${subAddQueue.size}/${subAddQueue.remainingCapacity() + subAddQueue.size}). Client [${subscription.clientId}] subscription to [${subscription.topicName}] will be LOST. Increase 'Queues.SubscriptionQueueSize' in config.yaml")
        }
    }

    private fun delSubscription(subscription: MqttSubscription) {
        vertx.eventBus().publish(subscriptionDelAddress, subscription)
        try {
            subDelQueue.add(subscription)
        } catch (e: IllegalStateException) {
            logger.severe("CRITICAL: Subscription delete queue overflow! Queue is full (${subDelQueue.size}/${subDelQueue.remainingCapacity() + subDelQueue.size}). Client [${subscription.clientId}] unsubscription from [${subscription.topicName}] will be LOST. Increase 'Queues.SubscriptionQueueSize' in config.yaml")
        }
    }

    private fun findClients(topicName: String): Set<Pair<String, Int>> {
        // Uses dual-index: O(1) for exact + O(depth) for wildcards
        val result = subscriptionManager.findAllSubscribers(topicName).toSet()
        logger.finest { "Found [${result.size}] clients [${result.joinToString(",")}] [${Utils.getCurrentFunctionName()}]" }
        return result
    }

    fun purgeSessions() = sessionStore.purgeSessions()

    fun purgeQueuedMessages() = sessionStore.purgeQueuedMessages()

    fun iterateNodeClients(nodeId: String, callback: (clientId: String, cleanSession: Boolean, lastWill: BrokerMessage) -> Unit)
    = sessionStore.iterateNodeClients(nodeId, callback)

    // Public method for node failure handling
    fun handleNodeFailure(deadNodeId: String) {
        logger.info("Handling node failure for node [${deadNodeId}]")
        // Remove all clients from the failed node
        val removedClients = clientNodeMapping.removeIf { it.value == deadNodeId }
        if (removedClients) {
            logger.info("Removed all clients from failed node [${deadNodeId}]")
        }
        // Clean up topic mappings for the failed node
        topicNodeMapping.removeValueFromAllSets(deadNodeId)
        logger.info("Cleaned up topic mappings for failed node [${deadNodeId}]")
    }


    private fun sendMessageToClient(clientId: String, message: BrokerMessage): Future<Boolean> {
        if (!bulkMessagingEnabled) {
            // Original non-bulk behavior
            if (message.qosLevel == 0) {
                vertx.eventBus().send(MqttClient.getMessagesAddress(clientId), message)
                return Future.succeededFuture(true)
            } else {
                val promise = Promise.promise<Boolean>()
                vertx.eventBus().request<Boolean>(MqttClient.getMessagesAddress(clientId), message)
                    .onComplete { ar ->
                        promise.complete(!ar.failed() && ar.result().body())
                    }
                return promise.future()
            }
        } else {
            // Bulk messaging: buffer the message
            val buffer = clientBulkBuffer.getOrPut(clientId) {
                BulkMessageBuffer(ArrayBlockingQueue(bulkMessagingBulkSize * 2))
            }

            try {
                // ArrayBlockingQueue.add() is thread-safe, no lock needed
                buffer.messages.add(message)

                // Only lock when checking if we need to flush (to protect lastFlushTime and prevent double-flush)
                if (buffer.messages.size >= bulkMessagingBulkSize) {
                    buffer.lock.lock()
                    try {
                        // Double-check after acquiring lock
                        if (buffer.messages.size >= bulkMessagingBulkSize) {
                            val bulkMessage = BulkClientMessage(buffer.messages.toList())
                            buffer.messages.clear()
                            buffer.lastFlushTime = System.currentTimeMillis()
                            vertx.eventBus().send(MqttClient.getMessagesAddress(clientId), bulkMessage)
                            clientMetrics[clientId]?.messagesOut?.addAndGet(bulkMessage.messages.size.toLong())
                            logger.finest { "Flushed bulk to client [$clientId]: ${bulkMessage.messages.size} messages (size threshold)" }
                        }
                    } finally {
                        buffer.lock.unlock()
                    }
                }
            } catch (e: IllegalStateException) {
                logger.warning("Bulk buffer overflow for client [$clientId], sending immediately")
                vertx.eventBus().send(MqttClient.getMessagesAddress(clientId), message)
            }

            return Future.succeededFuture(true)
        }
    }

    /**
     * Flush bulk message buffers for both local clients and remote nodes.
     * Called periodically by the bulk flusher timer or when buffers reach the bulk size limit.
     */
    private fun flushBulkBuffers() {
        val currentTime = System.currentTimeMillis()

        // Flush local client buffers
        val clientsToRemove = mutableListOf<String>()
        clientBulkBuffer.forEach { (clientId, buffer) ->
            buffer.lock.lock()
            try {
                val timeSinceFlush = currentTime - buffer.lastFlushTime
                val shouldFlush = buffer.messages.size > 0 &&
                                timeSinceFlush >= bulkMessagingTimeoutMs

                if (shouldFlush) {
                    val bulkMessage = BulkClientMessage(buffer.messages.toList())
                    buffer.messages.clear()
                    buffer.lastFlushTime = currentTime

                    vertx.eventBus().send(MqttClient.getMessagesAddress(clientId), bulkMessage)
                    clientMetrics[clientId]?.messagesOut?.addAndGet(bulkMessage.messages.size.toLong())
                    bulkMessagingClientsFlushed.addAndGet(bulkMessage.messages.size.toLong())
                    logger.finest { "Flushed bulk to client [$clientId]: ${bulkMessage.messages.size} messages (timeout)" }
                }

                // Mark for removal if empty and stale
                if (buffer.messages.isEmpty() && (currentTime - buffer.lastFlushTime > 5000)) {
                    clientsToRemove.add(clientId)
                }
            } finally {
                buffer.lock.unlock()
            }
        }
        clientsToRemove.forEach { clientBulkBuffer.remove(it) }

        // Flush remote node buffers
        val nodesToRemove = mutableListOf<String>()
        nodeBulkBuffer.forEach { (nodeId, buffer) ->
            buffer.lock.lock()
            try {
                val timeSinceFlush = currentTime - buffer.lastFlushTime
                val shouldFlush = buffer.messages.size > 0 &&
                                timeSinceFlush >= bulkMessagingTimeoutMs

                if (shouldFlush) {
                    val bulkMessage = BulkNodeMessage(buffer.messages.toList())
                    buffer.messages.clear()
                    buffer.lastFlushTime = currentTime

                    vertx.eventBus().publish(nodeMessageAddress(nodeId), bulkMessage)
                    messageBusOut.incrementAndGet()
                    bulkMessagingNodesFlushed.addAndGet(bulkMessage.messages.size.toLong())
                    logger.finest { "Flushed bulk to node [$nodeId]: ${bulkMessage.messages.size} messages (timeout)" }
                }

                // Mark for removal if empty and stale
                if (buffer.messages.isEmpty() && (currentTime - buffer.lastFlushTime > 5000)) {
                    nodesToRemove.add(nodeId)
                }
            } finally {
                buffer.lock.unlock()
            }
        }
        nodesToRemove.forEach { nodeBulkBuffer.remove(it) }
    }

    private fun addInFlightMessage(clientId: String, message: BrokerMessage) {
        logger.fine { "Adding in-flight message to queue [${message.topicName}] [${message.getPayloadAsJson()}]}" }
        inFlightMessages[clientId]?.let { queue ->
            if (queue.remainingCapacity() == 0) {
                logger.warning("In-flight messages queue full [${Utils.getCurrentFunctionName()}]")
            } else {
                queue.put(message)
            }
        } ?: run {
            ArrayBlockingQueue<BrokerMessage>(10_000).let { // TODO: configurable
                inFlightMessages[clientId] = it
                it.put(message)
            }
        }
    }

    private fun flushInFlightToQueue(clientId: String) {
        inFlightMessages[clientId]?.let { queue ->
            queue.forEach { enqueueMessage(it, listOf(clientId)) }
            queue.clear()
        }
    }

    //----------------------------------------------------------------------------------------------------

    fun subscribeRequest(client: MqttClient, topicName: String, qos: MqttQoS) {
        val request = JsonObject()
            .put(Const.COMMAND_KEY, COMMAND_SUBSCRIBE)
            .put(Const.TOPIC_KEY, topicName)
            .put(Const.CLIENT_KEY, client.clientId)
            .put(Const.QOS_KEY, qos.value())
        vertx.eventBus().request<Boolean>(commandAddress(), request)
            .onFailure { error ->
                logger.severe("Subscribe request failed [${error}] [${Utils.getCurrentFunctionName()}]")
            }
    }

    private fun subscribeCommand(command: Message<JsonObject>) {
        val clientId = command.body().getString(Const.CLIENT_KEY)
        val topicName = command.body().getString(Const.TOPIC_KEY)
        val qos = MqttQoS.valueOf(command.body().getInteger(Const.QOS_KEY))

        // Defensive guard: prevent adding root wildcard subscription when disabled
        if (topicName == "#" && !Monster.allowRootWildcardSubscription()) {
            logger.warning("Client [$clientId] attempted to add root wildcard subscription '#' which is disabled by configuration. Rejecting.")
            command.reply(false)
            return
        }

        messageHandler.findRetainedMessages(topicName, 0) { message -> // TODO: max must be configurable
            logger.finest { "Publish retained message [${message.topicName}] [${Utils.getCurrentFunctionName()}]" }
            val effectiveMessage = if (qos.value() < message.qosLevel) message.cloneWithNewQoS(qos.value()) else message
            sendMessageToClient(clientId, effectiveMessage)
        }.onComplete {
            logger.finest { "Retained messages published [${it.result()}] [${Utils.getCurrentFunctionName()}]" }
            addSubscription(MqttSubscription(clientId, topicName, qos))
            command.reply(true)
        }
    }

    //----------------------------------------------------------------------------------------------------

    fun unsubscribeRequest(client: MqttClient, topicName: String) {
        val request = JsonObject()
            .put(Const.COMMAND_KEY, COMMAND_UNSUBSCRIBE)
            .put(Const.TOPIC_KEY, topicName)
            .put(Const.CLIENT_KEY, client.clientId)
        vertx.eventBus().request<Boolean>(commandAddress(), request)
            .onFailure { error ->
                logger.severe("Unsubscribe request failed [${error}] [${Utils.getCurrentFunctionName()}]")
            }
    }

    private fun unsubscribeCommand(command: Message<JsonObject>) {
        val clientId = command.body().getString(Const.CLIENT_KEY)
        val topicName = command.body().getString(Const.TOPIC_KEY)
        delSubscription(MqttSubscription(clientId, topicName, MqttQoS.FAILURE /* not needed */))
        command.reply(true)
    }

    //----------------------------------------------------------------------------------------------------

    fun publishMessage(message: BrokerMessage) {
        // Special handling for API request topics - route to ApiService on the target node
        // Fast path: quick prefix check before regex matching
        if (ApiService.isApiRequestTopic(message.topicName)) {
            val details = ApiService.extractApiRequestDetails(message.topicName)
            if (details != null) {
                val eventBusAddress = "api.service.requests.${details.targetNodeId}"
                vertx.eventBus().send(eventBusAddress, message)
                logger.fine { "Routed API request message [${message.topicName}] to node [${details.targetNodeId}]" }
            }
            // Still allow distribution to normal subscribers
        }

        // NEW: If publish bulk processing is enabled, buffer the message instead of processing immediately
        if (publishBulkProcessingEnabled) {
            try {
                // ArrayBlockingQueue.add() is thread-safe, no lock needed
                publishBulkBuffer.messages.add(message)

                // Only lock when checking if we need to flush (to protect lastFlushTime and prevent double-flush)
                if (publishBulkBuffer.messages.size >= publishBulkSize) {
                    publishBulkBuffer.lock.lock()
                    try {
                        // Double-check after acquiring lock (prevent concurrent flushes)
                        if (publishBulkBuffer.messages.size >= publishBulkSize) {
                            flushPublishBatch()
                        }
                    } finally {
                        publishBulkBuffer.lock.unlock()
                    }
                }
            } catch (e: IllegalStateException) {
                logger.warning("Publish bulk buffer overflow, processing message immediately")
                processMessageImmediately(message)
            }
        } else {
            // Original single-message processing path
            processMessageImmediately(message)
        }

        // Still save to archive and handle Sparkplug expansion (happens for both paths)
        messageHandler.saveMessage(message)
        sparkplugHandler?.metricExpansion(message) { spbMessage ->
            logger.finest { "Publishing Sparkplug message [${spbMessage.topicName}] [${Utils.getCurrentFunctionName()}]" }
            publishMessage(spbMessage) // Recursive call for Sparkplug messages
        }
    }

    /**
     * Process a single message immediately (original publishMessage logic).
     * Used when bulk processing is disabled or as fallback.
     */
    private fun processMessageImmediately(message: BrokerMessage) {
        // Determine which nodes need this message based on topic subscriptions
        val targetNodes = getTargetNodesForTopic(message.topicName)
        val localNodeId = Monster.getClusterNodeId(vertx)

        val remoteNodes = targetNodes.filter { it != localNodeId }

        logger.finest { "Publishing message [${message.topicName}] to nodes [${targetNodes.joinToString(",")}]" }

        // Send to local clients if this node has subscriptions
        if (targetNodes.contains(localNodeId)) {
            processMessageForLocalClients(message)
        }

        // Send to remote nodes that have subscriptions (excluding local node)
        // Use publish() instead of send() for fire-and-forget delivery (non-blocking)
        // This prevents backpressure on the publisher when EventBus queues build up
        if (!bulkMessagingEnabled) {
            remoteNodes.forEach { nodeId ->
                vertx.eventBus().publish(nodeMessageAddress(nodeId), message)
                messageBusOut.incrementAndGet()
            }
        } else {
            // Bulk messaging: buffer messages for remote nodes
            remoteNodes.forEach { nodeId ->
                val buffer = nodeBulkBuffer.getOrPut(nodeId) {
                    BulkMessageBuffer(ArrayBlockingQueue(bulkMessagingBulkSize * 2))
                }

                try {
                    // ArrayBlockingQueue.add() is thread-safe, no lock needed
                    buffer.messages.add(message)

                    // Only lock when checking if we need to flush
                    if (buffer.messages.size >= bulkMessagingBulkSize) {
                        buffer.lock.lock()
                        try {
                            // Double-check after acquiring lock
                            if (buffer.messages.size >= bulkMessagingBulkSize) {
                                val bulkMessage = BulkNodeMessage(buffer.messages.toList())
                                buffer.messages.clear()
                                buffer.lastFlushTime = System.currentTimeMillis()
                                vertx.eventBus().publish(nodeMessageAddress(nodeId), bulkMessage)
                                messageBusOut.incrementAndGet()
                                logger.finest { "Flushed bulk to node [$nodeId]: ${bulkMessage.messages.size} messages (size threshold)" }
                            }
                        } finally {
                            buffer.lock.unlock()
                        }
                    }
                } catch (e: IllegalStateException) {
                    logger.warning("Bulk buffer overflow for node [$nodeId], sending immediately")
                    vertx.eventBus().publish(nodeMessageAddress(nodeId), message)
                    messageBusOut.incrementAndGet()
                }
            }
        }
    }

    /**
     * Flush the publish bulk batch to worker processing.
     * Sends batch to worker pool for parallel processing.
     *
     * NOTE: Locking is only used to protect the lastFlushTime field and prevent
     * concurrent flushes. The ArrayBlockingQueue itself is thread-safe.
     */
    private fun flushPublishBatch() {
        // Quick check without lock (acceptable if we miss a flush, timer will catch it)
        if (publishBulkBuffer.messages.isEmpty()) return

        publishBulkBuffer.lock.lock()
        try {
            // Re-check after acquiring lock to ensure we still have messages
            if (publishBulkBuffer.messages.isEmpty()) return

            val batch = PublishBatch(publishBulkBuffer.messages.toList())
            publishBulkBuffer.messages.clear()
            publishBulkBuffer.lastFlushTime = System.currentTimeMillis()

            logger.finest { "Flushed publish batch: ${batch.messages.size} messages" }

            // Send to worker pool for parallel processing (outside lock to avoid contention)
            if (publishWorkerPool != null) {
                publishWorkerPool!!.sendBatch(batch)
            } else {
                // Fallback: process directly if pool not initialized
                logger.warning("Worker pool not initialized, processing batch directly")
                processBatchDirectly(batch)
            }
        } finally {
            publishBulkBuffer.lock.unlock()
        }
    }

    /**
     * Process a batch of messages directly (for Phase 1).
     * Phase 2 will move this logic to worker threads.
     * This function groups messages by topic to minimize subscription lookups.
     */
    private fun processBatchDirectly(batch: PublishBatch) {
        val startTime = System.currentTimeMillis()

        // Step 1: Group messages by topic
        val messagesByTopic = batch.messages.groupBy { it.topicName }
        logger.finest { "Processing batch: ${batch.messages.size} messages, ${messagesByTopic.size} unique topics" }

        // Step 2: For each topic, process all messages together
        messagesByTopic.forEach { (topicName, messages) ->
            processTopic(topicName, messages)
        }

        val duration = System.currentTimeMillis() - startTime
        logger.finest { "Batch processed in ${duration}ms (${batch.messages.size} messages)" }
    }

    /**
     * Process all messages for a single topic.
     * Performs subscription lookup once per topic instead of per message.
     * Made internal so PublishWorker can call it.
     */
    internal fun processTopic(topicName: String, messages: List<BrokerMessage>) {
        // Single subscription lookup for the entire topic - THE KEY OPTIMIZATION
        val subscribers = findClients(topicName)

        if (subscribers.isEmpty()) {
            logger.finest { "No subscribers for topic [$topicName]" }
            return
        }

        logger.finest { "Found ${subscribers.size} subscribers for topic [$topicName]" }

        // Group by QoS and process
        messages.groupBy { it.qosLevel }.forEach { (msgQos, msgsWithQos) ->
            val (onlineClients, otherClients) = subscribers.partition { (clientId, _) ->
                clientStatus[clientId] == ClientStatus.ONLINE
            }

            // Send to online clients
            forwardBulkToOnlineClients(msgsWithQos, onlineClients, msgQos)

            // Handle created and offline clients
            handleCreatedAndOfflineClients(msgsWithQos, otherClients, msgQos)
        }

        // Handle remote nodes
        if (Monster.isClustered()) {
            val targetNodes = getTargetNodesForTopic(topicName)
            val localNodeId = Monster.getClusterNodeId(vertx)
            val remoteNodes = targetNodes.filter { it != localNodeId }

            remoteNodes.forEach { nodeId ->
                forwardToRemoteNode(messages, nodeId)
            }
        }
    }

    /**
     * Forward bulk of messages to online clients using existing bulk messaging system.
     */
    private fun forwardBulkToOnlineClients(
        messages: List<BrokerMessage>,
        clients: List<Pair<String, Int>>,
        qos: Int
    ) {
        clients.forEach { (clientId, subscriptionQos) ->
            messages.forEach { msg ->
                val effectiveQos = if (subscriptionQos < msg.qosLevel) subscriptionQos else msg.qosLevel
                val messageToSend = if (effectiveQos < msg.qosLevel) {
                    msg.cloneWithNewQoS(effectiveQos)
                } else {
                    msg
                }
                // Use existing bulk messaging system
                sendMessageToClient(clientId, messageToSend)
            }
        }
    }

    /**
     * Handle messages for created and offline clients.
     */
    private fun handleCreatedAndOfflineClients(
        messages: List<BrokerMessage>,
        clients: List<Pair<String, Int>>,
        qos: Int
    ) {
        val (createdClients, offlineClients) = clients.partition { (clientId, _) ->
            clientStatus[clientId] == ClientStatus.CREATED
        }

        // Created clients: queue in-flight
        createdClients.forEach { (clientId, _) ->
            messages.forEach { msg ->
                addInFlightMessage(clientId, msg)
            }
        }

        // Offline clients: queue for persistence
        if (offlineClients.isNotEmpty()) {
            messages.forEach { msg ->
                enqueueMessage(msg, offlineClients.map { it.first })
            }
        }
    }

    /**
     * Forward messages to remote node.
     */
    private fun forwardToRemoteNode(messages: List<BrokerMessage>, nodeId: String) {
        if (!bulkMessagingEnabled) {
            messages.forEach { msg ->
                vertx.eventBus().publish(nodeMessageAddress(nodeId), msg)
                messageBusOut.incrementAndGet()
            }
        } else {
            // Use bulk messaging for remote nodes
            // Get or create buffer once, then add all messages
            val buffer = nodeBulkBuffer.getOrPut(nodeId) {
                BulkMessageBuffer(ArrayBlockingQueue(bulkMessagingBulkSize * 2))
            }

            try {
                // Add all messages to buffer (ArrayBlockingQueue.add() is thread-safe)
                messages.forEach { msg ->
                    buffer.messages.add(msg)
                }

                // Only lock when checking if we need to flush
                if (buffer.messages.size >= bulkMessagingBulkSize) {
                    buffer.lock.lock()
                    try {
                        // Double-check after acquiring lock
                        if (buffer.messages.size >= bulkMessagingBulkSize) {
                            val bulkMessage = BulkNodeMessage(buffer.messages.toList())
                            buffer.messages.clear()
                            buffer.lastFlushTime = System.currentTimeMillis()
                            vertx.eventBus().publish(nodeMessageAddress(nodeId), bulkMessage)
                            messageBusOut.incrementAndGet()
                            logger.finest { "Flushed bulk to node [$nodeId]: ${bulkMessage.messages.size} messages" }
                        }
                    } finally {
                        buffer.lock.unlock()
                    }
                }
            } catch (e: IllegalStateException) {
                logger.warning("Bulk buffer overflow for node [$nodeId], sending messages individually")
                messages.forEach { msg ->
                    vertx.eventBus().publish(nodeMessageAddress(nodeId), msg)
                    messageBusOut.incrementAndGet()
                }
            }
        }
    }

    /**
     * Publish a message internally (for OPC UA Server writes back to MQTT)
     * @param clientId Internal client identifier (for sender tracking)
     * @param message The MQTT message to publish
     */
    fun publishInternal(clientId: String, message: BrokerMessage) {
        logger.finest("Internal publish: Client '$clientId' publishing to '${message.topicName}'")

        // Create message with sender identification for loop prevention
        val messageWithSender = BrokerMessage(
            messageUuid = message.messageUuid,
            messageId = message.messageId,
            topicName = message.topicName,
            payload = message.payload,
            qosLevel = message.qosLevel,
            isRetain = message.isRetain,
            isDup = message.isDup,
            isQueued = message.isQueued,
            clientId = clientId,
            time = message.time,
            senderId = clientId  // Mark sender for loop prevention
        )

        // Route through normal message handling
        publishMessage(messageWithSender)
    }

    private fun messageWithQos(message: BrokerMessage, qos: Int): BrokerMessage {
        return if (qos < message.qosLevel) {
            message.cloneWithNewQoS(qos)
        } else {
            message
        }
    }

    // REMOVED: consumeMessageFromBus - no longer needed
    // External messages from Kafka now go through publishMessage() for targeted distribution

    // Process messages for local clients only (used for both external and targeted internal messages)
    private fun processMessageForLocalClients(message: BrokerMessage) {
        val localNodeId = Monster.getClusterNodeId(vertx)

        // Invoke external message listeners (e.g., GraphQL subscriptions)
        messageListeners.values.forEach { (topicFilters, callback) ->
            if (topicFilters.any { filter -> matchesTopicFilter(message.topicName, filter) }) {
                try {
                    callback(message)
                } catch (e: Exception) {
                    logger.warning("Error invoking message listener: ${e.message}")
                }
            }
        }

        findClients(message.topicName).groupBy { (clientId, subscriptionQos) ->
            if (subscriptionQos < message.qosLevel) subscriptionQos else message.qosLevel // Potentially downgrade QoS
        }.forEach { (qos, clients) ->
            val m = messageWithQos(message, qos) // Potentially downgrade QoS

            // Group clients by their node - only process clients on this node
            val localClients = clients.filter { (clientId, _) ->
                val clientNodeId = clientNodeMapping.get(clientId)
                clientNodeId == null || clientNodeId == localNodeId // null means client not yet mapped or local
            }

            logger.finest { "Processing [${localClients.size}] local clients out of [${clients.size}] total clients [${Utils.getCurrentFunctionName()}]" }

            when (qos) {
                0 -> {
                    // QoS 0: Batch deliver with event loop yields to prevent blocking
                    processClientBatchAsync(localClients, m) { clientId, msg ->
                        sendMessageToClient(clientId, msg)
                    }
                }
                1, 2 -> {
                    val (online, others) = localClients.partition { (clientId, _) ->
                        clientStatus[clientId] == ClientStatus.ONLINE
                    }
                    logger.finest { "Online [${online.size}] Other [${others.size}] [${Utils.getCurrentFunctionName()}]" }

                    // Online clients: batch async delivery with event loop yields
                    processClientBatchAsync(online, m) { clientId, msg ->
                        sendMessageToClient(clientId, msg).onComplete {
                            if (it.failed() || !it.result()) {
                                logger.warning("Message sent to online client failed [${clientId}]")
                                enqueueMessage(msg, listOf(clientId))
                            }
                        }
                    }

                    if (others.isNotEmpty()) {
                        val (created, offline) = others.partition { (clientId, _) ->
                            clientStatus[clientId] == ClientStatus.CREATED
                        }
                        logger.finest { "Created [${created.size}] Offline [${offline.size}] [${Utils.getCurrentFunctionName()}]" }
                        created.forEach { (clientId, _) ->
                            addInFlightMessage(clientId, m)
                        }
                        if (offline.isNotEmpty()) {
                            enqueueMessage(m, offline.map { it.first })
                        }
                    }
                }
            }
        }

        // Also deliver to internal clients (OPC UA Server, etc.)
        deliverToInternalClients(message)
    }

    /**
     * Process client batch delivery asynchronously.
     * Batches clients into chunks and yields control to event loop between batches
     * to prevent blocking when delivering to many clients (1000+).
     *
     * @param clients List of (clientId, subscriptionQos) pairs
     * @param message The message to send
     * @param sendFn Function to send message to client: (clientId, message) -> Unit
     */
    private fun processClientBatchAsync(
        clients: List<Pair<String, Int>>,
        message: BrokerMessage,
        sendFn: (String, BrokerMessage) -> Unit
    ) {
        val BATCH_SIZE = 100  // Process 100 clients per event loop tick

        if (clients.size <= BATCH_SIZE) {
            // Small number of clients: process synchronously
            clients.forEach { (clientId, _) ->
                sendFn(clientId, message)
            }
            return
        }

        // Large number of clients: batch with event loop yields
        val batches = clients.chunked(BATCH_SIZE)
        var batchIndex = 0

        fun processBatch() {
            if (batchIndex >= batches.size) return

            val currentBatch = batches[batchIndex]
            currentBatch.forEach { (clientId, _) ->
                sendFn(clientId, message)
            }

            batchIndex++
            if (batchIndex < batches.size) {
                // Yield to event loop to allow other messages to be processed
                vertx.runOnContext { processBatch() }
            }
        }

        processBatch()
    }

    // Helper function to determine target nodes for a topic
    private fun getTargetNodesForTopic(topicName: String): Set<String> {
        val targetNodes = mutableSetOf<String>()

        // Check all topic filters to see which nodes have matching subscriptions
        topicNodeMapping.keys().forEach { topicFilter ->
            if (matchesTopicFilter(topicName, topicFilter)) {
                topicNodeMapping.getSet(topicFilter)?.let { nodes ->
                    targetNodes.addAll(nodes)
                }
            }
        }

        return targetNodes
    }

    // Simple topic filter matching using unified matcher
    private fun matchesTopicFilter(topicName: String, topicFilter: String): Boolean =
        TopicTree.matches(topicFilter, topicName)

    // --------------------------------------------------------------------------------------------------------
    // Internal subscription methods for OPC UA Server and other internal components
    // --------------------------------------------------------------------------------------------------------

    // Internal message handlers for each internal client
    private val internalSubscriptions = ConcurrentHashMap<String, ConcurrentHashMap<String, (BrokerMessage) -> Unit>>()

    /**
     * Subscribe internally to MQTT topics (for OPC UA Server, etc.)
     * @param clientId Internal client identifier
     * @param topicFilter MQTT topic filter (with wildcards)
     * @param qos QoS level (0, 1, or 2)
     * @param messageHandler Function to handle incoming messages
     */
    fun subscribeInternal(
        clientId: String,
        topicFilter: String,
        qos: Int,
        messageHandler: (BrokerMessage) -> Unit
    ) {
        logger.info("Internal subscription: Client '$clientId' subscribing to '$topicFilter' with QoS $qos")

        // Add to internal subscriptions
        internalSubscriptions.getOrPut(clientId) { ConcurrentHashMap() }[topicFilter] = messageHandler

        // Add to subscription manager (routes to exact or wildcard index)
        subscriptionManager.subscribe(clientId, topicFilter, qos)

        // Update topic-node mapping for cluster awareness
        val localNodeId = Monster.getClusterNodeId(vertx)
        topicNodeMapping.addToSet(topicFilter, localNodeId)

        logger.info("Internal client '$clientId' subscribed to '$topicFilter'")
    }

    /**
     * Unsubscribe internally from MQTT topics
     * @param clientId Internal client identifier
     * @param topicFilter MQTT topic filter to unsubscribe from
     */
    fun unsubscribeInternal(clientId: String, topicFilter: String) {
        logger.info("Internal unsubscription: Client '$clientId' unsubscribing from '$topicFilter'")

        // Remove from internal subscriptions
        internalSubscriptions[clientId]?.remove(topicFilter)
        if (internalSubscriptions[clientId]?.isEmpty() == true) {
            internalSubscriptions.remove(clientId)
        }

        // Remove from subscription manager
        subscriptionManager.unsubscribe(clientId, topicFilter)

        // Update topic-node mapping
        val localNodeId = Monster.getClusterNodeId(vertx)
        val hasOtherSubscriptions = subscriptionManager.findAllSubscribers(topicFilter).isNotEmpty()
        if (!hasOtherSubscriptions) {
            topicNodeMapping.removeFromSet(topicFilter, localNodeId)
        }

        logger.info("Internal client '$clientId' unsubscribed from '$topicFilter'")
    }

    /**
     * Check if message should be delivered to internal clients
     */
    private fun deliverToInternalClients(message: BrokerMessage) {
        internalSubscriptions.forEach { (clientId, subscriptions) ->
            subscriptions.forEach { (topicFilter, handler) ->
                if (matchesTopicFilter(message.topicName, topicFilter)) {
                    try {
                        // Skip delivery if message came from this internal client (loop prevention)
                        if (message.clientId != clientId) {
                            handler(message)
                        }
                    } catch (e: Exception) {
                        logger.warning("Error delivering message to internal client '$clientId': ${e.message}")
                    }
                }
            }
        }
    }

    /**
     * Publish bulk messaging metrics to MQTT $SYS topic.
     * Shows per-second rates for messages flushed to clients and nodes.
     */
    private fun publishBulkMessagingMetrics() {
        val nodeName = Monster.getClusterNodeId(vertx)
        val currentTime = System.currentTimeMillis()
        val timeDeltaMs = currentTime - lastBulkMessagingMetricsTime
        val timeDeltaSec = if (timeDeltaMs > 0) timeDeltaMs / 1000.0 else 1.0

        val currentClientsFlushed = bulkMessagingClientsFlushed.get()
        val currentNodesFlushed = bulkMessagingNodesFlushed.get()

        val clientsDelta = currentClientsFlushed - lastBulkMessagingClientsFlushed
        val nodesDelta = currentNodesFlushed - lastBulkMessagingNodesFlushed

        val clientsPerSec = (clientsDelta / timeDeltaSec).toLong()
        val nodesPerSec = (nodesDelta / timeDeltaSec).toLong()

        lastBulkMessagingMetricsTime = currentTime
        lastBulkMessagingClientsFlushed = currentClientsFlushed
        lastBulkMessagingNodesFlushed = currentNodesFlushed

        val clientBuffersCount = clientBulkBuffer.size
        val nodeBuffersCount = nodeBulkBuffer.size
        var totalClientMessages = 0L
        var totalNodeMessages = 0L

        clientBulkBuffer.values.forEach { buffer ->
            totalClientMessages += buffer.messages.size
        }
        nodeBulkBuffer.values.forEach { buffer ->
            totalNodeMessages += buffer.messages.size
        }

        val metricsJson = buildString {
            append("{")
            append("\"clientMessagesPerSec\":$clientsPerSec,")
            append("\"nodeMessagesPerSec\":$nodesPerSec,")
            append("\"clientBuffers\":$clientBuffersCount,")
            append("\"nodeBuffers\":$nodeBuffersCount,")
            append("\"bufferedClientMessages\":$totalClientMessages,")
            append("\"bufferedNodeMessages\":$totalNodeMessages")
            append("}")
        }

        try {
            val message = BrokerMessage("", "\$SYS/brokers/$nodeName/bulk/messaging", metricsJson)
            publishMessage(message)
        } catch (e: Exception) {
            logger.fine("Error publishing bulk messaging metrics: ${e.message}")
        }
    }

    /**
     * Shutdown the session handler and worker pool.
     * Called when the verticle stops.
     */
    fun shutdown() {
        logger.info("Shutting down SessionHandler...")
        val startTime = System.currentTimeMillis()

        try {
            // Cancel periodic timers
            if (bulkFlushTimerId >= 0) {
                vertx.cancelTimer(bulkFlushTimerId)
                logger.fine("Cancelled bulk flush timer")
            }
            if (bulkMessagingMetricsTimerId >= 0) {
                vertx.cancelTimer(bulkMessagingMetricsTimerId)
                logger.fine("Cancelled bulk messaging metrics timer")
            }
            if (publishBatchFlushTimerId >= 0) {
                vertx.cancelTimer(publishBatchFlushTimerId)
                logger.fine("Cancelled publish batch flush timer")
            }
            if (publishWorkerLogTimerId >= 0) {
                vertx.cancelTimer(publishWorkerLogTimerId)
                logger.fine("Cancelled worker log timer")
            }

            // Final worker load log before shutdown
            if (publishWorkerPool != null) {
                logger.info("Final worker load before shutdown:")
                publishWorkerPool?.logWorkerLoad()
            }

            // Shutdown worker pool
            if (publishWorkerPool != null) {
                logger.info("Shutting down worker pool...")
                publishWorkerPool?.shutdown()
            }

            val duration = System.currentTimeMillis() - startTime
            logger.info("SessionHandler shutdown complete (${duration}ms)")
        } catch (e: Exception) {
            logger.severe("Error during SessionHandler shutdown: ${e.message}")
            e.printStackTrace()
        }
    }
}