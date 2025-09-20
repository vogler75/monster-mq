package at.rocworks.handlers

import at.rocworks.Const
import at.rocworks.bus.EventBusAddresses
import at.rocworks.Monster
import at.rocworks.MqttClient
import at.rocworks.Utils
import at.rocworks.bus.IMessageBus
import at.rocworks.cluster.DataReplicator
import at.rocworks.cluster.SetMapReplicator
import at.rocworks.data.*
import at.rocworks.stores.ISessionStoreAsync
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

    private val topicIndex = TopicTree<String, Int>() // Topic index with client and QoS
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

    private val subAddQueue: ArrayBlockingQueue<MqttSubscription> = ArrayBlockingQueue(10_000) // TODO: configurable
    private val subDelQueue: ArrayBlockingQueue<MqttSubscription> = ArrayBlockingQueue(10_000) // TODO: configurable

    private val msgAddQueue: ArrayBlockingQueue<Pair<MqttMessage, List<String>>> = ArrayBlockingQueue(10_000) // TODO: configurable
    private val msgDelQueue: ArrayBlockingQueue<Pair<String, String>> = ArrayBlockingQueue(10_000) // TODO: configurable
    private var waitForFlush: Promise<Void>? = null

    // Use unified EventBus addresses
    private val subscriptionAddAddress = EventBusAddresses.Cluster.SUBSCRIPTION_ADD
    private val subscriptionDelAddress = EventBusAddresses.Cluster.SUBSCRIPTION_DELETE
    private val clientStatusAddress = EventBusAddresses.Cluster.CLIENT_STATUS
    private val clientMappingAddress = EventBusAddresses.Cluster.CLIENT_NODE_MAPPING
    private val topicMappingAddress = EventBusAddresses.Cluster.TOPIC_NODE_MAPPING
    private fun nodeMessageAddress(nodeId: String) = EventBusAddresses.Node.messages(nodeId)
    private fun localNodeMessageAddress() = nodeMessageAddress(Monster.getClusterNodeId(vertx))

    private val sparkplugHandler = Monster.getSparkplugExtension()

    private val inFlightMessages = HashMap<String, ArrayBlockingQueue<MqttMessage>>()

    private fun commandAddress() = EventBusAddresses.Node.commands(deploymentID())
    private fun metricsAddress() = EventBusAddresses.Node.metrics(Monster.getClusterNodeId(vertx))
    // REMOVED: messageAddress() - no longer using broadcast message bus

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    companion object {
        const val COMMAND_KEY = "C"
        const val COMMAND_SUBSCRIBE = "S"
        const val COMMAND_UNSUBSCRIBE = "U"
    }

    enum class ClientStatus {
        UNKNOWN, CREATED, ONLINE, PAUSED, DELETE
    }

    data class SessionMetrics(
        val messagesIn: AtomicLong = AtomicLong(0),
        val messagesOut: AtomicLong = AtomicLong(0)
    )

    data class ClientDetails(
        val nodeId: String,
        val clientAddress: String?,
        val cleanSession: Boolean,
        val sessionExpiryInterval: Int?
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
            topicIndex.add(subscription.topicName, subscription.clientId, subscription.qos.value())

            // Track topic subscriptions by node for targeted publishing
            val nodeId = clientNodeMapping.get(subscription.clientId) ?: Monster.getClusterNodeId(vertx)
            topicNodeMapping.addToSet(subscription.topicName, nodeId)
            logger.finest { "Added topic subscription [${subscription.topicName}] for node [${nodeId}]" }
        }

        vertx.eventBus().consumer<MqttSubscription>(subscriptionDelAddress) { message ->
            val subscription = message.body()
            topicIndex.del(subscription.topicName, subscription.clientId)

            // Clean up topic-node mapping if no more clients on this node for this topic
            val nodeId = clientNodeMapping.get(subscription.clientId) ?: Monster.getClusterNodeId(vertx)
            val remainingClientsOnNode = topicIndex.findDataOfTopicName(subscription.topicName)
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
            logger.fine("Client status [${message.body()}]")
            val clientId = message.body().getString("ClientId", "")
            val status = ClientStatus.valueOf(message.body().getString("Status", ""))
            val deliveryOptions = DeliveryOptions(JsonObject().put("NodeId", Monster.getClusterNodeId(vertx)))

            fun flushFinished() {
                logger.fine("Flushed messages finished")
                message.reply(true, deliveryOptions)
            }

            when (status) {
                ClientStatus.CREATED -> {
                    clientStatus[clientId] = ClientStatus.CREATED
                    waitForFlush?.let { promise ->
                        logger.fine("Existing wait for flush...")
                        promise.future().onComplete { flushFinished() }
                    } ?: Promise.promise<Void>().let { promise ->
                        logger.fine("New wait for flush...")
                        waitForFlush = promise
                        promise.future().onComplete { flushFinished() }
                    }
                }
                ClientStatus.ONLINE -> {
                    inFlightMessages[clientId]?.let { messages ->
                        logger.fine("Publishing [${messages.count()}] in-flight messages to client [${clientId}] [${Utils.getCurrentFunctionName()}]")
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
                when (payload.getString(COMMAND_KEY)) {
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

            metrics.put("messagesIn", totalMessagesIn)
                   .put("messagesOut", totalMessagesOut)
                   .put("nodeSessionCount", clientMetrics.size)
                   .put("messageBusIn", messageBusIn.get())
                   .put("messageBusOut", messageBusOut.get())
                   .put("topicIndexSize", topicIndex.size())
                   .put("clientNodeMappingSize", clientNodeMapping.size())
                   .put("topicNodeMappingSize", topicNodeMapping.size())

            message.reply(metrics)
        }

        // Metrics and reset handler - returns current values and resets counters to 0
        vertx.eventBus().consumer<JsonObject>(EventBusAddresses.Node.metricsAndReset(Monster.getClusterNodeId(vertx))) { message ->
            val metrics = JsonObject()
            var totalMessagesIn = 0L
            var totalMessagesOut = 0L
            val sessionMetricsArray = io.vertx.core.json.JsonArray()

            // Get and reset session metrics
            clientMetrics.forEach { (clientId, sessionMetrics) ->
                val inCount = sessionMetrics.messagesIn.getAndSet(0)
                val outCount = sessionMetrics.messagesOut.getAndSet(0)
                totalMessagesIn += inCount
                totalMessagesOut += outCount

                // Add individual client metrics to the response
                sessionMetricsArray.add(JsonObject()
                    .put("clientId", clientId)
                    .put("messagesIn", inCount)
                    .put("messagesOut", outCount)
                )
            }

            metrics.put("messagesIn", totalMessagesIn)
                   .put("messagesOut", totalMessagesOut)
                   .put("nodeSessionCount", clientMetrics.size)
                   .put("messageBusIn", messageBusIn.getAndSet(0))
                   .put("messageBusOut", messageBusOut.getAndSet(0))
                   .put("topicIndexSize", topicIndex.size())
                   .put("clientNodeMappingSize", clientNodeMapping.size())
                   .put("topicNodeMappingSize", topicNodeMapping.size())
                   .put("sessionMetrics", sessionMetricsArray)
            message.reply(metrics)
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
        vertx.eventBus().consumer<MqttMessage>(localNodeMessageAddress()) { message ->
            message.body()?.let { payload ->
                messageBusIn.incrementAndGet()
                logger.finest { "Received targeted message [${payload.topicName}] [${Utils.getCurrentFunctionName()}]" }
                processMessageForLocalClients(payload)
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
            // Add to topic index
            topicIndex.add(topicName, clientId, qos)

            // Build topic-to-node mapping based on where client is located
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
        logger.fine("Start [$name] loop")
        val block = arrayListOf<T>()
        var lastCheckTime = System.currentTimeMillis()

        fun flushed() {
            if (name == "MsgAddQueue" && waitForFlush != null) {
                waitForFlush?.complete()
                waitForFlush = null
            }
        }

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
                        flushed()
                        vertx.runOnContext { loop() }
                    }
                } else {
                    flushed()
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
        return clientMetrics.mapValues { (_, sessionMetrics) ->
            at.rocworks.extensions.graphql.SessionMetrics(
                messagesIn = sessionMetrics.messagesIn.getAndSet(0),
                messagesOut = sessionMetrics.messagesOut.getAndSet(0),
                timestamp = at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
            )
        }
    }

    fun getClientDetails(clientId: String): ClientDetails? = clientDetails[clientId]

    fun getSessionCount(): Int = clientMetrics.size

    fun getTopicIndexSize(): Int = topicIndex.size()

    fun getClientNodeMappingSize(): Int = clientNodeMapping.size()

    fun getTopicNodeMappingSize(): Int = topicNodeMapping.size()

    fun getMessageBusOutCount(): Long = messageBusOut.get()

    fun getMessageBusInCount(): Long = messageBusIn.get()

    fun setClient(clientId: String, cleanSession: Boolean, information: JsonObject): Future<Void> {
        logger.fine("Set client [$clientId] clean session [$cleanSession] information [$information]")

        // Initialize metrics and client details
        clientMetrics[clientId] = SessionMetrics()
        val nodeId = Monster.getClusterNodeId(vertx)
        clientDetails[clientId] = ClientDetails(
            nodeId = nodeId,
            clientAddress = information.getString("clientAddress"),
            cleanSession = cleanSession,
            sessionExpiryInterval = information.getInteger("sessionExpiryInterval")
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
            val message = MqttMessage(clientId, will)
            return sessionStore.setLastWill(clientId, message)
        } else {
            return sessionStore.setLastWill(clientId, null)
        }
    }

    fun isPresent(clientId: String): Future<Boolean> = sessionStore.isPresent(clientId)

    private fun enqueueMessage(message: MqttMessage, clientIds: List<String>) {
        if (enqueueMessages) msgAddQueue.add(Pair(message, clientIds))
    }

    fun dequeueMessages(clientId: String, callback: (MqttMessage)->Boolean) = sessionStore.dequeueMessages(clientId, callback)

    fun removeMessage(clientId: String, messageUuid: String) {
        msgDelQueue.add(Pair(clientId, messageUuid))
    }

    private fun addSubscription(subscription: MqttSubscription) {
        vertx.eventBus().publish(subscriptionAddAddress, subscription)
        try {
            subAddQueue.add(subscription)
        } catch (e: IllegalStateException) {
            // TODO: Alert
        }
    }

    private fun delSubscription(subscription: MqttSubscription) {
        vertx.eventBus().publish(subscriptionDelAddress, subscription)
        try {
            subDelQueue.add(subscription)
        } catch (e: IllegalStateException) {
            // TODO: Alert
        }
    }

    private fun findClients(topicName: String): Set<Pair<String, Int>> {
        val result = topicIndex.findDataOfTopicName(topicName).toSet()
        logger.finest { "Found [${result.size}] clients [${result.joinToString(",")}] [${Utils.getCurrentFunctionName()}]" }
        return result
    }

    fun purgeSessions() = sessionStore.purgeSessions()

    fun purgeQueuedMessages() = sessionStore.purgeQueuedMessages()

    fun iterateNodeClients(nodeId: String, callback: (clientId: String, cleanSession: Boolean, lastWill: MqttMessage) -> Unit)
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


    private fun sendMessageToClient(clientId: String, message: MqttMessage): Future<Boolean> {
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
    }

    private fun addInFlightMessage(clientId: String, message: MqttMessage) {
        logger.fine { "Adding in-flight message to queue [${message.topicName}] [${message.getPayloadAsJson()}]}" }
        inFlightMessages[clientId]?.let { queue ->
            if (queue.remainingCapacity() == 0) {
                logger.warning("In-flight messages queue full [${Utils.getCurrentFunctionName()}]")
            } else {
                queue.put(message)
            }
        } ?: run {
            ArrayBlockingQueue<MqttMessage>(10_000).let { // TODO: configurable
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
            .put(COMMAND_KEY, COMMAND_SUBSCRIBE)
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

        messageHandler.findRetainedMessages(topicName, 0) { message -> // TODO: max must be configurable
            logger.finest { "Publish retained message [${message.topicName}] [${Utils.getCurrentFunctionName()}]" }
            sendMessageToClient(clientId, message)
        }.onComplete {
            logger.finest { "Retained messages published [${it.result()}] [${Utils.getCurrentFunctionName()}]" }
            addSubscription(MqttSubscription(clientId, topicName, qos))
            command.reply(true)
        }
    }

    //----------------------------------------------------------------------------------------------------

    fun unsubscribeRequest(client: MqttClient, topicName: String) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_UNSUBSCRIBE)
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

    fun publishMessage(message: MqttMessage) {
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
        remoteNodes.forEach { nodeId ->
            vertx.eventBus().send(nodeMessageAddress(nodeId), message)
            messageBusOut.incrementAndGet()
        }

        // Note: Offline persistent client queuing is now handled automatically by
        // node failure detection in HealthHandler when nodes die

        // Still save to archive and handle Sparkplug expansion
        messageHandler.saveMessage(message)
        sparkplugHandler?.metricExpansion(message) { spbMessage ->
            logger.finest { "Publishing Sparkplug message [${spbMessage.topicName}] [${Utils.getCurrentFunctionName()}]" }
            publishMessage(spbMessage) // Recursive call for Sparkplug messages
        }
    }

    private fun messageWithQos(message: MqttMessage, qos: Int): MqttMessage {
        return if (qos < message.qosLevel) {
            message.cloneWithNewQoS(qos)
        } else {
            message
        }
    }

    // REMOVED: consumeMessageFromBus - no longer needed
    // External messages from Kafka now go through publishMessage() for targeted distribution

    // Process messages for local clients only (used for both external and targeted internal messages)
    private fun processMessageForLocalClients(message: MqttMessage) {
        val localNodeId = Monster.getClusterNodeId(vertx)

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
                0 -> localClients.forEach { (clientId, _) ->
                    sendMessageToClient(clientId, m)
                }
                1, 2 -> {
                    val (online, others) = localClients.partition { (clientId, _) ->
                        clientStatus[clientId] == ClientStatus.ONLINE
                    }
                    logger.finest { "Online [${online.size}] Other [${others.size}] [${Utils.getCurrentFunctionName()}]" }
                    online.forEach { (clientId, _) ->
                        sendMessageToClient(clientId, m).onComplete {
                            if (it.failed() || !it.result()) {
                                logger.warning("Message sent to online client failed [${clientId}]")
                                enqueueMessage(m, listOf(clientId))
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

    // Simple topic filter matching (supports + and # wildcards)
    private fun matchesTopicFilter(topicName: String, topicFilter: String): Boolean {
        if (topicFilter == topicName) return true
        if (topicFilter.contains('#') || topicFilter.contains('+')) {
            // Create a temporary topic tree with the filter to test matching
            val tempTree = TopicTree<String, String>()
            tempTree.add(topicFilter, "test", "value")
            return tempTree.isTopicNameMatching(topicName)
        }
        return false
    }

    // --------------------------------------------------------------------------------------------------------
    // Internal subscription methods for OPC UA Server and other internal components
    // --------------------------------------------------------------------------------------------------------

    // Internal message handlers for each internal client
    private val internalSubscriptions = ConcurrentHashMap<String, ConcurrentHashMap<String, (MqttMessage) -> Unit>>()

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
        messageHandler: (MqttMessage) -> Unit
    ) {
        logger.info("Internal subscription: Client '$clientId' subscribing to '$topicFilter' with QoS $qos")

        // Add to internal subscriptions
        internalSubscriptions.getOrPut(clientId) { ConcurrentHashMap() }[topicFilter] = messageHandler

        // Add to topic index for message routing
        topicIndex.add(topicFilter, clientId, qos)

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

        // Remove from topic index
        topicIndex.del(topicFilter, clientId)

        // Update topic-node mapping
        val localNodeId = Monster.getClusterNodeId(vertx)
        val hasOtherSubscriptions = topicIndex.findDataOfTopicName(topicFilter).isNotEmpty()
        if (!hasOtherSubscriptions) {
            topicNodeMapping.removeFromSet(topicFilter, localNodeId)
        }

        logger.info("Internal client '$clientId' unsubscribed from '$topicFilter'")
    }

    /**
     * Publish a message internally (for OPC UA Server writes back to MQTT)
     * @param clientId Internal client identifier (for sender tracking)
     * @param message The MQTT message to publish
     */
    fun publishInternal(clientId: String, message: MqttMessage) {
        logger.finest("Internal publish: Client '$clientId' publishing to '${message.topicName}'")

        // Create message with sender identification for loop prevention
        val messageWithSender = MqttMessage(
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
            sender = clientId  // Mark sender for loop prevention
        )

        // Route through normal message handling
        publishMessage(messageWithSender)
    }

    /**
     * Check if message should be delivered to internal clients
     */
    private fun deliverToInternalClients(message: MqttMessage) {
        internalSubscriptions.forEach { (clientId, subscriptions) ->
            subscriptions.forEach { (topicFilter, handler) ->
                if (matchesTopicFilter(message.topicName, topicFilter)) {
                    try {
                        // Skip delivery if message came from this internal client (loop prevention)
                        if (message.sender != clientId) {
                            handler(message)
                        }
                    } catch (e: Exception) {
                        logger.warning("Error delivering message to internal client '$clientId': ${e.message}")
                    }
                }
            }
        }
    }
}