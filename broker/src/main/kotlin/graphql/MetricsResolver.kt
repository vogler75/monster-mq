package at.rocworks.extensions.graphql

import at.rocworks.Utils
import at.rocworks.Version
import at.rocworks.bus.EventBusAddresses
import at.rocworks.Monster
import at.rocworks.stores.ISessionStoreAsync
import at.rocworks.stores.IMessageStore
import at.rocworks.stores.IMetricsStore
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

class MetricsResolver(
    private val vertx: Vertx,
    private val sessionStore: ISessionStoreAsync,
    private val sessionHandler: at.rocworks.handlers.SessionHandler,
    private val metricsStore: IMetricsStore?
) {
    companion object {
        private val logger: Logger = Utils.getLogger(MetricsResolver::class.java)

        private fun getMetricsAddress(nodeId: String): String {
            return EventBusAddresses.Node.metrics(nodeId)
        }
    }

    private fun round2(value: Double): Double {
        // Round to 2 decimal places for rate values
        return kotlin.math.round(value * 100.0) / 100.0
    }

    fun broker(): DataFetcher<CompletableFuture<Broker?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Broker?>()
            val nodeId = env.getArgument<String?>("nodeId") ?: Monster.getClusterNodeId(vertx)

            // Query the specific node for its metrics via EventBus
            val metricsAddress = getMetricsAddress(nodeId)

            vertx.eventBus().request<JsonObject>(metricsAddress, JsonObject()).onComplete { reply ->
                if (reply.succeeded()) {
                    val nodeMetrics = reply.result().body()

                    // Get cluster-wide metrics from database
                    vertx.executeBlocking<BrokerMetrics>(java.util.concurrent.Callable {
                        try {
                            val clusterSessionCount = getClusterSessionCount()
                            val queuedMessagesCount = getQueuedMessagesCount()

                            BrokerMetrics(
                                messagesIn = nodeMetrics.getDouble("messagesInRate", 0.0),
                                messagesOut = nodeMetrics.getDouble("messagesOutRate", 0.0),
                                nodeSessionCount = nodeMetrics.getInteger("nodeSessionCount", 0),
                                clusterSessionCount = clusterSessionCount,
                                queuedMessagesCount = queuedMessagesCount,
                                topicIndexSize = nodeMetrics.getInteger("topicIndexSize", 0),
                                clientNodeMappingSize = nodeMetrics.getInteger("clientNodeMappingSize", 0),
                                topicNodeMappingSize = nodeMetrics.getInteger("topicNodeMappingSize", 0),
                                messageBusIn = nodeMetrics.getDouble("messageBusInRate", 0.0),
                                messageBusOut = nodeMetrics.getDouble("messageBusOutRate", 0.0),
                                mqttClientIn = 0.0,
                            mqttClientOut = 0.0,
                            opcUaClientIn = 0.0,
                            opcUaClientOut = 0.0,
                            kafkaClientIn = 0.0,
                            kafkaClientOut = 0.0,
                            winCCUaClientIn = 0.0,
                            timestamp = TimestampConverter.currentTimeIsoString()
                            )
                        } catch (e: Exception) {
                            logger.severe("Error getting cluster metrics: ${e.message}")
                            throw e
                        }
                    }).onComplete { result ->
                        if (result.succeeded()) {
                            future.complete(Broker(nodeId, Version.getVersion()))
                        } else {
                            future.completeExceptionally(result.cause())
                        }
                    }
                } else {
                    logger.warning("Failed to get metrics from node $nodeId: ${reply.cause().message}")
                    future.complete(null)
                }
            }

            future
        }
    }

    fun brokers(): DataFetcher<CompletableFuture<List<Broker>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Broker>>()
            val nodeIds = Monster.getClusterNodeIds(vertx)

            // Query all nodes (including single "local" node in standalone mode)
            val brokerFutures = nodeIds.map { nodeId ->
                val brokerFuture = CompletableFuture<Broker?>()

                val metricsAddress = getMetricsAddress(nodeId)
                vertx.eventBus().request<JsonObject>(metricsAddress, JsonObject()).onComplete { reply ->
                    if (reply.succeeded()) {
                        val nodeMetrics = reply.result().body()

                        vertx.executeBlocking<BrokerMetrics>(java.util.concurrent.Callable {
                            try {
                                val clusterSessionCount = getClusterSessionCount()
                                val queuedMessagesCount = getQueuedMessagesCount()

                                BrokerMetrics(
                                    messagesIn = nodeMetrics.getDouble("messagesInRate", 0.0),
                                    messagesOut = nodeMetrics.getDouble("messagesOutRate", 0.0),
                                    nodeSessionCount = nodeMetrics.getInteger("nodeSessionCount", 0),
                                    clusterSessionCount = clusterSessionCount,
                                    queuedMessagesCount = queuedMessagesCount,
                                    topicIndexSize = nodeMetrics.getInteger("topicIndexSize", 0),
                                    clientNodeMappingSize = nodeMetrics.getInteger("clientNodeMappingSize", 0),
                                    topicNodeMappingSize = nodeMetrics.getInteger("topicNodeMappingSize", 0),
                                    messageBusIn = nodeMetrics.getDouble("messageBusInRate", 0.0),
                                    messageBusOut = nodeMetrics.getDouble("messageBusOutRate", 0.0),
                                    mqttClientIn = 0.0,
                                    mqttClientOut = 0.0,
                                    opcUaClientIn = 0.0,
                                    opcUaClientOut = 0.0,
                                    kafkaClientIn = 0.0,
                                    kafkaClientOut = 0.0,
                                    timestamp = TimestampConverter.currentTimeIsoString()
                                )
                            } catch (e: Exception) {
                                logger.severe("Error getting cluster metrics: ${e.message}")
                                throw e
                            }
                        }).onComplete { result ->
                            if (result.succeeded()) {
                                brokerFuture.complete(Broker(nodeId, Version.getVersion()))
                            } else {
                                brokerFuture.complete(null)
                            }
                        }
                    } else {
                        logger.warning("Failed to get metrics from node $nodeId: ${reply.cause().message}")
                        brokerFuture.complete(null)
                    }
                }

                brokerFuture
            }

            CompletableFuture.allOf(*brokerFutures.toTypedArray()).thenApply {
                val brokers = brokerFutures.mapNotNull { it.get() }.sortedBy { it.nodeId }
                future.complete(brokers)
                brokers
            }

            future
        }
    }

    fun sessions(): DataFetcher<CompletableFuture<List<Session>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Session>>()
            val nodeId = env.getArgument<String?>("nodeId")
            val cleanSessionFilter = env.getArgument<Boolean?>("cleanSession")
            val connectedFilter = env.getArgument<Boolean?>("connected")

            // Always use getAllSessions and filter by nodeId if needed
            getAllSessions(cleanSessionFilter, connectedFilter, future, nodeId)

            future
        }
    }

    fun session(): DataFetcher<CompletableFuture<Session?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Session?>()
            val clientId = env.getArgument<String?>("clientId")

            if (clientId == null) {
                future.complete(null)
                return@DataFetcher future
            }

            // First try local SessionHandler
            val clientMetrics = sessionHandler.getClientMetrics(clientId)
            val clientDetails = sessionHandler.getClientDetails(clientId)

            if (clientMetrics != null && clientDetails != null) {
                // Found locally - use async approach for subscriptions
                getSubscriptionsForClientAsync(clientId).thenAccept { subscriptions ->
                    val session = Session(
                        clientId = clientId,
                        nodeId = clientDetails.nodeId,
                        subscriptions = subscriptions,
                        cleanSession = clientDetails.cleanSession,
                        sessionExpiryInterval = clientDetails.sessionExpiryInterval?.toLong() ?: 0L,
                        clientAddress = clientDetails.clientAddress,
                        connected = sessionHandler.getClientStatus(clientId) == at.rocworks.handlers.SessionHandler.ClientStatus.ONLINE,
                        information = clientDetails.information
                    )
                    future.complete(session)
                }.exceptionally { e ->
                    logger.warning("Error getting subscriptions for local session $clientId: ${e.message}")
                    future.completeExceptionally(e)
                    null
                }
            } else {
                // Not found locally - search all nodes via sessionStore
                searchSessionOnRemoteNodes(clientId, future)
            }

            future
        }
    }

    private fun searchSessionOnRemoteNodes(clientId: String, future: CompletableFuture<Session?>) {
        // Use sessionStore to find which node has this session
        var sessionNodeId: String? = null
        var sessionConnected = false
        var sessionCleanSession = false

        sessionStore.iterateAllSessions { iterClientId, nodeId, connected, cleanSession ->
            if (iterClientId == clientId) {
                sessionNodeId = nodeId
                sessionConnected = connected
                sessionCleanSession = cleanSession
            }
        }.onComplete { result ->
            if (result.succeeded()) {
                if (sessionNodeId != null) {
                    // Found the session on a remote node - request details via message bus
                    val sessionDetailsAddress = EventBusAddresses.Node.sessionDetails(sessionNodeId!!, "*")
                    val request = JsonObject()
                    val deliveryOptions = io.vertx.core.eventbus.DeliveryOptions().addHeader("clientId", clientId)

                    vertx.eventBus().request<JsonObject>(sessionDetailsAddress, request, deliveryOptions).onComplete { reply ->
                        if (reply.succeeded()) {
                            val response = reply.result().body()
                            val found = response.getBoolean("found", false)

                            if (found) {
                                // Get subscriptions for this session
                                getSubscriptionsForClientAsync(clientId).thenAccept { subscriptions ->
                                    val session = Session(
                                        clientId = clientId,
                                        nodeId = response.getString("nodeId"),
                                        subscriptions = subscriptions,
                                        cleanSession = response.getBoolean("cleanSession", sessionCleanSession),
                                        sessionExpiryInterval = response.getLong("sessionExpiryInterval", 0L),
                                        clientAddress = response.getString("clientAddress"),
                                        connected = response.getBoolean("connected", sessionConnected),
                                        information = response.getString("information")
                                    )
                                    future.complete(session)
                                }.exceptionally { e ->
                                    logger.warning("Error getting subscriptions for remote session $clientId: ${e.message}")
                                    future.complete(null)
                                    null
                                }
                            } else {
                                future.complete(null)
                            }
                        } else {
                            logger.warning("Failed to get session details for client $clientId from node $sessionNodeId: ${reply.cause().message}")
                            future.complete(null)
                        }
                    }
                } else {
                    // Session not found in any node
                    future.complete(null)
                }
            } else {
                logger.warning("Error iterating sessions to find client $clientId: ${result.cause()?.message}")
                future.complete(null)
            }
        }
    }


    private fun getAllSessions(cleanSessionFilter: Boolean?, connectedFilter: Boolean?, future: CompletableFuture<List<Session>>, nodeIdFilter: String? = null) {
        val sessions = mutableListOf<Session>()
        val sessionProcessingQueue = mutableListOf<CompletableFuture<Session?>>()

        // Use async database access
        sessionStore.iterateAllSessions { clientId, nodeId, connected, cleanSession ->
            try {
                // Apply filters early to avoid unnecessary processing
                val passesNodeIdFilter = nodeIdFilter?.let { it == nodeId } ?: true
                val passesCleanSessionFilter = cleanSessionFilter?.let { it == cleanSession } ?: true
                val passesConnectedFilter = connectedFilter?.let { it == connected } ?: true

                if (passesNodeIdFilter && passesCleanSessionFilter && passesConnectedFilter) {
                    // Create a future for processing this session
                    val sessionFuture = CompletableFuture<Session?>()
                    sessionProcessingQueue.add(sessionFuture)

                    // Skip metrics embedding since they're now handled by separate resolvers

                    // Get client details from local session handler if available, otherwise use defaults
                    val clientDetails = if (nodeId == Monster.getClusterNodeId(vertx)) {
                        sessionHandler.getClientDetails(clientId)
                    } else null

                    // Get subscriptions asynchronously
                    getSubscriptionsForClientAsync(clientId).thenAccept { subscriptions ->
                        try {
                            val session = Session(
                                clientId = clientId,
                                nodeId = nodeId,
                                subscriptions = subscriptions,
                                cleanSession = cleanSession,
                                sessionExpiryInterval = clientDetails?.sessionExpiryInterval?.toLong() ?: 0L,
                                clientAddress = clientDetails?.clientAddress,
                                connected = connected,
                                information = clientDetails?.information
                            )
                            sessionFuture.complete(session)
                        } catch (e: Exception) {
                            logger.warning("Error creating session $clientId: ${e.message}")
                            sessionFuture.complete(null)
                        }
                    }.exceptionally { e ->
                        logger.warning("Error getting subscriptions for session $clientId: ${e.message}")
                        sessionFuture.complete(null)
                        null
                    }
                }
            } catch (e: Exception) {
                logger.warning("Error processing session $clientId: ${e.message}")
            }
        }.onComplete { result ->
            if (result.succeeded()) {
                // Wait for all session processing to complete
                val allSessionsFuture = CompletableFuture.allOf(*sessionProcessingQueue.toTypedArray())
                allSessionsFuture.thenAccept {
                    val completedSessions = sessionProcessingQueue.mapNotNull {
                        try {
                            it.get()
                        } catch (e: Exception) {
                            logger.warning("Error getting session result: ${e.message}")
                            null
                        }
                    }
                    future.complete(completedSessions)
                }.exceptionally { e ->
                    logger.severe("Error completing session processing: ${e.message}")
                    e.printStackTrace()
                    future.completeExceptionally(e)
                    null
                }
            } else {
                logger.severe("Error iterating sessions from database: ${result.cause()?.message}")
                result.cause()?.printStackTrace()
                future.completeExceptionally(result.cause())
            }
        }
    }

    private fun getSubscriptionsForClient(clientId: String): List<MqttSubscription> {
        val subscriptions = mutableListOf<MqttSubscription>()

        try {
            // Use the session store to get subscriptions from database
            val future = java.util.concurrent.CompletableFuture<Void>()

            sessionStore.iterateSubscriptions { topic, subscriptionClientId, qos ->
                if (subscriptionClientId == clientId) {
                    subscriptions.add(MqttSubscription(topicFilter = topic, qos = qos))
                }
            }.onComplete { result ->
                if (result.succeeded()) {
                    future.complete(null)
                } else {
                    logger.warning("Error iterating subscriptions for client $clientId: ${result.cause()?.message}")
                    future.complete(null)
                }
            }

            // Wait for completion (blocking operation, but necessary for synchronous GraphQL resolver)
            future.get(5, java.util.concurrent.TimeUnit.SECONDS)
        } catch (e: Exception) {
            logger.warning("Error getting subscriptions for client $clientId: ${e.message}")
        }

        return subscriptions
    }

    private fun getSubscriptionsForClientSync(clientId: String): List<MqttSubscription> {
        val subscriptions = mutableListOf<MqttSubscription>()

        try {
            // Use the synchronous store to get subscriptions
            sessionStore.sync.iterateSubscriptions { topic, subscriptionClientId, qos ->
                if (subscriptionClientId == clientId) {
                    subscriptions.add(MqttSubscription(topicFilter = topic, qos = qos))
                }
            }
        } catch (e: Exception) {
            logger.warning("Error getting subscriptions for client $clientId: ${e.message}")
        }

        return subscriptions
    }

    private fun getSubscriptionsForClientAsync(clientId: String): CompletableFuture<List<MqttSubscription>> {
        val future = CompletableFuture<List<MqttSubscription>>()
        val subscriptions = mutableListOf<MqttSubscription>()

        sessionStore.iterateSubscriptions { topic, subscriptionClientId, qos ->
            if (subscriptionClientId == clientId) {
                subscriptions.add(MqttSubscription(topicFilter = topic, qos = qos))
            }
        }.onComplete { result ->
            if (result.succeeded()) {
                future.complete(subscriptions)
            } else {
                logger.warning("Error getting subscriptions for client $clientId: ${result.cause()?.message}")
                future.complete(emptyList()) // Return empty list on error instead of failing
            }
        }

        return future
    }

    private fun getClusterSessionCount(): Int {
        // For now, return local count from SessionHandler
        return sessionHandler.getSessionCount()
    }

    private fun getQueuedMessagesCount(): Long {
        // This method is used synchronously in broker metrics within executeBlocking blocks
        return try {
            sessionStore.sync.countQueuedMessages()
        } catch (e: Exception) {
            logger.warning("Error getting queued messages count: ${e.message}")
            0L
        }
    }

    fun sessionQueuedMessageCount(): DataFetcher<CompletableFuture<Long>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Long>()

            // Get the parent Session object from the DataFetchingEnvironment
            val session = env.getSource<Session>()
            val clientId = session?.clientId

            if (clientId != null) {
                sessionStore.countQueuedMessagesForClient(clientId).onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                    } else {
                        logger.warning("Failed to get queued message count for client $clientId: ${result.cause()?.message}")
                        future.complete(0L)
                    }
                }
            } else {
                logger.warning("No clientId found in session context")
                future.complete(0L)
            }

            future
        }
    }

    fun brokerSessions(): DataFetcher<CompletableFuture<List<Session>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Session>>()

            // Get the parent Broker object from the DataFetchingEnvironment
            val broker = env.getSource<Broker>()
            val nodeId = broker?.nodeId

            if (nodeId == null) {
                future.complete(emptyList())
                return@DataFetcher future
            }

            val cleanSessionFilter = env.getArgument<Boolean?>("cleanSession")
            val connectedFilter = env.getArgument<Boolean?>("connected")

            // Use getAllSessions with nodeId filter for this specific broker node
            getAllSessions(cleanSessionFilter, connectedFilter, future, nodeId)

            future
        }
    }

    fun brokerMetrics(): DataFetcher<CompletableFuture<List<BrokerMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<BrokerMetrics>>()

            // Get the parent Broker object from the DataFetchingEnvironment
            val broker = env.getSource<Broker>()
            val nodeId = broker?.nodeId ?: Monster.getClusterNodeId(vertx)

            // Current metrics only - return single most recent entry as array
            getCurrentBrokerMetrics(nodeId) { metrics ->
                future.complete(listOf(metrics))
            }

            future
        }
    }

    fun brokerMetricsHistory(): DataFetcher<CompletableFuture<List<BrokerMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<BrokerMetrics>>()

            // Get the parent Broker object from the DataFetchingEnvironment
            val broker = env.getSource<Broker>()
            val nodeId = broker?.nodeId ?: Monster.getClusterNodeId(vertx)

            val from = env.getArgument<String?>("from")
            val to = env.getArgument<String?>("to")
            val lastMinutes = env.getArgument<Int?>("lastMinutes")

            if (metricsStore != null) {
                // Historical query - convert strings to Instants
                val fromInstant = from?.let { java.time.Instant.parse(it) }
                val toInstant = to?.let { java.time.Instant.parse(it) }

                metricsStore.getBrokerMetricsList(nodeId, fromInstant, toInstant, lastMinutes).onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result().map { bm ->
                            BrokerMetrics(
                                messagesIn = round2(bm.messagesIn),
                                messagesOut = round2(bm.messagesOut),
                                nodeSessionCount = bm.nodeSessionCount,
                                clusterSessionCount = bm.clusterSessionCount,
                                queuedMessagesCount = bm.queuedMessagesCount,
                                topicIndexSize = bm.topicIndexSize,
                                clientNodeMappingSize = bm.clientNodeMappingSize,
                                topicNodeMappingSize = bm.topicNodeMappingSize,
                                messageBusIn = round2(bm.messageBusIn),
                                messageBusOut = round2(bm.messageBusOut),
                                mqttClientIn = round2(bm.mqttClientIn),
                                mqttClientOut = round2(bm.mqttClientOut),
                                opcUaClientIn = round2(bm.opcUaClientIn),
                                opcUaClientOut = round2(bm.opcUaClientOut),
                                kafkaClientIn = round2(bm.kafkaClientIn),
                                kafkaClientOut = round2(bm.kafkaClientOut),
                                winCCUaClientIn = round2(bm.winCCUaClientIn),
                                timestamp = bm.timestamp
                            )
                        })
                    } else {
                        logger.warning("Failed to get historical broker metrics: ${result.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
            } else {
                // No metrics store - return empty list
                future.complete(emptyList())
            }

            future
        }
    }

    fun sessionMetrics(): DataFetcher<CompletableFuture<List<SessionMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<SessionMetrics>>()

            // Get the parent Session object from the DataFetchingEnvironment
            val session = env.getSource<Session>()
            val clientId = session?.clientId
            val nodeId = session?.nodeId

            if (clientId == null || nodeId == null) {
                future.complete(listOf(SessionMetrics(0.0, 0.0, TimestampConverter.currentTimeIsoString())))
                return@DataFetcher future
            }

            val currentNodeId = Monster.getClusterNodeId(vertx)

            if (nodeId == currentNodeId) {
                // Local node - get directly from session handler with rate calculation
                val metrics = sessionHandler.getClientMetricsWithRate(clientId) ?: SessionMetrics(0.0, 0.0, TimestampConverter.currentTimeIsoString())
                future.complete(listOf(metrics))
            } else {
                // Remote node - request via message bus
                val sessionMetricsAddress = EventBusAddresses.Node.sessionMetrics(nodeId, "*")
                val request = JsonObject()
                val deliveryOptions = io.vertx.core.eventbus.DeliveryOptions().addHeader("clientId", clientId)

                vertx.eventBus().request<JsonObject>(sessionMetricsAddress, request, deliveryOptions).onComplete { reply ->
                    if (reply.succeeded()) {
                        val response = reply.result().body()
                        val found = response.getBoolean("found", false)

                        val metrics = if (found) {
                            SessionMetrics(
                                messagesIn = response.getDouble("messagesIn", 0.0),
                                messagesOut = response.getDouble("messagesOut", 0.0),
                                timestamp = TimestampConverter.currentTimeIsoString()
                            )
                        } else {
                            SessionMetrics(0.0, 0.0, TimestampConverter.currentTimeIsoString())
                        }
                        future.complete(listOf(metrics))
                    } else {
                        logger.warning("Failed to get session metrics for client $clientId from node $nodeId: ${reply.cause().message}")
                        future.complete(listOf(SessionMetrics(0.0, 0.0, TimestampConverter.currentTimeIsoString())))
                    }
                }
            }

            future
        }
    }

    fun sessionMetricsHistory(): DataFetcher<CompletableFuture<List<SessionMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<SessionMetrics>>()

            // Get the parent Session object from the DataFetchingEnvironment
            val session = env.getSource<Session>()
            val clientId = session?.clientId

            if (clientId == null) {
                future.complete(emptyList())
                return@DataFetcher future
            }

            val from = env.getArgument<String?>("from")
            val to = env.getArgument<String?>("to")
            val lastMinutes = env.getArgument<Int?>("lastMinutes")

            if (metricsStore != null) {
                // Historical query - convert strings to Instants
                val fromInstant = from?.let { java.time.Instant.parse(it) }
                val toInstant = to?.let { java.time.Instant.parse(it) }

                metricsStore.getSessionMetricsList(clientId, fromInstant, toInstant, lastMinutes).onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                    } else {
                        logger.warning("Failed to get historical session metrics: ${result.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
            } else {
                // No metrics store - return empty list
                future.complete(emptyList())
            }

            future
        }
    }

    private fun getCurrentBrokerMetrics(nodeId: String, callback: (BrokerMetrics) -> Unit) {
        fun roundBrokerMetrics(bm: BrokerMetrics): BrokerMetrics {
            return BrokerMetrics(
                messagesIn = round2(bm.messagesIn),
                messagesOut = round2(bm.messagesOut),
                nodeSessionCount = bm.nodeSessionCount,
                clusterSessionCount = bm.clusterSessionCount,
                queuedMessagesCount = bm.queuedMessagesCount,
                topicIndexSize = bm.topicIndexSize,
                clientNodeMappingSize = bm.clientNodeMappingSize,
                topicNodeMappingSize = bm.topicNodeMappingSize,
                messageBusIn = round2(bm.messageBusIn),
                messageBusOut = round2(bm.messageBusOut),
                mqttClientIn = round2(bm.mqttClientIn),
                mqttClientOut = round2(bm.mqttClientOut),
                opcUaClientIn = round2(bm.opcUaClientIn),
                opcUaClientOut = round2(bm.opcUaClientOut),
                kafkaClientIn = round2(bm.kafkaClientIn),
                kafkaClientOut = round2(bm.kafkaClientOut),
                winCCOaClientIn = round2(bm.winCCOaClientIn),
                winCCUaClientIn = round2(bm.winCCUaClientIn),
                timestamp = bm.timestamp
            )
        }

        if (metricsStore != null) {
            // Use most recent stored metrics for smooth UI updates
            metricsStore.getBrokerMetricsList(nodeId, null, null, 1).onComplete { result ->
                if (result.succeeded() && result.result().isNotEmpty()) {
                     callback(roundBrokerMetrics(result.result().first()))
                } else {
                    // Fallback to live metrics if no stored data
                     getLiveBrokerMetrics(nodeId) { callback(roundBrokerMetrics(it)) }
                }
            }
        } else {
            // No metrics store - use live metrics
            getLiveBrokerMetrics(nodeId) { callback(roundBrokerMetrics(it)) }
        }
    }

    private fun getLiveBrokerMetrics(nodeId: String, callback: (BrokerMetrics) -> Unit) {
        val metricsAddress = getMetricsAddress(nodeId)

        vertx.eventBus().request<JsonObject>(metricsAddress, JsonObject()).onComplete { reply ->
            if (reply.succeeded()) {
                val nodeMetrics = reply.result().body()

                // Get cluster-wide metrics from database
                vertx.executeBlocking<BrokerMetrics>(java.util.concurrent.Callable {
                    try {
                        val clusterSessionCount = getClusterSessionCount()
                        val queuedMessagesCount = getQueuedMessagesCount()

                        BrokerMetrics(
                            // Use raw counts and set rates to 0 for live metrics (prevents sawtooth)
                            messagesIn = 0.0,
                            messagesOut = 0.0,
                            nodeSessionCount = nodeMetrics.getInteger("nodeSessionCount", 0),
                            clusterSessionCount = clusterSessionCount,
                            queuedMessagesCount = queuedMessagesCount,
                            topicIndexSize = nodeMetrics.getInteger("topicIndexSize", 0),
                            clientNodeMappingSize = nodeMetrics.getInteger("clientNodeMappingSize", 0),
                            topicNodeMappingSize = nodeMetrics.getInteger("topicNodeMappingSize", 0),
                            messageBusIn = 0.0,
                            messageBusOut = 0.0,
                            mqttClientIn = 0.0,
                            mqttClientOut = 0.0,
                            opcUaClientIn = 0.0,
                            opcUaClientOut = 0.0,
                            kafkaClientIn = 0.0,
                            kafkaClientOut = 0.0,
                            winCCOaClientIn = 0.0,
                            winCCUaClientIn = 0.0,
                            timestamp = TimestampConverter.currentTimeIsoString()
                        )
                    } catch (e: Exception) {
                        logger.severe("Error getting cluster metrics: ${e.message}")
                        throw e
                    }
                }).onComplete { result ->
                    if (result.succeeded()) {
                        callback(result.result())
                    } else {
                        logger.warning("Failed to get cluster metrics: ${result.cause()?.message}")
callback(BrokerMetrics(
                            messagesIn = 0.0,
                            messagesOut = 0.0,
                            nodeSessionCount = 0,
                            clusterSessionCount = 0,
                            queuedMessagesCount = 0,
                            topicIndexSize = 0,
                            clientNodeMappingSize = 0,
                            topicNodeMappingSize = 0,
                            messageBusIn = 0.0,
                            messageBusOut = 0.0,
                            mqttClientIn = 0.0,
                            mqttClientOut = 0.0,
                            opcUaClientIn = 0.0,
                            opcUaClientOut = 0.0,
                            kafkaClientIn = 0.0,
                            kafkaClientOut = 0.0,
                            winCCUaClientIn = 0.0,
                            timestamp = TimestampConverter.currentTimeIsoString()
                        ))
                    }
                }
            } else {
                logger.warning("Failed to get metrics from node $nodeId: ${reply.cause().message}")
                callback(BrokerMetrics(
                            messagesIn = 0.0,
                            messagesOut = 0.0,
                            nodeSessionCount = 0,
                            clusterSessionCount = 0,
                            queuedMessagesCount = 0,
                            topicIndexSize = 0,
                            clientNodeMappingSize = 0,
                            topicNodeMappingSize = 0,
                            messageBusIn = 0.0,
                            messageBusOut = 0.0,
                            mqttClientIn = 0.0,
                            mqttClientOut = 0.0,
                            opcUaClientIn = 0.0,
                            opcUaClientOut = 0.0,
                            kafkaClientIn = 0.0,
                            kafkaClientOut = 0.0,
                            winCCOaClientIn = 0.0,
                            winCCUaClientIn = 0.0,
                            timestamp = TimestampConverter.currentTimeIsoString()
                        ))
            }
        }
    }

    fun mqttClientMetrics(): DataFetcher<CompletableFuture<List<MqttClientMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<MqttClientMetrics>>()
            val mqttClient = env.getSource<Map<String, Any>>()
            val clientName = mqttClient?.get("name") as? String

            if (clientName == null) {
                future.complete(listOf(MqttClientMetrics(0.0, 0.0, TimestampConverter.currentTimeIsoString())))
                return@DataFetcher future
            }

            if (metricsStore != null) {
                metricsStore.getMqttClientMetricsList(clientName, null, null, 1).onComplete { result ->
                    if (result.succeeded() && result.result().isNotEmpty()) {
                        val m = result.result().first()
                        future.complete(listOf(MqttClientMetrics(round2(m.messagesIn), round2(m.messagesOut), m.timestamp)))
                    } else {
                        future.complete(listOf(MqttClientMetrics(0.0, 0.0, TimestampConverter.currentTimeIsoString())))
                    }
                }
            } else {
                future.complete(listOf(MqttClientMetrics(0.0, 0.0, TimestampConverter.currentTimeIsoString())))
            }

            future
        }
    }

    fun mqttClientMetricsHistory(): DataFetcher<CompletableFuture<List<MqttClientMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<MqttClientMetrics>>()
            val mqttClient = env.getSource<Map<String, Any>>()
            val clientName = mqttClient?.get("name") as? String

            if (clientName == null) {
                future.complete(emptyList())
                return@DataFetcher future
            }

            val from = env.getArgument<String?>("from")
            val to = env.getArgument<String?>("to")
            val lastMinutes = env.getArgument<Int?>("lastMinutes")

            if (metricsStore != null) {
                val fromInstant = from?.let { java.time.Instant.parse(it) }
                val toInstant = to?.let { java.time.Instant.parse(it) }

                metricsStore.getMqttClientMetricsList(clientName, fromInstant, toInstant, lastMinutes).onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result().map { MqttClientMetrics(round2(it.messagesIn), round2(it.messagesOut), it.timestamp) })
                    } else {
                        logger.warning("Failed to get historical MQTT client metrics: ${result.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
            } else {
                future.complete(emptyList())
            }

            future
        }
    }

    fun kafkaClientMetrics(): DataFetcher<CompletableFuture<List<KafkaClientMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<KafkaClientMetrics>>()
            val kafkaClient = env.getSource<Map<String, Any>>()
            val clientName = kafkaClient?.get("name") as? String

            if (clientName == null) {
                future.complete(listOf(KafkaClientMetrics(0.0, 0.0, TimestampConverter.currentTimeIsoString())))
                return@DataFetcher future
            }

            fun fetchLive() {
                // Fallback to live event bus metrics (no history) if store missing or empty
                val addr = at.rocworks.bus.EventBusAddresses.KafkaBridge.connectorMetrics(clientName)
                vertx.eventBus().request<io.vertx.core.json.JsonObject>(addr, io.vertx.core.json.JsonObject()).onComplete { reply ->
                    if (reply.succeeded()) {
                        val body = reply.result().body()
                        val inRate = body.getDouble("messagesInRate", body.getDouble("ratePerSec", 0.0))
                        val outRate = body.getDouble("messagesOutRate", 0.0)
                        future.complete(listOf(KafkaClientMetrics(round2(inRate), round2(outRate), TimestampConverter.currentTimeIsoString())))
                    } else {
                        future.complete(listOf(KafkaClientMetrics(0.0, 0.0, TimestampConverter.currentTimeIsoString())))
                    }
                }
            }

            if (metricsStore != null) {
                metricsStore.getKafkaClientMetricsList(clientName, null, null, 1).onComplete { result ->
                    if (result.succeeded()) {
                        val list = result.result()
                        if (list.isNotEmpty()) {
                            val m = list.first()
                            future.complete(listOf(KafkaClientMetrics(round2(m.messagesIn), round2(m.messagesOut), m.timestamp)))
                        } else {
                            fetchLive()
                        }
                    } else {
                        fetchLive()
                    }
                }
            } else {
                fetchLive()
            }

            future
        }
    }

    fun kafkaClientMetricsHistory(): DataFetcher<CompletableFuture<List<KafkaClientMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<KafkaClientMetrics>>()
            val kafkaClient = env.getSource<Map<String, Any>>()
            val clientName = kafkaClient?.get("name") as? String

            if (clientName == null) {
                future.complete(emptyList())
                return@DataFetcher future
            }

            val from = env.getArgument<String?>("from")
            val to = env.getArgument<String?>("to")
            val lastMinutes = env.getArgument<Int?>("lastMinutes")

            if (metricsStore != null) {
                val fromInstant = from?.let { java.time.Instant.parse(it) }
                val toInstant = to?.let { java.time.Instant.parse(it) }

                metricsStore.getKafkaClientMetricsList(clientName, fromInstant, toInstant, lastMinutes).onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result().map { KafkaClientMetrics(round2(it.messagesIn), round2(it.messagesOut), it.timestamp) })
                    } else {
                        logger.warning("Failed to get historical Kafka client metrics: ${result.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
            } else {
                future.complete(emptyList())
            }

            future
        }
    }

    // Archive Group Metrics (embedded field resolvers)
    fun archiveGroupMetricsField(): DataFetcher<CompletableFuture<List<ArchiveGroupMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<ArchiveGroupMetrics>>()
            val archiveGroup = env.getSource<Map<String, Any>>()
            val groupName = archiveGroup?.get("name") as? String
            if (groupName == null) {
                future.complete(emptyList())
                return@DataFetcher future
            }
            if (metricsStore == null) {
                future.complete(listOf(ArchiveGroupMetrics(0.0, 0, TimestampConverter.currentTimeIsoString())))
                return@DataFetcher future
            }
            // Get most recent metrics
            metricsStore.getLatestMetrics(at.rocworks.stores.MetricKind.ARCHIVEGROUP, groupName, null, null, 1).onComplete { result ->
                if (result.succeeded()) {
                    val metricsJson = result.result()
                    val messagesOut = metricsJson.getDouble("messagesOut", 0.0)
                    val bufferSize = metricsJson.getInteger("bufferSize", 0)
                    future.complete(listOf(ArchiveGroupMetrics(messagesOut, bufferSize, TimestampConverter.currentTimeIsoString())))
                } else {
                    logger.warning("Failed to get archive group metrics for $groupName: ${result.cause()?.message}")
                    future.complete(listOf(ArchiveGroupMetrics(0.0, 0, TimestampConverter.currentTimeIsoString())))
                }
            }
            future
        }
    }

    fun archiveGroupMetricsHistoryField(): DataFetcher<CompletableFuture<List<ArchiveGroupMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<ArchiveGroupMetrics>>()
            val archiveGroup = env.getSource<Map<String, Any>>()
            val groupName = archiveGroup?.get("name") as? String
            if (groupName == null) {
                future.complete(emptyList())
                return@DataFetcher future
            }
            if (metricsStore == null) {
                future.complete(emptyList())
                return@DataFetcher future
            }
            val from = env.getArgument<String?>("from")
            val to = env.getArgument<String?>("to")
            val lastMinutes = env.getArgument<Int?>("lastMinutes")
            val limit = env.getArgument<Int?>("limit") ?: 100
            val fromInstant = from?.let { java.time.Instant.parse(it) }
            val toInstant = to?.let { java.time.Instant.parse(it) }
            metricsStore.getMetricsHistory(at.rocworks.stores.MetricKind.ARCHIVEGROUP, groupName, fromInstant, toInstant, lastMinutes, limit).onComplete { result ->
                if (result.succeeded()) {
                    val list = result.result().map { (timestamp, metricsJson) ->
                        val messagesOut = metricsJson.getDouble("messagesOut", 0.0)
                        val bufferSize = metricsJson.getInteger("bufferSize", 0)
                        ArchiveGroupMetrics(round2(messagesOut), bufferSize, TimestampConverter.instantToIsoString(timestamp))
                    }
                    future.complete(list)
                } else {
                    logger.warning("Failed to get archive group metrics history for $groupName: ${result.cause()?.message}")
                    future.complete(emptyList())
                }
            }
            future
        }
    }

    fun winCCOaClientMetrics(): DataFetcher<CompletableFuture<List<WinCCOaClientMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<WinCCOaClientMetrics>>()
            val winCCOaClient = env.getSource<Map<String, Any>>()
            val clientName = winCCOaClient?.get("name") as? String

            if (clientName == null) {
                future.complete(listOf(WinCCOaClientMetrics(0.0, false, TimestampConverter.currentTimeIsoString())))
                return@DataFetcher future
            }

            if (metricsStore != null) {
                metricsStore.getWinCCOaClientMetricsList(clientName, null, null, 1).onComplete { result ->
                    if (result.succeeded() && result.result().isNotEmpty()) {
                        val m = result.result().first()
                        future.complete(listOf(WinCCOaClientMetrics(round2(m.messagesIn), m.connected, m.timestamp)))
                    } else {
                        future.complete(listOf(WinCCOaClientMetrics(0.0, false, TimestampConverter.currentTimeIsoString())))
                    }
                }
            } else {
                future.complete(listOf(WinCCOaClientMetrics(0.0, false, TimestampConverter.currentTimeIsoString())))
            }

            future
        }
    }

    fun winCCOaClientMetricsHistory(): DataFetcher<CompletableFuture<List<WinCCOaClientMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<WinCCOaClientMetrics>>()
            val winCCOaClient = env.getSource<Map<String, Any>>()
            val clientName = winCCOaClient?.get("name") as? String

            if (clientName == null) {
                future.complete(emptyList())
                return@DataFetcher future
            }

            val from = env.getArgument<String?>("from")
            val to = env.getArgument<String?>("to")
            val lastMinutes = env.getArgument<Int?>("lastMinutes")

            if (metricsStore != null) {
                val fromInstant = from?.let { java.time.Instant.parse(it) }
                val toInstant = to?.let { java.time.Instant.parse(it) }

                metricsStore.getWinCCOaClientMetricsList(clientName, fromInstant, toInstant, lastMinutes).onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result().map { WinCCOaClientMetrics(round2(it.messagesIn), it.connected, it.timestamp) })
                    } else {
                        logger.warning("Failed to get historical WinCC OA client metrics: ${result.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
            } else {
                future.complete(emptyList())
            }

            future
        }
    }

    fun winCCUaClientMetrics(): DataFetcher<CompletableFuture<List<WinCCUaClientMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<WinCCUaClientMetrics>>()
            val winCCUaClient = env.getSource<Map<String, Any>>()
            val clientName = winCCUaClient?.get("name") as? String

            if (clientName == null) {
                future.complete(listOf(WinCCUaClientMetrics(0.0, false, TimestampConverter.currentTimeIsoString())))
                return@DataFetcher future
            }

            fun fetchLive() {
                // Fallback to live event bus metrics (no history) if store missing or empty
                val addr = at.rocworks.bus.EventBusAddresses.WinCCUaBridge.connectorMetrics(clientName)
                vertx.eventBus().request<io.vertx.core.json.JsonObject>(addr, io.vertx.core.json.JsonObject()).onComplete { reply ->
                    if (reply.succeeded()) {
                        val body = reply.result().body()
                        val inRate = body.getDouble("messagesInRate", 0.0)
                        val connected = body.getBoolean("connected", false)
                        future.complete(listOf(WinCCUaClientMetrics(round2(inRate), connected, TimestampConverter.currentTimeIsoString())))
                    } else {
                        future.complete(listOf(WinCCUaClientMetrics(0.0, false, TimestampConverter.currentTimeIsoString())))
                    }
                }
            }

            if (metricsStore != null) {
                metricsStore.getWinCCUaClientMetricsList(clientName, null, null, 1).onComplete { result ->
                    if (result.succeeded()) {
                        val list = result.result()
                        if (list.isNotEmpty()) {
                            val m = list.first()
                            future.complete(listOf(WinCCUaClientMetrics(round2(m.messagesIn), m.connected, m.timestamp)))
                        } else {
                            fetchLive()
                        }
                    } else {
                        fetchLive()
                    }
                }
            } else {
                fetchLive()
            }

            future
        }
    }

    fun winCCUaClientMetricsHistory(): DataFetcher<CompletableFuture<List<WinCCUaClientMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<WinCCUaClientMetrics>>()
            val winCCUaClient = env.getSource<Map<String, Any>>()
            val clientName = winCCUaClient?.get("name") as? String

            if (clientName == null) {
                future.complete(emptyList())
                return@DataFetcher future
            }

            val from = env.getArgument<String?>("from")
            val to = env.getArgument<String?>("to")
            val lastMinutes = env.getArgument<Int?>("lastMinutes")

            if (metricsStore != null) {
                val fromInstant = from?.let { java.time.Instant.parse(it) }
                val toInstant = to?.let { java.time.Instant.parse(it) }

                metricsStore.getWinCCUaClientMetricsList(clientName, fromInstant, toInstant, lastMinutes).onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result().map { WinCCUaClientMetrics(round2(it.messagesIn), it.connected, it.timestamp) })
                    } else {
                        logger.warning("Failed to get historical WinCC Unified client metrics: ${result.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
            } else {
                future.complete(emptyList())
            }

            future
        }
    }

    // OPC UA Device Metrics (embedded field resolvers)
    fun opcUaDeviceMetricsField(): DataFetcher<CompletableFuture<List<OpcUaDeviceMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<OpcUaDeviceMetrics>>()
            val device = env.getSource<Map<String, Any>>()
            val deviceName = device?.get("name") as? String
            if (deviceName == null) {
                future.complete(emptyList())
                return@DataFetcher future
            }
            if (metricsStore == null) {
future.complete(listOf(OpcUaDeviceMetrics(0.0, 0.0, TimestampConverter.currentTimeIsoString())))
                return@DataFetcher future
            }
            // Get most recent metrics (limit 1 by using lastMinutes=1 or store-specific latest fetch)
            metricsStore.getOpcUaDeviceMetrics(deviceName, null, null, 1).onComplete { result ->
                if (result.succeeded()) {
                    val m = result.result()
                    future.complete(listOf(OpcUaDeviceMetrics(m.messagesIn, m.messagesOut, m.timestamp)))
                } else {
                    logger.warning("Failed to get OPC UA device metrics for $deviceName: ${result.cause()?.message}")
                future.complete(listOf(OpcUaDeviceMetrics(0.0, 0.0, TimestampConverter.currentTimeIsoString())))
                }
            }
            future
        }
    }

    fun opcUaDeviceMetricsHistoryField(): DataFetcher<CompletableFuture<List<OpcUaDeviceMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<OpcUaDeviceMetrics>>()
            val device = env.getSource<Map<String, Any>>()
            val deviceName = device?.get("name") as? String
            if (deviceName == null) {
                future.complete(emptyList())
                return@DataFetcher future
            }
            if (metricsStore == null) {
                future.complete(emptyList())
                return@DataFetcher future
            }
            val from = env.getArgument<String?>("from")
            val to = env.getArgument<String?>("to")
            val lastMinutes = env.getArgument<Int?>("lastMinutes")
            val limit = env.getArgument<Int?>("limit") ?: 100
            val fromInstant = from?.let { java.time.Instant.parse(it) }
            val toInstant = to?.let { java.time.Instant.parse(it) }
            metricsStore.getOpcUaDeviceMetricsHistory(deviceName, fromInstant, toInstant, lastMinutes, limit).onComplete { result ->
                if (result.succeeded()) {
                    val list = result.result().map { (_, metrics) ->
                        OpcUaDeviceMetrics(round2(metrics.messagesIn), round2(metrics.messagesOut), metrics.timestamp)
                    }
                    future.complete(list)
                } else {
                    logger.warning("Failed to get OPC UA device metrics history for $deviceName: ${result.cause()?.message}")
                    future.complete(emptyList())
                }
            }
            future
        }
    }

    // PLC4X Client Metrics (embedded field resolvers)
    fun plc4xClientMetrics(): DataFetcher<CompletableFuture<List<Plc4xDeviceMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Plc4xDeviceMetrics>>()
            val client = env.getSource<Map<String, Any>>()
            val clientName = client?.get("name") as? String

            if (clientName == null) {
                future.complete(listOf(Plc4xDeviceMetrics(0.0, false, TimestampConverter.currentTimeIsoString())))
                return@DataFetcher future
            }

            // Fetch live metrics from connector via EventBus
            val addr = EventBusAddresses.Plc4xBridge.connectorMetrics(clientName)
            vertx.eventBus().request<JsonObject>(addr, JsonObject()).onComplete { reply ->
                if (reply.succeeded()) {
                    val body = reply.result().body()
                    val inRate = body.getDouble("messagesInRate", 0.0)
                    val connected = body.getBoolean("connected", false)
                    future.complete(listOf(Plc4xDeviceMetrics(round2(inRate), connected, TimestampConverter.currentTimeIsoString())))
                } else {
                    future.complete(listOf(Plc4xDeviceMetrics(0.0, false, TimestampConverter.currentTimeIsoString())))
                }
            }

            future
        }
    }

    fun plc4xClientMetricsHistory(): DataFetcher<CompletableFuture<List<Plc4xDeviceMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Plc4xDeviceMetrics>>()
            val client = env.getSource<Map<String, Any>>()
            val clientName = client?.get("name") as? String

            if (clientName == null) {
                future.complete(emptyList())
                return@DataFetcher future
            }

            // Metrics history not yet implemented for PLC4X - would need metricsStore support
            future.complete(emptyList())

            future
        }
    }

    // Neo4j Client Metrics (embedded field resolvers)
    fun neo4jClientMetrics(): DataFetcher<CompletableFuture<List<Neo4jClientMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Neo4jClientMetrics>>()
            val client = env.getSource<Map<String, Any>>()
            val clientName = client?.get("name") as? String

            if (clientName == null) {
                future.complete(listOf(Neo4jClientMetrics(0.0, 0.0, 0.0, 0, 0.0, 0.0, TimestampConverter.currentTimeIsoString())))
                return@DataFetcher future
            }

            // Fetch live metrics from connector via EventBus
            val addr = EventBusAddresses.Neo4jBridge.connectorMetrics(clientName)
            vertx.eventBus().request<JsonObject>(addr, JsonObject()).onComplete { reply ->
                if (reply.succeeded()) {
                    val body = reply.result().body()
                    val messagesIn = body.getDouble("messagesIn", 0.0)
                    val messagesWritten = body.getDouble("messagesWritten", 0.0)
                    val errors = body.getDouble("errors", 0.0)
                    val pathQueueSize = body.getInteger("pathQueueSize", 0)
                    val messagesInRate = body.getDouble("messagesInRate", 0.0)
                    val messagesWrittenRate = body.getDouble("messagesWrittenRate", 0.0)
                    future.complete(listOf(Neo4jClientMetrics(
                        round2(messagesIn),
                        round2(messagesWritten),
                        round2(errors),
                        pathQueueSize,
                        round2(messagesInRate),
                        round2(messagesWrittenRate),
                        TimestampConverter.currentTimeIsoString()
                    )))
                } else {
                    future.complete(listOf(Neo4jClientMetrics(0.0, 0.0, 0.0, 0, 0.0, 0.0, TimestampConverter.currentTimeIsoString())))
                }
            }

            future
        }
    }

    fun neo4jClientMetricsHistory(): DataFetcher<CompletableFuture<List<Neo4jClientMetrics>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Neo4jClientMetrics>>()
            val client = env.getSource<Map<String, Any>>()
            val clientName = client?.get("name") as? String

            if (clientName == null) {
                future.complete(emptyList())
                return@DataFetcher future
            }

            // Metrics history not yet implemented for Neo4j - would need metricsStore support
            future.complete(emptyList())

            future
        }
    }
}
