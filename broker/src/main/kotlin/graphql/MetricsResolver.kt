package at.rocworks.extensions.graphql

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
        private val logger: Logger = Logger.getLogger(MetricsResolver::class.java.name)

        private fun getMetricsAddress(nodeId: String): String {
            return EventBusAddresses.Node.metrics(nodeId)
        }
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
                                messagesIn = nodeMetrics.getLong("messagesIn", 0L),
                                messagesOut = nodeMetrics.getLong("messagesOut", 0L),
                                nodeSessionCount = nodeMetrics.getInteger("nodeSessionCount", 0),
                                clusterSessionCount = clusterSessionCount,
                                queuedMessagesCount = queuedMessagesCount,
                                topicIndexSize = nodeMetrics.getInteger("topicIndexSize", 0),
                                clientNodeMappingSize = nodeMetrics.getInteger("clientNodeMappingSize", 0),
                                topicNodeMappingSize = nodeMetrics.getInteger("topicNodeMappingSize", 0),
                                messageBusIn = nodeMetrics.getLong("messageBusIn", 0L),
                                messageBusOut = nodeMetrics.getLong("messageBusOut", 0L),
                                timestamp = TimestampConverter.currentTimeIsoString()
                            )
                        } catch (e: Exception) {
                            logger.severe("Error getting cluster metrics: ${e.message}")
                            throw e
                        }
                    }).onComplete { result ->
                        if (result.succeeded()) {
                            future.complete(Broker(nodeId))
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
                                    messagesIn = nodeMetrics.getLong("messagesIn", 0L),
                                    messagesOut = nodeMetrics.getLong("messagesOut", 0L),
                                    nodeSessionCount = nodeMetrics.getInteger("nodeSessionCount", 0),
                                    clusterSessionCount = clusterSessionCount,
                                    queuedMessagesCount = queuedMessagesCount,
                                    topicIndexSize = nodeMetrics.getInteger("topicIndexSize", 0),
                                    clientNodeMappingSize = nodeMetrics.getInteger("clientNodeMappingSize", 0),
                                    topicNodeMappingSize = nodeMetrics.getInteger("topicNodeMappingSize", 0),
                                    messageBusIn = nodeMetrics.getLong("messageBusIn", 0L),
                                    messageBusOut = nodeMetrics.getLong("messageBusOut", 0L),
                                    timestamp = TimestampConverter.currentTimeIsoString()
                                )
                            } catch (e: Exception) {
                                logger.severe("Error getting cluster metrics: ${e.message}")
                                throw e
                            }
                        }).onComplete { result ->
                            if (result.succeeded()) {
                                brokerFuture.complete(Broker(nodeId))
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
            val clientId = env.getArgument<String>("clientId")

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
                        connected = sessionHandler.getClientStatus(clientId) == at.rocworks.handlers.SessionHandler.ClientStatus.ONLINE
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
                                        connected = response.getBoolean("connected", sessionConnected)
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
                                connected = connected
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
        // This method is used synchronously in broker metrics, but ideally should be async too
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
                        future.complete(result.result())
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
                future.complete(listOf(SessionMetrics(0, 0, TimestampConverter.currentTimeIsoString())))
                return@DataFetcher future
            }

            val currentNodeId = Monster.getClusterNodeId(vertx)

            if (nodeId == currentNodeId) {
                // Local node - get directly from session handler
                val clientMetrics = sessionHandler.getClientMetrics(clientId)
                val metrics = if (clientMetrics != null) {
                    SessionMetrics(
                        messagesIn = clientMetrics.messagesIn.get(),
                        messagesOut = clientMetrics.messagesOut.get(),
                        timestamp = TimestampConverter.currentTimeIsoString()
                    )
                } else {
                    SessionMetrics(0, 0, TimestampConverter.currentTimeIsoString())
                }
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
                                messagesIn = response.getLong("messagesIn", 0L),
                                messagesOut = response.getLong("messagesOut", 0L),
                                timestamp = TimestampConverter.currentTimeIsoString()
                            )
                        } else {
                            SessionMetrics(0, 0, TimestampConverter.currentTimeIsoString())
                        }
                        future.complete(listOf(metrics))
                    } else {
                        logger.warning("Failed to get session metrics for client $clientId from node $nodeId: ${reply.cause().message}")
                        future.complete(listOf(SessionMetrics(0, 0, TimestampConverter.currentTimeIsoString())))
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
                            messagesIn = nodeMetrics.getLong("messagesIn", 0L),
                            messagesOut = nodeMetrics.getLong("messagesOut", 0L),
                            nodeSessionCount = nodeMetrics.getInteger("nodeSessionCount", 0),
                            clusterSessionCount = clusterSessionCount,
                            queuedMessagesCount = queuedMessagesCount,
                            topicIndexSize = nodeMetrics.getInteger("topicIndexSize", 0),
                            clientNodeMappingSize = nodeMetrics.getInteger("clientNodeMappingSize", 0),
                            topicNodeMappingSize = nodeMetrics.getInteger("topicNodeMappingSize", 0),
                            messageBusIn = nodeMetrics.getLong("messageBusIn", 0L),
                            messageBusOut = nodeMetrics.getLong("messageBusOut", 0L),
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
                        callback(BrokerMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, TimestampConverter.currentTimeIsoString()))
                    }
                }
            } else {
                logger.warning("Failed to get metrics from node $nodeId: ${reply.cause().message}")
                callback(BrokerMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, TimestampConverter.currentTimeIsoString()))
            }
        }
    }
}