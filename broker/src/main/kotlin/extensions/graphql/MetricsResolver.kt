package at.rocworks.extensions.graphql

import at.rocworks.Monster
import at.rocworks.stores.ISessionStoreAsync
import at.rocworks.stores.IMessageStore
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

class MetricsResolver(
    private val vertx: Vertx,
    private val sessionStore: ISessionStoreAsync,
    private val sessionHandler: at.rocworks.handlers.SessionHandler
) {
    companion object {
        private val logger: Logger = Logger.getLogger(MetricsResolver::class.java.name)

        private fun getMetricsAddress(nodeId: String): String {
            return "monstermq.node.metrics.$nodeId"
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
                                queuedMessagesCount = queuedMessagesCount
                            )
                        } catch (e: Exception) {
                            logger.severe("Error getting cluster metrics: ${e.message}")
                            throw e
                        }
                    }).onComplete { result ->
                        if (result.succeeded()) {
                            future.complete(Broker(nodeId, result.result()))
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
                                    queuedMessagesCount = queuedMessagesCount
                                )
                            } catch (e: Exception) {
                                logger.severe("Error getting cluster metrics: ${e.message}")
                                throw e
                            }
                        }).onComplete { result ->
                            if (result.succeeded()) {
                                brokerFuture.complete(Broker(nodeId, result.result()))
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
                val brokers = brokerFutures.mapNotNull { it.get() }
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

            if (nodeId != null) {
                // Get sessions for specific node
                getSessionsForNode(nodeId, cleanSessionFilter, connectedFilter, future)
            } else {
                // Get sessions for all nodes
                getAllSessions(cleanSessionFilter, connectedFilter, future)
            }

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

            // Get session from SessionHandler
            val clientMetrics = sessionHandler.getClientMetrics(clientId)
            val clientDetails = sessionHandler.getClientDetails(clientId)

            if (clientMetrics != null && clientDetails != null) {
                // Found in SessionHandler - use blocking approach for subscriptions
                vertx.executeBlocking<Session>(java.util.concurrent.Callable {
                    val subscriptions = getSubscriptionsForClient(clientId)
                    Session(
                        clientId = clientId,
                        nodeId = clientDetails.nodeId,
                        metrics = SessionMetrics(
                            messagesIn = clientMetrics.messagesIn.get(),
                            messagesOut = clientMetrics.messagesOut.get()
                        ),
                        subscriptions = subscriptions,
                        cleanSession = clientDetails.cleanSession,
                        sessionExpiryInterval = clientDetails.sessionExpiryInterval?.toLong() ?: 0L,
                        clientAddress = clientDetails.clientAddress,
                        connected = sessionHandler.getClientStatus(clientId) == at.rocworks.handlers.SessionHandler.ClientStatus.ONLINE
                    )
                }).onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                    } else {
                        future.completeExceptionally(result.cause())
                    }
                }
            } else {
                // Not in SessionHandler, might be on another node or offline
                future.complete(null)
            }

            future
        }
    }

    private fun getSessionsForNode(nodeId: String, cleanSessionFilter: Boolean?, connectedFilter: Boolean?, future: CompletableFuture<List<Session>>) {
        if (nodeId == Monster.getClusterNodeId(vertx)) {
            // Local node - get from registry
            getAllSessions(cleanSessionFilter, connectedFilter, future)
        } else {
            // Remote node - would need to query via EventBus
            // For now, return empty list
            future.complete(emptyList())
        }
    }

    private fun getAllSessions(cleanSessionFilter: Boolean?, connectedFilter: Boolean?, future: CompletableFuture<List<Session>>) {
        vertx.executeBlocking<List<Session>>(java.util.concurrent.Callable {
            val allMetrics = sessionHandler.getAllClientMetrics()
            allMetrics.entries.mapNotNull { (clientId, metrics) ->
                val clientDetails = sessionHandler.getClientDetails(clientId)
                if (clientDetails != null) {
                    val subscriptions = getSubscriptionsForClient(clientId)
                    val connected = sessionHandler.getClientStatus(clientId) == at.rocworks.handlers.SessionHandler.ClientStatus.ONLINE
                    val session = Session(
                        clientId = clientId,
                        nodeId = clientDetails.nodeId,
                        metrics = SessionMetrics(
                            messagesIn = metrics.messagesIn.get(),
                            messagesOut = metrics.messagesOut.get()
                        ),
                        subscriptions = subscriptions,
                        cleanSession = clientDetails.cleanSession,
                        sessionExpiryInterval = clientDetails.sessionExpiryInterval?.toLong() ?: 0L,
                        clientAddress = clientDetails.clientAddress,
                        connected = connected
                    )

                    // Apply filters
                    val passesCleanSessionFilter = cleanSessionFilter?.let { it == session.cleanSession } ?: true
                    val passesConnectedFilter = connectedFilter?.let { it == session.connected } ?: true

                    if (passesCleanSessionFilter && passesConnectedFilter) {
                        session
                    } else {
                        null
                    }
                } else null
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                future.complete(result.result())
            } else {
                future.completeExceptionally(result.cause())
            }
        }
    }

    private fun getSubscriptionsForClient(clientId: String): List<MqttSubscription> {
        val subscriptions = mutableListOf<MqttSubscription>()

        // Direct SQLite query approach - more reliable for now
        try {
            // Using the Java SQLite driver directly since we know it's SQLite
            val dbFile = java.io.File("monstermq.db")
            if (dbFile.exists()) {
                val connection = java.sql.DriverManager.getConnection("jdbc:sqlite:monstermq.db")
                val statement = connection.prepareStatement("SELECT topic, qos FROM subscriptions WHERE client_id = ?")
                statement.setString(1, clientId)
                val resultSet = statement.executeQuery()

                while (resultSet.next()) {
                    val topic = resultSet.getString("topic")
                    val qos = resultSet.getInt("qos")
                    subscriptions.add(MqttSubscription(topicFilter = topic, qos = qos))
                }

                resultSet.close()
                statement.close()
                connection.close()
            }
        } catch (e: Exception) {
            logger.warning("Error getting subscriptions for client $clientId: ${e.message}")
        }

        return subscriptions
    }

    private fun getClusterSessionCount(): Int {
        // For now, return local count from SessionHandler
        return sessionHandler.getSessionCount()
    }

    private fun getQueuedMessagesCount(): Long {
        return sessionStore.sync.countQueuedMessages()
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

            // Use existing getSessionsForNode method to get sessions for this specific broker node
            getSessionsForNode(nodeId, cleanSessionFilter, connectedFilter, future)

            future
        }
    }
}