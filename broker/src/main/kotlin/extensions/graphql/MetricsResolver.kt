package at.rocworks.extensions.graphql

import at.rocworks.Monster
import at.rocworks.MqttClient
import at.rocworks.handlers.MetricsHandler
import at.rocworks.stores.ISessionStoreAsync
import at.rocworks.stores.IMessageStore
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

class MetricsResolver(
    private val vertx: Vertx,
    private val sessionStore: ISessionStoreAsync
) {
    companion object {
        private val logger: Logger = Logger.getLogger(MetricsResolver::class.java.name)
    }

    fun broker(): DataFetcher<CompletableFuture<Broker?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Broker?>()
            val nodeId = env.getArgument<String?>("nodeId") ?: Monster.getClusterNodeId(vertx)

            // Query the specific node for its metrics via EventBus
            val metricsAddress = MetricsHandler.getMetricsAddress(nodeId)

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

                val metricsAddress = MetricsHandler.getMetricsAddress(nodeId)
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

            if (nodeId != null) {
                // Get sessions for specific node
                getSessionsForNode(nodeId, future)
            } else {
                // Get sessions for all nodes
                getAllSessions(future)
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

            // Get session from local registry
            val sessionRegistry = MqttClient.getSessionRegistry()
            val mqttSession = sessionRegistry[clientId]

            if (mqttSession != null) {
                // Found in local registry - use blocking approach for subscriptions
                vertx.executeBlocking<Session>(java.util.concurrent.Callable {
                    val subscriptions = getSubscriptionsForClient(clientId)
                    Session(
                        clientId = mqttSession.clientId,
                        nodeId = mqttSession.nodeId ?: Monster.getClusterNodeId(vertx),
                        metrics = SessionMetrics(
                            messagesIn = mqttSession.messagesIn.get(),
                            messagesOut = mqttSession.messagesOut.get()
                        ),
                        subscriptions = subscriptions,
                        cleanSession = mqttSession.cleanSession,
                        sessionExpiryInterval = mqttSession.sessionExpiryInterval,
                        clientAddress = mqttSession.clientAddress
                    )
                }).onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                    } else {
                        future.completeExceptionally(result.cause())
                    }
                }
            } else {
                // Not in local registry, might be on another node or offline
                future.complete(null)
            }

            future
        }
    }

    private fun getSessionsForNode(nodeId: String, future: CompletableFuture<List<Session>>) {
        if (nodeId == Monster.getClusterNodeId(vertx)) {
            // Local node - get from registry
            getAllSessions(future)
        } else {
            // Remote node - would need to query via EventBus
            // For now, return empty list
            future.complete(emptyList())
        }
    }

    private fun getAllSessions(future: CompletableFuture<List<Session>>) {
        vertx.executeBlocking<List<Session>>(java.util.concurrent.Callable {
            val sessionRegistry = MqttClient.getSessionRegistry()
            sessionRegistry.values.map { mqttSession ->
                val subscriptions = getSubscriptionsForClient(mqttSession.clientId)
                Session(
                    clientId = mqttSession.clientId,
                    nodeId = mqttSession.nodeId ?: Monster.getClusterNodeId(vertx),
                    metrics = SessionMetrics(
                        messagesIn = mqttSession.messagesIn.get(),
                        messagesOut = mqttSession.messagesOut.get()
                    ),
                    subscriptions = subscriptions,
                    cleanSession = mqttSession.cleanSession,
                    sessionExpiryInterval = mqttSession.sessionExpiryInterval,
                    clientAddress = mqttSession.clientAddress
                )
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
        // For now, return local count
        return MqttClient.getSessionRegistry().size
    }

    private fun getQueuedMessagesCount(): Long {
        // For now, return 0
        return 0L
    }
}