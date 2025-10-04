package handlers

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.extensions.graphql.BrokerMetrics
import at.rocworks.extensions.graphql.SessionMetrics
import at.rocworks.extensions.graphql.TimestampConverter
import at.rocworks.handlers.SessionHandler
import at.rocworks.stores.IMetricsStoreAsync
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.logging.Logger

class MetricsHandler(
    private val sessionHandler: SessionHandler,
    private val metricsStore: IMetricsStoreAsync,
    private val collectionIntervalSeconds: Int = 1
) : AbstractVerticle() {
    companion object {
        private val logger: Logger = Utils.getLogger(MetricsHandler::class.java)
    }

    private val collectionIntervalMs = collectionIntervalSeconds * 1000L


    override fun start(startPromise: Promise<Void>) {
        metricsStore.startStore(vertx).onComplete { storeResult ->
            if (storeResult.succeeded()) {
                // Start periodic metrics collection
                vertx.setPeriodic(collectionIntervalMs) { _ ->
                    collectAndStoreMetrics()
                }

                logger.info("MetricsCollector started with ${collectionIntervalSeconds}s (${collectionIntervalMs}ms) interval")
                startPromise.complete()
            } else {
                logger.severe("Failed to start metrics collector: ${storeResult.cause()?.message}")
                startPromise.fail(storeResult.cause())
            }
        }
    }

    private fun collectAndStoreMetrics() {
        try {
            val timestamp = Instant.now()
            val nodeId = Monster.Companion.getClusterNodeId(vertx)

            logger.fine("Collecting metrics at $timestamp for nodeId: $nodeId")

            // Collect broker metrics (includes session metrics in the same call to avoid double reset)
            collectBrokerMetrics(timestamp, nodeId)

        } catch (e: Exception) {
            logger.warning("Error collecting metrics: ${e.message}")
            e.printStackTrace()
        }
    }

    private fun collectBrokerMetrics(timestamp: Instant, nodeId: String) {
        try {
            // Get current broker metrics via EventBus and reset counters
            val metricsAddress = EventBusAddresses.Node.metricsAndReset(nodeId)

            logger.fine("Requesting broker metrics with reset from address: $metricsAddress")

            vertx.eventBus().request<JsonObject>(metricsAddress, JsonObject()).onComplete { reply ->
                if (reply.succeeded()) {
                    val nodeMetrics = reply.result().body()
                    try {
                        val brokerMetrics = BrokerMetrics(
                            messagesIn = nodeMetrics.getDouble("messagesInRate", 0.0),
                            messagesOut = nodeMetrics.getDouble("messagesOutRate", 0.0),
                            nodeSessionCount = nodeMetrics.getInteger("nodeSessionCount", 0),
                            clusterSessionCount = sessionHandler.getSessionCount(),
                            queuedMessagesCount = 0L, // TODO: Get from session store if needed
                            topicIndexSize = nodeMetrics.getInteger("topicIndexSize", 0),
                            clientNodeMappingSize = nodeMetrics.getInteger("clientNodeMappingSize", 0),
                            topicNodeMappingSize = nodeMetrics.getInteger("topicNodeMappingSize", 0),
                            messageBusIn = nodeMetrics.getDouble("messageBusInRate", 0.0),
                            messageBusOut = nodeMetrics.getDouble("messageBusOutRate", 0.0),
                            timestamp = TimestampConverter.instantToIsoString(timestamp)
                        )

                        metricsStore.storeBrokerMetrics(timestamp, nodeId, brokerMetrics).onComplete { result ->
                            if (result.succeeded()) {
                                logger.fine("Successfully stored broker metrics for nodeId: $nodeId")
                            } else {
                                logger.warning("Error storing broker metrics: ${result.cause()?.message}")
                            }
                        }

                        // Process individual session metrics from the same response
                        val sessionMetricsArray = nodeMetrics.getJsonArray("sessionMetrics")
                        if (sessionMetricsArray != null) {
                            sessionMetricsArray.forEach { sessionMetricObj ->
                                val sessionMetric = sessionMetricObj as JsonObject
                                val clientId = sessionMetric.getString("clientId")
                                val messagesInRate = sessionMetric.getDouble("messagesInRate", 0.0)
                                val messagesOutRate = sessionMetric.getDouble("messagesOutRate", 0.0)

                                val sessionMetrics = SessionMetrics(
                                    messagesIn = messagesInRate,
                                    messagesOut = messagesOutRate,
                                    timestamp = TimestampConverter.instantToIsoString(timestamp)
                                )

                                metricsStore.storeSessionMetrics(timestamp, clientId, sessionMetrics).onComplete { result ->
                                    if (result.succeeded()) {
                                        logger.fine("Successfully stored session metrics for client: $clientId")
                                    } else {
                                        logger.warning("Error storing session metrics for client $clientId: ${result.cause()?.message}")
                                    }
                                }
                            }
                        }
                    } catch (e: Exception) {
                        logger.warning("Error processing broker metrics: ${e.message}")
                    }
                } else {
                    logger.warning("Failed to get broker metrics: ${reply.cause()?.message}")
                }
            }
        } catch (e: Exception) {
            logger.warning("Error collecting broker metrics: ${e.message}")
        }
    }

    private fun collectSessionMetrics(timestamp: Instant) {
        try {
            // Get all active clients from session handler and reset their counters
            val allClientMetrics = sessionHandler.getAllClientMetricsAndReset()

            logger.fine("Collecting session metrics for ${allClientMetrics.size} clients (with reset)")

            allClientMetrics.forEach { (clientId, sessionMetrics) ->
                logger.finest("Storing session metrics for client $clientId: $sessionMetrics")

                metricsStore.storeSessionMetrics(timestamp, clientId, sessionMetrics).onComplete { result ->
                    if (result.succeeded()) {
                        logger.finest("Successfully stored session metrics for client: $clientId")
                    } else {
                        logger.warning("Error storing session metrics for client $clientId: ${result.cause()?.message}")
                    }
                }
            }
        } catch (e: Exception) {
            logger.warning("Error collecting session metrics: ${e.message}")
        }
    }

}