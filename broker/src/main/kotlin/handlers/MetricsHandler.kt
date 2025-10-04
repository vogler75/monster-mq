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
            logger.fine("Collecting metrics at $timestamp for nodeId: $nodeId (concurrent aggregation)")

            // Placeholders for concurrent aggregation
            var nodeMetrics: JsonObject? = null
            var bridgeInTotal = 0.0
            var bridgeOutTotal = 0.0
            var opcUaInTotal = 0.0
            var opcUaOutTotal = 0.0
            var brokerDone = false
            var bridgeDone = false
            var opcUaDone = false

            fun tryAssembleAndStore() {
                if (brokerDone && bridgeDone && opcUaDone && nodeMetrics != null) {
                    try {
                        val nm = nodeMetrics!!
                        val brokerMetrics = BrokerMetrics(
                            messagesIn = nm.getDouble("messagesInRate", 0.0),
                            messagesOut = nm.getDouble("messagesOutRate", 0.0),
                            nodeSessionCount = nm.getInteger("nodeSessionCount", 0),
                            clusterSessionCount = sessionHandler.getSessionCount(),
                            queuedMessagesCount = 0L, // TODO: integrate queued messages if needed
                            topicIndexSize = nm.getInteger("topicIndexSize", 0),
                            clientNodeMappingSize = nm.getInteger("clientNodeMappingSize", 0),
                            topicNodeMappingSize = nm.getInteger("topicNodeMappingSize", 0),
                            messageBusIn = nm.getDouble("messageBusInRate", 0.0),
                            messageBusOut = nm.getDouble("messageBusOutRate", 0.0),
                            mqttBridgeIn = bridgeInTotal,
                             mqttBridgeOut = bridgeOutTotal,
                             opcUaIn = opcUaInTotal,
                             opcUaOut = opcUaOutTotal,
                            timestamp = TimestampConverter.instantToIsoString(timestamp)
                        )

                        metricsStore.storeBrokerMetrics(timestamp, nodeId, brokerMetrics).onComplete { result ->
                            if (result.succeeded()) {
                                logger.fine("Stored aggregated broker metrics (bridgeIn=$bridgeInTotal bridgeOut=$bridgeOutTotal) for nodeId: $nodeId")
                            } else {
                                logger.warning("Error storing broker metrics: ${result.cause()?.message}")
                            }
                        }

                        // Store session metrics from nodeMetrics response
                        nm.getJsonArray("sessionMetrics")?.forEach { sessionMetricObj ->
                            val sessionMetric = sessionMetricObj as JsonObject
                            val clientId = sessionMetric.getString("clientId")
                            val messagesInRate = sessionMetric.getDouble("messagesInRate", 0.0)
                            val messagesOutRate = sessionMetric.getDouble("messagesOutRate", 0.0)
                            val sessionMetrics = SessionMetrics(
                                messagesIn = messagesInRate,
                                messagesOut = messagesOutRate,
                                timestamp = TimestampConverter.instantToIsoString(timestamp)
                            )
                            metricsStore.storeSessionMetrics(timestamp, clientId, sessionMetrics).onComplete { res ->
                                if (!res.succeeded()) {
                                    logger.warning("Error storing session metrics for client $clientId: ${res.cause()?.message}")
                                }
                            }
                        }
                    } catch (e: Exception) {
                        logger.warning("Error assembling aggregated broker metrics: ${e.message}")
                    }
                }
            }

            // Broker metrics request (with reset)
            val metricsAddress = EventBusAddresses.Node.metricsAndReset(nodeId)
            vertx.eventBus().request<JsonObject>(metricsAddress, JsonObject()).onComplete { reply ->
                if (reply.succeeded()) {
                    nodeMetrics = reply.result().body()
                } else {
                    logger.warning("Failed to get broker metrics: ${reply.cause()?.message}")
                }
                brokerDone = true
                tryAssembleAndStore()
            }

            // MQTT bridge metrics aggregation
            val listAddress = EventBusAddresses.MqttBridge.CONNECTORS_LIST
            vertx.eventBus().request<JsonObject>(listAddress, JsonObject()).onComplete { listReply ->
                if (listReply.succeeded()) {
                    val body = listReply.result().body()
                    val devices = body.getJsonArray("devices") ?: io.vertx.core.json.JsonArray()
                    if (devices.isEmpty) {
                        bridgeDone = true
                        tryAssembleAndStore()
                    } else {
                        var remaining = devices.size()
                        devices.forEach { d ->
                            val deviceName = d as String
                            val mAddr = EventBusAddresses.MqttBridge.connectorMetrics(deviceName)
                            vertx.eventBus().request<JsonObject>(mAddr, JsonObject()).onComplete { mReply ->
                                if (mReply.succeeded()) {
                                    try {
                                        val m = mReply.result().body()
                                        val inRate = m.getDouble("messagesInRate", 0.0)
                                        val outRate = m.getDouble("messagesOutRate", 0.0)
                                        bridgeInTotal += inRate
                                        bridgeOutTotal += outRate
                                        // Store individual client metrics as before
                                        val mqttMetrics = at.rocworks.extensions.graphql.MqttClientMetrics(
                                            messagesIn = inRate,
                                            messagesOut = outRate,
                                            timestamp = TimestampConverter.instantToIsoString(timestamp)
                                        )
                                        metricsStore.storeMqttClientMetrics(timestamp, deviceName, mqttMetrics)
                                    } catch (e: Exception) {
                                        logger.warning("Error processing MQTT bridge metrics for $deviceName: ${e.message}")
                                    }
                                } else {
                                    logger.fine("No metrics for MQTT bridge $deviceName: ${mReply.cause()?.message}")
                                }
                                remaining -= 1
                                if (remaining == 0) {
                                     bridgeDone = true
                                     tryAssembleAndStore()
                                 }
                             }
                         }
                     }
                 } else {
                     logger.fine("Could not retrieve MQTT bridge connector list: ${listReply.cause()?.message}")
                     bridgeDone = true
                     tryAssembleAndStore()
                 }
             }

             // OPC UA bridge metrics aggregation
             val opcuaListAddress = EventBusAddresses.OpcUaBridge.CONNECTORS_LIST
             vertx.eventBus().request<JsonObject>(opcuaListAddress, JsonObject()).onComplete { listReply ->
                 if (listReply.succeeded()) {
                     val body = listReply.result().body()
                     val devices = body.getJsonArray("devices") ?: io.vertx.core.json.JsonArray()
                     if (devices.isEmpty) {
                         opcUaDone = true
                         tryAssembleAndStore()
                     } else {
                         var remaining = devices.size()
                         devices.forEach { d ->
                             val deviceName = d as String
                             val mAddr = EventBusAddresses.OpcUaBridge.connectorMetrics(deviceName)
                             vertx.eventBus().request<JsonObject>(mAddr, JsonObject()).onComplete { mReply ->
                                 if (mReply.succeeded()) {
                                     try {
                                         val m = mReply.result().body()
                                         val inRate = m.getDouble("messagesInRate", 0.0)
                                         val outRate = m.getDouble("messagesOutRate", 0.0)
                                         opcUaInTotal += inRate
                                         opcUaOutTotal += outRate
                                         // Store individual OPC UA device metrics
                                         val opcUaMetricsJson = JsonObject()
                                             .put("messagesIn", inRate)
                                             .put("messagesOut", outRate)
                                         metricsStore.storeMetrics(at.rocworks.stores.MetricKind.OPCUADEVICE, timestamp, deviceName, opcUaMetricsJson)
                                     } catch (e: Exception) {
                                         logger.warning("Error processing OPC UA metrics for $deviceName: ${e.message}")
                                     }
                                 } else {
                                     logger.fine("No metrics for OPC UA connector $deviceName: ${mReply.cause()?.message}")
                                 }
                                 remaining -= 1
                                 if (remaining == 0) {
                                     opcUaDone = true
                                     tryAssembleAndStore()
                                 }
                             }
                         }
                     }
                 } else {
                     logger.fine("Could not retrieve OPC UA connector list: ${listReply.cause()?.message}")
                     opcUaDone = true
                     tryAssembleAndStore()
                 }
             }
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

    private fun collectMqttBridgeMetrics(timestamp: java.time.Instant) {
        try {
            val listAddress = EventBusAddresses.MqttBridge.CONNECTORS_LIST
            vertx.eventBus().request<io.vertx.core.json.JsonObject>(listAddress, io.vertx.core.json.JsonObject()).onComplete { listReply ->
                if (listReply.succeeded()) {
                    val body = listReply.result().body()
                    val devices = body.getJsonArray("devices") ?: io.vertx.core.json.JsonArray()
                    if (devices.isEmpty) return@onComplete
                    devices.forEach { d ->
                        val deviceName = d as String
                        val metricsAddress = EventBusAddresses.MqttBridge.connectorMetrics(deviceName)
                        vertx.eventBus().request<io.vertx.core.json.JsonObject>(metricsAddress, io.vertx.core.json.JsonObject()).onComplete { mReply ->
                            if (mReply.succeeded()) {
                                try {
                                    val m = mReply.result().body()
                                    val inRate = m.getDouble("messagesInRate", 0.0)
                                    val outRate = m.getDouble("messagesOutRate", 0.0)
                                    val mqttMetrics = at.rocworks.extensions.graphql.MqttClientMetrics(
                                        messagesIn = inRate,
                                        messagesOut = outRate,
                                        timestamp = at.rocworks.extensions.graphql.TimestampConverter.instantToIsoString(timestamp)
                                    )
                                    metricsStore.storeMqttClientMetrics(timestamp, deviceName, mqttMetrics).onComplete { storeRes ->
                                        if (!storeRes.succeeded()) {
                                            logger.warning("Error storing MQTT bridge metrics for $deviceName: ${storeRes.cause()?.message}")
                                        }
                                    }
                                } catch (e: Exception) {
                                    logger.warning("Error processing MQTT bridge metrics for $deviceName: ${e.message}")
                                }
                            } else {
                                logger.fine("No metrics response from $metricsAddress: ${mReply.cause()?.message}")
                            }
                        }
                    }
                } else {
                    logger.fine("Could not retrieve MQTT bridge connector list: ${listReply.cause()?.message}")
                }
            }
        } catch (e: Exception) {
            logger.warning("Error collecting MQTT bridge metrics: ${e.message}")
        }
    }

}