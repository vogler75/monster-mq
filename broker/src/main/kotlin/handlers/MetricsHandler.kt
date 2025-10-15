package handlers

import at.rocworks.Const
import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.bus.IMessageBus
import at.rocworks.data.BrokerMessage
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
    private val messageBus: IMessageBus,
    private val messageHandler: at.rocworks.handlers.MessageHandler,
    private val collectionIntervalSeconds: Int = 1,
    private val retentionHours: Int = 24
) : AbstractVerticle() {
    companion object {
        private val logger: Logger = Utils.getLogger(MetricsHandler::class.java)
        private const val SYSTEM_CLIENT_ID = Const.SYS_TOPIC_NAME
        private const val PURGE_INTERVAL_HOURS = 1
    }

    private val collectionIntervalMs = collectionIntervalSeconds * 1000L
    private val purgeIntervalMs = PURGE_INTERVAL_HOURS * 60 * 60 * 1000L

    /**
     * Publish metrics to a $SYS topic (non-retained)
     */
    private fun publishMetrics(topic: String, payload: JsonObject) {
        try {
            val baseMessage = BrokerMessage(SYSTEM_CLIENT_ID, topic, payload.encode())
            val message = BrokerMessage(
                messageUuid = baseMessage.messageUuid,
                messageId = 0,
                topicName = topic,
                payload = payload.encode().toByteArray(),
                qosLevel = 0,
                isRetain = false,  // Metrics are not retained
                isDup = false,
                isQueued = false,
                clientId = SYSTEM_CLIENT_ID,
                noLog = true  // Don't log $SYS metrics to prevent infinite recursion
            )
            // Publish via SessionHandler for proper targeted distribution to subscribers only
            sessionHandler.publishMessage(message)
            logger.finest { "Published metrics to $topic" }
        } catch (e: Exception) {
            logger.warning("Error publishing metrics to $topic: ${e.message}")
        }
    }


    override fun start(startPromise: Promise<Void>) {
        metricsStore.startStore(vertx).onComplete { storeResult ->
            if (storeResult.succeeded()) {
                // Perform initial purge on startup
                performPurge()

                // Start periodic metrics collection
                vertx.setPeriodic(collectionIntervalMs) { _ ->
                    collectAndStoreMetrics()
                }

                // Start periodic metrics purging (every hour)
                vertx.setPeriodic(purgeIntervalMs) { _ ->
                    performPurge()
                }

                logger.info("MetricsCollector started with ${collectionIntervalSeconds}s (${collectionIntervalMs}ms) interval, retention: ${retentionHours}h")
                startPromise.complete()
            } else {
                logger.severe("Failed to start metrics collector: ${storeResult.cause()?.message}")
                startPromise.fail(storeResult.cause())
            }
        }
    }

    private fun performPurge() {
        try {
            val cutoffTime = Instant.now().minusSeconds(retentionHours * 3600L)
            logger.info("Purging metrics older than $cutoffTime (retention: ${retentionHours}h)")

            metricsStore.purgeOldMetrics(cutoffTime).onComplete { result ->
                if (result.succeeded()) {
                    val deletedCount = result.result()
                    logger.info("Successfully purged $deletedCount old metric records")
                } else {
                    logger.warning("Failed to purge old metrics: ${result.cause()?.message}")
                }
            }
        } catch (e: Exception) {
            logger.warning("Error during metrics purge: ${e.message}")
        }
    }

    private fun collectAndStoreMetrics() {
        try {
            val timestamp = Instant.now()
            val nodeId = Monster.Companion.getClusterNodeId(vertx)
            logger.fine { "Collecting metrics at $timestamp for nodeId: $nodeId (concurrent aggregation)" }

            // Placeholders for concurrent aggregation
            var nodeMetrics: JsonObject? = null
            var bridgeInTotal = 0.0
            var bridgeOutTotal = 0.0
            var opcUaClientInTotal = 0.0
            var opcUaClientOutTotal = 0.0
            var kafkaInTotal = 0.0
            var kafkaOutTotal = 0.0
            var winCCOaClientInTotal = 0.0
            var winCCUaClientInTotal = 0.0
            var neo4jClientInTotal = 0.0
            var brokerDone = false
            var bridgeDone = false
            var opcUaDone = false
            var kafkaDone = false
            var winCCOaDone = false
            var winCCUaDone = false
            var neo4jDone = false
            var archiveDone = false

            fun tryAssembleAndStore() {
                if (brokerDone && bridgeDone && opcUaDone && kafkaDone && winCCOaDone && winCCUaDone && neo4jDone && archiveDone && nodeMetrics != null) {
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
                            mqttClientIn = bridgeInTotal,
                            mqttClientOut = bridgeOutTotal,
                            opcUaClientIn = opcUaClientInTotal,
                            opcUaClientOut = opcUaClientOutTotal,
                            kafkaClientIn = kafkaInTotal,
                            kafkaClientOut = kafkaOutTotal,
                            winCCOaClientIn = winCCOaClientInTotal,
                            winCCUaClientIn = winCCUaClientInTotal,
                            neo4jClientIn = neo4jClientInTotal,
                            timestamp = TimestampConverter.instantToIsoString(timestamp)
                        )

                        metricsStore.storeBrokerMetrics(timestamp, nodeId, brokerMetrics).onComplete { result ->
                            if (result.succeeded()) {
                                    logger.fine { "Stored aggregated broker metrics (bridgeIn=$bridgeInTotal bridgeOut=$bridgeOutTotal kafkaIn=$kafkaInTotal winCCOaIn=$winCCOaClientInTotal winCCUaIn=$winCCUaClientInTotal neo4jIn=$neo4jClientInTotal) for nodeId: $nodeId" }
                            } else {
                                logger.warning("Error storing broker metrics: ${result.cause()?.message}")
                            }
                        }

                        // Publish broker metrics to $SYS topic
                        val brokerMetricsJson = JsonObject()
                            .put("messagesIn", brokerMetrics.messagesIn)
                            .put("messagesOut", brokerMetrics.messagesOut)
                            .put("nodeSessionCount", brokerMetrics.nodeSessionCount)
                            .put("clusterSessionCount", brokerMetrics.clusterSessionCount)
                            .put("queuedMessagesCount", brokerMetrics.queuedMessagesCount)
                            .put("topicIndexSize", brokerMetrics.topicIndexSize)
                            .put("clientNodeMappingSize", brokerMetrics.clientNodeMappingSize)
                            .put("topicNodeMappingSize", brokerMetrics.topicNodeMappingSize)
                            .put("messageBusIn", brokerMetrics.messageBusIn)
                            .put("messageBusOut", brokerMetrics.messageBusOut)
                            .put("mqttClientIn", brokerMetrics.mqttClientIn)
                            .put("mqttClientOut", brokerMetrics.mqttClientOut)
                            .put("opcUaClientIn", brokerMetrics.opcUaClientIn)
                            .put("opcUaClientOut", brokerMetrics.opcUaClientOut)
                            .put("kafkaClientIn", brokerMetrics.kafkaClientIn)
                            .put("kafkaClientOut", brokerMetrics.kafkaClientOut)
                            .put("winCCOaClientIn", brokerMetrics.winCCOaClientIn)
                            .put("winCCUaClientIn", brokerMetrics.winCCUaClientIn)
                            .put("neo4jClientIn", brokerMetrics.neo4jClientIn)
                            .put("timestamp", brokerMetrics.timestamp)
                        publishMetrics("${Const.SYS_TOPIC_NAME}/brokers/$nodeId/metrics", brokerMetricsJson)

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

                            // Publish session metrics to $SYS topic with connection statistics
                            val sessionMetricsJson = JsonObject()
                                .put("messagesIn", messagesInRate)
                                .put("messagesOut", messagesOutRate)
                                .put("timestamp", TimestampConverter.instantToIsoString(timestamp))

                            // Add connection statistics if available
                            if (sessionMetric.containsKey("connected")) {
                                sessionMetricsJson.put("connected", sessionMetric.getBoolean("connected", false))
                            }
                            if (sessionMetric.containsKey("lastPing")) {
                                sessionMetricsJson.put("lastPing", sessionMetric.getString("lastPing", ""))
                            }
                            if (sessionMetric.containsKey("inFlightMessagesRcv")) {
                                sessionMetricsJson.put("inFlightMessagesRcv", sessionMetric.getInteger("inFlightMessagesRcv", 0))
                            }
                            if (sessionMetric.containsKey("inFlightMessagesSnd")) {
                                sessionMetricsJson.put("inFlightMessagesSnd", sessionMetric.getInteger("inFlightMessagesSnd", 0))
                            }

                            publishMetrics("${Const.SYS_TOPIC_NAME}/sessions/mqtt/${clientId}/metrics", sessionMetricsJson)
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

                                        // Publish MQTT bridge metrics to $SYS topic
                                        val mqttMetricsJson = JsonObject()
                                            .put("messagesIn", inRate)
                                            .put("messagesOut", outRate)
                                            .put("timestamp", TimestampConverter.instantToIsoString(timestamp))
                                        publishMetrics("${Const.SYS_TOPIC_NAME}/sessions/mqtt-bridge/${deviceName}/metrics", mqttMetricsJson)
                                    } catch (e: Exception) {
                                        logger.warning("Error processing MQTT bridge metrics for $deviceName: ${e.message}")
                                    }
                                } else {
                                    logger.fine { "No metrics for MQTT bridge $deviceName: ${mReply.cause()?.message}" }
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
                     logger.fine { "Could not retrieve MQTT bridge connector list: ${listReply.cause()?.message}" }
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
                                         opcUaClientInTotal += inRate
                                         opcUaClientOutTotal += outRate
                                         // Store individual OPC UA device metrics
                                         val opcUaMetricsJson = JsonObject()
                                             .put("messagesIn", inRate)
                                             .put("messagesOut", outRate)
                                             .put("timestamp", TimestampConverter.instantToIsoString(timestamp))
                                         metricsStore.storeMetrics(at.rocworks.stores.MetricKind.OPCUADEVICE, timestamp, deviceName, opcUaMetricsJson)

                                         // Publish OPC UA session metrics to $SYS topic
                                         publishMetrics("${Const.SYS_TOPIC_NAME}/sessions/opcua/${deviceName}/metrics", opcUaMetricsJson)
                                     } catch (e: Exception) {
                                         logger.warning("Error processing OPC UA metrics for $deviceName: ${e.message}")
                                     }
                                 } else {
                                     logger.fine { "No metrics for OPC UA connector $deviceName: ${mReply.cause()?.message}" }
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
                     logger.fine { "Could not retrieve OPC UA connector list: ${listReply.cause()?.message}" }
                     opcUaDone = true
                     tryAssembleAndStore()
                 }
             }

            // Kafka bridge metrics aggregation
            val kafkaListAddress = EventBusAddresses.KafkaBridge.CONNECTORS_LIST
            vertx.eventBus().request<JsonObject>(kafkaListAddress, JsonObject()).onComplete { listReply ->
                if (listReply.succeeded()) {
                    val body = listReply.result().body()
                    val devices = body.getJsonArray("devices") ?: io.vertx.core.json.JsonArray()
                    if (devices.isEmpty) {
                        kafkaDone = true
                        tryAssembleAndStore()
                    } else {
                        var remaining = devices.size()
                        devices.forEach { d ->
                            val deviceName = d as String
                            val mAddr = EventBusAddresses.KafkaBridge.connectorMetrics(deviceName)
                            vertx.eventBus().request<JsonObject>(mAddr, JsonObject()).onComplete { mReply ->
                                if (mReply.succeeded()) {
                                    try {
                                        val m = mReply.result().body()
                                         val inRate = m.getDouble("messagesInRate", m.getDouble("ratePerSec", 0.0))
                                         val outRate = m.getDouble("messagesOutRate", 0.0)
                                         kafkaInTotal += inRate
                                         kafkaOutTotal += outRate
                                         // Store individual Kafka client metrics
                                         val kafkaMetrics = at.rocworks.extensions.graphql.KafkaClientMetrics(
                                             messagesIn = inRate,
                                             messagesOut = outRate,
                                             timestamp = TimestampConverter.instantToIsoString(timestamp)
                                         )
                                         metricsStore.storeKafkaClientMetrics(timestamp, deviceName, kafkaMetrics)

                                         // Publish Kafka session metrics to $SYS topic
                                         val kafkaMetricsJson = JsonObject()
                                             .put("messagesIn", inRate)
                                             .put("messagesOut", outRate)
                                             .put("timestamp", TimestampConverter.instantToIsoString(timestamp))
                                         publishMetrics("${Const.SYS_TOPIC_NAME}/sessions/kafka/${deviceName}/metrics", kafkaMetricsJson)
                                    } catch (e: Exception) {
                                        logger.warning("Error processing Kafka client metrics for $deviceName: ${e.message}")
                                    }
                                } else {
                                    logger.fine { "No metrics for Kafka client $deviceName: ${mReply.cause()?.message}" }
                                }
                                remaining -= 1
                                if (remaining == 0) {
                                    kafkaDone = true
                                    tryAssembleAndStore()
                                }
                            }
                        }
                    }
                } else {
                    logger.fine { "Could not retrieve Kafka client connector list: ${listReply.cause()?.message}" }
                    kafkaDone = true
                    tryAssembleAndStore()
                }
            }

            // WinCC OA Client metrics aggregation
            val winCCOaListAddress = EventBusAddresses.WinCCOaBridge.CONNECTORS_LIST
            vertx.eventBus().request<JsonObject>(winCCOaListAddress, JsonObject()).onComplete { listReply ->
                if (listReply.succeeded()) {
                    val body = listReply.result().body()
                    val devices = body.getJsonArray("devices") ?: io.vertx.core.json.JsonArray()
                    logger.fine { "WinCC OA metrics aggregation: found ${devices.size()} devices" }
                    if (devices.isEmpty) {
                        winCCOaDone = true
                        tryAssembleAndStore()
                    } else {
                        var remaining = devices.size()
                        devices.forEach { d ->
                            val deviceName = d as String
                            val mAddr = EventBusAddresses.WinCCOaBridge.connectorMetrics(deviceName)
                            vertx.eventBus().request<JsonObject>(mAddr, JsonObject()).onComplete { mReply ->
                                if (mReply.succeeded()) {
                                    try {
                                        val m = mReply.result().body()
                                        val inRate = m.getDouble("messagesInRate", 0.0)
                                        val connected = m.getBoolean("connected", false)
                                        logger.fine { "WinCC OA client '$deviceName' metrics: messagesInRate=$inRate connected=$connected" }
                                        winCCOaClientInTotal += inRate
                                        // Store individual client metrics
                                        val winCCOaMetrics = at.rocworks.extensions.graphql.WinCCOaClientMetrics(
                                            messagesIn = inRate,
                                            connected = connected,
                                            timestamp = TimestampConverter.instantToIsoString(timestamp)
                                        )
                                        metricsStore.storeWinCCOaClientMetrics(timestamp, deviceName, winCCOaMetrics)

                                        // Publish WinCC OA session metrics to $SYS topic
                                        val winCCOaMetricsJson = JsonObject()
                                            .put("messagesIn", inRate)
                                            .put("connected", connected)
                                            .put("timestamp", TimestampConverter.instantToIsoString(timestamp))
                                        publishMetrics("${Const.SYS_TOPIC_NAME}/sessions/winccoa/${deviceName}/metrics", winCCOaMetricsJson)
                                    } catch (e: Exception) {
                                        logger.warning("Error processing WinCC OA client metrics for $deviceName: ${e.message}")
                                    }
                                } else {
                                    logger.fine { "No metrics for WinCC OA client $deviceName: ${mReply.cause()?.message}" }
                                }
                                remaining -= 1
                                if (remaining == 0) {
                                    logger.fine { "WinCC OA metrics collection complete. Final total: $winCCOaClientInTotal" }
                                    winCCOaDone = true
                                    tryAssembleAndStore()
                                }
                            }
                        }
                    }
                } else {
                    logger.fine { "Could not retrieve WinCC OA client connector list: ${listReply.cause()?.message}" }
                    winCCOaDone = true
                    tryAssembleAndStore()
                }
            }

            // WinCC Unified (UA) Client metrics aggregation
            val winCCUaListAddress = EventBusAddresses.WinCCUaBridge.CONNECTORS_LIST
            vertx.eventBus().request<JsonObject>(winCCUaListAddress, JsonObject()).onComplete { listReply ->
                if (listReply.succeeded()) {
                    val body = listReply.result().body()
                    val devices = body.getJsonArray("devices") ?: io.vertx.core.json.JsonArray()
                    logger.fine { "WinCC Unified metrics aggregation: found ${devices.size()} devices" }
                    if (devices.isEmpty) {
                        winCCUaDone = true
                        tryAssembleAndStore()
                    } else {
                        var remaining = devices.size()
                        devices.forEach { d ->
                            val deviceName = d as String
                            val mAddr = EventBusAddresses.WinCCUaBridge.connectorMetrics(deviceName)
                            vertx.eventBus().request<JsonObject>(mAddr, JsonObject()).onComplete { mReply ->
                                if (mReply.succeeded()) {
                                    try {
                                        val m = mReply.result().body()
                                        val inRate = m.getDouble("messagesInRate", 0.0)
                                        val connected = m.getBoolean("connected", false)
                                        logger.fine { "WinCC Unified client '$deviceName' metrics: messagesInRate=$inRate connected=$connected" }
                                        winCCUaClientInTotal += inRate
                                        // Store individual client metrics
                                        val winCCUaMetrics = at.rocworks.extensions.graphql.WinCCUaClientMetrics(
                                            messagesIn = inRate,
                                            connected = connected,
                                            timestamp = TimestampConverter.instantToIsoString(timestamp)
                                        )
                                        metricsStore.storeWinCCUaClientMetrics(timestamp, deviceName, winCCUaMetrics)

                                        // Publish WinCC UA session metrics to $SYS topic
                                        val winCCUaMetricsJson = JsonObject()
                                            .put("messagesIn", inRate)
                                            .put("connected", connected)
                                            .put("timestamp", TimestampConverter.instantToIsoString(timestamp))
                                        publishMetrics("${Const.SYS_TOPIC_NAME}/sessions/winccua/${deviceName}/metrics", winCCUaMetricsJson)
                                    } catch (e: Exception) {
                                        logger.warning("Error processing WinCC Unified client metrics for $deviceName: ${e.message}")
                                    }
                                } else {
                                    logger.fine { "No metrics for WinCC Unified client $deviceName: ${mReply.cause()?.message}" }
                                }
                                remaining -= 1
                                if (remaining == 0) {
                                    logger.fine { "WinCC Unified metrics collection complete. Final total: $winCCUaClientInTotal" }
                                    winCCUaDone = true
                                    tryAssembleAndStore()
                                }
                            }
                        }
                    }
                } else {
                    logger.fine { "Could not retrieve WinCC Unified client connector list: ${listReply.cause()?.message}" }
                    winCCUaDone = true
                    tryAssembleAndStore()
                }
            }

            // Neo4j Client metrics aggregation
            val neo4jListAddress = EventBusAddresses.Neo4jBridge.CONNECTORS_LIST
            vertx.eventBus().request<JsonObject>(neo4jListAddress, JsonObject()).onComplete { listReply ->
                if (listReply.succeeded()) {
                    val body = listReply.result().body()
                    val devices = body.getJsonArray("devices") ?: io.vertx.core.json.JsonArray()
                    logger.fine { "Neo4j metrics aggregation: found ${devices.size()} devices" }
                    if (devices.isEmpty) {
                        neo4jDone = true
                        tryAssembleAndStore()
                    } else {
                        var remaining = devices.size()
                        devices.forEach { d ->
                            val deviceName = d as String
                            val mAddr = EventBusAddresses.Neo4jBridge.connectorMetrics(deviceName)
                            vertx.eventBus().request<JsonObject>(mAddr, JsonObject()).onComplete { mReply ->
                                if (mReply.succeeded()) {
                                    try {
                                        val m = mReply.result().body()
                                        val inRate = m.getDouble("messagesInRate", 0.0)
                                        logger.fine { "Neo4j client '$deviceName' metrics: messagesInRate=$inRate" }
                                        neo4jClientInTotal += inRate
                                        // Store individual client metrics
                                        val neo4jMetricsJson = JsonObject()
                                            .put("messagesIn", m.getDouble("messagesIn", 0.0))
                                            .put("messagesWritten", m.getDouble("messagesWritten", 0.0))
                                            .put("messagesSuppressed", m.getDouble("messagesSuppressed", 0.0))
                                            .put("errors", m.getDouble("errors", 0.0))
                                            .put("pathQueueSize", m.getInteger("pathQueueSize", 0))
                                            .put("messagesInRate", inRate)
                                            .put("messagesWrittenRate", m.getDouble("messagesWrittenRate", 0.0))
                                            .put("timestamp", TimestampConverter.instantToIsoString(timestamp))
                                        metricsStore.storeMetrics(at.rocworks.stores.MetricKind.NEO4JCLIENT, timestamp, deviceName, neo4jMetricsJson)

                                        // Publish Neo4j session metrics to $SYS topic
                                        publishMetrics("${Const.SYS_TOPIC_NAME}/sessions/neo4j/${deviceName}/metrics", neo4jMetricsJson)
                                    } catch (e: Exception) {
                                        logger.warning("Error processing Neo4j client metrics for $deviceName: ${e.message}")
                                    }
                                } else {
                                    logger.fine { "No metrics for Neo4j client $deviceName: ${mReply.cause()?.message}" }
                                }
                                remaining -= 1
                                if (remaining == 0) {
                                    logger.fine { "Neo4j metrics collection complete. Final total: $neo4jClientInTotal" }
                                    neo4jDone = true
                                    tryAssembleAndStore()
                                }
                            }
                        }
                    }
                } else {
                    logger.fine { "Could not retrieve Neo4j client connector list: ${listReply.cause()?.message}" }
                    neo4jDone = true
                    tryAssembleAndStore()
                }
            }

            // Archive group metrics aggregation
            val archiveListAddress = EventBusAddresses.Archive.GROUPS_LIST
            vertx.eventBus().request<JsonObject>(archiveListAddress, JsonObject()).onComplete { listReply ->
                if (listReply.succeeded()) {
                    val body = listReply.result().body()
                    val groups = body.getJsonArray("groups") ?: io.vertx.core.json.JsonArray()
                    if (groups.isEmpty) {
                        archiveDone = true
                        tryAssembleAndStore()
                    } else {
                        var remaining = groups.size()
                        groups.forEach { g ->
                            val groupName = g as String
                            val mAddr = EventBusAddresses.Archive.groupMetrics(groupName)
                            vertx.eventBus().request<JsonObject>(mAddr, JsonObject()).onComplete { mReply ->
                                if (mReply.succeeded()) {
                                    try {
                                        val m = mReply.result().body()
                                        val messagesOut = m.getDouble("messagesOut", 0.0)
                                        val bufferSize = m.getInteger("bufferSize", 0)
                                        // Store individual archive group metrics
                                        val archiveMetricsJson = JsonObject()
                                            .put("messagesOut", messagesOut)
                                            .put("bufferSize", bufferSize)
                                            .put("timestamp", TimestampConverter.instantToIsoString(timestamp))
                                        metricsStore.storeMetrics(at.rocworks.stores.MetricKind.ARCHIVEGROUP, timestamp, groupName, archiveMetricsJson)

                                        // Publish archive group metrics to $SYS topic
                                        publishMetrics("${Const.SYS_TOPIC_NAME}/archives/${groupName}/metrics", archiveMetricsJson)
                                    } catch (e: Exception) {
                                        logger.warning("Error processing archive group metrics for $groupName: ${e.message}")
                                    }
                                } else {
                                    logger.fine { "No metrics for archive group $groupName: ${mReply.cause()?.message}" }
                                }
                                remaining -= 1
                                if (remaining == 0) {
                                    archiveDone = true
                                    tryAssembleAndStore()
                                }
                            }
                        }
                    }
                } else {
                    logger.fine { "Could not retrieve archive groups list: ${listReply.cause()?.message}" }
                    archiveDone = true
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

            logger.fine { "Requesting broker metrics with reset from address: $metricsAddress" }

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
                            mqttClientIn = 0.0,
                            mqttClientOut = 0.0,
                            opcUaClientIn = 0.0,
                            opcUaClientOut = 0.0,
                            kafkaClientIn = 0.0,
                            kafkaClientOut = 0.0,
                            timestamp = TimestampConverter.instantToIsoString(timestamp)
                        )

                        metricsStore.storeBrokerMetrics(timestamp, nodeId, brokerMetrics).onComplete { result ->
                            if (result.succeeded()) {
                                logger.fine { "Successfully stored broker metrics for nodeId: $nodeId" }
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
                                        logger.fine { "Successfully stored session metrics for client: $clientId" }
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

            logger.fine { "Collecting session metrics for ${allClientMetrics.size} clients (with reset)" }

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
                                logger.fine { "No metrics response from $metricsAddress: ${mReply.cause()?.message}" }
                            }
                        }
                    }
                } else {
                    logger.fine { "Could not retrieve MQTT bridge connector list: ${listReply.cause()?.message}" }
                }
            }
        } catch (e: Exception) {
            logger.warning("Error collecting MQTT bridge metrics: ${e.message}")
        }
    }

}