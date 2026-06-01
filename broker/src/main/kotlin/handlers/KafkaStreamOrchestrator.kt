package at.rocworks.handlers

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.IKafkaQueueStore
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.util.concurrent.ConcurrentLinkedQueue

class KafkaStreamOrchestrator(
    private val configJson: JsonObject,
    private val kafkaStore: IKafkaQueueStore,
    private val sessionHandler: SessionHandler,
    private val deviceConfigStore: IDeviceConfigStore? = null
) : AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)
    private val listenerId = "kafka-stream-orchestrator"
    
    // Batching buffer for high performance writes
    private val buffer = ConcurrentLinkedQueue<BrokerMessage>()
    private var writeTimerId: Long? = null

    // Configure stream rules
    private val topicFilters = mutableListOf<String>()
    private val streamRetentionMs = mutableMapOf<String, Long>() // topicFilter -> retentionMs

    override fun start(startPromise: Promise<Void>) {
        try {
            val kafkaServerConfig = configJson.getJsonObject("KafkaServer", JsonObject())
            val streams = kafkaServerConfig.getJsonArray("Streams") ?: kafkaServerConfig.getJsonArray("streams") ?: io.vertx.core.json.JsonArray()

            // 1. Load static/dynamic streams from config
            streams.forEach { stream ->
                val streamObj = stream as JsonObject
                val filter = streamObj.getString("TopicFilter") ?: streamObj.getString("topicFilter")
                val retentionHours = (streamObj.getInteger("RetentionHours") ?: streamObj.getInteger("retentionHours") ?: 168).toLong()

                if (!filter.isNullOrBlank()) {
                    topicFilters.add(filter)
                    streamRetentionMs[filter] = retentionHours * 60 * 60 * 1000L
                    logger.info("Configured Kafka stream mapping: MQTT Topic Filter '$filter' -> Kafka Stream (Pruned after $retentionHours hours)")
                }
            }

            // 2. Load dynamic streams from DeviceConfigStore
            val currentNodeId = Monster.getClusterNodeId(vertx)
            val deviceLoadFuture = if (deviceConfigStore != null) {
                deviceConfigStore.getEnabledDevicesByNode(currentNodeId).map { list ->
                    list.filter { it.type == DeviceConfig.DEVICE_TYPE_KAFKA_STREAM }
                }
            } else {
                Future.succeededFuture(emptyList())
            }

            deviceLoadFuture.onComplete { ar ->
                if (ar.succeeded()) {
                    val dbDevices = ar.result()
                    dbDevices.forEach { dev ->
                        val filter = dev.namespace // namespace acts as the TopicFilter
                        val retentionHours = (dev.config.getInteger("retentionHours") ?: dev.config.getInteger("retentionDays")?.let { it * 24 } ?: 168).toLong()
                        if (filter.isNotBlank() && !topicFilters.contains(filter)) {
                            topicFilters.add(filter)
                            streamRetentionMs[filter] = retentionHours * 60 * 60 * 1000L
                            logger.info("Configured Kafka stream mapping from device '${dev.name}': MQTT Topic Filter '$filter' -> Kafka Stream (Pruned after $retentionHours hours)")
                        }
                    }

                    if (topicFilters.isNotEmpty()) {
                        // Register message listener on session handler to intercept publishes cluster-wide
                        sessionHandler.registerMessageListener(listenerId, topicFilters) { message ->
                            buffer.add(message)
                        }

                        // Batch write timer (every 100ms)
                        writeTimerId = vertx.setPeriodic(100) {
                            flushBuffer()
                        }

                        // Dynamic retention loop (runs every hour)
                        vertx.setPeriodic(1 * 60 * 60 * 1000L) {
                            runRetentionPrune()
                        }

                        // Initial retention prune
                        vertx.setTimer(5000) {
                            runRetentionPrune()
                        }

                        logger.info("Kafka Stream Orchestrator started and subscribed to: $topicFilters")
                    } else {
                        logger.info("No Kafka stream configurations defined. Orchestrator idle.")
                    }
                    startPromise.complete()
                } else {
                    logger.severe("Failed to load Kafka stream devices: ${ar.cause()?.message}")
                    startPromise.fail(ar.cause())
                }
            }
        } catch (e: Exception) {
            logger.severe("Failed to start Kafka Stream Orchestrator: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        try {
            writeTimerId?.let { vertx.cancelTimer(it) }
            sessionHandler.unregisterMessageListener(listenerId)
            flushBuffer() // final flush
            logger.info("Kafka Stream Orchestrator stopped.")
            stopPromise.complete()
        } catch (e: Exception) {
            stopPromise.fail(e)
        }
    }

    private fun flushBuffer() {
        if (buffer.isEmpty()) return

        val batch = mutableListOf<BrokerMessage>()
        while (buffer.isNotEmpty()) {
            buffer.poll()?.let { batch.add(it) }
        }

        if (batch.isNotEmpty()) {
            kafkaStore.enqueue(batch).onFailure { err ->
                logger.severe("Failed to enqueue batch of ${batch.size} messages to Kafka queue: ${err.message}")
            }
        }
    }

    private fun runRetentionPrune() {
        logger.info("Running scheduled Kafka stream retention prune...")
        // Prune logs using the max retention configured across streams
        val maxRetentionMs = streamRetentionMs.values.maxOrNull() ?: (7 * 24 * 60 * 60 * 1000L)
        val olderThanMs = System.currentTimeMillis() - maxRetentionMs

        kafkaStore.pruneExpired(olderThanMs).onComplete { ar ->
            if (ar.succeeded()) {
                val prunedCount = ar.result()
                if (prunedCount > 0) {
                    logger.info("Pruned $prunedCount expired messages from Kafka queue store.")
                }
            } else {
                logger.warning("Failed to prune expired Kafka messages: ${ar.cause()?.message}")
            }
        }
    }
}
