package at.rocworks.handlers

import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.IKafkaQueueStore
import at.rocworks.stores.KafkaMultiplexingQueueStore
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Message UUIDs of records the Kafka protocol server has already stored itself (Produce).
 * The orchestrator skips them, otherwise a produced record whose MQTT topic matches a stream
 * filter would be stored a second time when the MQTT publish is captured.
 *
 * Entries expire after a short time instead of being removed on first use, because several
 * Kafka servers (each with its own orchestrator) may run in the same JVM.
 */
object KafkaProducedMessages {
    private const val TTL_MS = 30_000L
    private val uuids = ConcurrentHashMap<String, Long>()

    fun register(uuid: String) {
        val now = System.currentTimeMillis()
        if (uuids.size > 1000) uuids.entries.removeIf { it.value < now }
        uuids[uuid] = now + TTL_MS
    }

    fun isRegistered(uuid: String): Boolean {
        val expires = uuids[uuid] ?: return false
        if (expires < System.currentTimeMillis()) {
            uuids.remove(uuid)
            return false
        }
        return true
    }
}

class KafkaStreamOrchestrator(
    private val configJson: JsonObject,
    private val kafkaStore: IKafkaQueueStore,
    private val sessionHandler: SessionHandler,
    @Suppress("unused") private val deviceConfigStore: IDeviceConfigStore? = null
) : AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)
    private val listenerId = "kafka-stream-orchestrator-" + (configJson.getJsonObject("KafkaServer")?.getString("name") ?: java.util.UUID.randomUUID().toString())

    // Batching buffer for high performance writes
    private val buffer = ConcurrentLinkedQueue<BrokerMessage>()
    private var writeTimerId: Long? = null
    private var pruneTimerId: Long? = null
    private var initialPruneTimerId: Long? = null

    private val topicFilters = mutableListOf<String>()

    override fun start(startPromise: Promise<Void>) {
        try {
            val kafkaServerConfig = configJson.getJsonObject("KafkaServer", JsonObject())
            val streams = kafkaServerConfig.getJsonArray("Streams") ?: kafkaServerConfig.getJsonArray("streams") ?: io.vertx.core.json.JsonArray()

            streams.forEach { stream ->
                val streamObj = stream as JsonObject
                val filter = streamObj.getString("TopicFilter") ?: streamObj.getString("topicFilter")
                if (!filter.isNullOrBlank() && !topicFilters.contains(filter)) {
                    topicFilters.add(filter)
                    logger.info("Configured Kafka stream mapping: MQTT Topic Filter '$filter' -> Kafka Stream")
                }
            }

            if (topicFilters.isNotEmpty()) {
                // Register message listener on session handler to intercept publishes cluster-wide
                sessionHandler.registerMessageListener(listenerId, topicFilters) { message ->
                    if (!KafkaProducedMessages.isRegistered(message.messageUuid)) {
                        buffer.add(message)
                    }
                }

                // Batch write timer (every 100ms)
                writeTimerId = vertx.setPeriodic(100) { flushBuffer() }

                // Retention loop (every hour) and an initial prune shortly after start
                pruneTimerId = vertx.setPeriodic(60 * 60 * 1000L) { runRetentionPrune() }
                initialPruneTimerId = vertx.setTimer(5000) { runRetentionPrune() }

                logger.info("Kafka Stream Orchestrator started and subscribed to: $topicFilters")
            } else {
                logger.info("No Kafka stream configurations defined. Orchestrator idle.")
            }
            startPromise.complete()
        } catch (e: Exception) {
            logger.severe("Failed to start Kafka Stream Orchestrator: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        try {
            writeTimerId?.let { vertx.cancelTimer(it) }
            pruneTimerId?.let { vertx.cancelTimer(it) }
            initialPruneTimerId?.let { vertx.cancelTimer(it) }
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
        while (true) {
            batch.add(buffer.poll() ?: break)
        }

        if (batch.isNotEmpty()) {
            // The store notifies long-polling consumers (per stream name) after a successful write
            kafkaStore.enqueue(batch).onFailure { err ->
                logger.severe("Failed to enqueue batch of ${batch.size} messages to Kafka queue: ${err.message}")
            }
        }
    }

    private fun runRetentionPrune() {
        logger.info("Running scheduled Kafka stream retention prune...")
        val prune = if (kafkaStore is KafkaMultiplexingQueueStore) {
            // Every stream is pruned with its own retention
            kafkaStore.pruneByRetention()
        } else {
            kafkaStore.pruneExpired(System.currentTimeMillis() - 7 * 24 * 60 * 60 * 1000L)
        }
        prune.onComplete { ar ->
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
