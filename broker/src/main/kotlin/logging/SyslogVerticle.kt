package at.rocworks.logging

import at.rocworks.Const
import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.handlers.MessageHandler
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.logging.Logger

/**
 * Syslog Verticle - Collects logs from the event bus and publishes them to MQTT
 *
 * This verticle:
 * 1. Listens on the event bus for log entries from MqttLogHandler
 * 2. Stores them in the InMemorySyslogStore if enabled
 * 3. Publishes them to MQTT topics ($SYS/syslogs/...)
 * 4. Maintains a circular buffer of recent logs for querying
 *
 * Features:
 * - In-memory storage with configurable maximum entries
 * - Optional persistence to MQTT
 * - Thread-safe log collection
 * - Supports filtering logs by level and logger
 */
class SyslogVerticle(
    private val enableSyslogStore: Boolean = true,
    private val maxEntries: Int = 1000,
    private val messageHandler: MessageHandler? = null
) : AbstractVerticle() {

    private val logger: Logger = Utils.getLogger(SyslogVerticle::class.java)
    private var nodeId: String = "unknown"

    companion object {
        const val LOG_TOPIC_PREFIX = "${Const.SYS_TOPIC_NAME}/${Const.LOG_TOPIC_NAME}"
    }

    override fun start(startPromise: Promise<Void>) {
        try {
            // Initialize node ID
            initializeNodeId()

            // Initialize the in-memory store if enabled
            if (enableSyslogStore) {
                InMemorySyslogStore.initialize(maxEntries)
                logger.info("SyslogVerticle started with in-memory store enabled (maxEntries=$maxEntries)")
            } else {
                logger.info("SyslogVerticle started with in-memory store disabled")
            }

            // Set up event bus consumer for incoming log entries
            vertx.eventBus().consumer<JsonObject>(EventBusAddresses.Syslog.LOGS) { message ->
                handleLogEntry(message.body())
            }

            logger.info("SyslogVerticle initialized successfully")
            startPromise.complete()
        } catch (e: Exception) {
            logger.severe("Failed to start SyslogVerticle: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        try {
            logger.info("SyslogVerticle stopping")
            stopPromise.complete()
        } catch (e: Exception) {
            logger.warning("Error stopping SyslogVerticle: ${e.message}")
            stopPromise.complete()
        }
    }

    /**
     * Handle incoming log entries from the event bus
     */
    private fun handleLogEntry(logData: JsonObject) {
        try {
            // Add to in-memory store if enabled
            if (enableSyslogStore) {
                InMemorySyslogStore.addLog(logData)
            }

            // Publish to MQTT
            publishToMqtt(logData)
        } catch (e: Exception) {
            logger.warning("Error handling log entry: ${e.message}")
        }
    }

    /**
     * Publish log entry to MQTT topics
     */
    private fun publishToMqtt(logData: JsonObject) {
        try {
            // Extract log information
            val levelName = logData.getString("level", "UNKNOWN").lowercase()
            val logNodeId = logData.getString("node", nodeId)

            // Create MQTT topic
            val topic = "$LOG_TOPIC_PREFIX/$logNodeId/$levelName"

            // Create MQTT message
            val message = BrokerMessage(
                messageId = 0,
                topicName = topic,
                payload = logData.encode().toByteArray(),
                qosLevel = 0,
                isRetain = false,
                isDup = false,
                isQueued = false,
                clientId = "syslog-verticle-$nodeId",
                time = Instant.now(),
                senderId = "syslog-verticle-$nodeId"
            )

            // Save and broadcast message via message handler (only this node publishes)
            messageHandler?.let { handler ->
                handler.saveMessage(message).onFailure { e ->
                    logger.warning("Failed to save log message: ${e.message}")
                }
            }

            // Broadcast to all nodes in cluster (so all subscribers receive it)
            vertx.eventBus().publish(EventBusAddresses.Cluster.BROADCAST, message)
        } catch (e: Exception) {
            logger.warning("Error publishing log to MQTT: ${e.message}")
        }
    }

    /**
     * Initialize the node ID from the cluster
     */
    private fun initializeNodeId() {
        try {
            nodeId = Monster.getClusterNodeId(vertx)
        } catch (e: Exception) {
            nodeId = "unknown"
            logger.warning("Failed to get cluster node ID, using 'unknown': ${e.message}")
        }
    }
}
