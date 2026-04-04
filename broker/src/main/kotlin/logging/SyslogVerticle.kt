package at.rocworks.logging

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.util.logging.Logger

/**
 * Syslog Verticle - Collects logs from the event bus and stores them in memory
 *
 * This verticle:
 * 1. Listens on the event bus for log entries from SysLogHandler
 * 2. Stores them in the InMemorySyslogStore if enabled
 * 3. Maintains a circular buffer of recent logs for querying
 *
 * Features:
 * - In-memory storage with configurable maximum entries
 * - Thread-safe log collection
 * - Supports filtering logs by level and logger
 */
class SyslogVerticle(
    private val enableSyslogStore: Boolean = true,
    private val maxEntries: Int = 1000
) : AbstractVerticle() {

    private val logger: Logger = Utils.getLogger(SyslogVerticle::class.java)
    private var nodeId: String = "unknown"

    override fun start(startPromise: Promise<Void>) {
        try {
            // Initialize node ID
            initializeNodeId()

            // Initialize the in-memory store if enabled
            if (enableSyslogStore) {
                InMemorySyslogStore.initialize(maxEntries)
                logger.fine("SyslogVerticle started with in-memory store enabled (maxEntries=$maxEntries)")
            } else {
                logger.fine("SyslogVerticle started with in-memory store disabled")
            }

            // Set up event bus consumer for incoming log entries on node-specific address
            val nodeSpecificAddress = EventBusAddresses.Syslog.logsForNode(nodeId)
            vertx.eventBus().consumer<JsonObject>(nodeSpecificAddress) { message ->
                handleLogEntry(message.body())
            }
            logger.fine("SyslogVerticle listening on address: $nodeSpecificAddress")

            logger.fine("SyslogVerticle initialized successfully")
            startPromise.complete()
        } catch (e: Exception) {
            logger.severe("Failed to start SyslogVerticle: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        try {
            logger.fine("SyslogVerticle stopping")
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
            if (enableSyslogStore) {
                InMemorySyslogStore.addLog(logData)
            }
        } catch (e: Exception) {
            logger.warning("Error handling log entry: ${e.message}")
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
