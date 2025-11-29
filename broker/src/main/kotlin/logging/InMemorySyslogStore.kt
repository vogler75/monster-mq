package at.rocworks.logging

import at.rocworks.Utils
import io.vertx.core.json.JsonObject
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.logging.Logger

/**
 * In-memory store for system logs with fixed capacity
 *
 * This singleton maintains a circular buffer of system logs with a configurable
 * maximum number of entries. When the buffer reaches capacity, oldest entries
 * are automatically removed.
 *
 * Thread-safe implementation using ConcurrentLinkedDeque.
 */
object InMemorySyslogStore {
    private val logger: Logger = Utils.getLogger(InMemorySyslogStore::class.java)
    private val logs: ConcurrentLinkedDeque<JsonObject> = ConcurrentLinkedDeque()
    private var maxEntries: Int = 1000

    /**
     * Initialize the store with a maximum number of entries
     */
    fun initialize(maxEntries: Int) {
        this.maxEntries = if (maxEntries > 0) maxEntries else 1000
        logs.clear()
        logger.fine("InMemorySyslogStore initialized with maxEntries=$maxEntries")
    }

    /**
     * Add a log entry to the store
     * If the store reaches maxEntries, the oldest entry is removed
     */
    fun addLog(logEntry: JsonObject) {
        try {
            logs.addLast(logEntry)

            // Remove oldest entry if we exceed maxEntries
            if (logs.size > maxEntries) {
                logs.removeFirst()
            }
        } catch (e: Exception) {
            logger.warning("Error adding log to InMemorySyslogStore: ${e.message}")
        }
    }

    /**
     * Get all stored logs
     */
    fun getAllLogs(): List<JsonObject> {
        return logs.toList()
    }

    /**
     * Get the last N logs
     */
    fun getLastLogs(count: Int): List<JsonObject> {
        val allLogs = logs.toList()
        return if (count <= 0) emptyList() else allLogs.takeLast(count)
    }

    /**
     * Get logs filtered by level
     */
    fun getLogsByLevel(level: String): List<JsonObject> {
        return logs.filter { it.getString("level") == level }
    }

    /**
     * Get logs filtered by logger name
     */
    fun getLogsByLogger(loggerName: String): List<JsonObject> {
        return logs.filter { it.getString("logger")?.contains(loggerName) == true }
    }

    /**
     * Clear all logs
     */
    fun clear() {
        logs.clear()
        logger.fine("InMemorySyslogStore cleared")
    }

    /**
     * Get the current number of stored logs
     */
    fun getSize(): Int = logs.size

    /**
     * Get the maximum number of entries allowed
     */
    fun getMaxEntries(): Int = maxEntries
}
