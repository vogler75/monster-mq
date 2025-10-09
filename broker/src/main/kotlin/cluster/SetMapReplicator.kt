package at.rocworks.cluster

import at.rocworks.Utils
import io.vertx.core.Vertx
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

/**
 * Specialized replicator for Map<String, Set<String>> data structure
 * Used for topic-to-nodes mapping
 */
class SetMapReplicator(
    private val vertx: Vertx,
    private val eventBusAddress: String,
    loggerName: String = "SetMapReplicator"
) {
    private val logger: Logger = Utils.getLogger(SetMapReplicator::class.java, loggerName)
    private val localData = ConcurrentHashMap<String, MutableSet<String>>()

    enum class EventType {
        ADD_TO_SET,      // Add value to set
        REMOVE_FROM_SET, // Remove value from set
        REMOVE_KEY,      // Remove entire key
        CLEAR            // Clear all
    }

    init {
        // Set up event bus consumer for replication events
        vertx.eventBus().consumer<JsonObject>(eventBusAddress) { message ->
            handleReplicationEvent(message)
        }

        logger.info("ClusterSetMapReplicator initialized at address: $eventBusAddress")
    }

    /**
     * Add a value to the set for a given key
     */
    fun addToSet(key: String, value: String) {
        localData.getOrPut(key) { ConcurrentHashMap.newKeySet() }.add(value)

        // Broadcast to other nodes
        val event = JsonObject()
            .put("eventType", EventType.ADD_TO_SET.name)
            .put("key", key)
            .put("value", value)

        vertx.eventBus().publish(eventBusAddress, event)
        logger.finest { "Added [$value] to set [$key] and broadcasted to cluster" }
    }

    /**
     * Remove a value from the set for a given key
     */
    fun removeFromSet(key: String, value: String) {
        val set = localData[key]
        if (set != null) {
            set.remove(value)
            if (set.isEmpty()) {
                localData.remove(key)
            }

            // Broadcast to other nodes
            val event = JsonObject()
                .put("eventType", EventType.REMOVE_FROM_SET.name)
                .put("key", key)
                .put("value", value)

            vertx.eventBus().publish(eventBusAddress, event)
            logger.finest { "Removed [$value] from set [$key] and broadcasted to cluster" }
        }
    }

    /**
     * Remove an entire key
     */
    fun removeKey(key: String) {
        localData.remove(key)

        // Broadcast to other nodes
        val event = JsonObject()
            .put("eventType", EventType.REMOVE_KEY.name)
            .put("key", key)

        vertx.eventBus().publish(eventBusAddress, event)
        logger.finest { "Removed key [$key] and broadcasted to cluster" }
    }

    /**
     * Remove a value from all sets
     */
    fun removeValueFromAllSets(value: String) {
        val keysToRemove = mutableListOf<String>()

        localData.forEach { (key, set) ->
            if (set.remove(value)) {
                if (set.isEmpty()) {
                    keysToRemove.add(key)
                }

                // Broadcast each removal
                val event = JsonObject()
                    .put("eventType", EventType.REMOVE_FROM_SET.name)
                    .put("key", key)
                    .put("value", value)

                vertx.eventBus().publish(eventBusAddress, event)
            }
        }

        // Remove empty keys
        keysToRemove.forEach { localData.remove(it) }

        if (keysToRemove.isNotEmpty()) {
            logger.fine { "Removed value [$value] from all sets and cleaned up ${keysToRemove.size} empty keys" }
        }
    }

    /**
     * Get the set for a given key
     */
    fun getSet(key: String): Set<String>? = localData[key]?.toSet()

    /**
     * Get all keys
     */
    fun keys(): Set<String> = localData.keys.toSet()

    /**
     * Clear all data
     */
    fun clear() {
        localData.clear()

        // Broadcast to other nodes
        val event = JsonObject()
            .put("eventType", EventType.CLEAR.name)

        vertx.eventBus().publish(eventBusAddress, event)
        logger.fine { "Cleared all entries and broadcasted to cluster" }
    }

    /**
     * Handle incoming replication events
     */
    private fun handleReplicationEvent(message: Message<JsonObject>) {
        val event = message.body()
        val eventType = EventType.valueOf(event.getString("eventType"))

        when (eventType) {
            EventType.ADD_TO_SET -> {
                val key = event.getString("key")
                val value = event.getString("value")
                if (key != null && value != null) {
                    localData.getOrPut(key) { ConcurrentHashMap.newKeySet() }.add(value)
                    logger.finest { "Received ADD_TO_SET event for key [$key], value [$value]" }
                }
            }

            EventType.REMOVE_FROM_SET -> {
                val key = event.getString("key")
                val value = event.getString("value")
                if (key != null && value != null) {
                    val set = localData[key]
                    if (set != null) {
                        set.remove(value)
                        if (set.isEmpty()) {
                            localData.remove(key)
                        }
                    }
                    logger.finest { "Received REMOVE_FROM_SET event for key [$key], value [$value]" }
                }
            }

            EventType.REMOVE_KEY -> {
                val key = event.getString("key")
                if (key != null) {
                    localData.remove(key)
                    logger.finest { "Received REMOVE_KEY event for key [$key]" }
                }
            }

            EventType.CLEAR -> {
                localData.clear()
                logger.fine { "Received CLEAR event" }
            }
        }
    }

    /**
     * Get the total size of all sets in the map
     */
    fun size(): Int {
        return localData.values.sumOf { it.size }
    }
}