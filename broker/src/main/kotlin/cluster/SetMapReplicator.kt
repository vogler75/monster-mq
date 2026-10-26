package at.rocworks.cluster

import at.rocworks.Utils
import at.rocworks.data.TopicTree
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
) {
    private val logger: Logger = Utils.getLogger(SetMapReplicator::class.java)
    private val localData = ConcurrentHashMap<String, MutableSet<String>>()

    // Keys with wildcards are also kept in a topic tree (key -> values), so finding the keys
    // matching a topic does not need a scan over all keys. Exact keys are looked up in localData.
    private val wildcardIndex = TopicTree<String, Boolean>()

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

        logger.fine("ClusterSetMapReplicator initialized at address: $eventBusAddress")
    }

    /**
     * Add a value to the set for a given key
     */
    fun addToSet(key: String, value: String) {
        localAdd(key, value)

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
        if (localData.containsKey(key)) {
            localRemove(key, value)

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
        localRemoveKey(key)

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
                if (Utils.isWildCardTopic(key)) wildcardIndex.del(key, value)
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
     * Get the values of all keys whose topic filter matches the given topic name.
     * Uses a hash lookup for the exact key and the topic tree for wildcard keys.
     */
    fun findValuesMatchingTopic(topicName: String): Set<String> {
        val result = HashSet<String>()
        localData[topicName]?.let { result.addAll(it) }
        wildcardIndex.findDataOfTopicName(topicName).forEach { (value, _) -> result.add(value) }
        return result
    }

    /**
     * Clear all data
     */
    fun clear() {
        localData.clear()
        wildcardIndex.clear()

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
                    localAdd(key, value)
                    logger.finest { "Received ADD_TO_SET event for key [$key], value [$value]" }
                }
            }

            EventType.REMOVE_FROM_SET -> {
                val key = event.getString("key")
                val value = event.getString("value")
                if (key != null && value != null) {
                    localRemove(key, value)
                    logger.finest { "Received REMOVE_FROM_SET event for key [$key], value [$value]" }
                }
            }

            EventType.REMOVE_KEY -> {
                val key = event.getString("key")
                if (key != null) {
                    localRemoveKey(key)
                    logger.finest { "Received REMOVE_KEY event for key [$key]" }
                }
            }

            EventType.CLEAR -> {
                localData.clear()
                wildcardIndex.clear()
                logger.fine { "Received CLEAR event" }
            }
        }
    }

    private fun localAdd(key: String, value: String) {
        localData.getOrPut(key) { ConcurrentHashMap.newKeySet() }.add(value)
        if (Utils.isWildCardTopic(key)) wildcardIndex.add(key, value, true)
    }

    private fun localRemove(key: String, value: String) {
        val set = localData[key] ?: return
        set.remove(value)
        if (set.isEmpty()) {
            localData.remove(key)
        }
        if (Utils.isWildCardTopic(key)) wildcardIndex.del(key, value)
    }

    private fun localRemoveKey(key: String) {
        val set = localData.remove(key) ?: return
        if (Utils.isWildCardTopic(key)) set.forEach { wildcardIndex.del(key, it) }
    }

    /**
     * Get the total size of all sets in the map
     */
    fun size(): Int {
        return localData.values.sumOf { it.size }
    }
}