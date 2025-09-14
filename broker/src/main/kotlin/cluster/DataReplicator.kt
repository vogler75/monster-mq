package at.rocworks.cluster

import at.rocworks.Utils
import io.vertx.core.Vertx
import io.vertx.core.eventbus.Message
import io.vertx.core.eventbus.MessageCodec
import io.vertx.core.json.JsonObject
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

/**
 * Generic cluster data replicator that synchronizes data across cluster nodes using Vert.x EventBus.
 *
 * @param T The type of data being replicated
 * @param vertx The Vert.x instance
 * @param eventBusAddress The EventBus address for this replicator
 * @param codec Optional codec for custom types (not needed for JsonObject)
 */
class DataReplicator<T>(
    private val vertx: Vertx,
    private val eventBusAddress: String,
    private val codec: MessageCodec<T, T>? = null,
    private val loggerName: String = "DataReplicator"
) {
    private val logger: Logger = Utils.getLogger(DataReplicator::class.java, loggerName)
    private val localData = ConcurrentHashMap<String, T>()

    // Event types for data synchronization
    enum class EventType {
        PUT,     // Add or update data
        REMOVE,  // Remove single entry
        CLEAR,   // Clear all entries
        BATCH    // Batch update
    }

    data class ReplicationEvent<T>(
        val eventType: EventType,
        val key: String? = null,
        val value: T? = null,
        val batch: Map<String, T>? = null
    )

    init {
        // Register codec if provided
        codec?.let {
            try {
                vertx.eventBus().registerDefaultCodec(it.javaClass as Class<T>, it)
            } catch (e: IllegalStateException) {
                // Codec might already be registered
                logger.fine("Codec already registered for ${it.javaClass.simpleName}")
            }
        }

        // Set up event bus consumer for replication events
        vertx.eventBus().consumer<JsonObject>(eventBusAddress) { message ->
            handleReplicationEvent(message)
        }

        logger.info("ClusterDataReplicator initialized at address: $eventBusAddress")
    }

    /**
     * Put a key-value pair and replicate to all nodes
     */
    fun put(key: String, value: T): T? {
        val previous = localData.put(key, value)

        // Broadcast to other nodes
        val event = JsonObject()
            .put("eventType", EventType.PUT.name)
            .put("key", key)
            .put("value", serializeValue(value))

        vertx.eventBus().publish(eventBusAddress, event)
        logger.finest { "Put [$key] and broadcasted to cluster" }

        return previous
    }

    /**
     * Get a value by key
     */
    fun get(key: String): T? = localData[key]

    /**
     * Remove a key and replicate to all nodes
     */
    fun remove(key: String): T? {
        val removed = localData.remove(key)

        if (removed != null) {
            // Broadcast to other nodes
            val event = JsonObject()
                .put("eventType", EventType.REMOVE.name)
                .put("key", key)

            vertx.eventBus().publish(eventBusAddress, event)
            logger.finest { "Removed [$key] and broadcasted to cluster" }
        }

        return removed
    }

    /**
     * Clear all entries and replicate to all nodes
     */
    fun clear() {
        localData.clear()

        // Broadcast to other nodes
        val event = JsonObject()
            .put("eventType", EventType.CLEAR.name)

        vertx.eventBus().publish(eventBusAddress, event)
        logger.fine("Cleared all entries and broadcasted to cluster")
    }

    /**
     * Put all entries from a map and replicate to all nodes
     */
    fun putAll(map: Map<String, T>) {
        localData.putAll(map)

        // Broadcast batch update to other nodes
        val event = JsonObject()
            .put("eventType", EventType.BATCH.name)
            .put("batch", JsonObject(map.mapValues { serializeValue(it.value) }))

        vertx.eventBus().publish(eventBusAddress, event)
        logger.fine("Put ${map.size} entries and broadcasted to cluster")
    }

    /**
     * Remove all entries matching a predicate
     */
    fun removeIf(predicate: (Map.Entry<String, T>) -> Boolean): Boolean {
        val toRemove = localData.entries.filter(predicate).map { it.key }

        if (toRemove.isNotEmpty()) {
            toRemove.forEach { key ->
                remove(key)
            }
            return true
        }

        return false
    }

    /**
     * Get all keys
     */
    fun keys(): Set<String> = localData.keys.toSet()

    /**
     * Get all values
     */
    fun values(): Collection<T> = localData.values

    /**
     * Get all entries
     */
    fun entries(): Set<Map.Entry<String, T>> = localData.entries.toSet()

    /**
     * Check if a key exists
     */
    fun containsKey(key: String): Boolean = localData.containsKey(key)

    /**
     * Get the size of the map
     */
    fun size(): Int = localData.size

    /**
     * Check if empty
     */
    fun isEmpty(): Boolean = localData.isEmpty()

    /**
     * Iterate over all entries
     */
    fun forEach(action: (String, T) -> Unit) {
        localData.forEach(action)
    }

    /**
     * Get a snapshot of the current data
     */
    fun snapshot(): Map<String, T> = HashMap(localData)

    /**
     * Handle incoming replication events
     */
    private fun handleReplicationEvent(message: Message<JsonObject>) {
        val event = message.body()
        val eventType = EventType.valueOf(event.getString("eventType"))

        when (eventType) {
            EventType.PUT -> {
                val key = event.getString("key")
                val value = deserializeValue(event.getValue("value"))
                if (key != null && value != null) {
                    localData[key] = value
                    logger.finest { "Received PUT event for key [$key]" }
                }
            }

            EventType.REMOVE -> {
                val key = event.getString("key")
                if (key != null) {
                    localData.remove(key)
                    logger.finest { "Received REMOVE event for key [$key]" }
                }
            }

            EventType.CLEAR -> {
                localData.clear()
                logger.fine("Received CLEAR event")
            }

            EventType.BATCH -> {
                val batch = event.getJsonObject("batch")
                if (batch != null) {
                    batch.forEach { entry ->
                        val value = deserializeValue(entry.value)
                        if (value != null) {
                            localData[entry.key] = value
                        }
                    }
                    logger.fine("Received BATCH event with ${batch.size()} entries")
                }
            }
        }
    }

    /**
     * Serialize value for transmission over EventBus
     */
    @Suppress("UNCHECKED_CAST")
    private fun serializeValue(value: T): Any {
        return when (value) {
            is String, is Number, is Boolean -> value
            is JsonObject -> value
            else -> JsonObject.mapFrom(value)
        }
    }

    /**
     * Deserialize value from EventBus transmission
     */
    @Suppress("UNCHECKED_CAST")
    private fun deserializeValue(value: Any?): T? {
        return value as? T
    }
}