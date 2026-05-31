package at.rocworks.stores

import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.util.concurrent.ConcurrentHashMap

class KafkaMultiplexingQueueStore(
    private val vertx: Vertx,
    private val configJson: JsonObject
) : IKafkaQueueStore {
    private val logger = Utils.getLogger(this::class.java)
    
    // MQTT Topic Filter -> Store (used for enqueuing publishes)
    private val streamStoresByFilter = ConcurrentHashMap<String, IKafkaQueueStore>()
    
    // Kafka Topic/Stream Name -> Store (used for fetches and offsets)
    private val streamStoresByName = ConcurrentHashMap<String, IKafkaQueueStore>()
    
    private fun sanitizeTableNameSuffix(name: String): String {
        val sanitized = name.lowercase()
            .replace(Regex("[^a-z0-9]"), "_")
            .replace(Regex("_+"), "_")
            .trim('_')
        return if (sanitized.isEmpty()) "default" else sanitized
    }

    fun initialize(): Future<Void> {
        val kafkaConfig = configJson.getJsonObject("KafkaServer", JsonObject())
        val streams = kafkaConfig.getJsonArray("Streams") ?: kafkaConfig.getJsonArray("streams") ?: JsonArray()
        
        val futures = mutableListOf<Future<Void>>()
        
        streams.forEach { stream ->
            val streamObj = stream as JsonObject
            val topicFilter = streamObj.getString("TopicFilter") ?: streamObj.getString("topicFilter")
            val streamName = streamObj.getString("StreamName") ?: streamObj.getString("streamName") ?: topicFilter
            val storeOverride = streamObj.getString("StoreType") ?: streamObj.getString("storeType")
            
            if (!topicFilter.isNullOrBlank() && !streamName.isNullOrBlank()) {
                val sanitizedSuffix = sanitizeTableNameSuffix(streamName)
                val streamConfig = configJson.copy()
                if (!storeOverride.isNullOrBlank()) {
                    streamConfig.getJsonObject("KafkaServer").put("storeType", storeOverride)
                }
                
                val promise = io.vertx.core.Promise.promise<Void>()
                futures.add(promise.future())
                
                KafkaQueueStoreFactory.create(vertx, streamConfig, sanitizedSuffix).onComplete { ar ->
                    if (ar.succeeded()) {
                        val storeInstance = ar.result()
                        streamStoresByFilter[topicFilter] = storeInstance
                        streamStoresByName[streamName] = storeInstance
                        logger.info("Initialized stream storage: '$streamName' (filter: '$topicFilter') -> table suffix: '$sanitizedSuffix'")
                        promise.complete()
                    } else {
                        logger.severe("Failed to initialize stream storage for '$streamName' ($storeOverride): ${ar.cause()?.message}")
                        promise.fail(ar.cause())
                    }
                }
            }
        }
        
        return Future.all<Void>(futures).map { null }
    }
    
    private fun getStoreForTopic(topic: String): IKafkaQueueStore? {
        for ((filter, store) in streamStoresByFilter) {
            if (mqttTopicMatchesFilter(topic, filter)) {
                return store
            }
        }
        return null
    }
    
    private fun mqttTopicMatchesFilter(topic: String, filter: String): Boolean {
        if (filter == "#") return true
        val topicParts = topic.split('/')
        val filterParts = filter.split('/')

        var i = 0
        while (i < filterParts.size) {
            val fp = filterParts[i]
            if (fp == "#") return true
            if (i >= topicParts.size) return false
            if (fp != "+" && fp != topicParts[i]) return false
            i++
        }
        return i == topicParts.size
    }

    override fun enqueue(messages: List<BrokerMessage>): Future<Void> {
        if (messages.isEmpty()) return Future.succeededFuture()
        
        val groups = messages.groupBy { getStoreForTopic(it.topicName) ?: streamStoresByName[it.topicName] }
        val futures = mutableListOf<Future<Void>>()
        groups.forEach { (store, msgs) ->
            if (store != null) {
                futures.add(store.enqueue(msgs))
            }
        }
        return Future.all<Void>(futures).map { null }
    }

    override fun fetch(topic: String, startOffset: Long, limit: Int): Future<List<Pair<Long, BrokerMessage>>> {
        val store = streamStoresByName[topic] ?: return Future.succeededFuture(emptyList())
        return store.fetch(topic, startOffset, limit)
    }

    override fun pruneExpired(olderThanMs: Long): Future<Int> {
        val uniqueStores = streamStoresByName.values.toSet()
        if (uniqueStores.isEmpty()) return Future.succeededFuture(0)
        
        val futures = uniqueStores.map { store ->
            store.pruneExpired(olderThanMs)
        }
        return Future.all<Int>(futures).map { results ->
            results.list<Int>().sum()
        }
    }

    override fun commitOffset(groupId: String, topic: String, partition: Int, offset: Long): Future<Void> {
        val store = streamStoresByName[topic] ?: return Future.succeededFuture()
        return store.commitOffset(groupId, topic, partition, offset)
    }

    override fun getOffset(groupId: String, topic: String, partition: Int): Future<Long?> {
        val store = streamStoresByName[topic] ?: return Future.succeededFuture(null)
        return store.getOffset(groupId, topic, partition)
    }

    override fun getEarliestOffset(topic: String): Future<Long> {
        val store = streamStoresByName[topic] ?: return Future.succeededFuture(0L)
        return store.getEarliestOffset(topic)
    }

    override fun getLatestOffset(topic: String): Future<Long> {
        val store = streamStoresByName[topic] ?: return Future.succeededFuture(0L)
        return store.getLatestOffset(topic)
    }
}
