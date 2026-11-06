package at.rocworks.stores

import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

class KafkaMultiplexingQueueStore(
    private val vertx: Vertx,
    private val configJson: JsonObject
) : IKafkaQueueStore {
    private val logger = Utils.getLogger(this::class.java)
    
    private class StreamDef(
        val name: String,
        val filter: String,
        val retentionMs: Long,
        val store: IKafkaQueueStore
    )

    // Streams in configuration order (routing is deterministic: first matching filter wins)
    @Volatile private var streams: List<StreamDef> = emptyList()

    private fun sanitizeTableNameSuffix(name: String): String {
        val sanitized = name.lowercase()
            .replace(Regex("[^a-z0-9]"), "_")
            .replace(Regex("_+"), "_")
            .trim('_')
        return if (sanitized.isEmpty()) "default" else sanitized
    }

    fun initialize(): Future<Void> {
        val kafkaConfig = configJson.getJsonObject("KafkaServer", JsonObject())
        val streamsConfig = kafkaConfig.getJsonArray("Streams") ?: kafkaConfig.getJsonArray("streams") ?: JsonArray()

        val futures = mutableListOf<Future<StreamDef?>>()

        streamsConfig.forEach { stream ->
            val streamObj = stream as JsonObject
            val topicFilter = streamObj.getString("TopicFilter") ?: streamObj.getString("topicFilter")
            val streamName = streamObj.getString("StreamName") ?: streamObj.getString("streamName") ?: topicFilter
            val storeOverride = streamObj.getString("StoreType") ?: streamObj.getString("storeType")
            val retentionHours = (streamObj.getInteger("RetentionHours") ?: streamObj.getInteger("retentionHours") ?: 168).toLong()

            if (!topicFilter.isNullOrBlank() && !streamName.isNullOrBlank()) {
                val sanitizedSuffix = sanitizeTableNameSuffix(streamName)
                val streamConfig = configJson.copy()
                if (!storeOverride.isNullOrBlank()) {
                    streamConfig.getJsonObject("KafkaServer").put("storeType", storeOverride)
                }

                futures.add(KafkaQueueStoreFactory.create(vertx, streamConfig, sanitizedSuffix).map { store ->
                    logger.info("Initialized stream storage: '$streamName' (filter: '$topicFilter') -> table suffix: '$sanitizedSuffix'")
                    StreamDef(streamName, topicFilter, retentionHours * 60 * 60 * 1000L, store) as StreamDef?
                }.recover { err ->
                    logger.severe("Failed to initialize stream storage for '$streamName' ($storeOverride): ${err.message}")
                    Future.failedFuture(err)
                })
            }
        }

        // Future.all keeps the order of the futures, i.e. the configuration order
        return Future.all(futures).map { composite ->
            streams = (0 until futures.size).mapNotNull { composite.resultAt<StreamDef?>(it) }
            null
        }
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

    private fun streamByName(name: String): StreamDef? = streams.firstOrNull { it.name == name }

    private fun streamByTopic(mqttTopic: String): StreamDef? =
        streams.firstOrNull { mqttTopicMatchesFilter(mqttTopic, it.filter) }

    /** Write grouped messages to their streams and wake up long-polling consumers afterwards. */
    private fun write(byStream: Map<StreamDef, List<BrokerMessage>>): Future<Void> {
        val futures = byStream.map { (stream, msgs) ->
            stream.store.enqueue(msgs).onSuccess {
                vertx.eventBus().publish("kafka.stream.updated.${stream.name}", System.currentTimeMillis())
            }
        }
        return Future.all(futures).map { null }
    }

    override fun enqueue(messages: List<BrokerMessage>): Future<Void> {
        if (messages.isEmpty()) return Future.succeededFuture()

        val groups = messages.mapNotNull { msg -> (streamByTopic(msg.topicName) ?: streamByName(msg.topicName))?.let { it to msg } }
            .groupBy({ it.first }, { it.second })
        return write(groups)
    }

    override fun enqueueForProduce(kafkaTopic: String, messages: List<BrokerMessage>): Future<Void> {
        if (messages.isEmpty()) return Future.succeededFuture()

        val groups = mutableMapOf<StreamDef, MutableList<BrokerMessage>>()
        val named = streamByName(kafkaTopic)
        messages.forEach { msg ->
            // The stream of the Kafka topic plus the stream the orchestrator would pick (deduplicated)
            listOfNotNull(named, streamByTopic(msg.topicName)).distinct().forEach { stream ->
                groups.getOrPut(stream) { mutableListOf() }.add(msg)
            }
        }
        return write(groups)
    }

    private fun getStoreForTopicOrName(topic: String): IKafkaQueueStore? {
        return (streamByName(topic) ?: streamByTopic(topic))?.store
    }

    /** Prune every stream with its own retention. */
    fun pruneByRetention(): Future<Int> {
        val now = System.currentTimeMillis()
        val futures = streams.map { it.store.pruneExpired(now - it.retentionMs) }
        if (futures.isEmpty()) return Future.succeededFuture(0)
        return Future.all(futures).map { results -> results.list<Int>().sum() }
    }

    override fun fetch(topic: String, startOffset: Long, limit: Int): Future<List<Pair<Long, BrokerMessage>>> {
        val store = getStoreForTopicOrName(topic) ?: return Future.succeededFuture(emptyList())
        return store.fetch(topic, startOffset, limit)
    }

    override fun pruneExpired(olderThanMs: Long): Future<Int> {
        val uniqueStores = streams.map { it.store }.toSet()
        if (uniqueStores.isEmpty()) return Future.succeededFuture(0)
        
        val futures = uniqueStores.map { store ->
            store.pruneExpired(olderThanMs)
        }
        return Future.all(futures).map { results ->
            results.list<Int>().sum()
        }
    }

    override fun commitOffset(groupId: String, topic: String, partition: Int, offset: Long): Future<Void> {
        val store = getStoreForTopicOrName(topic) ?: return Future.succeededFuture()
        return store.commitOffset(groupId, topic, partition, offset)
    }

    override fun getOffset(groupId: String, topic: String, partition: Int): Future<Long?> {
        val store = getStoreForTopicOrName(topic) ?: return Future.succeededFuture(null)
        return store.getOffset(groupId, topic, partition)
    }

    override fun getEarliestOffset(topic: String): Future<Long> {
        val store = getStoreForTopicOrName(topic) ?: return Future.succeededFuture(0L)
        return store.getEarliestOffset(topic)
    }

    override fun getLatestOffset(topic: String): Future<Long> {
        val store = getStoreForTopicOrName(topic) ?: return Future.succeededFuture(0L)
        return store.getLatestOffset(topic)
    }

    override fun getConsumerGroups(): Future<List<KafkaConsumerGroup>> {
        val uniqueStores = streams.map { it.store }.toSet()
        if (uniqueStores.isEmpty()) return Future.succeededFuture(emptyList())

        val futures = uniqueStores.map { it.getConsumerGroups() }
        return Future.all(futures).map { results ->
            val allGroups = mutableMapOf<String, KafkaConsumerGroup>()
            results.list<List<KafkaConsumerGroup>>().forEach { list ->
                list.forEach { group ->
                    val existing = allGroups[group.groupId]
                    if (existing == null) {
                        allGroups[group.groupId] = group
                    } else {
                        val mergedTopics = (existing.topics + group.topics).distinct()
                        val maxTime = maxOf(existing.lastCommitTime, group.lastCommitTime)
                        allGroups[group.groupId] = KafkaConsumerGroup(group.groupId, mergedTopics, maxTime)
                    }
                }
            }
            allGroups.values.toList()
        }
    }

    override fun deleteConsumerGroup(groupId: String): Future<Boolean> {
        val uniqueStores = streams.map { it.store }.toSet()
        if (uniqueStores.isEmpty()) return Future.succeededFuture(false)

        val futures = uniqueStores.map { it.deleteConsumerGroup(groupId) }
        return Future.all(futures).map { results ->
            results.list<Boolean>().any { it }
        }
    }
}
