package at.rocworks.stores

import at.rocworks.bus.EventBusAddresses
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.data.PurgeResult
import at.rocworks.data.TopicTree
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Callable

class MessageStoreHazelcast(
    private val name: String,
    private val hazelcast: HazelcastInstance
): AbstractVerticle(), IMessageStore {
    private val logger = Utils.getLogger(this::class.java, name)

    private val index = TopicTree<Void, Void>()
    private val store = hazelcast.getMap<String, BrokerMessage>(name)

    private val addAddress = EventBusAddresses.Store.add(name)
    private val delAddress = EventBusAddresses.Store.delete(name)

    override fun getName(): String = name
    override fun getType(): MessageStoreType = MessageStoreType.HAZELCAST

    override fun start(startPromise: Promise<Void>) {
        vertx.executeBlocking(Callable {
            vertx.eventBus().consumer<JsonArray>(addAddress) {
                index.addAll(it.body().map { it.toString() })
            }
            vertx.eventBus().consumer<JsonArray>(delAddress) {
                index.delAll(it.body().map { it.toString() })
            }
            logger.info("Indexing [$name] message store [${Utils.getCurrentFunctionName()}]")
            store.keys.forEach { index.add(it) }
            logger.info("Indexing [$name] message store finished [${Utils.getCurrentFunctionName()}]")
            startPromise.complete()
        })
    }

    override fun get(topicName: String): BrokerMessage? = store[topicName]
    
    override fun getAsync(topicName: String, callback: (BrokerMessage?) -> Unit) {
        // Hazelcast store can respond immediately
        callback(store[topicName])
    }

    override fun addAll(messages: List<BrokerMessage>) {
        val startTime = Instant.now()

        val topics = messages.map { it.topicName }.distinct()
        vertx.eventBus().publish(addAddress, JsonArray(topics))
        store.putAllAsync(messages.associateBy { it.topicName })

        val duration = Duration.between(startTime, Instant.now()).toMillis()
        logger.finest { "Write block of size [${messages.size}] to map took [$duration] [${Utils.getCurrentFunctionName()}]" }
    }

    override fun delAll(topics: List<String>) {
        vertx.eventBus().publish(delAddress, JsonArray(topics))
        topics.forEach { store.remove(it) } // there is no delAll
    }

    override fun findMatchingMessages(topicName: String, callback: (BrokerMessage) -> Boolean) {
        index.findMatchingTopicNames(topicName) { foundTopicName ->
            val message = store[foundTopicName]
            if (message != null) callback(message)
            else true
        }
    }

    override fun findMatchingTopics(topicPattern: String, callback: (String) -> Boolean) {
        // For Hazelcast stores, use the index to find matching topic names directly
        index.findMatchingTopicNames(topicPattern, callback)
    }
    
    override fun purgeOldMessages(olderThan: Instant): PurgeResult {
        val startTime = System.currentTimeMillis()
        
        logger.fine { "Starting purge for [$name] - removing messages older than $olderThan" }
        
        // Use Hazelcast predicates for efficient distributed filtering
        val predicate = Predicates.lessThan<String, BrokerMessage>("time", olderThan)
        val entriesToDelete = store.entrySet(predicate)
        
        val topicsToDelete = entriesToDelete.map { it.key }
        val deleteCount = topicsToDelete.size
        
        logger.fine { "Found $deleteCount messages to delete in [$name]" }
        
        // Delete in batches to avoid overwhelming the cluster
        if (topicsToDelete.isNotEmpty()) {
            val batchSize = 1000
            topicsToDelete.chunked(batchSize).forEachIndexed { index, batch ->
                delAll(batch)
                if ((index + 1) * batchSize % 10000 == 0) {
                    logger.fine { "Purge progress for [$name]: deleted ${(index + 1) * batchSize} messages" }
                }
            }
        }
        
        val elapsedTimeMs = System.currentTimeMillis() - startTime
        val result = PurgeResult(deleteCount, elapsedTimeMs)
        
        logger.fine { "Purge completed for [$name]: deleted ${result.deletedCount} messages in ${result.elapsedTimeMs}ms" }
        
        return result
    }

    override fun dropStorage(): Boolean {
        return try {
            store.clear()
            logger.info("Cleared Hazelcast storage for message store [$name]")
            true
        } catch (e: Exception) {
            logger.severe("Error clearing Hazelcast storage for message store [$name]: ${e.message}")
            false
        }
    }

    override fun getConnectionStatus(): Boolean {
        return try {
            hazelcast.lifecycleService.isRunning
        } catch (e: Exception) {
            false
        }
    }
}