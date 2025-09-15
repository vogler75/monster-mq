package at.rocworks.stores

import at.rocworks.bus.EventBusAddresses
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.data.PurgeResult
import at.rocworks.data.TopicTree
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import java.time.Instant
import java.util.concurrent.Callable

class MessageStoreMemory(private val name: String): AbstractVerticle(), IMessageStore {
    private val logger = Utils.getLogger(this::class.java, name)

    private val index = TopicTree<Void, Void>()
    private val store = getStore()

    private val addAddress = EventBusAddresses.Store.add(name)
    private val delAddress = EventBusAddresses.Store.delete(name)

    override fun getName(): String = name
    override fun getType(): MessageStoreType = MessageStoreType.MEMORY

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

    private fun getStore(): MutableMap<String, MqttMessage> = mutableMapOf<String, MqttMessage>()

    override fun get(topicName: String): MqttMessage? = store[topicName]
    
    override fun getAsync(topicName: String, callback: (MqttMessage?) -> Unit) {
        // Memory store can respond immediately
        callback(store[topicName])
    }

    override fun addAll(messages: List<MqttMessage>) {
        val topics = messages.map { it.topicName }.distinct()
        vertx.eventBus().publish(addAddress, JsonArray(topics))
        store.putAll(messages.map { Pair(it.topicName, it) })
    }

    override fun delAll(topics: List<String>) {
        vertx.eventBus().publish(delAddress, JsonArray(topics))
        topics.forEach { store.remove(it) } // there is no delAll
    }

    override fun findMatchingMessages(topicName: String, callback: (MqttMessage) -> Boolean) {
        index.findMatchingTopicNames(topicName) { foundTopicName ->
            val message = store[foundTopicName]
            if (message != null) callback(message)
            else true
        }
    }
    
    override fun purgeOldMessages(olderThan: Instant): PurgeResult {
        val startTime = System.currentTimeMillis()
        val topicsToDelete = mutableListOf<String>()
        var checkedCount = 0
        
        logger.fine("Starting purge for [$name] - removing messages older than $olderThan")
        
        // Iterate through all messages and collect topics to delete
        // Process in batches to avoid blocking for too long
        val iterator = store.entries.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            checkedCount++
            
            if (entry.value.time.isBefore(olderThan)) {
                topicsToDelete.add(entry.key)
            }
            
            // Log progress every 100,000 items
            if (checkedCount % 100000 == 0) {
                logger.fine { "Purge progress for [$name]: checked $checkedCount items, found ${topicsToDelete.size} to delete" }
            }
        }
        
        // Delete the collected topics
        if (topicsToDelete.isNotEmpty()) {
            delAll(topicsToDelete)
        }
        
        val elapsedTimeMs = System.currentTimeMillis() - startTime
        val result = PurgeResult(topicsToDelete.size, elapsedTimeMs)
        
        logger.fine("Purge completed for [$name]: deleted ${result.deletedCount} of $checkedCount messages in ${result.elapsedTimeMs}ms")
        
        return result
    }
}