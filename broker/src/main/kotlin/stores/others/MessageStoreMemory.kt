package at.rocworks.stores

import at.rocworks.Const
import at.rocworks.bus.EventBusAddresses
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.data.PurgeResult
import at.rocworks.data.TopicTree
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.Collections
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap

class MessageStoreMemory(
    private val name: String,
    private val maxMemoryEntries: Long? = null
): AbstractVerticle(), IMessageStoreExtended {
    private val logger = Utils.getLogger(this::class.java, name)

    // Mark stored topics so pruning an evicted child cannot remove a stored parent.
    private val index = TopicTree<Boolean, Boolean>()

    // Bounded stores use access order; addAll applies eviction together with index updates.
    private val store: MutableMap<String, BrokerMessage> = if (maxMemoryEntries != null && maxMemoryEntries > 0) {
        Collections.synchronizedMap(LinkedHashMap<String, BrokerMessage>(16, 0.75f, true))
    } else {
        ConcurrentHashMap<String, BrokerMessage>()
    }

    private val addAddress = EventBusAddresses.Store.add(name)
    private val delAddress = EventBusAddresses.Store.delete(name)
    private val indexSource = Utils.getUuid()
    private val indexUpdateOptions = DeliveryOptions().addHeader(INDEX_SOURCE_HEADER, indexSource)

    companion object {
        private const val INDEX_SOURCE_HEADER = "memory-store-source"
    }

    override fun getName(): String = name
    override fun getType(): MessageStoreType = MessageStoreType.MEMORY

    override fun start(startPromise: Promise<Void>) {
        vertx.executeBlocking(Callable {
            vertx.eventBus().consumer<JsonArray>(addAddress) {
                // Local mutations already updated the index synchronously. Replaying them
                // later could resurrect an evicted topic or delete a reinserted one.
                if (it.headers().get(INDEX_SOURCE_HEADER) != indexSource) {
                    synchronized(store) {
                        it.body().forEach { topic -> index.add(topic.toString(), true, true) }
                    }
                }
            }
            vertx.eventBus().consumer<JsonArray>(delAddress) {
                if (it.headers().get(INDEX_SOURCE_HEADER) != indexSource) {
                    synchronized(store) {
                        it.body().forEach { topic ->
                            val topicName = topic.toString()
                            if (!store.containsKey(topicName)) index.del(topicName, true)
                        }
                    }
                }
            }
            logger.info("Indexing [$name] message store [${Utils.getCurrentFunctionName()}]")
            synchronized(store) {
                store.keys.forEach { index.add(it, true, true) }
            }
            logger.info("Indexing [$name] message store finished [${Utils.getCurrentFunctionName()}]")
            startPromise.complete()
        })
    }


    override fun get(topicName: String): BrokerMessage? = store[topicName]
    
    override fun getAsync(topicName: String, callback: (BrokerMessage?) -> Unit) {
        // Memory store can respond immediately
        callback(store[topicName])
    }

    override fun addAll(messages: List<BrokerMessage>) {
        synchronized(store) {
            val evictedTopics = mutableSetOf<String>()
            messages.forEach { message ->
                store[message.topicName] = message
                if (maxMemoryEntries != null && maxMemoryEntries > 0 && store.size > maxMemoryEntries) {
                    val eldestTopic = store.keys.first()
                    store.remove(eldestTopic)
                    index.del(eldestTopic, true)
                    evictedTopics.add(eldestTopic)
                }
                // An earlier insertion in this batch may have evicted this topic.
                index.add(message.topicName, true, true)
            }

            // Notify peers of the final batch state, excluding temporary evictions.
            publishIndexUpdate(delAddress, evictedTopics.filterNot { store.containsKey(it) })
            val topics = messages.map { it.topicName }.distinct().filter { store.containsKey(it) }
            publishIndexUpdate(addAddress, topics)
        }
    }

    override fun delAll(topics: List<String>) {
        synchronized(store) {
            topics.forEach {
                store.remove(it)
                index.del(it, true)
            }
            publishIndexUpdate(delAddress, topics)
        }
    }

    private fun publishIndexUpdate(address: String, topics: List<String>) {
        if (topics.isNotEmpty()) {
            vertx?.eventBus()?.publish(address, JsonArray(topics), indexUpdateOptions)
        }
    }

    override fun findMatchingMessages(topicName: String, callback: (BrokerMessage) -> Boolean) {
        // Include stored parents as well as leaves; hierarchy-only nodes have no message.
        index.findBrowseTopics(topicName) { foundTopicName ->
            val message = store[foundTopicName]
            if (message != null) callback(message)
            else true
        }
    }

    override fun findMatchingTopics(topicPattern: String, callback: (String) -> Boolean) {
        // Use the efficient topic tree browsing method
        index.findBrowseTopics(topicPattern, callback)
    }

    override fun findTopicsByName(name: String, ignoreCase: Boolean, namespace: String): List<String> {
        val matcher = createNameMatcher(name, ignoreCase)
        val namespacePrefix = namespace.takeIf { it.isNotEmpty() }?.let { "$it/" }

        val keys = synchronized(store) { store.keys.toList() }
        return keys
            .asSequence()
            .filter { topic -> !topic.endsWith("/${Const.CONFIG_TOPIC}") && topic != Const.CONFIG_TOPIC }
            .filter { topic -> namespacePrefix == null || topic.startsWith(namespacePrefix, ignoreCase) }
            .filter { topic -> matcher(topic) }
            .sorted()
            .toList()
    }

    override fun findTopicsByConfig(config: String, description: String, ignoreCase: Boolean, namespace: String): List<Pair<String, String>> {
        val namespacePrefix = namespace.takeIf { it.isNotEmpty() }?.let { "$it/" }

        val values = synchronized(store) { store.values.toList() }
        return values
            .asSequence()
            .filter { message -> message.topicName.endsWith("/${Const.CONFIG_TOPIC}") }
            .filter { message -> namespacePrefix == null || message.topicName.startsWith(namespacePrefix, ignoreCase) }
            .mapNotNull { message ->
                val configText = message.getPayloadAsString()
                val configValue = try {
                    JsonObject(configText).getString(config) ?: ""
                } catch (_: Exception) {
                    ""
                }
                if (matchesConfigValue(configValue, description, ignoreCase)) {
                    message.topicName.removeSuffix("/${Const.CONFIG_TOPIC}") to configText
                } else {
                    null
                }
            }
            .sortedBy { it.first }
            .toList()
    }
    
    override fun purgeOldMessages(olderThan: Instant): PurgeResult {
        // No-op: addAll handles LRU eviction automatically.
        // If maxMemoryEntries is set, old entries are evicted when size exceeds the limit,
        // making time-based purging unnecessary.
        return PurgeResult(0, 0)
    }

    override fun dropStorage(): Boolean {
        return try {
            synchronized(store) {
                store.clear()
                index.clear()
            }
            logger.info("Cleared in-memory storage for message store [$name]")
            true
        } catch (e: Exception) {
            logger.severe("Error clearing in-memory storage for message store [$name]: ${e.message}")
            false
        }
    }

    override fun getConnectionStatus(): Boolean = true // Memory store is always connected

    override suspend fun tableExists(): Boolean = true // Memory store has no table requirements

    override suspend fun createTable(): Boolean {
        // Memory stores don't require table creation
        return true
    }

    private fun createNameMatcher(name: String, ignoreCase: Boolean): (String) -> Boolean {
        val hasWildcards = name.contains("*") || name.contains("+")
        if (!hasWildcards) {
            return { topic -> topic.contains(name, ignoreCase) }
        }

        val regexPattern = buildString {
            append("^")
            name.forEach { char ->
                when (char) {
                    '*' -> append(".*")
                    '+' -> append(".")
                    else -> append(Regex.escape(char.toString()))
                }
            }
            append("$")
        }
        val options = if (ignoreCase) setOf(RegexOption.IGNORE_CASE) else emptySet()
        val regex = Regex(regexPattern, options)
        return { topic -> regex.matches(topic) }
    }

    private fun matchesConfigValue(value: String, pattern: String, ignoreCase: Boolean): Boolean {
        if (pattern.isEmpty()) return true
        return try {
            val options = if (ignoreCase) setOf(RegexOption.IGNORE_CASE) else emptySet()
            Regex(pattern, options).containsMatchIn(value)
        } catch (_: Exception) {
            value.contains(pattern, ignoreCase)
        }
    }
}
