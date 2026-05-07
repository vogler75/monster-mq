package at.rocworks.stores

import at.rocworks.Const
import at.rocworks.bus.EventBusAddresses
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.data.PurgeResult
import at.rocworks.data.TopicTree
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.Callable

class MessageStoreMemory(
    private val name: String,
    private val maxMemoryEntries: Long? = null
): AbstractVerticle(), IMessageStoreExtended {
    private val logger = Utils.getLogger(this::class.java, name)

    private val index = TopicTree<Void, Void>()

    // Use LinkedHashMap with LRU eviction if maxMemoryEntries is set, otherwise use unlimited HashMap
    private val store = if (maxMemoryEntries != null && maxMemoryEntries > 0) {
        object : LinkedHashMap<String, BrokerMessage>(16, 0.75f, true) {
            // true = access-order (LRU mode): most recently accessed entries stay at the end
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, BrokerMessage>): Boolean {
                return size > maxMemoryEntries
            }
        }
    } else {
        mutableMapOf<String, BrokerMessage>()
    }

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


    override fun get(topicName: String): BrokerMessage? = store[topicName]
    
    override fun getAsync(topicName: String, callback: (BrokerMessage?) -> Unit) {
        // Memory store can respond immediately
        callback(store[topicName])
    }

    override fun addAll(messages: List<BrokerMessage>) {
        val topics = messages.map { it.topicName }.distinct()
        vertx.eventBus().publish(addAddress, JsonArray(topics))
        store.putAll(messages.map { Pair(it.topicName, it) })
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
        // Use the efficient topic tree browsing method
        index.findBrowseTopics(topicPattern, callback)
    }

    override fun findTopicsByName(name: String, ignoreCase: Boolean, namespace: String): List<String> {
        val matcher = createNameMatcher(name, ignoreCase)
        val namespacePrefix = namespace.takeIf { it.isNotEmpty() }?.let { "$it/" }

        return store.keys.toList()
            .asSequence()
            .filter { topic -> !topic.endsWith("/${Const.CONFIG_TOPIC}") && topic != Const.CONFIG_TOPIC }
            .filter { topic -> namespacePrefix == null || topic.startsWith(namespacePrefix, ignoreCase) }
            .filter { topic -> matcher(topic) }
            .sorted()
            .toList()
    }

    override fun findTopicsByConfig(config: String, description: String, ignoreCase: Boolean, namespace: String): List<Pair<String, String>> {
        val namespacePrefix = namespace.takeIf { it.isNotEmpty() }?.let { "$it/" }

        return store.values.toList()
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
        // No-op: LRU eviction in LinkedHashMap handles cleanup automatically.
        // If maxMemoryEntries is set, old entries are evicted when size exceeds the limit,
        // making time-based purging unnecessary.
        return PurgeResult(0, 0)
    }

    override fun dropStorage(): Boolean {
        return try {
            store.clear()
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
