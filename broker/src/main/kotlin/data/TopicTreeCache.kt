package at.rocworks.data

import at.rocworks.Const
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import java.util.*
import java.util.logging.Logger

import kotlin.system.exitProcess

class TopicTreeCache(hazelcast: HazelcastInstance, cacheName: String) : ITopicTree {
    private val logger = Logger.getLogger(this.javaClass.simpleName)
    //private val cachingProvider: CachingProvider = Caching.getCachingProvider()
    //private val cacheManager: CacheManager = cachingProvider.cacheManager
    private val rootNodeId = "0"

    data class Node (
        val children: HashMap<String, String> = hashMapOf(), // Level to Node(UUID key in cache)
        val dataset: MutableSet<String> = mutableSetOf()
    )

    //private val cacheConfig = MutableConfiguration<String, Node>()
    //    .setExpiryPolicyFactory { CreatedExpiryPolicy(Duration.ETERNAL) }
    //    .setStatisticsEnabled(true)  // Enable statistics

    //private val cache: Cache<String, Node> = cacheManager.createCache(cacheName, cacheConfig)
    private val cache: IMap<String, Node> = hazelcast.getMap(cacheName)

    init {
        logger.level = Const.DEBUG_LEVEL
        cache[rootNodeId] ?: run { cache.put(rootNodeId, Node()) }
    }

    private fun newNode(): String {
        val id = UUID.randomUUID().toString()
        val node = Node()
        try {
            cache.put(id, node)
            return id
        } catch (e: Exception) {
            logger.severe("Error adding item to cache [${e.message}]")
            throw e
        }
    }

    override fun add(topicName: MqttTopicName) = add(topicName, null)

    override fun add(topicName: MqttTopicName, data: String?) {
        fun addTopicNode(nodeId: String, topic: String, rest: List<String>) {
            val node = cache[nodeId]
            if (node != null) {
                val childId = node.children[topic]?:let {
                    newNode().also { newNodeId ->
                        node.children[topic] = newNodeId
                        cache.put(nodeId, node) // Update node with new data
                        if (cache[newNodeId] == null) {
                            logger.severe("Reread of cache item failed!")
                            exitProcess(-1)
                        }
                    }
                }

                logger.finest("Child [$topic] [$childId]")

                val child = cache[childId]
                if (child != null) {
                    if (rest.isEmpty()) {
                        data?.let {
                            child.dataset.add(it.toString())
                            cache.put(childId, child)
                        }
                    } else {
                        addTopicNode(childId, rest.first(), rest.drop(1))
                    }
                } else {
                    logger.severe("Child item does not exists [$childId]!")
                }
            } else {
                logger.severe("Failed to read cache item!")
            }
        }
        val xs = topicName.getLevels()
        if (xs.isNotEmpty()) addTopicNode(rootNodeId, xs.first(), xs.drop(1))
    }

    override fun del(topicName: MqttTopicName) = del(topicName, null)

    override fun del(topicName: MqttTopicName, data: String?) {
        fun delTopicNode(id: String, node: Node, first: String, rest: List<String>) {
            fun deleteIfEmpty(child: Node) {
                if (child.dataset.isEmpty() && child.children.isEmpty()) {
                    node.children.remove(first)
                    cache.put(id, node)
                }
            }
            if (rest.isEmpty()) {
                node.children[first]?.let { childId ->
                    cache[childId]?.let { child ->
                        data?.let { child.dataset.remove(it) }
                        cache.put(childId, child)
                        deleteIfEmpty(child)
                    }
                }
            } else {
                node.children[first]?.let { childId ->
                    cache[childId]?.let { child ->
                        delTopicNode(childId, child, rest.first(), rest.drop(1))
                        deleteIfEmpty(child)
                    }
                }
            }
        }
        val xs = topicName.getLevels()
        if (xs.isNotEmpty()) delTopicNode(rootNodeId, cache[rootNodeId]!!, xs.first(), xs.drop(1))
    }

    override fun findDataOfTopicName(topicName: MqttTopicName): List<String> {
        fun find(node: Node, current: String, rest: List<String>): List<String> {
            return node.children.flatMap { entry ->
                cache[entry.value]?.let { child ->
                    when (entry.key) {
                        "#" -> child.dataset.toList()
                        "+", current -> child.dataset.toList() +
                                if (rest.isNotEmpty()) find(child, rest.first(), rest.drop(1))
                                else listOf()
                        else -> listOf()
                    }
                } ?: listOf()
            }
        }
        val xs = topicName.getLevels()
        return if (xs.isNotEmpty()) find(cache.get(rootNodeId)!!, xs.first(), xs.drop(1)) else listOf()
    }

    fun findMatchingTopicNames(topicName: MqttTopicName, callback: (MqttTopicName)->Boolean) {
        fun find(node:Node, current: String, rest: List<String>, topic: MqttTopicName?): Boolean {
            logger.finest { "Find Node [$node] Current [$current] Rest [${rest.joinToString(",")}] Topic: [$topic]"}
            if (node.children.isEmpty() && rest.isEmpty()) { // is leaf
                if (topic != null) return callback(topic)
            } else {
                node.children.forEach { entry ->
                    val child = cache[entry.value]
                    if (child == null) {
                        logger.severe("Child entry [${entry.value}] has no object in cache!")
                    } else {
                        val check = topic?.addLevel(entry.key) ?: MqttTopicName(entry.key)
                        logger.finest { "  Check [$check] Child [$child]" }
                        when (current) {
                            "#" -> if (!find(child, "#", listOf(), check)) return false
                            "+", entry.key -> {
                                if (rest.isNotEmpty()) if (!find(child, rest.first(), rest.drop(1), check)) return false
                                else return callback(check)
                            }
                        }
                    }
                }
            }
            return true
        }
        val startTime = System.currentTimeMillis()
        val xs = topicName.getLevels()
        if (xs.isNotEmpty()) find(cache.get(rootNodeId)!!, xs.first(), xs.drop(1), null)
        val endTime = System.currentTimeMillis()
        val elapsedTime = endTime - startTime
        logger.fine { "Found matching topics in [$elapsedTime]ms." }

    }
    override fun findMatchingTopicNames(topicName: MqttTopicName): List<MqttTopicName> {
        fun find(node:Node, current: String, rest: List<String>, topic: MqttTopicName?): List<MqttTopicName> {
            return if (node.children.isEmpty() && rest.isEmpty()) // is leaf
                if (topic==null) listOf() else listOf(topic)
            else
                node.children.flatMap { entry ->
                    cache[entry.value]?.let { child ->
                        val check = topic?.addLevel(entry.key) ?: MqttTopicName(entry.key)
                        when (current) {
                            "#" -> find(child,"#", listOf(), check )
                            "+", entry.key -> if (rest.isNotEmpty()) find(child, rest.first(), rest.drop(1), check)
                            else listOf(check)
                            else -> listOf()
                        }
                    } ?: listOf()
                }
        }
        val startTime = System.currentTimeMillis()
        val xs = topicName.getLevels()
        val ret = if (xs.isNotEmpty()) find(cache.get(rootNodeId)!!, xs.first(), xs.drop(1), null) else listOf()
        val endTime = System.currentTimeMillis()
        val elapsedTime = endTime - startTime
        logger.fine { "Found ${ret.size} matching topics in [$elapsedTime]ms."}
        return ret
    }

    override fun toString(): String {
        return "TopicTree dump: \n" + printTreeNode("", cache.get(rootNodeId)!!).joinToString("\n")
    }

    private fun printTreeNode(root: String, node: Node, level: Int = -1): List<String> {
        val text = mutableListOf<String>()
        if (level > -1) text.add("  ".repeat(level)+"- [${root.padEnd(40-level*2)}] Dataset: "+node.dataset.joinToString {"[$it]"})
        node.children.forEach { child ->
            cache[child.value]?.let { next ->
                text.addAll(printTreeNode(child.key, next, level + 1))
            }
        }
        return text
    }
}