package at.rocworks.data

import at.rocworks.data.TopicTree.Node
import java.util.*
import javax.cache.Cache
import javax.cache.CacheManager
import javax.cache.Caching
import javax.cache.configuration.MutableConfiguration
import javax.cache.spi.CachingProvider

class TopicTreeCache : ITopicTree {
    private val cacheName = UUID.randomUUID().toString()
    private val cachingProvider: CachingProvider = Caching.getCachingProvider()
    private val cacheManager: CacheManager = cachingProvider.cacheManager

    data class Node (
        val children: HashMap<String, String> = hashMapOf(), // Level to Node(UUID key in cache)
        val dataset: MutableSet<String> = mutableSetOf()
    )

    private val cacheConfig = MutableConfiguration<String, Node>()
        .setStoreByValue(true)  // Cache stores copies of the values
        .setStatisticsEnabled(true)  // Enable statistics

    private val cache: Cache<String, Node> = cacheManager.createCache(cacheName, cacheConfig)
    private var root: Node? = null

    init {
        root = cache.get("root")
        if (root == null) {
            root = Node()
            cache.put("root", root)
        }
    }

    private fun newNode(): String {
        val id = UUID.randomUUID().toString()
        val node = Node()
        cache.put(id, node)
        return id
    }

    override fun add(topicName: MqttTopicName) = add(topicName, null)

    override fun add(topicName: MqttTopicName, data: String?) {
        fun addTopicNode(id: String, node: Node, first: String, rest: List<String>) {
            val childId = node.children.getOrPut(first) { newNode() }
            cache.put(id, node)
            val child = cache.get(childId)
            if (rest.isEmpty()) {
                data?.let {
                    child.dataset.add(it.toString())
                    cache.put(childId, child)
                }
            } else {
                addTopicNode(childId, child, rest.first(), rest.drop(1))
            }
        }
        val xs = topicName.getLevels()
        if (xs.isNotEmpty()) addTopicNode("root", root!!, xs.first(), xs.drop(1))
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
                    val child = cache.get(childId)
                    data?.let { child.dataset.remove(it.toString()) }
                    cache.put(childId, child)
                    deleteIfEmpty(child)
                }
            } else {
                node.children[first]?.let { childId ->
                    val child = cache.get(childId)
                    delTopicNode(childId, child, rest.first(), rest.drop(1))
                    deleteIfEmpty(child)
                }
            }
        }
        val xs = topicName.getLevels()
        if (xs.isNotEmpty()) delTopicNode("root", root!!, xs.first(), xs.drop(1))    }

    override fun findDataOfTopicName(topicName: MqttTopicName): List<String> {
        fun find(node: Node, current: String, rest: List<String>): List<String> {
            return node.children.flatMap { entry ->
                val child = cache.get(entry.value)
                when (entry.key) {
                    "#" -> child.dataset.toList()
                    "+", current -> child.dataset.toList() +
                            if (rest.isNotEmpty()) find(child, rest.first(), rest.drop(1))
                            else listOf()
                    else -> listOf()
                }
            }
        }
        val xs = topicName.getLevels()
        return if (xs.isNotEmpty()) find(root!!, xs.first(), xs.drop(1)) else listOf()
    }

    override fun findMatchingTopicNames(topicName: MqttTopicName): List<MqttTopicName> {
        fun find(node:Node, current: String, rest: List<String>, topic: MqttTopicName?): List<MqttTopicName> {
            return if (node.children.isEmpty() && rest.isEmpty()) // is leaf
                if (topic==null) listOf() else listOf(topic)
            else
                node.children.flatMap { entry ->
                    val child = cache.get(entry.value)
                    when (current) {
                        "#" -> find(child,"#", listOf(), topic?.addLevel(entry.key) ?: MqttTopicName(entry.key), )
                        "+", entry.key -> if (rest.isNotEmpty()) find(child, rest.first(), rest.drop(1), topic?.addLevel(entry.key) ?: MqttTopicName(entry.key))
                        else listOf(topic?.addLevel(entry.key) ?: MqttTopicName(entry.key))
                        else -> listOf()
                    }
                }
        }
        val xs = topicName.getLevels()
        return if (xs.isNotEmpty()) find(root!!, xs.first(), xs.drop(1), null) else listOf()
    }

    override fun toString(): String {
        return "TopicTree dump: \n" + printTreeNode("", root!!).joinToString("\n")
    }

    private fun printTreeNode(root: String, node: Node, level: Int = -1): List<String> {
        val text = mutableListOf<String>()
        if (level > -1) text.add("  ".repeat(level)+"- [${root.padEnd(40-level*2)}] Dataset: "+node.dataset.joinToString {"[$it]"})
        node.children.forEach {
            val node = cache.get(it.value)
            text.addAll(printTreeNode(it.key, node, level + 1))
        }
        return text
    }
}