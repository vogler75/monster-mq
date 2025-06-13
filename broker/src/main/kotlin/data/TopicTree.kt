package at.rocworks.data

import at.rocworks.Const
import at.rocworks.Utils
import io.vertx.core.impl.ConcurrentHashSet
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

class TopicTree<K, V> : ITopicTree<K, V> {
    private val logger = Utils.getLogger(this::class.java)

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    data class Node<K, V> (
        val children: ConcurrentHashMap<String, Node<K, V>> = ConcurrentHashMap(), // Level to Node
        val dataset: ConcurrentHashMap<K, V> = ConcurrentHashMap()
    )

    private val root = Node<K, V>()

    override fun getType(): TopicTreeType = TopicTreeType.LOCAL

    override fun add(topicName: String) = add(topicName, null, null)
    override fun add(topicName: String, key: K?, value: V?) {
        logger.finest { "Add topic [$topicName] key [$key] value [$value]" }
        fun addTopicNode(node: Node<K, V>, first: String, rest: List<String>) {
            val child = node.children.getOrPut(first) { Node() }
            if (rest.isEmpty()) {
                if (key!=null && value!=null) {
                    child.dataset[key] = value
                }
            } else {
                addTopicNode(child, rest.first(), rest.drop(1))
            }
        }
        val xs = Utils.getTopicLevels(topicName)
        if (xs.isNotEmpty()) addTopicNode(root, xs.first(), xs.drop(1))
    }

    override fun del(topicName: String) = del(topicName, null)
    override fun del(topicName: String, key: K?) {
        logger.finest { "Delete topic [$topicName] key [$key]" }
        fun delTopicNode(node: Node<K, V>, first: String, rest: List<String>) {
            fun deleteIfEmpty(child: Node<K, V>) {
                if (child.dataset.isEmpty() && child.children.isEmpty()) {
                    node.children.remove(first)
                }
            }
            if (rest.isEmpty()) {
                node.children[first]?.let { child ->
                    key?.let { child.dataset.remove(it) }
                    deleteIfEmpty(child)
                }
            } else {
                node.children[first]?.let { child ->
                    delTopicNode(child, rest.first(), rest.drop(1))
                    deleteIfEmpty(child)
                }
            }
        }
        val xs = Utils.getTopicLevels(topicName)
        if (xs.isNotEmpty()) delTopicNode(root, xs.first(), xs.drop(1))
    }

    override fun isTopicNameMatching(topicName: String): Boolean {
        fun find(node: Node<K, V>, current: String, rest: List<String>): Boolean {
            return node.children.any { child ->
                when (child.key) {
                    "#" -> true
                    "+", current -> (rest.isEmpty() && child.value.dataset.isNotEmpty() )
                            || (rest.isNotEmpty() && find(child.value, rest.first(), rest.drop(1)))
                    else -> false
                }
            }
        }
        val xs = Utils.getTopicLevels(topicName)
        return if (xs.isNotEmpty()) find(root, xs.first(), xs.drop(1)) else false
    }

    override fun findDataOfTopicName(topicName: String): List<Pair<K, V>> {
        fun find(node: Node<K, V>, current: String, rest: List<String>, level: Int): List<Pair<K, V>> {
            return node.children.flatMap { child ->
                when (child.key) {
                    "#" -> if (level == 1 && current == "\$SYS") listOf() else child.value.dataset.toList()
                    "+" -> {
                        (if (rest.isEmpty()) child.value.dataset.toList() else listOf()) +
                                (if (rest.isNotEmpty()) find(child.value, rest.first(), rest.drop(1), level+1)
                                else listOf())
                    }
                    current -> {
                        (if (rest.isEmpty()) child.value.dataset.toList() else listOf()) +
                                (if (rest.isNotEmpty()) find(child.value, rest.first(), rest.drop(1), level+1)
                                else find(child.value, "", listOf(), level+1))
                    }
                    else -> listOf()
                }
            }
        }
        val xs = Utils.getTopicLevels(topicName)
        return if (xs.isNotEmpty()) find(root, xs.first(), xs.drop(1), 1) else listOf()
    }

    override fun findMatchingTopicNames(topicName: String): List<String> {
        fun find(node: Node<K, V>, current: String, rest: List<String>, topic: String?): List<String> {
            return if (node.children.isEmpty() && rest.isEmpty()) // is leaf
                if (topic==null) listOf() else listOf(topic)
            else
                node.children.flatMap { child ->
                    when (current) {
                        "#" -> find(child.value,"#", listOf(), topic?.let { Utils.addTopicLevel(it, child.key) } ?: child.key)
                        "+", child.key -> if (rest.isNotEmpty()) find(child.value, rest.first(), rest.drop(1), topic?.let { Utils.addTopicLevel(it, child.key) } ?: child.key)
                               else listOf(topic?.let { Utils.addTopicLevel(it, child.key) } ?: child.key)
                        else -> listOf()
                    }
                }
        }
        val xs = Utils.getTopicLevels(topicName)
        return if (xs.isNotEmpty()) find(root, xs.first(), xs.drop(1), null) else listOf()
    }

    override fun findMatchingTopicNames(topicName: String, callback: (String)->Boolean) {
        fun find(node: Node<K, V>, current: String, rest: List<String>, topic: String?): Boolean {
            logger.finest { "Find Node [$node] Current [$current] Rest [${rest.joinToString(",")}] Topic: [$topic]"}
            if (node.children.isEmpty() && rest.isEmpty()) { // is leaf
                if (topic != null) return callback(topic)
            } else {
                node.children.forEach { child ->
                    val check = topic?.let { Utils.addTopicLevel(it, child.key) } ?: child.key
                    logger.finest { "  Check [$check] Child [$child]" }
                    when (current) {
                        "#" -> if (!find(child.value, "#", listOf(), check)) return false
                        "+", child.key -> {
                            if (rest.isNotEmpty()) if (!find(child.value, rest.first(), rest.drop(1), check)) return false
                            else return callback(check)
                        }
                    }
                }
            }
            return true
        }
        val startTime = System.currentTimeMillis()
        val xs = Utils.getTopicLevels(topicName)
        if (xs.isNotEmpty()) find(root, xs.first(), xs.drop(1), null)
        val endTime = System.currentTimeMillis()
        val elapsedTime = endTime - startTime
        logger.fine { "Found matching topics in [$elapsedTime]ms." }
    }

    override fun toString(): String {
        return "TopicTree dump: \n" + printTreeNode("", root).joinToString("\n")
    }

    private fun printTreeNode(root: String, node: Node<K, V>, level: Int = -1): List<String> {
        val text = mutableListOf<String>()
        if (level > -1) text.add("  ".repeat(level)+"- [${root.padEnd(40-level*2)}] Dataset: "+node.dataset.keys.joinToString {"[$it]"})
        node.children.forEach {
            text.addAll(printTreeNode(it.key, it.value, level + 1))
        }
        return text
    }
}