package at.rocworks.data

import at.rocworks.Utils
import io.vertx.core.impl.ConcurrentHashSet
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

class TopicTree<T> : ITopicTree<T> {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    data class Node<T> (
        val children: ConcurrentHashMap<String, Node<T>> = ConcurrentHashMap(), // Level to Node
        val dataset: ConcurrentHashSet<T> = ConcurrentHashSet()
    )

    private val root = Node<T>()

    override fun getType(): TopicTreeType = TopicTreeType.LOCAL

    override fun add(topicName: String) = add(topicName, null)
    override fun add(topicName: String, data: T?) {
        fun addTopicNode(node: Node<T>, first: String, rest: List<String>) {
            val child = node.children.getOrPut(first) { Node() }
            if (rest.isEmpty()) {
                data?.let { child.dataset.add(it) }
            } else {
                addTopicNode(child, rest.first(), rest.drop(1))
            }
        }
        val xs = Utils.getTopicLevels(topicName)
        if (xs.isNotEmpty()) addTopicNode(root, xs.first(), xs.drop(1))
    }

    override fun del(topicName: String) = del(topicName, null)
    override fun del(topicName: String, data: T?) {
        fun delTopicNode(node: Node<T>, first: String, rest: List<String>) {
            fun deleteIfEmpty(child: Node<T>) {
                if (child.dataset.isEmpty() && child.children.isEmpty()) {
                    node.children.remove(first)
                }
            }
            if (rest.isEmpty()) {
                node.children[first]?.let { child ->
                    data?.let { child.dataset.remove(it) }
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

    override fun findDataOfTopicName(topicName: String): List<T> {
        fun find(node: Node<T>, current: String, rest: List<String>): List<T> {
            return node.children.flatMap { child ->
                when (child.key) {
                    "#" -> child.value.dataset.toList()
                    "+", current -> child.value.dataset.toList() +
                            if (rest.isNotEmpty()) find(child.value, rest.first(), rest.drop(1))
                            else listOf()
                    else -> listOf()
                }
            }
        }
        val xs = Utils.getTopicLevels(topicName)
        return if (xs.isNotEmpty()) find(root, xs.first(), xs.drop(1)) else listOf()
    }

    override fun findMatchingTopicNames(topicName: String): List<String> {
        fun find(node: Node<T>, current: String, rest: List<String>, topic: String?): List<String> {
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
        fun find(node: Node<T>, current: String, rest: List<String>, topic: String?): Boolean {
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

    private fun printTreeNode(root: String, node: Node<T>, level: Int = -1): List<String> {
        val text = mutableListOf<String>()
        if (level > -1) text.add("  ".repeat(level)+"- [${root.padEnd(40-level*2)}] Dataset: "+node.dataset.joinToString {"[$it]"})
        node.children.forEach {
            text.addAll(printTreeNode(it.key, it.value, level + 1))
        }
        return text
    }
}