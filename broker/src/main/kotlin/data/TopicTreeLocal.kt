package at.rocworks.data

import io.vertx.core.impl.ConcurrentHashSet
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

class TopicTreeLocal : TopicTree {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    data class Node (
        val children: ConcurrentHashMap<String, Node> = ConcurrentHashMap(), // Level to Node
        val dataset: ConcurrentHashSet<String> = ConcurrentHashSet()
    )

    private val root = Node()

    override fun add(topicName: MqttTopicName) = add(topicName, null)
    override fun add(topicName: MqttTopicName, data: String?) {
        fun addTopicNode(node: Node, first: String, rest: List<String>) {
            val child = node.children.getOrPut(first) { Node() }
            if (rest.isEmpty()) {
                data?.let { child.dataset.add(it) }
            } else {
                addTopicNode(child, rest.first(), rest.drop(1))
            }
        }
        val xs = topicName.getLevels()
        if (xs.isNotEmpty()) addTopicNode(root, xs.first(), xs.drop(1))
    }

    override fun del(topicName: MqttTopicName) = del(topicName, null)
    override fun del(topicName: MqttTopicName, data: String?) {
        fun delTopicNode(node: Node, first: String, rest: List<String>) {
            fun deleteIfEmpty(child: Node) {
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
        val xs = topicName.getLevels()
        if (xs.isNotEmpty()) delTopicNode(root, xs.first(), xs.drop(1))
    }

    override fun findDataOfTopicName(topicName: MqttTopicName): List<String> {
        fun find(node: Node, current: String, rest: List<String>): List<String> {
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
        val xs = topicName.getLevels()
        return if (xs.isNotEmpty()) find(root, xs.first(), xs.drop(1)) else listOf()
    }

    override fun findMatchingTopicNames(topicName: MqttTopicName): List<MqttTopicName> {
        fun find(node: Node, current: String, rest: List<String>, topic: MqttTopicName?): List<MqttTopicName> {
            return if (node.children.isEmpty() && rest.isEmpty()) // is leaf
                if (topic==null) listOf() else listOf(topic)
            else
                node.children.flatMap { child ->
                    when (current) {
                        "#" -> find(child.value,"#", listOf(), topic?.addLevel(child.key) ?: MqttTopicName(child.key), )
                        "+", child.key -> if (rest.isNotEmpty()) find(child.value, rest.first(), rest.drop(1), topic?.addLevel(child.key) ?: MqttTopicName(child.key))
                               else listOf(topic?.addLevel(child.key) ?: MqttTopicName(child.key))
                        else -> listOf()
                    }
                }
        }
        val xs = topicName.getLevels()
        return if (xs.isNotEmpty()) find(root, xs.first(), xs.drop(1), null) else listOf()
    }

    override fun findMatchingTopicNames(topicName: MqttTopicName, callback: (MqttTopicName)->Boolean) {
        fun find(node: Node, current: String, rest: List<String>, topic: MqttTopicName?): Boolean {
            logger.finest { "Find Node [$node] Current [$current] Rest [${rest.joinToString(",")}] Topic: [$topic]"}
            if (node.children.isEmpty() && rest.isEmpty()) { // is leaf
                if (topic != null) return callback(topic)
            } else {
                node.children.forEach { child ->
                    val check = topic?.addLevel(child.key) ?: MqttTopicName(child.key)
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
        val xs = topicName.getLevels()
        if (xs.isNotEmpty()) find(root, xs.first(), xs.drop(1), null)
        val endTime = System.currentTimeMillis()
        val elapsedTime = endTime - startTime
        logger.fine { "Found matching topics in [$elapsedTime]ms." }
    }

    override fun toString(): String {
        return "TopicTree dump: \n" + printTreeNode("", root).joinToString("\n")
    }

    private fun printTreeNode(root: String, node: Node, level: Int = -1): List<String> {
        val text = mutableListOf<String>()
        if (level > -1) text.add("  ".repeat(level)+"- [${root.padEnd(40-level*2)}] Dataset: "+node.dataset.joinToString {"[$it]"})
        node.children.forEach {
            text.addAll(printTreeNode(it.key, it.value, level + 1))
        }
        return text
    }
}