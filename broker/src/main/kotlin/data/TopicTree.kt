package at.rocworks.data

import at.rocworks.Const
import at.rocworks.Utils
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
                    "#" -> if (level == 1 && current == Const.SYS_TOPIC_NAME) listOf() else child.value.dataset.toList()
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
                            if (rest.isNotEmpty()) {
                                if (!find(child.value, rest.first(), rest.drop(1), check)) return false
                            } else {
                                if (!callback(check)) return false
                            }
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

    /**
     * Find topic names for browsing based on MQTT topic patterns.
     * For single-level wildcard (+), returns only the next level topics.
     * For multi-level wildcard (#), returns all matching topics.
     *
     * Example: If pattern is "a/+" and we have topics "a/b/x" and "a/c/y",
     * this returns ["a/b", "a/c"] (only the first level after 'a').
     */
    fun findBrowseTopics(topicPattern: String, callback: (String) -> Boolean) {
        fun browse(node: Node<K, V>, patternLevels: List<String>, currentTopicLevels: List<String>): Boolean {
            if (patternLevels.isEmpty()) {
                // We've consumed all pattern levels - return the current topic
                if (currentTopicLevels.isNotEmpty()) {
                    val topicName = currentTopicLevels.joinToString("/")
                    return callback(topicName)
                }
                return true
            }

            val currentPattern = patternLevels.first()
            val remainingPattern = patternLevels.drop(1)

            return when (currentPattern) {
                "+" -> {
                    // Single-level wildcard: for each child, either return this level or continue deeper
                    node.children.all { (childName, childNode) ->
                        val newTopicLevels = currentTopicLevels + childName

                        if (remainingPattern.isEmpty()) {
                            // This is the last level in pattern - return this topic level
                            val topicName = newTopicLevels.joinToString("/")
                            callback(topicName)
                        } else {
                            // More pattern levels - continue browsing deeper
                            browse(childNode, remainingPattern, newTopicLevels)
                        }
                    }
                }
                "#" -> {
                    // Multi-level wildcard: return all topics at any depth
                    fun traverseAll(node: Node<K, V>, topicPath: List<String>): Boolean {
                        // Return current topic if we have a path
                        if (topicPath.isNotEmpty()) {
                            val topicName = topicPath.joinToString("/")
                            if (!callback(topicName)) return false
                        }

                        // Traverse all children
                        return node.children.all { (childName, childNode) ->
                            traverseAll(childNode, topicPath + childName)
                        }
                    }
                    traverseAll(node, currentTopicLevels)
                }
                else -> {
                    // Exact match: find the matching child and continue
                    val childNode = node.children[currentPattern]
                    if (childNode != null) {
                        val newTopicLevels = currentTopicLevels + currentPattern
                        if (remainingPattern.isEmpty()) {
                            // Pattern complete - return this topic if it exists or has children
                            val topicName = newTopicLevels.joinToString("/")
                            callback(topicName)
                        } else {
                            browse(childNode, remainingPattern, newTopicLevels)
                        }
                    } else {
                        true // No match found, continue
                    }
                }
            }
        }

        val patternLevels = Utils.getTopicLevels(topicPattern)
        if (patternLevels.isNotEmpty()) {
            browse(root, patternLevels, emptyList())
        }
    }

    fun size(): Int {
        fun countNodes(node: Node<K, V>): Int {
            var count = node.dataset.size
            node.children.values.forEach { child ->
                count += countNodes(child)
            }
            return count
        }
        return countNodes(root)
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