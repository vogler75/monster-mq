package at.rocworks

import at.rocworks.codecs.MqttTopicName

class TopicTree {
    private val root = TopicTreeNode()

    fun add(topicName: MqttTopicName) = add(topicName, null)
    fun add(topicName: MqttTopicName, client: ClientId?) {
        fun addTopicNode(node: TopicTreeNode, first: String, rest: List<String>) {
            val child = node.children.getOrPut(first) { TopicTreeNode() }
            if (rest.isEmpty()) {
                client?.let { child.clients.add(it) }
            } else {
                addTopicNode(child, rest.first(), rest.drop(1))
            }
        }
        val xs = topicName.getLevels()
        if (xs.isNotEmpty()) addTopicNode(root, xs.first(), xs.drop(1))
    }

    fun del(topicName: MqttTopicName) = del(topicName, null)
    fun del(topicName: MqttTopicName, client: ClientId?) {
        fun delTopicNode(node: TopicTreeNode, first: String, rest: List<String>) {
            fun deleteIfEmpty(child: TopicTreeNode) {
                if (child.clients.isEmpty() && child.children.isEmpty()) {
                    node.children.remove(first)
                }
            }
            if (rest.isEmpty()) {
                node.children[first]?.let { child ->
                    client?.let { child.clients.remove(it) }
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

    /*
    The given topicName will be matched with potential wildcard topics of the tree (tree contains wildcard topics)
     */
    fun findClientsOfTopicName(topicName: MqttTopicName): List<ClientId> {
        fun find(node: TopicTreeNode, current: String, rest: List<String>): List<ClientId> {
            return node.children.flatMap { child ->
                when (child.key) {
                    "#" -> child.value.clients.toList()
                    "+", current -> child.value.clients.toList() +
                            if (rest.isNotEmpty()) find(child.value, rest.first(), rest.drop(1))
                            else listOf()
                    else -> listOf()
                }
            }
        }
        val xs = topicName.getLevels()
        return if (xs.isNotEmpty()) find(root, xs.first(), xs.drop(1)) else listOf()
    }

    /*
    The given topicName can contain wildcards and this will be matched with the tree topics without wildcards
     */
    fun findMatchingTopicNames(topicName: MqttTopicName): List<MqttTopicName> {
        fun find(node: TopicTreeNode, current: String, rest: List<String>, topic: MqttTopicName?): List<MqttTopicName> {
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

    override fun toString(): String {
        return "TopicTree dump: \n" + printTreeNode("", root).joinToString("\n")
    }

    private fun printTreeNode(root: String, node: TopicTreeNode, level: Int = -1): List<String> {
        val text = mutableListOf<String>()
        if (level > -1) text.add("  ".repeat(level)+"- [${root.padEnd(40-level*2)}] Clients "+node.clients.joinToString {"[$it]"})
        node.children.forEach {
            text.addAll(printTreeNode(it.key, it.value, level + 1))
        }
        return text
    }
}