package at.rocworks

class TopicTree {
    private val root = TopicNode()

    fun add(topicName: TopicName) = add(topicName, null)
    fun add(topicName: TopicName, client: ClientId?) {
        fun addTopicNode(node: TopicNode, first: String, rest: List<String>) {
            val child = node.children.getOrPut(first) { TopicNode() }
            if (rest.isEmpty()) {
                client?.let { child.clients.add(it) }
            } else {
                addTopicNode(child, rest.first(), rest.drop(1))
            }
        }
        val xs = topicName.getLevels()
        if (xs.isNotEmpty()) addTopicNode(root, xs.first(), xs.drop(1))
    }

    fun del(topicName: TopicName) = del(topicName, null)
    fun del(topicName: TopicName, client: ClientId?) {
        fun delTopicNode(node: TopicNode, first: String, rest: List<String>) {
            fun deleteIfEmpty(child: TopicNode) {
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
    fun findClientsOfTopicName(topicName: TopicName): List<ClientId> {
        fun find(node: TopicNode, current: String, rest: List<String>): List<ClientId> {
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
    fun findMatchingTopicNames(topicName: TopicName): List<TopicName> {
        fun find(node: TopicNode, current: String, rest: List<String>, topic: TopicName?): List<TopicName> {
            return if (node.children.isEmpty() && rest.isEmpty()) // is leaf
                if (topic==null) listOf() else listOf(topic)
            else
                node.children.flatMap { child ->
                    when (current) {
                        "#" -> find(child.value,"#", listOf(), topic?.addLevel(child.key) ?: TopicName(child.key), )
                        "+", child.key -> if (rest.isNotEmpty()) find(child.value, rest.first(), rest.drop(1), topic?.addLevel(child.key) ?: TopicName(child.key))
                               else listOf(topic?.addLevel(child.key) ?: TopicName(child.key))
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

    private fun printTreeNode(root: String, node: TopicNode, level: Int = -1): List<String> {
        val text = mutableListOf<String>()
        if (level > -1) text.add("  ".repeat(level)+"- [${root.padEnd(40-level*2)}] Clients "+node.clients.joinToString {"[$it]"})
        node.children.forEach {
            text.addAll(printTreeNode(it.key, it.value, level + 1))
        }
        return text
    }
}