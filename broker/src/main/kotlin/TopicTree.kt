package at.rocworks

import com.hazelcast.client.Client

class TopicTree {
    private val root = TopicNode()

    fun add(topicName: TopicName, client: ClientId) {
        fun addTopicNode(node: TopicNode, first: String, rest: List<String>) {
            val child = node.children.getOrPut(first) { TopicNode() }
            if (rest.isEmpty()) {
                child.clients.add(client)
            } else {
                addTopicNode(child, rest.first(), rest.drop(1))
            }
        }
        val xs = topicName.getLevels()
        if (xs.isNotEmpty()) addTopicNode(root, xs.first(), xs.drop(1))
    }

    fun del(topicName: TopicName, client: ClientId) {
        fun delTopicNode(node: TopicNode, first: String, rest: List<String>) {
            fun deleteIfEmpty(child: TopicNode) {
                if (child.clients.isEmpty() && child.children.isEmpty()) {
                    node.children.remove(first)
                }
            }
            if (rest.isEmpty()) {
                node.children[first]?.let { child ->
                    child.clients.remove(client)
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

    fun findClients(topicName: TopicName): List<ClientId> {
        fun findTopicNode(node: TopicNode, first: String, rest: List<String>): List<ClientId> {
            return node.children.flatMap { child ->
                when (child.key) {
                    "#" -> child.value.clients.toList()
                    "+", first -> child.value.clients.toList() +
                            if (rest.isNotEmpty()) findTopicNode(child.value, rest.first(), rest.drop(1))
                            else listOf()
                    else -> listOf()
                }
            }
        }
        val xs = topicName.getLevels()
        return if (xs.isNotEmpty()) findTopicNode(root, xs.first(), xs.drop(1)) else listOf()
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