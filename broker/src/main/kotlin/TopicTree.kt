package at.rocworks

data class TopicNode (
    val children: MutableMap<String, TopicNode> = mutableMapOf(),
    val clients: MutableSet<String> = mutableSetOf()
)

class TopicTree {
    private val root = TopicNode()

    fun add(topicName: String, client: String) {
        fun addTopicNode(node: TopicNode, first: String, rest: List<String>) {
            val child = node.children.getOrPut(first) { TopicNode() }
            if (rest.isEmpty()) {
                child.clients.add(client)
            } else {
                addTopicNode(child, rest.first(), rest.drop(1))
            }
        }
        val xs = topicName.split("/")
        if (xs.isNotEmpty()) addTopicNode(root, xs.first(), xs.drop(1))
    }

    fun del(topicName: String, client: String) {
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
        val xs = topicName.split("/")
        if (xs.isNotEmpty()) delTopicNode(root, xs.first(), xs.drop(1))
    }

    fun find(topicName: String): List<String> {
        fun findTopicNode(node: TopicNode, first: String, rest: List<String>): List<String> {
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
        val xs = topicName.split("/")
        return if (xs.isNotEmpty()) findTopicNode(root, xs.first(), xs.drop(1)) else listOf()
    }

    override fun toString(): String {
        return "TopicTree dump: \n" + printTreeNode("", root).joinToString("\n")
    }

    private fun printTreeNode(root: String, node: TopicNode, level: Int = -1): List<String> {
        val text = mutableListOf<String>()
        if (level > -1) text.add("  ".repeat(level)+"- [$root] Clients ["+node.clients.joinToString(" | ")+"]")
        node.children.forEach {
            text.addAll(printTreeNode(it.key, it.value, level + 1))
        }
        return text
    }
}