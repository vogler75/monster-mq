package at.rocworks

data class TopicTreeNode (
    val children: MutableMap<String, TopicTreeNode> = mutableMapOf(), // Level to Node
    val clients: MutableSet<ClientId> = mutableSetOf()
)