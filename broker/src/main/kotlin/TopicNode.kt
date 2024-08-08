package at.rocworks

data class TopicNode (
    val children: MutableMap<String, TopicNode> = mutableMapOf(), // Level to Node
    val clients: MutableSet<ClientId> = mutableSetOf()
)