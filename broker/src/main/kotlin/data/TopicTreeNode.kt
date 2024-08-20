package at.rocworks.data

data class TopicTreeNode (
    val children: MutableMap<String, TopicTreeNode> = mutableMapOf(), // Level to Node
    val clients: MutableSet<MqttClientId> = mutableSetOf()
)