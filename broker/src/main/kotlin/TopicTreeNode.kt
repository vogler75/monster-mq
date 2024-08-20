package at.rocworks

import at.rocworks.codecs.MqttClientId

data class TopicTreeNode (
    val children: MutableMap<String, TopicTreeNode> = mutableMapOf(), // Level to Node
    val clients: MutableSet<MqttClientId> = mutableSetOf()
)