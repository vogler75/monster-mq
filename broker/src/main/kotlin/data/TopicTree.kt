package at.rocworks.data

interface TopicTree {
    fun add(topicName: MqttTopicName)
    fun add(topicName: MqttTopicName, data: String?)

    fun addAll(topicNames: List<MqttTopicName>) {
        topicNames.forEach(::add)
    }

    fun del(topicName: MqttTopicName)
    fun del(topicName: MqttTopicName, data: String?)

    /*
       The given topicName will be matched with potential wildcard topics of the tree (tree contains wildcard topics)
        */
    fun findDataOfTopicName(topicName: MqttTopicName): List<String>

    /*
       The given topicName can contain wildcards and this will be matched with the tree topics without wildcards
        */
    fun findMatchingTopicNames(topicName: MqttTopicName): List<MqttTopicName>
    fun findMatchingTopicNames(topicName: MqttTopicName, callback: (MqttTopicName)->Boolean)

}