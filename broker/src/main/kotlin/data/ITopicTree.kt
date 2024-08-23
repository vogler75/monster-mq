package at.rocworks.data

interface ITopicTree<T> {
    fun add(topicName: MqttTopicName)
    fun add(topicName: MqttTopicName, data: T?)
    fun del(topicName: MqttTopicName)
    fun del(topicName: MqttTopicName, data: T?)

    /*
       The given topicName will be matched with potential wildcard topics of the tree (tree contains wildcard topics)
        */
    fun findDataOfTopicName(topicName: MqttTopicName): List<T>

    /*
       The given topicName can contain wildcards and this will be matched with the tree topics without wildcards
        */
    fun findMatchingTopicNames(topicName: MqttTopicName): List<MqttTopicName>
}