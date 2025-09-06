package at.rocworks.stores

import at.rocworks.data.MqttMessage

enum class MessageStoreType {
    NONE,
    MEMORY,
    HAZELCAST,
    POSTGRES,
    CRATEDB,
    MONGODB,
    SQLITE
}

interface IMessageStore {
    fun getName(): String
    fun getType(): MessageStoreType

    operator fun get(topicName: String): MqttMessage?

    fun addAll(messages: List<MqttMessage>)
    fun delAll(topics: List<String>)
    fun findMatchingMessages(topicName: String, callback: (MqttMessage)->Boolean)
}

interface IMessageStoreExtended : IMessageStore {
    fun findTopicsByName(name: String, ignoreCase: Boolean, namespace: String) : List<String>
    fun findTopicsByConfig(config: String, description: String, ignoreCase: Boolean, namespace: String) : List<Pair<String, String>>
}