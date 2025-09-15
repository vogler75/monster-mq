package at.rocworks.stores

import at.rocworks.data.MqttMessage
import at.rocworks.data.PurgeResult
import java.time.Instant

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
    fun getAsync(topicName: String, callback: (MqttMessage?) -> Unit)

    fun addAll(messages: List<MqttMessage>)
    fun delAll(topics: List<String>)
    fun findMatchingMessages(topicName: String, callback: (MqttMessage)->Boolean)

    fun purgeOldMessages(olderThan: Instant): PurgeResult

    fun dropStorage(): Boolean
}

interface IMessageStoreExtended : IMessageStore {
    fun findTopicsByName(name: String, ignoreCase: Boolean, namespace: String) : List<String>
    fun findTopicsByConfig(config: String, description: String, ignoreCase: Boolean, namespace: String) : List<Pair<String, String>>
}