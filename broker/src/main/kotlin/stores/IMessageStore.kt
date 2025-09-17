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

    /**
     * Find topic names that match the given pattern for efficient topic tree browsing.
     * This method efficiently discovers topic hierarchy without loading message content.
     *
     * For example:
     * - Pattern "a/+" with topics "a/b/x" and "a/c/y" returns ["a/b", "a/c"]
     * - Pattern "a/b/+" with topic "a/b/c/d" returns ["a/b/c"]
     * - Pattern "a/#" returns all topics under "a/"
     *
     * @param topicPattern MQTT topic pattern with + (single level) or # (multi level) wildcards
     * @param callback Called for each matching topic name. Return false to stop iteration.
     */
    fun findMatchingTopics(topicPattern: String, callback: (String) -> Boolean)

    fun purgeOldMessages(olderThan: Instant): PurgeResult

    fun dropStorage(): Boolean

    fun getConnectionStatus(): Boolean
}

interface IMessageStoreExtended : IMessageStore {
    fun findTopicsByName(name: String, ignoreCase: Boolean, namespace: String) : List<String>
    fun findTopicsByConfig(config: String, description: String, ignoreCase: Boolean, namespace: String) : List<Pair<String, String>>
}