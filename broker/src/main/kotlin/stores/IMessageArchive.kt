package at.rocworks.stores

import at.rocworks.data.MqttMessage

enum class MessageArchiveType {
    NONE,
    POSTGRES,
    CRATEDB
}

interface IMessageArchive {
    fun getName(): String
    fun getType(): MessageStoreType
    fun addAllHistory(messages: List<MqttMessage>)
}