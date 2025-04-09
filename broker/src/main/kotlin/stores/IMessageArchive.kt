package at.rocworks.stores

import at.rocworks.data.MqttMessage

enum class MessageArchiveType {
    NONE,
    POSTGRES,
    CRATEDB,
    MONGODB,
    KAFKA
}

interface IMessageArchive {
    fun getName(): String
    fun getType(): MessageArchiveType
    fun addAllHistory(messages: List<MqttMessage>)
}