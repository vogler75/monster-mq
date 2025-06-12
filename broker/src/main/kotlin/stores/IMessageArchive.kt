package at.rocworks.stores

import at.rocworks.data.MqttMessage
import java.time.Instant

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
    fun addHistory(messages: List<MqttMessage>)
    fun getHistory(
        topic: String,
        startTime: Instant? = null,
        endTime: Instant? = null,
        limit: Int = 1000
    ): List<MqttMessage>
}