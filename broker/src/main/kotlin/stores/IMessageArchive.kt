package at.rocworks.stores

import at.rocworks.data.MqttMessage
import at.rocworks.data.PurgeResult
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import java.time.Instant

enum class MessageArchiveType {
    NONE,
    POSTGRES,
    CRATEDB,
    MONGODB,
    KAFKA,
    SQLITE
}

interface IMessageArchive {
    fun getName(): String
    fun getType(): MessageArchiveType
    fun addHistory(messages: List<MqttMessage>)

    fun purgeOldMessages(olderThan: Instant): PurgeResult

    fun dropStorage(): Boolean
}

interface IMessageArchiveExtended: IMessageArchive {
    fun getHistory(
        topic: String,
        startTime: Instant? = null,
        endTime: Instant? = null,
        limit: Int = 100
    ): JsonArray

    fun executeQuery(sql: String): JsonArray
}