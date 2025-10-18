package at.rocworks.stores

import at.rocworks.data.BrokerMessage
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

enum class PayloadFormat {
    DEFAULT,
    JSON;

    companion object {
        fun parse(value: String?): PayloadFormat {
            return when (value?.uppercase()) {
                null, "", "DEFAULT" -> DEFAULT
                "JSON" -> JSON
                else -> DEFAULT
            }
        }
    }
}

interface IMessageArchive {
    fun getName(): String
    fun getType(): MessageArchiveType
    fun addHistory(messages: List<BrokerMessage>)

    fun purgeOldMessages(olderThan: Instant): PurgeResult

    fun dropStorage(): Boolean

    fun getConnectionStatus(): Boolean

    /**
     * Check if the underlying storage table/collection exists.
     * Used in cluster scenarios to verify table creation.
     */
    suspend fun tableExists(): Boolean

    /**
     * Explicitly create the underlying storage table/collection.
     * Used in create/update operations to ensure table exists.
     * Returns true if table was created or already existed, false on error.
     */
    suspend fun createTable(): Boolean
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