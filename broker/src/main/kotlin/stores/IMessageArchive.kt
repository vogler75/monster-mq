package at.rocworks.stores

import at.rocworks.data.BrokerMessage
import at.rocworks.data.PurgeResult
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
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

    /**
     * Get aggregated history data with time bucketing for charting and analytics.
     * Performs database-level aggregation for efficiency.
     *
     * @param topics List of exact topic names (no wildcards)
     * @param startTime Start of time range
     * @param endTime End of time range
     * @param intervalMinutes Bucket size in minutes (1, 5, 15, 60, 1440)
     * @param functions List of aggregation functions ("AVG", "MIN", "MAX", "COUNT")
     * @param fields Optional JSON field paths to extract (empty = use raw numeric payload)
     * @return JsonObject with:
     *   - "columns": JsonArray of column names ["timestamp", "topic_field_func", ...]
     *   - "rows": JsonArray of JsonArrays, each row is [timestamp, value, value, ...]
     */
    fun getAggregatedHistory(
        topics: List<String>,
        startTime: Instant,
        endTime: Instant,
        intervalMinutes: Int,
        functions: List<String>,
        fields: List<String>
    ): JsonObject {
        // Default implementation returns empty result
        // Subclasses should override with database-specific aggregation
        return JsonObject()
            .put("columns", JsonArray().add("timestamp"))
            .put("rows", JsonArray())
    }
}