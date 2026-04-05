package at.rocworks.stores.devices

import io.vertx.core.json.JsonObject

/**
 * Common interface for Logger configurations (JDBC, HTTP/InfluxDB, etc.)
 */
interface ILoggerConfig {
    val topicFilters: List<String>
    val tableName: String?
    val tableNameJsonPath: String?
    val topicNameColumn: String?
    val payloadFormat: String
    val jsonSchema: JsonObject
    val queueType: String
    val queueSize: Int
    val diskPath: String
    val bulkSize: Int
    val bulkTimeoutMs: Long
    val reconnectDelayMs: Long

    fun toJson(): JsonObject
    fun validate(): List<String>
}
