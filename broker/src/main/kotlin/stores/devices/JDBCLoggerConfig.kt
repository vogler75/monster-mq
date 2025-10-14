package at.rocworks.stores.devices

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/**
 * Configuration for JDBC Logger - Schema-based data ingestion
 * Validates incoming MQTT JSON messages against JSON Schema and writes to pre-existing database tables
 */
data class JDBCLoggerConfig(
    // Database connection
    val databaseType: String,                       // "QuestDB", "PostgreSQL", "TimescaleDB"
    val jdbcUrl: String,                            // JDBC connection URL
    val username: String,
    val password: String,

    // Topic subscription
    val topicFilters: List<String> = emptyList(),   // MQTT wildcard patterns (e.g., "sensors/#")

    // Table configuration
    val tableName: String? = null,                  // Fixed table name (mutually exclusive with tableNameJsonPath)
    val tableNameJsonPath: String? = null,          // JSONPath to extract table name from payload (e.g., "$.metadata.table")

    // Schema and validation
    val payloadFormat: String = "JSON",             // "JSON", "XML" (future support)
    val jsonSchema: JsonObject,                     // JSON Schema for validation and field extraction

    // Queue configuration (memory or disk buffering)
    val queueType: String = "MEMORY",               // "MEMORY" or "DISK"
    val queueSize: Int = 10000,                     // Max buffered messages
    val diskPath: String = "./buffer",              // Path for disk queue files

    // Bulk write configuration
    val bulkSize: Int = 1000,                       // JDBC batch size (trigger write when reached)
    val bulkTimeoutMs: Long = 5000,                 // Max time to collect bulk (trigger write when reached)

    // Connection settings
    val reconnectDelayMs: Long = 5000               // Delay before reconnecting on failure
) {
    companion object {
        fun fromJson(obj: JsonObject): JDBCLoggerConfig {
            val topicFiltersArray = obj.getJsonArray("topicFilters", JsonArray())
            val topicFilters = topicFiltersArray.mapNotNull { it as? String }

            return JDBCLoggerConfig(
                databaseType = obj.getString("databaseType", "QuestDB"),
                jdbcUrl = obj.getString("jdbcUrl", "jdbc:questdb:http://localhost:9000"),
                username = obj.getString("username", "admin"),
                password = obj.getString("password", "quest"),
                topicFilters = topicFilters,
                tableName = obj.getString("tableName"),
                tableNameJsonPath = obj.getString("tableNameJsonPath"),
                payloadFormat = obj.getString("payloadFormat", "JSON"),
                jsonSchema = obj.getJsonObject("jsonSchema") ?: JsonObject(),
                queueType = obj.getString("queueType", "MEMORY"),
                queueSize = obj.getInteger("queueSize", 10000),
                diskPath = obj.getString("diskPath", "./buffer"),
                bulkSize = obj.getInteger("bulkSize", 1000),
                bulkTimeoutMs = obj.getLong("bulkTimeoutMs", 5000),
                reconnectDelayMs = obj.getLong("reconnectDelayMs", 5000)
            )
        }
    }

    fun toJson(): JsonObject {
        return JsonObject()
            .put("databaseType", databaseType)
            .put("jdbcUrl", jdbcUrl)
            .put("username", username)
            .put("password", password)
            .put("topicFilters", JsonArray(topicFilters))
            .put("tableName", tableName)
            .put("tableNameJsonPath", tableNameJsonPath)
            .put("payloadFormat", payloadFormat)
            .put("jsonSchema", jsonSchema)
            .put("queueType", queueType)
            .put("queueSize", queueSize)
            .put("diskPath", diskPath)
            .put("bulkSize", bulkSize)
            .put("bulkTimeoutMs", bulkTimeoutMs)
            .put("reconnectDelayMs", reconnectDelayMs)
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()

        // Database connection validation
        if (databaseType.isBlank()) {
            errors.add("databaseType cannot be blank")
        }

        if (jdbcUrl.isBlank()) {
            errors.add("jdbcUrl cannot be blank")
        }

        if (!jdbcUrl.startsWith("jdbc:")) {
            errors.add("jdbcUrl must start with 'jdbc:'")
        }

        if (username.isBlank()) {
            errors.add("username cannot be blank")
        }

        if (password.isBlank()) {
            errors.add("password cannot be blank")
        }

        // Topic subscription validation
        if (topicFilters.isEmpty()) {
            errors.add("topicFilters cannot be empty - at least one topic pattern is required")
        }

        // Table name validation (exactly one must be specified)
        if (tableName == null && tableNameJsonPath == null) {
            errors.add("Either 'tableName' or 'tableNameJsonPath' must be specified")
        }

        if (tableName != null && tableNameJsonPath != null) {
            errors.add("Cannot specify both 'tableName' and 'tableNameJsonPath' - use only one")
        }

        // Schema validation
        if (payloadFormat != "JSON" && payloadFormat != "XML") {
            errors.add("payloadFormat must be 'JSON' or 'XML'")
        }

        if (jsonSchema.isEmpty) {
            errors.add("jsonSchema cannot be empty")
        }

        // Queue validation
        if (queueType != "MEMORY" && queueType != "DISK") {
            errors.add("queueType must be 'MEMORY' or 'DISK'")
        }

        if (queueSize < 100) {
            errors.add("queueSize should be >= 100")
        }

        // Bulk write validation
        if (bulkSize < 1) {
            errors.add("bulkSize must be >= 1")
        }

        if (bulkTimeoutMs < 100) {
            errors.add("bulkTimeoutMs should be >= 100 ms")
        }

        return errors
    }
}
