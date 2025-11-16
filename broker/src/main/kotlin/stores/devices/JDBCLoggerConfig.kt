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
    val topicNameColumn: String? = null,            // Column name for MQTT topic (optional - if set, topic name is included in inserts)

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
    val reconnectDelayMs: Long = 5000,              // Delay before reconnecting on failure

    // Auto table creation
    val autoCreateTable: Boolean = true,            // Automatically create table if not exists
    val partitionBy: String = "DAY",                // QuestDB partition strategy: HOUR, DAY, WEEK, MONTH, YEAR, NONE

    // Database-specific configuration (JSON object for database-specific settings)
    // Examples:
    // - Snowflake: {privateKeyFile, account, url, role, scheme, port, database, schema}
    // - Future databases can add their own specific settings here
    val dbSpecificConfig: JsonObject = JsonObject()
) {
    /**
     * Get the JDBC driver class name inferred from the URL
     * This ensures the correct driver is loaded even when multiple JDBC drivers are on the classpath
     * Note: Neo4j is NOT supported in JDBC Logger (only in Flow Engine)
     * Note: QuestDB and TimescaleDB both use jdbc:postgresql:// URLs
     */
    fun getDriverClassName(): String {
        return when {
            jdbcUrl.contains("postgresql") -> "org.postgresql.Driver"
            jdbcUrl.contains("mysql") -> "com.mysql.cj.jdbc.Driver"
            else -> "org.postgresql.Driver"  // Default fallback for PostgreSQL-compatible databases (QuestDB, TimescaleDB, etc.)
        }
    }

    /**
     * Get a database-specific configuration value
     */
    fun getDbSpecificString(key: String, defaultValue: String = ""): String {
        return dbSpecificConfig.getString(key, defaultValue)
    }

    fun getDbSpecificInteger(key: String, defaultValue: Int = 0): Int {
        return dbSpecificConfig.getInteger(key, defaultValue)
    }

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
                topicNameColumn = obj.getString("topicNameColumn"),
                payloadFormat = obj.getString("payloadFormat", "JSON"),
                jsonSchema = obj.getJsonObject("jsonSchema") ?: JsonObject(),
                queueType = obj.getString("queueType", "MEMORY"),
                queueSize = obj.getInteger("queueSize", 10000),
                diskPath = obj.getString("diskPath", "./buffer"),
                bulkSize = obj.getInteger("bulkSize", 1000),
                bulkTimeoutMs = obj.getLong("bulkTimeoutMs", 5000),
                reconnectDelayMs = obj.getLong("reconnectDelayMs", 5000),
                autoCreateTable = obj.getBoolean("autoCreateTable", true),
                partitionBy = obj.getString("partitionBy", "DAY"),
                dbSpecificConfig = obj.getJsonObject("dbSpecificConfig") ?: JsonObject()
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
            .put("topicNameColumn", topicNameColumn)
            .put("payloadFormat", payloadFormat)
            .put("jsonSchema", jsonSchema)
            .put("queueType", queueType)
            .put("queueSize", queueSize)
            .put("diskPath", diskPath)
            .put("bulkSize", bulkSize)
            .put("bulkTimeoutMs", bulkTimeoutMs)
            .put("reconnectDelayMs", reconnectDelayMs)
            .put("autoCreateTable", autoCreateTable)
            .put("partitionBy", partitionBy)
            .put("dbSpecificConfig", dbSpecificConfig)
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()

        // Database connection validation
        if (databaseType.isBlank()) {
            errors.add("databaseType cannot be blank")
        }

        // JDBC URL validation
        if (jdbcUrl.isBlank()) {
            errors.add("jdbcUrl cannot be blank")
        }

        if (!jdbcUrl.startsWith("jdbc:")) {
            errors.add("jdbcUrl must start with 'jdbc:'")
        }

        if (username.isBlank()) {
            errors.add("username cannot be blank")
        }

        // Password is required for all databases except Snowflake (which uses private key authentication)
        if (databaseType.uppercase() != "SNOWFLAKE" && password.isBlank()) {
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

        // Database-specific validation
        if (databaseType.uppercase() == "SNOWFLAKE") {
            validateSnowflakeConfig(errors)
        }

        return errors
    }

    /**
     * Validate Snowflake-specific configuration
     */
    private fun validateSnowflakeConfig(errors: MutableList<String>) {
        val requiredFields = listOf(
            "account" to "Account",
            "privateKeyFile" to "Private Key File",
            "warehouse" to "Warehouse",
            "database" to "Database",
            "schema" to "Schema"
        )

        for ((key, displayName) in requiredFields) {
            if (dbSpecificConfig.getString(key).isNullOrBlank()) {
                errors.add("Snowflake configuration requires '$displayName' ($key)")
            }
        }

        // Role is optional - defaults to ACCOUNTADMIN
    }
}
