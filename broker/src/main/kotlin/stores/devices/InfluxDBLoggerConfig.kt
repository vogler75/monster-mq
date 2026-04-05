package at.rocworks.stores.devices

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/**
 * Configuration for InfluxDB Logger - Schema-based data ingestion via InfluxDB Line Protocol
 */
data class InfluxDBLoggerConfig(
    // HTTP Connection
    val endpointUrl: String,                        // HTTP POST endpoint URL
    val authType: String = "NONE",                  // "NONE", "BASIC", "TOKEN"
    val username: String? = null,
    val password: String? = null,
    val token: String? = null,

    // InfluxDB specific
    val org: String? = null,                            // InfluxDB v2: organization
    val bucket: String? = null,                         // InfluxDB v2: bucket
    val db: String? = null,                             // InfluxDB v1: database name
    val influxTags: List<String> = emptyList(),         // Fields to be treated as InfluxDB tags (indexed)

    // Topic subscription
    override val topicFilters: List<String> = emptyList(),

    // Target configuration (maps to InfluxDB measurement)
    override val tableName: String? = null,
    override val tableNameJsonPath: String? = null,
    override val topicNameColumn: String? = null,

    // Schema and validation
    override val payloadFormat: String = "JSON",
    override val jsonSchema: JsonObject,

    // Queue configuration
    override val queueType: String = "MEMORY",
    override val queueSize: Int = 10000,
    override val diskPath: String = "./buffer/influxdb",

    // Bulk write configuration
    override val bulkSize: Int = 1000,
    override val bulkTimeoutMs: Long = 5000,

    // Connection settings
    override val reconnectDelayMs: Long = 5000,

    // Additional HTTP Headers
    val headers: JsonObject = JsonObject()
) : ILoggerConfig {

    companion object {
        fun fromJson(obj: JsonObject): InfluxDBLoggerConfig {
            val topicFiltersArray = obj.getJsonArray("topicFilters", JsonArray())
            val topicFilters = topicFiltersArray.mapNotNull { it as? String }

            val influxTagsArray = obj.getJsonArray("influxTags", JsonArray())
            val influxTags = influxTagsArray.mapNotNull { it as? String }

            return InfluxDBLoggerConfig(
                endpointUrl = obj.getString("endpointUrl", ""),
                authType = obj.getString("authType", "NONE"),
                username = obj.getString("username"),
                password = obj.getString("password"),
                token = obj.getString("token"),
                org = obj.getString("org"),
                bucket = obj.getString("bucket"),
                db = obj.getString("db"),
                influxTags = influxTags,
                topicFilters = topicFilters,
                tableName = obj.getString("tableName"),
                tableNameJsonPath = obj.getString("tableNameJsonPath"),
                topicNameColumn = obj.getString("topicNameColumn"),
                payloadFormat = obj.getString("payloadFormat", "JSON"),
                jsonSchema = obj.getJsonObject("jsonSchema") ?: JsonObject(),
                queueType = obj.getString("queueType", "MEMORY"),
                queueSize = obj.getInteger("queueSize", 10000),
                diskPath = obj.getString("diskPath", "./buffer/influxdb"),
                bulkSize = obj.getInteger("bulkSize", 1000),
                bulkTimeoutMs = obj.getLong("bulkTimeoutMs", 5000),
                reconnectDelayMs = obj.getLong("reconnectDelayMs", 5000),
                headers = obj.getJsonObject("headers") ?: JsonObject()
            )
        }
    }

    override fun toJson(): JsonObject {
        return JsonObject()
            .put("endpointUrl", endpointUrl)
            .put("authType", authType)
            .put("username", username)
            .put("password", password)
            .put("token", token)
            .put("org", org)
            .put("bucket", bucket)
            .put("db", db)
            .put("influxTags", JsonArray(influxTags))
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
            .put("headers", headers)
    }

    override fun validate(): List<String> {
        val errors = mutableListOf<String>()
        if (endpointUrl.isBlank()) {
            errors.add("endpointUrl cannot be blank")
        }
        return errors
    }
}
