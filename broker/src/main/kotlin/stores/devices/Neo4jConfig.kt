package at.rocworks.stores.devices

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/**
 * Configuration for Neo4j Client connection
 * Subscribes to MQTT topics and writes topic hierarchies to Neo4j graph database
 */
data class Neo4jClientConfig(
    val url: String = "bolt://localhost:7687",
    val username: String = "neo4j",
    val password: String = "password",
    val topicFilters: List<String> = emptyList(),  // MQTT topic patterns to subscribe (e.g., ["sensors/#", "plc/+/data"])
    val queueSize: Int = 10000,                     // Size of the path writer queue
    val batchSize: Int = 100,                       // Number of values to batch before writing
    val reconnectDelayMs: Long = 5000               // Delay before reconnecting on failure
) {
    companion object {
        fun fromJson(obj: JsonObject): Neo4jClientConfig {
            val topicFiltersArray = obj.getJsonArray("topicFilters", JsonArray())
            val topicFilters = topicFiltersArray.mapNotNull { it as? String }

            return Neo4jClientConfig(
                url = obj.getString("url", "bolt://localhost:7687"),
                username = obj.getString("username", "neo4j"),
                password = obj.getString("password", "password"),
                topicFilters = topicFilters,
                queueSize = obj.getInteger("queueSize", 10000),
                batchSize = obj.getInteger("batchSize", 100),
                reconnectDelayMs = obj.getLong("reconnectDelayMs", 5000)
            )
        }
    }

    fun toJson(): JsonObject {
        return JsonObject()
            .put("url", url)
            .put("username", username)
            .put("password", password)
            .put("topicFilters", JsonArray(topicFilters))
            .put("queueSize", queueSize)
            .put("batchSize", batchSize)
            .put("reconnectDelayMs", reconnectDelayMs)
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()

        if (url.isBlank()) {
            errors.add("url cannot be blank")
        }

        if (!url.startsWith("bolt://") && !url.startsWith("neo4j://") && !url.startsWith("bolt+s://") && !url.startsWith("neo4j+s://")) {
            errors.add("url must start with bolt://, neo4j://, bolt+s://, or neo4j+s://")
        }

        if (username.isBlank()) {
            errors.add("username cannot be blank")
        }

        if (password.isBlank()) {
            errors.add("password cannot be blank")
        }

        if (topicFilters.isEmpty()) {
            errors.add("topicFilters cannot be empty - at least one topic pattern is required")
        }

        if (queueSize < 100) {
            errors.add("queueSize should be >= 100")
        }

        if (batchSize < 1) {
            errors.add("batchSize must be >= 1")
        }

        if (reconnectDelayMs < 500) {
            errors.add("reconnectDelayMs should be >= 500")
        }

        return errors
    }
}
