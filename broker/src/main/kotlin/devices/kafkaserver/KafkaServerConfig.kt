package at.rocworks.devices.kafkaserver

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

data class KafkaServerConfig(
    val name: String = "",
    val namespace: String = "",
    val nodeId: String = "*",
    val enabled: Boolean = true,
    val host: String = "0.0.0.0",
    val port: Int = 9092,
    val advertisedHost: String? = null,
    val advertisedPort: Int? = null,
    val storeType: String? = null,
    val streams: List<KafkaStreamMapping> = emptyList()
) {
    companion object {
        fun fromJsonObject(json: JsonObject): KafkaServerConfig {
            val streamsArray = json.getJsonArray("streams") ?: JsonArray()
            val streamsList = streamsArray.map { item ->
                val obj = item as JsonObject
                KafkaStreamMapping(
                    streamName = obj.getString("streamName") ?: "",
                    topicFilter = obj.getString("topicFilter") ?: "",
                    retentionHours = obj.getInteger("retentionHours") ?: 168,
                    storeType = obj.getString("storeType"),
                    allowWrite = obj.getBoolean("allowWrite") ?: obj.getBoolean("AllowWrite") ?: true
                )
            }

            return KafkaServerConfig(
                name = json.getString("name") ?: "",
                namespace = json.getString("namespace") ?: "",
                nodeId = json.getString("nodeId") ?: "*",
                enabled = json.getBoolean("enabled") ?: true,
                host = json.getString("host") ?: "0.0.0.0",
                port = json.getInteger("port") ?: 9092,
                advertisedHost = json.getString("advertisedHost"),
                advertisedPort = json.getInteger("advertisedPort"),
                storeType = json.getString("storeType"),
                streams = streamsList
            )
        }
    }

    fun toJsonObject(): JsonObject {
        val streamsArray = JsonArray()
        streams.forEach { mapping ->
            streamsArray.add(JsonObject().apply {
                put("streamName", mapping.streamName)
                put("topicFilter", mapping.topicFilter)
                put("retentionHours", mapping.retentionHours)
                put("storeType", mapping.storeType)
                put("allowWrite", mapping.allowWrite)
            })
        }

        return JsonObject().apply {
            put("name", name)
            put("namespace", namespace)
            put("nodeId", nodeId)
            put("enabled", enabled)
            put("host", host)
            put("port", port)
            if (advertisedHost != null) put("advertisedHost", advertisedHost)
            if (advertisedPort != null) put("advertisedPort", advertisedPort)
            put("storeType", storeType)
            put("streams", streamsArray)
        }
    }
}

data class KafkaStreamMapping(
    val streamName: String,
    val topicFilter: String,
    val retentionHours: Int = 168,
    val storeType: String? = null,
    val allowWrite: Boolean = true
)
