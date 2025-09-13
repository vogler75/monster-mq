package at.rocworks.data

import io.vertx.core.json.JsonObject
import java.io.Serializable

/**
 * Distributed client-to-node mapping for cluster message routing
 */
data class ClientNodeMapping(
    val clientId: String,
    val nodeId: String,
    val eventType: EventType
) : Serializable {

    enum class EventType {
        CLIENT_CONNECTED, CLIENT_DISCONNECTED, NODE_FAILURE
    }

    fun toJson(): JsonObject = JsonObject()
        .put("clientId", clientId)
        .put("nodeId", nodeId)
        .put("eventType", eventType.name)

    companion object {
        fun fromJson(json: JsonObject): ClientNodeMapping = ClientNodeMapping(
            clientId = json.getString("clientId"),
            nodeId = json.getString("nodeId"),
            eventType = EventType.valueOf(json.getString("eventType"))
        )
    }
}