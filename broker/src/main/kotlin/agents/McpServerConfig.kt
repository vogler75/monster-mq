package at.rocworks.agents

import io.vertx.core.json.JsonObject

/**
 * Configuration for a remote MCP (Model Context Protocol) server.
 * Stored as DeviceConfig with type = "MCP-Server".
 */
data class McpServerConfig(
    val url: String,
    val transport: String = "http"  // "http" = streamable HTTP transport
) {
    companion object {
        fun fromJsonObject(json: JsonObject): McpServerConfig {
            return McpServerConfig(
                url = json.getString("url", ""),
                transport = json.getString("transport", "http")
            )
        }
    }

    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("url", url)
            .put("transport", transport)
    }
}
