package at.rocworks.agents

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.handlers.ArchiveGroup
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.stores.IMessageArchiveExtended
import at.rocworks.stores.IMessageStore
import at.rocworks.data.BrokerMessage
import dev.langchain4j.agent.tool.P
import dev.langchain4j.agent.tool.Tool
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.logging.Logger

/**
 * Broker tools exposed to AI agents via LangChain4j @Tool annotations.
 * Reuses the same underlying stores as the MCP server and flow engine.
 */
class AgentTools(
    private val archiveHandler: ArchiveHandler?,
    private val retainedStore: IMessageStore?,
    private val agentClientId: String
) {
    private val logger: Logger = Utils.getLogger(AgentTools::class.java)

    private fun getArchiveGroups(): Map<String, ArchiveGroup> {
        return archiveHandler?.getDeployedArchiveGroups() ?: emptyMap()
    }

    @Tool("Publish a message to an MQTT topic. Use this to send data or commands to other systems.")
    fun publishMessage(
        @P("The MQTT topic to publish to") topic: String,
        @P("The message payload (text or JSON)") payload: String
    ): String {
        return try {
            val msg = BrokerMessage(agentClientId, topic, payload)
            val handler = Monster.getSessionHandler()
            handler?.publishMessage(msg)
            "Published to $topic"
        } catch (e: Exception) {
            logger.warning("publishMessage error: ${e.message}")
            "Error publishing: ${e.message}"
        }
    }

    @Tool("Get the current/last known value for one or more MQTT topics from the last-value store.")
    fun getTopicValues(
        @P("Comma-separated list of exact MQTT topics") topics: String,
        @P("Archive group name (default: 'Default')") archiveGroup: String?
    ): String {
        return try {
            val group = archiveGroup ?: "Default"
            val store = getArchiveGroups()[group]?.lastValStore
            if (store == null) return "No LastValueStore for archive group '$group'"

            val results = JsonArray()
            for (topic in topics.split(",").map { it.trim() }) {
                val msg = store[topic]
                if (msg != null) {
                    results.add(JsonObject()
                        .put("topic", msg.topicName)
                        .put("value", msg.getPayloadAsJson() ?: msg.getPayloadAsBase64())
                        .put("timestamp", msg.time.toEpochMilli())
                    )
                }
            }
            results.encodePrettily()
        } catch (e: Exception) {
            logger.warning("getTopicValues error: ${e.message}")
            "Error: ${e.message}"
        }
    }

    @Tool("Search for MQTT topics matching a pattern. Use MQTT wildcards: + for single level, # for multi level.")
    fun findTopics(
        @P("MQTT topic pattern (e.g., 'sensors/#', 'plant/+/temperature')") pattern: String,
        @P("Archive group name (default: 'Default')") archiveGroup: String?
    ): String {
        return try {
            val group = archiveGroup ?: "Default"
            val store = getArchiveGroups()[group]?.lastValStore
            if (store == null) return "No LastValueStore for archive group '$group'"

            val results = JsonArray()
            var count = 0
            store.findMatchingTopics(pattern) { topic ->
                if (count < 100) {
                    results.add(topic)
                    count++
                    true
                } else false
            }
            results.encodePrettily()
        } catch (e: Exception) {
            logger.warning("findTopics error: ${e.message}")
            "Error: ${e.message}"
        }
    }

    @Tool("Query historical messages from the message archive for a specific topic within a time range.")
    fun queryHistory(
        @P("MQTT topic to query") topic: String,
        @P("Start time in ISO-8601 format (e.g., '2024-01-01T00:00:00Z'), or null for no lower bound") startTime: String?,
        @P("End time in ISO-8601 format, or null for now") endTime: String?,
        @P("Maximum number of messages to return (default: 100)") limit: Int?,
        @P("Archive group name (default: 'Default')") archiveGroup: String?
    ): String {
        return try {
            val group = archiveGroup ?: "Default"
            val store = getArchiveGroups()[group]?.archiveStore as? IMessageArchiveExtended
            if (store == null) return "No archive store for group '$group'"

            val result = store.getHistory(
                topic = topic,
                startTime = startTime?.let { Instant.parse(it) },
                endTime = endTime?.let { Instant.parse(it) },
                limit = limit ?: 100
            )
            result.encodePrettily()
        } catch (e: Exception) {
            logger.warning("queryHistory error: ${e.message}")
            "Error: ${e.message}"
        }
    }

    @Tool("List all configured archive groups. Use this to discover available data stores before querying.")
    fun listArchiveGroups(): String {
        return try {
            val groups = getArchiveGroups()
            val result = JsonArray()
            for ((name, group) in groups) {
                result.add(JsonObject()
                    .put("name", name)
                    .put("archiveType", group.getArchiveType().name)
                    .put("lastValType", group.getLastValType().name)
                )
            }
            result.encodePrettily()
        } catch (e: Exception) {
            logger.warning("listArchiveGroups error: ${e.message}")
            "Error: ${e.message}"
        }
    }
}
