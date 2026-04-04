package at.rocworks.agents

import at.rocworks.Const
import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.handlers.ArchiveGroup
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.stores.IMessageArchiveExtended
import at.rocworks.stores.IMessageStore
import at.rocworks.data.BrokerMessage
import dev.langchain4j.agent.tool.P
import dev.langchain4j.agent.tool.Tool
import io.vertx.core.Vertx
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
    private val agentClientId: String,
    private val agentName: String,
    private val a2aOrg: String = "default",
    private val a2aSite: String = "default",
    private val defaultArchiveGroup: String = "Default",
    private val toolLogger: ((String, String, String) -> Unit)? = null,
    private val vertx: Vertx? = null,
    private val taskTimeoutMs: Long = 60000,
    private val getCurrentTaskId: (() -> String?)? = null,
    private val registerPendingTask: ((taskId: String, targetAgent: String, input: String) -> Unit)? = null,
    private val subAgentsAllowAll: Boolean = false,
    private val subAgents: List<String> = emptyList()
) {
    private val logger: Logger = Utils.getLogger(AgentTools::class.java)

    private val nativeToolNames: Set<String> by lazy {
        this::class.java.methods
            .filter { it.isAnnotationPresent(Tool::class.java) }
            .map { it.name }
            .toSet()
    }

    fun isNativeTool(name: String): Boolean = name in nativeToolNames

    private fun getArchiveGroups(): Map<String, ArchiveGroup> {
        return archiveHandler?.getDeployedArchiveGroups() ?: emptyMap()
    }

    private fun logTool(name: String, args: String, result: String): String {
        toolLogger?.invoke(name, args, result)
        return result
    }

    @Tool("Publish a message to an MQTT topic. Use this to send data or commands to other systems.")
    fun publishMessage(
        @P("The MQTT topic to publish to") topic: String,
        @P("The message payload (text or JSON)") payload: String
    ): String {
        val result = try {
            val msg = BrokerMessage(agentClientId, topic, payload)
            val handler = Monster.getSessionHandler()
            handler?.publishMessage(msg)
            "Published to $topic"
        } catch (e: Exception) {
            logger.warning("publishMessage error: ${e.message}")
            "Error publishing: ${e.message}"
        }
        return logTool("publishMessage", "topic=$topic", result)
    }

    @Tool("Get the current/last known value for one or more MQTT topics from the last-value store.")
    fun getTopicValues(
        @P("Comma-separated list of exact MQTT topics") topics: String,
        @P("Archive group name, or null to use the agent's default") archiveGroup: String?
    ): String {
        val result = try {
            val group = archiveGroup ?: defaultArchiveGroup
            val store = getArchiveGroups()[group]?.lastValStore
            if (store == null) return logTool("getTopicValues", "topics=$topics", "No LastValueStore for archive group '$group'")

            val results = JsonArray()
            for (topic in topics.split(",").map { it.trim() }) {
                val msg = store[topic]
                if (msg != null) {
                    results.add(JsonObject()
                        .put("topic", msg.topicName)
                        .put("value", msg.getPayloadAsJsonValueOrString())
                        .put("timestamp", msg.time.toEpochMilli())
                    )
                }
            }
            results.encodePrettily()
        } catch (e: Exception) {
            logger.warning("getTopicValues error: ${e.message}")
            "Error: ${e.message}"
        }
        return logTool("getTopicValues", "topics=$topics", result)
    }

    @Tool("Search for MQTT topics matching a pattern. Use MQTT wildcards: + for single level, # for multi level.")
    fun findTopics(
        @P("MQTT topic pattern (e.g., 'sensors/#', 'plant/+/temperature')") pattern: String,
        @P("Archive group name, or null to use the agent's default") archiveGroup: String?
    ): String {
        val result = try {
            val group = archiveGroup ?: defaultArchiveGroup
            val store = getArchiveGroups()[group]?.lastValStore
            if (store == null) return logTool("findTopics", "pattern=$pattern", "No LastValueStore for archive group '$group'")

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
        return logTool("findTopics", "pattern=$pattern", result)
    }

    @Tool("Query historical messages from the message archive for a specific topic within a time range.")
    fun queryHistory(
        @P("MQTT topic to query") topic: String,
        @P("Start time in ISO-8601 format (e.g., '2024-01-01T00:00:00Z'), or null for no lower bound") startTime: String?,
        @P("End time in ISO-8601 format, or null for now") endTime: String?,
        @P("Maximum number of messages to return (default: 100)") limit: Int?,
        @P("Archive group name, or null to use the agent's default") archiveGroup: String?
    ): String {
        val result = try {
            val group = archiveGroup ?: defaultArchiveGroup
            val store = getArchiveGroups()[group]?.archiveStore as? IMessageArchiveExtended
            if (store == null) return logTool("queryHistory", "topic=$topic", "No archive store for group '$group'")

            val r = store.getHistory(
                topic = topic,
                startTime = startTime?.let { Instant.parse(it) },
                endTime = endTime?.let { Instant.parse(it) },
                limit = limit ?: 100
            )
            // Convert to CSV: more token-efficient for LLMs than JSON
            val sb = StringBuilder()
            sb.appendLine("topic,time,value")
            for (i in 0 until r.size()) {
                val row = r.getJsonObject(i) ?: continue
                val topic_ = row.getString("topic") ?: ""
                val ts = row.getValue("timestamp")
                val time = if (ts is Number) Instant.ofEpochMilli(ts.toLong()).toString() else ts?.toString() ?: ""
                val value = row.getString("payload_json")
                    ?: row.getString("payload_base64")?.let { String(java.util.Base64.getDecoder().decode(it), Charsets.UTF_8) }
                    ?: ""
                sb.appendLine("$topic_,$time,$value")
            }
            sb.toString().trimEnd()
        } catch (e: Exception) {
            logger.warning("queryHistory error: ${e.message}")
            "Error: ${e.message}"
        }
        return logTool("queryHistory", "topic=$topic, start=$startTime, end=$endTime, limit=$limit", result)
    }

    // --- Agent Notes (persistent key/value memory via retained MQTT messages) ---

    private val notesPrefix = "a2a/v1/$a2aOrg/$a2aSite/agents/$agentName/memory"

    @Tool("Save a note to persistent memory. Use this to remember observations, decisions, or learned information across invocations. Keys can be hierarchical (e.g., 'decisions/heater', 'observations/2024-03-20').")
    fun saveNote(
        @P("A descriptive key for the note (e.g., 'last_decision', 'user_preference/threshold')") key: String,
        @P("The content to store (text or JSON)") content: String
    ): String {
        val result = try {
            val topic = "$notesPrefix/$key"
            val msg = BrokerMessage(agentClientId, topic, content).cloneWithRetainFlag(true)
            Monster.getSessionHandler()?.publishMessage(msg)
            "Saved note '$key'"
        } catch (e: Exception) {
            logger.warning("saveNote error: ${e.message}")
            "Error saving note: ${e.message}"
        }
        return logTool("saveNote", "key=$key", result)
    }

    @Tool("Recall a previously saved note by its exact key.")
    fun recallNote(
        @P("The exact key of the note to retrieve") key: String
    ): String {
        val result = try {
            val topic = "$notesPrefix/$key"
            val store = Monster.getRetainedStore()
            if (store == null) return logTool("recallNote", "key=$key", "No retained store available")
            val msg = store[topic]
            if (msg != null) {
                msg.getPayloadAsString()
            } else {
                "No note found for key '$key'"
            }
        } catch (e: Exception) {
            logger.warning("recallNote error: ${e.message}")
            "Error recalling note: ${e.message}"
        }
        return logTool("recallNote", "key=$key", result)
    }

    @Tool("Search for saved notes matching a pattern. Use MQTT wildcards: + for single level, # for all sub-levels. Example: 'decisions/#' finds all decision notes.")
    fun searchNotes(
        @P("Search pattern relative to the notes namespace (e.g., '#' for all, 'decisions/#', '+/temperature')") pattern: String
    ): String {
        val result = try {
            val fullPattern = "$notesPrefix/$pattern"
            val store = Monster.getRetainedStore()
            if (store == null) return logTool("searchNotes", "pattern=$pattern", "No retained store available")
            val results = JsonArray()
            var count = 0
            store.findMatchingMessages(fullPattern) { msg ->
                if (count < 100) {
                    val key = msg.topicName.removePrefix("$notesPrefix/")
                    results.add(JsonObject()
                        .put("key", key)
                        .put("content", msg.getPayloadAsJsonValueOrString())
                        .put("timestamp", msg.time.toString())
                    )
                    count++
                    true
                } else false
            }
            if (results.isEmpty) "No notes found matching '$pattern'" else results.encodePrettily()
        } catch (e: Exception) {
            logger.warning("searchNotes error: ${e.message}")
            "Error searching notes: ${e.message}"
        }
        return logTool("searchNotes", "pattern=$pattern", result)
    }

    @Tool("Delete a previously saved note by its exact key.")
    fun deleteNote(
        @P("The exact key of the note to delete") key: String
    ): String {
        val result = try {
            val topic = "$notesPrefix/$key"
            // Publishing an empty retained message deletes the retained entry
            val msg = BrokerMessage(
                Utils.getUuid(), 0, topic, ByteArray(0), 0,
                isRetain = true, isDup = false, isQueued = false, clientId = agentClientId
            )
            Monster.getSessionHandler()?.publishMessage(msg)
            "Deleted note '$key'"
        } catch (e: Exception) {
            logger.warning("deleteNote error: ${e.message}")
            "Error deleting note: ${e.message}"
        }
        return logTool("deleteNote", "key=$key", result)
    }

    // --- Agent Orchestration Tools (A2A over MQTT) ---

    @Tool("List all available AI agents in the system. Returns their names, descriptions, skills, and current status.")
    fun listAgents(): String {
        val result = try {
            val store = Monster.getRetainedStore()
                ?: return logTool("listAgents", "", "No retained store available")
            val agents = JsonArray()
            var count = 0
            store.findMatchingMessages("a2a/v1/$a2aOrg/$a2aSite/discovery/+") { msg ->
                if (count < 100) {
                    try {
                        val card = JsonObject(String(msg.payload, Charsets.UTF_8))
                        agents.add(JsonObject()
                            .put("name", card.getString("name"))
                            .put("description", card.getString("description"))
                            .put("status", card.getString("status"))
                            .put("skills", card.getValue("skills"))
                            .put("triggerType", card.getString("triggerType"))
                            .put("provider", card.getString("provider"))
                            .put("model", card.getString("model"))
                        )
                    } catch (e: Exception) {
                        // Skip malformed agent cards
                    }
                    count++
                    true
                } else false
            }
            // Filter out self
            val filtered = JsonArray()
            for (i in 0 until agents.size()) {
                val agent = agents.getJsonObject(i)
                val name = agent.getString("name")
                if (name == agentName) continue
                if (!subAgentsAllowAll && (subAgents.isEmpty() || name !in subAgents)) continue
                filtered.add(agent)
            }
            if (filtered.isEmpty) "No agents found" else filtered.encodePrettily()
        } catch (e: Exception) {
            logger.warning("listAgents error: ${e.message}")
            "Error listing agents: ${e.message}"
        }
        return logTool("listAgents", "", result)
    }

    @Tool("Get the full Agent Card for a specific agent, including its capabilities, skills, and configuration details.")
    fun getAgentCard(
        @P("The name of the agent to look up") agentName: String
    ): String {
        val result = try {
            val store = Monster.getRetainedStore()
                ?: return logTool("getAgentCard", "agent=$agentName", "No retained store available")
            val msg = store["a2a/v1/$a2aOrg/$a2aSite/discovery/$agentName"]
            if (msg != null) {
                msg.getPayloadAsString()
            } else {
                "No agent card found for '$agentName'"
            }
        } catch (e: Exception) {
            logger.warning("getAgentCard error: ${e.message}")
            "Error: ${e.message}"
        }
        return logTool("getAgentCard", "agent=$agentName", result)
    }

    @Tool("Send a task to another agent. The response will arrive asynchronously at your inbox. Use listAgents() first to discover available agents.")
    fun invokeAgent(
        @P("The name of the target agent to invoke") targetAgent: String,
        @P("The task input/instruction to send to the agent") input: String,
        @P("Optional: specific skill to invoke on the target agent") skill: String?
    ): String {
        val result = try {
            if (!subAgentsAllowAll && (subAgents.isEmpty() || targetAgent !in subAgents)) {
                return logTool("invokeAgent", "target=$targetAgent", "Agent '$targetAgent' is not in this agent's sub-agents list. Available: ${if (subAgents.isEmpty()) "(none)" else subAgents}")
            }

            val sessionHandler = Monster.getSessionHandler()
                ?: return logTool("invokeAgent", "target=$targetAgent", "No session handler available")

            val taskId = Utils.getUuid()
            val replyTo = "a2a/v1/$a2aOrg/$a2aSite/agents/$agentName/inbox/$taskId"

            // Register pending task for timeout monitoring and correlation
            registerPendingTask?.invoke(taskId, targetAgent, input)

            // Publish task to target agent's inbox
            val taskJson = JsonObject()
                .put("taskId", taskId)
                .put("input", input)
                .put("replyTo", replyTo)
                .put("callerAgent", agentName)
            val parentTaskId = getCurrentTaskId?.invoke()
            if (parentTaskId != null) taskJson.put("parentTaskId", parentTaskId)
            if (skill != null) taskJson.put("skill", skill)

            val msg = BrokerMessage(agentClientId, "a2a/v1/$a2aOrg/$a2aSite/agents/$targetAgent/inbox/$taskId", taskJson.encode())
            sessionHandler.publishMessage(msg)

            logger.fine("Agent $agentName sent task $taskId to agent $targetAgent")

            "Task submitted to agent '$targetAgent' (taskId=$taskId). Response will arrive asynchronously."
        } catch (e: Exception) {
            logger.warning("invokeAgent error: ${e.message}")
            "Error invoking agent '$targetAgent': ${e.message}"
        }
        return logTool("invokeAgent", "target=$targetAgent, input=${input.take(200)}", result)
    }

    @Tool("Get the health status of a specific agent, including uptime metrics and error counts.")
    fun getAgentHealth(
        @P("The name of the agent to check") agentName: String
    ): String {
        val result = try {
            val store = Monster.getRetainedStore()
                ?: return logTool("getAgentHealth", "agent=$agentName", "No retained store available")
            val msg = store["a2a/v1/$a2aOrg/$a2aSite/agents/$agentName/health"]
            if (msg != null) {
                msg.getPayloadAsString()
            } else {
                "No health data found for agent '$agentName'"
            }
        } catch (e: Exception) {
            logger.warning("getAgentHealth error: ${e.message}")
            "Error: ${e.message}"
        }
        return logTool("getAgentHealth", "agent=$agentName", result)
    }

}
