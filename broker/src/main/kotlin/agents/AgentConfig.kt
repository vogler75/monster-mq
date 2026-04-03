package at.rocworks.agents

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

data class AgentConfig(
    val org: String = "default",
    val site: String = "default",
    val description: String = "",
    val version: String = "1.0.0",
    val skills: List<AgentSkill> = emptyList(),
    val inputTopics: List<String> = emptyList(),
    val outputTopics: List<String> = emptyList(),
    val triggerType: TriggerType = TriggerType.MQTT,
    val cronExpression: String? = null,
    val cronIntervalMs: Long? = null,
    val cronPrompt: String? = null,
    val provider: String = "gemini",
    val model: String? = null,
    val apiKey: String? = null,
    val systemPrompt: String = "",
    val maxTokens: Int? = null,
    val temperature: Double = 0.7,
    val maxToolIterations: Int = 10,
    val memoryWindowSize: Int = 40,
    val stateEnabled: Boolean = true,
    val mcpServers: List<String> = emptyList(),
    val useMonsterMqMcp: Boolean = false,
    val defaultArchiveGroup: String = "Default",
    val contextLastvalTopics: Map<String, List<String>> = emptyMap(),  // archiveGroup -> list of topic filters
    val contextRetainedTopics: List<String> = emptyList(),      // topic filters for retained messages
    val contextHistoryQueries: List<ContextHistoryQuery> = emptyList(),  // history data queries
    val taskTimeoutMs: Long = 60000,  // timeout for sub-agent task invocations (default 60s)
    val subAgentsAllowAll: Boolean = false,      // when true, agent can call any other agent
    val subAgents: List<String> = emptyList(),  // restrict which agents this orchestrator can invoke (ignored when allowAll=true)
    val enableThinking: Boolean = false,
    val conversationLogEnabled: Boolean = false,  // Log full LLM conversations to log/agents/<name>.log
    val endpoint: String? = null,  // For Azure OpenAI: resource endpoint URL
    val serviceVersion: String? = null,  // For Azure OpenAI: API version (e.g. "2024-02-01")
    val providerName: String? = null,  // references a stored GenAiProvider by name
    val timezone: String? = null       // null = system default, e.g. "UTC", "Europe/Vienna"
) {
    companion object {
        fun fromJsonObject(json: JsonObject): AgentConfig {
            return AgentConfig(
                org = json.getString("org", "default"),
                site = json.getString("site", "default"),
                description = json.getString("description", ""),
                version = json.getString("version", "1.0.0"),
                skills = json.getJsonArray("skills", JsonArray()).filterIsInstance<JsonObject>().map { AgentSkill.fromJsonObject(it) },
                inputTopics = json.getJsonArray("inputTopics", JsonArray()).filterIsInstance<String>().toList(),
                outputTopics = json.getJsonArray("outputTopics", JsonArray()).filterIsInstance<String>().toList(),
                triggerType = TriggerType.fromString(json.getString("triggerType", "MQTT")),
                cronExpression = json.getString("cronExpression"),
                cronIntervalMs = json.getLong("cronIntervalMs"),
                cronPrompt = json.getString("cronPrompt"),
                provider = json.getString("provider", "gemini"),
                model = json.getString("model"),
                apiKey = json.getString("apiKey"),
                systemPrompt = json.getString("systemPrompt", ""),
                maxTokens = json.getInteger("maxTokens"),
                temperature = json.getDouble("temperature", 0.7),
                maxToolIterations = json.getInteger("maxToolIterations", 10),
                memoryWindowSize = json.getInteger("memoryWindowSize", 40),
                stateEnabled = json.getBoolean("stateEnabled", true),
                mcpServers = json.getJsonArray("mcpServers", JsonArray()).filterIsInstance<String>().toList(),
                useMonsterMqMcp = json.getBoolean("useMonsterMqMcp", false),
                defaultArchiveGroup = json.getString("defaultArchiveGroup", "Default"),
                contextLastvalTopics = json.getJsonObject("contextLastvalTopics", JsonObject()).let { obj ->
                    obj.fieldNames().associateWith { key ->
                        obj.getJsonArray(key, JsonArray()).filterIsInstance<String>()
                    }
                },
                contextRetainedTopics = json.getJsonArray("contextRetainedTopics", JsonArray()).filterIsInstance<String>().toList(),
                contextHistoryQueries = json.getJsonArray("contextHistoryQueries", JsonArray())
                    .filterIsInstance<JsonObject>().map { ContextHistoryQuery.fromJsonObject(it) },
                taskTimeoutMs = json.getLong("taskTimeoutMs", 60000),
                subAgentsAllowAll = json.getBoolean("subAgentsAllowAll", false),
                subAgents = json.getJsonArray("subAgents", JsonArray()).filterIsInstance<String>().toList(),
                enableThinking = json.getBoolean("enableThinking", false),
                conversationLogEnabled = json.getBoolean("conversationLogEnabled", false),
                endpoint = json.getString("endpoint"),
                serviceVersion = json.getString("serviceVersion"),
                providerName = json.getString("providerName"),
                timezone = json.getString("timezone")
            )
        }
    }

    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("org", org)
            .put("site", site)
            .put("description", description)
            .put("version", version)
            .put("skills", JsonArray(skills.map { it.toJsonObject() }))
            .put("inputTopics", JsonArray(inputTopics))
            .put("outputTopics", JsonArray(outputTopics))
            .put("triggerType", triggerType.name)
            .put("cronExpression", cronExpression)
            .put("cronIntervalMs", cronIntervalMs)
            .put("cronPrompt", cronPrompt)
            .put("provider", provider)
            .put("model", model)
            .put("apiKey", apiKey)
            .put("systemPrompt", systemPrompt)
            .put("maxTokens", maxTokens)
            .put("temperature", temperature)
            .put("maxToolIterations", maxToolIterations)
            .put("memoryWindowSize", memoryWindowSize)
            .put("stateEnabled", stateEnabled)
            .put("mcpServers", JsonArray(mcpServers))
            .put("useMonsterMqMcp", useMonsterMqMcp)
            .put("defaultArchiveGroup", defaultArchiveGroup)
            .put("contextLastvalTopics", JsonObject().also { obj ->
                contextLastvalTopics.forEach { (group, topics) -> obj.put(group, JsonArray(topics)) }
            })
            .put("contextRetainedTopics", JsonArray(contextRetainedTopics))
            .put("contextHistoryQueries", JsonArray(contextHistoryQueries.map { it.toJsonObject() }))
            .put("taskTimeoutMs", taskTimeoutMs)
            .put("subAgentsAllowAll", subAgentsAllowAll)
            .put("subAgents", JsonArray(subAgents))
            .put("enableThinking", enableThinking)
            .put("conversationLogEnabled", conversationLogEnabled)
            .put("endpoint", endpoint)
            .put("serviceVersion", serviceVersion)
            .put("providerName", providerName)
            .put("timezone", timezone)
    }
}

data class AgentSkill(
    val name: String,
    val description: String,
    val inputSchema: JsonObject? = null
) {
    companion object {
        fun fromJsonObject(json: JsonObject): AgentSkill {
            return AgentSkill(
                name = json.getString("name", ""),
                description = json.getString("description", ""),
                inputSchema = json.getJsonObject("inputSchema")
            )
        }
    }

    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("name", name)
            .put("description", description)
            .put("inputSchema", inputSchema)
    }
}

data class ContextHistoryQuery(
    val archiveGroup: String = "Default",
    val topics: List<String> = emptyList(),
    val lastSeconds: Int = 3600,          // how far back to look (default 1 hour)
    val interval: String = "RAW",         // RAW, ONE_MINUTE, FIVE_MINUTES, FIFTEEN_MINUTES, ONE_HOUR, ONE_DAY
    val function: String = "AVG",         // AVG, MIN, MAX (ignored for RAW)
    val fields: List<String> = emptyList(), // optional JSON field paths for aggregation (e.g. ["temperature", "pressure"])
    val decimals: Int? = null              // optional: round numeric values to N decimal places
) {
    companion object {
        fun fromJsonObject(json: JsonObject): ContextHistoryQuery {
            return ContextHistoryQuery(
                archiveGroup = json.getString("archiveGroup", "Default"),
                topics = json.getJsonArray("topics", JsonArray()).filterIsInstance<String>().toList(),
                lastSeconds = json.getInteger("lastSeconds", 3600),
                interval = json.getString("interval", "RAW"),
                function = json.getString("function", "AVG"),
                fields = json.getJsonArray("fields", JsonArray()).filterIsInstance<String>().toList(),
                decimals = json.getInteger("decimals")
            )
        }
    }

    fun toJsonObject(): JsonObject {
        val json = JsonObject()
            .put("archiveGroup", archiveGroup)
            .put("topics", JsonArray(topics))
            .put("lastSeconds", lastSeconds)
            .put("interval", interval)
            .put("function", function)
            .put("fields", JsonArray(fields))
        if (decimals != null) json.put("decimals", decimals)
        return json
    }

    fun intervalMinutes(): Int = when (interval.uppercase()) {
        "ONE_MINUTE" -> 1
        "FIVE_MINUTES" -> 5
        "FIFTEEN_MINUTES" -> 15
        "ONE_HOUR" -> 60
        "ONE_DAY" -> 1440
        else -> 0 // RAW
    }

    fun isRaw(): Boolean = interval.uppercase() == "RAW"
}

enum class TriggerType {
    MQTT, CRON, MANUAL;

    companion object {
        fun fromString(value: String): TriggerType {
            return try { valueOf(value.uppercase()) } catch (e: Exception) { MQTT }
        }
    }
}
