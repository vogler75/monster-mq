package at.rocworks.agents

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

data class AgentConfig(
    val description: String = "",
    val skills: List<AgentSkill> = emptyList(),
    val inputTopics: List<String> = emptyList(),
    val outputTopics: List<String> = emptyList(),
    val triggerType: TriggerType = TriggerType.MQTT,
    val cronExpression: String? = null,
    val cronIntervalMs: Long? = null,
    val provider: String = "gemini",
    val model: String? = null,
    val apiKey: String? = null,
    val systemPrompt: String = "",
    val maxTokens: Int? = null,
    val temperature: Double = 0.7,
    val maxToolIterations: Int = 10,
    val memoryWindowSize: Int = 20,
    val stateEnabled: Boolean = true,
    val mcpServers: List<String> = emptyList(),
    val useMonsterMqMcp: Boolean = false
) {
    companion object {
        fun fromJsonObject(json: JsonObject): AgentConfig {
            return AgentConfig(
                description = json.getString("description", ""),
                skills = json.getJsonArray("skills", JsonArray()).filterIsInstance<JsonObject>().map { AgentSkill.fromJsonObject(it) },
                inputTopics = json.getJsonArray("inputTopics", JsonArray()).filterIsInstance<String>().toList(),
                outputTopics = json.getJsonArray("outputTopics", JsonArray()).filterIsInstance<String>().toList(),
                triggerType = TriggerType.fromString(json.getString("triggerType", "MQTT")),
                cronExpression = json.getString("cronExpression"),
                cronIntervalMs = json.getLong("cronIntervalMs"),
                provider = json.getString("provider", "gemini"),
                model = json.getString("model"),
                apiKey = json.getString("apiKey"),
                systemPrompt = json.getString("systemPrompt", ""),
                maxTokens = json.getInteger("maxTokens"),
                temperature = json.getDouble("temperature", 0.7),
                maxToolIterations = json.getInteger("maxToolIterations", 10),
                memoryWindowSize = json.getInteger("memoryWindowSize", 20),
                stateEnabled = json.getBoolean("stateEnabled", true),
                mcpServers = json.getJsonArray("mcpServers", JsonArray()).filterIsInstance<String>().toList(),
                useMonsterMqMcp = json.getBoolean("useMonsterMqMcp", false)
            )
        }
    }

    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("description", description)
            .put("skills", JsonArray(skills.map { it.toJsonObject() }))
            .put("inputTopics", JsonArray(inputTopics))
            .put("outputTopics", JsonArray(outputTopics))
            .put("triggerType", triggerType.name)
            .put("cronExpression", cronExpression)
            .put("cronIntervalMs", cronIntervalMs)
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

enum class TriggerType {
    MQTT, CRON, MANUAL;

    companion object {
        fun fromString(value: String): TriggerType {
            return try { valueOf(value.uppercase()) } catch (e: Exception) { MQTT }
        }
    }
}
