package at.rocworks.agents

import io.vertx.core.json.JsonObject

data class GenAiProviderConfig(
    val type: String = "gemini",
    val model: String? = null,
    val apiKey: String? = null,
    val endpoint: String? = null,       // Azure OpenAI endpoint URL
    val serviceVersion: String? = null, // Azure OpenAI API version
    val baseUrl: String? = null,        // Ollama base URL
    val temperature: Double = 0.7,
    val maxTokens: Int? = null
) {
    companion object {
        fun fromJsonObject(json: JsonObject): GenAiProviderConfig {
            return GenAiProviderConfig(
                type = json.getString("type", "gemini"),
                model = json.getString("model"),
                apiKey = json.getString("apiKey"),
                endpoint = json.getString("endpoint"),
                serviceVersion = json.getString("serviceVersion"),
                baseUrl = json.getString("baseUrl"),
                temperature = json.getDouble("temperature", 0.7),
                maxTokens = json.getInteger("maxTokens")
            )
        }
    }

    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("type", type)
            .put("model", model)
            .put("apiKey", apiKey)
            .put("endpoint", endpoint)
            .put("serviceVersion", serviceVersion)
            .put("baseUrl", baseUrl)
            .put("temperature", temperature)
            .put("maxTokens", maxTokens)
    }
}
