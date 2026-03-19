package at.rocworks.agents

import at.rocworks.Utils
import dev.langchain4j.model.chat.ChatModel
import dev.langchain4j.model.chat.listener.ChatModelListener
import dev.langchain4j.model.googleai.GoogleAiGeminiChatModel
import dev.langchain4j.model.anthropic.AnthropicChatModel
import dev.langchain4j.model.openai.OpenAiChatModel
import dev.langchain4j.model.ollama.OllamaChatModel
import io.vertx.core.json.JsonObject
import java.util.logging.Logger

object LangChain4jFactory {
    private val logger: Logger = Utils.getLogger(LangChain4jFactory::class.java)

    fun createChatModel(config: AgentConfig, globalConfig: JsonObject, listeners: List<ChatModelListener> = emptyList()): ChatModel {
        val apiKey = resolveApiKey(config.apiKey, config.provider, globalConfig)
        logger.fine("Creating LangChain4j ${config.provider} model: ${config.model ?: "default"}")

        return when (config.provider.lowercase()) {
            "gemini" -> GoogleAiGeminiChatModel.builder()
                .apiKey(apiKey)
                .modelName(config.model ?: "gemini-2.0-flash")
                .temperature(config.temperature)
                .apply { config.maxTokens?.let { maxOutputTokens(it) } }
                .sendThinking(true)
                .returnThinking(true)
                .listeners(listeners)
                .build()

            "claude" -> AnthropicChatModel.builder()
                .apiKey(apiKey)
                .modelName(config.model ?: "claude-sonnet-4-20250514")
                .maxTokens(config.maxTokens ?: 4096)
                .temperature(config.temperature)
                .listeners(listeners)
                .build()

            "openai" -> OpenAiChatModel.builder()
                .apiKey(apiKey)
                .modelName(config.model ?: "gpt-4o")
                .temperature(config.temperature)
                .apply { config.maxTokens?.let { maxTokens(it) } }
                .listeners(listeners)
                .build()

            "ollama" -> OllamaChatModel.builder()
                .baseUrl(apiKey)
                .modelName(config.model ?: "llama3")
                .temperature(config.temperature)
                .listeners(listeners)
                .build()

            else -> throw IllegalArgumentException("Unknown AI provider: ${config.provider}. Supported: gemini, claude, openai, ollama")
        }
    }

    private fun resolveApiKey(agentApiKey: String?, provider: String, globalConfig: JsonObject): String {
        // 1. Agent-specific API key
        if (!agentApiKey.isNullOrBlank()) {
            return resolveEnvVar(agentApiKey)
        }

        val genAiConfig = globalConfig.getJsonObject("GenAI", JsonObject())

        // 2. Per-provider key from GenAI.Providers section
        val providers = genAiConfig.getJsonObject("Providers", JsonObject())
        val providerSection = when (provider.lowercase()) {
            "gemini" -> providers.getJsonObject("Gemini", JsonObject())
            "claude" -> providers.getJsonObject("Claude", JsonObject())
            "openai" -> providers.getJsonObject("OpenAI", JsonObject())
            "ollama" -> providers.getJsonObject("Ollama", JsonObject())
            else -> JsonObject()
        }
        val providerKey = if (provider.lowercase() == "ollama") {
            providerSection.getString("BaseUrl")
        } else {
            providerSection.getString("ApiKey")
        }
        if (!providerKey.isNullOrBlank()) {
            return resolveEnvVar(providerKey)
        }

        // 3. Global GenAI.ApiKey (fallback for single-provider setups)
        val globalKey = genAiConfig.getString("ApiKey")
        if (!globalKey.isNullOrBlank()) {
            return resolveEnvVar(globalKey)
        }

        // 4. Environment variable by convention
        val envVarName = when (provider.lowercase()) {
            "gemini" -> "GEMINI_API_KEY"
            "claude" -> "ANTHROPIC_API_KEY"
            "openai" -> "OPENAI_API_KEY"
            "ollama" -> "OLLAMA_BASE_URL"
            else -> null
        }
        if (envVarName != null) {
            val envValue = System.getenv(envVarName)
            if (!envValue.isNullOrBlank()) return envValue
        }

        // 5. Default for Ollama (local, no key needed)
        if (provider.lowercase() == "ollama") {
            return "http://localhost:11434"
        }

        throw IllegalArgumentException("No API key found for provider '$provider'. " +
            "Configure it in: agent config, GenAI.Providers.$provider, GenAI.ApiKey, or env ${envVarName ?: "variable"}")
    }

    private fun resolveEnvVar(value: String): String {
        val envVarPattern = Regex("""\$\{([^}]+)\}""")
        return envVarPattern.replace(value) { matchResult ->
            val varName = matchResult.groupValues[1]
            System.getenv(varName) ?: matchResult.value
        }
    }
}
