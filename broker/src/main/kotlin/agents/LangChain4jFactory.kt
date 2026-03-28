package at.rocworks.agents

import at.rocworks.Utils
import dev.langchain4j.model.azure.AzureOpenAiChatModel
import dev.langchain4j.model.chat.ChatModel
import dev.langchain4j.model.chat.listener.ChatModelListener
import dev.langchain4j.model.googleai.GoogleAiGeminiChatModel
import dev.langchain4j.model.anthropic.AnthropicChatModel
import dev.langchain4j.model.openai.OpenAiChatModel
import dev.langchain4j.model.ollama.OllamaChatModel
import io.vertx.core.json.JsonObject
import java.time.Duration
import java.util.logging.Logger

/**
 * Generic chat model configuration, usable by both agents and the internal assistant.
 */
data class ChatModelConfig(
    val provider: String,
    val model: String? = null,
    val apiKey: String? = null,
    val endpoint: String? = null,
    val serviceVersion: String? = null,
    val maxTokens: Int? = null,
    val temperature: Double = 0.7,
    val enableThinking: Boolean = false
)

object LangChain4jFactory {
    private val logger: Logger = Utils.getLogger(LangChain4jFactory::class.java)

    fun createChatModel(config: ChatModelConfig, globalConfig: JsonObject, listeners: List<ChatModelListener> = emptyList()): ChatModel {
        val apiKey = resolveApiKey(config.apiKey, config.provider, globalConfig)
        val model = config.model ?: resolveDefaultModel(config.provider, globalConfig)
        logger.fine("Creating LangChain4j ${config.provider} model: ${model ?: "default"}")

        return when (config.provider.lowercase()) {
            "gemini" -> GoogleAiGeminiChatModel.builder()
                .apiKey(apiKey)
                .modelName(model ?: "gemini-2.0-flash")
                .temperature(config.temperature)
                .apply { config.maxTokens?.let { maxOutputTokens(it) } }
                .sendThinking(config.enableThinking)
                .returnThinking(config.enableThinking)
                .listeners(listeners)
                .build()

            "claude" -> AnthropicChatModel.builder()
                .apiKey(apiKey)
                .modelName(model ?: "claude-sonnet-4-20250514")
                .maxTokens(config.maxTokens ?: 4096)
                .temperature(config.temperature)
                .listeners(listeners)
                .build()

            "openai" -> OpenAiChatModel.builder()
                .apiKey(apiKey)
                .modelName(model ?: "gpt-4o")
                .apply { config.endpoint?.let { baseUrl(it) } }
                .temperature(config.temperature)
                .apply { config.maxTokens?.let { maxTokens(it) } }
                .listeners(listeners)
                .build()

            "ollama" -> {
                val ollamaTimeoutSeconds = globalConfig
                    .getJsonObject("GenAI", JsonObject())
                    .getJsonObject("Providers", JsonObject())
                    .getJsonObject("Ollama", JsonObject())
                    .getInteger("TimeoutSeconds")
                OllamaChatModel.builder()
                    .baseUrl(apiKey)
                    .modelName(model ?: "llama3")
                    .temperature(config.temperature)
                    .apply { ollamaTimeoutSeconds?.let { timeout(Duration.ofSeconds(it.toLong())) } }
                    .listeners(listeners)
                    .build()
            }

            "azure-openai" -> {
                val endpoint = resolveEndpoint(config.endpoint, globalConfig)
                val deploymentName = model ?: resolveDeploymentName(globalConfig)
                val svcVersion = resolveServiceVersion(config.serviceVersion, globalConfig)
                AzureOpenAiChatModel.builder()
                    .endpoint(endpoint)
                    .apiKey(apiKey)
                    .deploymentName(deploymentName)
                    .apply { svcVersion?.let { serviceVersion(it) } }
                    .temperature(config.temperature)
                    .apply { config.maxTokens?.let { maxTokens(it) } }
                    .listeners(listeners)
                    .build()
            }

            else -> throw IllegalArgumentException("Unknown AI provider: ${config.provider}. Supported: gemini, claude, openai, ollama, azure-openai")
        }
    }

    fun createChatModel(config: AgentConfig, globalConfig: JsonObject, listeners: List<ChatModelListener> = emptyList()): ChatModel {
        return createChatModel(
            ChatModelConfig(
                provider = config.provider,
                model = config.model,
                apiKey = config.apiKey,
                endpoint = config.endpoint,
                serviceVersion = config.serviceVersion,
                maxTokens = config.maxTokens,
                temperature = config.temperature,
                enableThinking = config.enableThinking
            ),
            globalConfig,
            listeners
        )
    }

    /**
     * Creates a chat model from a stored GenAiProviderConfig + per-agent overrides.
     * The provider supplies type, apiKey, endpoint, serviceVersion, and default model.
     * The agent can override model, temperature, maxTokens, and enableThinking.
     */
    fun createChatModel(
        providerConfig: GenAiProviderConfig,
        agentConfig: AgentConfig,
        globalConfig: JsonObject,
        listeners: List<ChatModelListener> = emptyList()
    ): ChatModel {
        return createChatModel(
            ChatModelConfig(
                provider = providerConfig.type,
                model = agentConfig.model ?: providerConfig.model,
                apiKey = if (!providerConfig.baseUrl.isNullOrBlank()) providerConfig.baseUrl else providerConfig.apiKey,
                endpoint = providerConfig.endpoint,
                serviceVersion = providerConfig.serviceVersion,
                maxTokens = agentConfig.maxTokens ?: providerConfig.maxTokens,
                temperature = agentConfig.temperature,
                enableThinking = agentConfig.enableThinking
            ),
            globalConfig,
            listeners
        )
    }

    private fun resolveApiKey(agentApiKey: String?, provider: String, globalConfig: JsonObject): String {
        // 1. Agent-specific API key
        if (!agentApiKey.isNullOrBlank()) {
            val resolved = resolveEnvVar(agentApiKey)
            if (resolved != null) return resolved
        }

        val genAiConfig = globalConfig.getJsonObject("GenAI", JsonObject())

        // 2. Per-provider key from GenAI.Providers section
        val providers = genAiConfig.getJsonObject("Providers", JsonObject())
        val providerSection = when (provider.lowercase()) {
            "gemini" -> providers.getJsonObject("Gemini", JsonObject())
            "claude" -> providers.getJsonObject("Claude", JsonObject())
            "openai" -> providers.getJsonObject("OpenAI", JsonObject())
            "ollama" -> providers.getJsonObject("Ollama", JsonObject())
            "azure-openai" -> providers.getJsonObject("AzureOpenAI", JsonObject())
            else -> JsonObject()
        }
        val providerKey = if (provider.lowercase() == "ollama") {
            providerSection.getString("BaseUrl")
        } else {
            providerSection.getString("ApiKey")
        }
        if (!providerKey.isNullOrBlank()) {
            val resolved = resolveEnvVar(providerKey)
            if (resolved != null) return resolved
        }

        // 3. Environment variable by convention
        val envVarName = when (provider.lowercase()) {
            "gemini" -> "GEMINI_API_KEY"
            "claude" -> "ANTHROPIC_API_KEY"
            "openai" -> "OPENAI_API_KEY"
            "ollama" -> "OLLAMA_BASE_URL"
            "azure-openai" -> "AZURE_OPENAI_API_KEY"
            else -> null
        }
        if (envVarName != null) {
            val envValue = System.getenv(envVarName)
            if (!envValue.isNullOrBlank()) return envValue
        }

        // 4. Default for Ollama (local, no key needed)
        if (provider.lowercase() == "ollama") {
            return "http://localhost:11434"
        }

        val configKey = when (provider.lowercase()) {
            "gemini" -> "Gemini"
            "claude" -> "Claude"
            "openai" -> "OpenAI"
            "azure-openai" -> "AzureOpenAI"
            else -> provider
        }
        throw IllegalArgumentException("No API key found for provider '$provider'. " +
            "Configure it in: agent config, GenAI.Providers.$configKey.ApiKey, or env ${envVarName ?: "variable"}")
    }

    // Returns the resolved string, or null if a ${VAR} placeholder could not be substituted
    private fun resolveEnvVar(value: String): String? {
        val envVarPattern = Regex("""\$\{([^}]+)\}""")
        var hasUnresolved = false
        val result = envVarPattern.replace(value) { matchResult ->
            val varName = matchResult.groupValues[1]
            val envValue = System.getenv(varName)
            if (envValue.isNullOrBlank()) {
                hasUnresolved = true
                logger.warning("Environment variable '$varName' is not set")
                ""
            } else {
                envValue
            }
        }
        return if (hasUnresolved) null else result
    }

    /**
     * Resolves the default model from GenAI.Providers.<Provider>.Model in config.yaml.
     */
    private fun resolveDefaultModel(provider: String, globalConfig: JsonObject): String? {
        val providerSection = globalConfig.getJsonObject("GenAI", JsonObject())
            .getJsonObject("Providers", JsonObject())
        val section = when (provider.lowercase()) {
            "gemini" -> providerSection.getJsonObject("Gemini", null)
            "claude" -> providerSection.getJsonObject("Claude", null)
            "openai" -> providerSection.getJsonObject("OpenAI", null)
            "ollama" -> providerSection.getJsonObject("Ollama", null)
            "azure-openai" -> providerSection.getJsonObject("AzureOpenAI", null)
            else -> null
        }
        return section?.getString("Model")
    }

    private fun resolveEndpoint(agentEndpoint: String?, globalConfig: JsonObject): String {
        if (!agentEndpoint.isNullOrBlank()) {
            val resolved = resolveEnvVar(agentEndpoint)
            if (resolved != null) return resolved
        }
        val azureConfig = globalConfig.getJsonObject("GenAI", JsonObject())
            .getJsonObject("Providers", JsonObject())
            .getJsonObject("AzureOpenAI", JsonObject())
        val endpoint = azureConfig.getString("Endpoint")
        if (!endpoint.isNullOrBlank()) {
            val resolved = resolveEnvVar(endpoint)
            if (resolved != null) return resolved
        }
        val envEndpoint = System.getenv("AZURE_OPENAI_ENDPOINT")
        if (!envEndpoint.isNullOrBlank()) return envEndpoint
        throw IllegalArgumentException("No endpoint found for Azure OpenAI. Configure GenAI.Providers.AzureOpenAI.Endpoint or set AZURE_OPENAI_ENDPOINT")
    }

    private fun resolveDeploymentName(globalConfig: JsonObject): String {
        val azureConfig = globalConfig.getJsonObject("GenAI", JsonObject())
            .getJsonObject("Providers", JsonObject())
            .getJsonObject("AzureOpenAI", JsonObject())
        return azureConfig.getString("Deployment") ?: "gpt-4o"
    }

    private fun resolveServiceVersion(agentServiceVersion: String?, globalConfig: JsonObject): String? {
        if (!agentServiceVersion.isNullOrBlank()) {
            return resolveEnvVar(agentServiceVersion)
        }
        val azureConfig = globalConfig.getJsonObject("GenAI", JsonObject())
            .getJsonObject("Providers", JsonObject())
            .getJsonObject("AzureOpenAI", JsonObject())
        val sv = azureConfig.getString("ServiceVersion")
        if (!sv.isNullOrBlank()) {
            val resolved = resolveEnvVar(sv)
            if (resolved != null) return resolved
        }
        val envSv = System.getenv("AZURE_OPENAI_SERVICE_VERSION")
        return if (!envSv.isNullOrBlank()) envSv else null
    }
}
