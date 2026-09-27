package at.rocworks.agents

import at.rocworks.Utils
import dev.langchain4j.model.anthropic.AnthropicStreamingChatModel
import dev.langchain4j.model.azure.AzureOpenAiChatModel
import dev.langchain4j.model.azure.AzureOpenAiEmbeddingModel
import dev.langchain4j.model.azure.AzureOpenAiStreamingChatModel
import dev.langchain4j.model.chat.StreamingChatModel
import dev.langchain4j.model.embedding.EmbeddingModel
import dev.langchain4j.model.googleai.GoogleAiEmbeddingModel
import dev.langchain4j.model.googleai.GoogleAiGeminiStreamingChatModel
import dev.langchain4j.model.ollama.OllamaEmbeddingModel
import dev.langchain4j.model.ollama.OllamaStreamingChatModel
import dev.langchain4j.model.openai.OpenAiEmbeddingModel
import dev.langchain4j.model.openai.OpenAiStreamingChatModel
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
    val enableThinking: Boolean = false,
    val timeoutSeconds: Long? = null  // per-agent request timeout; overrides provider defaults when larger
)

object LangChain4jFactory {
    private val logger: Logger = Utils.getLogger(LangChain4jFactory::class.java)

    private data class ResolvedSettings(
        val apiKey: String,
        val model: String,
        val timeout: Duration?,
        val setTemperature: Boolean
    )

    private fun configProviderKey(provider: String) = when (provider.lowercase()) {
        "gemini" -> "Gemini"
        "claude" -> "Claude"
        "openai" -> "OpenAI"
        "ollama" -> "Ollama"
        "azure-openai" -> "AzureOpenAI"
        "llamacpp" -> "LlamaCpp"
        "openrouter" -> "OpenRouter"
        else -> provider
    }

    private fun resolveSettings(config: ChatModelConfig, globalConfig: JsonObject): ResolvedSettings {
        val apiKey = resolveApiKey(config.apiKey, config.provider, globalConfig)
        val configProviderKey = configProviderKey(config.provider)
        val model = (config.model ?: resolveDefaultModel(config.provider, globalConfig))?.takeIf { it.isNotBlank() }
            ?: throw IllegalArgumentException("No model configured for AI provider '${config.provider}'. A model must be specified in the agent/provider configuration or in GenAI.Providers.$configProviderKey.Model")

        val globalProviderTimeout = globalConfig
            .getJsonObject("GenAI", JsonObject())
            .getJsonObject("Providers", JsonObject())
            .getJsonObject(configProviderKey, JsonObject())
            .getInteger("TimeoutSeconds")?.toLong()
        val effectiveTimeout = listOfNotNull(config.timeoutSeconds, globalProviderTimeout).maxOrNull()

        return ResolvedSettings(
            apiKey = apiKey,
            model = model,
            timeout = effectiveTimeout?.let { Duration.ofSeconds(it) },
            setTemperature = config.temperature > 0.0 && !config.enableThinking
        )
    }

    fun createChatModel(config: ChatModelConfig, globalConfig: JsonObject, listeners: List<ChatModelListener> = emptyList()): ChatModel {
        val settings = resolveSettings(config, globalConfig)
        val apiKey = settings.apiKey
        val model = settings.model
        val shouldSetTemperature = settings.setTemperature
        val effectiveTimeout = settings.timeout?.seconds
        logger.fine("Creating LangChain4j ${config.provider} model: $model")

        return when (config.provider.lowercase()) {
            "gemini" -> GoogleAiGeminiChatModel.builder()
                .apiKey(apiKey)
                .modelName(model)
                .apply { if (shouldSetTemperature) temperature(config.temperature) }
                .apply { config.maxTokens?.let { maxOutputTokens(it) } }
                .sendThinking(config.enableThinking)
                .returnThinking(config.enableThinking)
                .apply { effectiveTimeout?.let { timeout(Duration.ofSeconds(it)) } }
                .listeners(listeners)
                .build()

            "claude" -> AnthropicChatModel.builder()
                .apiKey(apiKey)
                .modelName(model)
                .maxTokens(config.maxTokens ?: 4096)
                .apply { if (shouldSetTemperature) temperature(config.temperature) }
                .apply { effectiveTimeout?.let { timeout(Duration.ofSeconds(it)) } }
                .listeners(listeners)
                .build()

            "openai" -> OpenAiChatModel.builder()
                .apiKey(apiKey)
                .modelName(model)
                .apply { config.endpoint?.let { baseUrl(it) } }
                .apply { if (shouldSetTemperature) temperature(config.temperature) }
                .apply { config.maxTokens?.let { maxTokens(it) } }
                .apply { effectiveTimeout?.let { timeout(Duration.ofSeconds(it)) } }
                .listeners(listeners)
                .build()

            "ollama" -> OllamaChatModel.builder()
                .baseUrl(apiKey)
                .modelName(model)
                .apply { if (shouldSetTemperature) temperature(config.temperature) }
                .apply { effectiveTimeout?.let { timeout(Duration.ofSeconds(it)) } }
                .listeners(listeners)
                .build()

            "azure-openai" -> {
                val endpoint = resolveEndpoint(config.endpoint, globalConfig)
                val deploymentName = model
                val svcVersion = resolveServiceVersion(config.serviceVersion, globalConfig)
                AzureOpenAiChatModel.builder()
                    .endpoint(endpoint)
                    .apiKey(apiKey)
                    .deploymentName(deploymentName)
                    .apply { svcVersion?.let { serviceVersion(it) } }
                    .apply { if (shouldSetTemperature) temperature(config.temperature) }
                    .apply { config.maxTokens?.let { maxTokens(it) } }
                    .apply { effectiveTimeout?.let { timeout(Duration.ofSeconds(it)) } }
                    .listeners(listeners)
                    .build()
            }

            "llamacpp" -> {
                val effectiveEndpoint = config.endpoint ?: "http://localhost:8080/v1"
                OpenAiChatModel.builder()
                    .apiKey(apiKey.takeIf { !it.isNullOrBlank() } ?: "dummy-key")
                    .modelName(model)
                    .baseUrl(effectiveEndpoint)
                    .apply { if (shouldSetTemperature) temperature(config.temperature) }
                    .apply { config.maxTokens?.let { maxTokens(it) } }
                    .apply { effectiveTimeout?.let { timeout(Duration.ofSeconds(it)) } }
                    .listeners(listeners)
                    .build()
            }

            "openrouter" -> {
                val effectiveEndpoint = config.endpoint?.takeIf { it.isNotBlank() } ?: "https://openrouter.ai/api/v1"
                OpenAiChatModel.builder()
                    .apiKey(apiKey)
                    .modelName(model)
                    .baseUrl(effectiveEndpoint)
                    .apply { if (shouldSetTemperature) temperature(config.temperature) }
                    .apply { config.maxTokens?.let { maxTokens(it) } }
                    .apply { effectiveTimeout?.let { timeout(Duration.ofSeconds(it)) } }
                    .listeners(listeners)
                    .build()
            }

            else -> throw IllegalArgumentException("Unknown AI provider: ${config.provider}. Supported: gemini, claude, openai, ollama, azure-openai, llamacpp, openrouter")
        }
    }

    fun createChatModel(config: AgentConfig, globalConfig: JsonObject, listeners: List<ChatModelListener> = emptyList()): ChatModel {
        return createChatModel(toChatModelConfig(config), globalConfig, listeners)
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
        return createChatModel(toChatModelConfig(agentConfig, providerConfig), globalConfig, listeners)
    }

    /**
     * Builds the effective model configuration of an agent, optionally backed by a stored GenAI provider.
     */
    fun toChatModelConfig(agentConfig: AgentConfig, providerConfig: GenAiProviderConfig? = null): ChatModelConfig {
        if (providerConfig == null) {
            return ChatModelConfig(
                provider = agentConfig.provider,
                model = agentConfig.model,
                apiKey = agentConfig.apiKey,
                endpoint = agentConfig.endpoint,
                serviceVersion = agentConfig.serviceVersion,
                maxTokens = agentConfig.maxTokens,
                temperature = agentConfig.temperature,
                enableThinking = agentConfig.enableThinking,
                timeoutSeconds = agentConfig.taskTimeoutSeconds
            )
        }
        return ChatModelConfig(
            provider = providerConfig.type,
            model = agentConfig.model ?: providerConfig.model,
            apiKey = if (!providerConfig.baseUrl.isNullOrBlank()) providerConfig.baseUrl else providerConfig.apiKey,
            endpoint = providerConfig.endpoint,
            serviceVersion = providerConfig.serviceVersion,
            maxTokens = agentConfig.maxTokens ?: providerConfig.maxTokens,
            temperature = agentConfig.temperature,
            enableThinking = agentConfig.enableThinking,
            timeoutSeconds = agentConfig.taskTimeoutSeconds
        )
    }

    /**
     * Creates a token-streaming chat model. Supports the same providers as [createChatModel].
     */
    fun createStreamingChatModel(config: ChatModelConfig, globalConfig: JsonObject, listeners: List<ChatModelListener> = emptyList()): StreamingChatModel {
        val settings = resolveSettings(config, globalConfig)
        val apiKey = settings.apiKey
        val model = settings.model
        val timeout = settings.timeout
        logger.fine("Creating LangChain4j ${config.provider} streaming model: $model")

        return when (config.provider.lowercase()) {
            "gemini" -> GoogleAiGeminiStreamingChatModel.builder()
                .apiKey(apiKey)
                .modelName(model)
                .apply { if (settings.setTemperature) temperature(config.temperature) }
                .apply { config.maxTokens?.let { maxOutputTokens(it) } }
                .sendThinking(config.enableThinking)
                .returnThinking(config.enableThinking)
                .apply { timeout?.let { timeout(it) } }
                .listeners(listeners)
                .build()

            "claude" -> AnthropicStreamingChatModel.builder()
                .apiKey(apiKey)
                .modelName(model)
                .maxTokens(config.maxTokens ?: 4096)
                .apply { if (settings.setTemperature) temperature(config.temperature) }
                .apply { timeout?.let { timeout(it) } }
                .listeners(listeners)
                .build()

            "openai", "llamacpp", "openrouter" -> {
                val baseUrl = when (config.provider.lowercase()) {
                    "llamacpp" -> config.endpoint ?: "http://localhost:8080/v1"
                    "openrouter" -> config.endpoint?.takeIf { it.isNotBlank() } ?: "https://openrouter.ai/api/v1"
                    else -> config.endpoint
                }
                OpenAiStreamingChatModel.builder()
                    .apiKey(apiKey.takeIf { it.isNotBlank() } ?: "dummy-key")
                    .modelName(model)
                    .apply { baseUrl?.let { baseUrl(it) } }
                    .apply { if (settings.setTemperature) temperature(config.temperature) }
                    .apply { config.maxTokens?.let { maxTokens(it) } }
                    .apply { timeout?.let { timeout(it) } }
                    .listeners(listeners)
                    .build()
            }

            "ollama" -> OllamaStreamingChatModel.builder()
                .baseUrl(apiKey)
                .modelName(model)
                .apply { if (settings.setTemperature) temperature(config.temperature) }
                .apply { timeout?.let { timeout(it) } }
                .listeners(listeners)
                .build()

            "azure-openai" -> AzureOpenAiStreamingChatModel.builder()
                .endpoint(resolveEndpoint(config.endpoint, globalConfig))
                .apiKey(apiKey)
                .deploymentName(model)
                .apply { resolveServiceVersion(config.serviceVersion, globalConfig)?.let { serviceVersion(it) } }
                .apply { if (settings.setTemperature) temperature(config.temperature) }
                .apply { config.maxTokens?.let { maxTokens(it) } }
                .apply { timeout?.let { timeout(it) } }
                .listeners(listeners)
                .build()

            else -> throw IllegalArgumentException("Unknown AI provider: ${config.provider}. Supported: gemini, claude, openai, ollama, azure-openai, llamacpp, openrouter")
        }
    }

    /**
     * Creates an embedding model for semantic search. Claude has no embedding API, so a
     * separate embedding provider (gemini, openai, ollama, azure-openai) must be used with it.
     */
    fun createEmbeddingModel(
        provider: String,
        model: String?,
        apiKey: String?,
        endpoint: String?,
        serviceVersion: String?,
        globalConfig: JsonObject
    ): EmbeddingModel {
        val key = resolveApiKey(apiKey, provider, globalConfig)
        val timeout = Duration.ofSeconds(60)
        return when (provider.lowercase()) {
            "gemini" -> GoogleAiEmbeddingModel.builder()
                .apiKey(key)
                .modelName(model ?: "gemini-embedding-001")
                .timeout(timeout)
                .build()
            "openai", "llamacpp", "openrouter" -> OpenAiEmbeddingModel.builder()
                .apiKey(key)
                .modelName(model ?: "text-embedding-3-small")
                .apply { endpoint?.let { baseUrl(it) } }
                .timeout(timeout)
                .build()
            "ollama" -> OllamaEmbeddingModel.builder()
                .baseUrl(key)
                .modelName(model ?: "nomic-embed-text")
                .timeout(timeout)
                .build()
            "azure-openai" -> AzureOpenAiEmbeddingModel.builder()
                .endpoint(resolveEndpoint(endpoint, globalConfig))
                .apiKey(key)
                .deploymentName(model ?: "text-embedding-3-small")
                .apply { resolveServiceVersion(serviceVersion, globalConfig)?.let { serviceVersion(it) } }
                .timeout(timeout)
                .build()
            else -> throw IllegalArgumentException("Provider '$provider' does not support embeddings. Set embeddingProvider to gemini, openai, ollama or azure-openai")
        }
    }

    fun resolveApiKey(agentApiKey: String?, provider: String, globalConfig: JsonObject): String {
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
            "llamacpp" -> providers.getJsonObject("LlamaCpp", JsonObject())
            "openrouter" -> providers.getJsonObject("OpenRouter", JsonObject())
            else -> JsonObject()
        }
        val providerKey = if (provider.lowercase() == "ollama") {
            providerSection.getString("BaseUrl")
        } else if (provider.lowercase() == "llamacpp") {
            providerSection.getString("Endpoint")
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
            "llamacpp" -> "LLAMACPP_API_KEY"
            "openrouter" -> "OPENROUTER_API_KEY"
            else -> null
        }
        if (envVarName != null) {
            val envValue = System.getenv(envVarName)
            if (!envValue.isNullOrBlank()) return envValue
        }

        // 4. Default for Ollama / LlamaCpp (local, no key needed)
        if (provider.lowercase() == "ollama") {
            return "http://localhost:11434"
        }
        if (provider.lowercase() == "llamacpp") {
            return "dummy-key"
        }

        val configKey = when (provider.lowercase()) {
            "gemini" -> "Gemini"
            "claude" -> "Claude"
            "openai" -> "OpenAI"
            "azure-openai" -> "AzureOpenAI"
            "llamacpp" -> "LlamaCpp"
            "openrouter" -> "OpenRouter"
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
    fun resolveDefaultModel(provider: String, globalConfig: JsonObject): String? {
        val providerSection = globalConfig.getJsonObject("GenAI", JsonObject())
            .getJsonObject("Providers", JsonObject())
        val section = when (provider.lowercase()) {
            "gemini" -> providerSection.getJsonObject("Gemini", null)
            "claude" -> providerSection.getJsonObject("Claude", null)
            "openai" -> providerSection.getJsonObject("OpenAI", null)
            "ollama" -> providerSection.getJsonObject("Ollama", null)
            "azure-openai" -> providerSection.getJsonObject("AzureOpenAI", null)
            "llamacpp" -> providerSection.getJsonObject("LlamaCpp", null)
            "openrouter" -> providerSection.getJsonObject("OpenRouter", null)
            else -> null
        }
        return section?.getString("Model") ?: section?.getString("Deployment")
    }

    fun resolveEndpoint(agentEndpoint: String?, globalConfig: JsonObject): String {
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
