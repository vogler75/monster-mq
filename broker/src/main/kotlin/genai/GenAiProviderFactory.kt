package at.rocworks.genai

import at.rocworks.Utils
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * Factory for creating GenAI provider instances based on configuration
 */
object GenAiProviderFactory {
    private val logger: Logger = Utils.getLogger(GenAiProviderFactory::class.java)

    /**
     * Provider types supported by the factory
     */
    enum class ProviderType {
        GEMINI,
        CLAUDE,
        OPENAI;

        companion object {
            fun fromString(value: String?): ProviderType? {
                return when (value?.lowercase()) {
                    "gemini" -> GEMINI
                    "claude" -> CLAUDE
                    "openai" -> OPENAI
                    else -> null
                }
            }
        }
    }

    /**
     * Create a GenAI provider instance from configuration
     *
     * @param vertx Vert.x instance
     * @param config GenAI configuration section from config.yaml
     * @return Initialized GenAI provider or null if disabled/invalid
     */
    fun create(vertx: Vertx, config: JsonObject): CompletableFuture<IGenAiProvider?> {
        val future = CompletableFuture<IGenAiProvider?>()

        try {
            // Check if GenAI is enabled
            val enabled = config.getBoolean("Enabled", false)
            if (!enabled) {
                logger.info("GenAI is disabled in configuration")
                future.complete(null)
                return future
            }

            // Get provider type
            val providerTypeStr = config.getString("Provider", "gemini")
            val providerType = ProviderType.fromString(providerTypeStr)
            if (providerType == null) {
                logger.warning("Invalid GenAI provider type: $providerTypeStr. Supported: gemini, claude, openai")
                future.complete(null)
                return future
            }

            // Get API key (with environment variable substitution)
            val apiKey = resolveConfigValue(config.getString("ApiKey"))
            if (apiKey.isNullOrBlank()) {
                logger.warning("GenAI API key not configured or empty")
                future.complete(null)
                return future
            }

            // Build provider-specific configuration
            val providerConfig = mutableMapOf<String, Any>(
                "apiKey" to apiKey
            )

            config.getString("Model")?.let { providerConfig["model"] = it }
            config.getInteger("MaxTokens")?.let { providerConfig["maxTokens"] = it }
            config.getDouble("Temperature")?.let { providerConfig["temperature"] = it }
            config.getString("DocsPath")?.let { providerConfig["docsPath"] = it }

            // Create provider instance
            val provider: IGenAiProvider = when (providerType) {
                ProviderType.GEMINI -> {
                    logger.info("Creating Gemini provider")
                    GeminiProvider()
                }
                ProviderType.CLAUDE -> {
                    logger.warning("Claude provider not yet implemented")
                    future.complete(null)
                    return future
                }
                ProviderType.OPENAI -> {
                    logger.warning("OpenAI provider not yet implemented")
                    future.complete(null)
                    return future
                }
            }

            // Initialize provider
            provider.initialize(vertx, providerConfig)
                .thenAccept {
                    logger.info("GenAI provider ${provider.providerName} initialized successfully with model ${provider.modelName}")
                    future.complete(provider)
                }
                .exceptionally { error ->
                    logger.severe("Failed to initialize GenAI provider: ${error.message}")
                    future.completeExceptionally(error)
                    null
                }

        } catch (e: Exception) {
            logger.severe("Error creating GenAI provider: ${e.message}")
            future.completeExceptionally(e)
        }

        return future
    }

    /**
     * Resolve configuration value with environment variable substitution
     * Supports ${VAR_NAME} syntax
     */
    private fun resolveConfigValue(value: String?): String? {
        if (value == null) return null

        val envVarPattern = Regex("""\$\{([^}]+)\}""")
        return envVarPattern.replace(value) { matchResult ->
            val varName = matchResult.groupValues[1]
            System.getenv(varName) ?: matchResult.value
        }
    }
}
