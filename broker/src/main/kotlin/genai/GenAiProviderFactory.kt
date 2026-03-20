package at.rocworks.genai

import at.rocworks.Utils
import at.rocworks.agents.ChatModelConfig
import at.rocworks.agents.LangChain4jFactory
import at.rocworks.agents.LangChain4jProviderAdapter
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * Factory for creating GenAI provider instances based on configuration.
 * Uses LangChain4j for all providers (gemini, claude, openai, ollama).
 */
object GenAiProviderFactory {
    private val logger: Logger = Utils.getLogger(GenAiProviderFactory::class.java)

    /**
     * Create a GenAI provider instance from configuration.
     *
     * Reads from GenAI.Assistant if present, otherwise falls back to flat GenAI.* properties.
     *
     * @param vertx Vert.x instance
     * @param globalConfig Root configuration (full config.yaml as JsonObject)
     * @return Initialized GenAI provider or null if disabled/invalid
     */
    fun create(vertx: Vertx, globalConfig: JsonObject): CompletableFuture<IGenAiProvider?> {
        val future = CompletableFuture<IGenAiProvider?>()

        try {
            val genAiConfig = globalConfig.getJsonObject("GenAI", JsonObject())

            val enabled = genAiConfig.getBoolean("Enabled", false)
            if (!enabled) {
                logger.fine("GenAI is disabled in configuration")
                future.complete(null)
                return future
            }

            // Read from Assistant sub-object, falling back to flat GenAI.* properties
            val assistantConfig = genAiConfig.getJsonObject("Assistant", null)
            val provider = assistantConfig?.getString("Provider") ?: genAiConfig.getString("Provider", "gemini")
            val model = assistantConfig?.getString("Model") ?: genAiConfig.getString("Model")
            val temperature = assistantConfig?.getDouble("Temperature") ?: genAiConfig.getDouble("Temperature", 0.7)
            val docsPath = assistantConfig?.getString("DocsPath") ?: genAiConfig.getString("DocsPath", "docs")

            // For backward compat: flat ApiKey is used as a fallback by LangChain4jFactory.resolveApiKey
            val chatModelConfig = ChatModelConfig(
                provider = provider,
                model = model,
                temperature = temperature
            )

            logger.fine("Creating GenAI assistant with provider=$provider, model=${model ?: "default"}")

            val chatModel = LangChain4jFactory.createChatModel(chatModelConfig, globalConfig)
            val adapter = LangChain4jProviderAdapter(chatModel, provider, model ?: provider, docsPath)

            logger.info("GenAI assistant initialized: provider=$provider, model=${adapter.modelName}")
            future.complete(adapter)

        } catch (e: Exception) {
            logger.severe("Error creating GenAI provider: ${e.message}")
            future.complete(null)
        }

        return future
    }
}
