package at.rocworks.agents

import at.rocworks.Utils
import at.rocworks.genai.GenAiRequest
import at.rocworks.genai.GenAiResponse
import at.rocworks.genai.IGenAiProvider
import dev.langchain4j.model.chat.ChatModel
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * Adapter that wraps a LangChain4j ChatLanguageModel as an IGenAiProvider.
 * Provides backward compatibility with the existing GenAiResolver.
 */
class LangChain4jProviderAdapter(
    private val chatModel: ChatModel,
    override val providerName: String,
    override val modelName: String
) : IGenAiProvider {
    private val logger: Logger = Utils.getLogger(LangChain4jProviderAdapter::class.java)
    private var ready = true

    override fun initialize(vertx: Vertx, config: Map<String, Any>): CompletableFuture<Void> {
        // Already initialized via constructor
        return CompletableFuture.completedFuture(null)
    }

    override fun generate(request: GenAiRequest): CompletableFuture<GenAiResponse> {
        return CompletableFuture.supplyAsync {
            try {
                val fullPrompt = buildPrompt(request)
                val response = chatModel.chat(fullPrompt)
                GenAiResponse.success(text = response, model = modelName)
            } catch (e: Exception) {
                logger.severe("LangChain4j $providerName error: ${e.message}")
                GenAiResponse.error(message = "$providerName error: ${e.message}", model = modelName)
            }
        }
    }

    override fun isReady(): Boolean = ready

    override fun shutdown(): CompletableFuture<Void> {
        ready = false
        return CompletableFuture.completedFuture(null)
    }

    private fun buildPrompt(request: GenAiRequest): String {
        val parts = mutableListOf<String>()
        if (!request.context.isNullOrBlank()) {
            parts.add("Context:\n${request.context}")
        }
        parts.add(request.prompt)
        return parts.joinToString("\n\n")
    }
}
