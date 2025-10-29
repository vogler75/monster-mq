package at.rocworks.genai

import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture

/**
 * Interface for GenAI providers (Gemini, Claude, OpenAI, etc.)
 *
 * Implementations should be thread-safe and handle their own error recovery.
 */
interface IGenAiProvider {
    /**
     * The name of this provider (e.g., "gemini", "claude", "openai")
     */
    val providerName: String

    /**
     * The model being used by this provider
     */
    val modelName: String

    /**
     * Initialize the provider with configuration
     *
     * @param vertx Vert.x instance for async operations
     * @param config Provider-specific configuration
     * @return CompletableFuture that completes when initialization is done
     */
    fun initialize(vertx: Vertx, config: Map<String, Any>): CompletableFuture<Void>

    /**
     * Generate a response from the AI provider
     *
     * @param request The GenAI request containing prompt, context, and docs
     * @return CompletableFuture with the AI response
     */
    fun generate(request: GenAiRequest): CompletableFuture<GenAiResponse>

    /**
     * Check if the provider is ready to accept requests
     *
     * @return true if initialized and ready, false otherwise
     */
    fun isReady(): Boolean

    /**
     * Shutdown the provider and release resources
     *
     * @return CompletableFuture that completes when shutdown is done
     */
    fun shutdown(): CompletableFuture<Void>
}
