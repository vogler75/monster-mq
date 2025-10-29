package at.rocworks.extensions.graphql

import at.rocworks.Utils
import at.rocworks.genai.GenAiRequest
import at.rocworks.genai.IGenAiProvider
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL resolver for GenAI queries
 *
 * Provides AI-assisted JavaScript coding through the dashboard
 */
class GenAiResolver(
    private val vertx: Vertx,
    private val genAiProvider: IGenAiProvider?
) {
    private val logger: Logger = Utils.getLogger(GenAiResolver::class.java)

    /**
     * Root genai query resolver
     * Returns an empty map - actual resolvers are on GenAiQuery type
     */
    fun genai(): DataFetcher<CompletableFuture<Map<String, Any>?>> {
        return DataFetcher { _ ->
            CompletableFuture.completedFuture(
                if (genAiProvider != null && genAiProvider.isReady()) {
                    emptyMap()
                } else {
                    null // Return null if GenAI is not available
                }
            )
        }
    }

    /**
     * Ask query resolver
     * Sends a request to the AI provider with prompt, context, and docs
     */
    fun ask(): DataFetcher<CompletableFuture<GenAiResponseGraphQL>> {
        return DataFetcher { env ->
            val future = CompletableFuture<GenAiResponseGraphQL>()

            try {
                // Check if provider is available
                if (genAiProvider == null || !genAiProvider.isReady()) {
                    val errorResponse = GenAiResponseGraphQL(
                        response = "",
                        model = null,
                        error = "GenAI is not enabled or not configured"
                    )
                    future.complete(errorResponse)
                    return@DataFetcher future
                }

                // Get arguments
                val prompt = env.getArgument<String>("prompt")
                if (prompt.isNullOrBlank()) {
                    val errorResponse = GenAiResponseGraphQL(
                        response = "",
                        model = genAiProvider.modelName,
                        error = "Prompt is required and cannot be empty"
                    )
                    future.complete(errorResponse)
                    return@DataFetcher future
                }

                val context = env.getArgument<String?>("context")
                val docs = env.getArgument<List<String>?>("docs")

                logger.info("GenAI request received: prompt length=${prompt.length}, docs=${docs?.size ?: 0}")

                // Create request
                val request = GenAiRequest(
                    prompt = prompt,
                    context = context,
                    docs = docs
                )

                // Call provider asynchronously
                genAiProvider.ask(request)
                    .thenAccept { response ->
                        val graphqlResponse = GenAiResponseGraphQL(
                            response = response.response,
                            model = response.model,
                            error = response.error
                        )
                        future.complete(graphqlResponse)
                    }
                    .exceptionally { error ->
                        logger.warning("GenAI request failed: ${error.message}")
                        val errorResponse = GenAiResponseGraphQL(
                            response = "",
                            model = genAiProvider.modelName,
                            error = "AI request failed: ${error.message}"
                        )
                        future.complete(errorResponse)
                        null
                    }

            } catch (e: Exception) {
                logger.severe("Error processing GenAI request: ${e.message}")
                val errorResponse = GenAiResponseGraphQL(
                    response = "",
                    model = genAiProvider?.modelName,
                    error = "Internal error: ${e.message}"
                )
                future.complete(errorResponse)
            }

            future
        }
    }
}

/**
 * GraphQL response type for GenAI
 * Maps to the GenAiResponse type in the schema
 */
data class GenAiResponseGraphQL(
    val response: String,
    val model: String?,
    val error: String?
)
