package at.rocworks.genai

import at.rocworks.Utils
import com.azure.ai.openai.OpenAIClient
import com.azure.ai.openai.OpenAIClientBuilder
import com.azure.ai.openai.models.*
import com.azure.core.credential.AzureKeyCredential
import io.vertx.core.Vertx
import java.io.File
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * Azure OpenAI provider implementation
 *
 * Uses Azure OpenAI Service SDK to interact with deployed GPT models
 */
class AzureOpenAiProvider : IGenAiProvider {
    private val logger: Logger = Utils.getLogger(AzureOpenAiProvider::class.java)

    override val providerName: String = "azure-openai"
    override var modelName: String = "gpt-4o"
        private set

    private var client: OpenAIClient? = null
    private var vertx: Vertx? = null
    private var endpoint: String? = null
    private var deploymentName: String? = null
    private var docsPath: String = "docs"
    private var maxTokens: Int = 2048
    private var temperature: Double = 0.7
    private var initialized: Boolean = false

    override fun initialize(vertx: Vertx, config: Map<String, Any>): CompletableFuture<Void> {
        val future = CompletableFuture<Void>()

        try {
            this.vertx = vertx

            // Get required Azure endpoint
            endpoint = config["endpoint"] as? String
                ?: throw IllegalArgumentException("Azure OpenAI endpoint is required")

            // Get API key
            val apiKey = config["apiKey"] as? String
                ?: throw IllegalArgumentException("API key is required")

            // Get deployment name (required for Azure OpenAI)
            deploymentName = config["deployment"] as? String
                ?: throw IllegalArgumentException("Deployment name is required")

            // Get optional model name (for display purposes)
            modelName = config["model"] as? String ?: "gpt-4o"

            // Get optional parameters
            maxTokens = (config["maxTokens"] as? Number)?.toInt() ?: 2048
            temperature = (config["temperature"] as? Number)?.toDouble() ?: 0.7
            docsPath = config["docsPath"] as? String ?: "docs"

            logger.info("Initializing Azure OpenAI provider with endpoint: $endpoint, deployment: $deploymentName")

            // Create client
            client = OpenAIClientBuilder()
                .endpoint(endpoint)
                .credential(AzureKeyCredential(apiKey))
                .buildClient()

            initialized = true
            logger.info("Azure OpenAI provider initialized successfully")
            future.complete(null)

        } catch (e: Exception) {
            logger.severe("Failed to initialize Azure OpenAI provider: ${e.message}")
            initialized = false
            future.completeExceptionally(e)
        }

        return future
    }

    override fun generate(request: GenAiRequest): CompletableFuture<GenAiResponse> {
        if (!isReady()) {
            return CompletableFuture.completedFuture(
                GenAiResponse.error("Provider not initialized")
            )
        }

        // Execute on virtual thread pool to avoid blocking
        return CompletableFuture.supplyAsync {
            try {
                // Build the complete prompt with context and docs
                val fullPrompt = buildPrompt(request)

                logger.fine("Sending request to Azure OpenAI: ${fullPrompt.substring(0, minOf(100, fullPrompt.length))}...")

                // Create chat messages
                val messages = listOf(
                    ChatRequestSystemMessage("You are an expert JavaScript developer helping users write code for an MQTT/IoT workflow engine. Provide clear, concise, and working code examples."),
                    ChatRequestUserMessage(fullPrompt)
                )

                // Create completion options
                val options = ChatCompletionsOptions(messages)
                    .setMaxTokens(maxTokens)
                    .setTemperature(temperature)

                // Call Azure OpenAI API
                logger.info("Calling Azure OpenAI with deployment: $deploymentName")
                val response: ChatCompletions = client!!.getChatCompletions(deploymentName, options)

                // Extract response text
                val choices = response.choices
                if (choices.isEmpty()) {
                    logger.warning("Azure OpenAI returned no choices")
                    return@supplyAsync GenAiResponse.error(
                        message = "No response generated",
                        model = modelName
                    )
                }

                val responseText = choices[0].message.content ?: ""
                logger.info("Received response from Azure OpenAI")

                GenAiResponse.success(
                    text = responseText,
                    model = "${modelName} (${deploymentName})"
                )

            } catch (e: Exception) {
                logger.severe("Azure OpenAI API error: ${e.message}")
                e.printStackTrace()
                GenAiResponse.error(
                    message = "Azure OpenAI API error: ${e.message}",
                    model = modelName
                )
            }
        }
    }

    override fun isReady(): Boolean = initialized && client != null

    override fun shutdown(): CompletableFuture<Void> {
        val future = CompletableFuture<Void>()
        client = null
        initialized = false
        logger.info("Azure OpenAI provider shutdown")
        future.complete(null)
        return future
    }

    /**
     * Build the complete prompt including context and documentation
     */
    private fun buildPrompt(request: GenAiRequest): String {
        val parts = mutableListOf<String>()

        // Add documentation if specified
        if (!request.docs.isNullOrEmpty()) {
            parts.add("=== DOCUMENTATION ===")
            request.docs.forEach { docName ->
                val docContent = loadDocumentation(docName)
                if (docContent != null) {
                    parts.add("\n--- $docName ---\n")
                    parts.add(docContent)
                }
            }
            parts.add("\n=== END DOCUMENTATION ===\n")
        }

        // Add context if provided
        if (!request.context.isNullOrBlank()) {
            parts.add("=== CONTEXT ===")
            parts.add(request.context)
            parts.add("=== END CONTEXT ===\n")
        }

        // Add the main prompt
        parts.add(request.prompt)

        return parts.joinToString("\n")
    }

    /**
     * Load documentation file from resources
     */
    private fun loadDocumentation(filename: String): String? {
        return try {
            val resourcePath = "/$docsPath/$filename"
            val resource = this::class.java.getResourceAsStream(resourcePath)
            
            if (resource != null) {
                resource.bufferedReader().use { it.readText() }
            } else {
                // Try loading from file system
                val file = File("src/main/resources/$docsPath/$filename")
                if (file.exists()) {
                    file.readText()
                } else {
                    logger.warning("Documentation file not found: $filename")
                    null
                }
            }
        } catch (e: Exception) {
            logger.warning("Failed to load documentation $filename: ${e.message}")
            null
        }
    }
}
