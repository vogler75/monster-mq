package at.rocworks.genai

import at.rocworks.Utils
import com.google.genai.Client
import com.google.genai.types.GenerateContentConfig
import com.google.genai.types.GenerateContentResponse
import io.vertx.core.Vertx
import java.io.File
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * Gemini AI provider implementation
 *
 * Uses Google's Gen AI SDK to interact with Gemini models
 */
class GeminiProvider : IGenAiProvider {
    private val logger: Logger = Utils.getLogger(GeminiProvider::class.java)

    override val providerName: String = "gemini"
    override var modelName: String = "gemini-2.5-flash"
        private set

    private var client: Client? = null
    private var vertx: Vertx? = null
    private var docsPath: String = "docs"
    private var maxTokens: Int? = null  // null = use model default (no limit)
    private var temperature: Double = 0.7
    private var initialized: Boolean = false

    override fun initialize(vertx: Vertx, config: Map<String, Any>): CompletableFuture<Void> {
        val future = CompletableFuture<Void>()

        try {
            this.vertx = vertx

            // Get API key
            val apiKey = config["apiKey"] as? String
                ?: throw IllegalArgumentException("API key is required")

            // Get optional model name
            modelName = config["model"] as? String ?: "gemini-2.5-flash"

            // Get optional parameters (0 or null = no limit, use model default)
            val configMaxTokens = (config["maxTokens"] as? Number)?.toInt()
            maxTokens = if (configMaxTokens == null || configMaxTokens <= 0) null else configMaxTokens
            temperature = (config["temperature"] as? Number)?.toDouble() ?: 0.7
            docsPath = config["docsPath"] as? String ?: "docs"

            logger.fine("Initializing Gemini provider with model: $modelName")

            // Create client
            client = Client.builder()
                .apiKey(apiKey)
                .build()

            initialized = true
            logger.info("Gemini provider initialized successfully")
            future.complete(null)

        } catch (e: Exception) {
            logger.severe("Failed to initialize Gemini provider: ${e.message}")
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

                logger.finer("Sending request to Gemini: ${fullPrompt.substring(0, minOf(100, fullPrompt.length))}...")

                // Create generation config
                val configBuilder = GenerateContentConfig.builder()
                    .temperature(temperature.toFloat())

                // Only set maxOutputTokens if configured (null = use model default)
                if (maxTokens != null) {
                    configBuilder.maxOutputTokens(maxTokens!!)
                }

                val config = configBuilder.build()

                // Call Gemini API
                logger.fine("Calling Gemini API with model: $modelName")
                val response: GenerateContentResponse = client!!.models.generateContent(
                    modelName,
                    fullPrompt,
                    config
                )

                // Extract response text
                val responseText = response.text() ?: ""

                logger.fine("Received response from Gemini")

                GenAiResponse.success(
                    text = responseText,
                    model = modelName
                )

            } catch (e: Exception) {
                logger.severe("Gemini API error: ${e.message}")
                e.printStackTrace()
                GenAiResponse.error(
                    message = "Gemini API error: ${e.message}",
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
        logger.fine("Gemini provider shutdown")
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
            val docsContent = loadDocumentation(request.docs)
            if (docsContent.isNotEmpty()) {
                parts.add("# Documentation Context\n\n$docsContent")
            }
        }

        // Add context if provided
        if (!request.context.isNullOrBlank()) {
            parts.add("# Additional Context\n\n${request.context}")
        }

        // Add the main prompt
        parts.add("# Question\n\n${request.prompt}")

        return parts.joinToString("\n\n---\n\n")
    }

    /**
     * Load documentation files from the classpath (resources/docs) or filesystem
     */
    private fun loadDocumentation(docPaths: List<String>): String {
        val docContents = mutableListOf<String>()

        for (docPath in docPaths) {
            try {
                var content: String? = null
                var fileName = docPath

                // First, try loading from classpath (resources/docs/)
                val resourcePath = if (docPath.startsWith("/")) {
                    docPath.substring(1)
                } else {
                    "$docsPath/$docPath"
                }

                val resourceStream = this::class.java.classLoader.getResourceAsStream(resourcePath)
                if (resourceStream != null) {
                    content = resourceStream.bufferedReader().use { it.readText() }
                    logger.finer("Loaded documentation from classpath: $resourcePath")
                } else {
                    // Fallback to filesystem
                    val file = File(docPath).let { f ->
                        if (f.isAbsolute) f
                        else File(docsPath, docPath)
                    }

                    if (file.exists() && file.isFile) {
                        content = file.readText()
                        fileName = file.name
                        logger.finer("Loaded documentation from filesystem: ${file.absolutePath}")
                    } else {
                        logger.warning("Documentation file not found: $docPath (tried classpath:$resourcePath and filesystem:${file.absolutePath})")
                    }
                }

                if (content != null) {
                    docContents.add("## $fileName\n\n$content")
                }
            } catch (e: Exception) {
                logger.warning("Failed to load documentation file $docPath: ${e.message}")
            }
        }

        return docContents.joinToString("\n\n---\n\n")
    }
}
