package at.rocworks.agents

import at.rocworks.Utils
import at.rocworks.genai.GenAiRequest
import at.rocworks.genai.GenAiResponse
import at.rocworks.genai.IGenAiProvider
import dev.langchain4j.model.chat.ChatModel
import io.vertx.core.Vertx
import java.io.File
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * Adapter that wraps a LangChain4j ChatLanguageModel as an IGenAiProvider.
 * Provides backward compatibility with the existing GenAiResolver.
 */
class LangChain4jProviderAdapter(
    private val chatModel: ChatModel,
    override val providerName: String,
    override val modelName: String,
    private val docsPath: String = "docs"
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
