package at.rocworks.extensions.graphql

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.genai.GenAiRequest
import at.rocworks.genai.IGenAiProvider
import at.rocworks.handlers.ArchiveHandler
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL resolver for GenAI queries
 *
 * Provides AI-assisted JavaScript coding through the dashboard
 * and topic tree analysis capabilities
 */
class GenAiResolver(
    private val vertx: Vertx,
    private val genAiProvider: IGenAiProvider?,
    private val archiveHandler: ArchiveHandler? = null
) {
    private val logger: Logger = Utils.getLogger(GenAiResolver::class.java)

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    // Default system prompt for topic analysis
    private val topicAnalysisSystemPrompt = """
You are an MQTT topic tree analyst helping users understand their IoT data.
The data below shows MQTT topics and their current values in a hierarchical tree format.

**Output format:** Use Markdown formatting in your responses:
- Use ## headings for sections
- Use **bold** for emphasis
- Use `code` for topic names and values
- Use bullet lists for findings
- Use tables when comparing data

When analyzing:
- Identify naming patterns and hierarchy structure
- Spot anomalies or unusual values
- Recognize IoT patterns (Sparkplug, Homie, etc.)
- Provide concise, actionable insights

Topic data:
""".trimIndent()

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
     * Generate query resolver
     * Sends a request to the AI provider with prompt, context, and docs
     */
    fun generate(): DataFetcher<CompletableFuture<GenAiResponseGraphQL>> {
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
                genAiProvider.generate(request)
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

    /**
     * Analyze topics using AI
     * Collects topic values from LastValueStore and sends them to the LLM for analysis
     */
    fun analyzeTopics(): DataFetcher<CompletableFuture<TopicAnalysisResponseGraphQL>> {
        return DataFetcher { env ->
            val future = CompletableFuture<TopicAnalysisResponseGraphQL>()

            try {
                // Check if provider is available
                if (genAiProvider == null || !genAiProvider.isReady()) {
                    future.complete(TopicAnalysisResponseGraphQL(
                        response = "",
                        topicsAnalyzed = 0,
                        model = null,
                        error = "GenAI is not enabled or not configured"
                    ))
                    return@DataFetcher future
                }

                // Get arguments (required fields - null check for safety)
                val archiveGroupName = env.getArgument<String?>("archiveGroup") ?: "Default"
                val topicPattern = env.getArgument<String?>("topicPattern") ?: "#"
                val question = env.getArgument<String?>("question")
                val customSystemPrompt = env.getArgument<String?>("systemPrompt")
                val maxTopics = env.getArgument<Int?>("maxTopics") ?: 100
                val maxValueLength = env.getArgument<Int?>("maxValueLength") ?: 1000

                // Get chat history for follow-up questions
                @Suppress("UNCHECKED_CAST")
                val chatHistoryRaw = env.getArgument<List<Map<String, String>>?>("chatHistory")
                val chatHistory = chatHistoryRaw?.map { msg ->
                    ChatMessageInput(
                        role = msg["role"] ?: "user",
                        content = msg["content"] ?: ""
                    )
                } ?: emptyList()

                val isFollowUp = chatHistory.isNotEmpty()

                if (question.isNullOrBlank()) {
                    future.complete(TopicAnalysisResponseGraphQL(
                        response = "",
                        topicsAnalyzed = 0,
                        model = genAiProvider.modelName,
                        error = "Question is required and cannot be empty"
                    ))
                    return@DataFetcher future
                }

                // Check if archiveHandler is available
                if (archiveHandler == null) {
                    future.complete(TopicAnalysisResponseGraphQL(
                        response = "",
                        topicsAnalyzed = 0,
                        model = genAiProvider.modelName,
                        error = "ArchiveHandler is not available"
                    ))
                    return@DataFetcher future
                }

                // Get archive group
                val deployedGroups = archiveHandler.getDeployedArchiveGroups()
                val archiveGroup = deployedGroups[archiveGroupName]
                if (archiveGroup == null) {
                    future.complete(TopicAnalysisResponseGraphQL(
                        response = "",
                        topicsAnalyzed = 0,
                        model = genAiProvider.modelName,
                        error = "Archive group '$archiveGroupName' not found"
                    ))
                    return@DataFetcher future
                }

                val lastValueStore = archiveGroup.lastValStore
                if (lastValueStore == null) {
                    future.complete(TopicAnalysisResponseGraphQL(
                        response = "",
                        topicsAnalyzed = 0,
                        model = genAiProvider.modelName,
                        error = "Archive group '$archiveGroupName' has no LastValueStore configured"
                    ))
                    return@DataFetcher future
                }

                // Collect topics matching pattern
                val topics = mutableListOf<BrokerMessage>()
                lastValueStore.findMatchingMessages(topicPattern) { msg ->
                    if (topics.size < maxTopics) {
                        topics.add(msg)
                        true // continue
                    } else {
                        false // stop
                    }
                }

                if (topics.isEmpty()) {
                    future.complete(TopicAnalysisResponseGraphQL(
                        response = "",
                        topicsAnalyzed = 0,
                        model = genAiProvider.modelName,
                        error = "No topics found matching pattern '$topicPattern'"
                    ))
                    return@DataFetcher future
                }

                // Build hierarchical context string to reduce token usage
                val topicValues = mutableMapOf<String, String>()
                var analyzedCount = 0

                for (msg in topics) {
                    val value = formatTopicValue(msg, maxValueLength)
                    if (value != null) {
                        topicValues[msg.topicName] = value
                        analyzedCount++
                    }
                }

                val contextBuilder = StringBuilder()
                formatTopicsAsHierarchy(topicValues, contextBuilder)

                if (analyzedCount == 0) {
                    future.complete(TopicAnalysisResponseGraphQL(
                        response = "",
                        topicsAnalyzed = 0,
                        model = genAiProvider.modelName,
                        error = "No text-based topic values found (all values may be binary)"
                    ))
                    return@DataFetcher future
                }

                // Build prompt - always include topic data, add chat history for follow-ups
                val systemPrompt = if (!customSystemPrompt.isNullOrBlank()) customSystemPrompt else topicAnalysisSystemPrompt

                val fullPrompt = if (isFollowUp) {
                    // Follow-up question: include topic data AND chat history
                    val historyText = chatHistory.joinToString("\n\n") { msg ->
                        when (msg.role) {
                            "user" -> "User: ${msg.content}"
                            "assistant" -> "Assistant: ${msg.content}"
                            else -> "${msg.role}: ${msg.content}"
                        }
                    }
                    """
$systemPrompt

$contextBuilder

Previous conversation:
$historyText

User follow-up question: $question
""".trimIndent()
                } else {
                    // First question: include topic data only
                    """
$systemPrompt

$contextBuilder

User question: $question
""".trimIndent()
                }

                logger.info("Topic analysis request: pattern=$topicPattern, topics=$analyzedCount, isFollowUp=$isFollowUp, question length=${question.length}")
                logger.fine { "Topic analysis prompt being sent to LLM:\n$fullPrompt" }

                // Call GenAI provider
                val request = GenAiRequest(
                    prompt = fullPrompt,
                    context = null,
                    docs = null
                )

                genAiProvider.generate(request)
                    .thenAccept { response ->
                        // Log full LLM response for debugging truncation issues
                        logger.info("=== LLM Response Received ===")
                        logger.info("Model: ${response.model}")
                        logger.info("Error: ${response.error}")
                        logger.info("Response length: ${response.response.length} chars")
                        logger.info("=== FULL LLM RESPONSE START ===")
                        logger.info(response.response)
                        logger.info("=== FULL LLM RESPONSE END ===")

                        future.complete(TopicAnalysisResponseGraphQL(
                            response = response.response,
                            topicsAnalyzed = analyzedCount,
                            model = response.model,
                            error = response.error
                        ))
                    }
                    .exceptionally { error ->
                        logger.warning("Topic analysis request failed: ${error.message}")
                        future.complete(TopicAnalysisResponseGraphQL(
                            response = "",
                            topicsAnalyzed = analyzedCount,
                            model = genAiProvider.modelName,
                            error = "AI request failed: ${error.message}"
                        ))
                        null
                    }

            } catch (e: Exception) {
                logger.severe("Error processing topic analysis request: ${e.message}")
                future.complete(TopicAnalysisResponseGraphQL(
                    response = "",
                    topicsAnalyzed = 0,
                    model = genAiProvider?.modelName,
                    error = "Internal error: ${e.message}"
                ))
            }

            future
        }
    }

    /**
     * Format topic value for LLM context
     * Returns null if value should be excluded (binary, too large)
     */
    private fun formatTopicValue(msg: BrokerMessage, maxLength: Int): String? {
        val payload = msg.payload
        if (payload.isEmpty()) return ""

        // Skip large binaries (> 10KB)
        if (payload.size > 10 * 1024) return null

        // Try to convert to string
        val stringValue = try {
            String(payload, Charsets.UTF_8)
        } catch (e: Exception) {
            return null  // Not valid UTF-8, skip
        }

        // Skip if looks like binary (has control chars other than newline/tab)
        if (stringValue.any { it.code < 32 && it != '\n' && it != '\r' && it != '\t' }) {
            return null
        }

        // Truncate if too long
        return if (stringValue.length > maxLength) {
            stringValue.take(maxLength) + "...[truncated]"
        } else {
            stringValue
        }
    }

    /**
     * Format topics as a hierarchy tree to reduce token usage.
     * Uses ASCII tree characters (├── └── │) for clear visual hierarchy.
     *
     * Instead of repeating full paths like:
     *   ProveIt/Enterprise B/Site1/node/assetid: 1
     *   ProveIt/Enterprise B/Site1/node/name: Site1
     *
     * Outputs a tree structure:
     *   ProveIt/
     *   └── Enterprise B/
     *       └── Site1/
     *           └── node/
     *               ├── assetid: 1
     *               └── name: Site1
     */
    private fun formatTopicsAsHierarchy(topicValues: Map<String, String>, output: StringBuilder) {
        if (topicValues.isEmpty()) return

        // Build a tree structure from topic paths
        data class TreeNode(
            val name: String,
            val children: MutableMap<String, TreeNode> = mutableMapOf(),
            var value: String? = null
        )

        val root = TreeNode("")

        // Insert all topics into the tree
        for ((topic, value) in topicValues) {
            val parts = topic.split("/")
            var current = root

            for ((index, part) in parts.withIndex()) {
                if (part.isEmpty()) continue

                val isLeaf = index == parts.lastIndex
                val child = current.children.getOrPut(part) { TreeNode(part) }

                if (isLeaf) {
                    child.value = value
                }
                current = child
            }
        }

        // Render the tree with ASCII tree characters
        fun renderNode(node: TreeNode, prefix: String, isLast: Boolean, isRoot: Boolean) {
            // Sort children: directories first (those with children), then leaves
            val sortedChildren = node.children.values.sortedWith(
                compareBy({ it.children.isEmpty() && it.value != null }, { it.name })
            )

            for ((index, child) in sortedChildren.withIndex()) {
                val isLastChild = index == sortedChildren.lastIndex
                val hasChildren = child.children.isNotEmpty()
                val hasValue = child.value != null

                // Choose the connector based on position
                val connector = if (isLastChild) "└── " else "├── "
                // Choose continuation line for children
                val childPrefix = prefix + if (isLastChild) "    " else "│   "

                if (hasChildren && !hasValue) {
                    // Directory node - show with trailing slash
                    output.appendLine("$prefix$connector${child.name}/")
                    renderNode(child, childPrefix, isLastChild, false)
                } else if (hasValue && !hasChildren) {
                    // Leaf node - show name = value
                    output.appendLine("$prefix$connector${child.name} = ${child.value}")
                } else if (hasValue && hasChildren) {
                    // Both value and children - show value then children
                    output.appendLine("$prefix$connector${child.name} = ${child.value}")
                    renderNode(child, childPrefix, isLastChild, false)
                }
            }
        }

        renderNode(root, "", true, true)
    }
}

/**
 * Data class for chat history messages
 */
data class ChatMessageInput(
    val role: String,
    val content: String
)

/**
 * GraphQL response type for GenAI
 * Maps to the GenAiResponse type in the schema
 */
data class GenAiResponseGraphQL(
    val response: String,
    val model: String?,
    val error: String?
)

/**
 * GraphQL response type for Topic Analysis
 * Maps to the TopicAnalysisResponse type in the schema
 */
data class TopicAnalysisResponseGraphQL(
    val response: String,
    val topicsAnalyzed: Int,
    val model: String?,
    val error: String?
)
