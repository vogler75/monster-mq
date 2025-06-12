package at.rocworks.extensions

import at.rocworks.Utils
import at.rocworks.stores.IMessageArchive
import at.rocworks.stores.IMessageStore
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant

class McpHandler(
    private val vertx: Vertx,
    private val retainedStore: IMessageStore,
    private val messageStore: IMessageStore,
    private val messageArchive: IMessageArchive
) {
    private val logger = Utils.getLogger(this::class.java)

    private val tools: MutableMap<String, AsyncTool> = HashMap<String, AsyncTool>()

    companion object {
        private const val JSONRPC_VERSION = "2.0"
        private const val PROTOCOL_VERSION = "2024-11-05"

        const val JSONRPC_METHOD_NOT_FOUND = -32601
        const val JSONRPC_INVALID_ARGUMENT = -32602
        const val JSONRPC_INTERNAL_ERROR = -32603
    }

    data class AsyncTool(
        val name: String,
        val description: String,
        val inputSchema: JsonObject,
        val handler: AsyncToolHandler
    )

    init {
        registerTools()
    }

    private fun createResponse(id: Any?, result: JsonObject?): JsonObject {
        return JsonObject()
            .put("jsonrpc", JSONRPC_VERSION)
            .put("id", id)
            .put("result", result)
    }

    private fun createErrorResponse(id: Any?, code: Int, message: String?): JsonObject {
        val error = JsonObject()
            .put("code", code)
            .put("message", message)

        return JsonObject()
            .put("jsonrpc", JSONRPC_VERSION)
            .put("id", id)
            .put("error", error)
    }

    fun interface AsyncToolHandler {
        fun handle(arguments: JsonObject): Future<JsonArray>
    }

    internal class McpException(val code: Int, message: String) : RuntimeException(message)

    fun registerTool(tool: AsyncTool) {
        tools.put(tool.name, tool)
    }

    fun handleRequest(request: JsonObject): Future<JsonObject> {
        val method = request.getString("method")
        val id = request.getValue("id")
        val params = request.getJsonObject("params", JsonObject())

        logger.info("Handling MCP request: method=$method, id=$id, params=$params")

        // Handle notifications (messages without id) - these should not return responses
        if (id == null) {
            return handleNotification(method, params)
        }

        try {
            val resultFuture = when (method) {
                "initialize" -> Future.succeededFuture(handleInitialize(params))
                "tools/list" -> Future.succeededFuture(handleListTools())
                "tools/call" -> handleCallToolAsync(params)
                "resources/list" -> Future.succeededFuture(handleListResources())
                "resources/read" -> handleReadResource(params)
                "prompts/list" -> Future.succeededFuture(handleListPrompts())
                "prompts/get" -> handleGetPrompt(params)
                "ping" -> Future.succeededFuture(JsonObject())
                else -> Future.failedFuture(
                    McpException(
                        JSONRPC_METHOD_NOT_FOUND,
                        "Method not found"
                    )
                )
            }

            return resultFuture.map { result: JsonObject ->
                createResponse(id, result)
            }.recover { error: Throwable? ->
                val errorResponse: JsonObject = if (error is McpException) {
                    createErrorResponse(id, error.code, error.message)
                } else {
                    createErrorResponse(id, JSONRPC_INTERNAL_ERROR, "Internal error: " + error!!.message)
                }
                Future.succeededFuture(errorResponse)
            }
        } catch (e: Exception) {
            val errorResponse = createErrorResponse(id, JSONRPC_INTERNAL_ERROR, "Internal error: " + e.message)
            return Future.succeededFuture(errorResponse)
        }
    }

    /**
     * Handles MCP notifications.
     */
    private fun handleNotification(method: String, params: JsonObject): Future<JsonObject> {
        when (method) {
            "notifications/initialized" ->
                logger.info("Client initialized notification received")
            "notifications/cancelled" ->
                logger.info("Request cancelled notification received")
            else ->
                logger.severe("Unknown notification: $method")
        }

        // Return null to indicate no response should be sent for notifications
        return Future.succeededFuture(null)
    }

    private fun handleInitialize(params: JsonObject?): JsonObject {
        val capabilities = JsonObject()
            .put("tools", JsonObject())
            .put("resources", JsonObject().put("subscribe", false).put("listChanged", false))
            .put("prompts", JsonObject().put("listChanged", false))

        val serverInfo = JsonObject()
            .put("name", "monstermq-mcp-server")
            .put("version", "1.0.0")

        return JsonObject()
            .put("protocolVersion", PROTOCOL_VERSION)
            .put("capabilities", capabilities)
            .put("serverInfo", serverInfo)
    }

    private fun handleListTools(): JsonObject {
        val toolsArray = JsonArray()
        for (tool in tools.values) {
            val toolInfo = JsonObject()
                .put("name", tool.name)
                .put("description", tool.description)
            toolInfo.put("inputSchema", tool.inputSchema)
            toolsArray.add(toolInfo)
        }

        return JsonObject().put("tools", toolsArray)
    }

    private fun handleListResources(): JsonObject {
        // Return a sample resource - server status information
        val resourcesArray = JsonArray()

        val serverStatusResource = JsonObject()
            .put("uri", "monster://server/status")
            .put("name", "Server Status")
            .put("description", "Current server status and information")
            .put("mimeType", "application/json")

        resourcesArray.add(serverStatusResource)

        return JsonObject().put("resources", resourcesArray)
    }

    private fun handleReadResource(params: JsonObject): Future<JsonObject> {
        val uri = params.getString("uri")
        if (uri == null) {
            return Future.failedFuture(McpException(JSONRPC_INVALID_ARGUMENT, "URI parameter required"))
        }

        return when (uri) {
            "monster://server/status" -> {
                val status = JsonObject()
                    .put("status", "running")
                    .put("tools_count", tools.size)

                // Return proper resource contents format
                val contents = JsonArray().add(
                    JsonObject()
                        .put("uri", uri)
                        .put("mimeType", "application/json")
                        .put("text", status.encodePrettily())
                )

                Future.succeededFuture(JsonObject().put("contents", contents))
            }
            else -> Future.failedFuture(McpException(JSONRPC_INVALID_ARGUMENT, "Resource not found: $uri"))
        }
    }

    private fun handleListPrompts(): JsonObject {
        // Prompts in an MCP server are named, reusable LLM interactions that define structured instructions,
        // input schemas, and optional resource or tool context for generating model completions.
        val promptsArray = JsonArray()
        val codeReviewPrompt = JsonObject()
            .put("name", "find-topics")
            .put("description", "Find topics matching with a plain text")
            .put("arguments", JsonArray().add(
                    JsonObject()
                        .put("name", "text")
                        .put("description", "test to search for topics")
                        .put("required", true)
                )
            )
        promptsArray.add(codeReviewPrompt)
        return JsonObject().put("prompts", promptsArray)
    }

    private fun handleGetPrompt(params: JsonObject): Future<JsonObject> {
        val name = params.getString("name")
        val arguments = params.getJsonObject("arguments", JsonObject())

        if (name == null) {
            return Future.failedFuture(McpException(JSONRPC_INVALID_ARGUMENT, "Name parameter required"))
        }

        return when (name) {
            "find-topics" -> {
                val text = arguments.getString("text", "")
                val prompt = String.format(
                    """
                    Please find the topics that match the following text:
                    ```
                    %s
                    ```          
                    """.trimIndent(), text
                )

                val messages = JsonArray().add(
                    JsonObject()
                        .put("role", "user")
                        .put(
                            "content", JsonObject()
                                .put("type", "text")
                                .put("text", prompt)
                        )
                )

                Future.succeededFuture(
                    JsonObject()
                        .put("description", "Find topic prompt")
                        .put("messages", messages)
                )
            }
            else -> Future.failedFuture(McpException(JSONRPC_INVALID_ARGUMENT, "Prompt not found: $name"))
        }
    }

    private fun handleCallToolAsync(params: JsonObject): Future<JsonObject> {
        val toolName = params.getString("name")
        val arguments = params.getJsonObject("arguments", JsonObject())


        val tool = tools[toolName]
        if (tool == null) {
            return Future.failedFuture(McpException(JSONRPC_INVALID_ARGUMENT, "Tool not found: $toolName"))
        }

        return tool.handler.handle(arguments)
            .map { contentArray: JsonArray ->
                JsonObject().put("content", contentArray)
            }
            .recover { error: Throwable ->
                // Return tool error within the result (not as MCP protocol error)
                val errorContent = JsonArray().add(
                    JsonObject()
                        .put("type", "text")
                        .put("text", "Error: " + error.message)
                )
                val errorResult = JsonObject()
                    .put("content", errorContent)
                    .put("isError", true)
                Future.succeededFuture(errorResult)
            }
    }

    // --------------------------------------------------------------------------------------------------------------
    // Tools
    // --------------------------------------------------------------------------------------------------------------

    private fun registerTools() {
        // Register tools
        registerTool(
            AsyncTool(
                "find-topics-by-name",
                """
                Finds topics by name with wildcard options. 
                Topics are separated by '/' forming a hierarchical structure. 
                Wildcards can be used in the name, for example `my/topic/*` to match all topics under `my/topic/`.
                You can also pass a namespace, so that the search is limited to that namespace. 
                """.trimIndent(),
                JsonObject()
                    .put("type", "object")
                    .put(
                        "properties", JsonObject()
                            .put(
                                "name", JsonObject()
                                    .put("type", "string")
                                    .put("description", "Name to search for topics")
                            )
                            .put("ignoreCase", JsonObject()
                                .put("type", "boolean")
                                .put("description", "Whether to ignore case when matching names")
                                .put("default", true)
                            )
                            .put("namespace", JsonObject()
                                .put("type", "string")
                                .put("description", "Optional namespace to limit the search to a specific topic prefix")
                            )
                    )
                    .put("required", JsonArray().add("name")),
                ::findTopicsByNameTool
            )
        )
        registerTool(
            AsyncTool(
                "find-topics-by-description",
                """
                Finds topics by description with regex patterns.
                Topics are separated by '/' forming a hierarchical structure.
                Regex pattern can be used, for example `'*postgres|database*'` to match all topics with that description.
                Be sure to add the wildcards in the given description argument.
                You can also pass a namespace, so that the search is limited to that namespace. 
                """.trimIndent(),
                JsonObject()
                    .put("type", "object")
                    .put(
                        "properties", JsonObject()
                            .put(
                                "description", JsonObject()
                                    .put("type", "string")
                                    .put("description", "Description to search for topics, use wildcards like '*' or regex patterns")
                            )
                            .put("ignoreCase", JsonObject()
                                .put("type", "boolean")
                                .put("description", "Whether to ignore case when matching descriptions")
                                .put("default", true)
                            )
                            .put("namespace", JsonObject()
                                .put("type", "string")
                                .put("description", "Optional namespace to limit the search to a specific topic prefix")
                            )
                    )
                    .put("required", JsonArray().add("description")),
                ::findTopicsByDescriptionTool
            )
        )
        registerTool(
            AsyncTool(
                "get-topic-value",
                "Get the current/last value of a topic",
                JsonObject()
                    .put("type", "object")
                    .put(
                        "properties", JsonObject()
                            .put(
                                "topic", JsonObject()
                                    .put("type", "string")
                                    .put("description", "Topic to get the value for")
                            )
                    )
                    .put("required", JsonArray().add("topic")),
                ::getTopicValueTool
            )
        )
        registerTool(
            AsyncTool(
                "query-message-archive",
                "Get the message archive for a specific topic within a time range",
                JsonObject()
                    .put("type", "object")
                    .put(
                        "properties", JsonObject()
                            .put("topic", JsonObject()
                                 .put("type", "string")
                                 .put("description", "Topic to get the message archive for")
                            )
                            .put("startTime", JsonObject()
                                .put("type", "string")
                                .put("description", "Start time for the archive in ISO 8601 format")
                            )
                            .put("endTime", JsonObject()
                                .put("type", "string")
                                .put("description", "End time for the archive in ISO 8601 format")
                            )
                            .put("limit", JsonObject()
                                .put("type", "integer")
                                .put("description", "Maximum number of messages to return")
                                .put("default", 1000)
                            )
                    )
                    .put("required", JsonArray().add("topic")),
                ::queryMessageArchive
            )
        )
    }

    // --------------------------------------------------------------------------------------------------------------

    private fun findTopicsByNameTool(args: JsonObject): Future<JsonArray> {
        logger.info("findTopicByNameTool called with args: $args")
        if (!args.containsKey("name")) {
            return Future.failedFuture(McpException(JSONRPC_INVALID_ARGUMENT, "Name parameter required"))
        }
        val ignoreCase = args.getBoolean("ignoreCase", true)
        val namespace = args.getString("namespace", "")
        val name = args.getString("name", "")
        val list = messageStore.findTopicsByName(name, ignoreCase, namespace)
        val result = JsonArray()
        result.add(JsonObject().put("type", "text").put("text", "Matching Topics:"))
        list.forEach {
            result.add(JsonObject()
                .put("type", "text")
                .put("text", it)
            )
        }
        return Future.succeededFuture(result)
    }

    // --------------------------------------------------------------------------------------------------------------

    private fun findTopicsByDescriptionTool(args: JsonObject): Future<JsonArray> {
        logger.info("findTopicByDescriptionTool called with args: $args")
        if (!args.containsKey("description")) {
            return Future.failedFuture(McpException(JSONRPC_INVALID_ARGUMENT, "Description parameter required"))
        }
        val ignoreCase = args.getBoolean("ignoreCase", true)
        val description = args.getString("description", "")
        val namespace = args.getString("namespace", "")
        val list = retainedStore.findTopicsByConfig("Description", description, ignoreCase, namespace)
        val result = JsonArray()
        result.add(JsonObject().put("type", "text").put("text", "Result for description: $description"))
        result.add(JsonObject().put("type", "text").put("text", "Topic;Config"))
        list.forEach {
            result.add(JsonObject()
                .put("type", "text")
                .put("text", it.topic + ";" + it.config)
            )
        }
        return Future.succeededFuture(result)
    }

    // --------------------------------------------------------------------------------------------------------------

    private fun getTopicValueTool(args: JsonObject): Future<JsonArray> {
        logger.info("getTopicValueTool called with args: $args")
        if (!args.containsKey("topic")) {
            return Future.failedFuture(McpException(JSONRPC_INVALID_ARGUMENT, "Topic parameter required"))
        }

        val topic = args.getString("topic", "")
        val message = messageStore[topic] ?: return Future.succeededFuture(JsonArray())

        val result = JsonArray().add(
            JsonObject()
                .put("type", "text")
                .put("text", message.payload.toString(Charsets.UTF_8))
        )
        return Future.succeededFuture(result)
    }

    // --------------------------------------------------------------------------------------------------------------

    private fun queryMessageArchive(args: JsonObject): Future<JsonArray> {
        logger.info("queryMessageArchive called with args: $args")
        if (!args.containsKey("topic")) {
            return Future.failedFuture(McpException(JSONRPC_INVALID_ARGUMENT, "Topic parameter required"))
        }
        val topic = args.getString("topic", "")
        val startTime = args.getString("startTime")?.let { Instant.parse(it) }
        val endTime = args.getString("endTime")?.let { Instant.parse(it) }
        val limit = args.getInteger("limit", 1000)

        val messages = messageArchive.getHistory(topic, startTime, endTime, limit)
        val result = JsonArray()
        result.add(JsonObject().put("type", "text").put("text", "Result for topic: $topic"))
        result.add(JsonObject().put("type", "text").put("text", "Time;Payload"))
        messages.forEach { message ->
            result.add(
                JsonObject()
                    .put("type", "text")
                    .put("text", "${message.time};${message.payload.toString(Charsets.UTF_8)}")
            )
        }
        return Future.succeededFuture(result)
    }
}