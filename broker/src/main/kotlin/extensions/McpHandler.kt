package at.rocworks.extensions

import at.rocworks.Const
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
**Find Topics by Name**

Searches for topics (also called tags or datapoints) using name patterns with wildcard support. This tool helps locate specific data streams or topic hierarchies within a messaging or data system.

**MQTT Context:**
- Topics are MQTT topic strings that follow standard MQTT conventions
- Also known as "tags" or "datapoints" in industrial/IoT contexts
- Represents real-time data streams from sensors, devices, or applications
- Values are the payloads published to these MQTT topics

**Topic Structure:**
- Topics use a hierarchical naming convention with forward slashes as separators (e.g., `sensors/temperature/bedroom`)
- Each level represents a category or subcategory, creating a tree-like organization
- Topics function as unique identifiers for data streams, messages, or monitoring points

**Wildcard Patterns:**
- Use `*` to match any characters at that level
- `my/topic/*` - matches all direct children under `my/topic/` (e.g., `my/topic/sensor1`, `my/topic/data`)
- `sensors/*/temperature` - matches temperature topics across all sensor locations
- `*/status` - matches any status topic at the second level
- Case sensitivity is configurable (default: case-insensitive)

**Namespace Filtering:**
- Optional namespace parameter limits search scope to topics with a specific prefix
- Useful for filtering large topic hierarchies by system, device, or category
- Example: namespace `production/` only searches topics starting with `production/`

**Best Practices:**
- Start with broader patterns and narrow down if needed
- Use namespace filtering for large systems to improve performance
- Consider the hierarchical structure when designing search patterns
- Combine with other topic tools for comprehensive data exploration
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
**Find Topics by Description**

Searches for topics (also called tags or datapoints) by matching patterns against their description text using regex expressions. This tool helps discover relevant data streams based on their descriptive content rather than their hierarchical names.

**MQTT Context:**
- Topics are MQTT topic strings that follow standard MQTT conventions
- Also known as "tags" or "datapoints" in industrial/IoT contexts
- Represents real-time data streams from sensors, devices, or applications
- Values are the payloads published to these MQTT topics

**Topic Structure:**
- Topics use a hierarchical naming convention with forward slashes as separators (e.g., `sensors/temperature/bedroom`)
- Each topic has an associated description that explains its purpose or content
- This tool searches the description text, not the topic name itself

**Regex Pattern Matching:**
- Use standard regex patterns to match description content
- `.*postgres.*|.*database.*` - matches topics with descriptions containing "postgres" or "database"
- `temperature.*sensor` - matches descriptions starting with "temperature" and containing "sensor"
- `(cpu|memory).*usage` - matches descriptions about CPU or memory usage
- `error.*level.*[0-9]+` - matches error descriptions with numeric levels
- Case sensitivity is configurable (default: case-insensitive)

**Wildcard Usage:**
- Include wildcards (`.*`) in your regex patterns for flexible matching
- `.*keyword.*` - matches any description containing "keyword"
- Use `|` for OR conditions: `.*wifi.*|.*bluetooth.*`
- Use `^` and "\$" for exact start/end matching: "^System.*" matches descriptions starting with "System"

**Namespace Filtering:**
- Optional namespace parameter limits search to topics under a specific hierarchy
- Example: namespace `production/sensors/` only searches sensor topics in production
- Combines with regex patterns for precise filtering

**Best Practices:**
- Start with simple patterns like `.*keyword.*` and refine as needed
- Use namespace filtering to narrow scope before applying complex regex
- Test patterns incrementally - complex regex can be hard to debug
- Consider case sensitivity settings for your use case
- Combine with name-based searches for comprehensive topic discovery

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
                """
**Get Topic Value Tool**

Retrieves the current or most recent value stored for a specific MQTT topic. This tool provides real-time access to the latest data point or message published to an MQTT topic (also referred to as tags or datapoints in some systems).

**MQTT Context:**
- Topics are MQTT topic strings that follow standard MQTT conventions
- Also known as "tags" or "datapoints" in industrial/IoT contexts
- Represents real-time data streams from sensors, devices, or applications
- Values are the payloads published to these MQTT topics

**Functionality:**
- Returns the most recently published value for the specified MQTT topic
- Provides current state information for data streams, sensors, or message queues
- Shows the actual message payload content, not just metadata about the topic

**Input:**
- **topic** (required): The exact MQTT topic string (e.g., `sensors/temperature/bedroom`)
- Topic names are case-sensitive and must match exactly
- Use forward slashes to specify the complete topic path following MQTT standards

**Use Cases:**
- Check current sensor readings: `sensors/temperature/living_room`
- Get latest device status: `devices/thermostat/01/status`
- Retrieve configuration values: `config/database/connection_string`
- Monitor real-time metrics: `metrics/performance/response_time`
- Read IoT device data: `home/lights/kitchen/brightness`

**Return Value:**
- The actual MQTT message payload (could be number, string, JSON object, etc.)
- Timestamp information may be included depending on the MQTT broker configuration
- Returns null or error if topic doesn't exist or has no retained/published values

**Best Practices:**
- Use find-topics tools first to discover available MQTT topic names
- Ensure topic name follows MQTT naming conventions and hierarchy
- Consider that values represent point-in-time MQTT messages that may change rapidly
- For historical MQTT message analysis, use the message archive tool instead
- Remember that MQTT topics without retained messages may return no value

**Error Handling:**
- Verify MQTT topic exists and is accessible before attempting to get its value
- Handle cases where topics may not have any retained messages
- Consider MQTT broker connectivity and permissions
                    
                """.trimIndent(),
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
                """
**Query Message Archive Tool**

Retrieves historical MQTT messages for a specific topic within a specified time range. This tool enables analysis of message patterns, trends, and historical data from MQTT topics (also referred to as tags or datapoints).

**MQTT Context:**
- Queries archived MQTT messages from a specific topic
- Provides historical view of data streams, sensor readings, or device communications
- Useful for trend analysis, debugging, and historical reporting
- Messages are retrieved in chronological order within the specified time window

**Parameters:**

**topic** (required):
- The exact MQTT topic string to query (e.g., `sensors/temperature/bedroom`)
- Must match the topic name exactly (case-sensitive)
- Use forward slashes for hierarchical MQTT topic structure
- Single topic only - use multiple calls for multiple topics

**startTime** (optional):
- Start of the time range in ISO 8601 format
- Examples: `2024-01-15T10:30:00Z`, `2024-01-15T10:30:00.123Z`, `2024-01-15T10:30:00+02:00`
- If omitted, retrieves from the earliest available message
- Use UTC timezone (Z suffix) for consistent results across systems

**endTime** (optional):
- End of the time range in ISO 8601 format
- Same format as startTime: `2024-01-15T18:45:00Z`
- If omitted, retrieves up to the most recent message
- Must be after startTime if both are specified

**limit** (optional, default: 1000):
- Maximum number of messages to return
- Helps prevent overwhelming responses for high-frequency topics
- Messages are returned chronologically, so you get the oldest messages first
- Increase limit for comprehensive historical analysis, decrease for quick sampling

**Use Cases:**
- Analyze sensor data trends: `sensors/temperature/outdoor` over the last week
- Debug device behavior: `devices/thermostat/errors` during a specific incident
- Generate reports: `production/line1/output` for monthly reporting
- Monitor system performance: `system/cpu_usage` during peak hours
- Historical data export: retrieve all messages for backup or migration

**Return Value:**
- Array of historical MQTT messages with timestamps and payloads
- Each message includes the published timestamp and message content
- Messages are ordered chronologically (oldest first)
- May include message metadata depending on system configuration

**Best Practices:**
- Use specific time ranges to avoid retrieving excessive data
- Start with smaller time windows and adjust limit as needed
- Consider system performance when querying high-frequency topics
- Use ISO 8601 format with explicit timezone information
- Combine with topic discovery tools to identify relevant topics first
- For real-time data, use the get-topic-value tool instead

**Example Queries:**
- Last 24 hours: `startTime: "2024-01-15T00:00:00Z"`, `endTime: "2024-01-16T00:00:00Z"`
- Specific incident window: `startTime: "2024-01-15T14:30:00Z"`, `endTime: "2024-01-15T15:00:00Z"`
- Sample recent data: `limit: 100` (no time range for most recent 100 messages)                    
                """.trimIndent(),
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
                                .put("default", 100)
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

        result.add(JsonObject().put("type", "text").put("text", "Topic: Configuration & Description"))
        list.forEach {
            val config = retainedStore["$it/${Const.CONFIG_TOPIC_NAME}"] // TODO: should be optimized to do a fetch with the list of topics
            result.add(JsonObject()
                .put("type", "text")
                .put("text", "$it: ${config?.payload?.toString(Charsets.UTF_8) ?: "No config"}")
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
        result.add(JsonObject().put("type", "text").put("text", "Topic: Configuration & Description"))
        list.forEach {
            result.add(JsonObject()
                .put("type", "text")
                .put("text", "${it.topic}: ${it.config.ifEmpty { "No config" }}")
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
        result.add(JsonObject().put("type", "text").put("text", "Time:Payload"))
        messages.forEach { message ->
            result.add(
                JsonObject()
                    .put("type", "text")
                    .put("text", "${message.time}: ${message.payload.toString(Charsets.UTF_8)}")
            )
        }
        return Future.succeededFuture(result)
    }
}