package at.rocworks.extensions

import at.rocworks.Const
import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.*
import at.rocworks.handlers.SessionHandler
import io.vertx.core.*
import io.vertx.core.http.HttpClient
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.json.JsonObject
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

/**
 * API Service for handling JSON-RPC 2.0 requests over MQTT topics.
 *
 * Topic structure:
 * - Request: $API/<node-id>/<request-id>/request
 * - Response: $API/<node-id>/<request-id>/response
 *
 * Routes requests to the local GraphQL endpoint via HTTP.
 */
class ApiService(
    private val sessionHandler: SessionHandler,
    private val graphQLPort: Int = 4000,
    private val graphQLPath: String = "/graphql"
) : AbstractVerticle() {
    private val logger: Logger = Utils.getLogger(this::class.java)
    private val nodeId = Monster.getClusterNodeId(vertx)
    private val apiTopicPrefix = "\$API/$nodeId"

    // Track pending requests for timeout handling
    private val pendingRequests = ConcurrentHashMap<String, PendingRequest>()

    // Configuration
    private val requestTimeoutMs = 30000L // 30 seconds
    private val maxConcurrentRequests = 100

    private lateinit var httpClient: HttpClient

    data class PendingRequest(
        val requestId: String,
        val timerId: Long,
        val createdAt: Long = System.currentTimeMillis()
    )

    override fun start(startPromise: Promise<Void>) {
        try {
            logger.info("Starting API Service on node $nodeId with topic prefix: $apiTopicPrefix")

            // Create HTTP client for GraphQL communication
            val options = HttpClientOptions()
                .setDefaultHost("localhost")
                .setDefaultPort(graphQLPort)
                .setConnectTimeout(5000)

            httpClient = vertx.createHttpClient(options)

            // Subscribe internally to API request topics
            registerApiTopicHandler()

            // Periodic cleanup of timed out requests
            vertx.setPeriodic(5000) {
                cleanupTimedOutRequests()
            }

            logger.info("API Service started successfully")
            startPromise.complete()
        } catch (e: Exception) {
            logger.severe("Failed to start API Service: ${e.message}")
            e.printStackTrace()
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        try {
            // Cancel all pending request timers
            pendingRequests.values.forEach { req ->
                vertx.cancelTimer(req.timerId)
            }
            pendingRequests.clear()

            // Close HTTP client
            if (::httpClient.isInitialized) {
                httpClient.close()
            }

            logger.info("API Service stopped")
            stopPromise.complete()
        } catch (e: Exception) {
            stopPromise.fail(e)
        }
    }

    /**
     * Register handler for API topic messages via the event bus
     */
    private fun registerApiTopicHandler() {
        // Subscribe to messages via SessionHandler's internal message distribution
        // This uses the event bus address for node messages
        val nodeMessageAddress = "node.${nodeId}.messages"

        vertx.eventBus().consumer<BrokerMessage>(nodeMessageAddress) { message ->
            val brokerMessage = message.body()
            if (shouldHandleMessage(brokerMessage.topicName)) {
                handleApiRequest(brokerMessage)
            }
        }

        logger.info("Registered API topic handler for: ${'$'}API/$nodeId/+/request")
    }

    /**
     * Check if this message should be handled as an API request
     */
    private fun shouldHandleMessage(topic: String): Boolean {
        return topic.startsWith("$apiTopicPrefix/") && topic.endsWith("/request")
    }

    /**
     * Extract request ID from topic path
     * Topic format: $API/<node-id>/<request-id>/request
     */
    private fun extractRequestId(topic: String): String? {
        val pattern = """^\${'$'}API/[^/]+/([^/]+)/request$""".toRegex()
        return pattern.find(topic)?.groupValues?.get(1)
    }

    /**
     * Handle incoming API request
     */
    private fun handleApiRequest(message: BrokerMessage) {
        val requestId = extractRequestId(message.topicName) ?: run {
            logger.warning("Invalid API topic format: ${message.topicName}")
            return
        }

        // Check concurrent request limit
        if (pendingRequests.size >= maxConcurrentRequests) {
            sendErrorResponse(requestId, -32603, "Server busy: max concurrent requests reached")
            return
        }

        // Parse JSON-RPC request
        val payload = message.payload.toString(Charsets.UTF_8)
        val requestJson = try {
            JsonObject(payload)
        } catch (e: Exception) {
            logger.warning("Invalid JSON in API request: ${e.message}")
            sendErrorResponse(requestId, -32700, "Parse error")
            return
        }

        val jsonRpcRequest = JsonRpcRequest.fromJsonObject(requestJson) ?: run {
            logger.warning("Invalid JSON-RPC request format")
            sendErrorResponse(requestId, -32600, "Invalid Request")
            return
        }

        // Create timeout timer
        val timerId = vertx.setTimer(requestTimeoutMs) {
            logger.warning("Request $requestId timed out after ${requestTimeoutMs}ms")
            sendErrorResponse(requestId, -32603, "Request timeout")
            pendingRequests.remove(jsonRpcRequest.id)
        }

        pendingRequests[jsonRpcRequest.id] = PendingRequest(jsonRpcRequest.id, timerId)

        // Process the request asynchronously
        processJsonRpcRequest(jsonRpcRequest, requestId)
    }

    /**
     * Process JSON-RPC 2.0 request by forwarding to GraphQL HTTP endpoint
     */
    private fun processJsonRpcRequest(jsonRpcRequest: JsonRpcRequest, requestId: String) {
        try {
            // Build GraphQL query from JSON-RPC method and params
            val (query, variables) = buildGraphQLQuery(jsonRpcRequest.method, jsonRpcRequest.params ?: emptyMap())

            if (query.isEmpty()) {
                sendErrorResponse(requestId, -32601, "Method not found: ${jsonRpcRequest.method}")
                pendingRequests.remove(jsonRpcRequest.id)?.let { vertx.cancelTimer(it.timerId) }
                return
            }

            // Prepare GraphQL request
            val graphQLRequest = JsonObject()
                .put("query", query)
                .put("variables", JsonObject(variables))

            // Send HTTP POST request to GraphQL endpoint using Vert.X 5 API
            httpClient.request(io.vertx.core.http.HttpMethod.POST, graphQLPath)
                .onSuccess { httpRequest ->
                    httpRequest.putHeader("Content-Type", "application/json")
                        .response()
                        .onSuccess { response ->
                            response.body()
                                .onSuccess { buffer ->
                                    try {
                                        val graphQLResponse = JsonObject(buffer)

                                        // Extract data or errors from GraphQL response
                                        val result = if (graphQLResponse.containsKey("data")) {
                                            graphQLResponse.getValue("data")
                                        } else {
                                            null
                                        }

                                        if (graphQLResponse.containsKey("errors")) {
                                            val errorsArray = graphQLResponse.getJsonArray("errors")
                                            if (errorsArray.size() > 0) {
                                                val error = errorsArray.getJsonObject(0)
                                                sendErrorResponse(requestId, -32603, "GraphQL error: ${error.getString("message")}")
                                            } else {
                                                sendSuccessResponse(requestId, result, jsonRpcRequest.id)
                                            }
                                        } else {
                                            sendSuccessResponse(requestId, result, jsonRpcRequest.id)
                                        }
                                    } catch (e: Exception) {
                                        logger.warning("Failed to parse GraphQL response: ${e.message}")
                                        sendErrorResponse(requestId, -32603, "Failed to parse GraphQL response")
                                    }
                                    pendingRequests.remove(jsonRpcRequest.id)?.let { vertx.cancelTimer(it.timerId) }
                                }
                                .onFailure { error ->
                                    logger.warning("Failed to read GraphQL response body: ${error.message}")
                                    sendErrorResponse(requestId, -32603, "Failed to read GraphQL response")
                                    pendingRequests.remove(jsonRpcRequest.id)?.let { vertx.cancelTimer(it.timerId) }
                                }
                        }
                        .onFailure { error ->
                            logger.warning("GraphQL HTTP response failed: ${error.message}")
                            sendErrorResponse(requestId, -32603, "GraphQL HTTP response failed: ${error.message}")
                            pendingRequests.remove(jsonRpcRequest.id)?.let { vertx.cancelTimer(it.timerId) }
                        }
                    httpRequest.end(graphQLRequest.encode())
                }
                .onFailure { error ->
                    logger.warning("GraphQL HTTP request failed: ${error.message}")
                    sendErrorResponse(requestId, -32603, "GraphQL HTTP request failed: ${error.message}")
                    pendingRequests.remove(jsonRpcRequest.id)?.let { vertx.cancelTimer(it.timerId) }
                }

        } catch (e: Exception) {
            logger.warning("Error processing JSON-RPC request: ${e.message}")
            sendErrorResponse(requestId, -32603, "Internal error: ${e.message}")
            pendingRequests.remove(jsonRpcRequest.id)?.let { vertx.cancelTimer(it.timerId) }
        }
    }

    /**
     * Build GraphQL query from JSON-RPC method and params
     */
    private fun buildGraphQLQuery(method: String, params: Map<String, Any>): Pair<String, Map<String, Any>> {
        return when {
            method.startsWith("query.") -> {
                // Query method: method.fieldName -> query { fieldName(...params) { ... } }
                val fieldName = method.substring(6)
                val graphqlQuery = buildQueryString(fieldName, params)
                graphqlQuery to params
            }
            method.startsWith("mutation.") -> {
                // Mutation method: method.fieldName -> mutation { fieldName(...params) { ... } }
                val fieldName = method.substring(9)
                val graphqlMutation = buildMutationString(fieldName, params)
                graphqlMutation to params
            }
            else -> {
                // Default to query if not specified
                val graphqlQuery = buildQueryString(method, params)
                graphqlQuery to params
            }
        }
    }

    /**
     * Build a GraphQL query string from field name and params
     */
    private fun buildQueryString(fieldName: String, params: Map<String, Any>): String {
        val argsStr = params.entries.joinToString(",") { (key, value) ->
            "\$$key: String"
        }

        val paramStr = params.keys.joinToString(",") { key ->
            "$key: \$$key"
        }

        return if (paramStr.isNotEmpty()) {
            """
            query($argsStr) {
                $fieldName($paramStr)
            }
            """.trimIndent()
        } else {
            """
            query {
                $fieldName
            }
            """.trimIndent()
        }
    }

    /**
     * Build a GraphQL mutation string from field name and params
     */
    private fun buildMutationString(fieldName: String, params: Map<String, Any>): String {
        val argsStr = params.entries.joinToString(",") { (key, value) ->
            "\$$key: String"
        }

        val paramStr = params.keys.joinToString(",") { key ->
            "$key: \$$key"
        }

        return if (paramStr.isNotEmpty()) {
            """
            mutation($argsStr) {
                $fieldName($paramStr)
            }
            """.trimIndent()
        } else {
            """
            mutation {
                $fieldName
            }
            """.trimIndent()
        }
    }

    /**
     * Send successful JSON-RPC response
     */
    private fun sendSuccessResponse(requestId: String, data: Any?, jsonRpcId: String) {
        try {
            val response = JsonRpcResponse.success(jsonRpcId, data)
            publishResponse(requestId, response.toJsonObject())
        } catch (e: Exception) {
            logger.warning("Failed to send success response: ${e.message}")
        }
    }

    /**
     * Send error JSON-RPC response
     */
    private fun sendErrorResponse(requestId: String, code: Int, message: String) {
        try {
            val response = JsonRpcResponse.error(requestId, code, message)
            publishResponse(requestId, response.toJsonObject())
        } catch (e: Exception) {
            logger.warning("Failed to send error response: ${e.message}")
        }
    }

    /**
     * Publish JSON-RPC response to response topic
     */
    private fun publishResponse(requestId: String, response: JsonObject) {
        try {
            val responseTopic = "$apiTopicPrefix/$requestId/response"
            val responseMessage = BrokerMessage(
                messageUuid = java.util.UUID.randomUUID().toString(),
                messageId = 0,
                topicName = responseTopic,
                payload = response.encode().toByteArray(),
                qosLevel = 0,
                isRetain = false,
                isDup = false,
                isQueued = false,
                clientId = "API_SERVICE"
            )

            // Publish using SessionHandler's internal publish
            sessionHandler.publishInternal("API_SERVICE", responseMessage)

            logger.fine("Published API response to $responseTopic")
        } catch (e: Exception) {
            logger.warning("Failed to publish response: ${e.message}")
        }
    }

    /**
     * Clean up timed out requests
     */
    private fun cleanupTimedOutRequests() {
        val now = System.currentTimeMillis()
        val timedOut = pendingRequests.filter { (_, req) ->
            now - req.createdAt > requestTimeoutMs
        }

        timedOut.forEach { (id, req) ->
            pendingRequests.remove(id)
            vertx.cancelTimer(req.timerId)
        }

        if (timedOut.isNotEmpty()) {
            logger.info("Cleaned up ${timedOut.size} timed out requests")
        }
    }
}
