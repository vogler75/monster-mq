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
 * - Request: $API/<node-id>/<service>/<realm>/request/<request-id>
 * - Response: $API/<node-id>/<service>/<realm>/response/<request-id>
 * - Listen on: $API/<node-id>/<service>/+/request/+
 *
 * Where <realm> enables ACL-based access control (e.g., "tenant-a", "v1", "team-b")
 *
 * Example:
 * - Request: $API/node-1/graphql/tenant-a/request/req-123
 * - Response: $API/node-1/graphql/tenant-a/response/req-123
 *
 * Routes requests to the local GraphQL endpoint via HTTP.
 */
class ApiService(
    private val sessionHandler: SessionHandler,
    private val serviceName: String = "graphql",
    private val graphQLPort: Int = 4000,
    private val graphQLPath: String = "/graphql"
) : AbstractVerticle() {
    private val logger: Logger = Utils.getLogger(this::class.java)

    // Track pending requests for timeout handling
    private val pendingRequests = ConcurrentHashMap<String, PendingRequest>()

    // Configuration
    private val requestTimeoutMs = 30000L // 30 seconds
    private val maxConcurrentRequests = 100

    private lateinit var httpClient: HttpClient
    private lateinit var nodeId: String
    private lateinit var apiTopicPrefix: String

    data class PendingRequest(
        val requestId: String,
        val timerId: Long,
        val createdAt: Long = System.currentTimeMillis()
    )

    companion object {
        private val apiRequestPattern = """^\${'$'}API/([^/]+)/([^/]+)/([^/]+)/request/([^/]+)$""".toRegex()

        data class ApiRequestDetails(
            val targetNodeId: String,
            val service: String,
            val realm: String,
            val requestId: String
        )

        /**
         * Check if topic is an API request topic.
         * Quick prefix check before regex matching for performance.
         * This fast path avoids expensive regex matching for 99% of MQTT messages.
         */
        fun isApiRequestTopic(topic: String): Boolean {
            return topic.startsWith("\$API/")
        }

        /**
         * Extract API request details from topic.
         * Only call this after confirming isApiRequestTopic() returns true.
         *
         * Topic format: $API/<node>/<service>/<realm>/request/<request-id>
         */
        fun extractApiRequestDetails(topic: String): ApiRequestDetails? {
            val match = apiRequestPattern.find(topic) ?: return null
            val (nodeId, service, realm, requestId) = match.destructured
            return ApiRequestDetails(nodeId, service, realm, requestId)
        }
    }

    override fun start(startPromise: Promise<Void>) {
        try {
            // Initialize node-specific properties now that vertx is available
            nodeId = Monster.getClusterNodeId(vertx)
            apiTopicPrefix = "\$API/$nodeId/$serviceName"

            logger.fine("Starting API Service '$serviceName' on node $nodeId with topic prefix: $apiTopicPrefix")

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

            logger.fine("API Service stopped")
            stopPromise.complete()
        } catch (e: Exception) {
            stopPromise.fail(e)
        }
    }

    /**
     * Register handler for API topic messages via the event bus
     */
    private fun registerApiTopicHandler() {
        // Subscribe to messages routed by SessionHandler for API topics
        // Listen on node-specific address: api.service.requests.<node-id>
        val eventBusAddress = "api.service.requests.$nodeId"
        val consumer = vertx.eventBus().consumer<BrokerMessage>(eventBusAddress) { message ->
            val brokerMessage = message.body()
            logger.finer("ApiService received message on topic: ${brokerMessage.topicName}")
            if (shouldHandleMessage(brokerMessage.topicName)) {
                logger.finer("Processing API request from topic: ${brokerMessage.topicName}")
                handleApiRequest(brokerMessage)
            } else {
                logger.finer("Message does not match API request pattern: ${brokerMessage.topicName}")
            }
        }

        logger.fine("Registered API topic handler for: ${'$'}API/$nodeId/$serviceName/+/request/+ (listening on event bus: $eventBusAddress)")
    }

    /**
     * Check if this message should be handled as an API request
     */
    private fun shouldHandleMessage(topic: String): Boolean {
        // Topic format: $API/<node>/<service>/<realm>/request/<request-id>
        return topic.contains("$serviceName/") && topic.contains("/request/")
    }

    /**
     * Extract request ID from topic path
     * Topic format: $API/<node>/<service>/<realm>/request/<request-id>
     */
    private fun extractRequestId(topic: String): String? {
        val pattern = """^\${'$'}API/[^/]+/$serviceName/[^/]+/request/([^/]+)$""".toRegex()
        return pattern.find(topic)?.groupValues?.get(1)
    }

    /**
     * Extract the realm from topic path (e.g., "tenant-a", "v1", "team-b", etc.)
     * Topic format: $API/<node>/<service>/<realm>/request/<request-id>
     */
    private fun extractRealm(topic: String): String? {
        val pattern = """^\${'$'}API/[^/]+/$serviceName/([^/]+)/request/[^/]+$""".toRegex()
        return pattern.find(topic)?.groupValues?.get(1)
    }

    /**
     * Handle incoming API request
     */
    private fun handleApiRequest(message: BrokerMessage) {
        logger.finer("handleApiRequest called for topic: ${message.topicName}")

        val requestId = extractRequestId(message.topicName) ?: run {
            logger.warning("Invalid API topic format: ${message.topicName}")
            return
        }

        val realm = extractRealm(message.topicName) ?: run {
            logger.warning("Could not extract realm from topic: ${message.topicName}")
            return
        }

        logger.finer("Extracted requestId: $requestId, realm: $realm")

        // Check concurrent request limit
        if (pendingRequests.size >= maxConcurrentRequests) {
            logger.warning("Too many concurrent requests: ${pendingRequests.size}")
            sendErrorResponse(requestId, -32603, "Server busy: max concurrent requests reached", realm)
            return
        }

        // Parse JSON-RPC request
        val payload = message.payload.toString(Charsets.UTF_8)
        logger.finer("API Request payload: $payload")

        val requestJson = try {
            JsonObject(payload)
        } catch (e: Exception) {
            logger.warning("Invalid JSON in API request: ${e.message}")
            sendErrorResponse(requestId, -32700, "Parse error", realm)
            return
        }

        val jsonRpcRequest = JsonRpcRequest.fromJsonObject(requestJson) ?: run {
            logger.warning("Invalid JSON-RPC request format: $requestJson")
            sendErrorResponse(requestId, -32600, "Invalid Request", realm)
            return
        }

        logger.finer("Valid JSON-RPC request: method=${jsonRpcRequest.method}, id=${jsonRpcRequest.id}")

        // Create timeout timer
        val timerId = vertx.setTimer(requestTimeoutMs) {
            logger.warning("Request $requestId timed out after ${requestTimeoutMs}ms")
            sendErrorResponse(requestId, -32603, "Request timeout", realm)
            pendingRequests.remove(jsonRpcRequest.id)
        }

        pendingRequests[jsonRpcRequest.id] = PendingRequest(jsonRpcRequest.id, timerId)

        // Process the request asynchronously
        processJsonRpcRequest(jsonRpcRequest, requestId, realm)
    }

    /**
     * Process JSON-RPC 2.0 request by forwarding to GraphQL HTTP endpoint
     */
    private fun processJsonRpcRequest(jsonRpcRequest: JsonRpcRequest, requestId: String, realm: String) {
        try {
            // Build GraphQL query from JSON-RPC method and params
            val (query, variables) = buildGraphQLQuery(jsonRpcRequest.method, jsonRpcRequest.params ?: emptyMap())

            if (query.isEmpty()) {
                sendErrorResponse(requestId, -32601, "Method not found: ${jsonRpcRequest.method}", realm)
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
                                                sendErrorResponse(requestId, -32603, "GraphQL error: ${error.getString("message")}", realm)
                                            } else {
                                                sendSuccessResponse(requestId, result, jsonRpcRequest.id, realm)
                                            }
                                        } else {
                                            sendSuccessResponse(requestId, result, jsonRpcRequest.id, realm)
                                        }
                                    } catch (e: Exception) {
                                        logger.warning("Failed to parse GraphQL response: ${e.message}")
                                        sendErrorResponse(requestId, -32603, "Failed to parse GraphQL response", realm)
                                    }
                                    pendingRequests.remove(jsonRpcRequest.id)?.let { vertx.cancelTimer(it.timerId) }
                                }
                                .onFailure { error ->
                                    logger.warning("Failed to read GraphQL response body: ${error.message}")
                                    sendErrorResponse(requestId, -32603, "Failed to read GraphQL response", realm)
                                    pendingRequests.remove(jsonRpcRequest.id)?.let { vertx.cancelTimer(it.timerId) }
                                }
                        }
                        .onFailure { error ->
                            logger.warning("GraphQL HTTP response failed: ${error.message}")
                            sendErrorResponse(requestId, -32603, "GraphQL HTTP response failed: ${error.message}", realm)
                            pendingRequests.remove(jsonRpcRequest.id)?.let { vertx.cancelTimer(it.timerId) }
                        }
                    httpRequest.end(graphQLRequest.encode())
                }
                .onFailure { error ->
                    logger.warning("GraphQL HTTP request failed: ${error.message}")
                    sendErrorResponse(requestId, -32603, "GraphQL HTTP request failed: ${error.message}", realm)
                    pendingRequests.remove(jsonRpcRequest.id)?.let { vertx.cancelTimer(it.timerId) }
                }

        } catch (e: Exception) {
            logger.warning("Error processing JSON-RPC request: ${e.message}")
            sendErrorResponse(requestId, -32603, "Internal error: ${e.message}", realm)
            pendingRequests.remove(jsonRpcRequest.id)?.let { vertx.cancelTimer(it.timerId) }
        }
    }

    /**
     * The `method` field should contain the full GraphQL query or mutation string.
     * Return it as-is along with the params as variables.
     *
     * Examples:
     * - Query: "query { brokers { nodeId version uptime } }"
     * - Mutation: "mutation { publish(topic: \"test/topic\", payload: \"hello\") { success } }"
     * - With variables: "query($topicName: String) { currentValue(topic: $topicName) { payload } }"
     */
    private fun buildGraphQLQuery(method: String, params: Map<String, Any>): Pair<String, Map<String, Any>> {
        // Method field contains the full GraphQL query/mutation string
        return method to params
    }

    /**
     * Send successful JSON-RPC response
     */
    private fun sendSuccessResponse(requestId: String, data: Any?, jsonRpcId: String, realm: String) {
        try {
            logger.finer("Sending success response for requestId=$requestId, id=$jsonRpcId, realm=$realm")
            val response = JsonRpcResponse.success(jsonRpcId, data)
            publishResponse(requestId, response.toJsonObject(), realm)
        } catch (e: Exception) {
            logger.warning("Failed to send success response: ${e.message}")
            e.printStackTrace()
        }
    }

    /**
     * Send error JSON-RPC response
     */
    private fun sendErrorResponse(requestId: String, code: Int, message: String, realm: String) {
        try {
            logger.finer("Sending error response for requestId=$requestId, code=$code, message=$message, realm=$realm")
            val response = JsonRpcResponse.error(requestId, code, message)
            publishResponse(requestId, response.toJsonObject(), realm)
        } catch (e: Exception) {
            logger.warning("Failed to send error response: ${e.message}")
            e.printStackTrace()
        }
    }

    /**
     * Publish JSON-RPC response to response topic
     */
    private fun publishResponse(requestId: String, response: JsonObject, realm: String) {
        try {
            val responseTopic = "$apiTopicPrefix/$realm/response/$requestId"
            logger.finer("Publishing response to topic: $responseTopic, payload: ${response.encode()}")

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

            logger.finer("API response published to $responseTopic")
        } catch (e: Exception) {
            logger.warning("Failed to publish response: ${e.message}")
            e.printStackTrace()
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
            logger.fine("Cleaned up ${timedOut.size} timed out requests")
        }
    }
}
