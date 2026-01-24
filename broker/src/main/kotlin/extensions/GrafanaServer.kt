package at.rocworks.extensions

import at.rocworks.Utils
import at.rocworks.auth.UserManager
import at.rocworks.extensions.graphql.JwtService
import at.rocworks.handlers.ArchiveGroup
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.stores.IMessageArchiveExtended

import io.vertx.core.*
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.handler.CorsHandler
import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.Base64

/**
 * Grafana JSON Data Source API Server
 *
 * Implements the Grafana JSON Data Source plugin API to expose MonsterMQ's
 * time-series data for visualization in Grafana dashboards.
 *
 * Endpoints:
 * - GET  /api/grafana/                      Health check
 * - POST /api/grafana/metrics               List available metrics (topics) with payload options
 * - POST /api/grafana/metric-payload-options  Dynamic payload options
 * - POST /api/grafana/search                List available metrics (legacy)
 * - POST /api/grafana/query                 Query time-series data
 *
 * @see https://grafana.com/grafana/plugins/simpod-json-datasource/
 * @see https://github.com/simPod/GrafanaJsonDatasource
 */
class GrafanaServer(
    private val host: String,
    private val port: Int,
    private val archiveHandler: ArchiveHandler,
    private val userManager: UserManager,
    private val defaultArchiveGroup: String = "Default"
) : AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    companion object {
        private const val API_PATH = "/api/grafana"
    }

    override fun start(startPromise: Promise<Void>) {
        logger.fine("Starting Grafana API server")

        val router = Router.router(vertx)

        // Enable CORS for Grafana
        router.route().handler(CorsHandler.create()
            .addOrigin("*")
            .allowedMethod(io.vertx.core.http.HttpMethod.GET)
            .allowedMethod(io.vertx.core.http.HttpMethod.POST)
            .allowedMethod(io.vertx.core.http.HttpMethod.OPTIONS)
            .allowedHeader("Content-Type")
            .allowedHeader("Authorization")
            .allowedHeader("Accept")
            .allowedHeader("X-Archive-Group"))  // Custom header for archive group selection

        router.route().handler(BodyHandler.create())

        // Authentication handler for all Grafana API routes
        // Use "$API_PATH*" to match both /api/grafana and /api/grafana/*
        router.route("$API_PATH*").handler { ctx: RoutingContext ->
            if (!validateAuthentication(ctx)) {
                return@handler
            }
            ctx.next()
        }

        // Health check endpoint - GET /api/grafana/ or /api/grafana
        router.get(API_PATH).handler { ctx -> handleHealthCheck(ctx) }
        router.get("$API_PATH/").handler { ctx -> handleHealthCheck(ctx) }

        // Search endpoint - POST /api/grafana/search (legacy)
        router.post("$API_PATH/search").handler { ctx -> handleSearch(ctx) }

        // Metrics endpoint - POST /api/grafana/metrics
        router.post("$API_PATH/metrics").handler { ctx -> handleMetrics(ctx) }

        // Metric payload options endpoint - POST /api/grafana/metric-payload-options
        router.post("$API_PATH/metric-payload-options").handler { ctx -> handleMetricPayloadOptions(ctx) }

        // Query endpoint - POST /api/grafana/query
        router.post("$API_PATH/query").handler { ctx -> handleQuery(ctx) }

        // HTTP server setup
        val options = HttpServerOptions()
            .setPort(port)
            .setHost(host)

        vertx.createHttpServer(options)
            .requestHandler(router)
            .listen()
            .onSuccess { server ->
                logger.info("Grafana API Server started on port ${server.actualPort()} with path $API_PATH")
                startPromise.complete()
            }
            .onFailure { error ->
                logger.severe("Grafana API Server failed to start: $error")
                startPromise.fail(error)
            }
    }

    /**
     * Get archive group from X-Archive-Group header, or use default if not set
     */
    private fun getArchiveGroupFromHeader(ctx: RoutingContext): String {
        return ctx.request().getHeader("X-Archive-Group") ?: defaultArchiveGroup
    }

    /**
     * Health check endpoint
     * Returns 200 OK if the server is running
     */
    private fun handleHealthCheck(ctx: RoutingContext) {
        val archiveGroup = getArchiveGroupFromHeader(ctx)
        logger.fine("Grafana health check request for archive group: $archiveGroup")
        ctx.response()
            .setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(JsonObject()
                .put("status", "ok")
                .put("message", "MonsterMQ Grafana API")
                .put("archiveGroup", archiveGroup)
                .encode())
    }

    /**
     * Search endpoint - returns list of available topics
     *
     * Request: { "target": "optional filter" }
     * Response: ["topic1", "topic2", ...]
     */
    private fun handleSearch(ctx: RoutingContext) {
        val archiveGroupName = getArchiveGroupFromHeader(ctx)
        logger.fine("Grafana search request for archive group: $archiveGroupName")

        try {
            val body = ctx.body().asJsonObject() ?: JsonObject()
            logger.fine { "Grafana search body: ${body.encode()}" }
            val targetFilter = body.getString("target", "")

            // Get the specified archive group
            val archiveGroups = archiveHandler.getDeployedArchiveGroups()
            val group = archiveGroups[archiveGroupName]
            val topics = mutableSetOf<String>()

            if (group != null) {
                val lastValStore = group.lastValStore
                if (lastValStore != null) {
                    lastValStore.findMatchingMessages("#") { message ->
                        val topicName = message.topicName
                        if (targetFilter.isEmpty() || topicName.contains(targetFilter, ignoreCase = true)) {
                            topics.add(topicName)
                        }
                        topics.size < 1000
                    }
                } else {
                    logger.fine { "Grafana search: archive group '$archiveGroupName' has no lastValStore" }
                }
            } else {
                logger.warning("Grafana search: archive group '$archiveGroupName' not found")
            }

            val result = JsonArray(topics.sorted().toList())

            ctx.response()
                .setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .end(result.encode())

        } catch (e: Exception) {
            logger.warning("Error in Grafana search: ${e.message}")
            ctx.response()
                .setStatusCode(500)
                .putHeader("Content-Type", "application/json")
                .end(JsonObject().put("error", e.message).encode())
        }
    }

    /**
     * Metrics endpoint - returns list of available topics with payload configuration
     *
     * Request: { "metric": "current metric", "payload": {} }
     * Response: [
     *   {
     *     "label": "topic/path",
     *     "value": "topic/path",
     *     "payloads": [
     *       { "name": "field", "type": "input", "placeholder": "JSON field path" },
     *       { "name": "function", "type": "select", "options": [...] },
     *       { "name": "archiveGroup", "type": "select", "options": [...] }
     *     ]
     *   }
     * ]
     */
    private fun handleMetrics(ctx: RoutingContext) {
        val archiveGroupName = getArchiveGroupFromHeader(ctx)
        logger.fine("Grafana metrics request for archive group: $archiveGroupName")

        try {
            val body = ctx.body().asJsonObject() ?: JsonObject()
            logger.fine { "Grafana metrics body: ${body.encode()}" }

            // Build payload options (field, function, and interval)
            val payloads = JsonArray()
                .add(JsonObject()
                    .put("name", "field")
                    .put("label", "JSON Field")
                    .put("type", "input")
                    .put("placeholder", "e.g., Value or data.temperature")
                    .put("width", 40))
                .add(JsonObject()
                    .put("name", "function")
                    .put("label", "Aggregation")
                    .put("type", "select")
                    .put("options", JsonArray()
                        .add(JsonObject().put("label", "Average").put("value", "AVG"))
                        .add(JsonObject().put("label", "Minimum").put("value", "MIN"))
                        .add(JsonObject().put("label", "Maximum").put("value", "MAX"))
                        .add(JsonObject().put("label", "Sum").put("value", "SUM"))
                        .add(JsonObject().put("label", "Count").put("value", "COUNT"))))
                .add(JsonObject()
                    .put("name", "interval")
                    .put("label", "Interval")
                    .put("type", "select")
                    .put("options", JsonArray()
                        .add(JsonObject().put("label", "Auto").put("value", "auto"))
                        .add(JsonObject().put("label", "None").put("value", "none"))
                        .add(JsonObject().put("label", "1 Minute").put("value", "1"))
                        .add(JsonObject().put("label", "5 Minutes").put("value", "5"))
                        .add(JsonObject().put("label", "15 Minutes").put("value", "15"))
                        .add(JsonObject().put("label", "1 Hour").put("value", "60"))
                        .add(JsonObject().put("label", "1 Day").put("value", "1440"))))

            // Get topics from the archive group specified in the header
            val archiveGroups = archiveHandler.getDeployedArchiveGroups()
            val group = archiveGroups[archiveGroupName]
            val result = JsonArray()

            if (group != null) {
                val lastValStore = group.lastValStore
                if (lastValStore != null) {
                    logger.fine { "Grafana metrics: collecting topics from archive group '$archiveGroupName'" }
                    val topics = mutableSetOf<String>()
                    lastValStore.findMatchingMessages("#") { message ->
                        topics.add(message.topicName)
                        topics.size < 1000
                    }
                    // Return plain topic names (archive group is in header)
                    for (topic in topics.sorted()) {
                        result.add(JsonObject()
                            .put("label", topic)
                            .put("value", topic)
                            .put("payloads", payloads))
                    }
                    logger.fine { "Grafana metrics: returning ${topics.size} topics" }
                } else {
                    logger.warning("Grafana metrics: archive group '$archiveGroupName' has no lastValStore")
                }
            } else {
                logger.warning("Grafana metrics: archive group '$archiveGroupName' not found")
            }

            ctx.response()
                .setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .end(result.encode())

        } catch (e: Exception) {
            logger.warning("Error in Grafana metrics: ${e.message}")
            ctx.response()
                .setStatusCode(500)
                .putHeader("Content-Type", "application/json")
                .end(JsonObject().put("error", e.message).encode())
        }
    }

    /**
     * Metric payload options endpoint - returns dynamic options for payload fields
     *
     * This is called when a payload field has reloadMetric=true to get updated options
     * based on the current metric selection.
     */
    private fun handleMetricPayloadOptions(ctx: RoutingContext) {
        logger.fine("Grafana metric-payload-options request")

        try {
            val body = ctx.body().asJsonObject() ?: JsonObject()
            logger.fine { "Grafana metric-payload-options body: ${body.encode()}" }

            val name = body.getString("name", "")

            // archiveGroup is no longer a payload option - it's part of the metric URI
            val options = when (name) {
                "function" -> JsonArray()
                    .add(JsonObject().put("label", "Average").put("value", "AVG"))
                    .add(JsonObject().put("label", "Minimum").put("value", "MIN"))
                    .add(JsonObject().put("label", "Maximum").put("value", "MAX"))
                    .add(JsonObject().put("label", "Sum").put("value", "SUM"))
                    .add(JsonObject().put("label", "Count").put("value", "COUNT"))
                else -> JsonArray()
            }

            ctx.response()
                .setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .end(options.encode())

        } catch (e: Exception) {
            logger.warning("Error in Grafana metric-payload-options: ${e.message}")
            ctx.response()
                .setStatusCode(500)
                .putHeader("Content-Type", "application/json")
                .end(JsonObject().put("error", e.message).encode())
        }
    }

    /**
     * Query endpoint - returns time-series data
     *
     * Request:
     * {
     *   "range": { "from": "ISO-8601", "to": "ISO-8601" },
     *   "interval": "1m",
     *   "intervalMs": 60000,
     *   "targets": [
     *     { "target": "topic/path", "refId": "A", "payload": { "field": "value", "archiveGroup": "Default" } }
     *   ],
     *   "maxDataPoints": 500
     * }
     *
     * Response:
     * [
     *   {
     *     "target": "topic/path",
     *     "datapoints": [[value, timestamp_ms], ...]
     *   }
     * ]
     */
    private fun handleQuery(ctx: RoutingContext) {
        val archiveGroupName = getArchiveGroupFromHeader(ctx)
        logger.fine("Grafana query request for archive group: $archiveGroupName")

        try {
            val body = ctx.body().asJsonObject()
            logger.fine { "Grafana query body: ${body?.encodePrettily() ?: "null"}" }
            if (body == null) {
                ctx.response()
                    .setStatusCode(400)
                    .putHeader("Content-Type", "application/json")
                    .end(JsonObject().put("error", "Request body is required").encode())
                return
            }

            // Parse time range
            val range = body.getJsonObject("range")
            if (range == null) {
                ctx.response()
                    .setStatusCode(400)
                    .putHeader("Content-Type", "application/json")
                    .end(JsonObject().put("error", "Range is required").encode())
                return
            }

            val fromStr = range.getString("from")
            val toStr = range.getString("to")

            val startTime = try {
                Instant.parse(fromStr)
            } catch (e: DateTimeParseException) {
                ctx.response()
                    .setStatusCode(400)
                    .putHeader("Content-Type", "application/json")
                    .end(JsonObject().put("error", "Invalid from time format").encode())
                return
            }

            val endTime = try {
                Instant.parse(toStr)
            } catch (e: DateTimeParseException) {
                ctx.response()
                    .setStatusCode(400)
                    .putHeader("Content-Type", "application/json")
                    .end(JsonObject().put("error", "Invalid to time format").encode())
                return
            }

            // Parse interval
            val intervalMs = body.getLong("intervalMs", 60000L)
            val intervalMinutes = mapIntervalToMinutes(intervalMs)

            logger.fine { "Grafana query: from=$startTime, to=$endTime, intervalMs=$intervalMs (${intervalMinutes}min)" }

            // Parse targets
            val targets = body.getJsonArray("targets") ?: JsonArray()
            if (targets.isEmpty) {
                ctx.response()
                    .setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .end(JsonArray().encode())
                return
            }

            // Parse targets - archive group comes from header, topic is the metric name
            val grafanaTargets = mutableListOf<GrafanaTarget>()

            for (i in 0 until targets.size()) {
                val targetObj = targets.getJsonObject(i)
                val topic = targetObj.getString("target") ?: continue
                val refId = targetObj.getString("refId", "A")
                val payload = targetObj.getJsonObject("payload") ?: JsonObject()
                val field = payload.getString("field", "")
                val function = payload.getString("function", "AVG")
                val intervalStr = payload.getString("interval", "auto")

                // Determine interval: use payload interval if specified, otherwise use Grafana's intervalMs
                // Special value 0 means no aggregation (raw data)
                val targetIntervalMinutes = when {
                    intervalStr == "none" -> 0  // No aggregation, return raw data
                    intervalStr == "auto" || intervalStr.isEmpty() -> intervalMinutes  // Use auto-calculated from Grafana's intervalMs
                    else -> intervalStr.toIntOrNull() ?: intervalMinutes
                }

                logger.fine { "Grafana target: topic=$topic, field=$field, function=$function, interval=${targetIntervalMinutes}min" }

                // Skip empty topics
                if (topic.isEmpty()) {
                    logger.fine { "Grafana target: skipping empty topic" }
                    continue
                }

                grafanaTargets.add(GrafanaTarget(topic, refId, field, function, targetIntervalMinutes))
            }

            // Execute queries and build response
            val result = JsonArray()
            val archiveGroups = archiveHandler.getDeployedArchiveGroups()
            val archiveStore = archiveGroups[archiveGroupName]?.archiveStore as? IMessageArchiveExtended

            if (archiveStore == null) {
                logger.warning("No archive store found for archive group: $archiveGroupName")
                ctx.response()
                    .setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .end(result.encode())
                return
            }

            // Separate raw data targets (interval=0) from aggregated targets
            val rawTargets = grafanaTargets.filter { it.intervalMinutes == 0 }
            val aggregatedTargets = grafanaTargets.filter { it.intervalMinutes > 0 }

            // Handle raw data targets (no aggregation)
            for (target in rawTargets) {
                logger.fine { "Grafana query: raw data for topic=${target.topic}, field=${target.field}" }

                val historyData = archiveStore.getHistory(
                    topic = target.topic,
                    startTime = startTime,
                    endTime = endTime,
                    limit = 10000  // Reasonable limit for raw data
                )

                logger.fine { "Grafana query: got ${historyData.size()} raw records" }

                val datapoints = JsonArray()
                for (j in 0 until historyData.size()) {
                    val record = historyData.getJsonObject(j)
                    // timestamp is stored as epoch milliseconds (Long)
                    val timestampMs = record.getLong("timestamp")
                    // payload is in payload_json field, or payload_base64 for binary, or payload for some stores
                    val payloadStr = record.getString("payload_json")
                        ?: record.getString("payload")
                        ?: record.getString("payload_base64")?.let { base64 ->
                            try {
                                String(java.util.Base64.getDecoder().decode(base64))
                            } catch (e: Exception) {
                                null
                            }
                        }

                    if (timestampMs != null && payloadStr != null) {
                        try {
                            val numValue = if (target.field.isNotEmpty()) {
                                // Extract field from JSON payload
                                extractJsonField(payloadStr, target.field)
                            } else {
                                // Try to parse payload as number directly
                                // First try direct parsing (for plain numbers like "123.45")
                                payloadStr.toDoubleOrNull()
                                    // If that fails, try parsing as JSON number (JSONB may wrap numbers)
                                    ?: try {
                                        JsonObject("{\"v\":$payloadStr}").getDouble("v")
                                    } catch (e: Exception) {
                                        null
                                    }
                            }

                            if (numValue != null) {
                                datapoints.add(JsonArray().add(numValue).add(timestampMs))
                            } else {
                                logger.fine { "Grafana query: could not parse payload as number: $payloadStr" }
                            }
                        } catch (e: Exception) {
                            logger.fine { "Grafana query: error parsing record: ${e.message}" }
                        }
                    } else {
                        logger.fine { "Grafana query: missing timestamp or payload - timestamp=$timestampMs, payload=$payloadStr" }
                    }
                }

                logger.fine { "Grafana query: returning ${datapoints.size()} datapoints for raw target" }

                val targetName = if (target.field.isEmpty()) {
                    target.topic
                } else {
                    "${target.topic}.${target.field}"
                }

                result.add(JsonObject()
                    .put("target", targetName)
                    .put("datapoints", datapoints))
            }

            // Handle aggregated targets
            // Group targets by function AND interval (since aggregation is done per function and interval)
            val targetsByFunctionAndInterval = aggregatedTargets.groupBy { Pair(it.function, it.intervalMinutes) }

            for ((key, functionIntervalTargets) in targetsByFunctionAndInterval) {
                val (function, targetInterval) = key
                val topics = functionIntervalTargets.map { it.topic }
                val fields = functionIntervalTargets.mapNotNull { it.field.ifEmpty { null } }.distinct()

                logger.fine { "Grafana query: function=$function, interval=${targetInterval}min, topics=${topics.size}" }

                val aggregatedResult = archiveStore.getAggregatedHistory(
                    topics = topics,
                    startTime = startTime,
                    endTime = endTime,
                    intervalMinutes = targetInterval,
                    functions = listOf(function),
                    fields = fields
                )

                // Parse the aggregated result and build Grafana response
                val columns = aggregatedResult.getJsonArray("columns") ?: JsonArray()
                val rows = aggregatedResult.getJsonArray("rows") ?: JsonArray()

                // Build datapoints for each target
                for (target in functionIntervalTargets) {
                    val fieldSuffix = if (target.field.isEmpty()) "" else ".${target.field.replace(".", "_")}"
                    val colName = "${target.topic}${fieldSuffix}_${function.lowercase()}"
                    val colIndex = findColumnIndex(columns, colName)

                    if (colIndex < 0) {
                        logger.fine("Column not found: $colName")
                        continue
                    }

                    val datapoints = JsonArray()
                    for (rowIndex in 0 until rows.size()) {
                        val row = rows.getJsonArray(rowIndex)
                        val timestampStr = row.getString(0)
                        val value = row.getValue(colIndex)

                        if (value != null && timestampStr != null) {
                            try {
                                val timestampMs = Instant.parse(timestampStr).toEpochMilli()
                                val numValue = when (value) {
                                    is Number -> value.toDouble()
                                    else -> continue
                                }
                                // Grafana expects [value, timestamp_ms]
                                datapoints.add(JsonArray().add(numValue).add(timestampMs))
                            } catch (e: Exception) {
                                // Skip invalid data points
                            }
                        }
                    }

                    val targetName = if (target.field.isEmpty()) {
                        target.topic
                    } else {
                        "${target.topic}.${target.field}"
                    }

                    result.add(JsonObject()
                        .put("target", targetName)
                        .put("datapoints", datapoints))
                }
            }

            ctx.response()
                .setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .end(result.encode())

        } catch (e: Exception) {
            logger.severe("Error in Grafana query: ${e.message}")
            e.printStackTrace()
            ctx.response()
                .setStatusCode(500)
                .putHeader("Content-Type", "application/json")
                .end(JsonObject().put("error", e.message).encode())
        }
    }

    /**
     * Map Grafana interval (in milliseconds) to MonsterMQ aggregation interval (in minutes)
     */
    private fun mapIntervalToMinutes(intervalMs: Long): Int {
        val intervalMinutes = (intervalMs / 60000).toInt().coerceAtLeast(1)

        return when {
            intervalMinutes < 5 -> 1       // ONE_MINUTE
            intervalMinutes < 15 -> 5      // FIVE_MINUTES
            intervalMinutes < 60 -> 15     // FIFTEEN_MINUTES
            intervalMinutes < 1440 -> 60   // ONE_HOUR
            else -> 1440                   // ONE_DAY
        }
    }

    /**
     * Find column index by name
     */
    private fun findColumnIndex(columns: JsonArray, name: String): Int {
        for (i in 0 until columns.size()) {
            if (columns.getString(i) == name) {
                return i
            }
        }
        return -1
    }

    /**
     * Validate authentication for Grafana API requests.
     * Supports:
     * - No auth (if user management is disabled)
     * - Basic Auth (async)
     * - Bearer Token (JWT)
     *
     * For Basic Auth, this method suspends the request and calls ctx.next() on success.
     * Returns true only for immediate success (no-auth or JWT).
     * Returns false when request is handled (either error sent or async auth in progress).
     */
    private fun validateAuthentication(ctx: RoutingContext): Boolean {
        // If user management is disabled, allow all requests
        if (!userManager.isUserManagementEnabled()) {
            logger.fine("User management disabled, allowing Grafana API request")
            return true
        }

        val authHeader = ctx.request().getHeader("Authorization")

        if (authHeader == null) {
            logger.warning("Grafana API request rejected: No Authorization header")
            ctx.response()
                .setStatusCode(401)
                .putHeader("Content-Type", "application/json")
                .putHeader("WWW-Authenticate", "Basic realm=\"MonsterMQ Grafana API\"")
                .end(JsonObject().put("error", "Authentication required").encode())
            return false
        }

        // Try Bearer token (JWT) first
        if (authHeader.startsWith("Bearer ", ignoreCase = true)) {
            val token = authHeader.substring(7)
            val username = JwtService.extractUsername(token)
            if (username != null && !JwtService.isTokenExpired(token)) {
                logger.fine("Grafana API request authenticated via JWT for user: $username")
                return true
            }

            logger.warning("Grafana API request rejected: Invalid or expired JWT token")
            ctx.response()
                .setStatusCode(401)
                .putHeader("Content-Type", "application/json")
                .putHeader("WWW-Authenticate", "Bearer error=\"invalid_token\"")
                .end(JsonObject().put("error", "Invalid or expired token").encode())
            return false
        }

        // Try Basic Auth (async)
        if (authHeader.startsWith("Basic ", ignoreCase = true)) {
            try {
                val base64Credentials = authHeader.substring(6)
                val credentials = String(Base64.getDecoder().decode(base64Credentials))
                val parts = credentials.split(":", limit = 2)

                if (parts.size == 2) {
                    val username = parts[0]
                    val password = parts[1]

                    // Async authentication
                    userManager.authenticate(username, password).onComplete { authResult ->
                        if (authResult.succeeded() && authResult.result() != null) {
                            val user = authResult.result()!!
                            if (user.enabled) {
                                logger.fine("Grafana API request authenticated via Basic Auth for user: $username")
                                ctx.next()
                            } else {
                                logger.warning("Grafana API request rejected: User $username is disabled")
                                ctx.response()
                                    .setStatusCode(401)
                                    .putHeader("Content-Type", "application/json")
                                    .putHeader("WWW-Authenticate", "Basic realm=\"MonsterMQ Grafana API\"")
                                    .end(JsonObject().put("error", "User account is disabled").encode())
                            }
                        } else {
                            logger.warning("Grafana API request rejected: Invalid Basic Auth credentials")
                            ctx.response()
                                .setStatusCode(401)
                                .putHeader("Content-Type", "application/json")
                                .putHeader("WWW-Authenticate", "Basic realm=\"MonsterMQ Grafana API\"")
                                .end(JsonObject().put("error", "Invalid credentials").encode())
                        }
                    }
                    return false // Request handling continues asynchronously
                }
            } catch (e: Exception) {
                logger.warning("Grafana API Basic Auth parsing error: ${e.message}")
            }

            logger.warning("Grafana API request rejected: Invalid Basic Auth format")
            ctx.response()
                .setStatusCode(401)
                .putHeader("Content-Type", "application/json")
                .putHeader("WWW-Authenticate", "Basic realm=\"MonsterMQ Grafana API\"")
                .end(JsonObject().put("error", "Invalid credentials format").encode())
            return false
        }

        logger.warning("Grafana API request rejected: Unsupported authentication method")
        ctx.response()
            .setStatusCode(401)
            .putHeader("Content-Type", "application/json")
            .putHeader("WWW-Authenticate", "Basic realm=\"MonsterMQ Grafana API\"")
            .end(JsonObject().put("error", "Unsupported authentication method").encode())
        return false
    }

    /**
     * Extract a numeric value from a JSON string using a field path.
     * Supports nested fields like "data.temperature" or "sensor.value".
     */
    private fun extractJsonField(jsonStr: String, fieldPath: String): Double? {
        return try {
            val json = JsonObject(jsonStr)
            val parts = fieldPath.split(".")
            var current: Any? = json

            for (part in parts) {
                current = when (current) {
                    is JsonObject -> current.getValue(part)
                    else -> return null
                }
            }

            when (current) {
                is Number -> current.toDouble()
                is String -> current.toDoubleOrNull()
                else -> null
            }
        } catch (e: Exception) {
            null
        }
    }

    /**
     * Data class for Grafana query target
     */
    private data class GrafanaTarget(
        val topic: String,
        val refId: String,
        val field: String,
        val function: String,
        val intervalMinutes: Int
    )
}
