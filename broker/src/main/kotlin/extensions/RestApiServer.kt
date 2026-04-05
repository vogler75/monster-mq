package at.rocworks.extensions

import at.rocworks.Const
import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.auth.UserManager
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.data.BulkClientMessage
import at.rocworks.extensions.graphql.JwtService
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.handlers.SessionHandler
import at.rocworks.stores.IMessageArchiveExtended
import at.rocworks.stores.IMessageStore

import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import java.time.Instant
import java.util.Base64
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

/**
 * REST API for MonsterMQ.
 *
 * Endpoints:
 * - POST /api/v1/topics/{topic}      Publish (raw body)
 * - PUT  /api/v1/topics/{topic}      Publish (inline via query param)
 * - POST /api/v1/write               Bulk publish
 * - GET  /api/v1/topics/{topic}      Read (retained / last value / history)
 * - GET  /api/v1/subscribe           SSE subscribe
 * - GET  /api/v1/openapi.yaml        OpenAPI specification
 * - GET  /api/v1/docs                Swagger UI (redirect)
 */
class RestApiServer(
    private val vertx: Vertx,
    private val sessionHandler: SessionHandler,
    private val archiveHandler: ArchiveHandler,
    private val retainedStore: IMessageStore?,
    private val userManager: UserManager
) {
    private val logger = Utils.getLogger(this::class.java)

    companion object {
        private const val API_PREFIX = "/api/v1"
        private const val REST_CLIENT_PREFIX = "rest-api-"
    }

    // Track active SSE connections for cleanup
    private val activeSseConnections = ConcurrentHashMap<String, Boolean>()

    /**
     * Register all REST API routes on the given router.
     * MUST be called BEFORE the catch-all static handler.
     */
    fun registerRoutes(router: Router) {
        logger.info("Registering REST API routes under $API_PREFIX")

        // Auth middleware for all API routes (skip login endpoint)
        router.route("$API_PREFIX/*").handler { ctx ->
            if (ctx.request().path() == "$API_PREFIX/login") {
                ctx.next()
                return@handler
            }
            authenticateRequest(ctx) { username ->
                ctx.put("username", username)
                ctx.next()
            }
        }

        // Login endpoint (no auth required)
        router.post("$API_PREFIX/login").handler { ctx -> handleLogin(ctx) }

        // OpenAPI spec
        router.get("$API_PREFIX/openapi.yaml").handler { ctx -> handleOpenApiSpec(ctx) }

        // Swagger UI redirect
        router.get("$API_PREFIX/docs").handler { ctx -> handleSwaggerUiRedirect(ctx) }

        // SSE Subscribe (before topics wildcard route)
        router.get("$API_PREFIX/subscribe").handler { ctx -> handleSubscribe(ctx) }

        // Bulk write
        router.post("$API_PREFIX/write").handler { ctx -> handleBulkWrite(ctx) }
        router.post("$API_PREFIX/write/influx").handler { ctx -> handleInfluxWrite(ctx) }

        // Topic routes (wildcard path)
        router.post("$API_PREFIX/topics/*").handler { ctx -> handlePublishRawBody(ctx) }
        router.put("$API_PREFIX/topics/*").handler { ctx -> handlePublishInline(ctx) }
        router.get("$API_PREFIX/topics/*").handler { ctx -> handleReadTopics(ctx) }

        logger.info("REST API routes registered")
    }

    // ========== Login ==========

    private fun handleLogin(ctx: RoutingContext) {
        if (!userManager.isUserManagementEnabled()) {
            ctx.response().setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .end(JsonObject()
                    .put("success", true)
                    .put("token", null as String?)
                    .put("message", "Authentication disabled")
                    .put("username", "anonymous")
                    .encode())
            return
        }

        val body = try { ctx.body().asJsonObject() } catch (e: Exception) { null }
        val username = body?.getString("username") ?: ""
        val password = body?.getString("password") ?: ""

        if (username.isEmpty() || password.isEmpty()) {
            ctx.response().setStatusCode(400)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("Username and password are required"))
            return
        }

        userManager.authenticate(username, password).onComplete { authResult ->
            try {
                if (authResult.succeeded()) {
                    val user = authResult.result()
                    if (user != null && user.enabled) {
                        val token = JwtService.generateToken(user.username, user.isAdmin)
                        ctx.response().setStatusCode(200)
                            .putHeader("Content-Type", "application/json")
                            .end(JsonObject()
                                .put("success", true)
                                .put("token", token)
                                .put("username", user.username)
                                .encode())
                    } else {
                        ctx.response().setStatusCode(401)
                            .putHeader("Content-Type", "application/json")
                            .end(errorJson("Invalid username or password"))
                    }
                } else {
                    ctx.response().setStatusCode(401)
                        .putHeader("Content-Type", "application/json")
                        .end(errorJson("Authentication failed"))
                }
            } catch (e: Exception) {
                logger.warning("REST API login error: \${e.message}")
                ctx.response().setStatusCode(500)
                    .putHeader("Content-Type", "application/json")
                    .end(errorJson("Internal server error"))
            }
        }
    }

    // ========== Authentication ==========

    private fun authenticateRequest(ctx: RoutingContext, onSuccess: (String) -> Unit) {
        // If user management is disabled, allow all requests
        if (!userManager.isUserManagementEnabled()) {
            onSuccess("anonymous")
            return
        }

        val authHeader = ctx.request().getHeader("Authorization")

        if (authHeader == null) {
            if (userManager.isAnonymousEnabled()) {
                onSuccess("anonymous")
                return
            }
            sendUnauthorized(ctx, "Authentication required")
            return
        }

        if (authHeader.startsWith("Bearer ", ignoreCase = true)) {
            val token = authHeader.substring(7)
            val username = JwtService.extractUsername(token)
            if (username != null && !JwtService.isTokenExpired(token)) {
                onSuccess(username)
                return
            }
            ctx.response().setStatusCode(401)
                .putHeader("Content-Type", "application/json")
                .putHeader("WWW-Authenticate", "Bearer error=\"invalid_token\"")
                .end(errorJson("Invalid or expired token"))
            return
        }

        if (authHeader.startsWith("Basic ", ignoreCase = true)) {
            try {
                val credentials = String(Base64.getDecoder().decode(authHeader.substring(6)))
                val parts = credentials.split(":", limit = 2)
                if (parts.size == 2) {
                    userManager.authenticate(parts[0], parts[1]).onComplete { authResult ->
                        if (authResult.succeeded() && authResult.result()?.enabled == true) {
                            onSuccess(parts[0])
                        } else {
                            sendUnauthorized(ctx, "Invalid credentials")
                        }
                    }
                    return
                }
            } catch (e: Exception) {
                logger.warning("REST API Basic Auth parsing error: ${e.message}")
            }
        }

        sendUnauthorized(ctx, "Authentication required")
    }

    private fun sendUnauthorized(ctx: RoutingContext, message: String) {
        ctx.response().setStatusCode(401)
            .putHeader("Content-Type", "application/json")
            .putHeader("WWW-Authenticate", "Basic realm=\"MonsterMQ REST API\"")
            .end(errorJson(message))
    }

    // ========== Topic Path Extraction ==========

    private fun extractTopicFromPath(ctx: RoutingContext): String? {
        // The path after /api/v1/topics/ is the MQTT topic
        val fullPath = ctx.request().path()
        val prefix = "$API_PREFIX/topics/"
        if (!fullPath.startsWith(prefix)) {
            return null
        }
        val topicPath = fullPath.substring(prefix.length)
        if (topicPath.isBlank()) {
            return null
        }
        // URL-decode the topic path (handles %2B → +, %23 → #, etc.)
        return java.net.URLDecoder.decode(topicPath, "UTF-8")
    }

    // ========== Publish Handlers ==========

    private fun handlePublishRawBody(ctx: RoutingContext) {
        val topic = extractTopicFromPath(ctx)
        if (topic == null) {
            ctx.response().setStatusCode(400)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("Topic path is required"))
            return
        }

        val username = ctx.get<String>("username") ?: "anonymous"

        // ACL check
        if (userManager.isUserManagementEnabled() && !userManager.canPublish(username, topic)) {
            ctx.response().setStatusCode(403)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("Publish not allowed on topic: $topic"))
            return
        }

        val qos = ctx.request().getParam("qos")?.toIntOrNull() ?: 0
        val retain = ctx.request().getParam("retain")?.toBoolean() ?: false
        val payload = ctx.body()?.buffer()?.bytes ?: ByteArray(0)

        val message = BrokerMessage(
            messageUuid = Utils.getUuid(),
            messageId = 0,
            topicName = topic,
            payload = payload,
            qosLevel = qos.coerceIn(0, 2),
            isRetain = retain,
            isDup = false,
            isQueued = false,
            clientId = "$REST_CLIENT_PREFIX$username"
        )

        sessionHandler.publishMessage(message)

        ctx.response().setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(JsonObject()
                .put("success", true)
                .put("topic", topic)
                .encode())
    }

    private fun handlePublishInline(ctx: RoutingContext) {
        val topic = extractTopicFromPath(ctx)
        if (topic == null) {
            ctx.response().setStatusCode(400)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("Topic path is required"))
            return
        }

        val payload = ctx.request().getParam("payload")
        if (payload == null) {
            ctx.response().setStatusCode(400)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("Query parameter 'payload' is required"))
            return
        }

        val username = ctx.get<String>("username") ?: "anonymous"

        // ACL check
        if (userManager.isUserManagementEnabled() && !userManager.canPublish(username, topic)) {
            ctx.response().setStatusCode(403)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("Publish not allowed on topic: $topic"))
            return
        }

        val qos = ctx.request().getParam("qos")?.toIntOrNull() ?: 0
        val retain = ctx.request().getParam("retain")?.toBoolean() ?: false

        val message = BrokerMessage(
            messageUuid = Utils.getUuid(),
            messageId = 0,
            topicName = topic,
            payload = payload.toByteArray(),
            qosLevel = qos.coerceIn(0, 2),
            isRetain = retain,
            isDup = false,
            isQueued = false,
            clientId = "$REST_CLIENT_PREFIX$username"
        )

        sessionHandler.publishMessage(message)

        ctx.response().setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(JsonObject()
                .put("success", true)
                .put("topic", topic)
                .encode())
    }

    // ========== Bulk Write Handler ==========

    private fun handleBulkWrite(ctx: RoutingContext) {
        val body = try {
            ctx.body()?.asJsonObject()
        } catch (e: Exception) {
            null
        }

        if (body == null) {
            ctx.response().setStatusCode(400)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("Request body must be a JSON object"))
            return
        }

        val messages = body.getJsonArray("messages")
        val records = body.getJsonArray("records")

        if ((messages == null || messages.isEmpty) && (records == null || records.isEmpty)) {
            ctx.response().setStatusCode(400)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("'messages' or 'records' array is required and must not be empty"))
            return
        }

        val username = ctx.get<String>("username") ?: "anonymous"
        var publishedCount = 0
        val errors = JsonArray()

        // Process object-based messages
        if (messages != null && !messages.isEmpty) {
            for (i in 0 until messages.size()) {
                val msg = messages.getJsonObject(i) ?: continue
                val topic = msg.getString("topic")
                val value = msg.getValue("value")?.toString()
    
                if (topic == null || value == null) {
                    errors.add(JsonObject().put("index", i).put("error", "Missing topic or value"))
                    continue
                }
    
                // ACL check per message
                if (userManager.isUserManagementEnabled() && !userManager.canPublish(username, topic)) {
                    errors.add(JsonObject().put("index", i).put("error", "Publish not allowed on topic: $topic"))
                    continue
                }
    
                val qos = msg.getInteger("qos", 0)
                val retain = msg.getBoolean("retain", false)
    
                val brokerMessage = BrokerMessage(
                    messageUuid = Utils.getUuid(),
                    messageId = 0,
                    topicName = topic,
                    payload = value.toByteArray(),
                    qosLevel = qos.coerceIn(0, 2),
                    isRetain = retain,
                    isDup = false,
                    isQueued = false,
                    clientId = "$REST_CLIENT_PREFIX$username"
                )
    
                sessionHandler.publishMessage(brokerMessage)
                publishedCount++
            }
        }

        // Process compressed array-based records
        if (records != null && !records.isEmpty) {
            for (i in 0 until records.size()) {
                val record = try { records.getJsonArray(i) } catch(e: Exception) { null }
                if (record == null) {
                    errors.add(JsonObject().put("index", i).put("error", "Record must be an array"))
                    continue
                }

                val topic = try { if (record.size() > 0) record.getString(0) else null } catch(e: Exception) { null }
                val value = try { if (record.size() > 1) record.getValue(1)?.toString() else null } catch(e: Exception) { null }

                if (topic == null || value == null) {
                    errors.add(JsonObject().put("index", i).put("error", "Missing topic or value in record"))
                    continue
                }

                // ACL check per message
                if (userManager.isUserManagementEnabled() && !userManager.canPublish(username, topic)) {
                    errors.add(JsonObject().put("index", i).put("error", "Publish not allowed on topic: $topic"))
                    continue
                }

                val qos = try { if (record.size() > 2) record.getInteger(2) ?: 0 else 0 } catch(e: Exception) { 0 }
                val retain = try { if (record.size() > 3) record.getBoolean(3) ?: false else false } catch(e: Exception) { false }

                val brokerMessage = BrokerMessage(
                    messageUuid = Utils.getUuid(),
                    messageId = 0,
                    topicName = topic,
                    payload = value.toByteArray(),
                    qosLevel = qos.coerceIn(0, 2),
                    isRetain = retain,
                    isDup = false,
                    isQueued = false,
                    clientId = "$REST_CLIENT_PREFIX$username"
                )

                sessionHandler.publishMessage(brokerMessage)
                publishedCount++
            }
        }

        val response = JsonObject()
            .put("success", errors.isEmpty)
            .put("count", publishedCount)

        if (!errors.isEmpty) {
            response.put("errors", errors)
        }

        ctx.response().setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(response.encode())
    }

    // ========== InfluxDB Line Protocol Handler ==========

    private fun handleInfluxWrite(ctx: RoutingContext) {
        val body = ctx.body()?.asString()
        if (body.isNullOrBlank()) {
            ctx.response().setStatusCode(400)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("Empty body"))
            return
        }

        val username = ctx.get<String>("username") ?: "anonymous"
        val format = ctx.request().getParam("format") ?: "simple"
        val baseTopicParam = ctx.request().getParam("base")
        val baseTopic = if (baseTopicParam.isNullOrBlank()) "" else if (baseTopicParam.endsWith("/")) baseTopicParam else "$baseTopicParam/"
        val qos = ctx.request().getParam("qos")?.toIntOrNull()?.coerceIn(0, 2) ?: 0
        val retain = ctx.request().getParam("retain")?.toBoolean() ?: false

        var publishedCount = 0
        val errors = JsonArray()

        val lines = body.lines().map { it.trim() }.filter { it.isNotEmpty() && !it.startsWith("#") }
        for ((index, line) in lines.withIndex()) {
            // Split by unescaped space (simplistic Influx parsing)
            val parts = line.split(Regex(" (?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
            if (parts.size < 2) {
                errors.add(JsonObject().put("index", index).put("error", "Invalid line protocol format"))
                continue
            }

            val measAndTags = parts[0]
            val fieldsStr = parts[1]
            val timestampStr = if (parts.size > 2) parts[2] else null

            val mtParts = measAndTags.split(",")
            val measurement = mtParts[0]

            // Preserve tags in their original order
            val tagsPath = mtParts.drop(1).map {
                val kv = it.split("=")
                if (kv.size == 2) kv[1] else ""
            }.filter { it.isNotEmpty() }.joinToString("/")

            val measurementTopic = if (tagsPath.isEmpty()) measurement else "$measurement/$tagsPath"
            val fullBaseTopic = "$baseTopic$measurementTopic"

            // Parse fields
            val fields = fieldsStr.split(Regex(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
            val parsedFields = mutableMapOf<String, String>()
            for (field in fields) {
                val kv = field.split(Regex("=(?=([^\"]*\"[^\"]*\")*[^\"]*$)"), 2)
                if (kv.size == 2) {
                    // Remove quotes and InfluxDB trailing 'i' for integers
                    val cleanValue = kv[1].removeSurrounding("\"").let {
                        if (it.matches(Regex("-?\\d+i"))) it.dropLast(1) else it
                    }
                    parsedFields[kv[0]] = cleanValue
                }
            }

            if (parsedFields.isEmpty()) {
                errors.add(JsonObject().put("index", index).put("error", "No valid fields found"))
                continue
            }

            if (format.lowercase() == "json") {
                // Publish single JSON payload
                // ACL check
                if (userManager.isUserManagementEnabled() && !userManager.canPublish(username, fullBaseTopic)) {
                    errors.add(JsonObject().put("index", index).put("error", "Publish not allowed on topic: $fullBaseTopic"))
                    continue
                }

                val payloadObj = JsonObject()
                parsedFields.forEach { (k, v) ->
                    when {
                        v.equals("true", ignoreCase = true) -> payloadObj.put(k, true)
                        v.equals("false", ignoreCase = true) -> payloadObj.put(k, false)
                        v.matches(Regex("-?\\d+")) -> payloadObj.put(k, v.toLong())
                        v.matches(Regex("-?\\d+\\.\\d+")) -> payloadObj.put(k, v.toDouble())
                        else -> payloadObj.put(k, v)
                    }
                }
                if (timestampStr != null) {
                    val tsLong = timestampStr.toLongOrNull()
                    if (tsLong != null) {
                        payloadObj.put("timestamp_ns", tsLong)
                        try {
                            // Smart detect precision based on length (Influx defaults to ns = 19 digits)
                            val instant = when {
                                timestampStr.length <= 10 -> Instant.ofEpochSecond(tsLong)
                                timestampStr.length <= 13 -> Instant.ofEpochMilli(tsLong)
                                timestampStr.length <= 16 -> Instant.ofEpochSecond(tsLong / 1_000_000, (tsLong % 1_000_000) * 1_000)
                                else -> Instant.ofEpochSecond(tsLong / 1_000_000_000, tsLong % 1_000_000_000)
                            }
                            payloadObj.put("timestamp", instant.toString())
                        } catch (e: Exception) {
                            payloadObj.put("timestamp", timestampStr)
                        }
                    } else {
                        payloadObj.put("timestamp", timestampStr)
                    }
                }

                val brokerMessage = BrokerMessage(
                    messageUuid = Utils.getUuid(),
                    messageId = 0,
                    topicName = fullBaseTopic,
                    payload = payloadObj.encode().toByteArray(),
                    qosLevel = qos,
                    isRetain = retain,
                    isDup = false,
                    isQueued = false,
                    clientId = "$REST_CLIENT_PREFIX$username"
                )
                sessionHandler.publishMessage(brokerMessage)
                publishedCount++
            } else {
                // Default simple format: unroll fields to subtopics
                for ((fieldKey, fieldValue) in parsedFields) {
                    val topic = "$fullBaseTopic/$fieldKey"

                    // ACL check
                    if (userManager.isUserManagementEnabled() && !userManager.canPublish(username, topic)) {
                        errors.add(JsonObject().put("index", index).put("error", "Publish not allowed on topic: $topic"))
                        continue
                    }

                    val brokerMessage = BrokerMessage(
                        messageUuid = Utils.getUuid(),
                        messageId = 0,
                        topicName = topic,
                        payload = fieldValue.toByteArray(), // Value sent raw as byte array
                        qosLevel = qos,
                        isRetain = retain,
                        isDup = false,
                        isQueued = false,
                        clientId = "$REST_CLIENT_PREFIX$username"
                    )
                    sessionHandler.publishMessage(brokerMessage)
                    publishedCount++
                }
            }
        }

        if (errors.isEmpty) {
            ctx.response().setStatusCode(204).end() // Standard InfluxDB success code
        } else {
            val response = JsonObject()
                .put("success", false)
                .put("count", publishedCount)
                .put("errors", errors)
            ctx.response().setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .end(response.encode())
        }
    }

    // ========== Read Handler ==========

    private fun handleReadTopics(ctx: RoutingContext) {
        val topic = extractTopicFromPath(ctx)
        if (topic == null) {
            ctx.response().setStatusCode(400)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("Topic path is required"))
            return
        }

        val username = ctx.get<String>("username") ?: "anonymous"

        // ACL check
        if (userManager.isUserManagementEnabled() && !userManager.canSubscribe(username, topic)) {
            ctx.response().setStatusCode(403)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("Subscribe/read not allowed on topic: $topic"))
            return
        }

        // Determine which mode based on query parameters
        val hasRetained = ctx.request().params().contains("retained")
        val group = ctx.request().getParam("group")
        val start = ctx.request().getParam("start")
        val end = ctx.request().getParam("end")

        when {
            hasRetained -> handleReadRetained(ctx, topic)
            group != null && (start != null || end != null) -> handleReadHistory(ctx, topic, group, start, end)
            group != null -> handleReadLastValue(ctx, topic, group)
            else -> {
                ctx.response().setStatusCode(400)
                    .putHeader("Content-Type", "application/json")
                    .end(errorJson("Specify ?retained or ?group=<name>. See API docs at $API_PREFIX/docs"))
            }
        }
    }

    private fun handleReadRetained(ctx: RoutingContext, topic: String) {
        if (retainedStore == null) {
            ctx.response().setStatusCode(404)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("Retained store is not available"))
            return
        }

        val messages = JsonArray()
        retainedStore.findMatchingMessages(topic) { msg ->
            messages.add(brokerMessageToJson(msg))
            messages.size() < 10000 // safety limit
        }

        ctx.response().setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(JsonObject().put("messages", messages).encode())
    }

    private fun handleReadLastValue(ctx: RoutingContext, topic: String, group: String) {
        val archiveGroup = archiveHandler.getDeployedArchiveGroups()[group]
        if (archiveGroup == null) {
            ctx.response().setStatusCode(404)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("Archive group '$group' not found"))
            return
        }

        val lastValStore = archiveGroup.lastValStore
        if (lastValStore == null) {
            ctx.response().setStatusCode(404)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("Archive group '$group' has no last value store"))
            return
        }

        val messages = JsonArray()
        lastValStore.findMatchingMessages(topic) { msg ->
            messages.add(brokerMessageToJson(msg))
            messages.size() < 10000 // safety limit
        }

        ctx.response().setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(JsonObject().put("messages", messages).encode())
    }

    private fun handleReadHistory(ctx: RoutingContext, topic: String, group: String, start: String?, end: String?) {
        val archiveGroup = archiveHandler.getDeployedArchiveGroups()[group]
        if (archiveGroup == null) {
            ctx.response().setStatusCode(404)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("Archive group '$group' not found"))
            return
        }

        val archiveStore = archiveGroup.archiveStore
        if (archiveStore == null || archiveStore !is IMessageArchiveExtended) {
            ctx.response().setStatusCode(404)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("Archive group '$group' has no queryable archive store"))
            return
        }

        val limit = ctx.request().getParam("limit")?.toIntOrNull()?.coerceIn(1, 100000) ?: 1000

        val startTime = try {
            start?.let { Instant.parse(it) }
        } catch (e: Exception) {
            ctx.response().setStatusCode(400)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("Invalid 'start' parameter: ${e.message}"))
            return
        }

        val endTime = try {
            end?.let { Instant.parse(it) }
        } catch (e: Exception) {
            ctx.response().setStatusCode(400)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("Invalid 'end' parameter: ${e.message}"))
            return
        }

        val history = archiveStore.getHistory(topic, startTime, endTime, limit)

        ctx.response().setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(JsonObject().put("messages", history).encode())
    }

    // ========== SSE Subscribe Handler ==========

    private fun handleSubscribe(ctx: RoutingContext) {
        val topics = ctx.request().params().getAll("topic")
        if (topics.isNullOrEmpty()) {
            ctx.response().setStatusCode(400)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("At least one 'topic' query parameter is required"))
            return
        }

        val username = ctx.get<String>("username") ?: "anonymous"

        // ACL check for each topic
        if (userManager.isUserManagementEnabled()) {
            for (topic in topics) {
                if (!userManager.canSubscribe(username, topic)) {
                    ctx.response().setStatusCode(403)
                        .putHeader("Content-Type", "application/json")
                        .end(errorJson("Subscribe not allowed on topic: $topic"))
                    return
                }
            }
        }

        val connectionId = UUID.randomUUID().toString()
        val clientId = "$REST_CLIENT_PREFIX$connectionId"
        activeSseConnections[connectionId] = true

        // Setup SSE response
        val response = ctx.response()
        response.setChunked(true)
        response.putHeader("Content-Type", "text/event-stream")
        response.putHeader("Cache-Control", "no-cache")
        response.putHeader("Connection", "keep-alive")
        response.putHeader("X-Accel-Buffering", "no") // Disable nginx buffering

        // Send initial comment to establish connection
        response.write(": connected\n\n")

        // Register eventbus consumer for this SSE client
        val consumer = vertx.eventBus().consumer<Any>(EventBusAddresses.Client.messages(clientId)) { busMessage ->
            val body = busMessage.body()
            val msg = when (body) {
                is BrokerMessage -> body
                is BulkClientMessage -> {
                    // Handle bulk messages - send each individually
                    body.messages.forEach { m ->
                        if (activeSseConnections.containsKey(connectionId)) {
                            val event = sseEventJson(m)
                            response.write("data: $event\n\n")
                        }
                    }
                    return@consumer
                }
                else -> return@consumer
            }

            if (activeSseConnections.containsKey(connectionId)) {
                val event = sseEventJson(msg)
                response.write("data: $event\n\n")
            }
        }

        // Subscribe to all requested topics
        for (topic in topics) {
            sessionHandler.subscribeInternalClient(clientId, topic, 0)
        }

        // Send keepalive comments every 30 seconds
        val timerId = vertx.setPeriodic(30000) { _ ->
            if (activeSseConnections.containsKey(connectionId)) {
                try {
                    response.write(": keepalive\n\n")
                } catch (e: Exception) {
                    // Connection was closed
                    activeSseConnections.remove(connectionId)
                }
            }
        }

        // Cleanup on disconnect (single handler for all cleanup)
        response.closeHandler {
            logger.fine("SSE connection closed: $connectionId")
            vertx.cancelTimer(timerId)
            activeSseConnections.remove(connectionId)
            consumer.unregister()
            for (topic in topics) {
                sessionHandler.unsubscribeInternalClient(clientId, topic)
            }
            sessionHandler.unregisterInternalClient(clientId)
        }
    }

    // ========== OpenAPI / Swagger ==========

    private fun handleOpenApiSpec(ctx: RoutingContext) {
        val specStream = this::class.java.classLoader.getResourceAsStream("openapi.yaml")
        if (specStream != null) {
            val spec = specStream.bufferedReader().use { it.readText() }
            ctx.response().setStatusCode(200)
                .putHeader("Content-Type", "application/yaml")
                .end(spec)
        } else {
            ctx.response().setStatusCode(404)
                .putHeader("Content-Type", "application/json")
                .end(errorJson("OpenAPI specification not found"))
        }
    }

    private fun handleSwaggerUiRedirect(ctx: RoutingContext) {
        // Serve an inline Swagger UI HTML page that loads the spec from our endpoint
        val swaggerUiCss = "https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"
        val swaggerUiJs = "https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"
        val html = "<!DOCTYPE html>\n" +
            "<html lang=\"en\">\n" +
            "<head>\n" +
            "  <meta charset=\"UTF-8\">\n" +
            "  <title>MonsterMQ REST API - Swagger UI</title>\n" +
            "  <link rel=\"stylesheet\" type=\"text/css\" href=\"$swaggerUiCss\">\n" +
            "</head>\n" +
            "<body>\n" +
            "  <div id=\"swagger-ui\"></div>\n" +
            "  <script src=\"$swaggerUiJs\"></script>\n" +
            "  <script>\n" +
            "    SwaggerUIBundle({\n" +
            "      url: '/api/v1/openapi.yaml',\n" +
            "      dom_id: '#swagger-ui',\n" +
            "      presets: [\n" +
            "        SwaggerUIBundle.presets.apis,\n" +
            "        SwaggerUIBundle.SwaggerUIStandalonePreset\n" +
            "      ],\n" +
            "      layout: 'BaseLayout'\n" +
            "    });\n" +
            "  </script>\n" +
            "</body>\n" +
            "</html>"

        ctx.response().setStatusCode(200)
            .putHeader("Content-Type", "text/html")
            .end(html)
    }

    // ========== Helpers ==========

    private fun brokerMessageToJson(msg: BrokerMessage): JsonObject {
        return JsonObject()
            .put("topic", msg.topicName)
            .put("value", msg.getPayloadAsJsonValueOrString())
            .put("timestamp", msg.time.toString())
            .put("qos", msg.qosLevel)
            .put("retain", msg.isRetain)
    }

    private fun sseEventJson(msg: BrokerMessage): String {
        return JsonObject()
            .put("topic", msg.topicName)
            .put("value", msg.getPayloadAsJsonValueOrString())
            .put("timestamp", msg.time.toString())
            .encode()
    }

    private fun errorJson(message: String): String {
        return JsonObject().put("error", message).encode()
    }
}
