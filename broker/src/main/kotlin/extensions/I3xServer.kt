package at.rocworks.extensions

import at.rocworks.Utils
import at.rocworks.auth.UserManager
import at.rocworks.data.BrokerMessage
import at.rocworks.extensions.graphql.JwtService
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.handlers.SessionHandler
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
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * CESMII I3X API Server
 *
 * Implements the I3X (Industrial Information Interoperability eXchange) REST API,
 * exposing MonsterMQ's MQTT topics, current values, historical data, and publish
 * capabilities through a standardized manufacturing API.
 *
 * Concept mapping:
 * - Namespace   → Archive Group
 * - ObjectType  → "MqttTopic" (static type)
 * - Object      → MQTT topic (elementId = topic path)
 * - Value       → Current value from lastValStore (VQT format)
 * - History     → Historical records from archiveStore (VQT format)
 *
 * @see https://www.i3x.dev/
 * @see https://i3x.cesmii.net/docs
 */
class I3xServer(
    private val host: String,
    private val port: Int,
    private val archiveHandler: ArchiveHandler,
    private val sessionHandler: SessionHandler,
    private val userManager: UserManager,
) : AbstractVerticle() {

    private val logger = Utils.getLogger(this::class.java)

    private data class I3xSubscription(
        val id: String,
        val topics: MutableList<String> = mutableListOf(),
        val pendingQueue: ArrayDeque<JsonObject> = ArrayDeque()
    )

    private val subscriptions = ConcurrentHashMap<String, I3xSubscription>()

    private val NS_PREFIX = "urn:monstermq:archivegroup"
    private val REL_TYPE_ID = "urn:monstermq:reltype:parent"

    private fun typeId(groupName: String) = "$NS_PREFIX:$groupName:type:MqttTopic"
    private fun namespaceUri(groupName: String) = "$NS_PREFIX:$groupName"

    // elementId encodes the archive group as a prefix: "{groupName}/{mqttTopic}"
    // Archive group names must not contain '/' (they are simple config names like "Default")
    private fun parseElementId(elementId: String): Pair<String, String>? {
        val slashIdx = elementId.indexOf('/')
        if (slashIdx < 0) return null
        return elementId.substring(0, slashIdx) to elementId.substring(slashIdx + 1)
    }

    // Parses groupName from a typeId like "urn:monstermq:archivegroup:{groupName}:type:MqttTopic"
    private fun parseTypeId(tid: String): String? {
        val prefix = "$NS_PREFIX:"
        val suffix = ":type:MqttTopic"
        if (!tid.startsWith(prefix) || !tid.endsWith(suffix)) return null
        return tid.removePrefix(prefix).removeSuffix(suffix)
    }

    override fun start(startPromise: Promise<Void>) {
        logger.fine("Starting I3X API server")

        val router = Router.router(vertx)

        router.route().handler(
            CorsHandler.create()
                .addOrigin("*")
                .allowedMethod(io.vertx.core.http.HttpMethod.GET)
                .allowedMethod(io.vertx.core.http.HttpMethod.POST)
                .allowedMethod(io.vertx.core.http.HttpMethod.PUT)
                .allowedMethod(io.vertx.core.http.HttpMethod.DELETE)
                .allowedMethod(io.vertx.core.http.HttpMethod.OPTIONS)
                .allowedHeader("Content-Type")
                .allowedHeader("Authorization")
                .allowedHeader("Accept")
        )

        router.route().handler(BodyHandler.create())

        router.route("/*").handler { ctx ->
            if (!validateAuthentication(ctx)) return@handler
            ctx.next()
        }

        // Explore endpoints
        router.get("/namespaces").handler(::handleNamespaces)
        router.get("/objecttypes").handler(::handleObjectTypes)
        router.post("/objecttypes/query").handler(::handleObjectTypesQuery)
        router.get("/relationshiptypes").handler(::handleRelationshipTypes)
        router.get("/objects").handler(::handleObjects)
        router.post("/objects/list").handler(::handleObjectsList)
        router.post("/objects/related").handler(::handleObjectsRelated)

        // Query endpoints
        router.post("/objects/value").handler(::handleObjectsValue)
        router.post("/objects/history").handler(::handleObjectsHistory)

        // Update endpoints — elementId may contain slashes, so extract from raw path
        router.putWithRegex("/objects/(.+)/value").handler(::handleUpdateValue)
        router.putWithRegex("/objects/(.+)/history").handler(::handleUpdateHistory)

        // Subscribe endpoints
        router.get("/subscriptions").handler(::handleListSubscriptions)
        router.post("/subscriptions").handler(::handleCreateSubscription)
        router.get("/subscriptions/:id").handler(::handleGetSubscription)
        router.delete("/subscriptions/:id").handler(::handleDeleteSubscription)
        router.post("/subscriptions/:id/register").handler(::handleRegisterTopics)
        router.post("/subscriptions/:id/unregister").handler(::handleUnregisterTopics)
        router.get("/subscriptions/:id/stream").handler(::handleStream)
        router.post("/subscriptions/:id/sync").handler(::handleSync)

        vertx.createHttpServer(HttpServerOptions().setPort(port).setHost(host))
            .requestHandler(router)
            .listen()
            .onSuccess { server ->
                logger.info("I3X API Server started on port ${server.actualPort()}")
                startPromise.complete()
            }
            .onFailure { error ->
                logger.severe("I3X API Server failed to start: $error")
                startPromise.fail(error)
            }
    }

    // --- Helpers ---

    private fun mqttTopicType(groupName: String) = JsonObject()
        .put("elementId", typeId(groupName))
        .put("displayName", "MQTT Topic")
        .put("description", "An MQTT topic with a value payload")
        .put("namespaceUri", namespaceUri(groupName))
        .put("definition", JsonObject()
            .put("type", "object")
            .put("properties", JsonObject()
                .put("value", JsonObject().put("type", "any").put("description", "Topic payload value"))
                .put("timestamp", JsonObject().put("type", "string").put("description", "ISO 8601 timestamp")))
            .encode())
        .put("variables", JsonArray())

    private fun topicToObject(groupName: String, topicName: String) = JsonObject()
        .put("elementId", "$groupName/$topicName")
        .put("displayName", topicName)
        .put("typeId", typeId(groupName))

    private fun messageToVqt(message: BrokerMessage): JsonObject {
        val payloadStr = String(message.payload, Charsets.UTF_8)
        val value: Any = try { JsonObject(payloadStr) } catch (e: Exception) {
            payloadStr.toDoubleOrNull() ?: payloadStr
        }
        return JsonObject().put("v", value).put("q", 1).put("t", message.time.toString())
    }

    private fun ok(ctx: RoutingContext, body: JsonObject) {
        ctx.response().setStatusCode(200).putHeader("Content-Type", "application/json").end(body.encode())
    }

    private fun ok(ctx: RoutingContext, body: JsonArray) {
        ctx.response().setStatusCode(200).putHeader("Content-Type", "application/json").end(body.encode())
    }

    private fun err(ctx: RoutingContext, status: Int, message: String) {
        ctx.response().setStatusCode(status).putHeader("Content-Type", "application/json")
            .end(JsonObject().put("error", message).encode())
    }

    private fun parseInstant(s: String): Instant? = try { Instant.parse(s) } catch (e: DateTimeParseException) { null }

    // --- Explore Handlers ---

    private fun handleNamespaces(ctx: RoutingContext) {
        val result = JsonArray()
        archiveHandler.getDeployedArchiveGroups().keys.forEach { name ->
            result.add(JsonObject().put("uri", namespaceUri(name)).put("displayName", name))
        }
        ok(ctx, result)
    }

    private fun handleObjectTypes(ctx: RoutingContext) {
        val nsUri = ctx.queryParam("namespaceUri").firstOrNull()
        val groups = archiveHandler.getDeployedArchiveGroups()
        val result = JsonArray()
        if (nsUri == null) {
            groups.keys.forEach { result.add(mqttTopicType(it)) }
        } else {
            val groupName = nsUri.removePrefix("$NS_PREFIX:")
            if (groups.containsKey(groupName)) result.add(mqttTopicType(groupName))
        }
        ok(ctx, result)
    }

    private fun handleObjectTypesQuery(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val elementIds = body.getJsonArray("elementIds") ?: JsonArray()
        val groups = archiveHandler.getDeployedArchiveGroups()
        val result = JsonArray()
        for (i in 0 until elementIds.size()) {
            val groupName = parseTypeId(elementIds.getString(i) ?: continue) ?: continue
            if (groups.containsKey(groupName)) result.add(mqttTopicType(groupName))
        }
        ok(ctx, result)
    }

    private fun handleRelationshipTypes(ctx: RoutingContext) {
        ok(ctx, JsonArray().add(
            JsonObject()
                .put("elementId", REL_TYPE_ID)
                .put("displayName", "parent")
                .put("description", "MQTT topic path hierarchy")
        ))
    }

    private fun handleObjects(ctx: RoutingContext) {
        val typeIdParam = ctx.queryParam("typeId").firstOrNull()
        val groups = archiveHandler.getDeployedArchiveGroups()
        val result = JsonArray()
        val groupsToQuery = if (typeIdParam != null) {
            val groupName = parseTypeId(typeIdParam)
            if (groupName != null && groups.containsKey(groupName)) mapOf(groupName to groups[groupName]!!)
            else emptyMap()
        } else {
            groups
        }
        for ((groupName, archiveGroup) in groupsToQuery) {
            val lastValStore = archiveGroup.lastValStore ?: continue
            lastValStore.findMatchingMessages("#") { message ->
                result.add(topicToObject(groupName, message.topicName))
                result.size() < 10000
            }
        }
        ok(ctx, result)
    }

    private fun handleObjectsList(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val elementIds = body.getJsonArray("elementIds") ?: JsonArray()
        val groups = archiveHandler.getDeployedArchiveGroups()
        val result = JsonArray()
        for (i in 0 until elementIds.size()) {
            val (groupName, topicName) = parseElementId(elementIds.getString(i) ?: continue) ?: continue
            val lastValStore = groups[groupName]?.lastValStore ?: continue
            val msg = lastValStore[topicName]
            if (msg != null) result.add(topicToObject(groupName, msg.topicName))
        }
        ok(ctx, result)
    }

    private fun handleObjectsRelated(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val elementIds = body.getJsonArray("elementIds") ?: JsonArray()
        val groups = archiveHandler.getDeployedArchiveGroups()
        val result = JsonArray()
        for (i in 0 until elementIds.size()) {
            val (groupName, parentTopic) = parseElementId(elementIds.getString(i) ?: continue) ?: continue
            val lastValStore = groups[groupName]?.lastValStore ?: continue
            val prefix = if (parentTopic.endsWith("/")) parentTopic else "$parentTopic/"
            lastValStore.findMatchingMessages("$parentTopic/#") { message ->
                val rest = message.topicName.removePrefix(prefix)
                if (!rest.contains("/")) result.add(topicToObject(groupName, message.topicName))
                true
            }
        }
        ok(ctx, result)
    }

    // --- Query Handlers ---

    private fun handleObjectsValue(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val elementIds = body.getJsonArray("elementIds") ?: JsonArray()
        val maxDepth = body.getInteger("maxDepth", 0)
        val groups = archiveHandler.getDeployedArchiveGroups()
        val result = JsonArray()

        for (i in 0 until elementIds.size()) {
            val elementId = elementIds.getString(i) ?: continue
            val (groupName, topicName) = parseElementId(elementId) ?: continue
            val lastValStore = groups[groupName]?.lastValStore ?: continue

            if (maxDepth == 0) {
                val msg = lastValStore[topicName]
                if (msg != null) {
                    result.add(JsonObject()
                        .put("elementId", elementId)
                        .put("value", JsonObject().put("data", JsonArray().add(messageToVqt(msg)))))
                }
            } else {
                val pattern = if (topicName.endsWith("#")) topicName else "$topicName/#"
                lastValStore.findMatchingMessages(pattern) { msg ->
                    result.add(JsonObject()
                        .put("elementId", "$groupName/${msg.topicName}")
                        .put("value", JsonObject().put("data", JsonArray().add(messageToVqt(msg)))))
                    true
                }
            }
        }
        ok(ctx, result)
    }

    private fun handleObjectsHistory(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val elementIds = body.getJsonArray("elementIds") ?: JsonArray()
        val startTime = body.getString("startTime")?.let { parseInstant(it) }
        val endTime = body.getString("endTime")?.let { parseInstant(it) }
        val limit = body.getInteger("limit", 1000)
        val groups = archiveHandler.getDeployedArchiveGroups()
        val result = JsonArray()

        for (i in 0 until elementIds.size()) {
            val elementId = elementIds.getString(i) ?: continue
            val (groupName, topicName) = parseElementId(elementId) ?: continue
            val archiveStore = groups[groupName]?.archiveStore as? IMessageArchiveExtended ?: run {
                logger.warning("I3X history: no archiveStore for archive group '$groupName'")
                continue
            }
            try {
                val history = archiveStore.getHistory(topicName, startTime, endTime, limit)
                val data = JsonArray()
                for (j in 0 until history.size()) {
                    val record = history.getJsonObject(j)
                    val timestampMs = record.getLong("timestamp") ?: continue
                    val payloadStr = record.getString("payload_json")
                        ?: record.getString("payload")
                        ?: record.getString("payload_base64")?.let { b64 ->
                            try { String(Base64.getDecoder().decode(b64)) } catch (e: Exception) { null }
                        } ?: continue
                    val value: Any = try { JsonObject(payloadStr) } catch (e: Exception) {
                        payloadStr.toDoubleOrNull() ?: payloadStr
                    }
                    data.add(JsonObject()
                        .put("v", value)
                        .put("q", 1)
                        .put("t", Instant.ofEpochMilli(timestampMs).toString()))
                }
                result.add(JsonObject()
                    .put("elementId", elementId)
                    .put("value", JsonObject().put("data", data)))
            } catch (e: Exception) {
                logger.warning("I3X history error for '$elementId': ${e.message}")
            }
        }
        ok(ctx, result)
    }

    // --- Update Handlers ---

    private fun handleUpdateValue(ctx: RoutingContext) {
        val rawPath = ctx.request().path() // /objects/<elementId>/value
        val elementId = rawPath.removePrefix("/objects/").removeSuffix("/value")
        if (elementId.isEmpty()) {
            err(ctx, 400, "Missing elementId")
            return
        }
        val (_, topicName) = parseElementId(elementId)
            ?: return err(ctx, 400, "Invalid elementId format, expected {archiveGroup}/{mqttTopic}")
        val body = ctx.body().asString() ?: ""
        val message = BrokerMessage(
            messageId = 0,
            topicName = topicName,
            payload = body.toByteArray(Charsets.UTF_8),
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = "I3X_API"
        )
        sessionHandler.publishMessage(message)
        ok(ctx, JsonObject().put("elementId", elementId).put("status", "published"))
    }

    private fun handleUpdateHistory(ctx: RoutingContext) {
        err(ctx, 501, "Historical data update not yet implemented")
    }

    // --- Subscription Handlers ---

    private fun handleListSubscriptions(ctx: RoutingContext) {
        val result = JsonArray()
        subscriptions.values.forEach { result.add(subscriptionSummary(it)) }
        ok(ctx, JsonObject().put("subscriptions", result))
    }

    private fun handleCreateSubscription(ctx: RoutingContext) {
        val id = UUID.randomUUID().toString()
        subscriptions[id] = I3xSubscription(id)
        ok(ctx, JsonObject().put("subscriptionId", id).put("message", "Subscription created"))
    }

    private fun handleGetSubscription(ctx: RoutingContext) {
        val sub = subscriptions[ctx.pathParam("id")]
            ?: return err(ctx, 404, "Subscription not found")
        ok(ctx, subscriptionSummary(sub).put("registeredTopics", JsonArray(sub.topics)))
    }

    private fun handleDeleteSubscription(ctx: RoutingContext) {
        subscriptions.remove(ctx.pathParam("id"))
        ok(ctx, JsonObject().put("message", "Subscription deleted"))
    }

    private fun handleRegisterTopics(ctx: RoutingContext) {
        val sub = subscriptions[ctx.pathParam("id")]
            ?: return err(ctx, 404, "Subscription not found")
        val elementIds = ctx.body().asJsonObject()?.getJsonArray("elementIds") ?: JsonArray()
        for (i in 0 until elementIds.size()) {
            val topic = elementIds.getString(i) ?: continue
            if (!sub.topics.contains(topic)) sub.topics.add(topic)
        }
        ok(ctx, JsonObject().put("message", "Topics registered"))
    }

    private fun handleUnregisterTopics(ctx: RoutingContext) {
        val sub = subscriptions[ctx.pathParam("id")]
            ?: return err(ctx, 404, "Subscription not found")
        val elementIds = ctx.body().asJsonObject()?.getJsonArray("elementIds") ?: JsonArray()
        for (i in 0 until elementIds.size()) sub.topics.remove(elementIds.getString(i))
        ok(ctx, JsonObject().put("message", "Topics unregistered"))
    }

    /**
     * SSE stream endpoint — opens a Server-Sent Events stream for real-time updates.
     * Messages are routed via the internal Vert.X event bus address "mq.i3x.sub.<id>".
     * To receive messages, publish BrokerMessages to that address from within the broker.
     */
    private fun handleStream(ctx: RoutingContext) {
        val id = ctx.pathParam("id")
        val sub = subscriptions[id] ?: return err(ctx, 404, "Subscription not found")

        val response = ctx.response()
        response.putHeader("Content-Type", "text/event-stream")
            .putHeader("Cache-Control", "no-cache")
            .putHeader("Connection", "keep-alive")
            .setChunked(true)

        val busAddress = "mq.i3x.sub.$id"
        val consumer = vertx.eventBus().consumer<JsonObject>(busAddress) { msg ->
            if (!response.ended()) response.write("data: ${msg.body().encode()}\n\n")
        }

        val timerId = vertx.setPeriodic(30_000L) {
            if (!response.ended()) response.write(": heartbeat\n\n")
        }

        response.closeHandler {
            vertx.cancelTimer(timerId)
            consumer.unregister()
            logger.fine("I3X SSE stream closed for subscription $id")
        }

        // Flush any pending queued messages immediately
        while (sub.pendingQueue.isNotEmpty()) {
            response.write("data: ${sub.pendingQueue.removeFirst().encode()}\n\n")
        }
    }

    private fun handleSync(ctx: RoutingContext) {
        val sub = subscriptions[ctx.pathParam("id")]
            ?: return err(ctx, 404, "Subscription not found")
        val result = JsonArray()
        while (sub.pendingQueue.isNotEmpty()) result.add(sub.pendingQueue.removeFirst())
        ok(ctx, result)
    }

    private fun subscriptionSummary(sub: I3xSubscription) = JsonObject()
        .put("subscriptionId", sub.id)
        .put("topicCount", sub.topics.size)
        .put("pendingMessages", sub.pendingQueue.size)

    // --- Authentication (mirrors GrafanaServer) ---

    private fun validateAuthentication(ctx: RoutingContext): Boolean {
        if (!userManager.isUserManagementEnabled()) return true

        val authHeader = ctx.request().getHeader("Authorization")
        if (authHeader == null) {
            ctx.response().setStatusCode(401)
                .putHeader("Content-Type", "application/json")
                .putHeader("WWW-Authenticate", "Basic realm=\"MonsterMQ I3X API\"")
                .end(JsonObject().put("error", "Authentication required").encode())
            return false
        }

        if (authHeader.startsWith("Bearer ", ignoreCase = true)) {
            val token = authHeader.substring(7)
            val username = JwtService.extractUsername(token)
            if (username != null && !JwtService.isTokenExpired(token)) return true
            ctx.response().setStatusCode(401)
                .putHeader("Content-Type", "application/json")
                .putHeader("WWW-Authenticate", "Bearer error=\"invalid_token\"")
                .end(JsonObject().put("error", "Invalid or expired token").encode())
            return false
        }

        if (authHeader.startsWith("Basic ", ignoreCase = true)) {
            try {
                val credentials = String(Base64.getDecoder().decode(authHeader.substring(6)))
                val parts = credentials.split(":", limit = 2)
                if (parts.size == 2) {
                    userManager.authenticate(parts[0], parts[1]).onComplete { result ->
                        if (result.succeeded() && result.result()?.enabled == true) {
                            ctx.next()
                        } else {
                            ctx.response().setStatusCode(401)
                                .putHeader("Content-Type", "application/json")
                                .putHeader("WWW-Authenticate", "Basic realm=\"MonsterMQ I3X API\"")
                                .end(JsonObject().put("error", "Invalid credentials").encode())
                        }
                    }
                    return false
                }
            } catch (e: Exception) {
                logger.warning("I3X Basic Auth parsing error: ${e.message}")
            }
            ctx.response().setStatusCode(401)
                .putHeader("Content-Type", "application/json")
                .putHeader("WWW-Authenticate", "Basic realm=\"MonsterMQ I3X API\"")
                .end(JsonObject().put("error", "Invalid credentials format").encode())
            return false
        }

        ctx.response().setStatusCode(401)
            .putHeader("Content-Type", "application/json")
            .putHeader("WWW-Authenticate", "Basic realm=\"MonsterMQ I3X API\"")
            .end(JsonObject().put("error", "Unsupported authentication method").encode())
        return false
    }
}
