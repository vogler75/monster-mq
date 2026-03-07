package at.rocworks.extensions

import at.rocworks.Const
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
 * - ObjectType  → "MqttTopic" (single static type)
 * - Object      → MQTT topic level (elementId = {group}/{topic}, linked via parentId)
 * - Value       → Current value from lastValStore (VQT format; JSON payloads = properties)
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

    private val logger = Utils.getLogger(this::class.java).also { it.level = Const.DEBUG_LEVEL }

    private data class I3xSubscription(
        val id: String,
        val created: Instant = Instant.now(),
        val topics: MutableList<String> = mutableListOf(),
        val pendingQueue: ArrayDeque<JsonObject> = ArrayDeque(),
        var maxDepth: Int = 1
    )

    private val subscriptions = ConcurrentHashMap<String, I3xSubscription>()

    private val NS_PREFIX = "urn:monstermq:archivegroup"
    private val REL_TYPE_PARENT = "urn:monstermq:reltype:parent"

    private fun typeId(groupName: String) = "$NS_PREFIX:$groupName:type:MqttTopic"

    // elementId encodes the archive group as a prefix: "{groupName}/{mqttTopic}"
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

    private fun parentIdFromTopic(groupName: String, topicName: String): String {
        val lastSlash = topicName.lastIndexOf('/')
        return if (lastSlash > 0) "$groupName/${topicName.substring(0, lastSlash)}" else "/"
    }

    /**
     * Check if a candidate topic is within the allowed depth from a base topic.
     * I3X maxDepth semantics: 0 = infinite, 1 = exact match only, N = N-1 levels of children.
     */
    private fun topicMatchesDepth(baseTopic: String, candidateTopic: String, maxDepth: Int): Boolean {
        if (maxDepth == 0) return true
        if (candidateTopic == baseTopic) return true
        if (maxDepth == 1) return false
        val prefix = "$baseTopic/"
        if (!candidateTopic.startsWith(prefix)) return false
        val levels = candidateTopic.removePrefix(prefix).count { it == '/' } + 1
        return levels <= maxDepth - 1
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

        val basePath = "/i3x"

        router.route("$basePath/*").handler { ctx ->
            val body = if (ctx.request().method().name() == "POST") ctx.body()?.asString()?.take(200) else ""
            logger.fine("I3X ${ctx.request().method()} ${ctx.request().uri()} $body")
            if (!validateAuthentication(ctx)) return@handler
            ctx.next()
        }

        // Explore endpoints
        router.get("$basePath/namespaces").handler(::handleNamespaces)
        router.get("$basePath/objecttypes").handler(::handleObjectTypes)
        router.post("$basePath/objecttypes/query").handler(::handleObjectTypesQuery)
        router.get("$basePath/relationshiptypes").handler(::handleRelationshipTypes)
        router.post("$basePath/relationshiptypes/query").handler(::handleRelationshipTypesQuery)
        router.get("$basePath/objects").handler(::handleObjects)
        router.post("$basePath/objects/list").handler(::handleObjectsList)
        router.post("$basePath/objects/related").handler(::handleObjectsRelated)

        // Query endpoints
        router.post("$basePath/objects/value").handler(::handleObjectsValue)
        router.post("$basePath/objects/history").handler(::handleObjectsHistory)

        // Update endpoints — elementId may contain slashes, so extract from raw path
        router.putWithRegex("$basePath/objects/(.+)/value").handler(::handleUpdateValue)
        router.putWithRegex("$basePath/objects/(.+)/history").handler(::handleUpdateHistory)

        // Subscribe endpoints
        router.get("$basePath/subscriptions").handler(::handleListSubscriptions)
        router.post("$basePath/subscriptions").handler(::handleCreateSubscription)
        router.get("$basePath/subscriptions/:id").handler(::handleGetSubscription)
        router.delete("$basePath/subscriptions/:id").handler(::handleDeleteSubscription)
        router.post("$basePath/subscriptions/:id/register").handler(::handleRegisterTopics)
        router.post("$basePath/subscriptions/:id/unregister").handler(::handleUnregisterTopics)
        router.get("$basePath/subscriptions/:id/stream").handler(::handleStream)
        router.post("$basePath/subscriptions/:id/sync").handler(::handleSync)

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

    override fun stop() {
        for (sub in subscriptions.values) {
            sessionHandler.unregisterMessageListener("i3x-${sub.id}")
        }
        subscriptions.clear()
    }

    // --- Helpers ---

    private fun mqttTopicType(groupName: String) = JsonObject()
        .put("elementId", typeId(groupName))
        .put("displayName", "MQTT Topic")
        .put("description", "An MQTT topic with a value payload")
        .put("schema", JsonObject()
            .put("type", "object")
            .put("properties", JsonObject()
                .put("value", JsonObject().put("type", "any").put("description", "Topic payload value"))
                .put("timestamp", JsonObject().put("type", "string").put("description", "ISO 8601 timestamp")))
            .encode())
        .put("variables", JsonArray())

    private fun buildObjectJson(
        groupName: String,
        topicName: String,
        parentId: String,
        isComposition: Boolean,
        includeMetadata: Boolean
    ): JsonObject {
        val displayName = topicName.substringAfterLast("/")
        val obj = JsonObject()
            .put("elementId", "$groupName/$topicName")
            .put("displayName", displayName)
            .put("typeId", typeId(groupName))
            .put("parentId", parentId)
            .put("isComposition", isComposition)
            .put("namespaceUri", "$NS_PREFIX:$groupName")
        if (includeMetadata) {
            obj.put("relationships", JsonArray().add(
                JsonObject()
                    .put("relationshipTypeId", REL_TYPE_PARENT)
                    .put("targetElementId", parentId)
            ))
        }
        return obj
    }

    /**
     * Expand stored topic names into the full set of topic levels including intermediates.
     * E.g., topic "a/b/c" produces ["a", "a/b", "a/b/c"].
     */
    private fun expandAllTopicLevels(storeTopics: Collection<String>): Set<String> {
        val allTopics = mutableSetOf<String>()
        for (topic in storeTopics) {
            val parts = topic.split("/")
            for (j in 1..parts.size) {
                allTopics.add(parts.subList(0, j).joinToString("/"))
            }
        }
        return allTopics
    }

    /**
     * Compute which topics have children in the full expanded set.
     */
    private fun computeHasChildrenSet(allTopics: Set<String>): Set<String> {
        val hasChildren = mutableSetOf<String>()
        for (topic in allTopics) {
            val parent = topic.substringBeforeLast("/", "")
            if (parent.isNotEmpty()) hasChildren.add(parent)
        }
        return hasChildren
    }

    private fun messageToVqt(message: BrokerMessage): JsonObject {
        val payloadStr = String(message.payload, Charsets.UTF_8)
        val value: Any = try { JsonObject(payloadStr) } catch (_: Exception) {
            try { JsonArray(payloadStr) } catch (_: Exception) {
                payloadStr.toDoubleOrNull() ?: payloadStr
            }
        }
        return JsonObject()
            .put("value", value)
            .put("quality", "GOOD")
            .put("timestamp", message.time.toString())
    }

    private fun extractPayloadString(record: JsonObject): String? {
        return record.getString("payload_json")
            ?: record.getString("payload")
            ?: record.getString("payload_base64")?.let { b64 ->
                try { String(Base64.getDecoder().decode(b64)) } catch (e: Exception) { null }
            }
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
            result.add(JsonObject().put("uri", "$NS_PREFIX:$name").put("displayName", name))
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
                .put("elementId", REL_TYPE_PARENT)
                .put("displayName", "parent")
                .put("description", "MQTT topic path hierarchy")
        ))
    }

    private fun handleRelationshipTypesQuery(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val elementIds = body.getJsonArray("elementIds") ?: JsonArray()
        val result = JsonArray()
        for (i in 0 until elementIds.size()) {
            if (elementIds.getString(i) == REL_TYPE_PARENT) {
                result.add(JsonObject()
                    .put("elementId", REL_TYPE_PARENT)
                    .put("displayName", "parent")
                    .put("description", "MQTT topic path hierarchy"))
            }
        }
        ok(ctx, result)
    }

    private fun handleObjects(ctx: RoutingContext) {
        val typeIdParam = ctx.queryParam("typeId").firstOrNull()
        val includeMetadata = ctx.queryParam("includeMetadata").firstOrNull()?.toBooleanStrictOrNull() ?: false
        val groups = archiveHandler.getDeployedArchiveGroups()
        val result = JsonArray()
        val groupsToQuery = if (typeIdParam != null) {
            val groupName = parseTypeId(typeIdParam)
            if (groupName != null && groups.containsKey(groupName)) mapOf(groupName to groups[groupName]!!)
            else emptyMap()
        } else {
            groups
        }
        // Return only root objects (first topic level per group).
        // The browser navigates children via POST /objects/related.
        for ((groupName, archiveGroup) in groupsToQuery) {
            val lastValStore = archiveGroup.lastValStore ?: continue

            // Collect distinct first-level topic segments
            val rootSegments = mutableSetOf<String>()
            lastValStore.findMatchingMessages("#") { msg ->
                rootSegments.add(msg.topicName.substringBefore("/"))
                rootSegments.size < 10000
            }

            for (root in rootSegments.sorted()) {
                // A root is a composition if any topic goes deeper
                val hasChildren = rootSegments.isNotEmpty() // always true if we got here
                result.add(buildObjectJson(
                    groupName, root, "/", true, includeMetadata
                ))
            }
        }
        ok(ctx, result)
    }

    private fun handleObjectsList(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val elementIds = body.getJsonArray("elementIds") ?: JsonArray()
        val includeMetadata = body.getBoolean("includeMetadata", false)
        val groups = archiveHandler.getDeployedArchiveGroups()
        val result = JsonArray()
        for (i in 0 until elementIds.size()) {
            val fullId = elementIds.getString(i) ?: continue
            val (groupName, topicName) = parseElementId(fullId) ?: continue
            val lastValStore = groups[groupName]?.lastValStore ?: continue

            // Check if this topic exists directly or as an intermediate level
            var exists = lastValStore[topicName] != null
            var hasChildren = false
            lastValStore.findMatchingMessages("$topicName/#") { m ->
                if (m.topicName != topicName) { hasChildren = true; exists = true; false } else true
            }
            if (!exists) {
                // Also check if topic exists as prefix of any stored topic
                lastValStore.findMatchingMessages("$topicName/+") { m ->
                    exists = true; hasChildren = true; false
                }
            }
            if (!exists) continue
            result.add(buildObjectJson(
                groupName, topicName, parentIdFromTopic(groupName, topicName),
                hasChildren, includeMetadata
            ))
        }
        ok(ctx, result)
    }

    private fun handleObjectsRelated(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val elementIds = body.getJsonArray("elementIds") ?: JsonArray()
        val includeMetadata = body.getBoolean("includeMetadata", false)
        val relationshipType = body.getString("relationshiptype")
        val groups = archiveHandler.getDeployedArchiveGroups()
        val result = JsonArray()

        // If a specific relationship type is requested and it's not ours, return empty
        if (relationshipType != null && relationshipType != REL_TYPE_PARENT) {
            ok(ctx, result)
            return
        }

        for (i in 0 until elementIds.size()) {
            val fullId = elementIds.getString(i) ?: continue
            val (groupName, parentTopic) = parseElementId(fullId) ?: continue
            val lastValStore = groups[groupName]?.lastValStore ?: continue

            // Collect all descendant topics from the store
            val storeDescendants = mutableSetOf<String>()
            lastValStore.findMatchingMessages("$parentTopic/#") { msg ->
                if (msg.topicName != parentTopic) storeDescendants.add(msg.topicName)
                true
            }

            // Expand to include intermediate levels between parent and leaf topics
            val allDescendants = mutableSetOf<String>()
            val prefix = "$parentTopic/"
            for (desc in storeDescendants) {
                val suffix = desc.removePrefix(prefix)
                val parts = suffix.split("/")
                for (j in 1..parts.size) {
                    allDescendants.add("$parentTopic/${parts.subList(0, j).joinToString("/")}")
                }
            }

            // Direct children: one level below parent
            for (childTopic in allDescendants.sorted()) {
                if (!childTopic.startsWith(prefix)) continue
                if (childTopic.removePrefix(prefix).contains("/")) continue

                val hasSubChildren = allDescendants.any { it.startsWith("$childTopic/") }

                result.add(buildObjectJson(
                    groupName, childTopic, fullId, hasSubChildren, includeMetadata
                ))
            }
        }
        ok(ctx, result)
    }

    // --- Query Handlers ---

    private fun handleObjectsValue(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val elementIds = body.getJsonArray("elementIds") ?: JsonArray()
        val maxDepth = body.getInteger("maxDepth") ?: 1 // I3X spec: 0=infinite, 1=exact(default)
        val groups = archiveHandler.getDeployedArchiveGroups()
        val result = JsonObject()

        for (i in 0 until elementIds.size()) {
            val elementId = elementIds.getString(i) ?: continue
            val (groupName, topicName) = parseElementId(elementId) ?: continue
            val lastValStore = groups[groupName]?.lastValStore ?: continue

            if (maxDepth == 1) {
                val msg = lastValStore[topicName]
                if (msg != null) {
                    result.put(elementId, JsonObject().put("data", JsonArray().add(messageToVqt(msg))))
                }
            } else {
                val selfMsg = lastValStore[topicName]
                if (selfMsg != null) {
                    result.put(elementId, JsonObject().put("data", JsonArray().add(messageToVqt(selfMsg))))
                }
                lastValStore.findMatchingMessages("$topicName/#") { msg ->
                    if (msg.topicName != topicName && topicMatchesDepth(topicName, msg.topicName, maxDepth)) {
                        val childId = "$groupName/${msg.topicName}"
                        result.put(childId, JsonObject().put("data", JsonArray().add(messageToVqt(msg))))
                    }
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
        val maxDepth = body.getInteger("maxDepth") ?: 1
        val groups = archiveHandler.getDeployedArchiveGroups()
        val result = JsonObject()

        for (i in 0 until elementIds.size()) {
            val elementId = elementIds.getString(i) ?: continue
            val (groupName, topicName) = parseElementId(elementId) ?: continue

            val topicsToQuery = mutableListOf<Pair<String, String>>() // (elementId, mqttTopic)
            topicsToQuery.add(elementId to topicName)
            if (maxDepth != 1) {
                val lastValStore = groups[groupName]?.lastValStore
                lastValStore?.findMatchingMessages("$topicName/#") { msg ->
                    if (msg.topicName != topicName && topicMatchesDepth(topicName, msg.topicName, maxDepth)) {
                        topicsToQuery.add("$groupName/${msg.topicName}" to msg.topicName)
                    }
                    true
                }
            }

            for ((eid, mqttTopic) in topicsToQuery) {
                val archiveStore = groups[groupName]?.archiveStore as? IMessageArchiveExtended ?: continue
                try {
                    val history = archiveStore.getHistory(mqttTopic, startTime, endTime, limit)
                    val data = JsonArray()
                    for (j in 0 until history.size()) {
                        val record = history.getJsonObject(j)
                        val timestampMs = record.getLong("timestamp") ?: continue
                        val payloadStr = extractPayloadString(record) ?: continue
                        val value: Any = try { JsonObject(payloadStr) } catch (_: Exception) {
                            try { JsonArray(payloadStr) } catch (_: Exception) {
                                payloadStr.toDoubleOrNull() ?: payloadStr
                            }
                        }
                        data.add(JsonObject()
                            .put("value", value)
                            .put("quality", "GOOD")
                            .put("timestamp", Instant.ofEpochMilli(timestampMs).toString()))
                    }
                    result.put(eid, JsonObject().put("data", data))
                } catch (e: Exception) {
                    logger.warning("I3X history error for '$eid': ${e.message}")
                }
            }
        }
        ok(ctx, result)
    }

    // --- Update Handlers ---

    private fun handleUpdateValue(ctx: RoutingContext) {
        val rawPath = ctx.request().path() // /i3x/objects/<elementId>/value
        val elementId = rawPath.removePrefix("/i3x/objects/").removeSuffix("/value")
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
        subscriptions.values.forEach {
            result.add(JsonObject()
                .put("subscriptionId", it.id)
                .put("created", it.created.toString()))
        }
        ok(ctx, JsonObject().put("subscriptionIds", result))
    }

    private fun handleCreateSubscription(ctx: RoutingContext) {
        val id = UUID.randomUUID().toString()
        val sub = I3xSubscription(id)
        subscriptions[id] = sub
        ok(ctx, JsonObject()
            .put("subscriptionId", id)
            .put("created", sub.created.toString()))
    }

    private fun handleGetSubscription(ctx: RoutingContext) {
        val sub = subscriptions[ctx.pathParam("id")]
            ?: return err(ctx, 404, "Subscription not found")
        ok(ctx, JsonObject()
            .put("subscriptionId", sub.id)
            .put("created", sub.created.toString())
            .put("registeredTopics", JsonArray(sub.topics)))
    }

    private fun handleDeleteSubscription(ctx: RoutingContext) {
        val id = ctx.pathParam("id")
        val sub = subscriptions.remove(id)
        if (sub != null) {
            sessionHandler.unregisterMessageListener("i3x-$id")
        }
        ok(ctx, JsonObject().put("message", "Subscription deleted"))
    }

    private fun handleRegisterTopics(ctx: RoutingContext) {
        val sub = subscriptions[ctx.pathParam("id")]
            ?: return err(ctx, 404, "Subscription not found")
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val elementIds = body.getJsonArray("elementIds") ?: JsonArray()
        val maxDepth = body.getInteger("maxDepth") ?: 1
        sub.maxDepth = maxDepth
        for (i in 0 until elementIds.size()) {
            val eid = elementIds.getString(i) ?: continue
            if (!sub.topics.contains(eid)) sub.topics.add(eid)
        }
        rewireMessageListener(sub)
        ok(ctx, JsonObject().put("message", "Topics registered"))
    }

    private fun handleUnregisterTopics(ctx: RoutingContext) {
        val sub = subscriptions[ctx.pathParam("id")]
            ?: return err(ctx, 404, "Subscription not found")
        val elementIds = ctx.body().asJsonObject()?.getJsonArray("elementIds") ?: JsonArray()
        for (i in 0 until elementIds.size()) sub.topics.remove(elementIds.getString(i))
        rewireMessageListener(sub)
        ok(ctx, JsonObject().put("message", "Topics unregistered"))
    }

    /**
     * Wire or re-wire the message listener for a subscription.
     * Uses SessionHandler.registerMessageListener() (same pattern as GraphQL subscriptions)
     * to receive real-time MQTT messages matching the registered elementIds.
     */
    private fun rewireMessageListener(sub: I3xSubscription) {
        val listenerId = "i3x-${sub.id}"
        sessionHandler.unregisterMessageListener(listenerId)

        if (sub.topics.isEmpty()) return

        // Build MQTT topic filters from elementIds
        val topicFilters = mutableListOf<String>()
        val groupsByTopic = mutableMapOf<String, String>() // mqttTopic -> groupName
        for (eid in sub.topics) {
            val (groupName, topicName) = parseElementId(eid) ?: continue
            if (!topicFilters.contains(topicName)) topicFilters.add(topicName)
            groupsByTopic[topicName] = groupName
            if (sub.maxDepth != 1) {
                val wildcard = "$topicName/#"
                if (!topicFilters.contains(wildcard)) topicFilters.add(wildcard)
            }
        }

        if (topicFilters.isEmpty()) return

        sessionHandler.registerMessageListener(listenerId, topicFilters) { message ->
            // Resolve group name from the message topic
            val groupName = groupsByTopic[message.topicName]
                ?: groupsByTopic.entries.firstOrNull { message.topicName.startsWith("${it.key}/") }?.value
                ?: return@registerMessageListener

            val elementId = "$groupName/${message.topicName}"
            // Each event is {elementId: {data: [VQT]}} — keyed by elementId
            val event = JsonObject()
                .put(elementId, JsonObject().put("data", JsonArray().add(messageToVqt(message))))

            // Publish to event bus for SSE streaming
            vertx.eventBus().publish("mq.i3x.sub.${sub.id}", event)

            // Also queue for sync endpoint
            sub.pendingQueue.addLast(event)
            while (sub.pendingQueue.size > 10000) sub.pendingQueue.removeFirst()
        }
    }

    /**
     * SSE stream endpoint — opens a Server-Sent Events stream for real-time updates.
     * Messages are routed via the internal Vert.X event bus address "mq.i3x.sub.<id>".
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
            // Wrap in array: [{elementId: {data: [VQT]}}]
            if (!response.ended()) response.write("data: ${JsonArray().add(msg.body()).encode()}\n\n")
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
            response.write("data: ${JsonArray().add(sub.pendingQueue.removeFirst()).encode()}\n\n")
        }
    }

    private fun handleSync(ctx: RoutingContext) {
        val sub = subscriptions[ctx.pathParam("id")]
            ?: return err(ctx, 404, "Subscription not found")

        // Group pending messages by elementId and aggregate VQTs
        // Return array: [{elementId: {data: [VQT]}}, ...]
        val grouped = LinkedHashMap<String, JsonArray>()
        while (sub.pendingQueue.isNotEmpty()) {
            val event = sub.pendingQueue.removeFirst()
            for (key in event.fieldNames()) {
                val vqts = event.getJsonObject(key)?.getJsonArray("data") ?: continue
                val arr = grouped.getOrPut(key) { JsonArray() }
                for (j in 0 until vqts.size()) arr.add(vqts.getValue(j))
            }
        }

        val result = JsonArray()
        for ((eid, data) in grouped) {
            result.add(JsonObject().put(eid, JsonObject().put("data", data)))
        }
        ok(ctx, result)
    }

    // --- Authentication ---

    private fun validateAuthentication(ctx: RoutingContext): Boolean {
        if (!userManager.isUserManagementEnabled()) return true

        val authHeader = ctx.request().getHeader("Authorization")
        if (authHeader == null) {
            if (userManager.isAnonymousEnabled()) {
                ctx.put("i3x_username", "Anonymous")
                return true
            }
            ctx.response().setStatusCode(401)
                .putHeader("Content-Type", "application/json")
                .putHeader("WWW-Authenticate", "Basic realm=\"MonsterMQ I3X API\"")
                .end(JsonObject().put("error", "Authentication required").encode())
            return false
        }

        if (authHeader.startsWith("Bearer ", ignoreCase = true)) {
            val token = authHeader.substring(7)
            val username = JwtService.extractUsername(token)
            if (username != null && !JwtService.isTokenExpired(token)) {
                ctx.put("i3x_username", username)
                return true
            }
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
                            ctx.put("i3x_username", parts[0])
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
