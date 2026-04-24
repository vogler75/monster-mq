package at.rocworks.extensions

import at.rocworks.Utils
import at.rocworks.Version
import at.rocworks.auth.UserManager
import at.rocworks.data.BrokerMessage
import at.rocworks.extensions.graphql.JwtService
import at.rocworks.handlers.ArchiveGroup
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.handlers.SessionHandler
import at.rocworks.schema.CompiledNamespaceEntry
import at.rocworks.schema.JsonSchemaValidator
import at.rocworks.schema.TopicSchemaPolicyCache
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.IMessageArchiveExtended
import at.rocworks.stores.IMessageStore
import at.rocworks.stores.MessageStoreType
import at.rocworks.stores.PayloadDecoder

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
import java.util.concurrent.atomic.AtomicLong

/**
 * i3X v1 API server.
 *
 * Exposes MonsterMQ's topic tree, current values, history, and subscriptions
 * under `/i3x/v1` using the v1 envelope and address-space model described in
 * `dev/plans/I3X_SPEC.md`.
 */
class I3xServer(
    private val host: String,
    private val port: Int,
    private val brokerName: String,
    private val archiveHandler: ArchiveHandler,
    private val sessionHandler: SessionHandler,
    private val deviceConfigStore: IDeviceConfigStore?,
    private val userManager: UserManager,
) : AbstractVerticle() {

    private val logger = Utils.getLogger(this::class.java)

    companion object {
        const val SPEC_VERSION = "1.0-beta"
        const val SERVER_NAME = "monstermq-i3x"
        const val BASE_NAMESPACE_URI = "http://i3x.dev/base"

        // Built-in (synthetic) ObjectType element IDs.
        const val TYPE_TOPIC_FOLDER = "TopicFolder"
        const val TYPE_JSON_OBJECT = "JsonObject"
        const val TYPE_JSON_ARRAY = "JsonArray"
        const val TYPE_NUMBER = "Number"
        const val TYPE_STRING = "String"
        const val TYPE_BOOLEAN = "Boolean"
        const val TYPE_BINARY = "Binary"

        // Built-in relationship type element IDs.
        const val REL_HAS_PARENT = "HasParent"
        const val REL_HAS_CHILDREN = "HasChildren"
        const val REL_HAS_COMPONENT = "HasComponent"
        const val REL_COMPONENT_OF = "ComponentOf"

        private val SYNTHETIC_TYPES = listOf(
            TYPE_TOPIC_FOLDER to "Intermediate topic level with children but no retained value",
            TYPE_JSON_OBJECT to "JSON object payload",
            TYPE_JSON_ARRAY to "JSON array payload",
            TYPE_NUMBER to "Numeric payload",
            TYPE_STRING to "UTF-8 text payload",
            TYPE_BOOLEAN to "Boolean payload",
            TYPE_BINARY to "Binary payload (base64)"
        )

        private val REL_TYPES = listOf(
            Triple(REL_HAS_PARENT, REL_HAS_CHILDREN, "MQTT topic hierarchy: parent topic level"),
            Triple(REL_HAS_CHILDREN, REL_HAS_PARENT, "MQTT topic hierarchy: child topic levels"),
            Triple(REL_HAS_COMPONENT, REL_COMPONENT_OF, "JSON-object payload composition: fields of a JSON payload"),
            Triple(REL_COMPONENT_OF, REL_HAS_COMPONENT, "JSON-object payload composition: owning parent of a field")
        )
    }

    private val basePath = "/i3x/v1"
    private val baseNamespaceUri = "mqtt://$brokerName/"

    // --- Subscriptions (v1) ---

    private data class I3xSubscription(
        val clientId: String?,
        val subscriptionId: String,
        val displayName: String?,
        val created: Instant = Instant.now(),
        val registeredIds: MutableSet<String> = ConcurrentHashMap.newKeySet(),
        val topicFilters: MutableSet<String> = ConcurrentHashMap.newKeySet(),
        val pendingQueue: ArrayDeque<JsonObject> = ArrayDeque(),
        var nextSequence: AtomicLong = AtomicLong(1L),
        var maxDepth: Int = 1
    )

    // subscriptionId -> subscription
    private val subscriptions = ConcurrentHashMap<String, I3xSubscription>()

    override fun start(startPromise: Promise<Void>) {
        logger.fine("Starting I3X v1 API server")

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

        // /info is unauthenticated discovery.
        router.get("$basePath/info").handler(::handleInfo)

        // All other routes require authentication.
        router.route("$basePath/*").handler { ctx ->
            if (ctx.request().path() == "$basePath/info") {
                ctx.next(); return@handler
            }
            if (logger.isLoggable(java.util.logging.Level.FINE)) {
                val body = if (ctx.request().method().name() == "POST") ctx.body()?.asString()?.take(200) else ""
                logger.fine("I3X ${ctx.request().method()} ${ctx.request().uri()} $body")
            }
            if (!validateAuthentication(ctx)) return@handler
            ctx.next()
        }

        // Explore
        router.get("$basePath/namespaces").handler(::handleNamespaces)
        router.get("$basePath/objecttypes").handler(::handleObjectTypes)
        router.post("$basePath/objecttypes/query").handler(::handleObjectTypesQuery)
        router.get("$basePath/relationshiptypes").handler(::handleRelationshipTypes)
        router.post("$basePath/relationshiptypes/query").handler(::handleRelationshipTypesQuery)
        router.get("$basePath/objects").handler(::handleObjects)
        router.post("$basePath/objects/list").handler(::handleObjectsList)
        router.post("$basePath/objects/related").handler(::handleObjectsRelated)

        // Query
        router.post("$basePath/objects/value").handler(::handleObjectsValue)
        router.post("$basePath/objects/history").handler(::handleObjectsHistory)

        // Update — elementId can contain '/', match greedily.
        router.putWithRegex("$basePath/objects/(?<eid>.+)/value").handler(::handleUpdateValue)
        router.putWithRegex("$basePath/objects/(?<eid>.+)/history").handler(::handleUpdateHistory)
        router.getWithRegex("$basePath/objects/(?<eid>.+)/history").handler(::handleGetHistorySingle)

        // Subscriptions (v1 — all POST, clientId-scoped).
        router.post("$basePath/subscriptions").handler(::handleCreateSubscription)
        router.post("$basePath/subscriptions/register").handler(::handleRegisterTopics)
        router.post("$basePath/subscriptions/unregister").handler(::handleUnregisterTopics)
        router.post("$basePath/subscriptions/stream").handler(::handleStream)
        router.post("$basePath/subscriptions/sync").handler(::handleSync)
        router.post("$basePath/subscriptions/list").handler(::handleSubscriptionsList)
        router.post("$basePath/subscriptions/delete").handler(::handleSubscriptionsDelete)

        vertx.createHttpServer(HttpServerOptions().setPort(port).setHost(host))
            .requestHandler(router)
            .listen()
            .onSuccess { server ->
                logger.info("I3X v1 API Server started on port ${server.actualPort()}$basePath")
                startPromise.complete()
            }
            .onFailure { error ->
                logger.severe("I3X v1 API Server failed to start: $error")
                startPromise.fail(error)
            }
    }

    override fun stop() {
        for (sub in subscriptions.values) {
            sessionHandler.unregisterMessageListener("i3x-${sub.subscriptionId}")
        }
        subscriptions.clear()
    }

    // ---------------------------------------------------------------------
    //  Envelope helpers
    // ---------------------------------------------------------------------

    private fun sendOk(ctx: RoutingContext, result: Any?) {
        val body = JsonObject().put("success", true).put("result", result)
        ctx.response().setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(body.encode())
    }

    private fun sendError(ctx: RoutingContext, status: Int, message: String) {
        val body = JsonObject()
            .put("success", false)
            .put("error", JsonObject().put("code", status).put("message", message))
        ctx.response().setStatusCode(status)
            .putHeader("Content-Type", "application/json")
            .end(body.encode())
    }

    private fun sendBulk(ctx: RoutingContext, items: List<BulkItem>) {
        val results = JsonArray()
        var anyFailed = false
        for (item in items) {
            val entry = JsonObject()
                .put("success", item.success)
                .put("elementId", item.elementId)
            if (item.success) {
                entry.put("result", item.result)
            } else {
                anyFailed = true
                entry.put(
                    "error",
                    JsonObject()
                        .put("code", item.errorCode ?: 500)
                        .put("message", item.errorMessage ?: "Unknown error")
                )
            }
            results.add(entry)
        }
        val body = JsonObject().put("success", !anyFailed).put("results", results)
        ctx.response().setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(body.encode())
    }

    private data class BulkItem(
        val elementId: String,
        val success: Boolean,
        val result: Any? = null,
        val errorCode: Int? = null,
        val errorMessage: String? = null
    ) {
        companion object {
            fun ok(elementId: String, result: Any?): BulkItem = BulkItem(elementId, true, result)
            fun notFound(elementId: String, message: String = "Object not found"): BulkItem =
                BulkItem(elementId, false, errorCode = 404, errorMessage = message)
            fun error(elementId: String, code: Int, message: String): BulkItem =
                BulkItem(elementId, false, errorCode = code, errorMessage = message)
        }
    }

    // ---------------------------------------------------------------------
    //  /info
    // ---------------------------------------------------------------------

    private fun handleInfo(ctx: RoutingContext) {
        val capabilities = JsonObject()
            .put("query", JsonObject().put("history", true))
            .put("update", JsonObject().put("current", true).put("history", true))
            .put("subscribe", JsonObject().put("stream", true))
        val result = JsonObject()
            .put("specVersion", SPEC_VERSION)
            .put("serverVersion", Version.getVersion())
            .put("serverName", SERVER_NAME)
            .put("capabilities", capabilities)
        sendOk(ctx, result)
    }

    // ---------------------------------------------------------------------
    //  Namespaces
    // ---------------------------------------------------------------------

    private data class NamespaceEntry(val uri: String, val displayName: String)

    private fun collectNamespaces(): List<NamespaceEntry> {
        val result = mutableListOf<NamespaceEntry>()
        // Base broker namespace + synthetic.
        result.add(NamespaceEntry(baseNamespaceUri, brokerName))
        result.add(NamespaceEntry(BASE_NAMESPACE_URI, "i3x-base"))

        val devices = loadDevices()
        devices
            .filter { it.type == DeviceConfig.DEVICE_TYPE_TOPIC_NAMESPACE && it.enabled }
            .forEach { device ->
                val prefix = device.namespace.ifBlank { device.name }
                result.add(NamespaceEntry("mqtt://$brokerName/$prefix", device.name))
            }
        return result
    }

    private fun namespaceUriForPolicy(policyName: String, devices: List<DeviceConfig>): String {
        val owner = devices.firstOrNull {
            it.type == DeviceConfig.DEVICE_TYPE_TOPIC_NAMESPACE &&
                it.enabled &&
                it.config.getString("schemaPolicyName") == policyName
        }
        return if (owner != null) {
            val prefix = owner.namespace.ifBlank { owner.name }
            "mqtt://$brokerName/$prefix"
        } else {
            BASE_NAMESPACE_URI
        }
    }

    private fun handleNamespaces(ctx: RoutingContext) {
        val arr = JsonArray()
        collectNamespaces().forEach {
            arr.add(JsonObject().put("uri", it.uri).put("displayName", it.displayName))
        }
        sendOk(ctx, arr)
    }

    // ---------------------------------------------------------------------
    //  ObjectTypes
    // ---------------------------------------------------------------------

    private fun buildObjectType(
        elementId: String,
        namespaceUri: String,
        schema: JsonObject
    ): JsonObject = JsonObject()
        .put("elementId", elementId)
        .put("displayName", elementId)
        .put("namespaceUri", namespaceUri)
        .put("sourceTypeId", elementId)
        .put("schema", schema)

    private fun syntheticSchema(elementId: String): JsonObject = when (elementId) {
        TYPE_JSON_OBJECT -> JsonObject().put("type", "object")
        TYPE_JSON_ARRAY -> JsonObject().put("type", "array")
        TYPE_NUMBER -> JsonObject().put("type", "number")
        TYPE_STRING -> JsonObject().put("type", "string")
        TYPE_BOOLEAN -> JsonObject().put("type", "boolean")
        TYPE_BINARY -> JsonObject()
            .put("type", "object")
            .put("properties", JsonObject().put("payload_base64", JsonObject().put("type", "string")))
        TYPE_TOPIC_FOLDER -> JsonObject()
            .put("type", "object")
            .put("description", "Intermediate topic level with children but no retained value")
        else -> JsonObject()
    }

    private fun collectObjectTypes(): List<JsonObject> {
        val devices = loadDevices()
        val out = mutableListOf<JsonObject>()
        // Synthetic types.
        SYNTHETIC_TYPES.forEach { (id, _) ->
            out.add(buildObjectType(id, BASE_NAMESPACE_URI, syntheticSchema(id)))
        }
        // Explicit types from TopicSchema-Policy devices.
        devices
            .filter { it.type == DeviceConfig.DEVICE_TYPE_TOPIC_SCHEMA_POLICY }
            .forEach { device ->
                val schema = device.config.getJsonObject("jsonSchema") ?: JsonObject()
                val ns = namespaceUriForPolicy(device.name, devices)
                out.add(buildObjectType(device.name, ns, schema))
            }
        return out
    }

    private fun handleObjectTypes(ctx: RoutingContext) {
        val nsUri = ctx.queryParam("namespaceUri").firstOrNull()
        val all = collectObjectTypes()
        val filtered = if (nsUri != null) all.filter { it.getString("namespaceUri") == nsUri } else all
        val arr = JsonArray()
        filtered.forEach { arr.add(it) }
        sendOk(ctx, arr)
    }

    private fun handleObjectTypesQuery(ctx: RoutingContext) {
        val ids = ctx.body().asJsonObject()?.getJsonArray("elementIds") ?: JsonArray()
        val byId = collectObjectTypes().associateBy { it.getString("elementId") }
        val items = (0 until ids.size()).map { i ->
            val id = ids.getString(i) ?: return@map BulkItem.notFound("", "Missing elementId")
            val type = byId[id] ?: return@map BulkItem.notFound(id, "ObjectType not found")
            BulkItem.ok(id, type)
        }
        sendBulk(ctx, items)
    }

    // ---------------------------------------------------------------------
    //  RelationshipTypes
    // ---------------------------------------------------------------------

    private fun buildRelType(id: String, reverse: String): JsonObject = JsonObject()
        .put("elementId", id)
        .put("displayName", id)
        .put("namespaceUri", BASE_NAMESPACE_URI)
        .put("relationshipId", id)
        .put("reverseOf", reverse)

    private fun handleRelationshipTypes(ctx: RoutingContext) {
        val nsUri = ctx.queryParam("namespaceUri").firstOrNull()
        val all = REL_TYPES.map { buildRelType(it.first, it.second) }
        val filtered = if (nsUri != null) all.filter { it.getString("namespaceUri") == nsUri } else all
        val arr = JsonArray()
        filtered.forEach { arr.add(it) }
        sendOk(ctx, arr)
    }

    private fun handleRelationshipTypesQuery(ctx: RoutingContext) {
        val ids = ctx.body().asJsonObject()?.getJsonArray("elementIds") ?: JsonArray()
        val byId = REL_TYPES.associate { it.first to buildRelType(it.first, it.second) }
        val items = (0 until ids.size()).map { i ->
            val id = ids.getString(i) ?: return@map BulkItem.notFound("", "Missing elementId")
            val t = byId[id] ?: return@map BulkItem.notFound(id, "RelationshipType not found")
            BulkItem.ok(id, t)
        }
        sendBulk(ctx, items)
    }

    // ---------------------------------------------------------------------
    //  Objects
    // ---------------------------------------------------------------------

    /** Topic-tree snapshot used to answer structural queries. */
    private data class TopicTreeSnapshot(
        /** All topic levels (including intermediates). */
        val allLevels: Set<String>,
        /** Subset of [allLevels] that have a retained value. */
        val withValue: Set<String>,
        /** Subset of [allLevels] that have at least one child level. */
        val hasChildren: Set<String>,
        /** Root topic levels (top-level segments). */
        val roots: Set<String>,
        /** Retained messages, keyed by topic name — pre-fetched to avoid per-topic DB round-trips. */
        val messages: Map<String, BrokerMessage>
    )

    /**
     * Build a full snapshot of the address-space from archive groups' last-value stores.
     *
     * Only in-memory last-value stores (`MessageStoreType.MEMORY`) are consulted —
     * i3X reads a snapshot of the topic tree on every request, so scanning a
     * database-backed store on every call would be prohibitively expensive.
     * Configure an archive group with `lastValType: MEMORY` to expose topics
     * through i3X.
     */
    private fun snapshotRetainedTopics(): TopicTreeSnapshot {
        val messages = mutableMapOf<String, BrokerMessage>()
        val storeTopics = mutableSetOf<String>()

        fun ingest(store: IMessageStore) {
            try {
                store.findMatchingMessages("#") { msg ->
                    if (msg.topicName.isNotEmpty()) {
                        storeTopics.add(msg.topicName)
                        messages.putIfAbsent(msg.topicName, msg)
                    }
                    storeTopics.size < 100_000
                }
            } catch (e: Exception) {
                logger.warning("I3X: findMatchingMessages('#') failed on ${store.getName()}: ${e.message}")
            }
        }

        archiveHandler.getDeployedArchiveGroups().values.forEach { group ->
            val store = group.lastValStore ?: return@forEach
            if (store.getType() != MessageStoreType.MEMORY) return@forEach
            ingest(store)
        }

        val allLevels = mutableSetOf<String>()
        val hasChildren = mutableSetOf<String>()
        val roots = mutableSetOf<String>()
        for (topic in storeTopics) {
            if (topic.isEmpty()) continue
            val parts = topic.split("/")
            for (j in 1..parts.size) {
                val prefix = parts.subList(0, j).joinToString("/")
                allLevels.add(prefix)
                if (j == 1) roots.add(prefix)
                if (j > 1) {
                    val parent = parts.subList(0, j - 1).joinToString("/")
                    hasChildren.add(parent)
                }
            }
        }
        return TopicTreeSnapshot(allLevels, storeTopics, hasChildren, roots, messages)
    }

    private fun findRetainedMessage(snapshot: TopicTreeSnapshot, topic: String): BrokerMessage? =
        snapshot.messages[topic]

    private fun findMatchingSchemaPolicy(topic: String): CompiledNamespaceEntry? {
        return TopicSchemaPolicyCache.getInstance()?.matchNamespace(topic)
    }

    /** Decide the `typeElementId` for a topic based on its retained value and sub-topic structure. */
    private fun typeElementIdFor(
        topic: String,
        snapshot: TopicTreeSnapshot,
        msg: BrokerMessage?
    ): String {
        // If a schema policy claims this topic, use the policy name.
        findMatchingSchemaPolicy(topic)?.let { return it.schemaPolicyName }
        if (msg == null) return TYPE_TOPIC_FOLDER
        val decoded = PayloadDecoder.decode(msg.payload)
        return when (val v = decoded.payload) {
            is JsonObject -> TYPE_JSON_OBJECT
            is JsonArray -> TYPE_JSON_ARRAY
            is Number -> TYPE_NUMBER
            is Boolean -> TYPE_BOOLEAN
            is String -> when {
                v.equals("true", ignoreCase = true) || v.equals("false", ignoreCase = true) -> TYPE_BOOLEAN
                v.toDoubleOrNull() != null -> TYPE_NUMBER
                else -> TYPE_STRING
            }
            null -> if (decoded.base64 != null) TYPE_BINARY else TYPE_TOPIC_FOLDER
            else -> TYPE_STRING
        }
    }

    private fun isCompositionTopic(topic: String, snapshot: TopicTreeSnapshot, msg: BrokerMessage?): Boolean {
        if (snapshot.hasChildren.contains(topic)) return true
        if (msg == null) return true // folder-only
        val decoded = PayloadDecoder.decode(msg.payload)
        return decoded.payload is JsonObject || decoded.payload is JsonArray
    }

    private fun buildObjectJson(
        topic: String,
        snapshot: TopicTreeSnapshot,
        includeMetadata: Boolean
    ): JsonObject {
        val msg = findRetainedMessage(snapshot, topic)
        val typeId = typeElementIdFor(topic, snapshot, msg)
        val parent = topic.substringBeforeLast("/", "")
        val parentId: Any? = if (parent.isEmpty()) null else parent
        val obj = JsonObject()
            .put("elementId", topic)
            .put("displayName", topic.substringAfterLast("/"))
            .put("typeElementId", typeId)
            .put("parentId", parentId)
            .put("isComposition", isCompositionTopic(topic, snapshot, msg))
            .put("isExtended", false)
        if (includeMetadata) {
            val policy = findMatchingSchemaPolicy(topic)
            val metadata = JsonObject()
                .put(
                    "typeNamespaceUri",
                    if (policy != null)
                        namespaceUriForPolicy(policy.schemaPolicyName, loadDevices())
                    else BASE_NAMESPACE_URI
                )
                .put("sourceTypeId", typeId)
                .put("description", null)
                .put("relationships", null)
                .put("extendedAttributes", null)
                .put("system", null)
            obj.put("metadata", metadata)
        }
        return obj
    }

    private fun handleObjects(ctx: RoutingContext) {
        val typeIdParam = ctx.queryParam("typeElementId").firstOrNull()
        val includeMetadata =
            ctx.queryParam("includeMetadata").firstOrNull()?.toBooleanStrictOrNull() ?: false
        val rootOnly = ctx.queryParam("root").firstOrNull()?.toBooleanStrictOrNull() ?: false
        val parentIdParam = ctx.queryParam("parentId").firstOrNull()

        vertx.executeBlocking(java.util.concurrent.Callable {
            val snapshot = snapshotRetainedTopics()
            val candidates: Set<String> = when {
                rootOnly -> snapshot.roots
                parentIdParam != null -> {
                    val prefix = "$parentIdParam/"
                    snapshot.allLevels
                        .filter { it.startsWith(prefix) && !it.removePrefix(prefix).contains("/") }
                        .toSet()
                }
                else -> snapshot.allLevels
            }
            val arr = JsonArray()
            for (topic in candidates.sorted()) {
                val obj = buildObjectJson(topic, snapshot, includeMetadata)
                if (typeIdParam != null && obj.getString("typeElementId") != typeIdParam) continue
                arr.add(obj)
            }
            arr
        }).onSuccess { sendOk(ctx, it) }
            .onFailure { sendError(ctx, 500, "Objects query failed: ${it.message}") }
    }

    private fun handleObjectsList(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val ids = body.getJsonArray("elementIds") ?: JsonArray()
        val includeMetadata = body.getBoolean("includeMetadata", false)
        vertx.executeBlocking(java.util.concurrent.Callable {
            val snapshot = snapshotRetainedTopics()
            (0 until ids.size()).map { i ->
                val id = ids.getString(i)
                when {
                    id.isNullOrEmpty() -> BulkItem.notFound("", "Missing elementId")
                    !snapshot.allLevels.contains(id) -> BulkItem.notFound(id)
                    else -> BulkItem.ok(id, buildObjectJson(id, snapshot, includeMetadata))
                }
            }
        }).onSuccess { @Suppress("UNCHECKED_CAST") sendBulk(ctx, it as List<BulkItem>) }
            .onFailure { sendError(ctx, 500, "Objects list failed: ${it.message}") }
    }

    private fun handleObjectsRelated(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val ids = body.getJsonArray("elementIds") ?: JsonArray()
        val relationshipType = body.getString("relationshipType")
        val includeMetadata = body.getBoolean("includeMetadata", false)

        val supported = setOf(REL_HAS_CHILDREN, REL_HAS_PARENT, REL_HAS_COMPONENT, REL_COMPONENT_OF)
        if (relationshipType != null && relationshipType !in supported) {
            sendError(ctx, 400, "Unsupported relationshipType: $relationshipType")
            return
        }

        vertx.executeBlocking(java.util.concurrent.Callable {
            val snapshot = snapshotRetainedTopics()
            (0 until ids.size()).map { i ->
                val id = ids.getString(i)
                if (id.isNullOrEmpty()) return@map BulkItem.notFound("", "Missing elementId")
                if (!snapshot.allLevels.contains(id)) return@map BulkItem.notFound(id)

                val results = JsonArray()
                if (relationshipType == null || relationshipType == REL_HAS_CHILDREN) {
                    childTopics(id, snapshot).forEach { child ->
                        results.add(
                            JsonObject()
                                .put("sourceRelationship", REL_HAS_CHILDREN)
                                .put("object", buildObjectJson(child, snapshot, includeMetadata))
                        )
                    }
                }
                if (relationshipType == null || relationshipType == REL_HAS_PARENT) {
                    val parent = id.substringBeforeLast("/", "")
                    if (parent.isNotEmpty() && snapshot.allLevels.contains(parent)) {
                        results.add(
                            JsonObject()
                                .put("sourceRelationship", REL_HAS_PARENT)
                                .put("object", buildObjectJson(parent, snapshot, includeMetadata))
                        )
                    }
                }
                if (relationshipType == null || relationshipType == REL_HAS_COMPONENT) {
                    jsonFieldComponents(snapshot, id).forEach { child ->
                        results.add(
                            JsonObject()
                                .put("sourceRelationship", REL_HAS_COMPONENT)
                                .put("object", buildObjectJson(child, snapshot, includeMetadata))
                        )
                    }
                }
                if (relationshipType == REL_COMPONENT_OF) {
                    val parent = id.substringBeforeLast("/", "")
                    if (parent.isNotEmpty() && snapshot.allLevels.contains(parent)) {
                        results.add(
                            JsonObject()
                                .put("sourceRelationship", REL_COMPONENT_OF)
                                .put("object", buildObjectJson(parent, snapshot, includeMetadata))
                        )
                    }
                }
                BulkItem.ok(id, results)
            }
        }).onSuccess { @Suppress("UNCHECKED_CAST") sendBulk(ctx, it as List<BulkItem>) }
            .onFailure { sendError(ctx, 500, "Related query failed: ${it.message}") }
    }

    private fun childTopics(parent: String, snapshot: TopicTreeSnapshot): List<String> {
        val prefix = "$parent/"
        return snapshot.allLevels
            .filter { it.startsWith(prefix) && !it.removePrefix(prefix).contains("/") }
            .sorted()
    }

    private fun jsonFieldComponents(snapshot: TopicTreeSnapshot, topic: String): List<String> {
        val msg = findRetainedMessage(snapshot, topic) ?: return emptyList()
        val decoded = PayloadDecoder.decode(msg.payload)
        val obj = decoded.payload as? JsonObject ?: return emptyList()
        // Component ids are synthesized as "{topic}/{field}". They may or may not exist as real
        // sub-topics; the v1 spec allows the server to expose JSON fields as virtual components.
        return obj.fieldNames().map { "$topic/$it" }.sorted()
    }

    // ---------------------------------------------------------------------
    //  Values
    // ---------------------------------------------------------------------

    private fun qualityFor(topic: String, msg: BrokerMessage?): String {
        if (msg == null) return "GoodNoData"
        val decoded = try { PayloadDecoder.decode(msg.payload) } catch (_: Exception) { return "Bad" }
        if (decoded.payload == null && decoded.base64 == null) return "GoodNoData"
        // If a schema policy governs this topic and payload violates it, report Uncertain.
        findMatchingSchemaPolicy(topic)?.let { entry ->
            val payloadText = when (val p = decoded.payload) {
                is JsonObject -> p.encode()
                is JsonArray -> p.encode()
                is String -> p
                else -> null
            }
            if (payloadText != null) {
                try {
                    val res = entry.validator.validate(payloadText)
                    if (!res.valid) return "Uncertain"
                } catch (_: Exception) { /* fall through */ }
            }
        }
        return "Good"
    }

    private fun decodedValue(msg: BrokerMessage?): Any? {
        if (msg == null) return null
        val decoded = try { PayloadDecoder.decode(msg.payload) } catch (_: Exception) { return null }
        if (decoded.payload != null) return decoded.payload
        if (decoded.base64 != null) return JsonObject().put("payload_base64", decoded.base64)
        return null
    }

    private fun buildVqt(topic: String, msg: BrokerMessage?): JsonObject {
        val obj = JsonObject()
            .put("value", decodedValue(msg))
            .put("quality", qualityFor(topic, msg))
            .put("timestamp", (msg?.time ?: Instant.now()).toString())
        return obj
    }

    private fun buildValueResult(
        topic: String,
        snapshot: TopicTreeSnapshot,
        maxDepth: Int,
        currentDepth: Int
    ): JsonObject {
        val msg = findRetainedMessage(snapshot, topic)
        val vqt = buildVqt(topic, msg)
        val isComposition = isCompositionTopic(topic, snapshot, msg)
        vqt.put("isComposition", isComposition)
        if (!isComposition) return vqt

        val canRecurse = maxDepth == 0 || currentDepth < maxDepth
        if (!canRecurse) return vqt

        val components = JsonObject()
        // Sub-topic children.
        childTopics(topic, snapshot).forEach { child ->
            components.put(child, buildValueResult(child, snapshot, maxDepth, currentDepth + 1))
        }
        // JSON field components (keyed with synthesized elementId "topic/field").
        val decoded = msg?.let { PayloadDecoder.decode(it.payload) }
        val jsonObj = decoded?.payload as? JsonObject
        jsonObj?.let {
            for (field in it.fieldNames()) {
                val childId = "$topic/$field"
                if (components.containsKey(childId)) continue
                val childVal = it.getValue(field)
                val childIsComp = childVal is JsonObject || childVal is JsonArray
                val childJson = JsonObject()
                    .put("isComposition", childIsComp)
                    .put("value", childVal)
                    .put("quality", "Good")
                    .put("timestamp", (msg.time).toString())
                components.put(childId, childJson)
            }
        }
        vqt.put("components", components)
        return vqt
    }

    private fun handleObjectsValue(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val ids = body.getJsonArray("elementIds") ?: JsonArray()
        val maxDepth = body.getInteger("maxDepth") ?: 1
        vertx.executeBlocking(java.util.concurrent.Callable {
            val snapshot = snapshotRetainedTopics()
            (0 until ids.size()).map { i ->
                val id = ids.getString(i)
                when {
                    id.isNullOrEmpty() -> BulkItem.notFound("", "Missing elementId")
                    !snapshot.allLevels.contains(id) -> BulkItem.notFound(id)
                    else -> BulkItem.ok(id, buildValueResult(id, snapshot, maxDepth, 1))
                }
            }
        }).onSuccess { @Suppress("UNCHECKED_CAST") sendBulk(ctx, it as List<BulkItem>) }
            .onFailure { sendError(ctx, 500, "Value query failed: ${it.message}") }
    }

    private fun handleUpdateValue(ctx: RoutingContext) {
        val elementId = extractElementId(ctx, "/value") ?: return
        if (elementId.isEmpty()) return sendError(ctx, 400, "Missing elementId")
        val bodyStr = ctx.body().asString() ?: ""
        // v1 spec accepts `{ value, quality?, timestamp? }` but this project currently
        // writes the raw value; accept either.
        val payloadBytes: ByteArray = try {
            val maybe = JsonObject(bodyStr)
            if (maybe.containsKey("value")) {
                val v = maybe.getValue("value")
                when (v) {
                    is JsonObject -> v.encode().toByteArray(Charsets.UTF_8)
                    is JsonArray -> v.encode().toByteArray(Charsets.UTF_8)
                    null -> ByteArray(0)
                    else -> v.toString().toByteArray(Charsets.UTF_8)
                }
            } else bodyStr.toByteArray(Charsets.UTF_8)
        } catch (_: Exception) {
            bodyStr.toByteArray(Charsets.UTF_8)
        }

        val msg = BrokerMessage(
            messageId = 0,
            topicName = elementId,
            payload = payloadBytes,
            qosLevel = 0,
            isRetain = true,
            isDup = false,
            isQueued = false,
            clientId = "i3x-v1"
        )
        sessionHandler.publishMessage(msg)
        sendOk(
            ctx,
            JsonObject()
                .put("elementId", elementId)
                .put("status", "published")
        )
    }

    private fun extractElementId(ctx: RoutingContext, suffix: String): String? {
        val path = ctx.request().path()
        val prefix = "$basePath/objects/"
        if (!path.startsWith(prefix) || !path.endsWith(suffix)) {
            sendError(ctx, 400, "Invalid path")
            return null
        }
        val encoded = path.substring(prefix.length, path.length - suffix.length)
        return try {
            java.net.URLDecoder.decode(encoded, Charsets.UTF_8)
        } catch (_: Exception) {
            encoded
        }
    }

    // ---------------------------------------------------------------------
    //  History
    // ---------------------------------------------------------------------

    private fun historyArchiveFor(topic: String): IMessageArchiveExtended? {
        val groups = archiveHandler.getDeployedArchiveGroups().values
        for (g in groups) {
            if (g.filterTree.isTopicNameMatching(topic)) {
                (g.archiveStore as? IMessageArchiveExtended)?.let { return it }
            }
        }
        return null
    }

    private fun historyArchiveSupportingWrite(topic: String): List<ArchiveGroup> {
        return archiveHandler.getDeployedArchiveGroups().values.filter {
            it.filterTree.isTopicNameMatching(topic)
        }
    }

    private fun historyForTopic(
        topic: String,
        startTime: Instant?,
        endTime: Instant?,
        limit: Int
    ): JsonArray {
        val archive = historyArchiveFor(topic) ?: return JsonArray()
        val rows = archive.getHistory(topic, startTime, endTime, limit)
        val out = JsonArray()
        for (i in 0 until rows.size()) {
            val record = rows.getJsonObject(i) ?: continue
            val timestampMs = record.getLong("timestamp") ?: continue
            val value: Any? = when (val p = record.getValue("payload")) {
                is JsonObject, is JsonArray -> p
                is String -> try {
                    when (p.trimStart().firstOrNull()) {
                        '{' -> JsonObject(p)
                        '[' -> JsonArray(p)
                        else -> p.toDoubleOrNull() ?: p
                    }
                } catch (_: Exception) { p }
                null -> record.getString("payload_base64")?.let { JsonObject().put("payload_base64", it) }
                else -> p
            }
            out.add(
                JsonObject()
                    .put("value", value)
                    .put("quality", "Good")
                    .put("timestamp", Instant.ofEpochMilli(timestampMs).toString())
            )
        }
        return out
    }

    private fun leavesForComposition(topic: String, snapshot: TopicTreeSnapshot, maxDepth: Int): List<String> {
        if (maxDepth == 1) return listOf(topic)
        val leaves = mutableListOf<String>()
        fun collect(t: String, depth: Int) {
            val kids = childTopics(t, snapshot)
            if (kids.isEmpty()) {
                leaves.add(t)
                return
            }
            if (maxDepth != 0 && depth >= maxDepth) {
                leaves.add(t)
                return
            }
            for (kid in kids) collect(kid, depth + 1)
        }
        collect(topic, 1)
        return leaves
    }

    private fun handleObjectsHistory(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val ids = body.getJsonArray("elementIds") ?: JsonArray()
        val startTime = body.getString("startTime")?.let { parseInstant(it) }
        val endTime = body.getString("endTime")?.let { parseInstant(it) }
        val maxValues = body.getInteger("maxValues", 1000)
        val maxDepth = body.getInteger("maxDepth", 1)

        vertx.executeBlocking(java.util.concurrent.Callable {
            val snapshot = snapshotRetainedTopics()
            val items = mutableListOf<BulkItem>()
            for (i in 0 until ids.size()) {
                val id = ids.getString(i)
                if (id.isNullOrEmpty()) {
                    items.add(BulkItem.notFound("", "Missing elementId")); continue
                }
                if (!snapshot.allLevels.contains(id)) {
                    items.add(BulkItem.notFound(id)); continue
                }
                try {
                    if (maxDepth == 1) {
                        val values = historyForTopic(id, startTime, endTime, maxValues)
                        items.add(
                            BulkItem.ok(
                                id,
                                JsonObject().put("isComposition", false).put("values", values)
                            )
                        )
                    } else {
                        val leaves = leavesForComposition(id, snapshot, maxDepth)
                        for (leaf in leaves) {
                            val values = historyForTopic(leaf, startTime, endTime, maxValues)
                            items.add(
                                BulkItem.ok(
                                    leaf,
                                    JsonObject().put("isComposition", false).put("values", values)
                                )
                            )
                        }
                    }
                } catch (e: Exception) {
                    logger.warning("I3X history error for '$id': ${e.message}")
                    items.add(BulkItem.error(id, 500, "History query failed: ${e.message}"))
                }
            }
            items
        }).onSuccess { items ->
            sendBulk(ctx, items as List<BulkItem>)
        }.onFailure { err ->
            logger.severe("I3X history query error: ${err.message}")
            sendError(ctx, 500, "History query failed: ${err.message}")
        }
    }

    private fun handleGetHistorySingle(ctx: RoutingContext) {
        val elementId = extractElementId(ctx, "/history") ?: return
        val startTime = ctx.queryParam("startTime").firstOrNull()?.let { parseInstant(it) }
        val endTime = ctx.queryParam("endTime").firstOrNull()?.let { parseInstant(it) }
        val maxValues = ctx.queryParam("maxValues").firstOrNull()?.toIntOrNull() ?: 1000

        vertx.executeBlocking(java.util.concurrent.Callable {
            historyForTopic(elementId, startTime, endTime, maxValues)
        }).onSuccess { values ->
            sendOk(
                ctx,
                JsonObject().put("elementId", elementId).put("values", values)
            )
        }.onFailure { err ->
            sendError(ctx, 500, "History query failed: ${err.message}")
        }
    }

    private fun handleUpdateHistory(ctx: RoutingContext) {
        val elementId = extractElementId(ctx, "/history") ?: return
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val dataArr = body.getJsonArray("data") ?: JsonArray()

        val archives = historyArchiveSupportingWrite(elementId)
        if (archives.isEmpty()) {
            sendError(ctx, 404, "No archive group matches topic '$elementId'")
            return
        }

        val writable = archives.filter {
            it.archiveStore != null &&
                it.archiveStore!!.getType() != at.rocworks.stores.MessageArchiveType.KAFKA &&
                it.archiveStore!!.getType() != at.rocworks.stores.MessageArchiveType.NONE
        }

        val messages = mutableListOf<BrokerMessage>()
        for (j in 0 until dataArr.size()) {
            val rec = dataArr.getJsonObject(j) ?: continue
            val ts = rec.getString("timestamp")?.let { parseInstant(it) } ?: Instant.now()
            val value = rec.getValue("value")
            val payloadBytes: ByteArray = when (value) {
                is JsonObject -> value.encode().toByteArray(Charsets.UTF_8)
                is JsonArray -> value.encode().toByteArray(Charsets.UTF_8)
                null -> ByteArray(0)
                else -> value.toString().toByteArray(Charsets.UTF_8)
            }
            messages.add(
                BrokerMessage(
                    messageId = 0,
                    topicName = elementId,
                    payload = payloadBytes,
                    qosLevel = 0,
                    isRetain = false,
                    isDup = false,
                    isQueued = false,
                    clientId = "i3x-v1",
                    time = ts
                )
            )
        }

        val warnings = JsonArray()
        archives.forEach {
            val store = it.archiveStore
            if (store == null || it !in writable) {
                warnings.add("Archive group '${it.name}' does not support backfill; skipped")
            } else {
                try {
                    store.addHistory(messages)
                } catch (e: Exception) {
                    warnings.add("Archive group '${it.name}' write failed: ${e.message}")
                }
            }
        }

        sendOk(
            ctx,
            JsonObject()
                .put("elementId", elementId)
                .put("writtenCount", messages.size)
                .put("archiveGroups", JsonArray(archives.map { it.name }))
                .put("warnings", warnings)
        )
    }

    // ---------------------------------------------------------------------
    //  Subscriptions
    // ---------------------------------------------------------------------

    private fun handleCreateSubscription(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val clientId = body.getString("clientId")
        val displayName = body.getString("displayName")
        val subId = UUID.randomUUID().toString()
        val sub = I3xSubscription(clientId = clientId, subscriptionId = subId, displayName = displayName)
        subscriptions[subId] = sub
        sendOk(
            ctx,
            JsonObject()
                .put("clientId", clientId)
                .put("subscriptionId", subId)
                .put("displayName", displayName)
                .put("created", sub.created.toString())
        )
    }

    private fun handleRegisterTopics(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val subId = body.getString("subscriptionId") ?: return sendError(ctx, 400, "Missing subscriptionId")
        val sub = subscriptions[subId] ?: return sendError(ctx, 404, "Subscription not found")
        val ids = body.getJsonArray("elementIds") ?: JsonArray()
        val maxDepth = body.getInteger("maxDepth", 1)
        sub.maxDepth = maxDepth
        for (i in 0 until ids.size()) {
            val id = ids.getString(i) ?: continue
            if (id.isEmpty()) continue
            sub.registeredIds.add(id)
        }
        rewireMessageListener(sub)
        sendOk(
            ctx,
            JsonObject()
                .put("clientId", sub.clientId)
                .put("subscriptionId", subId)
                .put("elementIds", JsonArray(sub.registeredIds.toList()))
                .put("maxDepth", sub.maxDepth)
        )
    }

    private fun handleUnregisterTopics(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val subId = body.getString("subscriptionId") ?: return sendError(ctx, 400, "Missing subscriptionId")
        val sub = subscriptions[subId] ?: return sendError(ctx, 404, "Subscription not found")
        val ids = body.getJsonArray("elementIds") ?: JsonArray()
        for (i in 0 until ids.size()) {
            val id = ids.getString(i) ?: continue
            sub.registeredIds.remove(id)
        }
        rewireMessageListener(sub)
        sendOk(
            ctx,
            JsonObject()
                .put("subscriptionId", subId)
                .put("elementIds", JsonArray(sub.registeredIds.toList()))
        )
    }

    private fun handleStream(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val subId = body.getString("subscriptionId") ?: return sendError(ctx, 400, "Missing subscriptionId")
        val sub = subscriptions[subId] ?: return sendError(ctx, 404, "Subscription not found")

        val response = ctx.response()
            .putHeader("Content-Type", "text/event-stream")
            .putHeader("Cache-Control", "no-cache")
            .putHeader("Connection", "keep-alive")
            .setChunked(true)

        val busAddress = "mq.i3x.sub.${sub.subscriptionId}"
        val consumer = vertx.eventBus().consumer<JsonObject>(busAddress) { msg ->
            if (!response.ended()) response.write("data: ${msg.body().encode()}\n\n")
        }
        val timerId = vertx.setPeriodic(30_000L) {
            if (!response.ended()) response.write(": heartbeat\n\n")
        }
        response.closeHandler {
            vertx.cancelTimer(timerId)
            consumer.unregister()
        }

        // Flush queued updates on connect.
        synchronized(sub.pendingQueue) {
            while (sub.pendingQueue.isNotEmpty()) {
                response.write("data: ${sub.pendingQueue.removeFirst().encode()}\n\n")
            }
        }
    }

    private fun handleSync(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val subId = body.getString("subscriptionId") ?: return sendError(ctx, 400, "Missing subscriptionId")
        val sub = subscriptions[subId] ?: return sendError(ctx, 404, "Subscription not found")
        val lastSeq = body.getLong("lastSequenceNumber")

        val updates = JsonArray()
        synchronized(sub.pendingQueue) {
            if (lastSeq != null) {
                while (sub.pendingQueue.isNotEmpty() &&
                    (sub.pendingQueue.first().getLong("sequenceNumber") ?: 0L) <= lastSeq) {
                    sub.pendingQueue.removeFirst()
                }
            }
            for (u in sub.pendingQueue) updates.add(u)
        }
        sendOk(
            ctx,
            JsonObject()
                .put("clientId", sub.clientId)
                .put("subscriptionId", subId)
                .put("updates", updates)
        )
    }

    private fun handleSubscriptionsList(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val ids = body.getJsonArray("subscriptionIds") ?: JsonArray()
        val items = (0 until ids.size()).map { i ->
            val id = ids.getString(i)
            val sub = id?.let { subscriptions[it] }
            if (sub == null) BulkItem.notFound(id ?: "", "Subscription not found")
            else BulkItem.ok(
                id,
                JsonObject()
                    .put("clientId", sub.clientId)
                    .put("subscriptionId", sub.subscriptionId)
                    .put("displayName", sub.displayName)
                    .put("elementIds", JsonArray(sub.registeredIds.toList()))
                    .put("maxDepth", sub.maxDepth)
                    .put("created", sub.created.toString())
            )
        }
        sendBulk(ctx, items)
    }

    private fun handleSubscriptionsDelete(ctx: RoutingContext) {
        val body = ctx.body().asJsonObject() ?: JsonObject()
        val ids = body.getJsonArray("subscriptionIds") ?: JsonArray()
        val items = (0 until ids.size()).map { i ->
            val id = ids.getString(i)
            val sub = id?.let { subscriptions.remove(it) }
            if (sub == null) BulkItem.notFound(id ?: "", "Subscription not found")
            else {
                sessionHandler.unregisterMessageListener("i3x-${sub.subscriptionId}")
                BulkItem.ok(id, JsonObject().put("deleted", true))
            }
        }
        sendBulk(ctx, items)
    }

    private fun rewireMessageListener(sub: I3xSubscription) {
        val listenerId = "i3x-${sub.subscriptionId}"
        sessionHandler.unregisterMessageListener(listenerId)
        sub.topicFilters.clear()
        if (sub.registeredIds.isEmpty()) return

        // Keep the listener rewire simple: register exact topic filters and,
        // when maxDepth != 1, also register a wildcard subtree for each id.
        // We intentionally avoid `snapshotRetainedTopics()` here because this
        // runs on the event loop (called from register/unregister handlers).
        for (id in sub.registeredIds) {
            sub.topicFilters.add(id)
            if (sub.maxDepth != 1) {
                sub.topicFilters.add("$id/#")
            }
        }
        if (sub.topicFilters.isEmpty()) return

        sessionHandler.registerMessageListener(listenerId, sub.topicFilters.toList()) { message ->
            val seq = sub.nextSequence.getAndIncrement()
            val update = JsonObject()
                .put("sequenceNumber", seq)
                .put("elementId", message.topicName)
                .put("value", decodedValue(message))
                .put("quality", qualityFor(message.topicName, message))
                .put("timestamp", message.time.toString())
            vertx.eventBus().publish("mq.i3x.sub.${sub.subscriptionId}", update)
            synchronized(sub.pendingQueue) {
                sub.pendingQueue.addLast(update)
                while (sub.pendingQueue.size > 10_000) sub.pendingQueue.removeFirst()
            }
        }
    }

    // ---------------------------------------------------------------------
    //  Utilities
    // ---------------------------------------------------------------------

    private fun loadDevices(): List<DeviceConfig> {
        val store = deviceConfigStore ?: return emptyList()
        // Use a brief blocking wait — device configs are cached in-memory in practice.
        val promise = store.getAllDevices()
        return try {
            val ar = promise.toCompletionStage().toCompletableFuture().get(
                2, java.util.concurrent.TimeUnit.SECONDS
            )
            ar
        } catch (_: Exception) {
            emptyList()
        }
    }

    private fun parseInstant(s: String): Instant? = try {
        Instant.parse(s)
    } catch (_: DateTimeParseException) { null }

    // ---------------------------------------------------------------------
    //  Authentication
    // ---------------------------------------------------------------------

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
                .end(
                    JsonObject()
                        .put("success", false)
                        .put(
                            "error",
                            JsonObject().put("code", 401).put("message", "Authentication required")
                        ).encode()
                )
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
                .end(
                    JsonObject()
                        .put("success", false)
                        .put(
                            "error",
                            JsonObject().put("code", 401).put("message", "Invalid or expired token")
                        ).encode()
                )
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
                                .end(
                                    JsonObject()
                                        .put("success", false)
                                        .put(
                                            "error",
                                            JsonObject().put("code", 401)
                                                .put("message", "Invalid credentials")
                                        ).encode()
                                )
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
                .end(
                    JsonObject()
                        .put("success", false)
                        .put(
                            "error",
                            JsonObject().put("code", 401).put("message", "Invalid credentials format")
                        ).encode()
                )
            return false
        }

        ctx.response().setStatusCode(401)
            .putHeader("Content-Type", "application/json")
            .putHeader("WWW-Authenticate", "Basic realm=\"MonsterMQ I3X API\"")
            .end(
                JsonObject()
                    .put("success", false)
                    .put(
                        "error",
                        JsonObject().put("code", 401).put("message", "Unsupported authentication method")
                    ).encode()
            )
        return false
    }
}
