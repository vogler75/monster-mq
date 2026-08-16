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
        const val SPEC_VERSION = "1.0"
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
    private val basePaths = listOf("/i3x/v1", "/v1")
    private val baseNamespaceUri = "mqtt://$brokerName/"

    // --- Subscriptions (v1) ---

    private data class I3xSubscription(
        val clientId: String,
        val subscriptionId: String,
        val displayName: String?,
        val registeredIds: MutableSet<String> = ConcurrentHashMap.newKeySet(),
        val pendingQueue: ArrayDeque<JsonObject> = ArrayDeque(),
        var nextSequence: AtomicLong = AtomicLong(1L),
        var maxDepth: Int = 1
    ) {
        val listenerId: String get() = "i3x-$subscriptionId"
    }

    // subscriptionId -> subscription
    private val subscriptions = ConcurrentHashMap<String, I3xSubscription>()

    override fun start(startPromise: Promise<Void>) {
        logger.fine("Starting I3X v1 API server")

        val router = Router.router(vertx)

        // Suppress client abort / broken pipe / closed channel exceptions globally
        router.route().failureHandler { ctx ->
            val cause = ctx.failure()
            val msg = cause.message ?: ""
            val isConnectionClosed = cause.javaClass.name.contains("StacklessClosedChannelException") ||
                cause is java.io.IOException && (
                    msg.contains("Broken pipe", ignoreCase = true) ||
                    msg.contains("connection reset", ignoreCase = true) ||
                    msg.contains("connection was aborted", ignoreCase = true)
                )

            if (isConnectionClosed) {
                logger.fine("Client disconnected early: ${cause.javaClass.simpleName} $msg")
            } else {
                ctx.next()
            }
        }

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

        for (path in basePaths) {
            // /info is unauthenticated discovery.
            router.get("$path/info").handler(::handleInfo)

            // All other routes require authentication.
            router.route("$path/*").handler { ctx ->
                if (ctx.request().path() == "$path/info") {
                    ctx.next(); return@handler
                }
                if (logger.isLoggable(java.util.logging.Level.FINE)) {
                    val method = ctx.request().method().name()
                    val body = if (method == "POST" || method == "PUT") ctx.body()?.asString()?.take(200) else ""
                    logger.fine("I3X $method ${ctx.request().uri()} $body")
                }
                if (!validateAuthentication(ctx)) return@handler
                ctx.next()
            }

            // Explore
            router.get("$path/namespaces").handler(::handleNamespaces)
            router.get("$path/objecttypes").handler(::handleObjectTypes)
            router.post("$path/objecttypes/query").handler(::handleObjectTypesQuery)
            router.get("$path/relationshiptypes").handler(::handleRelationshipTypes)
            router.post("$path/relationshiptypes/query").handler(::handleRelationshipTypesQuery)
            router.get("$path/objects").handler(::handleObjects)
            router.post("$path/objects/list").handler(::handleObjectsList)
            router.post("$path/objects/related").handler(::handleObjectsRelated)

            // Query
            router.post("$path/objects/value").handler(::handleObjectsValue)
            router.post("$path/objects/history").handler(::handleObjectsHistory)

            // Bulk Update (PUT /objects/value and PUT /objects/history)
            router.put("$path/objects/value").handler(::handleBulkUpdateValue)
            router.put("$path/objects/history").handler(::handleBulkUpdateHistory)

            // Single Update / Query by elementId in path — elementId can contain '/', match greedily.
            val escapedBase = path.replace("/", "\\/")
            router.putWithRegex("$escapedBase\\/objects\\/(?<eid>.+)\\/value").handler(::handleUpdateValue)
            router.putWithRegex("$escapedBase\\/objects\\/(?<eid>.+)\\/history").handler(::handleUpdateHistory)
            router.getWithRegex("$escapedBase\\/objects\\/(?<eid>.+)\\/history").handler(::handleGetHistorySingle)

            // Subscriptions (v1)
            router.post("$path/subscriptions").handler(::handleCreateSubscription)
            router.post("$path/subscriptions/register").handler(::handleRegisterTopics)
            router.post("$path/subscriptions/unregister").handler(::handleUnregisterTopics)
            router.get("$path/subscriptions/stream").handler(::handleStream)
            router.post("$path/subscriptions/stream").handler(::handleStream)
            router.get("$path/subscriptions/:subId/stream").handler(::handleStream)
            router.post("$path/subscriptions/:subId/stream").handler(::handleStream)
            router.get("$path/subscriptions/sync").handler(::handleSync)
            router.post("$path/subscriptions/sync").handler(::handleSync)
            router.get("$path/subscriptions/:subId/sync").handler(::handleSync)
            router.post("$path/subscriptions/:subId/sync").handler(::handleSync)
            router.post("$path/subscriptions/list").handler(::handleSubscriptionsList)
            router.get("$path/subscriptions/list").handler(::handleSubscriptionsList)
            router.post("$path/subscriptions/delete").handler(::handleSubscriptionsDelete)
            router.delete("$path/subscriptions/delete").handler(::handleSubscriptionsDelete)
            router.delete("$path/subscriptions/:subId").handler(::handleSubscriptionsDelete)
        }

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
            cleanupSubscription(sub)
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
                .put(item.idField, item.id)
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
        val id: String,
        val success: Boolean,
        val result: Any? = null,
        val errorCode: Int? = null,
        val errorMessage: String? = null,
        val idField: String = "elementId"
    ) {
        companion object {
            fun ok(elementId: String, result: Any?): BulkItem = BulkItem(elementId, true, result)
            fun notFound(elementId: String, message: String = "Object not found"): BulkItem =
                BulkItem(elementId, false, errorCode = 404, errorMessage = message)
            fun error(elementId: String, code: Int, message: String): BulkItem =
                BulkItem(elementId, false, errorCode = code, errorMessage = message)
            fun subscriptionOk(subscriptionId: String, result: Any?): BulkItem =
                BulkItem(subscriptionId, true, result, idField = "subscriptionId")
            fun subscriptionNotFound(
                subscriptionId: String,
                message: String = "Subscription not found"
            ): BulkItem = BulkItem(
                subscriptionId,
                false,
                errorCode = 404,
                errorMessage = message,
                idField = "subscriptionId"
            )
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
        }).onSuccess { result: JsonArray -> sendOk(ctx, result) }
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
        }).onSuccess { items: List<BulkItem> -> sendBulk(ctx, items) }
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
        }).onSuccess { items: List<BulkItem> -> sendBulk(ctx, items) }
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
        }).onSuccess { items: List<BulkItem> -> sendBulk(ctx, items) }
            .onFailure { sendError(ctx, 500, "Value query failed: ${it.message}") }
    }

    private fun encodeValueToBytes(v: Any?): ByteArray = when (v) {
        is JsonObject -> v.encode().toByteArray(Charsets.UTF_8)
        is JsonArray -> v.encode().toByteArray(Charsets.UTF_8)
        null -> ByteArray(0)
        else -> v.toString().toByteArray(Charsets.UTF_8)
    }

    private fun handleBulkUpdateValue(ctx: RoutingContext) {
        val bodyStr = ctx.body()?.asString() ?: ""
        val updates = mutableListOf<Pair<String, ByteArray>>()

        try {
            val trimmed = bodyStr.trim()
            if (trimmed.startsWith("[")) {
                val arr = JsonArray(trimmed)
                for (i in 0 until arr.size()) {
                    val item = arr.getJsonObject(i) ?: continue
                    val elementId = item.getString("elementId") ?: continue
                    val valBytes = encodeValueToBytes(item.getValue("value"))
                    updates.add(elementId to valBytes)
                }
            } else if (trimmed.startsWith("{")) {
                val obj = JsonObject(trimmed)
                if (obj.containsKey("updates")) {
                    val arr = obj.getJsonArray("updates") ?: JsonArray()
                    for (i in 0 until arr.size()) {
                        val item = arr.getJsonObject(i) ?: continue
                        val elementId = item.getString("elementId") ?: continue
                        val valBytes = encodeValueToBytes(item.getValue("value"))
                        updates.add(elementId to valBytes)
                    }
                } else {
                    // Dictionary format: { "topic/1": value1, "topic/2": value2 }
                    for (field in obj.fieldNames()) {
                        val valObj = obj.getValue(field)
                        val valBytes = if (valObj is JsonObject && valObj.containsKey("value")) {
                            encodeValueToBytes(valObj.getValue("value"))
                        } else {
                            encodeValueToBytes(valObj)
                        }
                        updates.add(field to valBytes)
                    }
                }
            }
        } catch (e: Exception) {
            sendError(ctx, 400, "Invalid JSON payload: ${e.message}")
            return
        }

        if (updates.isEmpty()) {
            sendError(ctx, 400, "No valid updates found in request body")
            return
        }

        val items = mutableListOf<BulkItem>()
        for ((elementId, payloadBytes) in updates) {
            try {
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
                items.add(BulkItem.ok(elementId, JsonObject().put("status", "published")))
            } catch (e: Exception) {
                items.add(BulkItem.error(elementId, 500, e.message ?: "Publish failed"))
            }
        }
        sendBulk(ctx, items)
    }

    private fun handleUpdateValue(ctx: RoutingContext) {
        val elementId = extractElementId(ctx, "/value") ?: return
        if (elementId.isEmpty()) return sendError(ctx, 400, "Missing elementId")
        val bodyStr = ctx.body()?.asString() ?: ""
        // v1 spec accepts `{ value, quality?, timestamp? }` or raw JSON value; accept either.
        val payloadBytes: ByteArray = try {
            val maybe = JsonObject(bodyStr)
            if (maybe.containsKey("value")) {
                encodeValueToBytes(maybe.getValue("value"))
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
        val prefix = if (path.startsWith("/i3x/v1/objects/")) "/i3x/v1/objects/" else if (path.startsWith("/v1/objects/")) "/v1/objects/" else ""
        if (prefix.isEmpty() || !path.endsWith(suffix) || path.length <= prefix.length + suffix.length) {
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
            !it.archiveReadOnly && it.filterTree.isTopicNameMatching(topic)
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
        }).onSuccess { items: List<BulkItem> ->
            sendBulk(ctx, items)
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
        }).onSuccess { values: JsonArray ->
            sendOk(
                ctx,
                JsonObject().put("elementId", elementId).put("values", values)
            )
        }.onFailure { err ->
            sendError(ctx, 500, "History query failed: ${err.message}")
        }
    }

    private fun parseHistoryRecords(item: JsonObject): List<Pair<Any?, Instant>> {
        val records = mutableListOf<Pair<Any?, Instant>>()
        if (item.containsKey("values")) {
            val valArr = item.getJsonArray("values") ?: JsonArray()
            for (j in 0 until valArr.size()) {
                val rec = valArr.getJsonObject(j) ?: continue
                val ts = rec.getString("timestamp")?.let { parseInstant(it) } ?: Instant.now()
                records.add(rec.getValue("value") to ts)
            }
        } else if (item.containsKey("data")) {
            val dataArr = item.getJsonArray("data") ?: JsonArray()
            for (j in 0 until dataArr.size()) {
                val rec = dataArr.getJsonObject(j) ?: continue
                val ts = rec.getString("timestamp")?.let { parseInstant(it) } ?: Instant.now()
                records.add(rec.getValue("value") to ts)
            }
        } else if (item.containsKey("value")) {
            val ts = item.getString("timestamp")?.let { parseInstant(it) } ?: Instant.now()
            records.add(item.getValue("value") to ts)
        }
        return records
    }

    private fun handleBulkUpdateHistory(ctx: RoutingContext) {
        val bodyStr = ctx.body()?.asString() ?: ""
        data class HistoryUpdate(val elementId: String, val records: List<Pair<Any?, Instant>>)
        val updates = mutableListOf<HistoryUpdate>()

        try {
            val trimmed = bodyStr.trim()
            if (trimmed.startsWith("[")) {
                val arr = JsonArray(trimmed)
                for (i in 0 until arr.size()) {
                    val item = arr.getJsonObject(i) ?: continue
                    val elementId = item.getString("elementId") ?: continue
                    val records = parseHistoryRecords(item)
                    if (records.isNotEmpty()) updates.add(HistoryUpdate(elementId, records))
                }
            } else if (trimmed.startsWith("{")) {
                val obj = JsonObject(trimmed)
                val arr = obj.getJsonArray("updates") ?: obj.getJsonArray("data") ?: JsonArray()
                for (i in 0 until arr.size()) {
                    val item = arr.getJsonObject(i) ?: continue
                    val elementId = item.getString("elementId") ?: continue
                    val records = parseHistoryRecords(item)
                    if (records.isNotEmpty()) updates.add(HistoryUpdate(elementId, records))
                }
            }
        } catch (e: Exception) {
            sendError(ctx, 400, "Invalid JSON payload: ${e.message}")
            return
        }

        if (updates.isEmpty()) {
            sendError(ctx, 400, "No valid historical updates found in request body")
            return
        }

        vertx.executeBlocking(java.util.concurrent.Callable {
            val items = mutableListOf<BulkItem>()
            for (up in updates) {
                val elementId = up.elementId
                val archives = historyArchiveSupportingWrite(elementId)
                if (archives.isEmpty()) {
                    items.add(BulkItem.notFound(elementId, "No archive group matches topic '$elementId'"))
                    continue
                }
                val writable = archives.filter {
                    it.archiveStore != null &&
                        it.archiveStore!!.getType() != at.rocworks.stores.MessageArchiveType.NONE
                }
                val messages = up.records.map { (value, ts) ->
                    val payloadBytes = encodeValueToBytes(value)
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
                items.add(
                    BulkItem.ok(
                        elementId,
                        JsonObject()
                            .put("writtenCount", messages.size)
                            .put("archiveGroups", JsonArray(archives.map { it.name }))
                            .put("warnings", warnings)
                    )
                )
            }
            items
        }).onSuccess { items: List<BulkItem> -> sendBulk(ctx, items) }
            .onFailure { err -> sendError(ctx, 500, "Bulk history write failed: ${err.message}") }
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
                it.archiveStore!!.getType() != at.rocworks.stores.MessageArchiveType.NONE
        }

        val messages = mutableListOf<BrokerMessage>()
        for (j in 0 until dataArr.size()) {
            val rec = dataArr.getJsonObject(j) ?: continue
            val ts = rec.getString("timestamp")?.let { parseInstant(it) } ?: Instant.now()
            val value = rec.getValue("value")
            val payloadBytes = encodeValueToBytes(value)
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

    private fun extractSubId(ctx: RoutingContext): String? {
        return ctx.pathParam("subId")
            ?: ctx.pathParam("subscriptionId")
            ?: ctx.queryParam("subscriptionId").firstOrNull()
            ?: ctx.queryParam("subId").firstOrNull()
            ?: try { ctx.body()?.asJsonObject()?.getString("subscriptionId") } catch (_: Exception) { null }
            ?: try { ctx.body()?.asJsonObject()?.getString("subId") } catch (_: Exception) { null }
            ?: ctx.request().getHeader("X-Subscription-Id")
    }

    private fun extractClientId(ctx: RoutingContext): String? {
        val clientId = ctx.queryParam("clientId").firstOrNull()
            ?: try { ctx.body()?.asJsonObject()?.getString("clientId") } catch (_: Exception) { null }
            ?: ctx.request().getHeader("X-Client-Id")
        return clientId?.trim()?.takeIf { it.isNotEmpty() }
    }

    private fun ownedSubscription(ctx: RoutingContext): I3xSubscription? {
        val subId = extractSubId(ctx)
        if (subId.isNullOrBlank()) {
            sendError(ctx, 400, "Missing subscriptionId")
            return null
        }
        val clientId = extractClientId(ctx)
        if (clientId.isNullOrBlank()) {
            sendError(ctx, 400, "Missing clientId")
            return null
        }
        val sub = subscriptions[subId]
        if (sub == null || sub.clientId != clientId) {
            sendError(ctx, 404, "Subscription not found")
            return null
        }
        return sub
    }

    private fun enqueueUpdate(sub: I3xSubscription, entry: JsonObject) {
        synchronized(sub.pendingQueue) {
            val update = entry.copy().put("sequenceNumber", sub.nextSequence.getAndIncrement())
            sub.pendingQueue.addLast(update)
            while (sub.pendingQueue.size > 10_000) sub.pendingQueue.removeFirst()
            vertx.eventBus().publish("mq.i3x.stream.${sub.subscriptionId}", update)
        }
    }

    private fun enqueueInitialValues(sub: I3xSubscription, targetIds: List<String>) {
        vertx.executeBlocking(java.util.concurrent.Callable {
            val snapshot = snapshotRetainedTopics()
            val initialUpdates = mutableListOf<JsonObject>()
            for (id in targetIds) {
                if (sub.maxDepth == 1) {
                    val msg = findRetainedMessage(snapshot, id)
                    if (msg != null) {
                        val update = JsonObject()
                            .put("elementId", id)
                            .put("value", decodedValue(msg))
                            .put("quality", qualityFor(id, msg))
                            .put("timestamp", msg.time.toString())
                        initialUpdates.add(update)
                    }
                } else {
                    val leaves = leavesForComposition(id, snapshot, sub.maxDepth)
                    for (leaf in leaves) {
                        val msg = findRetainedMessage(snapshot, leaf)
                        if (msg != null) {
                            val update = JsonObject()
                                .put("elementId", leaf)
                                .put("value", decodedValue(msg))
                                .put("quality", qualityFor(leaf, msg))
                                .put("timestamp", msg.time.toString())
                            initialUpdates.add(update)
                        }
                    }
                }
            }
            initialUpdates
        }).onSuccess { updates ->
            for (update in updates) {
                if (subscriptions[sub.subscriptionId] === sub) enqueueUpdate(sub, update)
            }
        }
    }

    private fun handleCreateSubscription(ctx: RoutingContext) {
        val body = try { ctx.body()?.asJsonObject() } catch (_: Exception) { null } ?: JsonObject()
        val clientId = body.getString("clientId")?.trim()
        if (clientId.isNullOrEmpty()) return sendError(ctx, 400, "Missing clientId")
        val displayName = body.getString("displayName")
        val subId = UUID.randomUUID().toString()
        val sub = I3xSubscription(
            clientId = clientId,
            subscriptionId = subId,
            displayName = displayName
        )

        subscriptions[subId] = sub
        sendOk(
            ctx,
            JsonObject()
                .put("clientId", clientId)
                .put("subscriptionId", subId)
                .put("displayName", displayName)
        )
    }

    private fun handleRegisterTopics(ctx: RoutingContext) {
        val body = try { ctx.body()?.asJsonObject() } catch (_: Exception) { null } ?: JsonObject()
        val sub = ownedSubscription(ctx) ?: return
        val ids = body.getJsonArray("elementIds") ?: body.getJsonArray("elements") ?: JsonArray()
        val maxDepth = body.getInteger("maxDepth", sub.maxDepth)
        sub.maxDepth = maxDepth
        val addedIds = mutableListOf<String>()
        val items = mutableListOf<BulkItem>()
        for (i in 0 until ids.size()) {
            val id = ids.getString(i)?.trim().orEmpty()
            if (id.isEmpty()) {
                items.add(BulkItem.error(id, 400, "Element ID must not be empty"))
                continue
            }
            if (sub.registeredIds.add(id)) {
                addedIds.add(id)
            }
            items.add(BulkItem.ok(id, null))
        }
        rewireSubscriptions(sub)
        if (addedIds.isNotEmpty()) {
            enqueueInitialValues(sub, addedIds)
        }
        sendBulk(ctx, items)
    }

    private fun handleUnregisterTopics(ctx: RoutingContext) {
        val body = try { ctx.body()?.asJsonObject() } catch (_: Exception) { null } ?: JsonObject()
        val sub = ownedSubscription(ctx) ?: return
        val ids = body.getJsonArray("elementIds") ?: body.getJsonArray("elements") ?: JsonArray()
        val items = mutableListOf<BulkItem>()
        for (i in 0 until ids.size()) {
            val id = ids.getString(i)?.trim().orEmpty()
            if (id.isEmpty()) {
                items.add(BulkItem.error(id, 400, "Element ID must not be empty"))
                continue
            }
            sub.registeredIds.remove(id)
            items.add(BulkItem.ok(id, null))
        }
        rewireSubscriptions(sub)
        sendBulk(ctx, items)
    }

    private fun handleStream(ctx: RoutingContext) {
        val sub = ownedSubscription(ctx) ?: return

        val response = ctx.response()
            .putHeader("Content-Type", "text/event-stream; charset=utf-8")
            .putHeader("Cache-Control", "no-cache, no-transform")
            .putHeader("Connection", "keep-alive")
            .putHeader("X-Accel-Buffering", "no")
            .setChunked(true)

        // Send initial comment to establish SSE stream connection immediately
        response.write(": connected\n\n")

        val streamAddress = "mq.i3x.stream.${sub.subscriptionId}"
        lateinit var streamConsumer: io.vertx.core.eventbus.MessageConsumer<JsonObject>
        synchronized(sub.pendingQueue) {
            while (sub.pendingQueue.isNotEmpty()) {
                writeSseUpdate(response, sub.pendingQueue.removeFirst())
            }
            streamConsumer = vertx.eventBus().consumer<JsonObject>(streamAddress) { msg ->
                if (!response.ended()) writeSseUpdate(response, msg.body())
            }
        }
        val timerId = vertx.setPeriodic(15_000L) {
            if (!response.ended()) response.write(": heartbeat\n\n")
        }
        response.closeHandler {
            vertx.cancelTimer(timerId)
            streamConsumer.unregister()
        }
    }

    private fun handleSync(ctx: RoutingContext) {
        val body = try { ctx.body()?.asJsonObject() } catch (_: Exception) { null } ?: JsonObject()
        val sub = ownedSubscription(ctx) ?: return
        val lastSeq = ctx.queryParam("lastSequenceNumber").firstOrNull()?.toLongOrNull()
            ?: body.getLong("lastSequenceNumber")

        val batches = JsonArray()
        synchronized(sub.pendingQueue) {
            if (lastSeq != null) {
                while (sub.pendingQueue.isNotEmpty() &&
                    (sub.pendingQueue.first().getLong("sequenceNumber") ?: 0L) <= lastSeq) {
                    sub.pendingQueue.removeFirst()
                }
            }
            for (update in sub.pendingQueue) batches.add(toSyncBatch(update))
        }
        sendOk(ctx, batches)
    }

    private fun handleSubscriptionsList(ctx: RoutingContext) {
        val body = try { ctx.body()?.asJsonObject() } catch (_: Exception) { null } ?: JsonObject()
        val clientId = extractClientId(ctx)
        if (clientId.isNullOrBlank()) return sendError(ctx, 400, "Missing clientId")
        val ids = body.getJsonArray("subscriptionIds") ?: JsonArray()
        val items = (0 until ids.size()).map { i ->
            val id = ids.getString(i).orEmpty()
            val sub = subscriptions[id]?.takeIf { it.clientId == clientId }
            if (sub == null) {
                BulkItem.subscriptionNotFound(id)
            } else {
                val monitoredObjects = JsonArray(
                    sub.registeredIds.sorted().map { elementId ->
                        JsonObject()
                            .put("elementId", elementId)
                            .put("maxDepth", sub.maxDepth)
                    }
                )
                BulkItem.subscriptionOk(
                    id,
                    JsonObject()
                        .put("subscriptionId", sub.subscriptionId)
                        .put("displayName", sub.displayName)
                        .put("monitoredObjects", monitoredObjects)
                )
            }
        }
        sendBulk(ctx, items)
    }

    private fun handleSubscriptionsDelete(ctx: RoutingContext) {
        val body = try { ctx.body()?.asJsonObject() } catch (_: Exception) { null } ?: JsonObject()
        val clientId = extractClientId(ctx)
        if (clientId.isNullOrBlank()) return sendError(ctx, 400, "Missing clientId")
        val ids = body.getJsonArray("subscriptionIds")
            ?: extractSubId(ctx)?.let { JsonArray().add(it) }
            ?: JsonArray()
        val items = (0 until ids.size()).map { i ->
            val id = ids.getString(i).orEmpty()
            val sub = subscriptions[id]?.takeIf { it.clientId == clientId }
            if (sub == null || !subscriptions.remove(id, sub)) {
                BulkItem.subscriptionNotFound(id)
            } else {
                cleanupSubscription(sub)
                BulkItem.subscriptionOk(id, null)
            }
        }
        sendBulk(ctx, items)
    }

    private fun rewireSubscriptions(sub: I3xSubscription) {
        sessionHandler.unregisterMessageListener(sub.listenerId)

        val filters = sub.registeredIds
            .flatMap { subscriptionFilters(it, sub.maxDepth) }
            .distinct()
        if (filters.isNotEmpty()) {
            sessionHandler.registerMessageListener(sub.listenerId, filters) { message ->
                val entry = JsonObject()
                    .put("elementId", message.topicName)
                    .put("value", decodedValue(message))
                    .put("quality", qualityFor(message.topicName, message))
                    .put("timestamp", message.time.toString())
                if (subscriptions[sub.subscriptionId] === sub) enqueueUpdate(sub, entry)
            }
        }
    }

    private fun subscriptionFilters(elementId: String, maxDepth: Int): List<String> {
        val cleanId = elementId.trim().trim('/')
        if (cleanId.isEmpty()) return emptyList()
        if (maxDepth == 0) return listOf(cleanId, "$cleanId/#")

        val filters = mutableListOf(cleanId)
        var descendantFilter = cleanId
        for (depth in 2..maxDepth) {
            descendantFilter += "/+"
            filters.add(descendantFilter)
        }
        return filters
    }

    private fun toSyncBatch(update: JsonObject): JsonObject {
        val entry = update.copy()
        val sequenceNumber = entry.getLong("sequenceNumber") ?: 0L
        entry.remove("sequenceNumber")
        return JsonObject()
            .put("sequenceNumber", sequenceNumber)
            .put("updates", JsonArray().add(entry))
    }

    private fun writeSseUpdate(response: io.vertx.core.http.HttpServerResponse, update: JsonObject) {
        response.write("event: update\n")
        response.write("data: ${toSyncBatch(update).encode()}\n\n")
    }

    private fun cleanupSubscription(sub: I3xSubscription) {
        sessionHandler.unregisterMessageListener(sub.listenerId)
        sub.registeredIds.clear()
        synchronized(sub.pendingQueue) {
            sub.pendingQueue.clear()
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
