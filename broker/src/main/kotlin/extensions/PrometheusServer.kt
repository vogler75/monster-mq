package at.rocworks.extensions

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.auth.UserManager
import at.rocworks.bus.EventBusAddresses
import at.rocworks.extensions.graphql.JwtService
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
import java.util.Base64

/**
 * Prometheus-compatible metrics server.
 *
 * Endpoints:
 * - GET  /metrics                      Prometheus text exposition format (scraping endpoint)
 * - GET  /api/v1/labels                Label names
 * - GET  /api/v1/label/:name/values    Label values
 * - POST /api/v1/series                Series matching selector
 * - POST /api/v1/query_range           Range query (historical topic data from archive groups)
 * - GET  /api/v1/query_range           Range query (same, for GET requests)
 * - POST /api/v1/query                 Instant query (current value from LastValueStore)
 * - GET  /api/v1/query                 Instant query (same, for GET requests)
 */
class PrometheusServer(
    private val host: String,
    private val port: Int,
    private val archiveHandler: ArchiveHandler,
    private val userManager: UserManager
) : AbstractVerticle() {

    private val logger = Utils.getLogger(this::class.java)

    companion object {
        private val METRIC_NAMES = listOf(
            "monstermq_messages_in_total",
            "monstermq_messages_out_total",
            "monstermq_sessions_count",
            "monstermq_subscriptions_count",
            "monstermq_queue_depth",
            "monstermq_message_bus_in",
            "monstermq_message_bus_out",
            "monstermq_topic_value"
        )
    }

    override fun start(startPromise: Promise<Void>) {
        logger.fine("Starting Prometheus server")

        val router = Router.router(vertx)

        router.route().handler(
            CorsHandler.create()
                .addOrigin("*")
                .allowedMethod(io.vertx.core.http.HttpMethod.GET)
                .allowedMethod(io.vertx.core.http.HttpMethod.POST)
                .allowedMethod(io.vertx.core.http.HttpMethod.OPTIONS)
                .allowedHeader("Content-Type")
                .allowedHeader("Authorization")
                .allowedHeader("Accept")
        )

        router.route().handler(BodyHandler.create())

        router.route().handler { ctx ->
            if (!validateAuthentication(ctx)) return@handler
            ctx.next()
        }

        router.get("/metrics").handler { ctx -> handleMetrics(ctx) }

        router.get("/api/v1/labels").handler { ctx -> handleLabels(ctx) }
        router.get("/api/v1/label/:name/values").handler { ctx -> handleLabelValues(ctx) }
        router.post("/api/v1/series").handler { ctx -> handleSeries(ctx) }
        router.post("/api/v1/query_range").handler { ctx -> handleQueryRange(ctx) }
        router.get("/api/v1/query_range").handler { ctx -> handleQueryRange(ctx) }
        router.post("/api/v1/query").handler { ctx -> handleQuery(ctx) }
        router.get("/api/v1/query").handler { ctx -> handleQuery(ctx) }

        router.get("/").handler { ctx ->
            ctx.response()
                .setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .end(JsonObject().put("status", "ok").put("message", "MonsterMQ Prometheus API").encode())
        }

        val options = HttpServerOptions().setPort(port).setHost(host)
        vertx.createHttpServer(options)
            .requestHandler(router)
            .listen()
            .onSuccess { server ->
                logger.info("Prometheus Server started on port ${server.actualPort()}")
                startPromise.complete()
            }
            .onFailure { error ->
                logger.severe("Prometheus Server failed to start: $error")
                startPromise.fail(error)
            }
    }

    // ========== /metrics endpoint ==========

    private fun handleMetrics(ctx: RoutingContext) {
        val nodeIds = Monster.getClusterNodeIds(vertx)
        if (nodeIds.isEmpty()) {
            renderMetrics(ctx, emptyList())
            return
        }

        val results = java.util.concurrent.CopyOnWriteArrayList<Pair<String, JsonObject>>()
        val pending = java.util.concurrent.atomic.AtomicInteger(nodeIds.size)

        nodeIds.forEach { nodeId ->
            vertx.eventBus().request<JsonObject>(EventBusAddresses.Node.metrics(nodeId), JsonObject())
                .onComplete { ar ->
                    results.add(nodeId to (if (ar.succeeded()) ar.result().body() else JsonObject()))
                    if (pending.decrementAndGet() == 0) {
                        renderMetrics(ctx, results)
                    }
                }
        }
    }

    private fun renderMetrics(ctx: RoutingContext, nodeResults: List<Pair<String, JsonObject>>) {
        val sb = StringBuilder()
        appendBrokerMetrics(sb, nodeResults)
        appendTopicValueMetrics(sb)
        ctx.response()
            .setStatusCode(200)
            .putHeader("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            .end(sb.toString())
    }

    private fun appendBrokerMetrics(sb: StringBuilder, results: List<Pair<String, JsonObject>>) {
        appendMetricComment(sb, "monstermq_messages_in_total", "gauge", "MQTT messages received per second")
        results.forEach { (nodeId, nm) ->
            appendMetric(sb, "monstermq_messages_in_total", mapOf("node" to nodeId), nm.getDouble("messagesInRate", 0.0))
        }

        appendMetricComment(sb, "monstermq_messages_out_total", "gauge", "MQTT messages sent per second")
        results.forEach { (nodeId, nm) ->
            appendMetric(sb, "monstermq_messages_out_total", mapOf("node" to nodeId), nm.getDouble("messagesOutRate", 0.0))
        }

        appendMetricComment(sb, "monstermq_sessions_count", "gauge", "Number of client sessions on this node")
        results.forEach { (nodeId, nm) ->
            appendMetric(sb, "monstermq_sessions_count", mapOf("node" to nodeId), nm.getInteger("nodeSessionCount", 0).toDouble())
        }

        appendMetricComment(sb, "monstermq_subscriptions_count", "gauge", "Number of active subscriptions")
        results.forEach { (nodeId, nm) ->
            appendMetric(sb, "monstermq_subscriptions_count", mapOf("node" to nodeId), nm.getInteger("subscriptionCount", 0).toDouble())
        }

        appendMetricComment(sb, "monstermq_queue_depth", "gauge", "Number of queued messages")
        results.forEach { (nodeId, nm) ->
            appendMetric(sb, "monstermq_queue_depth", mapOf("node" to nodeId), nm.getInteger("queuedMessagesCount", 0).toDouble())
        }

        appendMetricComment(sb, "monstermq_message_bus_in", "gauge", "Message bus inbound rate per second")
        results.forEach { (nodeId, nm) ->
            appendMetric(sb, "monstermq_message_bus_in", mapOf("node" to nodeId), nm.getDouble("messageBusInRate", 0.0))
        }

        appendMetricComment(sb, "monstermq_message_bus_out", "gauge", "Message bus outbound rate per second")
        results.forEach { (nodeId, nm) ->
            appendMetric(sb, "monstermq_message_bus_out", mapOf("node" to nodeId), nm.getDouble("messageBusOutRate", 0.0))
        }
    }

    private fun appendTopicValueMetrics(sb: StringBuilder) {
        appendMetricComment(sb, "monstermq_topic_value", "gauge", "Current value of MQTT topic")
        archiveHandler.getDeployedArchiveGroups().forEach { (groupName, group) ->
            group.lastValStore?.findMatchingMessages("#") { message ->
                val payload = String(message.payload, Charsets.UTF_8).trim()
                val topicName = message.topicName
                val numVal = payload.toDoubleOrNull()
                if (numVal != null) {
                    appendMetric(sb, "monstermq_topic_value",
                        mapOf("topic" to topicName, "archive_group" to groupName, "field" to ""),
                        numVal)
                } else {
                    try {
                        val json = JsonObject(payload)
                        json.forEach { entry ->
                            val numField = when (val v = entry.value) {
                                is Number -> v.toDouble()
                                is String -> v.toDoubleOrNull()
                                else -> null
                            }
                            if (numField != null) {
                                appendMetric(sb, "monstermq_topic_value",
                                    mapOf("topic" to topicName, "archive_group" to groupName, "field" to entry.key),
                                    numField)
                            }
                        }
                    } catch (e: Exception) { /* non-JSON payload, skip */ }
                }
                true
            }
        }
    }

    private fun appendMetricComment(sb: StringBuilder, name: String, type: String, help: String) {
        sb.append("# HELP $name $help\n")
        sb.append("# TYPE $name $type\n")
    }

    private fun appendMetric(sb: StringBuilder, name: String, labels: Map<String, String>, value: Double) {
        if (value.isNaN() || value.isInfinite()) return
        val labelStr = labels.entries.joinToString(",") { (k, v) ->
            "$k=\"${v.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")}\""
        }
        sb.append("$name{$labelStr} $value\n")
    }

    // ========== Prometheus HTTP API ==========

    private fun handleLabels(ctx: RoutingContext) {
        val data = JsonArray().add("__name__").add("topic").add("archive_group").add("field").add("node")
        ctx.response().setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(JsonObject().put("status", "success").put("data", data).encode())
    }

    private fun handleLabelValues(ctx: RoutingContext) {
        val matchSelector = getMatchSelector(ctx)
        val matchLabels = extractLabelMatchers(matchSelector)
        val archiveGroupFilter = matchLabels["archive_group"]
        val topicFilter = matchLabels["topic"]

        fun groupsToScan() = archiveHandler.getDeployedArchiveGroups()
            .filter { (name, _) -> archiveGroupFilter == null || name == archiveGroupFilter }

        val groups = groupsToScan()
        logger.info("label/${ctx.pathParam("name")}/values: archiveGroup=${archiveGroupFilter ?: "(all)"} topic=${topicFilter ?: "(all)"} scanning groups=${groups.keys}")

        when (val labelName = ctx.pathParam("name")) {
            "__name__" -> {
                ctx.response().setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .end(JsonObject().put("status", "success").put("data", JsonArray(METRIC_NAMES)).encode())
            }
            "topic" -> {
                val topics = mutableSetOf<String>()
                groups.forEach { (groupName, group) ->
                    if (group.lastValStore == null) {
                        logger.warning("Archive group '$groupName' has no lastValStore — topics cannot be discovered")
                    } else {
                        group.lastValStore!!.findMatchingMessages(topicFilter ?: "#") { message ->
                            topics.add(message.topicName)
                            topics.size < 5000
                        }
                        logger.info("label/topic/values: group='$groupName' → ${topics.size} topics found")
                    }
                }
                ctx.response().setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .end(JsonObject().put("status", "success").put("data", JsonArray(topics.sorted())).encode())
            }
            "archive_group" -> {
                val allGroups = archiveHandler.getDeployedArchiveGroups().keys.sorted()
                logger.info("label/archive_group/values: returning ${allGroups.size} groups: $allGroups")
                ctx.response().setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .end(JsonObject().put("status", "success").put("data", JsonArray(allGroups)).encode())
            }
            "field" -> {
                if (topicFilter == null)
                    logger.info("label/field/values: no topic filter in match[] — returning fields from all topics in groups=${groups.keys}")
                else
                    logger.info("label/field/values: topic='$topicFilter' archiveGroup=${archiveGroupFilter ?: "(all)"}")
                val fields = mutableSetOf<String>()
                groups.forEach { (groupName, group) ->
                    group.lastValStore?.findMatchingMessages(topicFilter ?: "#") { message ->
                        val payload = String(message.payload, Charsets.UTF_8).trim()
                        if (payload.toDoubleOrNull() != null) {
                            fields.add("")
                        } else {
                            try {
                                JsonObject(payload).forEach { entry ->
                                    if (entry.value is Number || (entry.value as? String)?.toDoubleOrNull() != null)
                                        fields.add(entry.key)
                                }
                            } catch (e: Exception) { /* non-JSON */ }
                        }
                        fields.size < 1000
                    }
                    logger.info("label/field/values: group='$groupName' → fields so far: $fields")
                }
                ctx.response().setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .end(JsonObject().put("status", "success").put("data", JsonArray(fields.sorted())).encode())
            }
            else -> {
                logger.info("label/$labelName/values: unknown label, returning empty")
                ctx.response().setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .end(JsonObject().put("status", "success").put("data", JsonArray()).encode())
            }
        }
    }

    private fun handleSeries(ctx: RoutingContext) {
        val matchSelector = getMatchSelector(ctx)
        val matchLabels = extractLabelMatchers(matchSelector)
        val archiveGroupFilter = matchLabels["archive_group"]
        val topicFilter = matchLabels["topic"]

        logger.info("series: archiveGroup=${archiveGroupFilter ?: "(all)"} topic=${topicFilter ?: "(all)"}")
        val data = JsonArray()
        archiveHandler.getDeployedArchiveGroups()
            .filter { (name, _) -> archiveGroupFilter == null || name == archiveGroupFilter }
            .forEach { (groupName, group) ->
            group.lastValStore?.findMatchingMessages(topicFilter ?: "#") { message ->
                val payload = String(message.payload, Charsets.UTF_8).trim()
                val fields = mutableListOf<String>()
                if (payload.toDoubleOrNull() != null) {
                    fields.add("")
                } else {
                    try {
                        JsonObject(payload).forEach { entry ->
                            if (entry.value is Number || (entry.value as? String)?.toDoubleOrNull() != null)
                                fields.add(entry.key)
                        }
                    } catch (e: Exception) { /* non-JSON */ }
                }
                if (fields.isEmpty()) fields.add("")  // always emit at least one entry per topic
                fields.forEach { field ->
                    data.add(
                        JsonObject()
                            .put("__name__", "monstermq_topic_value")
                            .put("topic", message.topicName)
                            .put("archive_group", groupName)
                            .put("field", field)
                    )
                }
                data.size() < 5000
            }
        }
        ctx.response().setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(JsonObject().put("status", "success").put("data", data).encode())
    }

    private fun handleQueryRange(ctx: RoutingContext) {
        val queryStr = getParam(ctx, "query")
        val startStr = getParam(ctx, "start")
        val endStr = getParam(ctx, "end")
        val stepStr = getParam(ctx, "step").ifEmpty { "60" }

        if (queryStr.isEmpty()) {
            sendRangeResult(ctx, JsonArray())
            return
        }

        val startTime = parsePrometheusTime(startStr) ?: Instant.now().minusSeconds(3600)
        val endTime = parsePrometheusTime(endStr) ?: Instant.now()
        val stepSeconds = stepStr.toDoubleOrNull()?.toLong() ?: 60L
        val stepMinutes = (stepSeconds / 60).coerceAtLeast(1).toInt()

        val parsed = parsePromQL(queryStr)
        val topic = parsed.labels["topic"] ?: ""
        val archiveGroupName = parsed.labels["archive_group"] ?: "Default"
        val field = parsed.labels["field"] ?: ""

        logger.info("query_range: topic='$topic' archiveGroup='$archiveGroupName' field='$field' function=${parsed.function} step=${stepMinutes}min range=$startStr..$endStr")

        if (topic.isEmpty()) {
            logger.info("query_range: no topic label in query, returning empty")
            sendRangeResult(ctx, JsonArray())
            return
        }

        val archiveStore = archiveHandler.getDeployedArchiveGroups()[archiveGroupName]?.archiveStore as? IMessageArchiveExtended
        if (archiveStore == null) {
            logger.warning("query_range: no IMessageArchiveExtended found for archive group '$archiveGroupName'")
            sendRangeResult(ctx, JsonArray())
            return
        }

        val result = JsonArray()

        if (parsed.function != null) {
            val fields = if (field.isEmpty()) emptyList() else listOf(field)
            val aggResult = archiveStore.getAggregatedHistory(
                topics = listOf(topic),
                startTime = startTime,
                endTime = endTime,
                intervalMinutes = stepMinutes,
                functions = listOf(parsed.function),
                fields = fields
            )

            val columns = aggResult.getJsonArray("columns") ?: JsonArray()
            val rows = aggResult.getJsonArray("rows") ?: JsonArray()
            val fieldSuffix = if (field.isEmpty()) "" else ".${field.replace(".", "_")}"
            val colName = "$topic${fieldSuffix}_${parsed.function.lowercase()}"
            val colIndex = findColumnIndex(columns, colName)

            if (colIndex >= 0) {
                val values = JsonArray()
                for (i in 0 until rows.size()) {
                    val row = rows.getJsonArray(i) ?: continue
                    val tsStr = row.getString(0) ?: continue
                    val v = row.getValue(colIndex) ?: continue
                    try {
                        val epochSeconds = Instant.parse(tsStr).epochSecond
                        val numV = when (v) {
                            is Number -> v.toDouble()
                            else -> continue
                        }
                        values.add(JsonArray().add(epochSeconds).add(numV.toString()))
                    } catch (e: Exception) { /* skip invalid rows */ }
                }
                result.add(buildSeriesEntry(topic, archiveGroupName, field, values))
            }
        } else {
            val historyData = archiveStore.getHistory(
                topic = topic,
                startTime = startTime,
                endTime = endTime,
                limit = 10000
            )

            val values = JsonArray()
            for (i in 0 until historyData.size()) {
                val record = historyData.getJsonObject(i) ?: continue
                val timestampMs = record.getLong("timestamp") ?: continue
                val payloadStr = record.getString("payload_json")
                    ?: record.getString("payload")
                    ?: record.getString("payload_base64")?.let { b64 ->
                        try { String(Base64.getDecoder().decode(b64)) } catch (e: Exception) { null }
                    } ?: continue

                val numVal = extractNumericValue(payloadStr, field) ?: continue
                values.add(JsonArray().add(timestampMs / 1000).add(numVal.toString()))
            }
            result.add(buildSeriesEntry(topic, archiveGroupName, field, values))
        }

        sendRangeResult(ctx, result)
    }

    private fun handleQuery(ctx: RoutingContext) {
        val queryStr = getParam(ctx, "query")
        val now = Instant.now().epochSecond

        if (queryStr.isEmpty()) {
            sendVectorResult(ctx, JsonArray())
            return
        }

        val parsed = parsePromQL(queryStr)
        val topic = parsed.labels["topic"] ?: ""
        val archiveGroupName = parsed.labels["archive_group"] ?: "Default"
        val field = parsed.labels["field"] ?: ""

        val result = JsonArray()

        if (topic.isNotEmpty()) {
            val group = archiveHandler.getDeployedArchiveGroups()[archiveGroupName]
            group?.lastValStore?.findMatchingMessages(topic) { message ->
                val payload = String(message.payload, Charsets.UTF_8).trim()
                val numVal = extractNumericValue(payload, field)
                if (numVal != null) {
                    result.add(
                        JsonObject()
                            .put("metric", JsonObject()
                                .put("__name__", "monstermq_topic_value")
                                .put("topic", message.topicName)
                                .put("archive_group", archiveGroupName)
                                .put("field", field))
                            .put("value", JsonArray().add(now).add(numVal.toString()))
                    )
                }
                true
            }
        }

        sendVectorResult(ctx, result)
    }

    // ========== Helpers ==========

    private fun getParam(ctx: RoutingContext, name: String): String {
        return ctx.queryParams().get(name)
            ?: ctx.request().formAttributes().get(name)
            ?: ""
    }

    private fun buildSeriesEntry(topic: String, archiveGroup: String, field: String, values: JsonArray): JsonObject {
        return JsonObject()
            .put("metric", JsonObject()
                .put("__name__", "monstermq_topic_value")
                .put("topic", topic)
                .put("archive_group", archiveGroup)
                .put("field", field))
            .put("values", values)
    }

    private fun sendRangeResult(ctx: RoutingContext, result: JsonArray) {
        ctx.response().setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(JsonObject()
                .put("status", "success")
                .put("data", JsonObject()
                    .put("resultType", "matrix")
                    .put("result", result))
                .encode())
    }

    private fun sendVectorResult(ctx: RoutingContext, result: JsonArray) {
        ctx.response().setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(JsonObject()
                .put("status", "success")
                .put("data", JsonObject()
                    .put("resultType", "vector")
                    .put("result", result))
                .encode())
    }

    private fun extractNumericValue(payload: String, field: String): Double? {
        return if (field.isEmpty()) {
            payload.toDoubleOrNull()
                ?: try { JsonObject("{\"v\":$payload}").getDouble("v") } catch (e: Exception) { null }
        } else {
            extractJsonField(payload, field)
        }
    }

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

    private fun findColumnIndex(columns: JsonArray, name: String): Int {
        for (i in 0 until columns.size()) {
            if (columns.getString(i) == name) return i
        }
        return -1
    }

    // ========== PromQL Parsing ==========

    private data class ParsedQuery(
        val metric: String,
        val labels: Map<String, String>,
        val function: String?,   // "AVG", "MIN", "MAX", "COUNT" or null
        val window: String?
    )

    /**
     * Extract label key=value pairs from a PromQL selector string like
     * `monstermq_topic_value{archive_group="Default",topic="sensors/temp"}`.
     * Handles both equality (=) and regex (=~) matchers — regex is treated as equality
     * for simple values (no regex syntax) and ignored otherwise.
     */
    private fun extractLabelMatchers(selector: String): Map<String, String> {
        val labels = mutableMapOf<String, String>()
        Regex("""(\w+)=~?"([^"]*?)"""").findAll(selector).forEach { m ->
            labels[m.groupValues[1]] = m.groupValues[2]
        }
        return labels
    }

    private fun getMatchSelector(ctx: RoutingContext): String {
        return ctx.queryParams().getAll("match[]").firstOrNull()
            ?: ctx.request().formAttributes().getAll("match[]").firstOrNull()
            ?: ""
    }

    private fun parsePromQL(query: String): ParsedQuery {
        val q = query.trim()

        val funcPattern = Regex("""^(avg|min|max|count)_over_time\((.+)\[\w+]\s*\)$""", RegexOption.IGNORE_CASE)
        val funcMatch = funcPattern.find(q)

        val function: String?
        val window: String?
        val inner: String

        if (funcMatch != null) {
            function = funcMatch.groupValues[1].uppercase()
            inner = funcMatch.groupValues[2].trim()
            window = funcMatch.groupValues[3]
        } else {
            function = null
            window = null
            inner = q
        }

        val metricPattern = Regex("""^(\w+)\s*(\{[^}]*})?""")
        val ml = metricPattern.find(inner)
        val metric = ml?.groupValues?.get(1) ?: ""
        val labelsStr = ml?.groupValues?.get(2) ?: ""

        val labels = mutableMapOf<String, String>()
        if (labelsStr.isNotEmpty()) {
            Regex("""(\w+)="([^"]*?)"""").findAll(labelsStr).forEach { m ->
                labels[m.groupValues[1]] = m.groupValues[2]
            }
        }

        return ParsedQuery(metric, labels, function, window)
    }

    private fun parsePrometheusTime(s: String): Instant? {
        if (s.isEmpty()) return null
        return try {
            Instant.ofEpochMilli((s.toDouble() * 1000).toLong())
        } catch (e: Exception) {
            try { Instant.parse(s) } catch (e2: Exception) { null }
        }
    }

    // ========== Auth ==========

    private fun validateAuthentication(ctx: RoutingContext): Boolean {
        if (!userManager.isUserManagementEnabled()) return true

        val authHeader = ctx.request().getHeader("Authorization")

        if (authHeader == null) {
            if (userManager.isAnonymousEnabled()) {
                logger.fine("Prometheus API allowed for Anonymous user")
                return true
            }
            ctx.response().setStatusCode(401)
                .putHeader("Content-Type", "application/json")
                .putHeader("WWW-Authenticate", "Basic realm=\"MonsterMQ Prometheus\"")
                .end(JsonObject().put("error", "Authentication required").encode())
            return false
        }

        if (authHeader.startsWith("Bearer ", ignoreCase = true)) {
            val token = authHeader.substring(7)
            val username = JwtService.extractUsername(token)
            if (username != null && !JwtService.isTokenExpired(token)) {
                logger.fine("Prometheus API authenticated via JWT for user: $username")
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
                    userManager.authenticate(parts[0], parts[1]).onComplete { authResult ->
                        if (authResult.succeeded() && authResult.result()?.enabled == true) {
                            logger.fine("Prometheus API authenticated via Basic Auth for user: ${parts[0]}")
                            ctx.next()
                        } else {
                            ctx.response().setStatusCode(401)
                                .putHeader("Content-Type", "application/json")
                                .putHeader("WWW-Authenticate", "Basic realm=\"MonsterMQ Prometheus\"")
                                .end(JsonObject().put("error", "Invalid credentials").encode())
                        }
                    }
                    return false
                }
            } catch (e: Exception) {
                logger.warning("Prometheus API Basic Auth parsing error: ${e.message}")
            }
        }

        ctx.response().setStatusCode(401)
            .putHeader("Content-Type", "application/json")
            .putHeader("WWW-Authenticate", "Basic realm=\"MonsterMQ Prometheus\"")
            .end(JsonObject().put("error", "Authentication required").encode())
        return false
    }
}
