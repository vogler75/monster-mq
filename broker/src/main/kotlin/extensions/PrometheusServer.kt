package at.rocworks.extensions

import at.rocworks.Monster
import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.auth.UserManager
import at.rocworks.bus.EventBusAddresses
import at.rocworks.extensions.graphql.JwtService
import at.rocworks.extensions.graphql.BrokerMetrics
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.stores.IMessageArchiveExtended
import at.rocworks.stores.IMetricsStore

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
    private val userManager: UserManager,
    private val metricsStore: IMetricsStore? = null,
    private val rawQueryLimit: Int = 10000
) : AbstractVerticle() {

    private val logger = Utils.getLogger(this::class.java).also { it.level = Const.DEBUG_LEVEL }

    companion object {
        private val METRIC_NAMES = listOf("topics", "metrics")

        // Label values for metrics{metric="..."}
        val BROKER_METRIC_LABELS = listOf(
            "messages_in", "messages_out",
            "sessions", "subscriptions", "queue_depth",
            "bus_in", "bus_out",
            "mqtt_client_in", "mqtt_client_out",
            "kafka_client_in", "kafka_client_out",
            "opcua_client_in",
            "winccoa_client_in", "winccua_client_in",
            "neo4j_client_in"
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
                logger.fine("Prometheus Server started on port ${server.actualPort()}")
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
        appendMetricComment(sb, "metrics", "gauge", "MonsterMQ broker metric")
        results.forEach { (nodeId, nm) ->
            val values = mapOf(
                "messages_in"      to nm.getDouble("messagesInRate", 0.0),
                "messages_out"     to nm.getDouble("messagesOutRate", 0.0),
                "sessions"         to nm.getInteger("nodeSessionCount", 0).toDouble(),
                "subscriptions"    to nm.getInteger("subscriptionCount", 0).toDouble(),
                "queue_depth"      to nm.getInteger("queuedMessagesCount", 0).toDouble(),
                "bus_in"           to nm.getDouble("messageBusInRate", 0.0),
                "bus_out"          to nm.getDouble("messageBusOutRate", 0.0)
            )
            values.forEach { (metricLabel, value) ->
                appendMetric(sb, "metrics", mapOf("node" to nodeId, "metric" to metricLabel), value)
            }
        }
    }

    private fun appendTopicValueMetrics(sb: StringBuilder) {
        appendMetricComment(sb, "topics", "gauge", "Current value of MQTT topic")
        archiveHandler.getDeployedArchiveGroups().forEach { (groupName, group) ->
            group.lastValStore?.findMatchingMessages("#") { message ->
                val payload = String(message.payload, Charsets.UTF_8).trim()
                val topicName = message.topicName
                val numVal = payload.toDoubleOrNull()
                if (numVal != null) {
                    appendMetric(sb, "topics",
                        mapOf("group" to groupName, "topic" to topicName, "field" to ""),
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
                                appendMetric(sb, "topics",
                                    mapOf("group" to groupName, "topic" to topicName, "field" to entry.key),
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
        val data = JsonArray()
            .add("__name__").add("group").add("topic").add("field").add("node").add("metric")
        ctx.response().setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(JsonObject().put("status", "success").put("data", data).encode())
    }

    private fun handleLabelValues(ctx: RoutingContext) {
        val matchSelector = getMatchSelector(ctx)
        val matchLabels = extractLabelMatchers(matchSelector)
        val groupFilter = matchLabels["group"]
        val topicFilter = matchLabels["topic"]

        fun groupsToScan() = archiveHandler.getDeployedArchiveGroups()
            .filter { (name, _) -> groupFilter == null || name == groupFilter }

        val groups = groupsToScan()
        logger.fine("label/${ctx.pathParam("name")}/values: group=${groupFilter ?: "(all)"} topic=${topicFilter ?: "(all)"} scanning groups=${groups.keys}")

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
                        logger.fine("label/topic/values: group='$groupName' → ${topics.size} topics found")
                    }
                }
                ctx.response().setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .end(JsonObject().put("status", "success").put("data", JsonArray(topics.sorted())).encode())
            }
            "group" -> {
                val allGroups = archiveHandler.getDeployedArchiveGroups().keys.sorted()
                logger.fine("label/group/values: returning ${allGroups.size} groups: $allGroups")
                ctx.response().setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .end(JsonObject().put("status", "success").put("data", JsonArray(allGroups)).encode())
            }
            "field" -> {
                if (topicFilter == null)
                    logger.fine("label/field/values: no topic filter in match[] — returning fields from all topics in groups=${groups.keys}")
                else
                    logger.fine("label/field/values: topic='$topicFilter' group=${groupFilter ?: "(all)"}")
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
                    logger.fine("label/field/values: group='$groupName' → fields so far: $fields")
                }
                ctx.response().setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .end(JsonObject().put("status", "success").put("data", JsonArray(fields.sorted())).encode())
            }
            "metric" -> {
                logger.fine("label/metric/values: returning ${BROKER_METRIC_LABELS.size} broker metric labels")
                ctx.response().setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .end(JsonObject().put("status", "success").put("data", JsonArray(BROKER_METRIC_LABELS)).encode())
            }
            "node" -> {
                val nodeIds = Monster.getClusterNodeIds(vertx).sorted()
                logger.fine("label/node/values: returning ${nodeIds.size} nodes: $nodeIds")
                ctx.response().setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .end(JsonObject().put("status", "success").put("data", JsonArray(nodeIds)).encode())
            }
            else -> {
                logger.fine("label/$labelName/values: unknown label, returning empty")
                ctx.response().setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .end(JsonObject().put("status", "success").put("data", JsonArray()).encode())
            }
        }
    }

    private fun handleSeries(ctx: RoutingContext) {
        val matchSelector = getMatchSelector(ctx)
        val matchLabels = extractLabelMatchers(matchSelector)
        val groupFilter = matchLabels["group"]
        val topicFilter = matchLabels["topic"]
        val metricName = matchLabels["__name__"]

        logger.fine("series: __name__=${metricName ?: "(all)"} group=${groupFilter ?: "(all)"} topic=${topicFilter ?: "(all)"}")
        val data = JsonArray()

        // Topic value series
        if (metricName == null || metricName == "topics") {
            archiveHandler.getDeployedArchiveGroups()
                .filter { (name, _) -> groupFilter == null || name == groupFilter }
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
                        if (fields.isEmpty()) fields.add("")
                        fields.forEach { field ->
                            data.add(
                                JsonObject()
                                    .put("__name__", "topics")
                                    .put("group", groupName)
                                    .put("topic", message.topicName)
                                    .put("field", field)
                            )
                        }
                        data.size() < 5000
                    }
                }
        }

        // Broker metric series
        if (metricName == null || metricName == "metrics") {
            val nodeIds = Monster.getClusterNodeIds(vertx)
            nodeIds.forEach { nodeId ->
                BROKER_METRIC_LABELS.forEach { metricLabel ->
                    data.add(
                        JsonObject()
                            .put("__name__", "metrics")
                            .put("node", nodeId)
                            .put("metric", metricLabel)
                    )
                }
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

        logger.fine("query_range: query='$queryStr' start=$startStr end=$endStr step=$stepStr")

        if (queryStr.isEmpty()) {
            sendRangeResult(ctx, JsonArray())
            return
        }

        val startTime = parsePrometheusTime(startStr) ?: Instant.now().minusSeconds(3600)
        val endTime = parsePrometheusTime(endStr) ?: Instant.now()
        val stepSeconds = stepStr.toDoubleOrNull()?.toLong() ?: 60L

        val parsed = parsePromQL(queryStr)

        // ---- metrics: query from MetricsStore history ----
        if (parsed.metric == "metrics") {
            handleQueryRangeMetric(ctx, parsed, startTime, endTime, stepSeconds)
            return
        }

        // ---- topics: query from archive ----
        val intervalMinutes = snapToValidIntervalMinutes(stepSeconds)
        val topic = parsed.labels["topic"] ?: ""
        val groupName = parsed.labels["group"] ?: "Default"
        val field = parsed.labels["field"] ?: ""

        logger.fine("query_range: topic='$topic' group='$groupName' field='$field' function=${parsed.function ?: "raw"} interval=${intervalMinutes}min")

        if (topic.isEmpty()) {
            logger.fine("query_range: no topic label in query, returning empty")
            sendRangeResult(ctx, JsonArray())
            return
        }

        val archiveStore = archiveHandler.getDeployedArchiveGroups()[groupName]?.archiveStore as? IMessageArchiveExtended
        if (archiveStore == null) {
            logger.warning("query_range: no IMessageArchiveExtended found for archive group '$groupName'")
            sendRangeResult(ctx, JsonArray())
            return
        }

        val result = JsonArray()

        if (stepSeconds < 60 && parsed.function == null) {
            // Sub-minute step with no explicit function: return raw records up to the configured limit
            logger.fine("query_range: raw query — step=${stepSeconds}s < 60s, calling getHistory limit=$rawQueryLimit")
            val historyData = archiveStore.getHistory(
                topic = topic,
                startTime = startTime,
                endTime = endTime,
                limit = rawQueryLimit
            )
            logger.fine("query_range: raw result — ${historyData.size()} records")

            val raw = mutableListOf<Pair<Long, Double>>()
            for (i in 0 until historyData.size()) {
                val record = historyData.getJsonObject(i) ?: continue
                val timestampMs = record.getLong("timestamp") ?: continue
                val payloadStr = record.getString("payload_json")
                    ?: record.getString("payload")
                    ?: record.getString("payload_base64")?.let { b64 ->
                        try { String(Base64.getDecoder().decode(b64)) } catch (e: Exception) { null }
                    } ?: continue
                val numVal = extractNumericValue(payloadStr, field) ?: continue
                raw.add(timestampMs / 1000 to numVal)
            }

            val values = downsampleByStep(raw, startTime.epochSecond, endTime.epochSecond, stepSeconds)
            logger.fine("query_range: returning ${values.size()} data points (step=${stepSeconds}s, raw=${raw.size})")
            result.add(buildTopicSeriesEntry(topic, groupName, field, values))
        } else {
            // step >= 60s or explicit function: use getAggregatedHistory (default to AVG)
            val aggFunction = parsed.function ?: "AVG"
            val fields = if (field.isEmpty()) emptyList() else listOf(field)
            logger.fine("query_range: aggregated query — calling getAggregatedHistory interval=${intervalMinutes}min function=$aggFunction")
            val aggResult = archiveStore.getAggregatedHistory(
                topics = listOf(topic),
                startTime = startTime,
                endTime = endTime,
                intervalMinutes = intervalMinutes,
                functions = listOf(aggFunction),
                fields = fields
            )

            val columns = aggResult.getJsonArray("columns") ?: JsonArray()
            val rows = aggResult.getJsonArray("rows") ?: JsonArray()
            logger.fine("query_range: aggregated result — ${rows.size()} rows, columns=$columns")

            val fieldSuffix = if (field.isEmpty()) "" else ".${field.replace(".", "_")}"
            val colName = "$topic${fieldSuffix}_${aggFunction.lowercase()}"
            val colIndex = findColumnIndex(columns, colName)

            if (colIndex < 0) {
                logger.warning("query_range: column '$colName' not found in result columns=$columns")
            } else {
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
                logger.fine("query_range: returning ${values.size()} aggregated data points")
                result.add(buildTopicSeriesEntry(topic, groupName, field, values))
            }
        }

        sendRangeResult(ctx, result)
    }

    private fun handleQueryRangeMetric(
        ctx: RoutingContext,
        parsed: ParsedQuery,
        startTime: Instant,
        endTime: Instant,
        stepSeconds: Long
    ) {
        val metricLabel = parsed.labels["metric"] ?: ""
        val nodeFilter = parsed.labels["node"]
        val nodeIds = Monster.getClusterNodeIds(vertx).filter { nodeFilter == null || it == nodeFilter }

        logger.fine("query_range/metric: metric='$metricLabel' node=${nodeFilter ?: "(all)"} nodes=$nodeIds")

        if (nodeIds.isEmpty()) {
            sendRangeResult(ctx, JsonArray())
            return
        }

        if (metricsStore == null) {
            logger.warning("query_range/metric: no MetricsStore configured — cannot query historical broker metrics")
            sendRangeResult(ctx, JsonArray())
            return
        }

        val result = java.util.concurrent.CopyOnWriteArrayList<JsonObject>()
        val pending = java.util.concurrent.atomic.AtomicInteger(nodeIds.size)

        nodeIds.forEach { nodeId ->
            metricsStore.getBrokerMetricsHistory(nodeId, startTime, endTime, null)
                .onComplete { ar ->
                    if (ar.succeeded()) {
                        val history = ar.result()
                        val raw = history.mapNotNull { (ts, metrics) ->
                            val v = extractBrokerMetricValue(metrics, metricLabel) ?: return@mapNotNull null
                            ts.epochSecond to v
                        }
                        val values = downsampleByStep(raw, startTime.epochSecond, endTime.epochSecond, stepSeconds)
                        logger.fine("query_range/metric: node='$nodeId' metric='$metricLabel' → ${history.size} history points → ${values.size()} result points")
                        result.add(buildMetricSeriesEntry(nodeId, metricLabel, values))
                    } else {
                        logger.warning("query_range/metric: failed to get history for node '$nodeId': ${ar.cause()?.message}")
                    }
                    if (pending.decrementAndGet() == 0) {
                        sendRangeResult(ctx, JsonArray(result))
                    }
                }
        }
    }

    private fun handleQuery(ctx: RoutingContext) {
        val queryStr = getParam(ctx, "query")
        val now = Instant.now().epochSecond

        logger.fine("query: query='$queryStr'")

        if (queryStr.isEmpty()) {
            sendVectorResult(ctx, JsonArray())
            return
        }

        val parsed = parsePromQL(queryStr)

        // ---- metrics: query from event bus (current value) ----
        if (parsed.metric == "metrics") {
            val metricLabel = parsed.labels["metric"] ?: ""
            val nodeFilter = parsed.labels["node"]
            val nodeIds = Monster.getClusterNodeIds(vertx).filter { nodeFilter == null || it == nodeFilter }

            logger.fine("query/metric: metric='$metricLabel' node=${nodeFilter ?: "(all)"} nodes=$nodeIds")

            if (nodeIds.isEmpty()) {
                sendVectorResult(ctx, JsonArray())
                return
            }

            val result = java.util.concurrent.CopyOnWriteArrayList<JsonObject>()
            val pending = java.util.concurrent.atomic.AtomicInteger(nodeIds.size)

            nodeIds.forEach { nodeId ->
                vertx.eventBus().request<JsonObject>(EventBusAddresses.Node.metrics(nodeId), JsonObject())
                    .onComplete { ar ->
                        if (ar.succeeded()) {
                            val nm = ar.result().body()
                            val value = extractEventBusMetricValue(nm, metricLabel)
                            if (value != null) {
                                result.add(
                                    JsonObject()
                                        .put("metric", JsonObject()
                                            .put("__name__", "metrics")
                                            .put("node", nodeId)
                                            .put("metric", metricLabel))
                                        .put("value", JsonArray().add(now).add(value.toString()))
                                )
                            }
                        }
                        if (pending.decrementAndGet() == 0) {
                            sendVectorResult(ctx, JsonArray(result))
                        }
                    }
            }
            return
        }

        // ---- topics: query from LastValueStore ----
        val topic = parsed.labels["topic"] ?: ""
        val groupName = parsed.labels["group"] ?: "Default"
        val field = parsed.labels["field"] ?: ""

        val result = JsonArray()

        if (topic.isNotEmpty()) {
            val group = archiveHandler.getDeployedArchiveGroups()[groupName]
            group?.lastValStore?.findMatchingMessages(topic) { message ->
                val payload = String(message.payload, Charsets.UTF_8).trim()
                val numVal = extractNumericValue(payload, field)
                if (numVal != null) {
                    result.add(
                        JsonObject()
                            .put("metric", JsonObject()
                                .put("__name__", "topics")
                                .put("group", groupName)
                                .put("topic", message.topicName)
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

    private fun buildTopicSeriesEntry(topic: String, group: String, field: String, values: JsonArray): JsonObject {
        return JsonObject()
            .put("metric", JsonObject()
                .put("__name__", "topics")
                .put("group", group)
                .put("topic", topic)
                .put("field", field))
            .put("values", values)
    }

    private fun buildMetricSeriesEntry(nodeId: String, metricLabel: String, values: JsonArray): JsonObject {
        return JsonObject()
            .put("metric", JsonObject()
                .put("__name__", "metrics")
                .put("node", nodeId)
                .put("metric", metricLabel))
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
     * `topics{archive_group="Default",topic="sensors/temp"}`.
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

    /**
     * Snap a step (in seconds) to the nearest supported aggregation interval in minutes.
     * getAggregatedHistory supports: 1, 5, 15, 60, 1440 minutes.
     */
    private fun snapToValidIntervalMinutes(stepSeconds: Long): Int {
        val minutes = (stepSeconds / 60).coerceAtLeast(1)
        return when {
            minutes < 5    -> 1
            minutes < 15   -> 5
            minutes < 60   -> 15
            minutes < 1440 -> 60
            else           -> 1440
        }
    }

    /**
     * Bucket raw (epochSeconds, value) pairs into step-sized intervals and return
     * the average per bucket. Each bucket is aligned to multiples of stepSeconds.
     * Buckets with no data are omitted.
     */
    private fun downsampleByStep(
        raw: List<Pair<Long, Double>>,
        startEpoch: Long,
        endEpoch: Long,
        stepSeconds: Long
    ): JsonArray {
        val step = stepSeconds.coerceAtLeast(1)
        // Group values by bucket index
        val buckets = mutableMapOf<Long, MutableList<Double>>()
        for ((ts, v) in raw) {
            val bucket = (ts - startEpoch) / step
            buckets.getOrPut(bucket) { mutableListOf() }.add(v)
        }
        val values = JsonArray()
        buckets.entries.sortedBy { it.key }.forEach { (bucket, vals) ->
            val ts = startEpoch + bucket * step
            if (ts <= endEpoch) {
                val avg = vals.average()
                values.add(JsonArray().add(ts).add(avg.toString()))
            }
        }
        return values
    }

    private fun parsePrometheusTime(s: String): Instant? {
        if (s.isEmpty()) return null
        return try {
            Instant.ofEpochMilli((s.toDouble() * 1000).toLong())
        } catch (e: Exception) {
            try { Instant.parse(s) } catch (e2: Exception) { null }
        }
    }

    /** Map a metric label string to the corresponding field in BrokerMetrics (from MetricsStore). */
    private fun extractBrokerMetricValue(metrics: BrokerMetrics, label: String): Double? = when (label) {
        "messages_in"      -> metrics.messagesIn
        "messages_out"     -> metrics.messagesOut
        "sessions"         -> metrics.nodeSessionCount.toDouble()
        "subscriptions"    -> metrics.subscriptionCount.toDouble()
        "queue_depth"      -> metrics.queuedMessagesCount.toDouble()
        "bus_in"           -> metrics.messageBusIn
        "bus_out"          -> metrics.messageBusOut
        "mqtt_client_in"   -> metrics.mqttClientIn
        "mqtt_client_out"  -> metrics.mqttClientOut
        "kafka_client_in"  -> metrics.kafkaClientIn
        "kafka_client_out" -> metrics.kafkaClientOut
        "opcua_client_in"  -> metrics.opcUaClientIn
        "winccoa_client_in"-> metrics.winCCOaClientIn
        "winccua_client_in"-> metrics.winCCUaClientIn
        "neo4j_client_in"  -> metrics.neo4jClientIn
        else -> null
    }

    /** Map a metric label string to the corresponding field in the event bus metrics JSON. */
    private fun extractEventBusMetricValue(nm: JsonObject, label: String): Double? = when (label) {
        "messages_in"   -> nm.getDouble("messagesInRate")
        "messages_out"  -> nm.getDouble("messagesOutRate")
        "sessions"      -> nm.getInteger("nodeSessionCount")?.toDouble()
        "subscriptions" -> nm.getInteger("subscriptionCount")?.toDouble()
        "queue_depth"   -> nm.getInteger("queuedMessagesCount")?.toDouble()
        "bus_in"        -> nm.getDouble("messageBusInRate")
        "bus_out"       -> nm.getDouble("messageBusOutRate")
        else -> null
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
