package at.rocworks.logger

import at.rocworks.bus.EventBusAddresses
import at.rocworks.stores.devices.TimeBaseLoggerConfig
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.HttpRequest
import java.time.Instant
import java.time.format.DateTimeFormatter

/**
 * TimeBase Logger implementation.
 * Sends data as TVQ (Time-Value-Quality) entries to the TimeBase Historian REST API.
 * Each MQTT topic maps to a TimeBase tag within a dataset.
 *
 * Timestamp handling:
 * - If the JSON schema defines a field with format "timestamp" or "timestampms",
 *   that parsed timestamp is used (available via row.timestamp).
 * - If no timestamp field is defined, the MQTT message arrival time is used.
 */
class TimeBaseLogger : HttpLoggerBase<TimeBaseLoggerConfig>() {

    override fun loadConfig(cfgJson: JsonObject): TimeBaseLoggerConfig {
        return TimeBaseLoggerConfig.fromJson(cfgJson)
    }

    override fun getMetricsAddress(): String {
        return EventBusAddresses.TimeBaseLoggerBridge.connectorMetrics(device.name)
    }

    override fun connect(): Future<Void> {
        return super.connect().compose {
            logger.info("TimeBase logger connected to ${cfg.endpointUrl}, dataset: ${cfg.tableName ?: "(dynamic)"}")
            if (cfg.tableName != null) {
                ensureDatasetExists(cfg.tableName!!)
            } else {
                Future.succeededFuture()
            }
        }
    }

    /**
     * Check if dataset exists via GET /datasets, create it if missing via POST /datasets.
     * Non-blocking — uses Vert.x WebClient async calls.
     */
    private fun ensureDatasetExists(dataset: String): Future<Void> {
        val promise = Promise.promise<Void>()
        val client = this.client ?: return Future.failedFuture("WebClient not initialized")

        // Step 1: GET /datasets to check if it already exists
        val getUrl = "${cfg.endpointUrl}/datasets"
        logger.fine("TimeBase: checking existing datasets at GET $getUrl")
        val getRequest = client.getAbs(getUrl)
        applyAuth(getRequest)
        applyHeaders(getRequest)

        getRequest.send()
            .onSuccess { response ->
                logger.fine("TimeBase: GET /datasets returned ${response.statusCode()}: ${response.bodyAsString()?.take(500)}")
                if (response.statusCode() in 200..299) {
                    val datasets = try { response.bodyAsJsonArray() } catch (e: Exception) { JsonArray() }
                    val exists = datasets.any { entry ->
                        entry is JsonObject && entry.getString("n") == dataset
                    }
                    if (exists) {
                        logger.info("TimeBase dataset '$dataset' already exists")
                        promise.complete()
                    } else {
                        // Step 2: POST /datasets to create it
                        createDataset(dataset).onComplete { promise.handle(it) }
                    }
                } else {
                    logger.warning("TimeBase: failed to list datasets (HTTP ${response.statusCode()}), attempting to create '$dataset' anyway")
                    createDataset(dataset).onComplete { promise.handle(it) }
                }
            }
            .onFailure { error ->
                logger.warning("TimeBase: failed to check datasets: ${error.message}, attempting to create '$dataset' anyway")
                createDataset(dataset).onComplete { promise.handle(it) }
            }

        return promise.future()
    }

    /**
     * Create a dataset via POST /datasets.
     * Uses sensible defaults: purge age 90 days, no size limit.
     */
    private fun createDataset(dataset: String): Future<Void> {
        val promise = Promise.promise<Void>()
        val client = this.client ?: return Future.failedFuture("WebClient not initialized")
        val url = "${cfg.endpointUrl}/datasets"
        val body = JsonObject()
            .put("n", dataset)
            .put("pa", 90)   // purge age: 90 days
            .put("ps", 0)    // purge size: unlimited
        logger.fine("TimeBase: POST $url body=${body.encode()}")
        val request = client.postAbs(url)
        applyAuth(request)
        applyHeaders(request)
        request.putHeader("Content-Type", "application/json")
        request.sendJsonObject(body)
            .onSuccess { response ->
                if (response.statusCode() in 200..299) {
                    logger.info("TimeBase dataset '$dataset' created successfully")
                } else {
                    logger.warning("TimeBase: failed to create dataset '$dataset': HTTP ${response.statusCode()} ${response.bodyAsString()}")
                }
                promise.complete()
            }
            .onFailure { error ->
                logger.warning("TimeBase: failed to create dataset '$dataset': ${error.message}")
                promise.complete()
            }
        return promise.future()
    }

    override fun getEndpointUrl(): String = cfg.endpointUrl

    override fun getAuthType(): String = cfg.authType

    override fun applyAuth(request: HttpRequest<Buffer>) {
        when (cfg.authType.uppercase()) {
            "TOKEN" -> cfg.token?.let { request.putHeader("Authorization", "Bearer $it") }
            "BASIC" -> if (cfg.username != null && cfg.password != null) {
                request.basicAuthentication(cfg.username, cfg.password)
            }
        }
    }

    override fun applyHeaders(request: HttpRequest<Buffer>) {
        cfg.headers.forEach { (k, v) -> request.putHeader(k, v.toString()) }
    }

    override fun writeBulk(tableName: String, rows: List<BufferedRow>) {
        if (rows.isEmpty()) return

        // Group TVQ entries by tag name.
        // If valueField is set and found, use topic as tag name with that single value.
        // For each field in the row, create a tag: {topic}/{fieldName} if multiple fields,
        // or just {topic} if valueField matches a single field.
        val tagData = mutableMapOf<String, MutableList<JsonObject>>()

        rows.forEach { row ->
            val ts = formatTimestamp(row.timestamp)

            // Filter out timestamp fields (java.sql.Timestamp) — they are metadata, not values
            val valueFields = row.fields.filter { (_, v) -> v != null && v !is java.sql.Timestamp }

            if (valueFields.size == 1) {
                // Only one field — use topic as tag name
                val tvq = JsonObject().put("t", ts).put("v", valueFields.values.first()).put("q", 0)
                tagData.getOrPut(row.topic) { mutableListOf() }.add(tvq)
            } else if (valueFields.isNotEmpty()) {
                // Multiple fields — each becomes its own tag: {topic}/{fieldName}
                valueFields.forEach { (fieldName, value) ->
                    val tagName = "${row.topic}/$fieldName"
                    val tvq = JsonObject().put("t", ts).put("v", value).put("q", 0)
                    tagData.getOrPut(tagName) { mutableListOf() }.add(tvq)
                }
            }
        }

        if (tagData.isEmpty()) {
            logger.fine { "TimeBase writeBulk: no values extracted from ${rows.size} rows, skipping" }
            return
        }

        val body = JsonObject()
        tagData.forEach { (tag, tvqs) -> body.put(tag, JsonArray(tvqs)) }

        // POST to {baseUrl}/datasets/{dataset}/data
        val url = "${cfg.endpointUrl}/datasets/${tableName}/data"
        logger.fine { "TimeBase writeBulk: POST $url tags=${tagData.size} rows=${rows.size}" }
        logger.finest { "TimeBase writeBulk body: ${body.encode().take(1000)}" }
        sendPostTo(url, body.encode()) { request ->
            request.putHeader("Content-Type", "application/json")
        }
    }

    private fun formatTimestamp(instant: Instant): String {
        return DateTimeFormatter.ISO_INSTANT.format(instant)
    }
}
