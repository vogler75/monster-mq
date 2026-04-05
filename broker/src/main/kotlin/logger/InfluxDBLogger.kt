package at.rocworks.logger

import at.rocworks.bus.EventBusAddresses
import at.rocworks.stores.devices.InfluxDBLoggerConfig
import io.vertx.core.buffer.Buffer
import io.vertx.ext.web.client.HttpRequest

/**
 * InfluxDB Logger implementation.
 * Formats data as InfluxDB Line Protocol and sends via HTTP POST.
 */
class InfluxDBLogger : HttpLoggerBase<InfluxDBLoggerConfig>() {

    override fun loadConfig(cfgJson: io.vertx.core.json.JsonObject): InfluxDBLoggerConfig {
        return InfluxDBLoggerConfig.fromJson(cfgJson)
    }

    override fun getMetricsAddress(): String {
        return EventBusAddresses.InfluxDBLoggerBridge.connectorMetrics(device.name)
    }

    override fun getEndpointUrl(): String = cfg.endpointUrl

    override fun getAuthType(): String = cfg.authType

    override fun applyAuth(request: HttpRequest<Buffer>) {
        when (cfg.authType.uppercase()) {
            "TOKEN" -> cfg.token?.let { request.putHeader("Authorization", "Token $it") }
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

        val payload = rows.joinToString("\n") { formatInfluxLine(it) }

        sendPost(payload) { request ->
            // InfluxDB v2 query params
            if (cfg.org != null) request.addQueryParam("org", cfg.org)
            if (cfg.bucket != null) request.addQueryParam("bucket", cfg.bucket)
            // InfluxDB v1 query params
            if (cfg.db != null) request.addQueryParam("db", cfg.db)
            if (cfg.db != null && cfg.authType.uppercase() == "BASIC") {
                // v1 also accepts credentials as query params
                if (cfg.username != null) request.addQueryParam("u", cfg.username)
                if (cfg.password != null) request.addQueryParam("p", cfg.password)
            }
        }
    }

    private fun formatInfluxLine(row: BufferedRow): String {
        val measurement = escapeKey(row.tableName)

        val tags = mutableMapOf<String, String>()
        val fields = mutableMapOf<String, String>()

        // Add topic as tag if topicNameColumn is configured
        if (cfg.topicNameColumn != null) {
            tags[escapeKey(cfg.topicNameColumn!!)] = escapeKey(row.topic)
        }

        row.fields.forEach { (key, value) ->
            if (value != null) {
                if (cfg.influxTags.contains(key)) {
                    tags[escapeKey(key)] = escapeKey(value.toString())
                } else {
                    fields[escapeKey(key)] = formatFieldValue(value)
                }
            }
        }

        val tagString = if (tags.isNotEmpty()) "," + tags.entries.joinToString(",") { "${it.key}=${it.value}" } else ""
        val fieldString = fields.entries.joinToString(",") { "${it.key}=${it.value}" }
        val timestampNanos = (row.timestamp.epochSecond * 1_000_000_000L) + row.timestamp.nano

        return "$measurement$tagString $fieldString $timestampNanos"
    }

    private fun escapeKey(key: String): String {
        return key.replace(",", "\\,")
            .replace(" ", "\\ ")
            .replace("=", "\\=")
    }

    private fun formatFieldValue(value: Any): String {
        return when (value) {
            is String -> "\"" + value.replace("\"", "\\\"") + "\""
            is Boolean -> if (value) "T" else "F"
            is Number -> {
                if (value is Long || value is Int || value is Short || value is Byte) {
                    "${value}i"
                } else {
                    value.toString()
                }
            }
            else -> "\"" + value.toString().replace("\"", "\\\"") + "\""
        }
    }
}
