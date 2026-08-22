package at.rocworks.extensions.redfish

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.time.format.DateTimeParseException

object RedfishMapper {

    /**
     * Extracts normalized sensor records from an incoming MQTT JSON payload according to gateway configuration.
     */
    fun extractSensorRecords(
        payload: ByteArray,
        topic: String,
        gw: GatewayConfig
    ): List<NormalizedSensorRecord> {
        if (payload.isEmpty()) return emptyList()

        val jsonStr = try {
            String(payload, Charsets.UTF_8).trim()
        } catch (e: Exception) {
            return emptyList()
        }
        if (jsonStr.isEmpty()) return emptyList()

        val root: Any = try {
            if (jsonStr.startsWith("{")) {
                JsonObject(jsonStr).map
            } else if (jsonStr.startsWith("[")) {
                JsonArray(jsonStr).list
            } else {
                return emptyList()
            }
        } catch (e: Exception) {
            return emptyList()
        }

        val jsonSchema = gw.jsonSchema
        val mapping = (jsonSchema["mapping"] as? Map<*, *>)?.mapKeys { it.key.toString() }?.mapValues { it.value.toString() }
        val props = jsonSchema["properties"] as? Map<*, *>
        val arrayPath = jsonSchema["arrayPath"] as? String

        val items = mutableListOf<Map<String, Any?>>()
        val rootMap = root as? Map<String, Any?>

        if (!arrayPath.isNullOrBlank()) {
            val (arrayVal, ok) = evaluateJsonPath(root, arrayPath)
            if (ok && arrayVal is List<*>) {
                for (elem in arrayVal) {
                    if (elem is Map<*, *>) {
                        val merged = mutableMapOf<String, Any?>()
                        if (rootMap != null) {
                            merged.putAll(rootMap)
                        }
                        for ((k, v) in elem) {
                            if (k != null) merged[k.toString()] = v
                        }
                        items.add(merged)
                    }
                }
            }
        } else if (rootMap != null) {
            items.add(rootMap)
        } else if (root is List<*>) {
            for (elem in root) {
                if (elem is Map<*, *>) {
                    @Suppress("UNCHECKED_CAST")
                    items.add(elem as Map<String, Any?>)
                }
            }
        }

        if (items.isEmpty()) return emptyList()

        val results = mutableListOf<NormalizedSensorRecord>()
        val now = Instant.now()
        val topicPrefix = gw.topicPrefix.ifBlank { "redfish" }
        val defaultChassisId = gw.chassisId.ifBlank { "EdgeNode" }

        for (item in items) {
            val record = mapSingleRecord(item, topic, topicPrefix, defaultChassisId, gw, props, mapping, now)
            if (record != null) {
                results.add(record)
            }
        }

        return results
    }

    private fun mapSingleRecord(
        data: Map<String, Any?>,
        topic: String,
        topicPrefix: String,
        defaultChassisId: String,
        gw: GatewayConfig,
        props: Map<*, *>?,
        mapping: Map<String, String>?,
        fallbackTime: Instant
    ): NormalizedSensorRecord? {
        // 1. Reading (required)
        val readingVal = extractFieldValue("reading", data, props, mapping) ?: return null
        val readingDouble = toDouble(readingVal) ?: return null

        // 2. Sensor ID
        var sensorId = extractFieldValue("sensorId", data, props, mapping)?.toString()?.trim()
        if (sensorId.isNullOrEmpty()) {
            val parts = topic.trim('/').split('/')
            sensorId = if (parts.isNotEmpty() && parts.last().isNotBlank()) parts.last() else "sensor"
        }

        // 3. Chassis ID
        var chassisId = extractFieldValue("chassisId", data, props, mapping)?.toString()?.trim()
        if (chassisId.isNullOrEmpty()) {
            chassisId = defaultChassisId
        }

        // 4. Name
        var name = extractFieldValue("name", data, props, mapping)?.toString()?.trim()
        if (name.isNullOrEmpty()) {
            name = sensorId
        }

        // 5. ReadingType
        var readingType = extractFieldValue("readingType", data, props, mapping)?.toString()?.trim()
        if (readingType.isNullOrEmpty()) {
            readingType = gw.defaultReadingType.ifBlank { "Temperature" }
        }

        // 6. ReadingUnits
        var readingUnits = extractFieldValue("readingUnits", data, props, mapping)?.toString()?.trim()
        if (readingUnits.isNullOrEmpty()) {
            readingUnits = gw.defaultReadingUnits.ifBlank {
                when {
                    readingType.equals("Temperature", ignoreCase = true) -> "Cel"
                    readingType.equals("Voltage", ignoreCase = true) -> "V"
                    readingType.equals("Current", ignoreCase = true) -> "A"
                    readingType.equals("Power", ignoreCase = true) -> "W"
                    readingType.equals("Humidity", ignoreCase = true) -> "%"
                    readingType.equals("Pressure", ignoreCase = true) -> "Pa"
                    readingType.equals("EnergykWh", ignoreCase = true) -> "kWh"
                    readingType.equals("LiquidFlow", ignoreCase = true) -> "L/min"
                    else -> "Cel"
                }
            }
        }

        // 7. Timestamp
        val tsVal = extractFieldValue("ts", data, props, mapping)
            ?: extractFieldValue("timestamp", data, props, mapping)
        val timestampStr = if (tsVal != null) parseTimestamp(tsVal) else RedfishUtils.formatTimeRFC3339(fallbackTime)

        // 8. Range Min / Max
        val rangeMin = extractFieldValue("rangeMin", data, props, mapping)?.let { toDouble(it) }
        val rangeMax = extractFieldValue("rangeMax", data, props, mapping)?.let { toDouble(it) }

        // 9. State & Health
        val state = extractFieldValue("state", data, props, mapping)?.toString()?.trim()?.ifBlank { "Enabled" } ?: "Enabled"
        var health = extractFieldValue("health", data, props, mapping)?.toString()?.trim()
        if (health.isNullOrEmpty()) {
            health = RedfishUtils.calculateHealth(readingDouble, gw.thresholds)
        }

        return NormalizedSensorRecord(
            chassisId = chassisId,
            sensorId = sensorId,
            name = name,
            reading = readingDouble,
            readingType = readingType,
            readingUnits = readingUnits,
            rangeMin = rangeMin,
            rangeMax = rangeMax,
            state = state,
            health = health,
            thresholds = gw.thresholds,
            sourceTopic = topic,
            timestamp = timestampStr,
            gatewayName = "",
            topicPrefix = topicPrefix
        )
    }

    private fun extractFieldValue(
        fieldName: String,
        data: Map<String, Any?>,
        props: Map<*, *>?,
        mapping: Map<String, String>?
    ): Any? {
        if (mapping != null) {
            val jsonPath = mapping[fieldName]
            if (!jsonPath.isNullOrBlank()) {
                val (v, ok) = evaluateJsonPath(data, jsonPath)
                if (ok && v != null) return v
            }
        }
        return data[fieldName]
    }

    /**
     * Evaluates a dot/bracket JSONPath on an arbitrary object structure.
     * Examples:
     *   "$.temperature" -> data["temperature"]
     *   "$.metrics.temp" -> data["metrics"]["temp"]
     *   "$.sensors[*]" -> all elements of data["sensors"] list
     *   "$.items[0].value" -> data["items"][0]["value"]
     */
    fun evaluateJsonPath(root: Any?, path: String): Pair<Any?, Boolean> {
        if (root == null) return null to false
        var cleanPath = path.trim()
        if (cleanPath.startsWith("$")) cleanPath = cleanPath.substring(1)
        if (cleanPath.startsWith(".")) cleanPath = cleanPath.substring(1)
        if (cleanPath.isEmpty()) return root to true

        val tokens = tokenizePath(cleanPath)
        var current: Any? = root

        for (i in tokens.indices) {
            if (current == null) return null to false
            val token = tokens[i]

            if (token.isArrayWildcard) {
                val list = current as? List<*> ?: return null to false
                if (i == tokens.size - 1) {
                    return list to true
                }
                val remainingTokens = tokens.subList(i + 1, tokens.size)
                val subResults = mutableListOf<Any?>()
                for (elem in list) {
                    val (res, ok) = evaluateTokens(elem, remainingTokens)
                    if (ok && res != null) {
                        subResults.add(res)
                    }
                }
                return subResults to true
            }

            if (token.isArrayIndex) {
                val list = current as? List<*> ?: return null to false
                if (token.index < 0 || token.index >= list.size) return null to false
                current = list[token.index]
                continue
            }

            val map = current as? Map<*, *> ?: return null to false
            if (!map.containsKey(token.key)) return null to false
            current = map[token.key]
        }

        return current to true
    }

    private fun evaluateTokens(current: Any?, tokens: List<PathToken>): Pair<Any?, Boolean> {
        var curr = current
        for (token in tokens) {
            if (curr == null) return null to false
            if (token.isArrayIndex) {
                val list = curr as? List<*> ?: return null to false
                if (token.index < 0 || token.index >= list.size) return null to false
                curr = list[token.index]
                continue
            }
            val map = curr as? Map<*, *> ?: return null to false
            if (!map.containsKey(token.key)) return null to false
            curr = map[token.key]
        }
        return curr to true
    }

    private data class PathToken(
        val key: String = "",
        val isArrayIndex: Boolean = false,
        val isArrayWildcard: Boolean = false,
        val index: Int = 0
    )

    private fun tokenizePath(path: String): List<PathToken> {
        val tokens = mutableListOf<PathToken>()
        var i = 0
        val n = path.length

        while (i < n) {
            if (path[i] == '.') {
                i++
                continue
            }
            if (path[i] == '[') {
                val close = path.indexOf(']', i)
                if (close == -1) break
                val inside = path.substring(i + 1, close).trim()
                if (inside == "*") {
                    tokens.add(PathToken(isArrayWildcard = true))
                } else {
                    val idx = inside.toIntOrNull()
                    if (idx != null) {
                        tokens.add(PathToken(isArrayIndex = true, index = idx))
                    } else {
                        val propKey = inside.trim('\'', '"')
                        tokens.add(PathToken(key = propKey))
                    }
                }
                i = close + 1
                continue
            }

            var end = i
            while (end < n && path[end] != '.' && path[end] != '[') {
                end++
            }
            val key = path.substring(i, end).trim()
            if (key.isNotEmpty()) {
                tokens.add(PathToken(key = key))
            }
            i = end
        }

        return tokens
    }

    private fun toDouble(v: Any?): Double? {
        return when (v) {
            is Number -> v.toDouble()
            is String -> v.toDoubleOrNull()
            is Boolean -> if (v) 1.0 else 0.0
            else -> null
        }
    }

    private fun parseTimestamp(v: Any): String {
        return when (v) {
            is Number -> {
                val n = v.toLong()
                val instant = if (n > 10_000_000_000L) Instant.ofEpochMilli(n) else Instant.ofEpochSecond(n)
                RedfishUtils.formatTimeRFC3339(instant)
            }
            is String -> {
                val str = v.trim()
                try {
                    val instant = Instant.parse(str)
                    RedfishUtils.formatTimeRFC3339(instant)
                } catch (e: DateTimeParseException) {
                    val num = str.toLongOrNull()
                    if (num != null) {
                        val instant = if (num > 10_000_000_000L) Instant.ofEpochMilli(num) else Instant.ofEpochSecond(num)
                        RedfishUtils.formatTimeRFC3339(instant)
                    } else {
                        RedfishUtils.formatTimeRFC3339()
                    }
                }
            }
            else -> RedfishUtils.formatTimeRFC3339()
        }
    }
}
