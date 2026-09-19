package at.rocworks.devices.script

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.handlers.ArchiveGroup
import at.rocworks.flowengine.JdbcManagerHolder
import at.rocworks.stores.devices.DatabaseConnectionConfig
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

/**
 * Data class representing a message published by a script.
 */
data class ScriptPublishedMessage(
    val topic: String,
    val payload: String,
    val qos: Int = 0,
    val retain: Boolean = false
)

/**
 * Polyglot MsgProxy implementing Map<String, Any?> so that in Python both
 * msg["topic"] and msg.topic work seamlessly.
 */
class ScriptMsgProxy(
    val topic: String,
    val payload: Any?,
    val raw_payload: String,
    val timestamp: Long,
    val qos: Int,
    val retain: Boolean
) : Map<String, Any?> {

    private val map: Map<String, Any?> = mapOf(
        "topic" to topic,
        "payload" to payload,
        "raw_payload" to raw_payload,
        "timestamp" to timestamp,
        "qos" to qos,
        "retain" to retain
    )

    override val entries: Set<Map.Entry<String, Any?>> get() = map.entries
    override val keys: Set<String> get() = map.keys
    override val size: Int get() = map.size
    override val values: Collection<Any?> get() = map.values
    override fun isEmpty(): Boolean = map.isEmpty()
    override fun get(key: String): Any? = map[key]
    override fun containsKey(key: String): Boolean = map.containsKey(key)
    override fun containsValue(value: Any?): Boolean = map.containsValue(value)

    fun toMap(): Map<String, Any?> = map

    companion object {
        fun fromBrokerMessage(msg: BrokerMessage): ScriptMsgProxy {
            val raw = String(msg.payload)
            val parsed = try {
                val trimmed = raw.trim()
                if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
                    JsonObject(trimmed).map
                } else if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
                    JsonArray(trimmed).list
                } else {
                    raw
                }
            } catch (e: Exception) {
                raw
            }

            return ScriptMsgProxy(
                topic = msg.topicName,
                payload = parsed,
                raw_payload = raw,
                timestamp = System.currentTimeMillis(),
                qos = msg.qosLevel,
                retain = msg.isRetain
            )
        }
    }
}

/**
 * Proxy for MQTT publishing and subscription from scripts.
 */
class ScriptMqttProxy(
    private val scriptName: String,
    private val dryRun: Boolean = false,
    private val publisher: ((topic: String, payload: ByteArray, qos: Int, retain: Boolean) -> Boolean)? = null
) {
    companion object {
        private val logger: Logger = Utils.getLogger(ScriptMqttProxy::class.java)
    }

    val publishedMessages = mutableListOf<ScriptPublishedMessage>()

    @JvmOverloads
    @Suppress("unused")
    fun publish(topic: String, payload: Any?, qos: Any? = 0, retain: Any? = false): Boolean {
        if (topic.isBlank()) {
            logger.warning("[$scriptName] mqtt.publish: topic cannot be blank")
            return false
        }
        if (Utils.isWildCardTopic(topic)) {
            logger.warning("[$scriptName] mqtt.publish: wildcard topic '$topic' not allowed")
            return false
        }

        val qosInt = when (qos) {
            is Number -> qos.toInt()
            is String -> qos.toIntOrNull() ?: 0
            else -> 0
        }.coerceIn(0, 2)

        val retainBool = when (retain) {
            is Boolean -> retain
            is Number -> retain.toInt() != 0
            is String -> retain.equals("true", ignoreCase = true)
            else -> false
        }

        val cleanPayload = ScriptValueHelper.toJavaObject(payload)
        val payloadStr = when (cleanPayload) {
            null -> ""
            is String -> cleanPayload
            is Map<*, *> -> @Suppress("UNCHECKED_CAST") JsonObject(cleanPayload as Map<String, Any?>).encode()
            is List<*> -> JsonArray(cleanPayload).encode()
            else -> cleanPayload.toString()
        }

        val pubMsg = ScriptPublishedMessage(topic, payloadStr, qosInt, retainBool)
        publishedMessages.add(pubMsg)

        if (!dryRun && publisher != null) {
            return try {
                publisher.invoke(topic, payloadStr.toByteArray(), qosInt, retainBool)
            } catch (e: Exception) {
                logger.warning("[$scriptName] mqtt.publish error for topic '$topic': ${e.message}")
                false
            }
        }
        return true
    }

    @Suppress("unused")
    fun subscribe(filter: String, callback: Any? = null): Boolean {
        logger.info("[$scriptName] mqtt.subscribe registered filter: $filter")
        return true
    }
}

/**
 * Built-in JSON module for encoding and decoding JSON in scripts (matches Edge Starlark API).
 */
class ScriptJsonProxy {
    @Suppress("unused")
    fun encode(value: Any?): String {
        val clean = ScriptValueHelper.toJavaObject(value)
        return when (clean) {
            null -> "null"
            is String -> clean
            is Map<*, *> -> @Suppress("UNCHECKED_CAST") JsonObject(clean as Map<String, Any?>).encode()
            is List<*> -> JsonArray(clean).encode()
            else -> io.vertx.core.json.Json.encode(clean)
        }
    }

    @Suppress("unused")
    fun decode(jsonStr: Any?): Any? {
        val s = when (jsonStr) {
            null -> return null
            is String -> jsonStr
            is CharSequence -> jsonStr.toString()
            else -> jsonStr.toString()
        }
        val trimmed = s.trim()
        return try {
            if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
                JsonObject(trimmed).map
            } else if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
                JsonArray(trimmed).list
            } else {
                io.vertx.core.json.Json.decodeValue(trimmed)
            }
        } catch (e: Exception) {
            trimmed
        }
    }

    @Suppress("unused")
    fun dumps(value: Any?): String = encode(value)

    @Suppress("unused")
    fun loads(jsonStr: Any?): Any? = decode(jsonStr)
}

/**
 * Proxy for archive and historical data queries.
 */
class ScriptArchiveProxy(
    private val archiveGroups: Map<String, ArchiveGroup>? = null
) {
    companion object {
        private val logger: Logger = Utils.getLogger(ScriptArchiveProxy::class.java)
    }

    @Suppress("unused")
    fun get_last_value(topic: String, archive_group: String = "Default"): Map<String, Any?>? {
        val group = archiveGroups?.get(archive_group) ?: archiveGroups?.values?.firstOrNull()
        if (group == null) {
            logger.warning("archive.get_last_value: Archive group '$archive_group' not found")
            return null
        }
        val store = group.lastValStore
        if (store == null) {
            logger.warning("archive.get_last_value: LastValueStore not configured in group '$archive_group'")
            return null
        }
        val msg = store[topic] ?: return null
        return mapOf(
            "topic" to msg.topicName,
            "payload" to String(msg.payload),
            "timestamp" to System.currentTimeMillis(),
            "qos" to msg.qosLevel,
            "retain" to msg.isRetain
        )
    }

    @Suppress("unused")
    fun get_last_values(pattern: String = "#", limit: Int = 100, archive_group: String = "Default"): List<Map<String, Any?>> {
        val group = archiveGroups?.get(archive_group) ?: archiveGroups?.values?.firstOrNull() ?: return emptyList()
        val store = group.lastValStore ?: return emptyList()
        val list = mutableListOf<Map<String, Any?>>()
        store.findMatchingMessages(pattern) { msg ->
            list.add(mapOf(
                "topic" to msg.topicName,
                "payload" to String(msg.payload),
                "timestamp" to System.currentTimeMillis(),
                "qos" to msg.qosLevel,
                "retain" to msg.isRetain
            ))
            list.size < limit
        }
        return list
    }
}

/**
 * Proxy for JDBC SQL database queries and executions.
 */
class ScriptDatabaseProxy {
    companion object {
        private val logger: Logger = Utils.getLogger(ScriptDatabaseProxy::class.java)
    }

    private val jdbcManager = JdbcManagerHolder.getInstance()

    @Suppress("unused")
    fun query(connName: String, sql: String, args: Any? = null): List<Map<String, Any?>> {
        val connection = getConnection(connName) ?: return emptyList()
        return try {
            val preparedStmt = connection.prepareStatement(sql)
            bindArguments(preparedStmt, args)
            val resultSet = preparedStmt.executeQuery()
            val metaData = resultSet.metaData
            val columnCount = metaData.columnCount
            val rows = mutableListOf<Map<String, Any?>>()

            while (resultSet.next()) {
                val row = mutableMapOf<String, Any?>()
                for (i in 1..columnCount) {
                    val colName = metaData.getColumnLabel(i) ?: metaData.getColumnName(i)
                    row[colName] = resultSet.getObject(i)
                }
                rows.add(row)
            }
            rows
        } catch (e: Exception) {
            logger.severe("Script database query failed on '$connName': ${e.message}")
            emptyList()
        }
    }

    @Suppress("unused")
    fun execute(connName: String, sql: String, args: Any? = null): Map<String, Any?> {
        val connection = getConnection(connName)
            ?: return mapOf("success" to false, "error" to "Connection '$connName' not found")
        return try {
            val preparedStmt = connection.prepareStatement(sql)
            bindArguments(preparedStmt, args)
            val affected = preparedStmt.executeUpdate()
            mapOf("success" to true, "affected_rows" to affected)
        } catch (e: Exception) {
            logger.severe("Script database execute failed on '$connName': ${e.message}")
            mapOf("success" to false, "error" to (e.message ?: "Execution failed"))
        }
    }

    private fun getConnection(name: String): java.sql.Connection? {
        val config = Monster.getConfig()
        val dbs = config.getJsonObject("Databases") ?: return null
        val dbConfig = dbs.getJsonObject(name) ?: return null
        val jdbcUrl = dbConfig.getString("Url") ?: dbConfig.getString("jdbcUrl") ?: return null
        val user = dbConfig.getString("Username", "")
        val pass = dbConfig.getString("Password", "")

        return jdbcManager.getConnection(
            DatabaseConnectionConfig(name, jdbcUrl, user, pass)
        )
    }

    private fun bindArguments(stmt: java.sql.PreparedStatement, args: Any?) {
        val argList = when (args) {
            is List<*> -> args
            is JsonArray -> args.list
            null -> emptyList<Any?>()
            else -> listOf(args)
        }
        argList.forEachIndexed { index, arg ->
            stmt.setObject(index + 1, arg)
        }
    }
}

object ScriptValueHelper {
    fun toJavaObject(obj: Any?): Any? {
        if (obj == null) return null
        if (obj is String || obj is Number || obj is Boolean) return obj
        if (obj is CharSequence) return obj.toString()
        if (obj is Map<*, *>) {
            val map = mutableMapOf<String, Any?>()
            for ((k, v) in obj) {
                val keyStr = k?.toString() ?: continue
                map[keyStr] = toJavaObject(v)
            }
            return map
        }
        if (obj is List<*>) {
            return obj.map { toJavaObject(it) }
        }
        if (obj is Iterable<*>) {
            return obj.map { toJavaObject(it) }
        }
        val className = obj.javaClass.name
        if (className.contains("truffle", ignoreCase = true) || className.contains("polyglot", ignoreCase = true)) {
            return obj.toString()
        }
        return obj
    }
}

/**
 * Node-wide shared in-memory dictionary.
 */
class ScriptGlobalStore {
    private val data = ConcurrentHashMap<String, Any>()

    @JvmOverloads
    @Suppress("unused")
    fun get(key: String, defaultValue: Any? = null): Any? = data[key] ?: defaultValue

    @Suppress("unused")
    fun set(key: String, value: Any?): Any? {
        val cleanValue = ScriptValueHelper.toJavaObject(value)
        if (cleanValue == null) {
            data.remove(key)
        } else {
            data[key] = cleanValue
        }
        return cleanValue
    }

    @Suppress("unused")
    fun delete(key: String): Boolean = data.remove(key) != null

    @Suppress("unused")
    fun list(): Map<String, Any> = HashMap(data)

    @Suppress("unused")
    fun clear() {
        data.clear()
    }
}

/**
 * Proxy for logging from scripts to broker logger, recentLogs buffer, and capturing for test results.
 */
class ScriptLogProxy(
    private val scriptName: String,
    private val recentLogs: ScriptCircularLogBuffer? = null
) {
    companion object {
        private val logger: Logger = Utils.getLogger(ScriptLogProxy::class.java)
    }

    val capturedLogs = mutableListOf<String>()

    @Suppress("unused")
    fun info(vararg messages: Any?) = logInternal("INFO", *messages)

    @Suppress("unused")
    fun warn(vararg messages: Any?) = logInternal("WARN", *messages)

    @Suppress("unused")
    fun error(vararg messages: Any?) = logInternal("ERROR", *messages)

    @Suppress("unused")
    fun debug(vararg messages: Any?) = logInternal("DEBUG", *messages)

    @Suppress("unused")
    fun log(vararg messages: Any?) = logInternal("INFO", *messages)

    private fun logInternal(level: String, vararg messages: Any?) {
        val msg = messages.joinToString(" ")
        val formatted = "[$level] $msg"
        capturedLogs.add(formatted)
        recentLogs?.add(formatted)

        when (level) {
            "WARN" -> logger.warning("[$scriptName] $msg")
            "ERROR" -> logger.severe("[$scriptName] $msg")
            "DEBUG" -> logger.fine("[$scriptName] $msg")
            else -> logger.info("[$scriptName] $msg")
        }
    }
}

/**
 * Proxy for inter-script callable invocations.
 */
class ScriptScriptsProxy(
    private val scriptInvoker: ((scriptName: String, args: Map<String, Any?>) -> Any?)? = null
) {
    @Suppress("unused")
    fun call(scriptName: String, args: Any? = null): Any? {
        if (scriptInvoker == null) return null
        val cleanArgs = when (args) {
            is Map<*, *> -> @Suppress("UNCHECKED_CAST") (ScriptValueHelper.toJavaObject(args) as? Map<String, Any?> ?: emptyMap())
            is JsonObject -> args.map
            else -> emptyMap<String, Any?>()
        }
        val result = scriptInvoker.invoke(scriptName, cleanArgs)
        return ScriptValueHelper.toJavaObject(result)
    }
}
