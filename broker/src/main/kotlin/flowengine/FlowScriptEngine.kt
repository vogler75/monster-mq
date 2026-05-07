package at.rocworks.flowengine

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.handlers.ArchiveGroup
import at.rocworks.stores.IMessageArchiveExtended
import at.rocworks.stores.devices.FlowClass
import at.rocworks.stores.devices.FlowNode
import at.rocworks.stores.devices.DatabaseConnectionConfig
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.graalvm.polyglot.Context
import java.time.Instant
import java.util.logging.Logger

/**
 * Polyglot script execution engine supporting JavaScript and Python via GraalVM
 *
 * Requirements:
 * - GraalVM JDK (https://www.graalvm.org/downloads/)
 * - For Python support: gu install python
 */
class FlowScriptEngine {
    companion object {
        private val logger: Logger = Utils.getLogger(FlowScriptEngine::class.java)
    }

    // Initialize GraalVM Context with JavaScript support
    // Python can be added later by including the GraalPy Maven dependency
    private val context: Context = Context.newBuilder("js")
        .allowAllAccess(true) // Required for host object access
        .option("js.ecmascript-version", "2022")
        .build()

    // Cache for compiled script functions
    private val compiledScripts = mutableMapOf<String, org.graalvm.polyglot.Value>()

    init {
        logger.fine { "GraalVM script engine initialized with languages: ${context.engine.languages.keys}" }
    }

    /**
     * Execute a script with provided context
     *
     * @param script Code to execute
     * @param language Script language ("javascript" or "python")
     * @param inputs Map of input port names to their values
     * @param state Mutable state object for the node
     * @param flowVariables Flow-wide variables
     * @param onOutput Callback when script calls outputs.send(portName, value)
     * @param mqttPublisher Callback when script calls mqtt.publish(topic, payload, qos, retain)
     * @param instanceName Optional flow instance name for log prefixing
     * @param flowClass Optional flow class definition for database node access
     * @return ExecutionResult with success status and any errors
     */
    fun execute(
        script: String,
        language: String = "javascript",
        inputs: Map<String, InputValue>,
        state: MutableMap<String, Any>,
        flowVariables: Map<String, Any>,
        onOutput: (portName: String, value: Any?) -> Unit,
        mqttPublisher: ((topic: String, payload: Any?, qos: Int, retain: Boolean) -> Boolean)? = null,
        instanceName: String? = null,
        flowClass: FlowClass? = null
    ): ExecutionResult {
        try {
            val bindings = context.getBindings(normalizeLanguage(language))

            // Prepare inputs as JSON and parse in JavaScript
            val inputsObj = JsonObject()
            inputs.forEach { (portName, inputValue) ->
                inputsObj.put(portName, JsonObject()
                    .put("value", inputValue.value)
                    .put("type", inputValue.type.name.lowercase())
                    .apply {
                        inputValue.timestamp?.let { put("timestamp", it) }
                        inputValue.topic?.let { put("topic", it) }
                    }
                )
            }
            // Parse JSON in JavaScript context to create proper JavaScript object
            // Escape backslashes and double quotes for safe embedding in JS string
            val jsonString = inputsObj.encode().replace("\\", "\\\\").replace("\"", "\\\"")
            val inputsJsObj = context.eval("js", "JSON.parse(\"$jsonString\")")
            bindings.putMember("inputs", inputsJsObj)

            // Prepare msg object
            val triggerInput = inputs.values.firstOrNull()
            if (triggerInput != null) {
                val msgJson = JsonObject()
                    .put("value", triggerInput.value)
                    .put("timestamp", triggerInput.timestamp)
                    .put("topic", triggerInput.topic)
                val msgJsonString = msgJson.encode().replace("\\", "\\\\").replace("\"", "\\\"")
                val msgJsObj = context.eval("js", "JSON.parse(\"$msgJsonString\")")
                bindings.putMember("msg", msgJsObj)
            }

            // State and flow variables as JavaScript objects
            val stateJson = JsonObject(state as Map<String, Any>)
            val stateJsonString = stateJson.encode().replace("\\", "\\\\").replace("\"", "\\\"")
            val stateJsObj = context.eval("js", "JSON.parse(\"$stateJsonString\")")
            bindings.putMember("state", stateJsObj)

            val flowJson = JsonObject(flowVariables)
            val flowJsonString = flowJson.encode().replace("\\", "\\\\").replace("\"", "\\\"")
            val flowJsObj = context.eval("js", "JSON.parse(\"$flowJsonString\")")
            bindings.putMember("flow", flowJsObj)

            // Outputs helper
            val outputs = OutputsProxy(onOutput)
            bindings.putMember("outputs", outputs)

            // Console helper
            val consoleProxy = ConsoleProxy(instanceName)
            bindings.putMember("console", consoleProxy)

            // MQTT helper
            val mqttProxy = MqttProxy(mqttPublisher)
            bindings.putMember("mqtt", mqttProxy)

            // Database nodes helper (for accessing database nodes from scripts)
            if (flowClass != null) {
                val databasesProxy = DatabasesProxy(flowClass)
                // Create a dynamic proxy object that allows accessing databases by name
                val dbsWrapper = object {
                    @Suppress("unused")
                    fun get(nodeId: String): Any? {
                        return databasesProxy.get(nodeId)
                    }
                }
                bindings.putMember("dbs", dbsWrapper)
            }

            // Archive store helper (for querying last values and history from scripts)
            val archiveProxy = ArchiveProxy()
            bindings.putMember("archive", archiveProxy)

            // Get or compile the script function
            val scriptFunction = compiledScripts.getOrPut(script) {
                logger.fine { "Compiling script for first time:\n$script" }
                // Wrap user script in a function to provide fresh scope on each execution
                val wrappedScript = """
                    (function() {
                        ${script}
                    })
                """.trimIndent()
                context.eval(normalizeLanguage(language), wrappedScript)
            }

            // Execute the compiled function
            scriptFunction.execute()

            // Read back the modified state from JavaScript and update the Kotlin state map
            val stateObj = bindings.getMember("state")
            if (stateObj != null) {
                // Convert state back to JSON, then parse into Kotlin map to preserve values across executions
                val stateJsonString = context.eval("js", "JSON.stringify(state)").asString()
                val updatedStateJson = JsonObject(stateJsonString)
                state.clear()
                updatedStateJson.map.forEach { (key, value) ->
                    state[key] = value
                }
            }

            return ExecutionResult(
                success = true,
                logs = consoleProxy.getLogs(),
                errors = emptyList()
            )

        } catch (e: Exception) {
            // Extract detailed error information including line numbers
            val errorMessage = StringBuilder()
            errorMessage.append(e.message ?: "Unknown error")

            // Try to get polyglot exception with source location
            if (e is org.graalvm.polyglot.PolyglotException) {
                errorMessage.append("\n")
                if (e.isGuestException) {
                    val sourceLocation = e.sourceLocation
                    if (sourceLocation != null) {
                        errorMessage.append("  at line ${sourceLocation.startLine}")
                        if (sourceLocation.startColumn > 0) {
                            errorMessage.append(", column ${sourceLocation.startColumn}")
                        }
                        errorMessage.append("\n")
                    }
                }

                // Add stack trace
                val stackTrace = e.polyglotStackTrace.toList()
                if (stackTrace.isNotEmpty()) {
                    errorMessage.append("Stack trace:\n")
                    stackTrace.take(5).forEach { frame ->
                        errorMessage.append("  at ${frame.rootName ?: "<anonymous>"}")
                        val loc = frame.sourceLocation
                        if (loc != null) {
                            errorMessage.append(" (line ${loc.startLine})")
                        }
                        errorMessage.append("\n")
                    }
                }
            }

            val fullError = errorMessage.toString()
            logger.warning("Script execution error:\n$fullError")

            return ExecutionResult(
                success = false,
                logs = emptyList(),
                errors = listOf(fullError)
            )
        }
    }

    /**
     * Close the script engine and release resources
     */
    fun close() {
        compiledScripts.clear()
        context.close()
    }

    /**
     * Check if a language is supported
     */
    fun isLanguageSupported(language: String): Boolean {
        val normalized = normalizeLanguage(language)
        return context.engine.languages.containsKey(normalized)
    }

    /**
     * Get list of supported languages
     */
    fun getSupportedLanguages(): List<String> {
        return context.engine.languages.keys.toList()
    }

    private fun normalizeLanguage(language: String): String {
        return when (language.lowercase()) {
            "javascript", "js" -> "js"
            "python", "py" -> "python"
            else -> language
        }
    }

    /**
     * Result of script execution
     */
    data class ExecutionResult(
        val success: Boolean,
        val logs: List<String> = emptyList(),
        val errors: List<String> = emptyList()
    )

    /**
     * Input value with metadata
     */
    data class InputValue(
        val value: Any?,
        val type: InputType,
        val timestamp: Long? = null,
        val topic: String? = null
    )

    enum class InputType {
        TOPIC
    }

    /**
     * Proxy for outputs object with send() method
     */
    class OutputsProxy(private val onOutput: (portName: String, value: Any?) -> Unit) {
        @Suppress("unused")
        fun send(portName: String, value: Any?) {
            onOutput(portName, value)
        }
    }

    /**
     * Proxy for console object with log/warn/error methods
     */
    class ConsoleProxy(private val instanceName: String? = null) {
        private val logs = mutableListOf<String>()
        private val prefix = if (instanceName != null) "[$instanceName] " else ""

        @Suppress("unused")
        fun log(vararg messages: Any?) {
            val msg = "$prefix[LOG] ${messages.joinToString(" ")}"
            logs.add(msg)
            logger.info(msg)
        }

        @Suppress("unused")
        fun warn(vararg messages: Any?) {
            val msg = "$prefix[WARN] ${messages.joinToString(" ")}"
            logs.add(msg)
            logger.warning(msg)
        }

        @Suppress("unused")
        fun error(vararg messages: Any?) {
            val msg = "$prefix[ERROR] ${messages.joinToString(" ")}"
            logs.add(msg)
            logger.severe(msg)
        }

        fun getLogs(): List<String> = logs.toList()
    }

    /**
     * Proxy for MQTT publish operations from scripts.
     */
    class MqttProxy(
        private val publisher: ((topic: String, payload: Any?, qos: Int, retain: Boolean) -> Boolean)? = null
    ) {
        @Suppress("unused")
        fun publish(topic: String, payload: Any?): Boolean {
            return publishInternal(topic, payload, 0, false)
        }

        @Suppress("unused")
        fun publish(topic: String, payload: Any?, qos: Int): Boolean {
            return publishInternal(topic, payload, qos, false)
        }

        @Suppress("unused")
        fun publish(topic: String, payload: Any?, qos: Int, retain: Boolean): Boolean {
            return publishInternal(topic, payload, qos, retain)
        }

        private fun publishInternal(topic: String, payload: Any?, qos: Int, retain: Boolean): Boolean {
            if (topic.isBlank()) {
                logger.warning("mqtt.publish rejected blank topic")
                return false
            }
            if (Utils.isWildCardTopic(topic)) {
                logger.warning("mqtt.publish rejected wildcard topic '$topic'")
                return false
            }
            if (qos !in 0..2) {
                logger.warning("mqtt.publish rejected invalid QoS '$qos' for topic '$topic'")
                return false
            }
            if (publisher == null) {
                logger.warning("mqtt.publish is not available in this execution context")
                return false
            }
            return try {
                publisher.invoke(topic, payload, qos, retain)
            } catch (e: Exception) {
                logger.warning("mqtt.publish error for topic '$topic': ${e.message}")
                false
            }
        }
    }

    /**
     * Proxy for accessing database nodes from scripts
     * Provides access to all database nodes in the flow with fluent API:
     * flow.dbs.<databaseNodeName>.open()
     * flow.dbs.<databaseNodeName>.execute(sql, arguments)
     * flow.dbs.<databaseNodeName>.close()
     */
    class DatabasesProxy(private val flowClass: FlowClass) {
        private val databaseNodes: Map<String, FlowNode> = flowClass.nodes
            .filter { it.type == "database" }
            .associateBy { it.id }

        @Suppress("unused")
        fun get(nodeId: String): DatabaseNodeProxy? {
            val node = databaseNodes[nodeId] ?: return null
            return DatabaseNodeProxy(node)
        }

        inner class DatabaseNodeProxy(private val node: FlowNode) {
            private var connection: java.sql.Connection? = null
            private val jdbcManager = JdbcManagerHolder.getInstance()

            @Suppress("unused")
            fun open(): Boolean {
                return try {
                    val jdbcUrl = node.config.getString("jdbcUrl")
                    val username = node.config.getString("username", "")
                    val password = node.config.getString("password", "")

                    if (jdbcUrl.isBlank()) {
                        logger.warning("Database node ${node.id} has no jdbcUrl configured")
                        return false
                    }

                    // Driver is automatically inferred from the JDBC URL
                    val connectionConfig = DatabaseConnectionConfig(
                        name = node.id,
                        jdbcUrl = jdbcUrl,
                        username = username,
                        password = password
                    )

                    connection = jdbcManager.getConnection(connectionConfig)
                    true
                } catch (e: Exception) {
                    logger.severe("Failed to open database connection for ${node.id}: ${e.message}")
                    false
                }
            }

            @Suppress("unused")
            fun execute(sql: String, arguments: Any? = null): JsonObject {
                return try {
                    if (connection == null) {
                        open()
                    }

                    if (connection == null) {
                        return JsonObject()
                            .put("success", false)
                            .put("error", "Database connection not available")
                    }

                    // Parse arguments
                    val args = when (arguments) {
                        is List<*> -> arguments
                        is JsonObject -> arguments.getMap().values.toList()
                        null -> emptyList<Any?>()
                        else -> listOf(arguments)
                    }

                    // Prepare statement
                    val preparedStmt = connection!!.prepareStatement(sql)
                    args.forEachIndexed { index, arg ->
                        preparedStmt.setObject(index + 1, arg)
                    }

                    // Determine if it's SELECT or DML
                    val trimmedSql = sql.trim().uppercase()
                    val isSelect = trimmedSql.startsWith("SELECT")

                    val result = if (isSelect) {
                        val resultSet = preparedStmt.executeQuery()
                        val metaData = resultSet.metaData
                        val columnCount = metaData.columnCount
                        val result = JsonObject()

                        // Get column info
                        val columnNames = mutableListOf<Any>()
                        val columnTypes = mutableListOf<Any>()

                        for (i in 1..columnCount) {
                            columnNames.add(metaData.getColumnName(i))
                            columnTypes.add(metaData.getColumnTypeName(i))
                        }

                        val rows = mutableListOf<Any>()
                        rows.add(columnNames)
                        rows.add(columnTypes)

                        while (resultSet.next()) {
                            val row = mutableListOf<Any?>()
                            for (i in 1..columnCount) {
                                row.add(resultSet.getObject(i))
                            }
                            rows.add(row)
                        }

                        resultSet.close()
                        preparedStmt.close()

                        result.put("success", true)
                            .put("rows", io.vertx.core.json.JsonArray(rows))
                    } else {
                        val affectedRows = preparedStmt.executeUpdate()
                        preparedStmt.close()

                        JsonObject()
                            .put("success", true)
                            .put("affectedRows", affectedRows)
                    }

                    result
                } catch (e: Exception) {
                    logger.severe("Database execution error for ${node.id}: ${e.message}")
                    JsonObject()
                        .put("success", false)
                        .put("error", e.message ?: "Unknown error")
                }
            }

            @Suppress("unused")
            fun close(): Boolean {
                return try {
                    connection?.close()
                    connection = null
                    true
                } catch (e: Exception) {
                    logger.severe("Failed to close database connection for ${node.id}: ${e.message}")
                    false
                }
            }

            @Suppress("unused")
            fun isConnected(): Boolean {
                return try {
                    connection != null && !connection!!.isClosed && connection!!.isValid(5)
                } catch (e: Exception) {
                    false
                }
            }
        }
    }

    /**
     * Proxy for querying archive stores (last values and history) from scripts.
     *
     * Usage:
     *   archive.getLastValue("test/topic")
     *   archive.getLastValues("test/#", 50)
     *   archive.getHistory("test/topic", "2024-01-01T00:00:00Z", null, 100)
     *   archive.getAggregatedHistory(["test/topic"], "5m", "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z")
     */
    class ArchiveProxy {

        private fun getArchiveGroups(): Map<String, ArchiveGroup> {
            return Monster.getArchiveHandler()?.getDeployedArchiveGroups() ?: emptyMap()
        }

        private fun brokerMessageToJson(msg: BrokerMessage): JsonObject {
            val obj = JsonObject()
                .put("topic", msg.topicName)
                .put("timestamp", msg.time.toEpochMilli())
                .put("qos", msg.qosLevel)
            obj.put("value", msg.getPayloadAsJsonValueOrString())
            return obj
        }

        @Suppress("unused")
        fun getLastValue(topic: String, archiveGroup: String = "Default"): JsonObject? {
            return try {
                val store = getArchiveGroups()[archiveGroup]?.lastValStore
                if (store == null) {
                    logger.warning("No LastValueStore for archive group '$archiveGroup'")
                    return null
                }
                val msg = store[topic] ?: return null
                brokerMessageToJson(msg)
            } catch (e: Exception) {
                logger.warning("archive.getLastValue error: ${e.message}")
                null
            }
        }

        @Suppress("unused")
        fun getLastValues(topicFilter: String, limit: Int = 100, archiveGroup: String = "Default"): JsonArray {
            val results = JsonArray()
            try {
                val store = getArchiveGroups()[archiveGroup]?.lastValStore
                if (store == null) {
                    logger.warning("No LastValueStore for archive group '$archiveGroup'")
                    return results
                }
                var count = 0
                store.findMatchingMessages(topicFilter) { msg ->
                    if (count < limit) {
                        results.add(brokerMessageToJson(msg))
                        count++
                        true // continue
                    } else {
                        false // stop
                    }
                }
            } catch (e: Exception) {
                logger.warning("archive.getLastValues error: ${e.message}")
            }
            return results
        }

        @Suppress("unused")
        fun getHistory(
            topicFilter: String,
            startTime: String? = null,
            endTime: String? = null,
            limit: Int = 100,
            archiveGroup: String = "Default"
        ): JsonArray {
            return try {
                val store = getArchiveGroups()[archiveGroup]?.archiveStore as? IMessageArchiveExtended
                if (store == null) {
                    logger.warning("No extended archive store for archive group '$archiveGroup'")
                    return JsonArray()
                }
                store.getHistory(
                    topic = topicFilter,
                    startTime = startTime?.let { Instant.parse(it) },
                    endTime = endTime?.let { Instant.parse(it) },
                    limit = limit
                )
            } catch (e: Exception) {
                logger.warning("archive.getHistory error: ${e.message}")
                JsonArray()
            }
        }

        @Suppress("unused")
        fun getAggregatedHistory(
            topics: List<String>,
            interval: String,
            startTime: String,
            endTime: String,
            functions: List<String>? = null,
            fields: List<String>? = null,
            archiveGroup: String = "Default"
        ): JsonObject {
            return try {
                val store = getArchiveGroups()[archiveGroup]?.archiveStore as? IMessageArchiveExtended
                if (store == null) {
                    logger.warning("No extended archive store for archive group '$archiveGroup'")
                    return JsonObject().put("columns", JsonArray()).put("rows", JsonArray())
                }
                val intervalMinutes = parseInterval(interval)
                store.getAggregatedHistory(
                    topics = topics,
                    startTime = Instant.parse(startTime),
                    endTime = Instant.parse(endTime),
                    intervalMinutes = intervalMinutes,
                    functions = functions ?: listOf("AVG"),
                    fields = fields ?: emptyList()
                )
            } catch (e: Exception) {
                logger.warning("archive.getAggregatedHistory error: ${e.message}")
                JsonObject().put("columns", JsonArray()).put("rows", JsonArray())
            }
        }

        private fun parseInterval(interval: String): Int {
            val match = Regex("^(\\d+)([mhd])$").matchEntire(interval)
                ?: throw IllegalArgumentException("Invalid interval format: '$interval' (expected e.g. 1m, 5m, 1h, 1d)")
            val value = match.groupValues[1].toInt()
            return when (match.groupValues[2]) {
                "m" -> value
                "h" -> value * 60
                "d" -> value * 1440
                else -> throw IllegalArgumentException("Invalid interval unit: '${match.groupValues[2]}'")
            }
        }
    }
}
