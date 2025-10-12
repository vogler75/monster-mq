package at.rocworks.flowengine

import at.rocworks.Utils
import io.vertx.core.json.JsonObject
import org.graalvm.polyglot.Context
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

    init {
        logger.info("GraalVM script engine initialized with languages: ${context.engine.languages.keys}")
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
     * @return ExecutionResult with success status and any errors
     */
    fun execute(
        script: String,
        language: String = "javascript",
        inputs: Map<String, InputValue>,
        state: MutableMap<String, Any>,
        flowVariables: Map<String, Any>,
        onOutput: (portName: String, value: Any?) -> Unit
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
            val inputsJsObj = context.eval("js", "JSON.parse('${inputsObj.encode().replace("'", "\\'")}')")
            bindings.putMember("inputs", inputsJsObj)

            // Prepare msg object
            val triggerInput = inputs.values.firstOrNull()
            if (triggerInput != null) {
                val msgJson = JsonObject()
                    .put("value", triggerInput.value)
                    .put("timestamp", triggerInput.timestamp)
                    .put("topic", triggerInput.topic)
                val msgJsObj = context.eval("js", "JSON.parse('${msgJson.encode().replace("'", "\\'")}')")
                bindings.putMember("msg", msgJsObj)
            }

            // State and flow variables as JavaScript objects
            val stateJson = JsonObject(state as Map<String, Any>)
            val stateJsObj = context.eval("js", "JSON.parse('${stateJson.encode().replace("'", "\\'")}')")
            bindings.putMember("state", stateJsObj)

            val flowJson = JsonObject(flowVariables)
            val flowJsObj = context.eval("js", "JSON.parse('${flowJson.encode().replace("'", "\\'")}')")
            bindings.putMember("flow", flowJsObj)

            // Outputs helper
            val outputs = OutputsProxy(onOutput)
            bindings.putMember("outputs", outputs)

            // Console helper
            val consoleProxy = ConsoleProxy()
            bindings.putMember("console", consoleProxy)

            // Execute script
            context.eval(normalizeLanguage(language), script)

            return ExecutionResult(
                success = true,
                logs = consoleProxy.getLogs(),
                errors = emptyList()
            )

        } catch (e: Exception) {
            logger.warning("Script execution error: ${e.message}")
            return ExecutionResult(
                success = false,
                logs = emptyList(),
                errors = listOf(e.message ?: "Unknown error")
            )
        }
    }

    /**
     * Close the script engine and release resources
     */
    fun close() {
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
        TOPIC,
        TEXT
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
    class ConsoleProxy {
        private val logs = mutableListOf<String>()

        @Suppress("unused")
        fun log(vararg messages: Any?) {
            val msg = "[LOG] ${messages.joinToString(" ")}"
            logs.add(msg)
            logger.info(msg)
        }

        @Suppress("unused")
        fun warn(vararg messages: Any?) {
            val msg = "[WARN] ${messages.joinToString(" ")}"
            logs.add(msg)
            logger.warning(msg)
        }

        @Suppress("unused")
        fun error(vararg messages: Any?) {
            val msg = "[ERROR] ${messages.joinToString(" ")}"
            logs.add(msg)
            logger.severe(msg)
        }

        fun getLogs(): List<String> = logs.toList()
    }
}
