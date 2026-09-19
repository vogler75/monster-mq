package at.rocworks.devices.script

import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.devices.ScriptConfig
import org.graalvm.polyglot.Context
import org.graalvm.polyglot.HostAccess
import org.graalvm.polyglot.PolyglotException
import org.graalvm.polyglot.Source
import java.util.concurrent.TimeUnit
import java.util.logging.Logger

/**
 * Result of a script execution.
 */
data class ScriptExecutionResult(
    val success: Boolean,
    val returnValue: Any? = null,
    val outputMessages: List<ScriptPublishedMessage> = emptyList(),
    val logs: List<String> = emptyList(),
    val errors: List<String> = emptyList(),
    val executionTimeMs: Float = 0f
)

/**
 * GraalVM Polyglot script execution engine.
 * Supports Python (GraalPy) and JavaScript (GraalJS).
 */
class ScriptEngine(
    val scriptName: String,
    val config: ScriptConfig,
    private val globalStore: ScriptGlobalStore? = null,
    private val scriptStorage: ScriptStorage? = null,
    private val recentLogs: ScriptCircularLogBuffer? = null,
    private val mqttPublisher: ((topic: String, payload: ByteArray, qos: Int, retain: Boolean) -> Boolean)? = null,
    private val scriptInvoker: ((scriptName: String, args: Map<String, Any?>) -> Any?)? = null,
    private val archiveProxy: ScriptArchiveProxy? = null,
    private val databaseProxy: ScriptDatabaseProxy = ScriptDatabaseProxy()
) {
    companion object {
        private val logger: Logger = Utils.getLogger(ScriptEngine::class.java)

        fun normalizeLanguage(lang: String): String {
            return when (lang.lowercase().trim()) {
                "javascript", "js" -> "js"
                "python", "py", "starlark", "star" -> "python"
                else -> "python"
            }
        }
    }

    private val targetLanguage: String = normalizeLanguage(config.language)
    private val state = mutableMapOf<String, Any>()
    private val compiledSource: Source

    init {
        // Pre-compile script on creation to catch syntax errors early
        compiledSource = Source.newBuilder(targetLanguage, config.script, scriptName).build()
        // Validate syntax by parsing with an engine context
        Context.newBuilder(targetLanguage).build().use { ctx ->
            ctx.parse(compiledSource)
        }
    }

    private fun unwrapPolyglotValue(v: org.graalvm.polyglot.Value?): Any? {
        if (v == null || v.isNull) return null
        if (v.isHostObject) return v.asHostObject()
        if (v.isBoolean) return v.asBoolean()
        if (v.isString) return v.asString()
        if (v.fitsInInt()) return v.asInt()
        if (v.fitsInLong()) return v.asLong()
        if (v.fitsInDouble()) return v.asDouble()
        if (v.hasArrayElements()) {
            val list = mutableListOf<Any?>()
            for (i in 0 until v.arraySize) {
                list.add(unwrapPolyglotValue(v.getArrayElement(i)))
            }
            return list
        }
        if (v.hasMembers()) {
            val map = mutableMapOf<String, Any?>()
            for (key in v.memberKeys) {
                map[key] = unwrapPolyglotValue(v.getMember(key))
            }
            return map
        }
        return v.toString()
    }

    /**
     * Execute script in a sandboxed Context.
     */
    fun execute(
        msg: BrokerMessage?,
        args: Map<String, Any?>? = null,
        dryRun: Boolean = false,
        timeoutMsOverride: Int? = null
    ): ScriptExecutionResult {
        val startNano = System.nanoTime()
        val timeout = (timeoutMsOverride ?: config.timeoutMs).let { if (it <= 0) 200 else it }

        val logProxy = ScriptLogProxy(scriptName, recentLogs)
        val mqttProxy = ScriptMqttProxy(scriptName, dryRun, mqttPublisher)
        val scriptsProxy = ScriptScriptsProxy(scriptInvoker)
        val msgProxy = msg?.let { ScriptMsgProxy.fromBrokerMessage(it) }

        var polyglotContext: Context? = null
        try {
            polyglotContext = Context.newBuilder(targetLanguage)
                .allowAllAccess(true)
                .allowHostAccess(HostAccess.ALL)
                .build()

            val bindings = polyglotContext.getBindings(targetLanguage)

            // Inject proxies and globals
            bindings.putMember("args", args ?: emptyMap<String, Any?>())
            bindings.putMember("state", state)
            bindings.putMember("global", globalStore ?: ScriptGlobalStore())
            bindings.putMember("globals", globalStore ?: ScriptGlobalStore())
            bindings.putMember("shared", globalStore ?: ScriptGlobalStore())
            bindings.putMember("storage", scriptStorage ?: ScriptStorage(scriptName, null, "local"))
            bindings.putMember("scripts", scriptsProxy)
            bindings.putMember("db", databaseProxy)
            bindings.putMember("archive", archiveProxy ?: ScriptArchiveProxy())
            bindings.putMember("log", logProxy)
            bindings.putMember("console", logProxy)

            val jsonProxy = ScriptJsonProxy()
            bindings.putMember("json", jsonProxy)

            if (targetLanguage == "python") {
                bindings.putMember("_raw_msg", msgProxy?.toMap())
                bindings.putMember("_raw_mqtt", mqttProxy)
                bindings.putMember("_raw_json", jsonProxy)
                polyglotContext.eval("python", """
class _MsgWrapper:
    def __init__(self, d):
        self._d = d
    def __getattr__(self, name):
        if self._d is not None and name in self._d:
            return self._d[name]
        raise AttributeError(f"'msg' object has no attribute '{name}'")
    def __getitem__(self, key):
        if self._d is not None:
            return self._d[key]
        raise TypeError("'NoneType' object is not subscriptable")
    def __contains__(self, key):
        return self._d is not None and key in self._d
    def get(self, key, default=None):
        if self._d is not None:
            return self._d.get(key, default)
        return default
    def __repr__(self):
        return repr(self._d)

msg = _MsgWrapper(_raw_msg) if _raw_msg is not None else None

class _MqttWrapper:
    def __init__(self, target):
        self._target = target
    def publish(self, topic, payload, qos=0, retain=False):
        return self._target.publish(topic, payload, int(qos), bool(retain))
    def subscribe(self, filter, callback):
        return self._target.subscribe(filter, callback)

mqtt = _MqttWrapper(_raw_mqtt)

class _JsonWrapper:
    def __init__(self, target):
        self._target = target
    def encode(self, obj):
        return self._target.encode(obj)
    def decode(self, s):
        return self._target.decode(s)
    def dumps(self, obj, *args, **kwargs):
        return self._target.encode(obj)
    def loads(self, s, *args, **kwargs):
        return self._target.decode(s)

json = _JsonWrapper(_raw_json)
""".trimIndent())
            } else {
                bindings.putMember("msg", msgProxy?.toMap())
                bindings.putMember("mqtt", mqttProxy)
            }

            // Evaluate script
            val evalResult = polyglotContext.eval(compiledSource)

            // Check for explicit return value or result variable
            val retVal = when {
                bindings.hasMember("return_value") -> unwrapPolyglotValue(bindings.getMember("return_value"))
                bindings.hasMember("result") -> unwrapPolyglotValue(bindings.getMember("result"))
                evalResult != null && !evalResult.isNull && !evalResult.canExecute() -> unwrapPolyglotValue(evalResult)
                else -> null
            }

            val elapsedMs = (System.nanoTime() - startNano) / 1_000_000.0f

            return ScriptExecutionResult(
                success = true,
                returnValue = retVal,
                outputMessages = mqttProxy.publishedMessages,
                logs = logProxy.capturedLogs,
                errors = emptyList(),
                executionTimeMs = elapsedMs
            )

        } catch (e: Exception) {
            val elapsedMs = (System.nanoTime() - startNano) / 1_000_000.0f
            val errList = mutableListOf<String>()

            val errMsg = if (e is PolyglotException) {
                val loc = e.sourceLocation
                if (loc != null) {
                    "${e.message} (line ${loc.startLine}, col ${loc.startColumn})"
                } else {
                    e.message ?: "Execution error"
                }
            } else {
                e.message ?: "Unknown error"
            }

            errList.add(errMsg)
            logProxy.error("Script execution failed: $errMsg")

            return ScriptExecutionResult(
                success = false,
                returnValue = null,
                outputMessages = mqttProxy.publishedMessages,
                logs = logProxy.capturedLogs,
                errors = errList,
                executionTimeMs = elapsedMs
            )
        } finally {
            try {
                polyglotContext?.close(true)
            } catch (ignored: Exception) {}
        }
    }
}
