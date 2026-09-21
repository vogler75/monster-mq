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
) : AutoCloseable {
    companion object {
        private val logger: Logger = Utils.getLogger(ScriptEngine::class.java)

        fun normalizeLanguage(lang: String): String {
            return when (lang.lowercase().trim()) {
                "javascript", "js" -> "js"
                "python", "py", "starlark", "star" -> "python"
                else -> "python"
            }
        }

        private const val PYTHON_PRELUDE = """
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

class _MqttWrapper:
    def __init__(self, target):
        self._target = target
    def publish(self, topic, payload, qos=0, retain=False):
        return self._target.publish(topic, payload, int(qos), bool(retain))
    def subscribe(self, filter, callback):
        return self._target.subscribe(filter, callback)

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

class _TriggerTimeWrapper:
    def __init__(self, d):
        self._d = d
    def __getattr__(self, name):
        if self._d is not None:
            try:
                val = self._d.get(name) if hasattr(self._d, "get") else None
                if val is not None:
                    return val
            except Exception:
                pass
        raise AttributeError(f"'trigger_time' object has no attribute '{name}'")
    def __getitem__(self, key):
        if self._d is not None:
            try:
                val = self._d.get(key) if hasattr(self._d, "get") else None
                if val is not None:
                    return val
            except Exception:
                pass
        raise KeyError(key)
    def __contains__(self, key):
        if self._d is not None:
            try:
                return self._d.containsKey(key) if hasattr(self._d, "containsKey") else (key in self._d)
            except Exception:
                return False
        return False
    def get(self, key, default=None):
        if self._d is not None:
            try:
                val = self._d.get(key) if hasattr(self._d, "get") else None
                if val is not None:
                    return val
            except Exception:
                pass
        return default
    def __str__(self):
        if self._d is not None:
            try:
                val = self._d.get("iso") if hasattr(self._d, "get") else None
                return str(val) if val is not None else ""
            except Exception:
                return ""
        return ""
    def __repr__(self):
        return f"<TriggerTime {self.__str__()}>" if self._d is not None else "None"
    def __int__(self):
        if self._d is not None:
            try:
                val = self._d.get("timestamp") if hasattr(self._d, "get") else None
                return int(val) if val is not None else 0
            except Exception:
                return 0
        return 0
    def __float__(self):
        if self._d is not None:
            try:
                val = self._d.get("timestamp") if hasattr(self._d, "get") else None
                return float(val) if val is not None else 0.0
            except Exception:
                return 0.0
        return 0.0

class _TriggerContextWrapper:
    def __init__(self, t_type, t_time):
        self.type = t_type
        self.time = t_time
    def __getattr__(self, name):
        if name == "type":
            return self.type
        elif name == "time":
            return self.time
        raise AttributeError(f"'trigger' object has no attribute '{name}'")
    def __getitem__(self, key):
        if key == "type":
            return self.type
        elif key == "time":
            return self.time
        raise KeyError(key)
    def __repr__(self):
        return f"<Trigger type={self.type} time={self.time}>"

_raw_msg = None
_raw_mqtt = None
_raw_json = None
_raw_trigger_time = None
_raw_trigger_type = None
msg = None
mqtt = None
json = None
trigger_time = None
triggerTime = None
trigger = None
"""
    }

    private val targetLanguage: String = normalizeLanguage(config.language)
    private val state = mutableMapOf<String, Any>()
    private val compiledSource: Source
    private var persistentContext: Context? = null

    init {
        // Pre-compile script on creation to catch syntax errors early
        compiledSource = Source.newBuilder(targetLanguage, config.script, scriptName).build()
        // Validate syntax by parsing with an engine context
        Context.newBuilder(targetLanguage).build().use { ctx ->
            ctx.parse(compiledSource)
        }
    }

    private fun initContext(ctx: Context) {
        val bindings = ctx.getBindings(targetLanguage)
        bindings.putMember("state", state)
        bindings.putMember("global", globalStore ?: ScriptGlobalStore())
        bindings.putMember("globals", globalStore ?: ScriptGlobalStore())
        bindings.putMember("shared", globalStore ?: ScriptGlobalStore())
        bindings.putMember("storage", scriptStorage ?: ScriptStorage(scriptName, null, "local"))
        bindings.putMember("scripts", ScriptScriptsProxy(scriptInvoker))
        bindings.putMember("db", databaseProxy)
        bindings.putMember("archive", archiveProxy ?: ScriptArchiveProxy())
        bindings.putMember("json", ScriptJsonProxy())

        if (targetLanguage == "python") {
            ctx.eval("python", PYTHON_PRELUDE)
        }
    }

    @Synchronized
    private fun getOrCreateContext(): Context {
        val existing = persistentContext
        if (existing != null) {
            return existing
        }
        val ctx = Context.newBuilder(targetLanguage)
            .allowAllAccess(true)
            .allowHostAccess(HostAccess.ALL)
            .build()
        initContext(ctx)
        persistentContext = ctx
        return ctx
    }

    override fun close() {
        synchronized(this) {
            try {
                persistentContext?.close(true)
            } catch (ignored: Exception) {}
            persistentContext = null
        }
    }

    private fun unwrapPolyglotValue(v: org.graalvm.polyglot.Value?, visited: MutableSet<Any> = mutableSetOf()): Any? {
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
                list.add(unwrapPolyglotValue(v.getArrayElement(i), visited))
            }
            return list
        }
        if (v.hasHashEntries()) {
            if (!visited.add(v)) return v.toString()
            val map = mutableMapOf<String, Any?>()
            val iterator = v.hashKeysIterator
            while (iterator != null && iterator.hasIteratorNextElement()) {
                val keyVal = iterator.iteratorNextElement
                val key = unwrapPolyglotValue(keyVal, visited)?.toString() ?: continue
                map[key] = unwrapPolyglotValue(v.getHashValue(keyVal), visited)
            }
            return map
        }
        if (v.hasMembers() && !v.canExecute()) {
            if (!visited.add(v)) return v.toString()
            val map = mutableMapOf<String, Any?>()
            for (key in v.memberKeys) {
                if (key.startsWith("__") && key.endsWith("__")) continue
                try {
                    map[key] = unwrapPolyglotValue(v.getMember(key), visited)
                } catch (e: Exception) {
                    map[key] = null
                }
            }
            return map
        }
        return v.toString()
    }

    /**
     * Execute script in a sandboxed Context.
     */
    @Synchronized
    fun execute(
        msg: BrokerMessage?,
        args: Map<String, Any?>? = null,
        dryRun: Boolean = false,
        timeoutMsOverride: Int? = null,
        triggerContext: ScriptTriggerContext? = null
    ): ScriptExecutionResult {
        val startNano = System.nanoTime()
        val timeout = (timeoutMsOverride ?: config.timeoutMs).let { if (it <= 0) 200 else it }

        val resolvedTrigger = triggerContext ?: ScriptTriggerContext(
            type = if (msg != null) "TOPIC" else if (args != null && args.isNotEmpty()) "CALLABLE" else "TIMER",
            time = if (msg != null) java.time.Instant.now() else java.time.Instant.now()
        )
        val triggerTimeProxy = ScriptTriggerTimeProxy(resolvedTrigger.time)

        val logProxy = ScriptLogProxy(scriptName, recentLogs)
        val mqttProxy = ScriptMqttProxy(scriptName, dryRun, mqttPublisher)
        val scriptsProxy = ScriptScriptsProxy(scriptInvoker)
        val msgProxy = msg?.let { ScriptMsgProxy.fromBrokerMessage(it) }
        val jsonProxy = ScriptJsonProxy()

        val polyglotContext = if (dryRun) {
            val ctx = Context.newBuilder(targetLanguage)
                .allowAllAccess(true)
                .allowHostAccess(HostAccess.ALL)
                .build()
            initContext(ctx)
            ctx
        } else {
            getOrCreateContext()
        }

        try {
            val bindings = polyglotContext.getBindings(targetLanguage)

            // Inject per-execution proxies and globals
            bindings.putMember("args", args ?: emptyMap<String, Any?>())
            bindings.putMember("scripts", scriptsProxy)
            bindings.putMember("log", logProxy)
            bindings.putMember("console", logProxy)
            bindings.putMember("json", jsonProxy)
            bindings.putMember("trigger_time", triggerTimeProxy.toMap())
            bindings.putMember("triggerTime", triggerTimeProxy.toMap())
            bindings.putMember("trigger", mapOf("type" to resolvedTrigger.type, "time" to triggerTimeProxy.toMap()))

            if (targetLanguage == "python") {
                bindings.putMember("_raw_msg", msgProxy?.toMap())
                bindings.putMember("_raw_mqtt", mqttProxy)
                bindings.putMember("_raw_json", jsonProxy)
                bindings.putMember("_raw_trigger_time", triggerTimeProxy.toMap())
                bindings.putMember("_raw_trigger_type", resolvedTrigger.type)
                polyglotContext.eval("python", """
                    msg = _MsgWrapper(_raw_msg) if _raw_msg is not None else None
                    mqtt = _MqttWrapper(_raw_mqtt)
                    json = _JsonWrapper(_raw_json)
                    trigger_time = _TriggerTimeWrapper(_raw_trigger_time) if _raw_trigger_time is not None else None
                    triggerTime = trigger_time
                    trigger = _TriggerContextWrapper(_raw_trigger_type, trigger_time)
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
                targetLanguage != "python" && evalResult != null && !evalResult.isNull && !evalResult.canExecute() && !evalResult.hasMembers() -> unwrapPolyglotValue(evalResult)
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

        } catch (t: Throwable) {
            val elapsedMs = (System.nanoTime() - startNano) / 1_000_000.0f
            val errList = mutableListOf<String>()

            if (t is PolyglotException && t.isCancelled) {
                // If context was cancelled, reset persistentContext so next execution rebuilds it
                try {
                    polyglotContext.close(true)
                } catch (ignored: Exception) {}
                persistentContext = null
            }

            val errMsg = if (t is PolyglotException) {
                val loc = t.sourceLocation
                if (loc != null) {
                    "${t.message} (line ${loc.startLine}, col ${loc.startColumn})"
                } else {
                    t.message ?: "Execution error"
                }
            } else {
                t.message ?: t.javaClass.simpleName
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
            if (dryRun) {
                try {
                    polyglotContext.close(true)
                } catch (ignored: Exception) {}
            }
        }
    }
}
