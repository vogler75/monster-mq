package at.rocworks.devices.script

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.data.BulkClientMessage
import at.rocworks.handlers.SessionHandler
import at.rocworks.bus.EventBusAddresses
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.ScriptConfig
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.eventbus.Message
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger

/**
 * Vert.x verticle managing the lifecycle, subscriptions, timer, and execution
 * of an individual script device.
 */
class ScriptConnector(
    private val deviceConfig: DeviceConfig,
    private val deviceStore: IDeviceConfigStore?,
    private val globalStore: ScriptGlobalStore,
    private val scriptInvoker: ((scriptName: String, args: Map<String, Any?>) -> Any?)? = null
) : AbstractVerticle() {

    companion object {
        private val logger: Logger = Utils.getLogger(ScriptConnector::class.java)
    }

    private lateinit var scriptConfig: ScriptConfig
    private lateinit var scriptStorage: ScriptStorage
    private lateinit var engine: ScriptEngine

    val recentLogs = ScriptCircularLogBuffer(100)

    // Stats
    val executionCount = AtomicLong(0)
    val errorCount = AtomicLong(0)
    @Volatile var lastExecutionTime: String? = null
    @Volatile var lastExecutionStatus: String? = null

    private var internalClientId: String? = null
    private var timerId: Long? = null
    private var lastPayloadHash: Int? = null
    @Volatile private var isStopped = false

    // For SINGLETON mode sequential execution
    private var currentExecution: Future<Void> = Future.succeededFuture()

    override fun start(startPromise: Promise<Void>) {
        try {
            isStopped = false
            scriptConfig = ScriptConfig.fromJsonObject(deviceConfig.config)
            scriptStorage = ScriptStorage(deviceConfig.name, deviceStore, deviceConfig.nodeId)

            // Compile script engine
            engine = ScriptEngine(
                scriptName = deviceConfig.name,
                config = scriptConfig,
                globalStore = globalStore,
                scriptStorage = scriptStorage,
                recentLogs = recentLogs,
                mqttPublisher = { topic, payload, qos, retain ->
                    publishToBroker(topic, payload, qos, retain)
                },
                scriptInvoker = scriptInvoker
            )

            // Load persistent storage, then set up subscriptions / timers
            scriptStorage.load()
                .compose { setupTriggers() }
                .onComplete { res ->
                    if (res.succeeded()) {
                        logger.info("ScriptConnector started for '${deviceConfig.name}' (${scriptConfig.language}, trigger: ${scriptConfig.triggerType})")
                        startPromise.complete()
                    } else {
                        logger.severe("Failed to start ScriptConnector for '${deviceConfig.name}': ${res.cause()?.message}")
                        startPromise.fail(res.cause())
                    }
                }

        } catch (e: Exception) {
            logger.severe("Exception initializing ScriptConnector for '${deviceConfig.name}': ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        isStopped = true
        // Cancel timer
        timerId?.let { vertx.cancelTimer(it) }
        timerId = null

        // Unsubscribe internal client
        internalClientId?.let { clientId ->
            val sessionHandler = Monster.getSessionHandler()
            if (sessionHandler != null) {
                scriptConfig.topicFilters.forEach { topic ->
                    sessionHandler.unsubscribeInternalClient(clientId, topic)
                }
                sessionHandler.unregisterInternalClient(clientId)
            }
        }

        // Close engine context
        engine.close()

        logger.info("ScriptConnector stopped for '${deviceConfig.name}'")
        stopPromise.complete()
    }

    private fun setupTriggers(): Future<Void> {
        val promise = Promise.promise<Void>()

        val trig = scriptConfig.triggerType
        val hasTopic = trig == ScriptConfig.TRIGGER_TOPIC || trig == ScriptConfig.TRIGGER_BOTH
        val hasTimer = trig == ScriptConfig.TRIGGER_TIMER || trig == ScriptConfig.TRIGGER_BOTH

        // Setup timer
        if (hasTimer && scriptConfig.timerIntervalMs > 0) {
            val intervalMs = scriptConfig.timerIntervalMs.toLong()
            logger.fine { "Setting up timer for '${deviceConfig.name}' every ${intervalMs}ms (aligned to wall-clock)" }
            scheduleNextTimer(intervalMs)
        }

        // Setup MQTT subscriptions
        if (hasTopic && scriptConfig.topicFilters.isNotEmpty()) {
            val sessionHandler = Monster.getSessionHandler()
            if (sessionHandler == null) {
                logger.warning("SessionHandler not available yet; topic subscriptions may be deferred")
            } else {
                internalClientId = "script-${deviceConfig.name}"
                vertx.eventBus().consumer<Any>(EventBusAddresses.Client.messages(internalClientId!!)) { busMessage: Message<Any> ->
                    try {
                        if (busMessage.replyAddress() != null) {
                            busMessage.reply(true)
                        }
                        val messages = when (val body = busMessage.body()) {
                            is BrokerMessage -> listOf(body)
                            is BulkClientMessage -> body.messages
                            else -> emptyList()
                        }
                        messages.forEach { msg ->
                            handleIncomingTopicMessage(msg)
                        }
                    } catch (e: Exception) {
                        logger.warning("Error in script message consumer for '${deviceConfig.name}': ${e.message}")
                    }
                }

                scriptConfig.topicFilters.forEach { pattern ->
                    sessionHandler.subscribeInternalClient(internalClientId!!, pattern, 0)
                }
            }
        }

        promise.complete()
        return promise.future()
    }

    private fun scheduleNextTimer(intervalMs: Long) {
        if (isStopped) return

        val now = System.currentTimeMillis()
        val (nextBoundaryMs, delayMs) = if (intervalMs >= 1000L) {
            // Align to round wall-clock boundary: e.g. :00s for 60s, :00/:05/:10 for 5m
            val next = ((now / intervalMs) + 1) * intervalMs
            val delay = (next - now).coerceAtLeast(1L)
            Pair(next, delay)
        } else {
            // Sub-second interval: simple periodic delay
            Pair(now + intervalMs, intervalMs)
        }

        timerId = vertx.setTimer(delayMs) {
            if (isStopped) return@setTimer
            val triggerTime = Instant.ofEpochMilli(nextBoundaryMs)
            val triggerContext = ScriptTriggerContext(
                type = "TIMER",
                time = triggerTime
            )
            dispatchExecution(null, null, triggerContext)
            scheduleNextTimer(intervalMs)
        }
    }

    private fun handleIncomingTopicMessage(msg: BrokerMessage) {
        if (scriptConfig.triggerOnChangeOnly) {
            val hash = msg.payload.contentHashCode()
            if (lastPayloadHash != null && lastPayloadHash == hash) {
                return // Skip unchanged payload
            }
            lastPayloadHash = hash
        }

        dispatchExecution(msg, null, ScriptTriggerContext("TOPIC", Instant.now()))
    }

    /**
     * Dispatch script execution handling SINGLETON (sequential) vs MULTI_INSTANCE (concurrent).
     */
    fun dispatchExecution(
        msg: BrokerMessage?,
        args: Map<String, Any?>?,
        origin: String
    ): Future<ScriptExecutionResult> {
        val triggerTime = if (origin == "TIMER" && scriptConfig.timerIntervalMs >= 1000) {
            val interval = scriptConfig.timerIntervalMs.toLong()
            val now = System.currentTimeMillis()
            Instant.ofEpochMilli((now / interval) * interval)
        } else {
            Instant.now()
        }
        return dispatchExecution(msg, args, ScriptTriggerContext(origin, triggerTime))
    }

    fun dispatchExecution(
        msg: BrokerMessage?,
        args: Map<String, Any?>?,
        triggerContext: ScriptTriggerContext
    ): Future<ScriptExecutionResult> {
        val promise = Promise.promise<ScriptExecutionResult>()

        val task: () -> ScriptExecutionResult = {
            try {
                val res = engine.execute(msg, args, dryRun = false, triggerContext = triggerContext)
                executionCount.incrementAndGet()
                lastExecutionTime = DateTimeFormatter.ISO_INSTANT.format(triggerContext.time)
                if (res.success) {
                    lastExecutionStatus = "SUCCESS"
                } else {
                    errorCount.incrementAndGet()
                    lastExecutionStatus = "ERROR"
                }
                res
            } catch (t: Throwable) {
                executionCount.incrementAndGet()
                errorCount.incrementAndGet()
                lastExecutionTime = DateTimeFormatter.ISO_INSTANT.format(triggerContext.time)
                lastExecutionStatus = "ERROR"
                recentLogs.add("[ERROR] Unhandled script execution error: ${t.message}")
                logger.severe("Script '${deviceConfig.name}' unhandled execution error: ${t.message}")
                ScriptExecutionResult(
                    success = false,
                    errors = listOf(t.message ?: "Unknown fatal error")
                )
            }
        }

        if (scriptConfig.instanceMode == ScriptConfig.MODE_SINGLETON) {
            synchronized(this) {
                currentExecution = currentExecution
                    .recover { Future.succeededFuture() }
                    .compose {
                        vertx.executeBlocking(java.util.concurrent.Callable {
                            task()
                        }).onComplete { res ->
                            if (res.succeeded()) promise.complete(res.result())
                            else promise.fail(res.cause())
                        }.mapEmpty()
                    }
            }
        } else {
            vertx.executeBlocking(java.util.concurrent.Callable {
                task()
            }).onComplete { res ->
                if (res.succeeded()) promise.complete(res.result())
                else promise.fail(res.cause())
            }
        }

        return promise.future()
    }

    /**
     * Synchronous callable execution for scripts.call().
     */
    fun executeCallable(args: Map<String, Any?>): Any? {
        val triggerContext = ScriptTriggerContext("CALLABLE", Instant.now())
        return try {
            val res = engine.execute(null, args, dryRun = false, triggerContext = triggerContext)
            executionCount.incrementAndGet()
            lastExecutionTime = DateTimeFormatter.ISO_INSTANT.format(triggerContext.time)
            if (res.success) {
                lastExecutionStatus = "SUCCESS"
            } else {
                errorCount.incrementAndGet()
                lastExecutionStatus = "ERROR"
            }
            res.returnValue
        } catch (t: Throwable) {
            executionCount.incrementAndGet()
            errorCount.incrementAndGet()
            lastExecutionTime = DateTimeFormatter.ISO_INSTANT.format(triggerContext.time)
            lastExecutionStatus = "ERROR"
            recentLogs.add("[ERROR] Unhandled script execution error: ${t.message}")
            logger.severe("Script '${deviceConfig.name}' unhandled execution error: ${t.message}")
            null
        }
    }

    private fun publishToBroker(topic: String, payload: ByteArray, qos: Int, retain: Boolean): Boolean {
        return try {
            val sessionHandler = Monster.getSessionHandler()
            if (sessionHandler != null) {
                val brokerMsg = BrokerMessage(
                    messageId = 0,
                    topicName = topic,
                    payload = payload,
                    qosLevel = qos,
                    isRetain = retain,
                    isDup = false,
                    isQueued = false,
                    clientId = "script-${deviceConfig.name}"
                )
                sessionHandler.publishMessage(brokerMsg)
                true
            } else {
                logger.warning("SessionHandler not available for publishing from script '${deviceConfig.name}'")
                false
            }
        } catch (e: Exception) {
            logger.severe("Error publishing message from script '${deviceConfig.name}': ${e.message}")
            false
        }
    }
}
