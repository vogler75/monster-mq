package at.rocworks.logger

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.logger.queue.ILoggerQueue
import at.rocworks.logger.queue.LoggerQueueDisk
import at.rocworks.logger.queue.LoggerQueueMemory
import at.rocworks.schema.JsonSchemaValidator
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.devices.ILoggerConfig
import com.jayway.jsonpath.JsonPath
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import org.json.JSONObject
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger
import kotlin.concurrent.thread

/**
 * Abstract base class for Logger implementations.
 * Provides queue management, JSON schema validation, topic subscription, and bulk writing logic.
 */
abstract class LoggerBase<T : ILoggerConfig> : AbstractVerticle() {

    protected val logger: Logger = Utils.getLogger(this::class.java)

    protected lateinit var device: DeviceConfig
    protected lateinit var cfg: T

    // JSON Schema validator (shared class)
    protected lateinit var jsonSchemaValidator: JsonSchemaValidator

    // JSONPath for dynamic table name extraction
    protected var tableNameJsonPath: JsonPath? = null

    // Message queue (memory or disk-backed)
    protected lateinit var queue: ILoggerQueue

    // Writer thread
    protected val writerThreadStop = AtomicBoolean(false)
    protected var writerThread: Thread? = null

    // Metrics counters
    protected val messagesInCounter = AtomicLong(0)
    protected val messagesValidatedCounter = AtomicLong(0)
    protected val messagesSkippedCounter = AtomicLong(0)
    protected val messagesWrittenCounter = AtomicLong(0)
    protected val validationErrorsCounter = AtomicLong(0)
    protected val writeErrorsCounter = AtomicLong(0)
    protected val duplicatesIgnoredCounter = AtomicLong(0)
    protected var lastMetricsReset = System.currentTimeMillis()

    // Bulk timeout tracking
    protected var lastBulkWrite = System.currentTimeMillis()

    // Connection state for retry logic
    protected val isReconnecting = AtomicBoolean(false)
    protected var reconnectionAttempts = 0
    protected var lastReconnectionAttempt = 0L
    protected var lastConnectionErrorLog = 0L

    /**
     * Buffered row ready for write
     */
    data class BufferedRow(
        val tableName: String,
        val fields: Map<String, Any?>,
        val timestamp: Instant,
        val topic: String
    )

    abstract fun loadConfig(cfgJson: io.vertx.core.json.JsonObject): T
    abstract fun connect(): Future<Void>
    abstract fun disconnect(): Future<Void>
    abstract fun writeBulk(tableName: String, rows: List<BufferedRow>)
    abstract fun isConnectionError(e: Exception): Boolean

    override fun start(startPromise: Promise<Void>) {
        try {
            // Load device configuration
            val cfgJson = config().getJsonObject("device")
            device = DeviceConfig.fromJsonObject(cfgJson)
            cfg = loadConfig(device.config)

            logger.info("Starting Logger: ${device.name}")

            // Validate configuration
            val validationErrors = cfg.validate()
            if (validationErrors.isNotEmpty()) {
                throw IllegalArgumentException("Config validation failed: ${validationErrors.joinToString(", ")}")
            }

            // Initialize JSON Schema validator
            initializeJsonSchemaValidator()

            // Initialize JSONPath if configured for dynamic table names
            if (cfg.tableNameJsonPath != null) {
                tableNameJsonPath = JsonPath.compile(cfg.tableNameJsonPath)
                logger.fine("Using dynamic table name extraction: ${cfg.tableNameJsonPath}")
            } else {
                logger.fine("Using fixed table name: ${cfg.tableName}")
            }

            // Initialize queue (memory or disk)
            initializeQueue()

            // Connect to destination
            connect()
                .onSuccess {
                    logger.fine("Connection established")
                    subscribeToTopics()
                    setupEventBusHandlers()
                    startWriterThread()
                    logger.info("Logger ${device.name} started successfully")
                    startPromise.complete()
                }
                .onFailure { error ->
                    logger.severe("Failed to connect: ${error.message}")
                    startPromise.fail(error)
                }

        } catch (e: Exception) {
            logger.severe("Failed to start Logger: ${e.message}")
            e.printStackTrace()
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        try {
            logger.info("Stopping Logger: ${device.name}")
            writerThreadStop.set(true)
            writerThread?.join(5000)
            unsubscribeFromTopics()
            disconnect()
                .onSuccess {
                    queue.close()
                    logger.info("Logger ${device.name} stopped successfully")
                    stopPromise.complete()
                }
                .onFailure { error ->
                    logger.warning("Error during disconnect: ${error.message}")
                    stopPromise.complete()
                }
        } catch (e: Exception) {
            logger.warning("Error stopping Logger: ${e.message}")
            stopPromise.complete()
        }
    }

    protected fun initializeJsonSchemaValidator() {
        try {
            jsonSchemaValidator = JsonSchemaValidator(cfg.jsonSchema)
            logger.info("JSON Schema validator initialized")
        } catch (e: Exception) {
            throw IllegalArgumentException("Failed to load JSON Schema: ${e.message}", e)
        }
    }

    protected fun initializeQueue() {
        queue = when (cfg.queueType) {
            "MEMORY" -> LoggerQueueMemory(logger, cfg.queueSize, cfg.bulkSize, 10L)
            "DISK" -> LoggerQueueDisk(device.name, logger, cfg.queueSize, cfg.bulkSize, 10L, cfg.diskPath)
            else -> {
                logger.warning("Unknown queue type: ${cfg.queueType}, using MEMORY")
                LoggerQueueMemory(logger, cfg.queueSize, cfg.bulkSize, 10L)
            }
        }
    }

    protected fun subscribeToTopics() {
        val sessionHandler = Monster.getSessionHandler() ?: return
        val clientId = "logger-${device.name}"
        vertx.eventBus().consumer<Any>(EventBusAddresses.Client.messages(clientId)) { busMessage ->
            try {
                when (val body = busMessage.body()) {
                    is BrokerMessage -> handleIncomingMessage(body)
                    is at.rocworks.data.BulkClientMessage -> body.messages.forEach { handleIncomingMessage(it) }
                }
            } catch (e: Exception) {
                logger.warning("Error processing message: ${e.message}")
            }
        }
        cfg.topicFilters.forEach { topicFilter ->
            sessionHandler.subscribeInternalClient(clientId, topicFilter, 0)
        }
    }

    protected fun unsubscribeFromTopics() {
        val sessionHandler = Monster.getSessionHandler() ?: return
        val clientId = "logger-${device.name}"
        cfg.topicFilters.forEach { topicFilter ->
            sessionHandler.unsubscribeInternalClient(clientId, topicFilter)
        }
        sessionHandler.unregisterInternalClient(clientId)
    }

    abstract fun getMetricsAddress(): String

    protected open fun setupEventBusHandlers() {
        val metricsAddress = getMetricsAddress()
        vertx.eventBus().consumer<io.vertx.core.json.JsonObject>(metricsAddress) { message ->
            try {
                val metrics = getMetrics()
                val response = io.vertx.core.json.JsonObject()
                    .put("messagesIn", metrics["messagesIn"])
                    .put("messagesValidated", metrics["messagesValidated"])
                    .put("messagesWritten", metrics["messagesWritten"])
                    .put("messagesSkipped", metrics["messagesSkipped"])
                    .put("validationErrors", metrics["validationErrors"])
                    .put("writeErrors", metrics["writeErrors"])
                    .put("queueSize", metrics["queueSize"])
                    .put("queueCapacity", metrics["queueCapacity"])
                    .put("queueFull", metrics["queueFull"])
                    .put("connected", true)
                message.reply(response)
            } catch (e: Exception) {
                message.fail(500, e.message)
            }
        }
    }

    protected fun handleIncomingMessage(message: BrokerMessage) {
        try {
            messagesInCounter.incrementAndGet()
            val payloadString = String(message.payload, Charsets.UTF_8)
            val payloadJson = JSONObject(payloadString)

            if (cfg.jsonSchema.getJsonObject("mapping") == null) {
                val result = jsonSchemaValidator.validate(payloadJson)
                if (!result.valid) {
                    validationErrorsCounter.incrementAndGet()
                    return
                }
            }

            val tableName = extractTableName(payloadJson)
            if (tableName == null) {
                messagesSkippedCounter.incrementAndGet()
                return
            }

            val arrayPath = cfg.jsonSchema.getString("arrayPath")
            if (arrayPath != null) {
                val ctx = com.jayway.jsonpath.JsonPath.parse(payloadJson.toString())
                val arrayResult = ctx.read<Any>(arrayPath)
                if (arrayResult is List<*>) {
                    arrayResult.forEach { arrayItem ->
                        if (arrayItem != null) {
                            val combinedJson = JSONObject(payloadJson.toString())
                            if (arrayItem is Map<*, *>) {
                                arrayItem.forEach { (key, value) -> combinedJson.put(key.toString(), value) }
                            }
                            if (hasRequiredFields(extractFields(combinedJson))) {
                                messagesValidatedCounter.incrementAndGet()
                                queue.add(message)
                            }
                        }
                    }
                }
            } else {
                if (hasRequiredFields(extractFields(payloadJson))) {
                    messagesValidatedCounter.incrementAndGet()
                    queue.add(message)
                } else {
                    messagesSkippedCounter.incrementAndGet()
                }
            }
        } catch (e: Exception) {
            validationErrorsCounter.incrementAndGet()
        }
    }

    protected fun extractTableName(payloadJson: JSONObject): String? {
        return try {
            if (cfg.tableName != null) cfg.tableName
            else if (tableNameJsonPath != null) tableNameJsonPath!!.read<Any>(payloadJson.toString())?.toString()
            else null
        } catch (e: Exception) { null }
    }

    protected fun extractFields(payloadJson: JSONObject): Map<String, Any?> {
        val fields = mutableMapOf<String, Any?>()
        val mapping = cfg.jsonSchema.getJsonObject("mapping")
        val schemaProperties = cfg.jsonSchema.getJsonObject("properties")

        schemaProperties?.fieldNames()?.forEach { fieldName ->
            val fieldSchema = schemaProperties.getJsonObject(fieldName)
            val format = fieldSchema?.getString("format")
            val jsonPath = mapping?.getString(fieldName)
            val value = if (jsonPath != null) {
                try { com.jayway.jsonpath.JsonPath.parse(payloadJson.toString()).read<Any>(jsonPath) } catch (e: Exception) { null }
            } else {
                if (payloadJson.has(fieldName)) payloadJson.get(fieldName) else null
            }

            if (value != null) {
                fields[fieldName] = when {
                    value == JSONObject.NULL -> null
                    format == "timestamp" -> parseTimestamp(value)
                    format == "timestampms" -> parseTimestampMs(value)
                    value is JSONObject -> value.toString()
                    value is org.json.JSONArray -> value.toString()
                    else -> normalizeValueType(value, fieldSchema)
                }
            } else if (format == "timestamp" || format == "timestampms") {
                fields[fieldName] = java.sql.Timestamp(System.currentTimeMillis())
            }
        }
        return fields
    }

    protected fun parseTimestamp(value: Any): java.sql.Timestamp {
        return when (value) {
            is String -> {
                try { java.sql.Timestamp.from(java.time.Instant.parse(value)) }
                catch (e: Exception) { try { java.sql.Timestamp(value.toLong()) } catch (e2: Exception) { java.sql.Timestamp(System.currentTimeMillis()) } }
            }
            is Number -> java.sql.Timestamp(value.toLong())
            else -> java.sql.Timestamp(System.currentTimeMillis())
        }
    }

    protected fun parseTimestampMs(value: Any): java.sql.Timestamp {
        return when (value) {
            is Number -> java.sql.Timestamp(value.toLong())
            is String -> try { java.sql.Timestamp(value.toLong()) } catch (e: Exception) { java.sql.Timestamp(System.currentTimeMillis()) }
            else -> java.sql.Timestamp(System.currentTimeMillis())
        }
    }

    protected fun hasRequiredFields(fields: Map<String, Any?>): Boolean {
        val required = cfg.jsonSchema.getJsonArray("required") ?: return true
        return required.all { fields.containsKey(it.toString()) && fields[it.toString()] != null }
    }

    protected fun normalizeValueType(value: Any, fieldSchema: io.vertx.core.json.JsonObject?): Any {
        val schemaType = fieldSchema?.getString("type") ?: "string"
        return when (schemaType) {
            "integer" -> when (value) { is Number -> value.toLong(); is String -> value.toLongOrNull() ?: 0L; else -> 0L }
            "number" -> when (value) { is Number -> value.toDouble(); is String -> value.toDoubleOrNull() ?: 0.0; else -> 0.0 }
            "boolean" -> when (value) { is Boolean -> value; is String -> value.lowercase() in listOf("true", "1", "yes", "on"); is Number -> value.toDouble() != 0.0; else -> false }
            else -> value.toString()
        }
    }

    protected fun startWriterThread() {
        writerThread = thread(start = true, name = "logger-${device.name}") {
            writerThreadStop.set(false)
            while (!writerThreadStop.get()) {
                try { writeExecutor() } catch (e: Exception) { Thread.sleep(1000) }
            }
        }
    }

    protected val accumulatedRows = mutableListOf<BufferedRow>()

    protected fun writeExecutor() {
        val newMessages = mutableListOf<BufferedRow>()
        val blockSize = queue.pollBlock { message ->
            try {
                val payloadJson = JSONObject(String(message.payload, Charsets.UTF_8))
                val tableName = extractTableName(payloadJson)
                if (tableName != null) {
                    val arrayPath = cfg.jsonSchema.getString("arrayPath")
                    if (arrayPath != null) {
                        val ctx = com.jayway.jsonpath.JsonPath.parse(payloadJson.toString())
                        val arrayResult = ctx.read<Any>(arrayPath)
                        if (arrayResult is List<*>) {
                            arrayResult.forEach { arrayItem ->
                                if (arrayItem != null) {
                                    val combinedJson = JSONObject(payloadJson.toString())
                                    if (arrayItem is Map<*, *>) arrayItem.forEach { (k, v) -> combinedJson.put(k.toString(), v) }
                                    val fields = extractFields(combinedJson)
                                    if (hasRequiredFields(fields)) newMessages.add(BufferedRow(tableName, fields, message.time, message.topicName))
                                }
                            }
                        }
                    } else {
                        val fields = extractFields(payloadJson)
                        if (hasRequiredFields(fields)) newMessages.add(BufferedRow(tableName, fields, message.time, message.topicName))
                    }
                }
            } catch (e: Exception) { logger.warning("Error parsing message: ${e.message}") }
        }

        val now = System.currentTimeMillis()
        val timeoutReached = (now - lastBulkWrite) >= cfg.bulkTimeoutMs
        var shouldCommitQueue = false
        var hasConnectionError = false

        if (accumulatedRows.isNotEmpty() && (accumulatedRows.size >= cfg.bulkSize || timeoutReached)) {
            val byTable = accumulatedRows.groupBy { it.tableName }
            var allSuccess = true
            byTable.forEach { (tableName, tableRows) ->
                try {
                    writeBulk(tableName, tableRows)
                    messagesWrittenCounter.addAndGet(tableRows.size.toLong())
                    reconnectionAttempts = 0
                } catch (e: Exception) {
                    writeErrorsCounter.incrementAndGet()
                    if (isConnectionError(e)) {
                        handleConnectionError(e)
                        hasConnectionError = true
                        allSuccess = false
                        return@forEach
                    } else {
                        logger.severe("Non-recoverable error writing to $tableName: ${e.message}")
                        allSuccess = false
                    }
                }
            }
            if (allSuccess || !hasConnectionError) {
                accumulatedRows.clear()
                lastBulkWrite = now
                shouldCommitQueue = true
            }
        } else {
            shouldCommitQueue = true
        }

        if (shouldCommitQueue) {
            accumulatedRows.addAll(newMessages)
            if (blockSize > 0) queue.pollCommit()
        }

        if (blockSize == 0 && accumulatedRows.isEmpty()) Thread.sleep(10)
        else if (hasConnectionError) Thread.sleep(100)
    }

    protected open fun handleConnectionError(e: Exception) {
        val now = System.currentTimeMillis()
        if (now - lastConnectionErrorLog > 30000) {
            logger.warning("Connection error: ${e.javaClass.simpleName}: ${e.message}")
            lastConnectionErrorLog = now
        }
        if (now - lastReconnectionAttempt < cfg.reconnectDelayMs) return
        if (isReconnecting.compareAndSet(false, true)) {
            try {
                logger.info("Reconnecting (attempt ${reconnectionAttempts + 1})...")
                lastReconnectionAttempt = now
                reconnectionAttempts++
                try { disconnect().toCompletionStage().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS) } catch (de: Exception) {}
                Thread.sleep(cfg.reconnectDelayMs)
                connect().toCompletionStage().toCompletableFuture().get(15, java.util.concurrent.TimeUnit.SECONDS)
                logger.info("Reconnected after $reconnectionAttempts attempts")
                reconnectionAttempts = 0
            } catch (re: Exception) {
                logger.severe("Failed to reconnect: ${re.message}")
            } finally { isReconnecting.set(false) }
        }
    }

    protected fun getMetrics(): Map<String, Any> {
        val now = System.currentTimeMillis()
        val elapsedSec = (now - lastMetricsReset).coerceAtLeast(1000) / 1000.0
        lastMetricsReset = now
        return mapOf(
            "messagesIn" to (messagesInCounter.getAndSet(0) / elapsedSec),
            "messagesValidated" to (messagesValidatedCounter.getAndSet(0) / elapsedSec),
            "messagesWritten" to (messagesWrittenCounter.getAndSet(0) / elapsedSec),
            "messagesSkipped" to (messagesSkippedCounter.getAndSet(0) / elapsedSec),
            "validationErrors" to (validationErrorsCounter.getAndSet(0) / elapsedSec),
            "writeErrors" to (writeErrorsCounter.getAndSet(0) / elapsedSec),
            "queueSize" to queue.getSize(),
            "queueCapacity" to queue.getCapacity(),
            "queueFull" to queue.isQueueFull()
        )
    }
}
