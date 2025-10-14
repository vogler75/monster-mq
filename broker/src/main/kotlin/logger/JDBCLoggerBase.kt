package at.rocworks.logger

import at.rocworks.Const
import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.logger.queue.ILoggerQueue
import at.rocworks.logger.queue.LoggerQueueDisk
import at.rocworks.logger.queue.LoggerQueueMemory
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.devices.JDBCLoggerConfig
import com.jayway.jsonpath.JsonPath
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import org.everit.json.schema.Schema
import org.everit.json.schema.ValidationException
import org.everit.json.schema.loader.SchemaLoader
import org.json.JSONObject
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger
import kotlin.concurrent.thread

/**
 * Abstract base class for JDBC Logger implementations.
 * Provides queue management, JSON schema validation, topic subscription, and bulk writing logic.
 * Subclasses implement database-specific connection and write operations.
 */
abstract class JDBCLoggerBase : AbstractVerticle() {

    protected val logger: Logger = Utils.getLogger(this::class.java)

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    protected lateinit var device: DeviceConfig
    protected lateinit var cfg: JDBCLoggerConfig

    // JSON Schema validator
    private lateinit var jsonSchemaValidator: Schema

    // JSONPath for dynamic table name extraction
    private var tableNameJsonPath: JsonPath? = null

    // Message queue (memory or disk-backed)
    private lateinit var queue: ILoggerQueue

    // Writer thread
    private val writerThreadStop = AtomicBoolean(false)
    private var writerThread: Thread? = null

    // Metrics counters (reset on each metrics request to calculate rates)
    private val messagesInCounter = AtomicLong(0)
    private val messagesValidatedCounter = AtomicLong(0)
    private val messagesSkippedCounter = AtomicLong(0)
    private val messagesWrittenCounter = AtomicLong(0)
    private val validationErrorsCounter = AtomicLong(0)
    private val writeErrorsCounter = AtomicLong(0)
    private var lastMetricsReset = System.currentTimeMillis()

    // Bulk timeout tracking
    private var lastBulkWrite = System.currentTimeMillis()

    // Connection state
    private val isReconnecting = AtomicBoolean(false)
    private var reconnectionAttempts = 0
    private var lastReconnectionAttempt = 0L
    private var lastConnectionErrorLog = 0L

    /**
     * Buffered row ready for database write
     */
    data class BufferedRow(
        val tableName: String,
        val fields: Map<String, Any?>,
        val timestamp: Instant,
        val topic: String
    )

    override fun start(startPromise: Promise<Void>) {
        try {
            // Load device configuration
            val cfgJson = config().getJsonObject("device")
            device = DeviceConfig.fromJsonObject(cfgJson)
            cfg = JDBCLoggerConfig.fromJson(device.config)

            logger.info("Starting JDBC Logger: ${device.name} (${cfg.databaseType})")

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
                logger.info("Using dynamic table name extraction: ${cfg.tableNameJsonPath}")
            } else {
                logger.info("Using fixed table name: ${cfg.tableName}")
            }

            // Initialize queue (memory or disk)
            initializeQueue()

            // Connect to database
            connect()
                .onSuccess {
                    logger.info("Database connection established")

                    // Subscribe to MQTT topics
                    subscribeToTopics()

                    // Register EventBus handler for metrics requests
                    setupEventBusHandlers()

                    // Start writer thread
                    startWriterThread()

                    logger.info("JDBC Logger ${device.name} started successfully")
                    startPromise.complete()
                }
                .onFailure { error ->
                    logger.severe("Failed to connect to database: ${error.message}")
                    startPromise.fail(error)
                }

        } catch (e: Exception) {
            logger.severe("Failed to start JDBC Logger: ${e.message}")
            e.printStackTrace()
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        try {
            logger.info("Stopping JDBC Logger: ${device.name}")

            // Stop writer thread
            writerThreadStop.set(true)
            writerThread?.join(5000) // Wait up to 5 seconds

            // Unsubscribe from topics
            unsubscribeFromTopics()

            // Close database connection
            disconnect()
                .onSuccess {
                    // Close queue
                    queue.close()
                    logger.info("JDBC Logger ${device.name} stopped successfully")
                    stopPromise.complete()
                }
                .onFailure { error ->
                    logger.warning("Error during disconnect: ${error.message}")
                    stopPromise.complete() // Complete anyway
                }

        } catch (e: Exception) {
            logger.warning("Error stopping JDBC Logger: ${e.message}")
            stopPromise.complete()
        }
    }

    private fun initializeJsonSchemaValidator() {
        try {
            val schemaJson = JSONObject(cfg.jsonSchema.encode())
            val schemaLoader = SchemaLoader.builder()
                .schemaJson(schemaJson)
                .build()
            jsonSchemaValidator = schemaLoader.load().build()
            logger.info("JSON Schema validator initialized")
        } catch (e: Exception) {
            throw IllegalArgumentException("Failed to load JSON Schema: ${e.message}", e)
        }
    }

    private fun initializeQueue() {
        queue = when (cfg.queueType) {
            "MEMORY" -> {
                logger.info("Using MEMORY queue (size: ${cfg.queueSize}, block: ${cfg.bulkSize})")
                LoggerQueueMemory(
                    logger = logger,
                    queueSize = cfg.queueSize,
                    blockSize = cfg.bulkSize,
                    pollTimeout = 10L // 10ms poll timeout
                )
            }
            "DISK" -> {
                logger.info("Using DISK queue (size: ${cfg.queueSize}, block: ${cfg.bulkSize}, path: ${cfg.diskPath})")
                LoggerQueueDisk(
                    deviceName = device.name,
                    logger = logger,
                    queueSize = cfg.queueSize,
                    blockSize = cfg.bulkSize,
                    pollTimeout = 10L,
                    diskPath = cfg.diskPath
                )
            }
            else -> {
                logger.warning("Unknown queue type: ${cfg.queueType}, using MEMORY")
                LoggerQueueMemory(logger, cfg.queueSize, cfg.bulkSize, 10L)
            }
        }
    }

    private fun subscribeToTopics() {
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler == null) {
            logger.warning("SessionHandler not available, cannot subscribe to topics")
            return
        }

        cfg.topicFilters.forEach { topicFilter ->
            logger.info("Subscribing to MQTT topic filter: $topicFilter")
            sessionHandler.subscribeInternal(
                clientId = "jdbc-logger-${device.name}",
                topicFilter = topicFilter,
                qos = 0
            ) { message ->
                handleIncomingMessage(message)
            }
        }
    }

    private fun unsubscribeFromTopics() {
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler == null) {
            return
        }

        cfg.topicFilters.forEach { topicFilter ->
            logger.info("Unsubscribing from MQTT topic filter: $topicFilter")
            sessionHandler.unsubscribeInternal("jdbc-logger-${device.name}", topicFilter)
        }
    }

    private fun setupEventBusHandlers() {
        // Register metrics handler
        val metricsAddress = at.rocworks.bus.EventBusAddresses.JDBCLoggerBridge.connectorMetrics(device.name)
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
                    .put("connected", true) // Always true if handler is running
                message.reply(response)
            } catch (e: Exception) {
                logger.warning("Error handling metrics request: ${e.message}")
                message.fail(500, e.message)
            }
        }
        logger.info("EventBus metrics handler registered at: $metricsAddress")
    }

    private fun handleIncomingMessage(message: BrokerMessage) {
        try {
            messagesInCounter.incrementAndGet()

            logger.finest { "Received message on topic: ${message.topicName}" }

            // Parse payload as JSON
            val payloadString = String(message.payload, Charsets.UTF_8)
            val payloadJson: JSONObject

            try {
                payloadJson = JSONObject(payloadString)
            } catch (e: Exception) {
                validationErrorsCounter.incrementAndGet()
                if (validationErrorsCounter.get() % 100 == 0L) {
                    logger.warning("Failed to parse JSON (${validationErrorsCounter.get()} total): ${e.message}")
                }
                return
            }

            // Validate against JSON Schema
            try {
                jsonSchemaValidator.validate(payloadJson)
            } catch (e: ValidationException) {
                validationErrorsCounter.incrementAndGet()
                logger.warning("Schema validation failed for topic ${message.topicName}: ${e.message}")
                return
            }

            // Extract table name
            val tableName = extractTableName(payloadJson)
            if (tableName == null) {
                messagesSkippedCounter.incrementAndGet()
                if (messagesSkippedCounter.get() % 100 == 0L) {
                    logger.warning("Could not extract table name (${messagesSkippedCounter.get()} total skipped)")
                }
                return
            }

            // Check if array expansion is configured
            val arrayPath = cfg.jsonSchema.getString("arrayPath")

            if (arrayPath != null) {
                // Array expansion mode: extract array and create multiple rows
                try {
                    val ctx = com.jayway.jsonpath.JsonPath.parse(payloadJson.toString())
                    val arrayResult = ctx.read<Any>(arrayPath)

                    if (arrayResult is List<*>) {
                        logger.finest { "Expanding array with ${arrayResult.size} items from path: $arrayPath" }

                        arrayResult.forEach { arrayItem ->
                            if (arrayItem != null) {
                                // Create a combined JSON with root fields + array item fields
                                val combinedJson = JSONObject(payloadJson.toString())

                                // If array item is an object, merge its fields
                                if (arrayItem is Map<*, *>) {
                                    arrayItem.forEach { (key, value) ->
                                        combinedJson.put(key.toString(), value)
                                    }
                                }

                                // Extract fields from combined JSON
                                val fields = extractFields(combinedJson)

                                // Check if all required fields are present
                                if (hasRequiredFields(fields)) {
                                    messagesValidatedCounter.incrementAndGet()
                                    queue.add(message)
                                } else {
                                    messagesSkippedCounter.incrementAndGet()
                                    logger.fine { "Missing required fields in array item from topic ${message.topicName}" }
                                }
                            }
                        }
                    } else {
                        logger.warning("arrayPath '$arrayPath' did not return an array")
                        messagesSkippedCounter.incrementAndGet()
                    }
                } catch (e: Exception) {
                    validationErrorsCounter.incrementAndGet()
                    logger.warning("Error expanding array from path '$arrayPath': ${e.message}")
                }
            } else {
                // Normal mode: single row
                val fields = extractFields(payloadJson)

                // Check if all required fields are present
                if (!hasRequiredFields(fields)) {
                    messagesSkippedCounter.incrementAndGet()
                    logger.fine { "Missing required fields in message on topic ${message.topicName}" }
                    return
                }

                messagesValidatedCounter.incrementAndGet()
                queue.add(message)
            }

        } catch (e: Exception) {
            validationErrorsCounter.incrementAndGet()
            logger.warning("Error handling message from topic ${message.topicName}: ${e.message}")
        }
    }

    private fun extractTableName(payloadJson: JSONObject): String? {
        return try {
            if (cfg.tableName != null) {
                // Fixed table name
                cfg.tableName
            } else if (tableNameJsonPath != null) {
                // Extract from JSON using JSONPath
                val result = tableNameJsonPath!!.read<Any>(payloadJson.toString())
                result?.toString()
            } else {
                null
            }
        } catch (e: Exception) {
            logger.warning("Failed to extract table name using JSONPath: ${e.message}")
            null
        }
    }

    private fun extractFields(payloadJson: JSONObject): Map<String, Any?> {
        val fields = mutableMapOf<String, Any?>()

        // Check if mapping is defined
        val mapping = cfg.jsonSchema.getJsonObject("mapping")

        // Extract only fields defined in JSON Schema properties
        val schemaProperties = cfg.jsonSchema.getJsonObject("properties")
        schemaProperties?.fieldNames()?.forEach { fieldName ->
            val fieldSchema = schemaProperties.getJsonObject(fieldName)
            val format = fieldSchema?.getString("format")

            // Get the JSONPath for this field, or use the field name directly
            val jsonPath = mapping?.getString(fieldName)

            val value = if (jsonPath != null) {
                // Use JSONPath to extract value
                try {
                    val ctx = com.jayway.jsonpath.JsonPath.parse(payloadJson.toString())
                    ctx.read<Any>(jsonPath)
                } catch (e: Exception) {
                    logger.fine { "JSONPath '$jsonPath' not found for field '$fieldName': ${e.message}" }
                    null
                }
            } else {
                // Direct field access (backwards compatibility)
                if (payloadJson.has(fieldName)) {
                    payloadJson.get(fieldName)
                } else {
                    null
                }
            }

            if (value != null) {
                fields[fieldName] = when {
                    value == JSONObject.NULL -> null
                    format == "timestamp" -> parseTimestamp(value)
                    format == "timestampms" -> parseTimestampMs(value)
                    value is JSONObject -> value.toString() // Convert nested objects to JSON string
                    value is org.json.JSONArray -> value.toString() // Convert arrays to JSON string
                    else -> normalizeValueType(value, fieldSchema)
                }
            } else if (format == "timestamp" || format == "timestampms") {
                // If timestamp field is missing, use current timestamp
                fields[fieldName] = java.sql.Timestamp(System.currentTimeMillis())
            }
        }

        return fields
    }

    private fun parseTimestamp(value: Any): java.sql.Timestamp {
        return when (value) {
            is String -> {
                try {
                    // Try ISO 8601 format
                    val instant = java.time.Instant.parse(value)
                    java.sql.Timestamp.from(instant)
                } catch (e: Exception) {
                    try {
                        // Try parsing as epoch milliseconds
                        java.sql.Timestamp(value.toLong())
                    } catch (e2: Exception) {
                        logger.warning("Failed to parse timestamp '$value', using current time")
                        java.sql.Timestamp(System.currentTimeMillis())
                    }
                }
            }
            is Number -> java.sql.Timestamp(value.toLong())
            else -> {
                logger.warning("Unknown timestamp format for value '$value', using current time")
                java.sql.Timestamp(System.currentTimeMillis())
            }
        }
    }

    private fun parseTimestampMs(value: Any): java.sql.Timestamp {
        return when (value) {
            is Number -> java.sql.Timestamp(value.toLong())
            is String -> {
                try {
                    java.sql.Timestamp(value.toLong())
                } catch (e: Exception) {
                    logger.warning("Failed to parse timestampms '$value' as long, using current time")
                    java.sql.Timestamp(System.currentTimeMillis())
                }
            }
            else -> {
                logger.warning("Unknown timestampms format for value '$value', using current time")
                java.sql.Timestamp(System.currentTimeMillis())
            }
        }
    }

    private fun hasRequiredFields(fields: Map<String, Any?>): Boolean {
        val required = cfg.jsonSchema.getJsonArray("required") ?: return true
        return required.all { requiredField ->
            fields.containsKey(requiredField.toString()) && fields[requiredField.toString()] != null
        }
    }

    /**
     * Normalizes value types to ensure consistent JDBC parameter types.
     * This prevents "Can't change resolved type for param" errors in PostgreSQL/QuestDB.
     */
    private fun normalizeValueType(value: Any, fieldSchema: io.vertx.core.json.JsonObject?): Any {
        val schemaType = fieldSchema?.getString("type") ?: "string"
        
        return when (schemaType) {
            "integer" -> {
                // Always convert to Long for consistency (PostgreSQL BIGINT)
                when (value) {
                    is Number -> value.toLong()
                    is String -> value.toLongOrNull() ?: 0L
                    else -> 0L
                }
            }
            "number" -> {
                // Always convert to Double for consistency (PostgreSQL DOUBLE PRECISION)
                when (value) {
                    is Number -> value.toDouble()
                    is String -> value.toDoubleOrNull() ?: 0.0
                    else -> 0.0
                }
            }
            "boolean" -> {
                when (value) {
                    is Boolean -> value
                    is String -> value.lowercase() in listOf("true", "1", "yes", "on")
                    is Number -> value.toDouble() != 0.0
                    else -> false
                }
            }
            else -> {
                // "string" or unknown types - convert to String
                value.toString()
            }
        }
    }

    private fun startWriterThread() {
        writerThread = thread(start = true, name = "jdbc-logger-${device.name}") {
            logger.info("Writer thread started")
            writerThreadStop.set(false)
            lastBulkWrite = System.currentTimeMillis()

            while (!writerThreadStop.get()) {
                try {
                    writeExecutor()
                } catch (e: Exception) {
                    logger.warning("Error in writer thread: ${e.message}")
                    e.printStackTrace()
                    Thread.sleep(1000) // Wait before retrying
                }
            }

            logger.info("Writer thread stopped")
        }
    }

    // Accumulation buffer for rows (persists across writeExecutor calls)
    private val accumulatedRows = mutableListOf<BufferedRow>()

    private fun writeExecutor() {
        // Process new messages from queue and collect them in a temporary buffer
        val newMessages = mutableListOf<BufferedRow>()
        
        // Poll block from queue
        val blockSize = queue.pollBlock { message ->
            try {
                // Parse and process each message
                val payloadString = String(message.payload, Charsets.UTF_8)
                val payloadJson = JSONObject(payloadString)

                val tableName = extractTableName(payloadJson)
                if (tableName != null) {
                    val fields = extractFields(payloadJson)
                    if (hasRequiredFields(fields)) {
                        newMessages.add(BufferedRow(tableName, fields, message.time, message.topicName))
                    }
                }
            } catch (e: Exception) {
                logger.warning("Error processing message in write executor: ${e.message}")
            }
        }

        // Check if we should write based on accumulation or timeout
        val now = System.currentTimeMillis()
        val timeoutReached = (now - lastBulkWrite) >= cfg.bulkTimeoutMs

        // Combine accumulated rows with new messages for size calculation
        val totalMessages = accumulatedRows.size + newMessages.size
        var shouldCommitQueue = false
        var hasConnectionError = false

        if (accumulatedRows.isNotEmpty() && (accumulatedRows.size >= cfg.bulkSize || timeoutReached)) {
            // We have enough rows or timeout reached, write them
            val byTable = accumulatedRows.groupBy { it.tableName }

            var allSuccess = true

            // Write each table's bulk
            byTable.forEach { (tableName, tableRows) ->
                try {
                    logger.fine { "Writing bulk of ${tableRows.size} rows to table $tableName" }
                    writeBulk(tableName, tableRows)
                    messagesWrittenCounter.addAndGet(tableRows.size.toLong())
                    // Reset reconnection attempt counter on successful write
                    reconnectionAttempts = 0
                } catch (e: Exception) {
                    writeErrorsCounter.incrementAndGet()

                    // Check if it's a connection error (these should reconnect)
                    if (isConnectionError(e)) {
                        handleConnectionError(e)
                        hasConnectionError = true
                        allSuccess = false
                        return@forEach // Continue to next table but don't try to write more
                    }

                    // For non-connection errors (schema/constraint violations), log as severe and skip
                    logger.severe("Non-recoverable error writing bulk to table $tableName: ${e.javaClass.name}: ${e.message}")
                    logger.severe("Skipping ${tableRows.size} messages for table $tableName to prevent infinite retry loop")
                    allSuccess = false
                    // Continue to next table, but don't retry these messages
                }
            }

            // Only clear accumulated rows if writes succeeded or had non-recoverable errors
            if (allSuccess || !hasConnectionError) {
                accumulatedRows.clear()
                lastBulkWrite = now
                shouldCommitQueue = true
            } else {
                // Connection error occurred, don't clear accumulated rows - they will be retried
                logger.fine("Keeping ${accumulatedRows.size} messages in buffer due to connection error, will retry")
                // Don't update lastBulkWrite so we'll retry immediately on next iteration
            }
        } else {
            // No bulk write attempted, safe to commit small batches
            shouldCommitQueue = true
        }

        // Only add new messages to accumulated buffer and commit queue if successful
        if (shouldCommitQueue) {
            // Add new messages to accumulation buffer
            accumulatedRows.addAll(newMessages)
            
            // Commit queue block
            if (blockSize > 0) {
                queue.pollCommit()
            }
        } else {
            // Don't commit queue - messages will be retried
            // Don't add newMessages to accumulatedRows - they're already in the queue
            logger.fine("Not committing ${newMessages.size} new messages due to connection error")
        }

        if (blockSize == 0 && accumulatedRows.isEmpty()) {
            // No messages polled and no messages to retry, sleep briefly
            Thread.sleep(10)
        } else if (hasConnectionError) {
            // We have messages to retry but connection failed, sleep briefly before retrying
            Thread.sleep(100)
        }
    }

    // Abstract methods to be implemented by subclasses

    /**
     * Connect to the database
     */
    abstract fun connect(): Future<Void>

    /**
     * Disconnect from the database
     */
    abstract fun disconnect(): Future<Void>

    /**
     * Write a bulk of rows to the specified table using JDBC batch
     * @param tableName The table to write to
     * @param rows The rows to write
     */
    abstract fun writeBulk(tableName: String, rows: List<BufferedRow>)

    /**
     * Check if an exception is a connection error (for retry logic)
     * @param e The exception to check
     * @return true if this is a connection error that should trigger retry
     */
    abstract fun isConnectionError(e: Exception): Boolean

    // Metrics
    fun getMetrics(): Map<String, Any> {
        val now = System.currentTimeMillis()
        val elapsedMs = now - lastMetricsReset
        val elapsedSec = if (elapsedMs > 0) elapsedMs / 1000.0 else 1.0

        // Get counter values and reset them
        val inCount = messagesInCounter.getAndSet(0)
        val validatedCount = messagesValidatedCounter.getAndSet(0)
        val writtenCount = messagesWrittenCounter.getAndSet(0)
        val skippedCount = messagesSkippedCounter.getAndSet(0)
        val validationErrorCount = validationErrorsCounter.getAndSet(0)
        val writeErrorCount = writeErrorsCounter.getAndSet(0)
        lastMetricsReset = now

        return mapOf(
            "messagesIn" to (inCount / elapsedSec),
            "messagesValidated" to (validatedCount / elapsedSec),
            "messagesWritten" to (writtenCount / elapsedSec),
            "messagesSkipped" to (skippedCount / elapsedSec),
            "validationErrors" to (validationErrorCount / elapsedSec),
            "writeErrors" to (writeErrorCount / elapsedSec),
            "queueSize" to queue.getSize(),
            "queueCapacity" to queue.getCapacity(),
            "queueFull" to queue.isQueueFull()
        )
    }

    /**
     * Handles connection errors with exponential backoff and persistent retries
     */
    private fun handleConnectionError(e: Exception) {
        val now = System.currentTimeMillis()

        // Rate limit connection error logging to avoid spam (max once per 30 seconds)
        if (now - lastConnectionErrorLog > 30000) {
            logger.warning("Database connection error detected: ${e.javaClass.simpleName}: ${e.message}")
            lastConnectionErrorLog = now
        }

        // Don't attempt reconnection too frequently - use configured delay
        if (now - lastReconnectionAttempt < cfg.reconnectDelayMs) {
            return
        }

        // Only one thread should handle reconnection at a time
        if (isReconnecting.compareAndSet(false, true)) {
            try {

                logger.info("Attempting to reconnect to database (attempt ${reconnectionAttempts + 1})...")
                lastReconnectionAttempt = now
                reconnectionAttempts++

                // Disconnect first
                try {
                    disconnect().toCompletionStage().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS)
                } catch (de: Exception) {
                    logger.fine("Error during disconnect: ${de.message}")
                }

                // Wait before reconnecting (use configured delay)
                Thread.sleep(cfg.reconnectDelayMs)

                // Attempt reconnection
                connect().toCompletionStage().toCompletableFuture().get(15, java.util.concurrent.TimeUnit.SECONDS)
                
                logger.info("Successfully reconnected to database after ${reconnectionAttempts} attempts")
                reconnectionAttempts = 0

            } catch (re: Exception) {
                // Rate limit reconnection failure logging
                if (reconnectionAttempts % 5 == 1 || now - lastConnectionErrorLog > 60000) {
                    logger.severe("Failed to reconnect to database (attempt ${reconnectionAttempts}): ${re.message}")
                    logger.info("Next reconnection attempt in ${cfg.reconnectDelayMs / 1000} seconds")
                }
            } finally {
                isReconnecting.set(false)
            }
        } else {
            // Another thread is handling reconnection, wait briefly
            Thread.sleep(1000)
        }
    }
}
