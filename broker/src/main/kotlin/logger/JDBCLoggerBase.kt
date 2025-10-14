package at.rocworks.logger

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

    // Metrics
    private val messagesIn = AtomicLong(0)
    private val messagesValidated = AtomicLong(0)
    private val messagesSkipped = AtomicLong(0)
    private val messagesWritten = AtomicLong(0)
    private val validationErrors = AtomicLong(0)
    private val writeErrors = AtomicLong(0)

    // Bulk timeout tracking
    private var lastBulkWrite = System.currentTimeMillis()

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
                    .put("messagesWritten", metrics["messagesWritten"])
                    .put("errors", metrics["writeErrors"])
                    .put("queueSize", metrics["queueSize"])
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
            messagesIn.incrementAndGet()

            // Temporary INFO logging to diagnose message flow
            if (messagesIn.get() % 100 == 0L) {
                logger.info("Received ${messagesIn.get()} messages, validated: ${messagesValidated.get()}, written: ${messagesWritten.get()}, errors: ${validationErrors.get()}, skipped: ${messagesSkipped.get()}, queue: ${queue.getSize()}/${queue.getCapacity()}")
            }

            logger.finest { "Received message on topic: ${message.topicName}" }

            // Parse payload as JSON
            val payloadString = String(message.payload, Charsets.UTF_8)
            val payloadJson: JSONObject

            try {
                payloadJson = JSONObject(payloadString)
            } catch (e: Exception) {
                validationErrors.incrementAndGet()
                if (validationErrors.get() % 100 == 0L) {
                    logger.warning("Failed to parse JSON (${validationErrors.get()} total): ${e.message}")
                }
                return
            }

            // Validate against JSON Schema
            try {
                jsonSchemaValidator.validate(payloadJson)
            } catch (e: ValidationException) {
                validationErrors.incrementAndGet()
                logger.warning("Schema validation failed for topic ${message.topicName}: ${e.message}")
                return
            }

            // Extract table name
            val tableName = extractTableName(payloadJson)
            if (tableName == null) {
                messagesSkipped.incrementAndGet()
                if (messagesSkipped.get() % 100 == 0L) {
                    logger.warning("Could not extract table name (${messagesSkipped.get()} total skipped)")
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
                                    messagesValidated.incrementAndGet()
                                    queue.add(message)
                                } else {
                                    messagesSkipped.incrementAndGet()
                                    logger.fine { "Missing required fields in array item from topic ${message.topicName}" }
                                }
                            }
                        }
                    } else {
                        logger.warning("arrayPath '$arrayPath' did not return an array")
                        messagesSkipped.incrementAndGet()
                    }
                } catch (e: Exception) {
                    validationErrors.incrementAndGet()
                    logger.warning("Error expanding array from path '$arrayPath': ${e.message}")
                }
            } else {
                // Normal mode: single row
                val fields = extractFields(payloadJson)

                // Check if all required fields are present
                if (!hasRequiredFields(fields)) {
                    messagesSkipped.incrementAndGet()
                    logger.fine { "Missing required fields in message on topic ${message.topicName}" }
                    return
                }

                messagesValidated.incrementAndGet()
                queue.add(message)
            }

        } catch (e: Exception) {
            validationErrors.incrementAndGet()
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
                    else -> value
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
                        accumulatedRows.add(BufferedRow(tableName, fields, message.time, message.topicName))
                    }
                }
            } catch (e: Exception) {
                logger.warning("Error processing message in write executor: ${e.message}")
            }
        }

        // Always commit the queue block after processing (prevents retries)
        // Accumulation happens in accumulatedRows buffer instead
        if (blockSize > 0) {
            queue.pollCommit()
        }

        // Check if we should write based on accumulation or timeout
        val now = System.currentTimeMillis()
        val timeoutReached = (now - lastBulkWrite) >= cfg.bulkTimeoutMs

        if (accumulatedRows.isNotEmpty() && (accumulatedRows.size >= cfg.bulkSize || timeoutReached)) {
            // We have enough rows or timeout reached, write them
            val byTable = accumulatedRows.groupBy { it.tableName }

            var allSuccess = true

            // Write each table's bulk
            byTable.forEach { (tableName, tableRows) ->
                try {
                    logger.fine { "Writing bulk of ${tableRows.size} rows to table $tableName" }
                    writeBulk(tableName, tableRows)
                    messagesWritten.addAndGet(tableRows.size.toLong())
                } catch (e: Exception) {
                    writeErrors.incrementAndGet()

                    // Check if it's a connection error (these should retry)
                    if (isConnectionError(e)) {
                        logger.warning("Database connection error detected, will retry accumulated buffer: ${e.javaClass.simpleName}: ${e.message}")
                        // Sleep and retry next iteration (rows stay in accumulatedRows)
                        Thread.sleep(1000)
                        return
                    }

                    // For non-connection errors (schema/constraint violations), log as severe and skip
                    logger.severe("Non-recoverable error writing bulk to table $tableName: ${e.javaClass.name}: ${e.message}")
                    logger.severe("Skipping ${tableRows.size} messages for table $tableName to prevent infinite retry loop")
                    allSuccess = false
                    // Continue to next table, but don't retry these messages
                }
            }

            // Clear accumulated rows after successful write (or non-recoverable error)
            accumulatedRows.clear()
            lastBulkWrite = now
        } else if (blockSize == 0) {
            // No messages polled, sleep briefly
            Thread.sleep(10)
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
        return mapOf(
            "messagesIn" to messagesIn.get(),
            "messagesValidated" to messagesValidated.get(),
            "messagesSkipped" to messagesSkipped.get(),
            "messagesWritten" to messagesWritten.get(),
            "validationErrors" to validationErrors.get(),
            "writeErrors" to writeErrors.get(),
            "queueSize" to queue.getSize(),
            "queueCapacity" to queue.getCapacity(),
            "queueFull" to queue.isQueueFull()
        )
    }
}
