package at.rocworks.stores.mongodb

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.IMessageArchiveExtended
import at.rocworks.stores.MessageArchiveType
import at.rocworks.data.PurgeResult
import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.*
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.bson.Document
import org.bson.conversions.Bson
import org.bson.types.Binary
import java.time.Instant
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * Enhanced MongoDB Message Archive with performance optimizations
 * - Implements IMessageArchiveExtended for GraphQL/MCP support
 * - Connection pooling and async operations
 * - Optimized indexes for time-series queries
 * - Batch operations with write concern tuning
 */
class MessageArchiveMongoDB(
    private val name: String,
    private val connectionString: String,
    private val databaseName: String,
    private val payloadFormat: at.rocworks.stores.PayloadFormat = at.rocworks.stores.PayloadFormat.DEFAULT
): AbstractVerticle(), IMessageArchiveExtended {

    private val logger = Utils.getLogger(this::class.java, name)
    private val collectionName = name.lowercase()

    @Volatile
    private var mongoClient: MongoClient? = null
    @Volatile
    private var database: MongoDatabase? = null
    @Volatile
    private var collection: MongoCollection<Document>? = null
    @Volatile
    private var isConnected: Boolean = false
    @Volatile
    private var lastConnectionAttempt: Long = 0

    private val connectionRetryInterval = 30_000L // 30 seconds
    private val healthCheckInterval = 10_000L // 10 seconds for more responsive detection

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun start(startPromise: Promise<Void>) {
        logger.info("Starting MongoDB Message Archive [$name] with async connection management")

        // Start connection establishment in background
        initiateConnection()

        // Set up periodic health check and reconnection
        vertx.setPeriodic(healthCheckInterval) {
            performHealthCheck()
        }

        // Complete startup immediately - connections will be established asynchronously
        startPromise.complete()
        logger.info("MongoDB Message Archive [$name] startup completed - connections will be established in background")
    }

    /**
     * Initiates MongoDB connection in background thread
     */
    private fun initiateConnection() {
        if (System.currentTimeMillis() - lastConnectionAttempt < connectionRetryInterval) {
            return // Too soon to retry
        }

        lastConnectionAttempt = System.currentTimeMillis()

        vertx.executeBlocking(java.util.concurrent.Callable {
            try {
                logger.info("Attempting to connect to MongoDB for archive [$name]...")

                // Enhanced connection settings with pooling
                val settings = MongoClientSettings.builder()
                    .applyConnectionString(ConnectionString(connectionString))
                    .applyToConnectionPoolSettings { builder ->
                        builder.maxSize(50)              // Max connections in pool
                        builder.minSize(10)              // Min connections to maintain
                        builder.maxWaitTime(2, TimeUnit.SECONDS)
                        builder.maxConnectionLifeTime(30, TimeUnit.MINUTES)
                        builder.maxConnectionIdleTime(10, TimeUnit.MINUTES)
                    }
                    .applyToSocketSettings { builder ->
                        builder.connectTimeout(5, TimeUnit.SECONDS)
                        builder.readTimeout(10, TimeUnit.SECONDS)
                    }
                    .build()

                val newClient = MongoClients.create(settings)
                val newDatabase = newClient.getDatabase(databaseName)

                // Test connection with ping
                newDatabase.runCommand(Document("ping", 1))

                // Create time-series collection if not exists
                if (!newDatabase.listCollectionNames().into(mutableListOf()).contains(collectionName)) {
                    val timeSeriesOptions = TimeSeriesOptions("time")
                        .metaField("meta")
                        .granularity(TimeSeriesGranularity.SECONDS) // Better for MQTT

                    val createCollectionOptions = CreateCollectionOptions()
                        .timeSeriesOptions(timeSeriesOptions)
                        .expireAfter(365, TimeUnit.DAYS) // Optional: auto-expire old data

                    newDatabase.createCollection(collectionName, createCollectionOptions)
                    logger.info("Created time-series collection: $collectionName")
                }

                val newCollection = newDatabase.getCollection(collectionName)

                // Create optimized indexes for queries
                createIndexes(newCollection)

                // Update connection state atomically
                synchronized(this) {
                    // Close old connection if exists
                    mongoClient?.close()

                    mongoClient = newClient
                    database = newDatabase
                    collection = newCollection
                    isConnected = true
                }

                logger.info("MongoDB Message Archive [$name] connected successfully")

            } catch (e: Exception) {
                logger.warning("Failed to connect to MongoDB for archive [$name]: ${e.message}")
                synchronized(this) {
                    isConnected = false
                }
            }
        })
    }

    /**
     * Performs periodic health check and reconnection if needed
     */
    private fun performHealthCheck() {
        if (!isConnected) {
            logger.fine { "MongoDB archive [$name] not connected, attempting reconnection..." }
            initiateConnection()
            return
        }

        // Test existing connection
        vertx.executeBlocking(java.util.concurrent.Callable {
            try {
                database?.runCommand(Document("ping", 1))
                // Connection is healthy
            } catch (e: Exception) {
                logger.warning("MongoDB archive [$name] health check failed: ${e.message}")
                synchronized(this) {
                    isConnected = false
                }
                // Will reconnect on next health check
            }
        })
    }

    private fun createIndexes(targetCollection: MongoCollection<Document>) {
        // MongoDB time-series collections automatically create a compound index on (meta, time)
        // With our optimized structure (only 'topic' in meta), the automatic index covers:
        // - topic + time queries (most common access pattern)
        //
        // Additional index on time only for pure time-range queries (e.g., purge operations)
        try {
            targetCollection.createIndex(
                Indexes.descending("time"),
                IndexOptions().name("time_idx")
            )
            logger.fine { "Created time_idx index for collection: $collectionName" }
        } catch (e: Exception) {
            logger.warning("Error creating time index: ${e.message}")
        }
    }

    override fun addHistory(messages: List<BrokerMessage>) {
        if (messages.isEmpty()) return

        try {
            val documents = messages.map { message ->
                // Document structure optimized for time-series index:
                // - 'meta' contains ONLY 'topic' for optimal (topic, time) index
                // - Other fields at top level (don't affect index efficiency)
                val doc = Document(mapOf(
                    "meta" to Document("topic", message.topicName),
                    "time" to Date(message.time.toEpochMilli()),
                    "client_id" to message.clientId,
                    "qos" to message.qosLevel,
                    "retained" to message.isRetain,
                    "message_uuid" to message.messageUuid
                ))

                // Only try JSON conversion if payloadFormat is JSON
                if (payloadFormat == at.rocworks.stores.PayloadFormat.JSON) {
                    try {
                        // Use MongoDB's Document.parse() directly - it will throw if not valid JSON
                        doc["payload"] = Document.parse(String(message.payload, Charsets.UTF_8))
                    } catch (e: Exception) {
                        // Not valid JSON - store as binary
                        doc["payload_blob"] = message.payload
                    }
                } else {
                    // DEFAULT format - store as binary only
                    doc["payload_blob"] = message.payload
                }

                doc
            }

            // Batch insert with unordered for better performance
            val options = InsertManyOptions()
                .ordered(false)  // Continue on error
                .bypassDocumentValidation(true)  // Faster inserts

            getActiveCollection()?.insertMany(documents, options) ?: run {
                logger.warning("MongoDB not connected, skipping batch insert for [$name]")
            }
        } catch (e: Exception) {
            logger.warning("Error inserting batch data: ${e.message}")
        }
    }

    override fun getHistory(
        topic: String,
        startTime: Instant?,
        endTime: Instant?,
        limit: Int
    ): JsonArray {
        logger.fine { "MongoDB getHistory: topic=$topic, startTime=$startTime, endTime=$endTime, limit=$limit" }
        
        val filters = mutableListOf<Bson>()
        
        // Topic filter - support wildcards
        if (topic.contains("#")) {
           val regex = topic.replace("#", ".*")
           filters.add(Filters.regex("meta.topic", "^$regex$"))
        } else {
            filters.add(Filters.eq("meta.topic", topic))
        }
        
        // Time range filters
        startTime?.let {
            val dateFrom = Date.from(it)
            logger.fine { "MongoDB time filter: startTime=$it -> Date=$dateFrom (epoch=${dateFrom.time})" }
            filters.add(Filters.gte("time", dateFrom))
        }
        endTime?.let {
            val dateTo = Date.from(it)
            logger.fine { "MongoDB time filter: endTime=$it -> Date=$dateTo (epoch=${dateTo.time})" }
            filters.add(Filters.lte("time", dateTo))
        }

        val filter = if (filters.isEmpty()) {
            Document()
        } else {
            Filters.and(filters)
        }

        val messages = JsonArray()
        
        try {
            val activeCollection = getActiveCollection()
            if (activeCollection == null) {
                logger.warning("MongoDB not connected, returning empty history for [$name]")
                return messages
            }

            val startQuery = System.currentTimeMillis()

            activeCollection.find(filter)
                .sort(Sorts.descending("time"))
                .limit(limit)
                .forEach { doc ->
                    val meta = doc.get("meta", Document::class.java)
                    val timestamp = doc.getDate("time").toInstant().toEpochMilli()

                    // Read fields: new structure has qos/client_id at top level, legacy has them in meta
                    val messageObj = JsonObject()
                        .put("topic", meta?.getString("topic") ?: topic)
                        .put("timestamp", timestamp)
                        .put("qos", doc.getInteger("qos") ?: meta?.getInteger("qos") ?: 0)
                        .put("client_id", doc.getString("client_id") ?: meta?.getString("client_id") ?: "")

                    // Handle both new format (payload as BSON) and legacy format (payload_blob/payload_json)
                    val nativePayload = doc.get("payload")
                    if (nativePayload != null) {
                        // New format: payload stored as native BSON object
                        val payloadJson = when (nativePayload) {
                            is Document -> nativePayload.toJson()
                            is List<*> -> JsonArray(nativePayload).encode()
                            else -> nativePayload.toString()
                        }
                        messageObj.put("payload_json", payloadJson)
                        messageObj.put("payload_base64", Base64.getEncoder().encodeToString(payloadJson.toByteArray()))
                    } else {
                        // Legacy format: payload_blob and/or payload_json
                        val payloadBytes = when (val payloadBlob = doc.get("payload_blob")) {
                            is Binary -> payloadBlob.data
                            is ByteArray -> payloadBlob
                            else -> ByteArray(0)
                        }
                        messageObj.put("payload_base64", Base64.getEncoder().encodeToString(payloadBytes))
                        messageObj.put("payload_json", doc.getString("payload_json"))
                    }

                    messages.add(messageObj)
                }
            
            val queryDuration = System.currentTimeMillis() - startQuery
            logger.fine { "MongoDB query completed in ${queryDuration}ms, returned ${messages.size()} messages" }
            
        } catch (e: Exception) {
            logger.severe("Error retrieving history for topic [$topic]: ${e.message}")
            e.printStackTrace()
        }
        
        return messages
    }

    override fun executeQuery(sql: String): JsonArray {
        logger.warning("SQL queries not supported in MongoDB. Use MongoDB aggregation pipeline instead.")
        return JsonArray().add("MongoDB does not support SQL queries")
    }

    override fun getAggregatedHistory(
        topics: List<String>,
        startTime: Instant,
        endTime: Instant,
        intervalMinutes: Int,
        functions: List<String>,
        fields: List<String>
    ): JsonObject {
        if (topics.isEmpty()) {
            return JsonObject()
                .put("columns", JsonArray().add("timestamp"))
                .put("rows", JsonArray())
        }

        logger.fine { "MongoDB getAggregatedHistory: topics=$topics, interval=${intervalMinutes}min, functions=$functions, fields=$fields" }

        val result = JsonObject()
        val columns = JsonArray().add("timestamp")
        val rows = JsonArray()

        try {
            val activeCollection = getActiveCollection()
            if (activeCollection == null) {
                logger.warning("MongoDB not connected, returning empty aggregation result for [$name]")
                return result.put("columns", columns).put("rows", rows)
            }

            // Build the aggregation pipeline
            val pipeline = mutableListOf<Document>()

            // Stage 1: Match documents by topics and time range
            pipeline.add(Document("\$match", Document(mapOf(
                "meta.topic" to Document("\$in", topics),
                "time" to Document(mapOf(
                    "\$gte" to Date.from(startTime),
                    "\$lte" to Date.from(endTime)
                ))
            ))))

            // Stage 2: Project the fields we need, including value extraction
            val projectFields = mutableMapOf<String, Any>(
                "topic" to "\$meta.topic",
                "time" to 1
            )

            // For each field (or raw value), create a projection
            val effectiveFields = if (fields.isEmpty()) listOf("") else fields
            for ((fieldIndex, field) in effectiveFields.withIndex()) {
                val valueExpr = if (field.isEmpty()) {
                    // Try to convert payload to double - handle native BSON, payload_blob (binary), and payload_json
                    // Priority: 1) payload (native BSON), 2) payload_blob (binary text), 3) payload_json (string)
                    // Use $function to decode binary data as UTF-8 text
                    Document("\$function", Document(mapOf(
                        "body" to """
                            function(payload, payload_blob, payload_json) {
                                // Helper function to decode base64 to UTF-8 string
                                function base64ToUtf8(base64) {
                                    // Decode base64 to bytes, then to UTF-8 string
                                    var binStr = '';
                                    var bytes = [];
                                    var chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
                                    var i = 0;
                                    base64 = base64.replace(/[^A-Za-z0-9+/]/g, '');
                                    while (i < base64.length) {
                                        var enc1 = chars.indexOf(base64.charAt(i++));
                                        var enc2 = chars.indexOf(base64.charAt(i++));
                                        var enc3 = chars.indexOf(base64.charAt(i++));
                                        var enc4 = chars.indexOf(base64.charAt(i++));
                                        var chr1 = (enc1 << 2) | (enc2 >> 4);
                                        var chr2 = ((enc2 & 15) << 4) | (enc3 >> 2);
                                        var chr3 = ((enc3 & 3) << 6) | enc4;
                                        binStr += String.fromCharCode(chr1);
                                        if (enc3 !== 64) binStr += String.fromCharCode(chr2);
                                        if (enc4 !== 64) binStr += String.fromCharCode(chr3);
                                    }
                                    return binStr;
                                }

                                // Case 1: payload exists (native BSON - could be object with numeric fields or numeric value)
                                if (payload !== null && payload !== undefined) {
                                    if (typeof payload === 'number') return payload;
                                    if (typeof payload === 'string') {
                                        var n = parseFloat(payload);
                                        return isNaN(n) ? null : n;
                                    }
                                    return null;
                                }
                                // Case 2: payload_blob exists (binary containing text of number)
                                if (payload_blob !== null && payload_blob !== undefined) {
                                    try {
                                        // payload_blob is BinData - get base64 string and decode it
                                        var base64Str = payload_blob.base64();
                                        var str = base64ToUtf8(base64Str);
                                        var n = parseFloat(str);
                                        return isNaN(n) ? null : n;
                                    } catch (e) {
                                        return null;
                                    }
                                }
                                // Case 3: payload_json exists (string)
                                if (payload_json !== null && payload_json !== undefined) {
                                    var n = parseFloat(payload_json);
                                    return isNaN(n) ? null : n;
                                }
                                return null;
                            }
                        """.trimIndent(),
                        "args" to listOf("\$payload", "\$payload_blob", "\$payload_json"),
                        "lang" to "js"
                    )))
                } else {
                    // Extract field from payload object - handle nested paths
                    val pathParts = field.split(".")
                    val fieldPath = "\$payload." + pathParts.joinToString(".")
                    Document("\$toDouble", Document("\$ifNull", listOf(fieldPath, null)))
                }
                projectFields["value_$fieldIndex"] = valueExpr
            }

            pipeline.add(Document("\$project", Document(projectFields)))

            // Stage 3: Group by time bucket and topic
            // MongoDB 5.0+ supports $dateTrunc for time bucketing
            val dateTruncUnit = when {
                intervalMinutes >= 1440 -> "day"
                intervalMinutes >= 60 -> "hour"
                else -> "minute"
            }
            val binSize = when {
                intervalMinutes >= 1440 -> intervalMinutes / 1440
                intervalMinutes >= 60 -> intervalMinutes / 60
                else -> intervalMinutes
            }

            val bucketExpr = Document("\$dateTrunc", Document(mapOf(
                "date" to "\$time",
                "unit" to dateTruncUnit,
                "binSize" to binSize
            )))

            // Build accumulator expressions for each field and function
            val groupAccumulators = mutableMapOf<String, Any>(
                "_id" to Document(mapOf(
                    "bucket" to bucketExpr,
                    "topic" to "\$topic"
                ))
            )

            val columnNames = mutableListOf<String>()
            for ((fieldIndex, field) in effectiveFields.withIndex()) {
                val fieldAlias = if (field.isEmpty()) "" else ".${field.replace(".", "_")}"
                val valueRef = "\$value_$fieldIndex"

                for (func in functions) {
                    val funcLower = func.lowercase()
                    val accumKey = "agg_${fieldIndex}_$funcLower"

                    val accumExpr = when (func.uppercase()) {
                        "AVG" -> Document("\$avg", valueRef)
                        "MIN" -> Document("\$min", valueRef)
                        "MAX" -> Document("\$max", valueRef)
                        "COUNT" -> Document("\$sum", Document("\$cond", listOf(
                            Document("\$ne", listOf(valueRef, null)),
                            1,
                            0
                        )))
                        else -> Document("\$avg", valueRef)
                    }
                    groupAccumulators[accumKey] = accumExpr
                }
            }

            pipeline.add(Document("\$group", Document(groupAccumulators)))

            // Stage 4: Sort by bucket time
            pipeline.add(Document("\$sort", Document("_id.bucket", 1)))

            logger.fine { "MongoDB aggregation pipeline: $pipeline" }

            val queryStart = System.currentTimeMillis()

            // Execute aggregation and collect results grouped by bucket
            val bucketData = mutableMapOf<String, MutableMap<String, Any?>>()

            activeCollection.aggregate(pipeline)
                .allowDiskUse(true)
                .forEach { doc ->
                    val id = doc.get("_id", Document::class.java)
                    val bucket = id?.getDate("bucket")?.toInstant()?.toString() ?: return@forEach
                    val topic = id.getString("topic") ?: return@forEach

                    if (!bucketData.containsKey(bucket)) {
                        bucketData[bucket] = mutableMapOf()
                    }

                    // Store aggregated values for this topic in this bucket
                    for ((fieldIndex, field) in effectiveFields.withIndex()) {
                        val fieldAlias = if (field.isEmpty()) "" else ".${field.replace(".", "_")}"
                        for (func in functions) {
                            val funcLower = func.lowercase()
                            val accumKey = "agg_${fieldIndex}_$funcLower"
                            val colName = "$topic$fieldAlias" + "_$funcLower"
                            val value = doc.get(accumKey)
                            bucketData[bucket]!![colName] = when (value) {
                                is Number -> value.toDouble()
                                else -> null
                            }
                        }
                    }
                }

            val queryDuration = System.currentTimeMillis() - queryStart
            logger.fine { "MongoDB aggregation query completed in ${queryDuration}ms" }

            // Build column names (after processing to know all topics)
            for (topic in topics) {
                for (field in effectiveFields) {
                    val fieldAlias = if (field.isEmpty()) "" else ".${field.replace(".", "_")}"
                    for (func in functions) {
                        val funcLower = func.lowercase()
                        val colName = "$topic$fieldAlias" + "_$funcLower"
                        columnNames.add(colName)
                        columns.add(colName)
                    }
                }
            }

            // Convert bucket data to rows (sorted by timestamp)
            bucketData.keys.sorted().forEach { bucket ->
                val row = JsonArray()
                row.add(bucket)

                for (colName in columnNames) {
                    val value = bucketData[bucket]?.get(colName)
                    if (value != null) {
                        row.add(value)
                    } else {
                        row.addNull()
                    }
                }
                rows.add(row)
            }

        } catch (e: Exception) {
            logger.severe("Error executing MongoDB aggregation query: ${e.message}")
            e.printStackTrace()
        }

        result.put("columns", columns)
        result.put("rows", rows)
        logger.fine { "MongoDB aggregation returned ${rows.size()} rows with ${columns.size()} columns" }
        return result
    }

    /**
     * MongoDB-specific: Execute aggregation pipeline
     */
    fun executeAggregation(pipeline: List<Document>): JsonArray {
        val result = JsonArray()
        try {
            val startTime = System.currentTimeMillis()
            
            val activeCollection = getActiveCollection()
            if (activeCollection == null) {
                logger.warning("MongoDB not connected, returning empty search results for [$name]")
                return result
            }

            activeCollection.aggregate(pipeline)
                .allowDiskUse(true)  // Allow using disk for large aggregations
                .forEach { doc ->
                    result.add(JsonObject(doc.toJson()))
                }
            
            val duration = System.currentTimeMillis() - startTime
            logger.fine { "Aggregation completed in ${duration}ms with ${result.size()} results" }
            
        } catch (e: Exception) {
            logger.severe("Error executing aggregation: ${e.message}")
        }
        return result
    }

    /**
     * Get statistics for a topic over time
     */
    fun getTopicStatistics(topic: String, startTime: Instant, endTime: Instant): JsonObject {
        val pipeline = listOf(
            Document("\$match", Document(mapOf(
                "meta.topic" to topic,
                "time" to Document(mapOf(
                    "\$gte" to Date.from(startTime),
                    "\$lte" to Date.from(endTime)
                ))
            ))),
            Document("\$group", Document(mapOf(
                "_id" to Document("\$dateToString", Document(mapOf(
                    "format" to "%Y-%m-%d %H:00",
                    "date" to "\$time"
                ))),
                "count" to Document("\$sum", 1),
                "avg_size" to Document("\$avg", Document("\$bsonSize", "\$payload_blob"))
            ))),
            Document("\$sort", Document("_id", 1))
        )
        
        val results = executeAggregation(pipeline)
        return JsonObject().put("statistics", results)
    }

    override fun getName(): String = name
    override fun getType(): MessageArchiveType = MessageArchiveType.MONGODB

    override fun getConnectionStatus(): Boolean {
        // Just return the cached connection status - no real-time testing
        // Real-time testing is done by background health checks
        return isConnected && mongoClient != null && database != null && collection != null
    }

    /**
     * Ensures connection is available, returns null if not connected
     */
    private fun getActiveCollection(): MongoCollection<Document>? {
        return if (isConnected) collection else null
    }

    override fun purgeOldMessages(olderThan: Instant): PurgeResult {
        val startTime = System.currentTimeMillis()
        var deletedCount = 0
        
        logger.fine { "Starting purge for [$name] - removing messages older than $olderThan" }
        
        try {
            // For time-series collections, use Date format for filtering
            val filter = Filters.lt("time", Date.from(olderThan))
            val result = getActiveCollection()?.deleteMany(filter) ?: run {
                logger.warning("MongoDB not connected, skipping purge for [$name]")
                return PurgeResult(0, System.currentTimeMillis() - startTime)
            }
            deletedCount = result.deletedCount.toInt()
        } catch (e: Exception) {
            logger.severe("Error purging old messages from [$name]: ${e.message}")
        }
        
        val elapsedTimeMs = System.currentTimeMillis() - startTime
        val purgeResult = PurgeResult(deletedCount, elapsedTimeMs)
        
        logger.fine { "Purge completed for [$name]: deleted ${purgeResult.deletedCount} messages in ${purgeResult.elapsedTimeMs}ms" }
        
        return purgeResult
    }

    override fun dropStorage(): Boolean {
        return try {
            getActiveCollection()?.drop() ?: run {
                logger.warning("MongoDB not connected, cannot drop collection for [$name]")
            }
            logger.info("Dropped collection [$collectionName] for message archive [$name]")
            true
        } catch (e: Exception) {
            logger.severe("Error dropping collection [$collectionName] for message archive [$name]: ${e.message}")
            false
        }
    }

    override fun stop() {
        mongoClient?.close()
        logger.info("MongoDB connection closed.")
    }

    override suspend fun tableExists(): Boolean {
        return try {
            if (!isConnected || database == null) {
                logger.warning("MongoDB not connected, cannot check collection existence for [$collectionName]")
                return false
            }
            val collections = database!!.listCollectionNames().into(mutableListOf())
            collections.contains(collectionName)
        } catch (e: Exception) {
            logger.warning("Error checking if collection [$collectionName] exists: ${e.message}")
            false
        }
    }

    override suspend fun createTable(): Boolean {
        return try {
            if (!isConnected || database == null) {
                logger.warning("MongoDB not connected, cannot create collection for [$collectionName]")
                return false
            }

            // Create time-series collection if not exists
            if (!database!!.listCollectionNames().into(mutableListOf()).contains(collectionName)) {
                val timeSeriesOptions = TimeSeriesOptions("time")
                    .metaField("meta")
                    .granularity(TimeSeriesGranularity.SECONDS) // Better for MQTT

                val createCollectionOptions = CreateCollectionOptions()
                    .timeSeriesOptions(timeSeriesOptions)
                    .expireAfter(365, TimeUnit.DAYS) // Optional: auto-expire old data

                database!!.createCollection(collectionName, createCollectionOptions)
                logger.info("Created time-series collection: $collectionName")
            }

            val newCollection = database!!.getCollection(collectionName)

            // Create optimized indexes for queries
            createIndexes(newCollection)

            logger.info("Table (collection) created for message archive [$name]")
            true
        } catch (e: Exception) {
            logger.warning("Error creating table: ${e.message}")
            false
        }
    }
}