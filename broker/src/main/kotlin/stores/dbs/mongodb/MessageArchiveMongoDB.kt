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
        try {
            // Compound index for topic + time queries (most common)
            targetCollection.createIndex(
                Document(mapOf(
                    "meta.topic" to 1,
                    "time" to -1
                )),
                IndexOptions().name("topic_time_idx")
            )

            // Index for time-range queries
            targetCollection.createIndex(
                Indexes.descending("time"),
                IndexOptions().name("time_idx")
            )

            // Text index for topic pattern matching (better than regex)
            // NOTE: Commented out because MongoDB time-series collections do not support text indexes
            // Error: "Text indexes are not supported on time-series collections" (code 72)
            // Topic pattern matching will fall back to regex queries which are less efficient
            // but still functional for the archive use case
            /*
            targetCollection.createIndex(
                Indexes.text("meta.topic"),
                IndexOptions().name("topic_text_idx")
            )
            */

            // Index for client_id queries
            targetCollection.createIndex(
                Indexes.ascending("meta.client_id"),
                IndexOptions().name("client_idx")
            )

            logger.info("Created optimized indexes for collection: $collectionName")
        } catch (e: Exception) {
            logger.warning("Error creating indexes: ${e.message}")
        }
    }

    override fun addHistory(messages: List<BrokerMessage>) {
        if (messages.isEmpty()) return

        try {
            val documents = messages.map { message ->
                val doc = Document(mapOf(
                    "meta" to Document(mapOf(
                        "topic" to message.topicName,
                        "client_id" to message.clientId,
                        "message_uuid" to message.messageUuid,
                        "qos" to message.qosLevel,
                        "retained" to message.isRetain
                    )),
                    "time" to Date(message.time.toEpochMilli())
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
            filters.add(Filters.gte("time", Date.from(it)))
        }
        endTime?.let {
            filters.add(Filters.lte("time", Date.from(it)))
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

                    val messageObj = JsonObject()
                        .put("topic", meta?.getString("topic") ?: topic)
                        .put("timestamp", timestamp)
                        .put("qos", meta?.getInteger("qos") ?: 0)
                        .put("client_id", meta?.getString("client_id") ?: "")

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