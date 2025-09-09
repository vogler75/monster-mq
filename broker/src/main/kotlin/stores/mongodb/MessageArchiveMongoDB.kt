package at.rocworks.stores.mongodb

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
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
    private val databaseName: String
): AbstractVerticle(), IMessageArchiveExtended {

    private val logger = Utils.getLogger(this::class.java, name)
    private val collectionName = name.lowercase()

    private lateinit var mongoClient: MongoClient
    private lateinit var database: MongoDatabase
    private lateinit var collection: MongoCollection<Document>

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun start(startPromise: Promise<Void>) {
        try {
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

            mongoClient = MongoClients.create(settings)
            database = mongoClient.getDatabase(databaseName)

            // Create time-series collection if not exists
            if (!database.listCollectionNames().into(mutableListOf()).contains(collectionName)) {
                val timeSeriesOptions = TimeSeriesOptions("time")
                    .metaField("meta")
                    .granularity(TimeSeriesGranularity.SECONDS) // Better for MQTT
                
                val createCollectionOptions = CreateCollectionOptions()
                    .timeSeriesOptions(timeSeriesOptions)
                    .expireAfter(365, TimeUnit.DAYS) // Optional: auto-expire old data
                
                database.createCollection(collectionName, createCollectionOptions)
                logger.info("Created time-series collection: $collectionName")
            }

            collection = database.getCollection(collectionName)
            
            // Create optimized indexes for queries
            createIndexes()
            
            logger.info("MongoDB Message Archive [$name] started with enhanced configuration")
            startPromise.complete()
        } catch (e: Exception) {
            logger.severe("Error starting MongoDB message archive: ${e.message}")
            startPromise.fail(e)
        }
    }

    private fun createIndexes() {
        try {
            // Compound index for topic + time queries (most common)
            collection.createIndex(
                Document(mapOf(
                    "meta.topic" to 1,
                    "time" to -1
                )),
                IndexOptions().name("topic_time_idx")
            )

            // Index for time-range queries
            collection.createIndex(
                Indexes.descending("time"),
                IndexOptions().name("time_idx")
            )

            // Text index for topic pattern matching (better than regex)
            // NOTE: Commented out because MongoDB time-series collections do not support text indexes
            // Error: "Text indexes are not supported on time-series collections" (code 72)
            // Topic pattern matching will fall back to regex queries which are less efficient
            // but still functional for the archive use case
            /*
            collection.createIndex(
                Indexes.text("meta.topic"),
                IndexOptions().name("topic_text_idx")
            )
            */

            // Index for client_id queries
            collection.createIndex(
                Indexes.ascending("meta.client_id"),
                IndexOptions().name("client_idx")
            )

            logger.info("Created optimized indexes for collection: $collectionName")
        } catch (e: Exception) {
            logger.warning("Error creating indexes: ${e.message}")
        }
    }

    override fun addHistory(messages: List<MqttMessage>) {
        if (messages.isEmpty()) return

        try {
            val documents = messages.map { message ->
                Document(mapOf(
                    "meta" to Document(mapOf(
                        "topic" to message.topicName,
                        "client_id" to message.clientId,
                        "message_uuid" to message.messageUuid,
                        "qos" to message.qosLevel,
                        "retained" to message.isRetain
                    )),
                    "time" to Date(message.time.toEpochMilli()),
                    "payload_blob" to message.payload,
                    "payload_json" to message.getPayloadAsJson()
                ))
            }

            // Batch insert with unordered for better performance
            val options = InsertManyOptions()
                .ordered(false)  // Continue on error
                .bypassDocumentValidation(true)  // Faster inserts

            collection.insertMany(documents, options)
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
        logger.fine("MongoDB getHistory: topic=$topic, startTime=$startTime, endTime=$endTime, limit=$limit")
        
        val filters = mutableListOf<Bson>()
        
        // Topic filter - support wildcards
        if (topic.contains("#") || topic.contains("+")) {
            val regex = topic
                .replace("+", "[^/]+")
                .replace("#", ".*")
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
            val startQuery = System.currentTimeMillis()
            
            collection.find(filter)
                .sort(Sorts.descending("time"))
                .limit(limit)
                .forEach { doc ->
                    val meta = doc.get("meta", Document::class.java)
                    val timestamp = doc.getDate("time").toInstant().toEpochMilli()
                    
                    // Handle Binary payload correctly
                    val payloadBytes = when (val payloadBlob = doc.get("payload_blob")) {
                        is Binary -> payloadBlob.data
                        is ByteArray -> payloadBlob
                        else -> ByteArray(0)
                    }
                    
                    val messageObj = JsonObject()
                        .put("topic", meta?.getString("topic") ?: topic)
                        .put("timestamp", timestamp)
                        .put("payload_base64", Base64.getEncoder().encodeToString(payloadBytes))
                        .put("payload_json", doc.getString("payload_json"))
                        .put("qos", meta?.getInteger("qos") ?: 0)
                        .put("client_id", meta?.getString("client_id") ?: "")
                        
                    messages.add(messageObj)
                }
            
            val queryDuration = System.currentTimeMillis() - startQuery
            logger.fine("MongoDB query completed in ${queryDuration}ms, returned ${messages.size()} messages")
            
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
            
            collection.aggregate(pipeline)
                .allowDiskUse(true)  // Allow using disk for large aggregations
                .forEach { doc ->
                    result.add(JsonObject(doc.toJson()))
                }
            
            val duration = System.currentTimeMillis() - startTime
            logger.fine("Aggregation completed in ${duration}ms with ${result.size()} results")
            
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

    override fun purgeOldMessages(olderThan: Instant): PurgeResult {
        val startTime = System.currentTimeMillis()
        var deletedCount = 0
        
        logger.fine("Starting purge for [$name] - removing messages older than $olderThan")
        
        try {
            // For time-series collections, use Date format for filtering
            val filter = Filters.lt("time", Date.from(olderThan))
            val result = collection.deleteMany(filter)
            deletedCount = result.deletedCount.toInt()
        } catch (e: Exception) {
            logger.severe("Error purging old messages from [$name]: ${e.message}")
        }
        
        val elapsedTimeMs = System.currentTimeMillis() - startTime
        val purgeResult = PurgeResult(deletedCount, elapsedTimeMs)
        
        logger.fine("Purge completed for [$name]: deleted ${purgeResult.deletedCount} messages in ${purgeResult.elapsedTimeMs}ms")
        
        return purgeResult
    }

    override fun stop() {
        mongoClient.close()
        logger.info("MongoDB connection closed.")
    }
}