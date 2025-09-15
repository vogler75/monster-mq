package at.rocworks.stores.mongodb

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.stores.IMessageStoreExtended
import at.rocworks.stores.MessageStoreType
import at.rocworks.data.PurgeResult
import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.WriteConcern
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.*
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import org.bson.Document
import org.bson.conversions.Bson
import org.bson.types.Binary
import java.time.Instant
import java.util.concurrent.TimeUnit

/**
 * Enhanced MongoDB Message Store with performance optimizations
 * - Optimized wildcard topic matching using topic levels
 * - Connection pooling and write concern tuning
 * - Async operations to prevent blocking
 * - Implements IMessageStoreExtended for advanced features
 */
class MessageStoreMongoDB(
    private val name: String,
    private val connectionString: String,
    private val databaseName: String
) : AbstractVerticle(), IMessageStoreExtended {

    private val logger = Utils.getLogger(this::class.java, name)
    private val collectionName = name.lowercase()
    
    private lateinit var mongoClient: MongoClient
    private lateinit var database: MongoDatabase
    private lateinit var collection: MongoCollection<Document>
    
    // Cache for error reporting
    private var lastError: Int = 0

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun start(startPromise: Promise<Void>) {
        try {
            // Enhanced connection settings
            val settings = MongoClientSettings.builder()
                .applyConnectionString(ConnectionString(connectionString))
                .applyToConnectionPoolSettings { builder ->
                    builder.maxSize(50)
                    builder.minSize(10)
                    builder.maxWaitTime(2, TimeUnit.SECONDS)
                }
                .writeConcern(WriteConcern.W1.withJournal(false)) // Faster writes
                .build()

            mongoClient = MongoClients.create(settings)
            database = mongoClient.getDatabase(databaseName)

            if (!database.listCollectionNames().contains(collectionName)) {
                database.createCollection(collectionName)
                logger.info("Created collection: $collectionName")
            }

            collection = database.getCollection(collectionName)
            createOptimizedIndexes()
            
            logger.info("Enhanced Message Store [$name] is ready")
            startPromise.complete()
        } catch (e: Exception) {
            logger.severe("Error starting MongoDB connection: ${e.message}")
            startPromise.fail(e)
        }
    }

    private fun createOptimizedIndexes() {
        try {
            // Unique index on topic for exact matches
            collection.createIndex(
                Indexes.ascending("topic"),
                IndexOptions().unique(true).name("topic_unique_idx")
            )

            // Compound index for topic levels (optimized wildcard matching)
            collection.createIndex(
                Document(mapOf(
                    "topic_levels.L0" to 1,
                    "topic_levels.L1" to 1,
                    "topic_levels.L2" to 1,
                    "topic_levels.L3" to 1,
                    "topic_levels.L4" to 1
                )),
                IndexOptions().name("topic_levels_idx").sparse(true)
            )

            // Index for client_id queries
            collection.createIndex(
                Indexes.ascending("client_id"),
                IndexOptions().name("client_idx")
            )

            // Index for time-based queries
            collection.createIndex(
                Indexes.descending("time"),
                IndexOptions().name("time_idx")
            )

            logger.info("Created optimized indexes for $collectionName")
        } catch (e: Exception) {
            logger.warning("Error creating indexes: ${e.message}")
        }
    }

    private fun topicLevelsAsDocument(topicName: String): Document {
        val levels = Utils.getTopicLevels(topicName)
        val document = Document()
        levels.forEachIndexed { index, level ->
            document.append("L$index", level)
        }
        document.append("depth", levels.size)
        return document
    }

    override fun get(topicName: String): MqttMessage? {
        try {
            val document = collection.find(Filters.eq("topic", topicName)).first()
            document?.let {
                val payload = when (val p = it.get("payload")) {
                    is Binary -> p.data
                    is ByteArray -> p
                    else -> ByteArray(0)
                }
                
                return MqttMessage(
                    messageUuid = it.getString("message_uuid"),
                    messageId = 0,
                    topicName = topicName,
                    payload = payload,
                    qosLevel = it.getInteger("qos"),
                    isRetain = it.getBoolean("retained"),
                    isQueued = false,
                    clientId = it.getString("client_id"),
                    isDup = false
                )
            }
        } catch (e: Exception) {
            if (lastError != e.hashCode()) {
                logger.warning("Error fetching topic [$topicName]: ${e.message}")
                lastError = e.hashCode()
            }
        }
        return null
    }

    override fun getAsync(topicName: String, callback: (MqttMessage?) -> Unit) {
        // Use Vertx to execute MongoDB query asynchronously
        vertx.executeBlocking<MqttMessage?> {
            get(topicName)
        }.onComplete { result ->
            if (result.succeeded()) {
                callback(result.result())
            } else {
                logger.warning("Error in async get for topic [$topicName]: ${result.cause()?.message}")
                callback(null)
            }
        }
    }

    override fun addAll(messages: List<MqttMessage>) {
        if (messages.isEmpty()) return

        try {
            val bulkOperations = messages.map { message ->
                val filter = Filters.eq("topic", message.topicName)
                val update = Document("\$set", Document(mapOf(
                    "topic_levels" to topicLevelsAsDocument(message.topicName),
                    "time" to Instant.ofEpochMilli(message.time.toEpochMilli()),
                    "payload" to Binary(message.payload),
                    "payload_json" to message.getPayloadAsJson(),
                    "qos" to message.qosLevel,
                    "retained" to message.isRetain,
                    "client_id" to message.clientId,
                    "message_uuid" to message.messageUuid
                )))
                UpdateOneModel<Document>(filter, update, UpdateOptions().upsert(true))
            }
            
            // Bulk write with unordered for better performance
            val options = BulkWriteOptions().ordered(false)
            collection.bulkWrite(bulkOperations, options)
            
            if (lastError != 0) {
                logger.info("Batch insert successful after error")
                lastError = 0
            }
        } catch (e: Exception) {
            logger.warning("Error in bulk write: ${e.message}")
            lastError = e.hashCode()
        }
    }

    override fun delAll(topics: List<String>) {
        if (topics.isEmpty()) return

        try {
            val filter = Filters.`in`("topic", topics)
            collection.deleteMany(filter)
        } catch (e: Exception) {
            logger.warning("Error deleting topics: ${e.message}")
        }
    }

    override fun findMatchingMessages(topicName: String, callback: (MqttMessage) -> Boolean) {
        try {
            val filter = createWildcardFilter(topicName)
            val startTime = System.currentTimeMillis()
            var count = 0

            collection.find(filter).iterator().use { cursor ->
                while (cursor.hasNext()) {
                    val document = cursor.next()
                count++
                val topic = document.getString("topic") ?: ""
                val payload = when (val p = document.get("payload")) {
                    is Binary -> p.data
                    is ByteArray -> p
                    else -> ByteArray(0)
                }
                
                val message = MqttMessage(
                    messageUuid = document.getString("message_uuid") ?: "",
                    messageId = 0,
                    topicName = topic,
                    payload = payload,
                    qosLevel = document.getInteger("qos") ?: 0,
                    isRetain = true,
                    isDup = false,
                    isQueued = false,
                    clientId = document.getString("client_id") ?: ""
                )
                
                if (!callback(message)) {
                        break
                    }
                }
            }

            val duration = System.currentTimeMillis() - startTime
            logger.fine("Found $count messages in ${duration}ms for pattern [$topicName]")
            
        } catch (e: Exception) {
            logger.warning("Error finding messages for pattern [$topicName]: ${e.message}")
        }
    }

    /**
     * Create optimized filter for wildcard topic matching
     */
    private fun createWildcardFilter(topicPattern: String): Bson {
        if (!topicPattern.contains("+") && !topicPattern.contains("#")) {
            // Exact match - use indexed field
            return Filters.eq("topic", topicPattern)
        }

        val levels = Utils.getTopicLevels(topicPattern)
        val filters = mutableListOf<Bson>()

        levels.forEachIndexed { index, level ->
            when (level) {
                "#" -> {
                    // Multi-level wildcard - match depth >= current
                    if (index > 0) {
                        filters.add(Filters.gte("topic_levels.depth", index))
                    }
                    return@forEachIndexed
                }
                "+" -> {
                    // Single-level wildcard - just check depth
                    // Level can be anything, no filter needed
                }
                else -> {
                    // Exact level match
                    filters.add(Filters.eq("topic_levels.L$index", level))
                }
            }
        }

        // If pattern doesn't end with #, ensure exact depth match
        if (!topicPattern.endsWith("#")) {
            filters.add(Filters.eq("topic_levels.depth", levels.size))
        }

        return if (filters.isEmpty()) {
            Document() // Match all
        } else {
            Filters.and(filters)
        }
    }

    // IMessageStoreExtended implementations
    override fun findTopicsByName(name: String, ignoreCase: Boolean, namespace: String): List<String> {
        val filter = if (namespace.isNotEmpty()) {
            Filters.and(
                Filters.regex("topic", "^$namespace"),
                if (ignoreCase) {
                    Filters.regex("topic", name.replace("*", ".*"), "i")
                } else {
                    Filters.regex("topic", name.replace("*", ".*"))
                }
            )
        } else {
            if (ignoreCase) {
                Filters.regex("topic", name.replace("*", ".*"), "i")
            } else {
                Filters.regex("topic", name.replace("*", ".*"))
            }
        }

        return collection.find(filter)
            .projection(Projections.include("topic"))
            .map { it.getString("topic") }
            .into(mutableListOf())
    }

    override fun findTopicsByConfig(config: String, description: String, ignoreCase: Boolean, namespace: String): List<Pair<String, String>> {
        // This would require a separate config collection
        // For now, return empty list
        logger.warning("findTopicsByConfig not fully implemented for MongoDB")
        return emptyList()
    }

    override fun getName(): String = name
    override fun getType(): MessageStoreType = MessageStoreType.MONGODB

    override fun purgeOldMessages(olderThan: Instant): PurgeResult {
        val startTime = System.currentTimeMillis()
        var deletedCount = 0
        
        logger.fine("Starting purge for [$name] - removing messages older than $olderThan")
        
        try {
            val filter = Filters.lt("time", Instant.ofEpochMilli(olderThan.toEpochMilli()))
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

    override fun dropStorage(): Boolean {
        return try {
            collection.drop()
            logger.info("Dropped collection [$collectionName] for message store [$name]")
            true
        } catch (e: Exception) {
            logger.severe("Error dropping collection [$collectionName] for message store [$name]: ${e.message}")
            false
        }
    }

    override fun stop() {
        mongoClient.close()
        logger.info("MongoDB connection closed.")
    }
}