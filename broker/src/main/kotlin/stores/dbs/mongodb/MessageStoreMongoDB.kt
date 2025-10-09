package at.rocworks.stores.mongodb

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
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

    // Cache for error reporting
    private var lastError: Int = 0

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun start(startPromise: Promise<Void>) {
        logger.info("Starting MongoDB Message Store [$name] with async connection management")

        // Start connection establishment in background
        initiateConnection()

        // Set up periodic health check and reconnection
        vertx.setPeriodic(healthCheckInterval) {
            performHealthCheck()
        }

        // Complete startup immediately - connections will be established asynchronously
        startPromise.complete()
        logger.info("MongoDB Message Store [$name] startup completed - connections will be established in background")
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
                logger.info("Attempting to connect to MongoDB for store [$name]...")

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
                    .writeConcern(WriteConcern.W1.withJournal(false)) // Faster writes
                    .build()

                val newClient = MongoClients.create(settings)
                val newDatabase = newClient.getDatabase(databaseName)

                // Test connection with ping
                newDatabase.runCommand(org.bson.Document("ping", 1))

                // Create collection if not exists
                if (!newDatabase.listCollectionNames().into(mutableListOf()).contains(collectionName)) {
                    newDatabase.createCollection(collectionName)
                    logger.info("Created collection: $collectionName")
                }

                val newCollection = newDatabase.getCollection(collectionName)

                // Create optimized indexes for queries
                createOptimizedIndexes(newCollection)

                // Update connection state atomically
                synchronized(this) {
                    // Close old connection if exists
                    mongoClient?.close()

                    mongoClient = newClient
                    database = newDatabase
                    collection = newCollection
                    isConnected = true
                }

                logger.info("MongoDB Message Store [$name] connected successfully")

            } catch (e: Exception) {
                logger.warning("Failed to connect to MongoDB for store [$name]: ${e.message}")
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
            logger.fine { "MongoDB store [$name] not connected, attempting reconnection..." }
            initiateConnection()
            return
        }

        // Test existing connection
        vertx.executeBlocking(java.util.concurrent.Callable {
            try {
                database?.runCommand(org.bson.Document("ping", 1))
                // Connection is healthy
            } catch (e: Exception) {
                logger.warning("MongoDB store [$name] health check failed: ${e.message}")
                synchronized(this) {
                    isConnected = false
                }
                // Will reconnect on next health check
            }
        })
    }

    private fun createOptimizedIndexes(targetCollection: MongoCollection<Document>) {
        try {
            // Unique index on topic for exact matches
            targetCollection.createIndex(
                Indexes.ascending("topic"),
                IndexOptions().unique(true).name("topic_unique_idx")
            )

            // Compound index for topic levels (optimized wildcard matching)
            targetCollection.createIndex(
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
            targetCollection.createIndex(
                Indexes.ascending("client_id"),
                IndexOptions().name("client_idx")
            )

            // Index for time-based queries
            targetCollection.createIndex(
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

    override fun get(topicName: String): BrokerMessage? {
        try {
            val activeCollection = getActiveCollection() ?: run {
                logger.fine { "MongoDB not connected, returning null for topic [$topicName]" }
                return null
            }

            val document = activeCollection.find(Filters.eq("topic", topicName)).first()
            document?.let {
                val payload = when (val p = it.get("payload")) {
                    is Binary -> p.data
                    is ByteArray -> p
                    else -> ByteArray(0)
                }

                return BrokerMessage(
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

    override fun getAsync(topicName: String, callback: (BrokerMessage?) -> Unit) {
        // Use Vertx to execute MongoDB query asynchronously
        vertx.executeBlocking<BrokerMessage?> {
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

    override fun addAll(messages: List<BrokerMessage>) {
        if (messages.isEmpty()) return

        try {
            val activeCollection = getActiveCollection() ?: run {
                logger.warning("MongoDB not connected, skipping batch insert for [$name]")
                return
            }

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
            activeCollection.bulkWrite(bulkOperations, options)
            
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
            val activeCollection = getActiveCollection() ?: run {
                logger.warning("MongoDB not connected, skipping delete for [$name]")
                return
            }

            val filter = Filters.`in`("topic", topics)
            activeCollection.deleteMany(filter)
        } catch (e: Exception) {
            logger.warning("Error deleting topics: ${e.message}")
        }
    }

    override fun findMatchingMessages(topicName: String, callback: (BrokerMessage) -> Boolean) {
        try {
            val activeCollection = getActiveCollection() ?: run {
                logger.warning("MongoDB not connected, skipping find for [$name]")
                return
            }

            val filter = createWildcardFilter(topicName)
            val startTime = System.currentTimeMillis()
            var count = 0

            activeCollection.find(filter).iterator().use { cursor ->
                while (cursor.hasNext()) {
                    val document = cursor.next()
                count++
                val topic = document.getString("topic") ?: ""
                val payload = when (val p = document.get("payload")) {
                    is Binary -> p.data
                    is ByteArray -> p
                    else -> ByteArray(0)
                }

                val message = BrokerMessage(
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
            logger.fine { "Found $count messages in ${duration}ms for pattern [$topicName]" }

        } catch (e: Exception) {
            logger.warning("Error finding messages for pattern [$topicName]: ${e.message}")
        }
    }

    override fun findMatchingTopics(topicPattern: String, callback: (String) -> Boolean) {
        try {
            val activeCollection = getActiveCollection() ?: run {
                logger.warning("MongoDB not connected, skipping find topics for [$name]")
                return
            }

            // For exact topic match (no wildcards), use existence check (like LIMIT 1)
            if (!topicPattern.contains("+") && !topicPattern.contains("#")) {
                val filter = Filters.eq("topic", topicPattern)
                val exists = activeCollection.find(filter).limit(1).iterator().use { it.hasNext() }
                if (exists) {
                    callback(topicPattern)
                }
                return
            }

            // For wildcard patterns, use aggregation pipeline to extract topic names efficiently
            val levels = Utils.getTopicLevels(topicPattern)

            // Handle pattern like 'a/+' - find topics like 'a/b' even if only 'a/b/c' exists
            val extractDepth = if (topicPattern.endsWith("#")) {
                // Multi-level wildcard - extract at the # level and deeper
                levels.size - 1
            } else {
                // Single level or exact match - extract at pattern depth
                levels.size
            }

            val matchStage = Document("\$match", createWildcardFilter(topicPattern))

            // Use aggregation to extract topic prefixes at the desired depth
            val projectStage = Document("\$project", Document().apply {
                put("_id", 0)

                // Create array of topic levels for projection
                val topicLevelsArray = Document("\$slice", listOf("\$topic_levels.L", extractDepth))
                put("extracted_levels", topicLevelsArray)

                // Convert array back to topic string
                val topicString = Document("\$reduce", Document().apply {
                    put("input", "\$extracted_levels")
                    put("initialValue", "")
                    put("in", Document("\$concat", listOf(
                        "\$\$value",
                        Document("\$cond", listOf(
                            Document("\$eq", listOf("\$\$value", "")),
                            "",
                            "/"
                        )),
                        "\$\$this"
                    )))
                })
                put("extracted_topic", topicString)
            })

            // Group to get distinct topic names
            val groupStage = Document("\$group", Document().apply {
                put("_id", "\$extracted_topic")
            })

            // Filter out empty topics
            val filterStage = Document("\$match", Document("_id", Document("\$ne", "")))

            val pipeline = listOf(matchStage, projectStage, groupStage, filterStage)

            logger.fine { "MongoDB aggregation pipeline for findMatchingTopics: $pipeline" }

            val startTime = System.currentTimeMillis()
            var count = 0

            activeCollection.aggregate(pipeline).iterator().use { cursor ->
                while (cursor.hasNext()) {
                    val document = cursor.next()
                    count++
                    val topic = document.getString("_id")
                    if (topic != null && topic.isNotEmpty()) {
                        if (!callback(topic)) {
                            break // Stop if callback returns false
                        }
                    }
                }
            }

            val duration = System.currentTimeMillis() - startTime
            logger.fine { "Found $count distinct topics in ${duration}ms for pattern [$topicPattern]" }

        } catch (e: Exception) {
            logger.warning("Error finding topics for pattern [$topicPattern]: ${e.message}")
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

        val activeCollection = getActiveCollection() ?: run {
            logger.warning("MongoDB not connected, returning empty topics list for [$name]")
            return emptyList()
        }

        return activeCollection.find(filter)
            .projection(Projections.include("topic"))
            .map { it.getString("topic") }
            .into(mutableListOf())
    }

    override fun findTopicsByConfig(config: String, description: String, ignoreCase: Boolean, namespace: String): List<Pair<String, String>> {
        val resultTopics = mutableListOf<Pair<String, String>>()

        try {
            val activeCollection = getActiveCollection() ?: run {
                logger.warning("MongoDB not connected, returning empty list")
                return emptyList()
            }

            // Build filters
            val filters = mutableListOf<Bson>()

            // Filter for topics ending with MCP_CONFIG_TOPIC
            val topicLevels = Utils.getTopicLevels(Const.MCP_CONFIG_TOPIC)
            if (topicLevels.isNotEmpty()) {
                val lastLevel = topicLevels.last()
                filters.add(Filters.regex("topic", ".*/${lastLevel}$"))
            }

            // Namespace filter
            if (namespace.isNotEmpty()) {
                if (ignoreCase) {
                    filters.add(Filters.regex("topic", "^$namespace/.*", "i"))
                } else {
                    filters.add(Filters.regex("topic", "^$namespace/.*"))
                }
            }

            // Config field contains description pattern
            if (description.isNotEmpty()) {
                val regexPattern = if (ignoreCase) {
                    Filters.regex("payload_json.$config", ".*$description.*", "i")
                } else {
                    Filters.regex("payload_json.$config", ".*$description.*")
                }
                filters.add(regexPattern)
            }

            val query = if (filters.isNotEmpty()) Filters.and(filters) else Document()

            logger.fine { "findTopicsByConfig query: $query [${Utils.getCurrentFunctionName()}]" }

            val cursor = activeCollection.find(query).sort(Document("topic", 1))

            for (doc in cursor) {
                val topic = doc.getString("topic") ?: ""
                // Remove the config topic suffix
                val cleanTopic = topic.replace("/${Const.MCP_CONFIG_TOPIC}", "")
                val configJson = doc.getString("payload_json") ?: ""
                resultTopics.add(Pair(cleanTopic, configJson))
            }
        } catch (e: Exception) {
            logger.severe("Error finding topics by config in MongoDB: ${e.message}")
        }

        logger.fine { "findTopicsByConfig result: ${resultTopics.size} topics found [${Utils.getCurrentFunctionName()}]" }
        return resultTopics
    }

    override fun getName(): String = name
    override fun getType(): MessageStoreType = MessageStoreType.MONGODB

    override fun purgeOldMessages(olderThan: Instant): PurgeResult {
        val startTime = System.currentTimeMillis()
        var deletedCount = 0
        
        logger.fine { "Starting purge for [$name] - removing messages older than $olderThan" }
        
        try {
            val filter = Filters.lt("time", Instant.ofEpochMilli(olderThan.toEpochMilli()))
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
            logger.info("Dropped collection [$collectionName] for message store [$name]")
            true
        } catch (e: Exception) {
            logger.severe("Error dropping collection [$collectionName] for message store [$name]: ${e.message}")
            false
        }
    }

    /**
     * Ensures connection is available, returns null if not connected
     */
    private fun getActiveCollection(): MongoCollection<Document>? {
        return if (isConnected) collection else null
    }

    override fun getConnectionStatus(): Boolean {
        // Just return the cached connection status - no real-time testing
        // Real-time testing is done by background health checks
        return isConnected && mongoClient != null && database != null && collection != null
    }

    override fun stop() {
        mongoClient?.close()
        logger.info("MongoDB connection closed.")
    }
}