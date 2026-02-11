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
 * - Optimized wildcard topic matching using fixed topic level fields (like PostgreSQL)
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

    private companion object {
        const val MAX_FIXED_TOPIC_LEVELS = 9
        val FIXED_TOPIC_COLUMN_NAMES = (0 until MAX_FIXED_TOPIC_LEVELS).map { "topic_${it+1}" }

        fun splitTopic(topicName: String): Triple<List<String>, List<String>, String> {
            val levels = Utils.getTopicLevels(topicName)
            val first = levels.take(MAX_FIXED_TOPIC_LEVELS)
            val rest = levels.drop(MAX_FIXED_TOPIC_LEVELS)
            val last = levels.lastOrNull() ?: ""
            return Triple(first, rest, last)
        }
    }

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

            // Compound index for fixed topic levels (optimized wildcard matching)
            // Similar to PostgreSQL approach with indexed topic_1..topic_9 fields
            val topicIndexFields = mutableMapOf<String, Int>()
            FIXED_TOPIC_COLUMN_NAMES.forEach { topicIndexFields[it] = 1 }
            targetCollection.createIndex(
                Document(topicIndexFields),
                IndexOptions().name("topic_levels_idx")
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

    /**
     * Create document fields for storing topic levels in fixed columns (like PostgreSQL)
     * Returns a map that should be merged into the main document
     */
    private fun getTopicLevelFields(topicName: String): Map<String, Any?> {
        val (first, rest, last) = splitTopic(topicName)
        val fields = mutableMapOf<String, Any?>()

        // Store fixed topic levels in topic_1, topic_2, ... topic_9 fields
        FIXED_TOPIC_COLUMN_NAMES.forEachIndexed { index, fieldName ->
            fields[fieldName] = first.getOrNull(index) ?: ""
        }

        // Store remaining levels in array
        fields["topic_r"] = if (rest.isNotEmpty()) rest else listOf<String>()

        // Store last level
        fields["topic_l"] = last

        return fields
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
                val updateFieldsMap: MutableMap<String, Any?> = mutableMapOf(
                    "topic" to message.topicName,  // Explicitly set topic field
                    "time" to Instant.ofEpochMilli(message.time.toEpochMilli()),
                    "payload" to Binary(message.payload),
                    "payload_json" to message.getPayloadAsJson(),
                    "qos" to message.qosLevel,
                    "retained" to message.isRetain,
                    "client_id" to message.clientId,
                    "message_uuid" to message.messageUuid,
                    "creation_time" to message.time.toEpochMilli(),
                    "message_expiry_interval" to message.messageExpiryInterval
                )
                // Add fixed topic level fields
                updateFieldsMap.putAll(getTopicLevelFields(message.topicName))

                val update = Document("\$set", Document(updateFieldsMap))
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
            val currentTimeMillis = System.currentTimeMillis()
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
                
                val creationTime = document.getLong("creation_time") ?: currentTimeMillis
                val messageExpiryInterval = document.getLong("message_expiry_interval")
                
                // Phase 5: Check if message has expired
                if (messageExpiryInterval != null && messageExpiryInterval >= 0) {
                    val ageSeconds = (currentTimeMillis - creationTime) / 1000
                    if (ageSeconds >= messageExpiryInterval) {
                        continue // Skip expired message
                    }
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
                    clientId = document.getString("client_id") ?: "",
                    time = java.time.Instant.ofEpochMilli(creationTime),
                    messageExpiryInterval = messageExpiryInterval
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
            // Wait for connection with timeout (max 5 seconds for browsing)
            val connectionWaitStart = System.currentTimeMillis()
            val connectionTimeoutMs = 5_000L

            while (!isConnected && (System.currentTimeMillis() - connectionWaitStart) < connectionTimeoutMs) {
                if (collection != null) {
                    // Connection appears ready, break out of wait loop
                    break
                }
                Thread.sleep(50) // Check every 50ms
            }

            val activeCollection = getActiveCollection() ?: run {
                logger.warning("MongoDB not connected after ${System.currentTimeMillis() - connectionWaitStart}ms, skipping find topics for [$name]")
                return
            }

            // For exact topic match (no wildcards), use existence check
            if (!topicPattern.contains("+") && !topicPattern.contains("#")) {
                val filter = Filters.eq("topic", topicPattern)
                val exists = activeCollection.find(filter).limit(1).iterator().use { it.hasNext() }
                if (exists) {
                    callback(topicPattern)
                }
                return
            }

            // For wildcard patterns, fetch matching documents and extract full topics
            val matchFilter = createWildcardFilter(topicPattern)
            val patternLevels = Utils.getTopicLevels(topicPattern)

            val startTime = System.currentTimeMillis()
            var count = 0
            val extractedTopics = mutableSetOf<String>()

            // Debug: log collection diagnostics
            val totalDocs = activeCollection.countDocuments()
            val sample = activeCollection.find().limit(1).first()
            val sampleKeys = sample?.keys ?: emptySet()

            // Count unique topics (should equal totalDocs if collection is correct)
            val allDocs = activeCollection.find().projection(Projections.include("topic")).into(mutableListOf())
            val uniqueTopics = allDocs.mapNotNull { it.getString("topic") }.toSet()

            logger.info { "Collection diagnostic for [$name]:" }
            logger.info { "  Total documents: $totalDocs" }
            logger.info { "  Unique topics: ${uniqueTopics.size}" }
            logger.fine { "  Sample document keys: $sampleKeys" }
            logger.fine { "  Sample document: $sample" }
            logger.fine { "  Filter for pattern [$topicPattern]: $matchFilter" }

            // Fetch documents matching the filter (this is fast with proper indexes)
            var docCount = 0
            activeCollection.find(matchFilter).iterator().use { cursor ->
                while (cursor.hasNext()) {
                    val document = cursor.next()
                    docCount++
                    // The simplest approach: just use the full topic stored in the document
                    val topic = document.getString("topic")
                    if (topic != null) {
                        // Apply the same topic extraction logic as PostgreSQL
                        val browseResult = extractBrowseTopicFromPattern(topic, topicPattern)
                        if (browseResult != null && !extractedTopics.contains(browseResult)) {
                            extractedTopics.add(browseResult)
                            count++
                            val continueProcessing = callback(browseResult)
                            if (!continueProcessing) {
                                break
                            }
                        }
                    }
                }
            }
            logger.fine { "Scanned $docCount documents, extracted $count unique browse topics for pattern [$topicPattern]" }

            val duration = System.currentTimeMillis() - startTime
            logger.fine { "Found $count distinct topics in ${duration}ms for pattern [$topicPattern]" }

        } catch (e: Exception) {
            logger.warning("Error finding topics for pattern [$topicPattern]: ${e.message}")
        }
    }

    /**
     * Extract the topic level to show based on the browse pattern
     * Same logic as in QueryResolver and PostgreSQL implementation
     */
    private fun extractBrowseTopicFromPattern(messageTopic: String, pattern: String): String? {
        val patternLevels = Utils.getTopicLevels(pattern)
        val messageLevels = Utils.getTopicLevels(messageTopic)

        // Find the index of the wildcard in the pattern
        val wildcardIndex = patternLevels.indexOfFirst { it == "+" || it == "#" }
        if (wildcardIndex == -1) {
            // No wildcard, should match exactly
            return if (messageTopic == pattern) messageTopic else null
        }

        // Check if the message topic has enough levels to match the pattern up to wildcard
        if (messageLevels.size < wildcardIndex) {
            return null
        }

        // Check if the prefix matches
        for (i in 0 until wildcardIndex) {
            if (patternLevels[i] != messageLevels[i]) {
                return null
            }
        }

        // For single-level wildcard (+), return the topic up to and including the matched level
        if (patternLevels[wildcardIndex] == "+") {
            if (messageLevels.size > wildcardIndex) {
                return messageLevels.take(wildcardIndex + 1).joinToString("/")
            }
        }

        // For multi-level wildcard (#), return the full topic
        if (patternLevels[wildcardIndex] == "#") {
            return messageTopic
        }

        return null
    }

    /**
     * Create optimized filter for wildcard topic matching using fixed topic level fields
     * Handles backward compatibility with old topic_levels nested structure
     */
    private fun createWildcardFilter(topicPattern: String): Bson {
        if (!topicPattern.contains("+") && !topicPattern.contains("#")) {
            // Exact match - use indexed field
            return Filters.eq("topic", topicPattern)
        }

        val levels = Utils.getTopicLevels(topicPattern)
        val filters = mutableListOf<Bson>()

        // Simple patterns like "+" on root should return all documents
        // and let Kotlin code filter them by browse logic
        // This also works with old documents that don't have topic_N fields
        if (levels.size == 1 && levels[0] == "+") {
            // Return all documents - will be filtered in findMatchingTopics
            return Document()
        }

        levels.forEachIndexed { index, level ->
            when (level) {
                "#" -> {
                    // Multi-level wildcard - match any depth at or beyond current level
                    return@forEachIndexed
                }
                "+" -> {
                    // Single-level wildcard - any value at this position
                    // Don't add a filter - document may have the field or not
                }
                else -> {
                    // Exact level match - try both new and old schema
                    if (index < MAX_FIXED_TOPIC_LEVELS) {
                        filters.add(Filters.eq(FIXED_TOPIC_COLUMN_NAMES[index], level))
                    } else {
                        val arrayIndex = index - MAX_FIXED_TOPIC_LEVELS
                        filters.add(Filters.eq("topic_r.$arrayIndex", level))
                    }
                }
            }
        }

        // If pattern has no wildcards, ensure exact depth match
        // For browse queries with +, don't constrain depth - let Kotlin group by browse level
        if (!topicPattern.contains("+") && !topicPattern.contains("#")) {
            if (levels.size <= MAX_FIXED_TOPIC_LEVELS) {
                // For patterns with 9 or fewer levels, the next fixed field should be empty
                // But this may not work with old documents, so only add if we have other filters
                if (filters.isNotEmpty()) {
                    filters.add(Filters.eq(FIXED_TOPIC_COLUMN_NAMES[levels.size], ""))
                }
            } else {
                val expectedRestCount = levels.size - MAX_FIXED_TOPIC_LEVELS
                if (filters.isNotEmpty()) {
                    filters.add(Filters.eq("topic_r.${expectedRestCount}", null))
                }
            }
        }

        return if (filters.isEmpty()) {
            Document() // Match all - will filter in Kotlin
        } else {
            Filters.and(filters)
        }
    }

    // IMessageStoreExtended implementations
    override fun findTopicsByName(name: String, ignoreCase: Boolean, namespace: String): List<String> {
        // Convert wildcard pattern to regex:
        // * matches any sequence of characters (becomes .* in regex)
        // + matches single character (like MQTT single-level wildcard, becomes . in regex)
        // If no wildcards present, do substring search (more intuitive)

        val hasWildcards = name.contains("*") || name.contains("+")

        // Escape special regex chars except * and +, then replace wildcards
        val escapedName = name
            .replace("\\", "\\\\")
            .replace(".", "\\.")
            .replace("^", "\\^")
            .replace("$", "\\$")
            .replace("?", "\\?")
            .replace("[", "\\[")
            .replace("]", "\\]")
            .replace("{", "\\{")
            .replace("}", "\\}")
            .replace("|", "\\|")
            .replace("(", "\\(")
            .replace(")", "\\)")
            .replace("*", ".*")  // * becomes .* (match any characters)
            .replace("+", ".")   // + becomes . (match single character)

        val regexPattern = buildString {
            if (hasWildcards) {
                // With wildcards: anchor the pattern to full topic path
                append("^")
                if (namespace.isNotEmpty()) {
                    append(Regex.escape(namespace))
                    append("/")
                }
                append(escapedName)
                append("$")
            } else {
                // No wildcards: substring search (more intuitive)
                if (namespace.isNotEmpty()) {
                    append("^")
                    append(Regex.escape(namespace))
                    append("/")
                }
                append(".*")
                append(escapedName)
                append(".*")
            }
        }

        val filter = if (ignoreCase) {
            Filters.regex("topic", regexPattern, "i")
        } else {
            Filters.regex("topic", regexPattern)
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
            val topicLevels = Utils.getTopicLevels(Const.CONFIG_TOPIC)
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
                val cleanTopic = topic.replace("/${Const.CONFIG_TOPIC}", "")
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
            // Wait for connection with timeout (max 30 seconds)
            val startTime = System.currentTimeMillis()
            val timeoutMs = 30_000L

            while (!isConnected && (System.currentTimeMillis() - startTime) < timeoutMs) {
                if (database != null) {
                    // Connection appears ready, break out of wait loop
                    break
                }
                Thread.sleep(100) // Check every 100ms
            }

            if (!isConnected || database == null) {
                logger.warning("MongoDB not connected after ${System.currentTimeMillis() - startTime}ms, cannot create collection for [$collectionName]")
                return false
            }

            // Create collection if not exists
            if (!database!!.listCollectionNames().into(mutableListOf()).contains(collectionName)) {
                database!!.createCollection(collectionName)
                logger.info("Created collection: $collectionName")
            }

            val newCollection = database!!.getCollection(collectionName)

            // Create optimized indexes for queries
            createOptimizedIndexes(newCollection)

            logger.info("Table (collection) created for message store [$name]")
            true
        } catch (e: Exception) {
            logger.warning("Error creating table: ${e.message}")
            false
        }
    }
}