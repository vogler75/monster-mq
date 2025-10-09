package at.rocworks.stores.sqlite

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.IMessageStoreExtended
import at.rocworks.stores.MessageStoreType
import at.rocworks.data.PurgeResult
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant

/**
 * Clean SQLiteVerticle-only implementation of MessageStore
 * No SharedSQLiteConnection - uses only event bus communication
 */
class MessageStoreSQLite(
    private val name: String,
    private val dbPath: String
): AbstractVerticle(), IMessageStoreExtended {
    private val logger = Utils.getLogger(this::class.java, name)
    private lateinit var sqlClient: SQLiteClient
    private val tableName = name.lowercase()

    private companion object {
        const val MAX_FIXED_TOPIC_LEVELS = 9
        val FIXED_TOPIC_COLUMN_NAMES = (0 until MAX_FIXED_TOPIC_LEVELS).map { "topic_${it+1}" }

        fun splitTopic(topicName: String): Triple<List<String>, List<String>, String> {
            val levels = Utils.getTopicLevels(topicName)
            val first = levels.take(MAX_FIXED_TOPIC_LEVELS)
            val rest = levels.drop(MAX_FIXED_TOPIC_LEVELS)
            val last = if (levels.isNotEmpty()) levels.last() else ""
            return Triple(first, rest, last)
        }
    }

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun getName(): String = name
    override fun getType(): MessageStoreType = MessageStoreType.SQLITE

    override fun start(startPromise: Promise<Void>) {
        // Initialize SQLiteClient - assumes SQLiteVerticle is already deployed by Monster.kt
        sqlClient = SQLiteClient(vertx, dbPath)
        
        // Create tables using SQLiteClient
        val fixedTopicColumns = FIXED_TOPIC_COLUMN_NAMES.joinToString(", ") { "$it TEXT NOT NULL DEFAULT ''" }
        val createTableSQL = JsonArray()
            .add("""
            CREATE TABLE IF NOT EXISTS $tableName (
                topic TEXT PRIMARY KEY, -- full topic                   
                ${fixedTopicColumns}, -- topic levels for wildcard matching                        
                topic_r TEXT, -- remaining topic levels as JSON
                topic_l TEXT NOT NULL, -- last level of the topic
                time TEXT,                    
                payload_blob BLOB,
                payload_json TEXT,
                qos INTEGER,
                retained BOOLEAN,
                client_id TEXT, 
                message_uuid TEXT
            )
            """.trimIndent())
            .add("""
            CREATE INDEX IF NOT EXISTS ${tableName}_topic ON $tableName (${(FIXED_TOPIC_COLUMN_NAMES + "topic_r").joinToString(", ")})
            """.trimIndent())

        sqlClient.initDatabase(createTableSQL).onComplete { result ->
            if (result.succeeded()) {
                logger.info("SQLite Message store [$name] is ready [start]")
                startPromise.complete()
            } else {
                logger.severe("Failed to initialize SQLite message store: ${result.cause()?.message}")
                startPromise.fail(result.cause())
            }
        }
    }

    override fun get(topicName: String): BrokerMessage? {
        val sql = """SELECT payload_blob, qos, retained, client_id, message_uuid FROM $tableName 
                     WHERE topic = ?"""
        val params = JsonArray().add(topicName)
        
        return try {
            val results = sqlClient.executeQuerySync(sql, params)
            if (results.size() > 0) {
                val row = results.getJsonObject(0)
                val payload = row.getBinary("payload_blob") ?: ByteArray(0)
                val qos = row.getInteger("qos", 0)
                val retained = row.getBoolean("retained", false)
                val clientId = row.getString("client_id") ?: ""
                val messageUuid = row.getString("message_uuid") ?: ""

                BrokerMessage(
                    messageUuid = messageUuid,
                    messageId = 0,
                    topicName = topicName,
                    payload = payload,
                    qosLevel = qos,
                    isRetain = retained,
                    isQueued = false,
                    clientId = clientId,
                    isDup = false
                )
            } else {
                null
            }
        } catch (e: Exception) {
            logger.severe("Error fetching data for topic [$topicName]: ${e.message} [get]")
            null
        }
    }

    /**
     * Async version of get() for better performance with GraphQL queries
     */
    override fun getAsync(topicName: String, callback: (BrokerMessage?) -> Unit) {
        val sql = """SELECT payload_blob, qos, retained, client_id, message_uuid FROM $tableName 
                     WHERE topic = ?"""
        val params = JsonArray().add(topicName)
        
        sqlClient.executeQuery(sql, params).onComplete { result ->
            if (result.succeeded()) {
                val results = result.result()
                if (results.size() > 0) {
                    try {
                        val row = results.getJsonObject(0)
                        val payload = row.getBinary("payload_blob") ?: ByteArray(0)
                        val qos = row.getInteger("qos", 0)
                        val retained = row.getBoolean("retained", false)
                        val clientId = row.getString("client_id") ?: ""
                        val messageUuid = row.getString("message_uuid") ?: ""

                        val message = BrokerMessage(
                            messageUuid = messageUuid,
                            messageId = 0,
                            topicName = topicName,
                            payload = payload,
                            qosLevel = qos,
                            isRetain = retained,
                            isQueued = false,
                            clientId = clientId,
                            isDup = false
                        )
                        callback(message)
                    } catch (e: Exception) {
                        logger.severe("Error parsing SQLite result for topic [$topicName]: ${e.message}")
                        callback(null)
                    }
                } else {
                    callback(null)
                }
            } else {
                logger.severe("Error fetching data for topic [$topicName]: ${result.cause()?.message} [getAsync]")
                callback(null)
            }
        }
    }

    override fun addAll(messages: List<BrokerMessage>) {
        if (messages.isEmpty()) return
        
        val fixedColumns = FIXED_TOPIC_COLUMN_NAMES.joinToString(", ")
        val placeholders = FIXED_TOPIC_COLUMN_NAMES.joinToString(", ") { "?" }
        val sql = """INSERT INTO $tableName (topic, $fixedColumns, topic_r, topic_l,
                   time, payload_blob, payload_json, qos, retained, client_id, message_uuid) 
                   VALUES (?, $placeholders, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
                   ON CONFLICT (topic) DO UPDATE 
                   SET time = excluded.time, 
                   payload_blob = excluded.payload_blob, 
                   payload_json = excluded.payload_json,
                   qos = excluded.qos, 
                   retained = excluded.retained, 
                   client_id = excluded.client_id, 
                   message_uuid = excluded.message_uuid 
                   """

        val batchParams = JsonArray()
        messages.forEach { message ->
            val (first, rest, last) = splitTopic(message.topicName)
            val params = JsonArray().add(message.topicName)
            
            // Add fixed topic columns
            repeat(MAX_FIXED_TOPIC_LEVELS) { 
                params.add(first.getOrNull(it) ?: "") 
            }
            
            // Store remaining levels as JSON string
            val restJson = if (rest.isNotEmpty()) {
                "[\"${rest.joinToString("\",\"")}\"]"
            } else {
                "[]"
            }
            params.add(restJson)
            params.add(last)
            params.add(message.time.toString())
            params.add(message.payload)
            params.add(message.getPayloadAsJson())
            params.add(message.qosLevel)
            params.add(message.isRetain)
            params.add(message.clientId)
            params.add(message.messageUuid)
            
            batchParams.add(params)
        }

        sqlClient.executeBatch(sql, batchParams).onComplete { result ->
            if (result.failed()) {
                logger.severe("Error inserting batch data: ${result.cause()?.message}")
            } else {
                logger.fine { "Added ${messages.size} messages to store" }
            }
        }
    }

    override fun delAll(topics: List<String>) {
        if (topics.isEmpty()) return
        
        val sql = "DELETE FROM $tableName WHERE topic = ?"
        val batchParams = JsonArray()
        topics.forEach { topic ->
            batchParams.add(JsonArray().add(topic))
        }
        
        sqlClient.executeBatch(sql, batchParams).onComplete { result ->
            if (result.failed()) {
                logger.severe("Error deleting batch data: ${result.cause()?.message}")
            } else {
                logger.fine { "Deleted ${topics.size} topics from store" }
            }
        }
    }

    override fun findMatchingMessages(topicName: String, callback: (BrokerMessage) -> Boolean) {
        val levels = Utils.getTopicLevels(topicName)
        val filter = levels.mapIndexed { index, level ->
            when (level) {
                "+", "#" -> null
                else -> {
                    if (index >= MAX_FIXED_TOPIC_LEVELS) {
                        // For SQLite, we need to use JSON functions to extract array elements
                        val jsonIndex = index - MAX_FIXED_TOPIC_LEVELS
                        Pair("json_extract(topic_r, '$[$jsonIndex]') = ?", level)
                    } else {
                        Pair(FIXED_TOPIC_COLUMN_NAMES[index] + " = ?", level)
                    }
                }
            }
        }.filterNotNull()

        val where = filter.joinToString(" AND ") { it.first }.ifEmpty { "1=1" } +
                (if (topicName.endsWith("#")) ""
                else {
                    if (levels.size < MAX_FIXED_TOPIC_LEVELS) {
                        " AND " + FIXED_TOPIC_COLUMN_NAMES[levels.size] + " = ''"
                    } else {
                        // For SQLite, check JSON array length
                        " AND json_array_length(topic_r) = " + (levels.size - MAX_FIXED_TOPIC_LEVELS)
                    }
                })
        val sql = "SELECT topic, payload_blob, qos, client_id, message_uuid " +
                  "FROM $tableName WHERE $where"

        val params = JsonArray()
        filter.forEach { params.add(it.second) }

        logger.fine { "SQL: $sql [findMatchingMessages]" }

        sqlClient.executeQuery(sql, params).onComplete { result ->
            if (result.succeeded()) {
                val results = result.result()
                results.forEach { row ->
                    val rowObj = row as JsonObject
                    val topic = rowObj.getString("topic")
                    val payload = rowObj.getBinary("payload_blob") ?: ByteArray(0)
                    val qos = rowObj.getInteger("qos", 0)
                    val clientId = rowObj.getString("client_id", "")
                    val messageUuid = rowObj.getString("message_uuid", "")
                    val message = BrokerMessage(
                        messageUuid = messageUuid,
                        messageId = 0,
                        topicName = topic,
                        payload = payload,
                        qosLevel = qos,
                        isRetain = true,
                        isDup = false,
                        isQueued = false,
                        clientId = clientId
                    )
                    callback(message)
                }
            } else {
                logger.severe("Error finding data for topic [$topicName]: ${result.cause()?.message}")
            }
        }
    }

    override fun findMatchingTopics(topicPattern: String, callback: (String) -> Boolean) {
        val levels = Utils.getTopicLevels(topicPattern)

        // For exact topic match (no wildcards), use simple existence check
        if (!topicPattern.contains("+") && !topicPattern.contains("#")) {
            val sql = "SELECT 1 FROM $tableName WHERE topic = ? LIMIT 1"
            val params = JsonArray().add(topicPattern)

            sqlClient.executeQuery(sql, params).onComplete { result ->
                if (result.succeeded()) {
                    val results = result.result()
                    if (results.size() > 0) {
                        callback(topicPattern)
                    }
                }
            }
            return
        }

        // Handle pattern like 'a/+' - find distinct topic combinations at the desired depth
        val extractDepth = if (topicPattern.endsWith("#")) {
            levels.size - 1 // Multi-level wildcard - extract at the # level and deeper
        } else {
            levels.size // Single level or exact match - extract at pattern depth
        }

        // Build filter for wildcard matching
        val filter = levels.mapIndexed { index, level ->
            when (level) {
                "+", "#" -> null
                else -> {
                    if (index >= MAX_FIXED_TOPIC_LEVELS) {
                        val jsonIndex = index - MAX_FIXED_TOPIC_LEVELS
                        Pair("json_extract(topic_r, '$[$jsonIndex]') = ?", level)
                    } else {
                        Pair(FIXED_TOPIC_COLUMN_NAMES[index] + " = ?", level)
                    }
                }
            }
        }.filterNotNull()

        val where = filter.joinToString(" AND ") { it.first }.ifEmpty { "1=1" } +
                (if (topicPattern.endsWith("#")) ""
                else {
                    if (levels.size < MAX_FIXED_TOPIC_LEVELS) {
                        " AND " + FIXED_TOPIC_COLUMN_NAMES[levels.size] + " = ''"
                    } else {
                        " AND json_array_length(topic_r) = " + (levels.size - MAX_FIXED_TOPIC_LEVELS)
                    }
                })

        // Use DISTINCT with LIMIT approach to find topic combinations efficiently
        val sql = buildString {
            append("SELECT DISTINCT ")

            if (extractDepth <= MAX_FIXED_TOPIC_LEVELS) {
                // All levels are in fixed columns - select those columns
                append((0 until extractDepth).map { FIXED_TOPIC_COLUMN_NAMES[it] }.joinToString(", "))
            } else {
                // Mix of fixed columns and JSON array
                append(FIXED_TOPIC_COLUMN_NAMES.joinToString(", "))
                append(", topic_r")
            }

            append(" FROM $tableName WHERE $where")
            append(" AND (")

            // Ensure the topic levels are not empty at the target depth
            if (extractDepth <= MAX_FIXED_TOPIC_LEVELS) {
                append(FIXED_TOPIC_COLUMN_NAMES[extractDepth - 1] + " != ''")
            } else {
                append("json_array_length(topic_r) >= " + (extractDepth - MAX_FIXED_TOPIC_LEVELS))
            }
            append(")")
        }

        val params = JsonArray()
        filter.forEach { params.add(it.second) }

        logger.fine { "Optimized SQL for findMatchingTopics: $sql" }

        sqlClient.executeQuery(sql, params).onComplete { result ->
            if (result.succeeded()) {
                val results = result.result()
                results.forEach { row ->
                    val rowObj = row as JsonObject

                    // Reconstruct topic name from the selected columns
                    val topicParts = mutableListOf<String>()

                    if (extractDepth <= MAX_FIXED_TOPIC_LEVELS) {
                        // All from fixed columns
                        for (i in 0 until extractDepth) {
                            val part = rowObj.getString(FIXED_TOPIC_COLUMN_NAMES[i])
                            if (part != null && part.isNotEmpty()) {
                                topicParts.add(part)
                            }
                        }
                    } else {
                        // Fixed columns + JSON array
                        for (i in 0 until MAX_FIXED_TOPIC_LEVELS) {
                            val part = rowObj.getString(FIXED_TOPIC_COLUMN_NAMES[i])
                            if (part != null && part.isNotEmpty()) {
                                topicParts.add(part)
                            }
                        }

                        // Add from JSON array
                        val topicRArray = rowObj.getJsonArray("topic_r")
                        if (topicRArray != null) {
                            val remainingLevels = extractDepth - MAX_FIXED_TOPIC_LEVELS
                            for (i in 0 until minOf(remainingLevels, topicRArray.size())) {
                                val part = topicRArray.getString(i)
                                if (part != null && part.isNotEmpty()) {
                                    topicParts.add(part)
                                }
                            }
                        }
                    }

                    if (topicParts.isNotEmpty()) {
                        val topic = topicParts.joinToString("/")
                        if (!callback(topic)) {
                            return@forEach // Stop if callback returns false
                        }
                    }
                }
            } else {
                logger.severe("Error finding topics for pattern [$topicPattern]: ${result.cause()?.message}")
            }
        }
    }

    override fun findTopicsByName(name: String, ignoreCase: Boolean, namespace: String): List<String> {
        val resultTopics = mutableListOf<String>()
        val sqlSearchPattern = name.replace("*", "%").replace("+", "_")
        val sqlNamespacePattern = if (namespace.isEmpty()) "%" else "$namespace/%"

        val sql = """
        SELECT topic
        FROM $tableName AS t 
        WHERE topic_l != '${Const.MCP_CONFIG_TOPIC}'
        AND ${if (ignoreCase) "LOWER(topic)" else "topic"} LIKE ${if (ignoreCase) "LOWER(?)" else "?"}        
        AND ${if (ignoreCase) "LOWER(topic)" else "topic"} LIKE ${if (ignoreCase) "LOWER(?)" else "?"} 
        ORDER BY topic
        """.trimIndent()

        val params = JsonArray().add(sqlNamespacePattern).add(sqlSearchPattern)
        
        val results = sqlClient.executeQuerySync(sql, params)
        results.forEach { row ->
            val rowObj = row as JsonObject
            val fullTopic = rowObj.getString("topic") ?: ""
            resultTopics.add(fullTopic)
        }

        logger.fine { "findTopicsByName result: ${resultTopics.size} topics found" }
        return resultTopics
    }

    override fun findTopicsByConfig(config: String, description: String, ignoreCase: Boolean, namespace: String): List<Pair<String, String>> {
        val resultTopics = mutableListOf<Pair<String, String>>()
        val sqlSearchPattern = description
        val sqlNamespacePattern = if (namespace.isEmpty()) "%" else "$namespace/%"

        // SQLite uses different JSON syntax than PostgreSQL
        val sql = """
        SELECT REPLACE(topic, '/${Const.MCP_CONFIG_TOPIC}', '') AS topic, payload_json AS config
        FROM $tableName
        WHERE topic_l = '${Const.MCP_CONFIG_TOPIC}' 
        AND ${if (ignoreCase) "LOWER(topic)" else "topic"} LIKE ${if (ignoreCase) "LOWER(?)" else "?"}
        AND ${if (ignoreCase) "LOWER(json_extract(payload_json, '$.$config'))" else "json_extract(payload_json, '$.$config')"} LIKE ${if (ignoreCase) "LOWER(?)" else "?"}
        ORDER BY topic
        """.trimIndent()

        val params = JsonArray().add(sqlNamespacePattern).add("%$sqlSearchPattern%")
        
        val results = sqlClient.executeQuerySync(sql, params)
        results.forEach { row ->
            val rowObj = row as JsonObject
            val fullTopic = rowObj.getString("topic") ?: ""
            val configJson = rowObj.getString("config") ?: ""
            resultTopics.add(Pair(fullTopic, configJson))
        }

        logger.fine { "findTopicsByConfig result: ${resultTopics.size} topics found" }
        return resultTopics
    }

    override fun purgeOldMessages(olderThan: Instant): PurgeResult {
        val startTime = System.currentTimeMillis()
        
        logger.fine { "Starting purge for [$name] - removing messages older than $olderThan" }
        
        val sql = "DELETE FROM $tableName WHERE time < ?"
        val params = JsonArray().add(olderThan.toString())
        
        return try {
            val future = sqlClient.executeUpdate(sql, params)
            val deletedCount = future.toCompletionStage().toCompletableFuture().get(5000, java.util.concurrent.TimeUnit.MILLISECONDS)
            
            val elapsedTimeMs = System.currentTimeMillis() - startTime
            val purgeResult = PurgeResult(deletedCount, elapsedTimeMs)
            
            logger.fine { "Purge completed for [$name]: deleted ${purgeResult.deletedCount} messages in ${purgeResult.elapsedTimeMs}ms" }
            purgeResult
        } catch (e: Exception) {
            logger.severe("Error purging old messages from [$name]: ${e.message}")
            val elapsedTimeMs = System.currentTimeMillis() - startTime
            PurgeResult(0, elapsedTimeMs)
        }
    }

    override fun dropStorage(): Boolean {
        return try {
            val sql = "DROP TABLE IF EXISTS $tableName"
            sqlClient.executeUpdate(sql).toCompletionStage().toCompletableFuture().get(5000, java.util.concurrent.TimeUnit.MILLISECONDS)
            logger.info("Dropped table [$tableName] for message store [$name]")
            true
        } catch (e: Exception) {
            logger.severe("Error dropping table [$tableName] for message store [$name]: ${e.message}")
            false
        }
    }

    override fun getConnectionStatus(): Boolean {
        return try {
            ::sqlClient.isInitialized
        } catch (e: Exception) {
            false
        }
    }
}