package at.rocworks.stores.cratedb

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.DatabaseConnection
import at.rocworks.stores.IMessageStoreExtended
import at.rocworks.stores.MessageStoreType
import at.rocworks.stores.PayloadFormat
import at.rocworks.data.PurgeResult
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import java.sql.*
import java.time.Instant

class MessageStoreCrateDB(
    private val name: String,
    private val url: String,
    private val username: String,
    private val password: String,
    private val payloadFormat: PayloadFormat = PayloadFormat.DEFAULT
): AbstractVerticle(), IMessageStoreExtended {
    private val logger = Utils.getLogger(this::class.java, name)
    private val tableName = name.lowercase()
    private var lastAddAllError: Int = 0
    private var lastGetError: Int = 0
    private var lastDelAllError: Int = 0
    private var lastFetchError: Int = 0

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    // 1. Update table schema
    private companion object {
        const val MAX_FIXED_TOPIC_LEVELS = 9
        val FIXED_TOPIC_COLUMN_NAMES = (0 until MAX_FIXED_TOPIC_LEVELS).map { "topic_${it+1}" }

        fun splitTopic(topicName: String): Triple<List<String>, List<String>, String> {
            val levels = Utils.getTopicLevels(topicName)
            val first = levels.take(MAX_FIXED_TOPIC_LEVELS)
            val rest = levels.drop(MAX_FIXED_TOPIC_LEVELS)
            val last = levels.last()
            return Triple(first, rest, last)
        }
    }

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun init(connection: Connection): Future<Void> {
            val promise = Promise.promise<Void>()
            try {
                connection.autoCommit = false
                connection.createStatement().use { statement ->
                    val fixedTopicColumns = FIXED_TOPIC_COLUMN_NAMES.joinToString(", ") { "$it VARCHAR" }
                    statement.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS $tableName (
                        topic VARCHAR PRIMARY KEY,
                        $fixedTopicColumns,
                        topic_r VARCHAR[],
                        topic_l VARCHAR,
                        time TIMESTAMPTZ,
                        payload_b64 VARCHAR INDEX OFF,
                        payload_obj OBJECT(DYNAMIC),
                        qos INT,
                        retained BOOLEAN,
                        client_id VARCHAR(65535),
                        message_uuid VARCHAR(36)
                    )
                    """.trimIndent())
                    connection.commit()
                    logger.info("Message store [$name] is ready [${Utils.getCurrentFunctionName()}]")
                    promise.complete()
                }
            } catch (e: Exception) {
                logger.severe("Error in creating table [$name]: ${e.message} [${Utils.getCurrentFunctionName()}]")
                promise.fail(e)
            }
            return promise.future()
        }
    }

    override fun getName(): String = name
    override fun getType(): MessageStoreType = MessageStoreType.CRATEDB

    override fun start(startPromise: Promise<Void>) {
        db.start(vertx, startPromise)
    }

    override fun get(topicName: String): BrokerMessage? {
        try {
            db.connection?.let { connection ->
                val sql = "SELECT payload_b64, qos, retained, client_id, message_uuid FROM $tableName WHERE topic = ?"
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, topicName)

                    val resultSet = preparedStatement.executeQuery()

                    if (resultSet.next()) {
                        val payload = BrokerMessage.getPayloadFromBase64(resultSet.getString(1))
                        val qos = resultSet.getInt(2)
                        val retained = resultSet.getBoolean(3)
                        val clientId = resultSet.getString(4)
                        val messageUuid = resultSet.getString(5)

                        if (lastGetError != 0) {
                            logger.info("Read successful after error [${Utils.getCurrentFunctionName()}]")
                            lastGetError = 0
                        }

                        return BrokerMessage(
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
                    }
                }
            }
        } catch (e: SQLException) {
            if (e.errorCode != lastGetError) { // avoid spamming the logs
                logger.warning("Error fetching data for topic [$topicName]: ${e.message} [${Utils.getCurrentFunctionName()}]")
                lastGetError = e.errorCode
            }
        }
        return null
    }

    override fun getAsync(topicName: String, callback: (BrokerMessage?) -> Unit) {
        // Use Vertx to execute database query asynchronously
        vertx.executeBlocking<BrokerMessage?> {
            get(topicName)
        }.onComplete { result ->
            if (result.succeeded()) {
                callback(result.result())
            } else {
                logger.severe("Error in async get for topic [$topicName]: ${result.cause()?.message}")
                callback(null)
            }
        }
    }

    override fun addAll(messages: List<BrokerMessage>) {
        val fixedColumns = FIXED_TOPIC_COLUMN_NAMES.joinToString(", ")
        val fixedPlaceholders = FIXED_TOPIC_COLUMN_NAMES.joinToString(", ") { "?" }
        val sql = "INSERT INTO $tableName (topic, $fixedColumns, topic_r, topic_l, time, payload_b64, payload_obj, qos, retained, client_id, message_uuid) "+
                "VALUES (?, $fixedPlaceholders, ?::varchar[], ?, ?, ?, ?, ?, ?, ?, ?) "+
                "ON CONFLICT (topic) DO UPDATE "+
                "SET time = EXCLUDED.time, "+
                "payload_b64 = EXCLUDED.payload_b64, "+
                "payload_obj = EXCLUDED.payload_obj, "+
                "qos = EXCLUDED.qos, "+
                "retained = EXCLUDED.retained, "+
                "client_id = EXCLUDED.client_id, "+
                "message_uuid = EXCLUDED.message_uuid "

        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    val failedIndexes = mutableListOf<Int>()
                    var successCount = 0

                    messages.forEachIndexed { index, message ->
                        try {
                            val (first, rest, last) = splitTopic(message.topicName)
                            preparedStatement.setString(1, message.topicName)
                            repeat(MAX_FIXED_TOPIC_LEVELS) { preparedStatement.setString(it + 2, first.getOrNull(it) ?: "") }

                            // Handle array - CrateDB may have issues with createArrayOf
                            try {
                                preparedStatement.setArray(MAX_FIXED_TOPIC_LEVELS + 2, connection.createArrayOf("varchar", rest.toTypedArray()))
                            } catch (e: Exception) {
                                logger.finer("Array creation failed for topic [${message.topicName}], using null: ${e.message}")
                                preparedStatement.setArray(MAX_FIXED_TOPIC_LEVELS + 2, null)
                            }

                            preparedStatement.setString(MAX_FIXED_TOPIC_LEVELS + 3, last)
                            preparedStatement.setTimestamp(MAX_FIXED_TOPIC_LEVELS + 4, Timestamp.from(message.time))

                            // Handle payload based on configured format
                            if (payloadFormat == PayloadFormat.JSON) {
                                val payloadJson = message.getPayloadAsJson()
                                if (payloadJson != null) {
                                    // JSON format configured and payload is valid JSON
                                    preparedStatement.setNull(MAX_FIXED_TOPIC_LEVELS + 5, Types.VARCHAR) // payload_b64 = NULL
                                    preparedStatement.setString(MAX_FIXED_TOPIC_LEVELS + 6, payloadJson)  // payload_obj = JSON
                                } else {
                                    // JSON format configured but payload is not valid JSON - store as base64
                                    preparedStatement.setString(MAX_FIXED_TOPIC_LEVELS + 5, message.getPayloadAsBase64())
                                    preparedStatement.setNull(MAX_FIXED_TOPIC_LEVELS + 6, Types.VARCHAR) // payload_obj = NULL
                                }
                            } else {
                                // DEFAULT format - store only as base64
                                preparedStatement.setString(MAX_FIXED_TOPIC_LEVELS + 5, message.getPayloadAsBase64())
                                preparedStatement.setNull(MAX_FIXED_TOPIC_LEVELS + 6, Types.VARCHAR) // payload_obj = NULL
                            }

                            preparedStatement.setInt(MAX_FIXED_TOPIC_LEVELS + 7, message.qosLevel)
                            preparedStatement.setBoolean(MAX_FIXED_TOPIC_LEVELS + 8, message.isRetain)
                            preparedStatement.setString(MAX_FIXED_TOPIC_LEVELS + 9, message.clientId)
                            preparedStatement.setString(MAX_FIXED_TOPIC_LEVELS + 10, message.messageUuid)
                            preparedStatement.addBatch()
                        } catch (e: Exception) {
                            logger.fine("Error preparing batch statement for message at index $index [${e.message}]")
                            failedIndexes.add(index)
                        }
                    }

                    // Execute batch and check results
                    try {
                        val updateCounts = preparedStatement.executeBatch()
                        updateCounts.forEachIndexed { index, count ->
                            if (count == Statement.EXECUTE_FAILED) {
                                failedIndexes.add(index)
                                logger.fine("Batch statement at index $index failed to execute")
                            } else {
                                successCount++
                            }
                        }
                    } catch (e: BatchUpdateException) {
                        val updateCounts = e.updateCounts
                        updateCounts.forEachIndexed { index, count ->
                            if (count == Statement.EXECUTE_FAILED) {
                                failedIndexes.add(index)
                            } else {
                                successCount++
                            }
                        }
                        logger.warning("Batch update exception with ${failedIndexes.size} failed statements: ${e.message} [${Utils.getCurrentFunctionName()}]")
                    }

                    connection.commit()

                    if (failedIndexes.isNotEmpty()) {
                        logger.warning("Batch insert: ${messages.size} total, $successCount succeeded, ${failedIndexes.size} failed [${Utils.getCurrentFunctionName()}]")
                        lastAddAllError = 1 // Mark that we had an error
                    } else {
                        if (lastAddAllError != 0) {
                            logger.info("Batch insert successful after error [${Utils.getCurrentFunctionName()}]")
                            lastAddAllError = 0
                        }
                    }
                }
            }
        } catch (e: SQLException) {
            if (e.errorCode != lastAddAllError) {
                logger.warning("Error inserting batch data [${e.errorCode}] [${e.message}] [${Utils.getCurrentFunctionName()}]")
                lastAddAllError = e.errorCode
            }
        }
    }


    override fun delAll(topics: List<String>) {
        val sql = "DELETE FROM $tableName WHERE topic = ? " // TODO: can be converted to use IN operator with a list of topics
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    topics.forEach{ topic ->
                        preparedStatement.setString(1, topic)
                        preparedStatement.addBatch()
                    }
                    preparedStatement.executeBatch()
                    connection.commit()
                    if (lastAddAllError != 0) {
                        logger.info("Batch delete successful after error [${Utils.getCurrentFunctionName()}]")
                        lastAddAllError = 0
                    }
                }
            }
        } catch (e: SQLException) {
            if (e.errorCode != lastDelAllError) { // avoid spamming the logs
                logger.warning("Error deleting batch data [${e.message}] [${Utils.getCurrentFunctionName()}]")
                lastDelAllError = e.errorCode
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
                        Pair("topic_r[${index - MAX_FIXED_TOPIC_LEVELS + 1}] = ?", level)
                    } else {
                        Pair(FIXED_TOPIC_COLUMN_NAMES[index] + " = ?", level)
                    }
                }
            }
        }.filterNotNull()
        try {
            db.connection?.let { connection ->
                val where = filter.joinToString(" AND ") { it.first }.ifEmpty { "1=1" } +
                        (if (topicName.endsWith("#")) ""
                         else {
                            if (levels.size < MAX_FIXED_TOPIC_LEVELS) {
                                " AND " + FIXED_TOPIC_COLUMN_NAMES[levels.size] + " = ''"
                            } else {
                                " AND COALESCE(ARRAY_LENGTH(topic_r, 1),0) = " + (levels.size - MAX_FIXED_TOPIC_LEVELS)
                            }
                        })
                val sql = "SELECT topic, payload_b64, qos, client_id, message_uuid " +
                          "FROM $tableName WHERE $where "
                logger.fine { "SQL: $sql [${Utils.getCurrentFunctionName()}]" }
                connection.prepareStatement(sql).use { preparedStatement ->
                    filter.forEachIndexed { i, x -> preparedStatement.setString(i + 1, x.second) }
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        val topic = resultSet.getString(1)
                        val payload = BrokerMessage.getPayloadFromBase64(resultSet.getString(2))
                        val qos = resultSet.getInt(3)
                        val clientId = resultSet.getString(4)
                        val messageUuid = resultSet.getString(5)
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
                }
                if (lastFetchError != 0) {
                    logger.info("Read successful after error [${Utils.getCurrentFunctionName()}]")
                    lastFetchError = 0
                }
            }
        } catch (e: SQLException) {
            if (e.errorCode != lastFetchError) {
                logger.warning("Error finding data for topic [$topicName]: ${e.message} [${Utils.getCurrentFunctionName()}]")
                lastFetchError = e.errorCode
            }
        }
    }

    override fun findMatchingTopics(topicPattern: String, callback: (String) -> Boolean) {
        // Use SQL-level optimization to extract only topic names without loading message content
        val levels = Utils.getTopicLevels(topicPattern)

        // For exact topic match (no wildcards), use simple existence check with LIMIT 1
        if (!topicPattern.contains("+") && !topicPattern.contains("#")) {
            try {
                db.connection?.let { connection ->
                    val sql = "SELECT 1 FROM $tableName WHERE topic = ? LIMIT 1"
                    connection.prepareStatement(sql).use { preparedStatement ->
                        preparedStatement.setString(1, topicPattern)
                        val resultSet = preparedStatement.executeQuery()
                        if (resultSet.next()) {
                            callback(topicPattern)
                        }
                    }
                }
            } catch (e: SQLException) {
                logger.warning("Error finding exact topic [$topicPattern]: ${e.message}")
            }
            return
        }

        // For wildcard patterns, build efficient SQL filter
        val filter = levels.mapIndexed { index, level ->
            when (level) {
                "+", "#" -> null
                else -> {
                    if (index >= MAX_FIXED_TOPIC_LEVELS) {
                        Pair("topic_r[${index - MAX_FIXED_TOPIC_LEVELS + 1}] = ?", level)
                    } else {
                        Pair(FIXED_TOPIC_COLUMN_NAMES[index] + " = ?", level)
                    }
                }
            }
        }.filterNotNull()

        val where = filter.joinToString(" AND ") { it.first }.ifEmpty { "1=1" }

        // Handle pattern like 'a/+' - find topics like 'a/b' even if only 'a/b/c' exists
        val extractDepth = if (topicPattern.endsWith("#")) {
            // Multi-level wildcard - extract at the # level and deeper
            levels.size - 1
        } else {
            // Single level or exact match - extract at pattern depth
            levels.size
        }

        val sql = buildString {
            append("SELECT DISTINCT ")

            // Build topic reconstruction from fixed columns and array
            if (extractDepth <= MAX_FIXED_TOPIC_LEVELS) {
                // All levels are in fixed columns - use array_cat to combine them
                val levelColumns = (0 until extractDepth).map {
                    "CASE WHEN ${FIXED_TOPIC_COLUMN_NAMES[it]} = '' THEN NULL ELSE ${FIXED_TOPIC_COLUMN_NAMES[it]} END"
                }
                append("array_to_string(ARRAY[${levelColumns.joinToString(", ")}], '/')")
            } else {
                // Mix of fixed columns and array - combine fixed + slice of topic_r
                val fixedArray = "ARRAY[${FIXED_TOPIC_COLUMN_NAMES.joinToString(", ") {
                    "CASE WHEN $it = '' THEN NULL ELSE $it END"
                }}]"
                val arraySlice = "topic_r[1:${extractDepth - MAX_FIXED_TOPIC_LEVELS}]"
                append("array_to_string(array_cat($fixedArray, $arraySlice), '/')")
            }

            append(" FROM $tableName WHERE $where")
        }

        logger.fine { "SQL for findMatchingTopics: $sql" }

        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    filter.forEachIndexed { i, x -> preparedStatement.setString(i + 1, x.second) }
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        val topic = resultSet.getString("extracted_topic")
                        if (topic != null && topic.isNotEmpty()) {
                            if (!callback(topic)) {
                                break // Stop if callback returns false
                            }
                        }
                    }
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error finding topics for pattern [$topicPattern]: ${e.message}")
        }
    }

    override fun purgeOldMessages(olderThan: Instant): PurgeResult {
        val startTime = System.currentTimeMillis()
        var deletedCount = 0
        
        logger.fine { "Starting purge for [$name] - removing messages older than $olderThan" }
        
        try {
            db.connection?.let { connection ->
                val sql = "DELETE FROM $tableName WHERE time < ?"
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setTimestamp(1, Timestamp.from(olderThan))
                    deletedCount = preparedStatement.executeUpdate()
                }
            }
        } catch (e: SQLException) {
            logger.severe("Error purging old messages from [$name]: ${e.message}")
        }
        
        val elapsedTimeMs = System.currentTimeMillis() - startTime
        val result = PurgeResult(deletedCount, elapsedTimeMs)
        
        logger.fine { "Purge completed for [$name]: deleted ${result.deletedCount} messages in ${result.elapsedTimeMs}ms" }
        
        return result
    }

    override fun dropStorage(): Boolean {
        return try {
            db.connection?.let { connection ->
                val sql = "DROP TABLE IF EXISTS $tableName"
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.executeUpdate()
                }
                logger.info("Dropped table [$tableName] for message store [$name]")
                true
            } ?: false
        } catch (e: SQLException) {
            logger.severe("Error dropping table [$tableName] for message store [$name]: ${e.message}")
            false
        }
    }

    override fun getConnectionStatus(): Boolean = db.check()

    override fun findTopicsByName(name: String, ignoreCase: Boolean, namespace: String): List<String> {
        val resultTopics = mutableListOf<String>()
        val sqlSearchPattern = name.replace("*", "%").replace("+", "_")
        val sqlNamespacePattern = if (namespace.isEmpty()) "%" else "$namespace/%"

        val sql = """
        SELECT topic
        FROM $tableName AS t
        WHERE topic_l <> '${Const.CONFIG_TOPIC}'
        AND ${if (ignoreCase) "LOWER(topic)" else "topic"} LIKE ${if (ignoreCase) "LOWER(?)" else "?"}
        AND ${if (ignoreCase) "LOWER(topic)" else "topic"} LIKE ${if (ignoreCase) "LOWER(?)" else "?"}
        ORDER BY topic
        """.trimIndent()

        logger.fine { "findTopicsByName SQL: $sql with pattern '$sqlSearchPattern' [${Utils.getCurrentFunctionName()}]" }

        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, sqlNamespacePattern)
                    preparedStatement.setString(2, sqlSearchPattern)
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        val fullTopic = resultSet.getString("topic") ?: ""
                        resultTopics.add(fullTopic)
                    }
                }
            }
        } catch (e: SQLException) {
            logger.severe("Error finding topics by name: ${e.message}")
        }

        logger.fine { "findTopicsByName result: ${resultTopics.size} topics found [${Utils.getCurrentFunctionName()}]" }
        return resultTopics
    }

    override fun findTopicsByConfig(config: String, description: String, ignoreCase: Boolean, namespace: String): List<Pair<String, String>> {
        val resultTopics = mutableListOf<Pair<String, String>>()
        val sqlSearchPattern = description
        val sqlNamespacePattern = if (namespace.isEmpty()) "%" else "$namespace/%"

        // CrateDB uses different JSON operators than PostgreSQL
        // CrateDB: payload_json['config'] ~ 'pattern'
        val sql = """
        SELECT RTRIM(topic, '/${Const.CONFIG_TOPIC}') AS topic, payload_json AS config
        FROM $tableName
        WHERE topic_l = '${Const.CONFIG_TOPIC}'
        AND ${if (ignoreCase) "LOWER(topic)" else "topic"} LIKE ${if (ignoreCase) "LOWER(?)" else "?"}
        AND ${if (ignoreCase) "LOWER(payload_json['${config}'])" else "payload_json['${config}']"} LIKE ${if (ignoreCase) "LOWER(?)" else "?"}
        ORDER BY topic
        """.trimIndent()

        logger.fine { "findTopicsByConfig SQL: $sql with pattern '$sqlSearchPattern' [${Utils.getCurrentFunctionName()}]" }

        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, sqlNamespacePattern)
                    preparedStatement.setString(2, "%$sqlSearchPattern%")
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        val fullTopic = resultSet.getString("topic") ?: ""
                        val configJson = resultSet.getString("config") ?: ""
                        resultTopics.add(Pair(fullTopic, configJson))
                    }
                }
            }
        } catch (e: SQLException) {
            logger.severe("Error finding topics by config: ${e.message}")
        }

        logger.fine { "findTopicsByConfig result: ${resultTopics.size} topics found [${Utils.getCurrentFunctionName()}]" }
        return resultTopics
    }

    override suspend fun tableExists(): Boolean {
        return try {
            db.connection?.let { connection ->
                val sql = "SELECT 1 FROM information_schema.tables WHERE table_name = ?"
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, tableName)
                    val resultSet = preparedStatement.executeQuery()
                    resultSet.next()
                }
            } ?: false
        } catch (e: SQLException) {
            logger.warning("Error checking if table [$tableName] exists: ${e.message}")
            false
        }
    }

    override suspend fun createTable(): Boolean {
        return try {
            db.connection?.let { connection ->
                connection.createStatement().use { statement ->
                    val fixedTopicColumns = FIXED_TOPIC_COLUMN_NAMES.joinToString(", ") { "$it VARCHAR" }
                    statement.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS $tableName (
                        topic VARCHAR PRIMARY KEY,
                        $fixedTopicColumns,
                        topic_r VARCHAR[],
                        topic_l VARCHAR,
                        time TIMESTAMPTZ,
                        payload_b64 VARCHAR INDEX OFF,
                        payload_obj OBJECT(DYNAMIC),
                        qos INT,
                        retained BOOLEAN,
                        client_id VARCHAR(65535),
                        message_uuid VARCHAR(36)
                    )
                    """.trimIndent())
                    logger.info("Message store table [$name] initialized (created or already exists) [${Utils.getCurrentFunctionName()}]")
                }
                true
            } ?: false
        } catch (e: Exception) {
            logger.warning("Error creating table: ${e.message}")
            false
        }
    }
}