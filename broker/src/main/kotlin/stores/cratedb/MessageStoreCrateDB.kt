package at.rocworks.stores.cratedb

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.stores.DatabaseConnection
import at.rocworks.stores.IMessageStore
import at.rocworks.stores.MessageStoreType
import at.rocworks.stores.PurgeResult
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import java.sql.*
import java.time.Instant

class MessageStoreCrateDB(
    private val name: String,
    private val url: String,
    private val username: String,
    private val password: String
): AbstractVerticle(), IMessageStore {
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
                        payload_obj OBJECT,
                        qos INT,
                        retained BOOLEAN,
                        client_id VARCHAR(65535),
                        message_uuid VARCHAR(36)
                    )
                    """.trimIndent())
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

    override fun get(topicName: String): MqttMessage? {
        try {
            db.connection?.let { connection ->
                val sql = "SELECT payload_b64, qos, retained, client_id, message_uuid FROM $tableName WHERE topic = ?"
                connection.prepareStatement(sql).use { preparedStatement ->
                    val topicLevels = Utils.getTopicLevels(topicName).toTypedArray()
                    preparedStatement.setArray(1, connection.createArrayOf("varchar", topicLevels))

                    val resultSet = preparedStatement.executeQuery()

                    if (resultSet.next()) {
                        val payload = MqttMessage.getPayloadFromBase64(resultSet.getString(1))
                        val qos = resultSet.getInt(2)
                        val retained = resultSet.getBoolean(3)
                        val clientId = resultSet.getString(4)
                        val messageUuid = resultSet.getString(5)

                        if (lastGetError != 0) {
                            logger.info("Read successful after error [${Utils.getCurrentFunctionName()}]")
                            lastGetError = 0
                        }

                        return MqttMessage(
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

    override fun addAll(messages: List<MqttMessage>) {
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
                    messages.forEach { message ->
                        val (first, rest, last) = splitTopic(message.topicName)
                        preparedStatement.setString(1, message.topicName)
                        repeat(MAX_FIXED_TOPIC_LEVELS) { preparedStatement.setString(it + 2, first.getOrNull(it) ?: "") }
                        preparedStatement.setArray(MAX_FIXED_TOPIC_LEVELS + 2, connection.createArrayOf("varchar", rest.toTypedArray()))
                        preparedStatement.setString(MAX_FIXED_TOPIC_LEVELS + 3, last)
                        preparedStatement.setTimestamp(MAX_FIXED_TOPIC_LEVELS + 4, Timestamp.from(message.time))
                        preparedStatement.setString(MAX_FIXED_TOPIC_LEVELS + 5, message.getPayloadAsBase64())
                        preparedStatement.setString(MAX_FIXED_TOPIC_LEVELS + 6, message.getPayloadAsJson())
                        preparedStatement.setInt(MAX_FIXED_TOPIC_LEVELS + 7, message.qosLevel)
                        preparedStatement.setBoolean(MAX_FIXED_TOPIC_LEVELS + 8, message.isRetain)
                        preparedStatement.setString(MAX_FIXED_TOPIC_LEVELS + 9, message.clientId)
                        preparedStatement.setString(MAX_FIXED_TOPIC_LEVELS + 10, message.messageUuid)
                        preparedStatement.addBatch()
                    }
                    preparedStatement.executeBatch()
                    if (lastAddAllError != 0) {
                        logger.info("Batch insert successful after error [${Utils.getCurrentFunctionName()}]")
                        lastAddAllError = 0
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

    override fun findMatchingMessages(topicName: String, callback: (MqttMessage) -> Boolean) {
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
                        val payload = MqttMessage.getPayloadFromBase64(resultSet.getString(2))
                        val qos = resultSet.getInt(3)
                        val clientId = resultSet.getString(4)
                        val messageUuid = resultSet.getString(5)
                        val message = MqttMessage(
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

    override fun purgeOldMessages(olderThan: Instant): PurgeResult {
        val startTime = System.currentTimeMillis()
        var deletedCount = 0
        
        logger.fine("Starting purge for [$name] - removing messages older than $olderThan")
        
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
        
        logger.fine("Purge completed for [$name]: deleted ${result.deletedCount} messages in ${result.elapsedTimeMs}ms")
        
        return result
    }
}