package at.rocworks.stores

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import java.sql.*

class MessageStorePostgres(
    private val name: String,
    private val url: String,
    private val username: String,
    private val password: String
): AbstractVerticle(), IMessageStore {
    private val logger = Utils.getLogger(this::class.java, name)
    private val tableName = name.lowercase()
    private var lastAddAllError: Int = Int.MAX_VALUE
    private var lastGetError: Int = Int.MAX_VALUE
    private var lastDelAllError: Int = Int.MAX_VALUE
    private var lastFetchError: Int = Int.MAX_VALUE

    private companion object {
        const val MAX_FIXED_TOPIC_LEVELS = 9
        val FIXED_TOPIC_COLUMN_NAMES = (0 until MAX_FIXED_TOPIC_LEVELS).map { "topic_${it+1}" }
        val ALL_PK_COLUMNS = FIXED_TOPIC_COLUMN_NAMES + "topic_r"

        fun splitTopic(topicName: String): Pair<List<String>, List<String>> {
            val levels = Utils.getTopicLevels(topicName)
            val first = levels.take(MAX_FIXED_TOPIC_LEVELS)
            val rest = levels.drop(MAX_FIXED_TOPIC_LEVELS)
            return Pair(first, rest)
        }
    }

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun init(connection: Connection): Future<Void> {
            val promise = Promise.promise<Void>()
            try {
                connection.createStatement().use { statement ->
                    val pkColumnsString = ALL_PK_COLUMNS.joinToString(", ")
                    val fixedTopicColumns = FIXED_TOPIC_COLUMN_NAMES.joinToString(", ") { "$it text NOT NULL" }
                    statement.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS $tableName (
                        ${fixedTopicColumns},                         
                        topic_r text[] NOT NULL,
                        time TIMESTAMPTZ,                    
                        payload_blob BYTEA,
                        payload_json JSONB,
                        qos INT,
                        retained BOOLEAN,
                        client_id VARCHAR(65535), 
                        message_uuid VARCHAR(36),
                        PRIMARY KEY ($pkColumnsString)
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
    override fun getType(): MessageStoreType = MessageStoreType.POSTGRES

    override fun start(startPromise: Promise<Void>) {
        db.start(vertx, startPromise)
    }

    override fun get(topicName: String): MqttMessage? {
        try {
            db.connection?.let { connection ->
                val fixedTopicColumns = FIXED_TOPIC_COLUMN_NAMES.joinToString("AND ") { "$it=?" }
                val sql = """SELECT payload_blob, qos, retained, client_id, message_uuid FROM $tableName 
                             WHERE $fixedTopicColumns AND topic_r=?"""
                connection.prepareStatement(sql).use { preparedStatement ->
                    val (first, rest) = splitTopic(topicName)
                    repeat(MAX_FIXED_TOPIC_LEVELS) { preparedStatement.setString(it + 1, first.getOrNull(it) ?: "") }
                    preparedStatement.setArray(MAX_FIXED_TOPIC_LEVELS + 1, connection.createArrayOf("text", rest.toTypedArray()))

                    val resultSet = preparedStatement.executeQuery()

                    if (resultSet.next()) {
                        val payload = resultSet.getBytes(1)
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
        val sql = """INSERT INTO $tableName (topic_1, topic_2, topic_3, topic_4, topic_5, topic_6, topic_7, topic_8, topic_9, topic_r, 
                   time, payload_blob, payload_json, qos, retained, client_id, message_uuid) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::JSONB, ?, ?, ?, ?) 
                   ON CONFLICT (topic_1, topic_2, topic_3, topic_4, topic_5, topic_6, topic_7, topic_8, topic_9, topic_r) DO UPDATE 
                   SET time = EXCLUDED.time, 
                   payload_blob = EXCLUDED.payload_blob, 
                   payload_json = EXCLUDED.payload_json,
                   qos = EXCLUDED.qos, 
                   retained = EXCLUDED.retained, 
                   client_id = EXCLUDED.client_id, 
                   message_uuid = EXCLUDED.message_uuid 
                   """

        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    messages.forEach { message ->
                        val (first, rest) = splitTopic(message.topicName)
                        repeat(MAX_FIXED_TOPIC_LEVELS) { preparedStatement.setString(it + 1, first.getOrNull(it) ?: "") }
                        preparedStatement.setArray(MAX_FIXED_TOPIC_LEVELS + 1, connection.createArrayOf("text", rest.toTypedArray()))
                        preparedStatement.setTimestamp(11, Timestamp.from(message.time))
                        preparedStatement.setBytes(12, message.payload)
                        preparedStatement.setString(13, message.getPayloadAsJson())
                        preparedStatement.setInt(14, message.qosLevel)
                        preparedStatement.setBoolean(15, message.isRetain)
                        preparedStatement.setString(16, message.clientId)
                        preparedStatement.setString(17, message.messageUuid)
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
            if (e.errorCode != lastAddAllError) { // avoid spamming the logs
                logger.warning("Error inserting batch data [${e.errorCode}] [${e.message}] [${Utils.getCurrentFunctionName()}]")
                lastAddAllError = e.errorCode
            }
        }
    }

    override fun delAll(topics: List<String>) {
        val sql = "DELETE FROM $tableName WHERE topic_1 = ? AND topic_2 = ? AND topic_3 = ? AND topic_4 = ? AND topic_5 = ? AND topic_6 = ? AND topic_7 = ? AND topic_8 = ? AND topic_9 = ? AND topic_r = ?"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    for (topic in topics) {
                        val (first, rest) = splitTopic(topic)
                        repeat(MAX_FIXED_TOPIC_LEVELS) { preparedStatement.setString(it + 1, first.getOrNull(it) ?: "") }
                        preparedStatement.setArray(MAX_FIXED_TOPIC_LEVELS + 1, connection.createArrayOf("text", rest.toTypedArray()))
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
        val filter = Utils.getTopicLevels(topicName).mapIndexed { index, level ->
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
                val where = filter.joinToString(" AND ") { it.first }.ifEmpty { "1=1" }
                val sql = "SELECT topic_1, topic_2, topic_3, topic_4, topic_5, topic_6, topic_7, topic_8, topic_9, topic_r, " +
                          "payload_blob, qos, client_id, message_uuid "+
                          "FROM $tableName WHERE $where"
                logger.finest { "SQL: $sql [${Utils.getCurrentFunctionName()}]" }
                connection.prepareStatement(sql).use { preparedStatement ->
                    filter.forEachIndexed { i, x -> preparedStatement.setString(i + 1, x.second) }

                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        val topic = (((1 until MAX_FIXED_TOPIC_LEVELS + 1).map { resultSet.getString(it) }.filterNot { it.isNullOrEmpty() }) +
                                (resultSet.getArray(MAX_FIXED_TOPIC_LEVELS + 1).array as Array<String>)).joinToString("/")

                        val payload = resultSet.getBytes(11)
                        val qos = resultSet.getInt(12)
                        val clientId = resultSet.getString(13)
                        val messageUuid = resultSet.getString(14)
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
            if (e.errorCode != lastFetchError) { // avoid spamming the logs
                logger.warning("Error finding data for topic [$topicName]: ${e.message} [${Utils.getCurrentFunctionName()}]")
                lastFetchError = e.errorCode
            }
        }
    }
}