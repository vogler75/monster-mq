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
    private val tableName = name
    private var lastAddAllError: Int = 0
    private var lastGetError: Int = 0
    private var lastDelAllError: Int = 0
    private var lastFetchError: Int = 0

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun init(connection: Connection): Future<Void> {
            val promise = Promise.promise<Void>()
            try {
                connection.createStatement().use { statement ->
                    statement.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS $tableName (
                        topic text[] PRIMARY KEY,
                        time TIMESTAMPTZ,                    
                        payload_blob BYTEA,
                        payload_json JSONB,
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
    override fun getType(): MessageStoreType = MessageStoreType.POSTGRES

    override fun start(startPromise: Promise<Void>) {
        db.start(vertx, startPromise)
    }

    override fun get(topicName: String): MqttMessage? {
        try {
            db.connection?.let { connection ->
                val sql = "SELECT payload_blob, qos, retained, client_id, message_uuid FROM $tableName WHERE topic = ?"
                connection.prepareStatement(sql).use { preparedStatement ->
                    val topicLevels = Utils.getTopicLevels(topicName).toTypedArray()
                    preparedStatement.setArray(1, connection.createArrayOf("text", topicLevels))

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
        val sql = "INSERT INTO $tableName (topic, time, payload_blob, payload_json, qos, retained, client_id, message_uuid) "+
                   "VALUES (?, ?, ?, ?::JSONB, ?, ?, ?, ?) "+
                   "ON CONFLICT (topic) DO UPDATE "+
                   "SET time = EXCLUDED.time, "+
                   "payload_blob = EXCLUDED.payload_blob, "+
                   "payload_json = EXCLUDED.payload_json, "+
                   "qos = EXCLUDED.qos, "+
                   "retained = EXCLUDED.retained, "+
                   "client_id = EXCLUDED.client_id, "+
                   "message_uuid = EXCLUDED.message_uuid "

        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    messages.forEach { message ->
                        val topic = Utils.getTopicLevels(message.topicName).toTypedArray()
                        preparedStatement.setArray(1, connection.createArrayOf("text", topic))
                        preparedStatement.setTimestamp(2, Timestamp.from(message.time))
                        preparedStatement.setBytes(3, message.payload)
                        preparedStatement.setString(4, message.getPayloadAsJson())
                        preparedStatement.setInt(5, message.qosLevel)
                        preparedStatement.setBoolean(6, message.isRetain)
                        preparedStatement.setString(7, message.clientId)
                        preparedStatement.setString(8, message.messageUuid)
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
        val rows = topics.map { Utils.getTopicLevels(it).toTypedArray() }
        val sql = "DELETE FROM $tableName WHERE topic = ? " // TODO: can be converted to use IN operator with a list of topics
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    for (topic in rows) {
                        preparedStatement.setArray(1, connection.createArrayOf("text", topic))
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
        val topicLevels = Utils.getTopicLevels(topicName).mapIndexed { index, level ->
            when (level) {
                "+", "#" -> null
                else -> {
                    Pair("topic[${index+1}] = ?", level)
                }
            }
        }.filterNotNull()
        try {
            db.connection?.let { connection ->
                val where = topicLevels.joinToString(" AND ") { it.first }.ifEmpty { "1=1" }
                val sql = "SELECT array_to_string(topic, '/'), payload_blob, qos, client_id, message_uuid FROM $tableName WHERE $where"
                logger.finest { "SQL: $sql [${Utils.getCurrentFunctionName()}]" }
                connection.prepareStatement(sql).use { preparedStatement ->
                    topicLevels.forEachIndexed { index, level ->
                        preparedStatement.setString(index + 1, level.second)
                    }
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        val topic = resultSet.getString(1)
                        val payload = resultSet.getBytes(2)
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
            if (e.errorCode != lastFetchError) { // avoid spamming the logs
                logger.warning("Error finding data for topic [$topicName]: ${e.message} [${Utils.getCurrentFunctionName()}]")
                lastFetchError = e.errorCode
            }
        }
    }
}