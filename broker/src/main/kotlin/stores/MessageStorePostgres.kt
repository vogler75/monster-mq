package at.rocworks.stores

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import java.sql.*
import java.time.Instant

class MessageStorePostgres(
    private val name: String,
    private val url: String,
    private val username: String,
    private val password: String
): AbstractVerticle(), IMessageStore {
    private val logger = Utils.getLogger(this::class.java, name)
    private val tableName = "${name}"
    private val tableNameHistory = "${name}history"

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun init(connection: Connection) {
            try {
                val statement: Statement = connection.createStatement()
                statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS $tableName (
                    topic text[] PRIMARY KEY,
                    time TIMESTAMPTZ,                    
                    payload BYTEA,
                    payload_json JSONB,
                    qos INT,
                    client_id VARCHAR(65535), 
                    message_uuid VARCHAR(36)
                )
                """.trimIndent())
                statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS $tableNameHistory (
                    topic text[],
                    time TIMESTAMPTZ,                    
                    payload BYTEA,
                    payload_json JSONB,
                    qos INT,
                    client_id VARCHAR(65535), 
                    message_uuid VARCHAR(36),
                    PRIMARY KEY (topic, time)
                )
                """.trimIndent())
                statement.executeUpdate("CREATE INDEX IF NOT EXISTS ${tableNameHistory}_time_idx ON $tableNameHistory (time);")
                logger.info("Message store [$name] is ready [${Utils.getCurrentFunctionName()}]")
            } catch (e: Exception) {
                logger.severe("Error in creating table [$name]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            }
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
                val sql = "SELECT payload, qos, client_id, message_uuid FROM $tableName WHERE topic = ?"
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                val topicLevels = Utils.getTopicLevels(topicName).toTypedArray()
                preparedStatement.setArray(1, connection.createArrayOf("text", topicLevels))

                val resultSet = preparedStatement.executeQuery()

                if (resultSet.next()) {
                    val payload = resultSet.getBytes(1)
                    val qos = resultSet.getInt(2)
                    val clientId = resultSet.getString(3)
                    val messageUuid = resultSet.getString(4)
                    return MqttMessage(
                        messageUuid = messageUuid,
                        messageId = 0,
                        topicName = topicName,
                        payload = payload,
                        qosLevel = qos,
                        isRetain = true,
                        clientId = clientId,
                        isDup = false
                    )
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error fetching data for topic [$topicName]: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
        return null
    }

    override fun addAll(messages: List<MqttMessage>) {
        val sql = "INSERT INTO $tableName (topic, time, payload, payload_json, qos, client_id, message_uuid) "+
                   "VALUES (?, ?, ?, ?::JSONB, ?, ?, ?) "+
                   "ON CONFLICT (topic) DO UPDATE "+
                   "SET time = EXCLUDED.time, "+
                   "payload = EXCLUDED.payload, "+
                   "payload_json = EXCLUDED.payload_json, "+
                   "qos = EXCLUDED.qos, "+
                   "client_id = EXCLUDED.client_id, "+
                   "message_uuid = EXCLUDED.message_uuid "

        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

                messages.forEach { message ->
                    val topic = Utils.getTopicLevels(message.topicName).toTypedArray()
                    preparedStatement.setArray(1, connection.createArrayOf("text", topic))
                    preparedStatement.setTimestamp(2, Timestamp.from(Instant.now()))
                    preparedStatement.setBytes(3, message.payload)
                    preparedStatement.setString(4, message.getPayloadAsJson())
                    preparedStatement.setInt(5, message.qosLevel)
                    preparedStatement.setString(6, message.clientId)
                    preparedStatement.setString(7, message.messageUuid)
                    preparedStatement.addBatch()
                }

                preparedStatement.executeBatch()
                logger.finest { "Batch insert successful [${Utils.getCurrentFunctionName()}]" }
            }
        } catch (e: SQLException) {
            logger.warning("Error inserting batch data [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun delAll(messages: List<MqttMessage>) {
        val rows: MutableList<Array<String>> = ArrayList()
        messages.forEach { message ->
            val levels = Utils.getTopicLevels(message.topicName).toTypedArray()
            rows.add(levels)
        }

        val sql = "DELETE FROM $tableName WHERE topic = ? "

        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

                for (topic in rows) {
                    preparedStatement.setArray(1, connection.createArrayOf("text", topic))
                    preparedStatement.addBatch()
                }

                preparedStatement.executeBatch()
                logger.finer { "Batch deleted successful [${Utils.getCurrentFunctionName()}]" }
            }
        } catch (e: SQLException) {
            logger.warning("Error deleting batch data [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun addAllHistory(messages: List<MqttMessage>) {
        val sql = "INSERT INTO $tableNameHistory (topic, time, payload, payload_json, qos, client_id, message_uuid) "+
                   "VALUES (?, ?, ?, ?::JSONB, ?, ?, ?) "+
                   "ON CONFLICT (topic, time) DO UPDATE "+
                   "SET payload = EXCLUDED.payload, "+
                   "payload_json = EXCLUDED.payload_json, "+
                   "qos = EXCLUDED.qos, "+
                   "client_id = EXCLUDED.client_id, "+
                   "message_uuid = EXCLUDED.message_uuid"

        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

                messages.forEach { message ->
                    val topic = Utils.getTopicLevels(message.topicName).toTypedArray()
                    preparedStatement.setArray(1, connection.createArrayOf("text", topic))
                    preparedStatement.setTimestamp(2, Timestamp.from(Instant.now()))
                    preparedStatement.setBytes(3, message.payload)
                    preparedStatement.setString(4, message.getPayloadAsJson())
                    preparedStatement.setInt(5, message.qosLevel)
                    preparedStatement.setString(6, message.clientId)
                    preparedStatement.setString(7, message.messageUuid)
                    preparedStatement.addBatch()
                }

                preparedStatement.executeBatch()
                logger.finest { "Batch insert successful [${Utils.getCurrentFunctionName()}]" }
            }
        } catch (e: SQLException) {
            logger.warning("Error inserting batch data [${e.message}] [${Utils.getCurrentFunctionName()}]")
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
                val sql = "SELECT array_to_string(topic, '/'), payload, qos, client_id, message_uuid FROM $tableName WHERE $where"
                logger.finest { "SQL: $sql [${Utils.getCurrentFunctionName()}]" }
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                topicLevels.forEachIndexed { index, level ->
                    preparedStatement.setString(index+1, level.second)
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
                        clientId = clientId
                    )
                    callback(message)
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error finding data for topic [$topicName]: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
    }
}