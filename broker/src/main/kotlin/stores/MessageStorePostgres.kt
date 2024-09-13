package at.rocworks.stores

import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import java.sql.*
import java.time.Instant
import java.util.logging.Logger

class MessageStorePostgres(
    private val name: String,
    private val url: String,
    private val username: String,
    private val password: String
): AbstractVerticle(), IMessageStore {
    private val logger = Utils.getLogger(this::class.java, name)

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun init(connection: Connection) {
            try {
                val statement: Statement = connection.createStatement()
                statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS $name (
                    topic text[] PRIMARY KEY,
                    payload BYTEA,
                    qos INT,
                    time TIMESTAMPTZ
                )
                """.trimIndent())
                logger.info("Table [$name] is ready [${Utils.getCurrentFunctionName()}]")
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
                val sql = "SELECT payload, qos FROM $name WHERE topic = ?"
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                val topicLevels = Utils.getTopicLevels(topicName).toTypedArray()
                preparedStatement.setArray(1, connection.createArrayOf("text", topicLevels))

                val resultSet = preparedStatement.executeQuery()

                if (resultSet.next()) {
                    val payload = resultSet.getBytes(1)
                    val qos = resultSet.getInt(2)
                    return MqttMessage(
                        topicName = topicName,
                        payload = payload,
                        qosLevel = qos,
                        isRetain = true
                    )
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error fetching data for topic [$topicName]: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
        return null
    }

    override fun addAll(messages: List<MqttMessage>) {
        val rows: MutableList<Pair<Array<String>, ByteArray>> = ArrayList()


        val sql = "INSERT INTO $name (topic, payload, qos, time) VALUES (?, ?, ?, ?) "+
                   "ON CONFLICT (topic) DO UPDATE "+
                   "SET payload = EXCLUDED.payload, qos = EXCLUDED.qos, time = EXCLUDED.time"

        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

                messages.forEach { message ->
                    val topic = Utils.getTopicLevels(message.topicName).toTypedArray()
                    preparedStatement.setArray(1, connection.createArrayOf("text", topic))
                    preparedStatement.setBytes(2, message.payload)
                    preparedStatement.setInt(3, message.qosLevel)
                    preparedStatement.setTimestamp(4, Timestamp.from(Instant.now()))
                    preparedStatement.addBatch()
                }

                preparedStatement.executeBatch()
                logger.finest { "Batch insert of [${rows.count()}] rows successful [${Utils.getCurrentFunctionName()}]" }
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

        val sql = "DELETE FROM $name WHERE topic = ? "

        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

                for (topic in rows) {
                    preparedStatement.setArray(1, connection.createArrayOf("text", topic))
                    preparedStatement.addBatch()
                }

                preparedStatement.executeBatch()
                logger.finer { "Batch deleted of [${rows.count()}] rows successful [${Utils.getCurrentFunctionName()}]" }
            }
        } catch (e: SQLException) {
            logger.warning("Error deleting batch data [${e.message}] [${Utils.getCurrentFunctionName()}]")
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
                val sql = "SELECT array_to_string(topic, '/'), payload, qos FROM $name WHERE $where"
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                topicLevels.forEachIndexed { index, level ->
                    preparedStatement.setString(index+1, level.second)
                }
                val resultSet = preparedStatement.executeQuery()
                if (resultSet.next()) {
                    val topic = resultSet.getString(1)
                    val payload = resultSet.getBytes(2)
                    val qos = resultSet.getInt(3)
                    val message = MqttMessage(
                        topicName = topic,
                        payload = payload,
                        qosLevel = qos,
                        isRetain = true
                    )
                    callback(message)
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error finding data for topic [$topicName]: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
    }
}