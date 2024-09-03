package at.rocworks.stores

import at.rocworks.data.MqttMessage
import io.vertx.core.AbstractVerticle
import java.sql.*
import java.time.Instant
import java.util.logging.Logger

class MessageStorePostgres(
    private val name: String,
    private val url: String,
    private val username: String,
    private val password: String
): AbstractVerticle(), IMessageStore {
    private val logger = Logger.getLogger(this.javaClass.simpleName+"/"+name)

    private var connection: Connection? = null

    /*
        CREATE TABLE retained (
            topic text[] PRIMARY KEY,
            payload BYTEA,
            time TIMESTAMPTZ
        )
    */

    override fun start() {
        connectDatabase()
        vertx.setPeriodic(5000) {
            if (!checkConnection())
                connectDatabase()
        }
    }

    private fun checkConnection(): Boolean {
        if (connection != null && !connection!!.isClosed) {
            connection!!.prepareStatement("SELECT 1").use { stmt ->
                stmt.executeQuery().use { rs ->
                    if (rs.next()) {
                        return true // Connection is good
                    }
                }
            }
        }
        return false
    }

    private fun connectDatabase() {
        if (connection == null) {
            try {
                logger.info("Connect to PostgreSQL Database...")
                DriverManager.getConnection(url, username, password)
                    ?.let { // TODO: check failure and retry to connect
                        connection = it
                        it.autoCommit = true
                        logger.info("Connection established.")
                    }
            } catch (e: Exception) {
                logger.warning("Error opening connection [${e.message}]")
            }
        }
    }

    override fun get(topicName: String): MqttMessage? {
        try {
            connection?.let { connection ->
                val sql = "SELECT payload FROM retained WHERE topic = ?"
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                val topicLevels = topicName.split("/").toTypedArray()
                preparedStatement.setArray(1, connection.createArrayOf("text", topicLevels))

                val resultSet = preparedStatement.executeQuery()

                if (resultSet.next()) {
                    val payload = resultSet.getBytes("payload")
                    return MqttMessage(
                        messageId = 0,
                        topicName = topicName,
                        payload = payload,
                        qosLevel = 0,
                        isRetain = true,
                        isDup = false
                    )
                }
            }
        } catch (e: SQLException) {
            logger.warning("Get: Error fetching data for topic [$topicName]: ${e.message}")
        }
        return null
    }

    override fun addAll(messages: List<MqttMessage>) {
        val rows: MutableList<Pair<Array<String>, ByteArray>> = ArrayList()
        messages.forEach { message ->
            val levels = message.topicName.split("/").toTypedArray()
            rows.add(Pair(levels, message.payload))
        }

        val sql = "INSERT INTO retained (topic, payload, time) VALUES (?, ?, ?) "+
                    "ON CONFLICT (topic) DO UPDATE SET payload = EXCLUDED.payload, time = EXCLUDED.time"

        try {
            connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

                for ((topic, payload) in rows) {
                    preparedStatement.setArray(1, connection.createArrayOf("text", topic))
                    preparedStatement.setBytes(2, payload)
                    preparedStatement.setTimestamp(3, Timestamp.from(Instant.now()))
                    preparedStatement.addBatch()
                }

                preparedStatement.executeBatch()
                logger.finer { "AddAll: Batch insert of [${rows.count()}] rows successful." }
            }
        } catch (e: SQLException) {
            logger.warning("AddAll: Error inserting batch data [${e.message}]")
        }
    }

    override fun delAll(messages: List<MqttMessage>) {
        val rows: MutableList<Array<String>> = ArrayList()
        messages.forEach { message ->
            val levels = message.topicName.split("/").toTypedArray()
            rows.add(levels)
        }

        val sql = "DELETE FROM retained WHERE topic = ? "

        try {
            connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

                for (topic in rows) {
                    preparedStatement.setArray(1, connection.createArrayOf("text", topic))
                    preparedStatement.addBatch()
                }

                preparedStatement.executeBatch()
                logger.finer { "DellAll: Batch deleted of [${rows.count()}] rows successful." }
            }
        } catch (e: SQLException) {
            logger.warning("AddAll: Error inserting batch data [${e.message}]")
        }
    }

    override fun findMatchingMessages(topicName: String, callback: (MqttMessage) -> Boolean) {
        val topicLevels = topicName.split("/").mapIndexed { index, level ->
            when (level) {
                "+", "#" -> null
                else -> {
                    Pair("topic[${index+1}] = ?", level)
                }
            }
        }.filterNotNull()

        try {
            connection?.let { connection ->
                val sql = "SELECT array_to_string(topic, '/'), payload FROM retained WHERE "+topicLevels.joinToString(" AND ") { it.first }
                logger.info("FindMatchingMessages [$sql])")
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                topicLevels.forEachIndexed { index, level ->
                    preparedStatement.setString(index+1, level.second)
                }
                val resultSet = preparedStatement.executeQuery()
                if (resultSet.next()) {
                    val topic = resultSet.getString(1)
                    val payload = resultSet.getBytes(2)
                    val message = MqttMessage(
                        messageId = 0,
                        topicName = topic,
                        payload = payload,
                        qosLevel = 0,
                        isRetain = true,
                        isDup = false
                    )
                    callback(message)
                }
            }
        } catch (e: SQLException) {
            logger.warning("FindMatchingMessages: Error fetching data for topic [$topicName]: ${e.message}")
        }
    }
}