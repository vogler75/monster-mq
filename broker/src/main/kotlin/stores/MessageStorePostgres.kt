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

    override fun start() {
        connection = connectDatabase()
        vertx.setPeriodic(5000) { // TODO: configurable
            if (!checkConnection())
                connection = connectDatabase()
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

    private fun connectDatabase(): Connection? {
        try {
            logger.info("Connect to PostgreSQL Database...")
            DriverManager.getConnection(url, username, password)
                ?.let { // TODO: check failure and retry to connect
                    it.autoCommit = true
                    logger.info("Connection established.")
                    checkTable(it)
                    return it
                }
        } catch (e: Exception) {
            logger.warning("Error opening connection [${e.message}]")
        }
        return null
    }

    private fun checkTable(connection: Connection) {
        try {
            val statement: Statement = connection.createStatement()
            statement.executeUpdate("""
            CREATE TABLE IF NOT EXISTS $name (
                topic text[] PRIMARY KEY,
                payload BYTEA,
                time TIMESTAMPTZ
            )
            """.trimIndent())
            logger.info("Table [$name] is ready.")
        } catch (e: Exception) {
            logger.severe("Error in creating table [$name]: ${e.message}")
        }
    }

    override fun get(topicName: String): MqttMessage? {
        try {
            connection?.let { connection ->
                val sql = "SELECT payload FROM $name WHERE topic = ?"
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

        val sql = "INSERT INTO $name (topic, payload, time) VALUES (?, ?, ?) "+
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

        val sql = "DELETE FROM $name WHERE topic = ? "

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
                val sql = "SELECT array_to_string(topic, '/'), payload FROM $name WHERE "+topicLevels.joinToString(" AND ") { it.first }
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