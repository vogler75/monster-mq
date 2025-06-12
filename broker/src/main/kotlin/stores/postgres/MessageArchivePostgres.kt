package at.rocworks.stores.postgres

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.stores.DatabaseConnection
import at.rocworks.stores.IMessageArchive
import at.rocworks.stores.MessageArchiveType
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import java.sql.*
import java.time.Instant

class MessageArchivePostgres (
    private val name: String,
    private val url: String,
    private val username: String,
    private val password: String
): AbstractVerticle(), IMessageArchive {
    private val logger = Utils.getLogger(this::class.java, name)
    private val tableName = name.lowercase()

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun init(connection: Connection): Future<Void> {
            val promise = Promise.promise<Void>()
            try {
                connection.autoCommit = false
                connection.createStatement().use { statement ->
                    statement.executeUpdate(
                        """
                    CREATE TABLE IF NOT EXISTS $tableName (
                        topic text,
                        time TIMESTAMPTZ,                    
                        payload_blob BYTEA,
                        payload_json JSONB,
                        qos INT,
                        retained BOOLEAN,
                        client_id VARCHAR(65535), 
                        message_uuid VARCHAR(36),
                        PRIMARY KEY (topic, time)
                    )
                    """.trimIndent()
                    )
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS ${tableName}_time_idx ON $tableName (time);")

                    // Check if TimescaleDB extension is available
                    val resultSet = statement.executeQuery("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb';")
                    if (resultSet.next()) {
                        logger.info("TimescaleDB extension is available [${Utils.getCurrentFunctionName()}]")
                        val hypertableCheck =
                            statement.executeQuery("SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = '$tableName';")
                        if (!hypertableCheck.next()) {
                            logger.info("Table $tableName convert to hypertable... [${Utils.getCurrentFunctionName()}]")
                            statement.executeQuery("SELECT create_hypertable('$tableName', 'time');")
                            logger.info("Table $tableName converted to hypertable [${Utils.getCurrentFunctionName()}]")
                        } else {
                            logger.info("Table $tableName is already a hypertable [${Utils.getCurrentFunctionName()}]")
                        }
                    } else {
                        logger.warning("TimescaleDB extension is not available [${Utils.getCurrentFunctionName()}]")
                    }
                }
                connection.commit()
                logger.info("Message store [$name] is ready [${Utils.getCurrentFunctionName()}]")
                promise.complete()
            } catch (e: Exception) {
                logger.severe("Error in creating table [$name]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            }
            return promise.future()
        }
    }

    override fun getName(): String = name
    override fun getType() = MessageArchiveType.POSTGRES

    override fun start(startPromise: Promise<Void>) {
        db.start(vertx, startPromise)
    }

    override fun addHistory(messages: List<MqttMessage>) {
        val sql = "INSERT INTO $tableName (topic, time, payload_blob, payload_json, qos, retained, client_id, message_uuid) "+
                "VALUES (?, ?, ?, ?::JSONB, ?, ?, ?, ?) "+
                "ON CONFLICT (topic, time) DO NOTHING" // TODO: ignore duplicates, what if time resolution is not enough?

        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

                messages.forEach { message ->
                    preparedStatement.setString(1, message.topicName)
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
                connection.commit()
            }
        } catch (e: SQLException) {
            logger.warning("Error inserting batch data [${e.errorCode}] [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun getHistory(
        topic: String,
        startTime: Instant?,
        endTime: Instant?,
        limit: Int
    ): List<MqttMessage> {
        val sql = StringBuilder("SELECT topic, time, payload_blob, qos, retained, client_id, message_uuid FROM $tableName WHERE topic = ?")
        val params = mutableListOf<Any>()
        params.add(topic)

        startTime?.let {
            sql.append(" AND time >= ?")
            params.add(Timestamp.from(it))
        }
        endTime?.let {
            sql.append(" AND time <= ?")
            params.add(Timestamp.from(it))
        }
        sql.append(" ORDER BY time DESC LIMIT ?")
        params.add(limit)

        val messages = mutableListOf<MqttMessage>()

        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql.toString()).use { preparedStatement ->
                    for ((index, param) in params.withIndex()) {
                        when (param) {
                            is String -> preparedStatement.setString(index + 1, param)
                            is Timestamp -> preparedStatement.setTimestamp(index + 1, param)
                            is Int -> preparedStatement.setInt(index + 1, param)
                            else -> preparedStatement.setObject(index + 1, param)
                        }
                    }
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        val topicName = resultSet.getString("topic")
                        val time = resultSet.getTimestamp("time").toInstant()
                        val payloadBlob = resultSet.getBytes("payload_blob")
                        val qos = resultSet.getInt("qos")
                        val retained = resultSet.getBoolean("retained")
                        val clientId = resultSet.getString("client_id")
                        val messageUuid = resultSet.getString("message_uuid")

                        messages.add(
                            MqttMessage(
                                messageUuid = messageUuid,
                                messageId = 0,
                                topicName = topicName,
                                payload = payloadBlob,
                                qosLevel = qos,
                                isRetain = retained,
                                isDup = false,
                                isQueued = false,
                                clientId = clientId,
                                time = time
                            )
                        )
                    }
                }
            }
        } catch (e: SQLException) {
            logger.severe("Error retrieving history for topic [$topic]: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
        return messages
    }
}