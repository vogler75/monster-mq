package at.rocworks.stores

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import java.sql.*

class MessageArchivePostgres (
    private val name: String,
    private val url: String,
    private val username: String,
    private val password: String
): AbstractVerticle(), IMessageArchive {
    private val logger = Utils.getLogger(this::class.java, name)
    private val tableName = name
    private var lastAddAllHistoryError: Int = 0

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
                        topic text[],
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

    override fun addAllHistory(messages: List<MqttMessage>) {
        val sql = "INSERT INTO $tableName (topic, time, payload_blob, payload_json, qos, retained, client_id, message_uuid) "+
                "VALUES (?, ?, ?, ?::JSONB, ?, ?, ?, ?) "+
                "ON CONFLICT (topic, time) DO NOTHING" // TODO: ignore duplicates, what if time resolution is not enough?

        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

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
                connection.commit()

                if (lastAddAllHistoryError != 0) {
                    logger.info("Batch insert successful after error [${Utils.getCurrentFunctionName()}]")
                    lastAddAllHistoryError = 0
                }
            }
        } catch (e: SQLException) {
            if (e.errorCode != lastAddAllHistoryError) { // avoid spamming the logs
                logger.warning("Error inserting batch data [${e.errorCode}] [${e.message}] [${Utils.getCurrentFunctionName()}]")
                lastAddAllHistoryError = e.errorCode
            }
        }
    }
}