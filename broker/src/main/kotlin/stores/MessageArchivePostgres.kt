package at.rocworks.stores

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
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
    private val tableName = name

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun init(connection: Connection): Future<Void> {
            val promise = Promise.promise<Void>()
            try {
                val statement: Statement = connection.createStatement()
                statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS $tableName (
                    topic text[],
                    time TIMESTAMPTZ,                    
                    payload BYTEA,
                    payload_json JSONB,
                    qos INT,
                    retained BOOLEAN,
                    client_id VARCHAR(65535), 
                    message_uuid VARCHAR(36),
                    PRIMARY KEY (topic, time)
                )
                """.trimIndent())
                statement.executeUpdate("CREATE INDEX IF NOT EXISTS ${tableName}_time_idx ON $tableName (time);")
                logger.info("Message store [$name] is ready [${Utils.getCurrentFunctionName()}]")
                promise.complete()
            } catch (e: Exception) {
                logger.severe("Error in creating table [$name]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            }
            return promise.future()
        }
    }

    override fun getName(): String = name
    override fun getType(): MessageStoreType = MessageStoreType.POSTGRES

    override fun start(startPromise: Promise<Void>) {
        db.start(vertx, startPromise)
    }

    override fun addAllHistory(messages: List<MqttMessage>) {
        val sql = "INSERT INTO $tableName (topic, time, payload, payload_json, qos, retained, client_id, message_uuid) "+
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
                logger.finest { "Batch insert successful [${Utils.getCurrentFunctionName()}]" }
            }
        } catch (e: SQLException) {
            logger.warning("Error inserting batch data [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }
}