package at.rocworks.stores.postgres

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.stores.DatabaseConnection
import at.rocworks.stores.IMessageArchive
import at.rocworks.stores.IMessageArchiveExtended
import at.rocworks.stores.MessageArchiveType
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.sql.*
import java.time.Instant
import java.util.concurrent.Callable

class MessageArchivePostgres (
    private val name: String,
    private val url: String,
    private val username: String,
    private val password: String
): AbstractVerticle(), IMessageArchiveExtended {
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
    ): JsonArray {
        logger.info("PostgreSQL getHistory called with: topic=$topic, startTime=$startTime, endTime=$endTime, limit=$limit")
        
        val sql = StringBuilder("SELECT topic, time, payload_blob, payload_json, qos, retained, client_id, message_uuid FROM $tableName WHERE topic = ?")
        val params = mutableListOf<Any>()
        params.add(topic)

        startTime?.let {
            sql.append(" AND time >= ?")
            params.add(Timestamp.from(it))
            logger.info("Added startTime parameter: $it")
        }
        endTime?.let {
            sql.append(" AND time <= ?")
            params.add(Timestamp.from(it))
            logger.info("Added endTime parameter: $it")
        }
        sql.append(" ORDER BY time DESC LIMIT ?")
        params.add(limit)
        
        logger.info("Executing PostgreSQL SQL: ${sql.toString()}")
        logger.info("With parameters: $params")

        val messages = JsonArray()

        try {
            val startTime = System.currentTimeMillis()
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
                    val queryDuration = System.currentTimeMillis() - startTime
                    logger.info("PostgreSQL query completed in ${queryDuration}ms")
                    
                    val processingStart = System.currentTimeMillis()
                    var rowCount = 0
                    while (resultSet.next()) {
                        rowCount++
                        val messageObj = JsonObject()
                            .put("topic", resultSet.getString("topic") ?: topic)
                            .put("timestamp", resultSet.getTimestamp("time").toInstant().toEpochMilli())
                            .put("payload_base64", java.util.Base64.getEncoder().encodeToString(resultSet.getBytes("payload_blob") ?: ByteArray(0)))
                            .put("qos", resultSet.getInt("qos"))
                            .put("client_id", resultSet.getString("client_id") ?: "")
                            
                        messages.add(messageObj)
                    }
                    val processingDuration = System.currentTimeMillis() - processingStart
                    logger.info("PostgreSQL data processing took ${processingDuration}ms, processed $rowCount rows, returning ${messages.size()} messages")
                }
            }
        } catch (e: SQLException) {
            logger.severe("Error retrieving history for topic [$topic]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            e.printStackTrace()
        }
        return messages
    }

    override fun executeQuery(sql: String): JsonArray {
        return try {
            logger.fine("Executing SQL query: $sql [${Utils.getCurrentFunctionName()}]")
            db.connection?.let { connection ->
                connection.createStatement().use { statement ->
                    statement.executeQuery(sql).use { resultSet ->
                        val metaData = resultSet.metaData
                        val columnCount = metaData.columnCount
                        val result = JsonArray()

                        // Add header row
                        val header = JsonArray()
                        for (i in 1..columnCount) {
                            header.add(metaData.getColumnName(i))
                        }
                        result.add(header)

                        // Add data rows
                        while (resultSet.next()) {
                            val row = JsonArray()
                            for (i in 1..columnCount) {
                                row.add(resultSet.getObject(i))
                            }
                            result.add(row)
                        }
                        logger.fine("SQL query executed successfully with [${result.size()}] rows. [${Utils.getCurrentFunctionName()}]")
                        result
                    }
                }
            } ?: run {
                logger.warning("No database connection available. [${Utils.getCurrentFunctionName()}]")
                JsonArray().add("No database connection available.")
            }
        } catch (e: SQLException) {
            logger.severe("Error executing query: ${e.message} [${Utils.getCurrentFunctionName()}]")
            JsonArray().add("Error executing query: ${e.message}")
        }
    }
}