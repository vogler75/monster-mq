package at.rocworks.stores.postgres

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.DatabaseConnection
import at.rocworks.stores.IMessageArchive
import at.rocworks.stores.IMessageArchiveExtended
import at.rocworks.stores.MessageArchiveType
import at.rocworks.data.PurgeResult
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
    private val password: String,
    private val payloadFormat: at.rocworks.stores.PayloadFormat = at.rocworks.stores.PayloadFormat.DEFAULT
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
                            // Check if table is empty before converting to hypertable
                            val countResult = statement.executeQuery("SELECT COUNT(*) as cnt FROM $tableName;")
                            val isEmpty = countResult.next() && countResult.getInt("cnt") == 0

                            if (isEmpty) {
                                logger.info("Table $tableName is empty, converting to hypertable... [${Utils.getCurrentFunctionName()}]")
                                statement.executeQuery("SELECT create_hypertable('$tableName', 'time');")
                                logger.info("Table $tableName converted to hypertable [${Utils.getCurrentFunctionName()}]")
                            } else {
                                logger.warning("Table $tableName is not empty - skipping hypertable conversion. To convert existing data, run: SELECT create_hypertable('$tableName', 'time', migrate_data => true); [${Utils.getCurrentFunctionName()}]")
                            }
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

    override fun getConnectionStatus(): Boolean = db.check()

    override fun start(startPromise: Promise<Void>) {
        db.start(vertx, startPromise)
    }

    override fun addHistory(messages: List<BrokerMessage>) {
        val sql = "INSERT INTO $tableName (topic, time, payload_blob, payload_json, qos, retained, client_id, message_uuid) "+
                "VALUES (?, ?, ?, ?::JSONB, ?, ?, ?, ?) "+
                "ON CONFLICT (topic, time) DO NOTHING" // TODO: ignore duplicates, what if time resolution is not enough?

        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

                messages.forEach { message ->
                    preparedStatement.setString(1, message.topicName)
                    preparedStatement.setTimestamp(2, Timestamp.from(message.time))

                    // Only try JSON conversion if payloadFormat is JSON
                    if (payloadFormat == at.rocworks.stores.PayloadFormat.JSON) {
                        val payloadJson = message.getPayloadAsJson()
                        if (payloadJson != null) {
                            preparedStatement.setNull(3, Types.BINARY)
                            preparedStatement.setString(4, payloadJson)
                        } else {
                            // JSON format requested but payload is not valid JSON - store as binary
                            preparedStatement.setBytes(3, message.payload)
                            preparedStatement.setNull(4, Types.VARCHAR)
                        }
                    } else {
                        // DEFAULT format - store as binary only
                        preparedStatement.setBytes(3, message.payload)
                        preparedStatement.setNull(4, Types.VARCHAR)
                    }

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
        logger.fine("PostgreSQL getHistory called with: topic=$topic, startTime=$startTime, endTime=$endTime, limit=$limit")
        val sql = StringBuilder("SELECT topic, time, payload_blob, payload_json, qos, retained, client_id, message_uuid FROM $tableName WHERE topic LIKE ?")
        val topicPattern = topic.replace("#", "%") // replace "#" wildcard with "%" for SQL LIKE
        val params = mutableListOf<Any>()
        params.add(topicPattern)

        startTime?.let {
            sql.append(" AND time >= ?")
            params.add(Timestamp.from(it))
            logger.fine("Added startTime parameter: $it")
        }
        endTime?.let {
            sql.append(" AND time <= ?")
            params.add(Timestamp.from(it))
            logger.fine("Added endTime parameter: $it")
        }
        sql.append(" ORDER BY time DESC LIMIT ?")
        params.add(limit)
        
        logger.fine("Executing PostgreSQL SQL: ${sql.toString()} with parameters: $params")

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
                    logger.fine("PostgreSQL query completed in ${queryDuration}ms")
                    
                    val processingStart = System.currentTimeMillis()
                    var rowCount = 0
                    while (resultSet.next()) {
                        rowCount++
                        val messageObj = JsonObject()
                            .put("topic", resultSet.getString("topic") ?: topic)
                            .put("timestamp", resultSet.getTimestamp("time").toInstant().toEpochMilli())
                            .put("qos", resultSet.getInt("qos"))
                            .put("client_id", resultSet.getString("client_id") ?: "")

                        // Add payload - prefer JSON if available, otherwise use base64 blob
                        val payloadJson = resultSet.getString("payload_json")
                        if (payloadJson != null) {
                            messageObj.put("payload_json", payloadJson)
                        } else {
                            val payloadBlob = resultSet.getBytes("payload_blob")
                            if (payloadBlob != null) {
                                messageObj.put("payload_base64", java.util.Base64.getEncoder().encodeToString(payloadBlob))
                            }
                        }

                        messages.add(messageObj)
                    }
                    val processingDuration = System.currentTimeMillis() - processingStart
                    logger.fine("PostgreSQL data processing took ${processingDuration}ms, processed $rowCount rows, returning ${messages.size()} messages")
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
            logger.fine { "Executing SQL query: $sql [${Utils.getCurrentFunctionName()}]" }
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
                        logger.fine { "SQL query executed successfully with [${result.size()}] rows. [${Utils.getCurrentFunctionName()}]" }
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
    
    override fun purgeOldMessages(olderThan: Instant): PurgeResult {
        val startTime = System.currentTimeMillis()
        var deletedCount = 0
        
        logger.fine { "Starting purge for [$name] - removing messages older than $olderThan" }
        
        try {
            db.connection?.let { connection ->
                val sql = "DELETE FROM $tableName WHERE time < ?"
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setTimestamp(1, Timestamp.from(olderThan))
                    deletedCount = preparedStatement.executeUpdate()
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            logger.severe("Error purging old messages from [$name]: ${e.message}")
        }
        
        val elapsedTimeMs = System.currentTimeMillis() - startTime
        val result = PurgeResult(deletedCount, elapsedTimeMs)
        
        logger.fine { "Purge completed for [$name]: deleted ${result.deletedCount} messages in ${result.elapsedTimeMs}ms" }
        
        return result
    }

    override fun dropStorage(): Boolean {
        return try {
            db.connection?.let { connection ->
                val sql = "DROP TABLE IF EXISTS $tableName CASCADE"
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.executeUpdate()
                }
                connection.commit()
                logger.info("Dropped table [$tableName] for message archive [$name]")
                true
            } ?: false
        } catch (e: SQLException) {
            logger.severe("Error dropping table [$tableName] for message archive [$name]: ${e.message}")
            false
        }
    }
}