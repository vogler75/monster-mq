package at.rocworks.stores.cratedb

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.stores.DatabaseConnection
import at.rocworks.stores.IMessageArchiveExtended
import at.rocworks.stores.MessageArchiveType
import at.rocworks.data.PurgeResult
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.sql.*
import java.time.Instant

class MessageArchiveCrateDB (
    private val name: String,
    private val url: String,
    private val username: String,
    private val password: String
): AbstractVerticle(), IMessageArchiveExtended {
    private val logger = Utils.getLogger(this::class.java, name)
    private val tableName = name.lowercase()
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
                        topic VARCHAR,
                        time TIMESTAMPTZ,                    
                        payload_b64 VARCHAR INDEX OFF,
                        payload_obj OBJECT,
                        qos INT,
                        retained BOOLEAN,
                        client_id VARCHAR(65535), 
                        message_uuid VARCHAR(36),
                        PRIMARY KEY (topic, time)
                    )
                    """.trimIndent()
                    )
                }
                logger.info("Message store [$name] is ready [${Utils.getCurrentFunctionName()}]")
                promise.complete()
            } catch (e: Exception) {
                logger.severe("Error in creating table [$name]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            }
            return promise.future()
        }
    }

    override fun getName(): String = name
    override fun getType() = MessageArchiveType.CRATEDB

    override fun start(startPromise: Promise<Void>) {
        db.start(vertx, startPromise)
    }

    override fun addHistory(messages: List<MqttMessage>) {
        val sql = "INSERT INTO $tableName (topic, time, payload_b64, payload_obj, qos, retained, client_id, message_uuid) "+
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "+
                "ON CONFLICT (topic, time) DO NOTHING" // TODO: ignore duplicates, what if time resolution is not enough?

        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

                messages.forEach { message ->
                    preparedStatement.setString(1, message.topicName)
                    preparedStatement.setTimestamp(2, Timestamp.from(message.time))
                    preparedStatement.setString(3, message.getPayloadAsBase64())
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

    override fun getHistory(
        topic: String,
        startTime: Instant?,
        endTime: Instant?,
        limit: Int
    ): JsonArray {
        logger.fine("CrateDB getHistory called with: topic=$topic, startTime=$startTime, endTime=$endTime, limit=$limit")
        
        val sql = StringBuilder("SELECT topic, time, payload_b64, payload_obj, qos, retained, client_id, message_uuid FROM $tableName WHERE topic = ?")
        val params = mutableListOf<Any>(topic)

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
        
        logger.fine("Executing CrateDB SQL: ${sql.toString()}")
        logger.fine("With parameters: $params")

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
                    logger.fine("CrateDB query completed in ${queryDuration}ms")
                    
                    val processingStart = System.currentTimeMillis()
                    var rowCount = 0
                    while (resultSet.next()) {
                        rowCount++
                        val messageObj = JsonObject()
                            .put("topic", resultSet.getString("topic") ?: topic)
                            .put("timestamp", resultSet.getTimestamp("time").toInstant().toEpochMilli())
                            .put("payload_base64", resultSet.getString("payload_b64") ?: "")
                            .put("payload_json", resultSet.getString("payload_obj"))
                            .put("qos", resultSet.getInt("qos"))
                            .put("client_id", resultSet.getString("client_id") ?: "")
                            
                        messages.add(messageObj)
                    }
                    val processingDuration = System.currentTimeMillis() - processingStart
                    logger.fine("CrateDB data processing took ${processingDuration}ms, processed $rowCount rows, returning ${messages.size()} messages")
                }
            }
        } catch (e: SQLException) {
            logger.severe("Error retrieving history for topic [$topic]: ${e.message}")
            e.printStackTrace()
        }
        return messages
    }

    override fun executeQuery(sql: String): JsonArray {
        return try {
            logger.fine("Executing CrateDB query: $sql")
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
                        logger.fine("CrateDB query executed successfully with ${result.size()} rows")
                        result
                    }
                }
            } ?: run {
                logger.warning("No database connection available")
                JsonArray().add("No database connection available.")
            }
        } catch (e: SQLException) {
            logger.severe("Error executing CrateDB query: ${e.message}")
            JsonArray().add("Error executing query: ${e.message}")
        }
    }

    override fun purgeOldMessages(olderThan: Instant): PurgeResult {
        val startTime = System.currentTimeMillis()
        var deletedCount = 0
        
        logger.fine("Starting purge for [$name] - removing messages older than $olderThan")
        
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
        
        logger.fine("Purge completed for [$name]: deleted ${result.deletedCount} messages in ${result.elapsedTimeMs}ms")
        
        return result
    }
}