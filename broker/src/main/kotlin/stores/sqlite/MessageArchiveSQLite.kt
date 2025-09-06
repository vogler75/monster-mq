package at.rocworks.stores.sqlite

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.stores.IMessageArchiveExtended
import at.rocworks.stores.MessageArchiveType
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import java.sql.*
import java.time.Instant

class MessageArchiveSQLite (
    private val name: String,
    private val dbPath: String
): AbstractVerticle(), IMessageArchiveExtended {
    private val logger = Utils.getLogger(this::class.java, name)
    private val tableName = name.lowercase()

    init {
        logger.level = Const.DEBUG_LEVEL
    }


    override fun getName(): String = name
    override fun getType() = MessageArchiveType.SQLITE

    override fun start(startPromise: Promise<Void>) {
        // Initialize shared connection
        SharedSQLiteConnection.initialize(vertx)
        
        // Create tables using shared connection
        SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
            connection.createStatement().use { statement ->
                // Create table optimized for time-series data
                statement.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS $tableName (
                        topic TEXT NOT NULL,
                        time TEXT NOT NULL,  -- ISO-8601 timestamp as text                   
                        payload_blob BLOB,
                        payload_json TEXT,
                        qos INTEGER,
                        retained BOOLEAN,
                        client_id TEXT, 
                        message_uuid TEXT,
                        PRIMARY KEY (topic, time)
                    )
                """.trimIndent())
                
                // Create indexes for efficient time-based queries
                statement.executeUpdate("CREATE INDEX IF NOT EXISTS ${tableName}_time_idx ON $tableName (time);")
                statement.executeUpdate("CREATE INDEX IF NOT EXISTS ${tableName}_topic_time_idx ON $tableName (topic, time);")
                logger.info("SQLite Message archive [$name] is ready [${Utils.getCurrentFunctionName()}]")
            }
        }.onComplete(startPromise)
    }

    override fun addHistory(messages: List<MqttMessage>) {
        val sql = "INSERT INTO $tableName (topic, time, payload_blob, payload_json, qos, retained, client_id, message_uuid) "+
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "+
                "ON CONFLICT (topic, time) DO NOTHING"

        try {
            SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    messages.forEach { message ->
                        preparedStatement.setString(1, message.topicName)
                        preparedStatement.setString(2, message.time.toString()) // ISO-8601 format
                        preparedStatement.setBytes(3, message.payload)
                        preparedStatement.setString(4, message.getPayloadAsJson())
                        preparedStatement.setInt(5, message.qosLevel)
                        preparedStatement.setBoolean(6, message.isRetain)
                        preparedStatement.setString(7, message.clientId)
                        preparedStatement.setString(8, message.messageUuid)
                        preparedStatement.addBatch()
                    }
                    preparedStatement.executeBatch()
                }
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
        val sql = StringBuilder("SELECT time, payload_blob FROM $tableName WHERE topic = ?")
        val params = mutableListOf<Any>()
        params.add(topic)

        startTime?.let {
            sql.append(" AND time >= ?")
            params.add(it.toString()) // ISO-8601 string comparison works in SQLite
        }
        endTime?.let {
            sql.append(" AND time <= ?")
            params.add(it.toString())
        }
        sql.append(" ORDER BY time DESC LIMIT ?")
        params.add(limit)

        val messages = JsonArray().apply {
            add(JsonArray().apply {
                add("time")
                add("payload")
            })  // Header row
        }

        try {
            SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
                connection.prepareStatement(sql.toString()).use { preparedStatement ->
                    for ((index, param) in params.withIndex()) {
                        when (param) {
                            is String -> preparedStatement.setString(index + 1, param)
                            is Int -> preparedStatement.setInt(index + 1, param)
                            else -> preparedStatement.setObject(index + 1, param)
                        }
                    }
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        val timeStr = resultSet.getString("time")
                        val payloadBlob = resultSet.getBytes("payload_blob")
                        messages.add(JsonArray().add(timeStr).add(payloadBlob.toString(Charsets.UTF_8)))
                    }
                }
            }
        } catch (e: SQLException) {
            logger.severe("Error retrieving history for topic [$topic]: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
        return messages
    }

    override fun executeQuery(sql: String): JsonArray {
        return try {
            logger.fine("Executing SQLite query: $sql [${Utils.getCurrentFunctionName()}]")
            SharedSQLiteConnection.executeBlocking(dbPath) { connection ->
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
                                val value = resultSet.getObject(i)
                                // Convert SQLite types to JSON-compatible types
                                when (value) {
                                    is ByteArray -> row.add(value.toString(Charsets.UTF_8))
                                    else -> row.add(value)
                                }
                            }
                            result.add(row)
                        }
                        logger.fine("SQLite query executed successfully with [${result.size()}] rows. [${Utils.getCurrentFunctionName()}]")
                        result
                    }
                }
            }.result() ?: run {
                logger.warning("Cannot execute query without database connection [${Utils.getCurrentFunctionName()}]")
                JsonArray()
            }
        } catch (e: SQLException) {
            logger.severe("Error executing SQLite query [$sql]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            JsonArray()
        }
    }
}