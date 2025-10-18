package at.rocworks.stores.sqlite

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.IMessageArchiveExtended
import at.rocworks.stores.MessageArchiveType
import at.rocworks.data.PurgeResult
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.sql.DriverManager
import java.time.Instant
import java.util.Base64
import java.util.concurrent.TimeUnit

/**
 * Clean SQLiteVerticle-only implementation of MessageArchive
 * No SharedSQLiteConnection - uses only event bus communication
 */
class MessageArchiveSQLite(
    private val name: String,
    private val dbPath: String,
    private val payloadFormat: at.rocworks.stores.PayloadFormat = at.rocworks.stores.PayloadFormat.DEFAULT
): AbstractVerticle(), IMessageArchiveExtended {
    private val logger = Utils.getLogger(this::class.java, name)
    private lateinit var sqlClient: SQLiteClient
    private val tableName = name.lowercase()

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun getName(): String = name
    override fun getType() = MessageArchiveType.SQLITE

    override fun start(startPromise: Promise<Void>) {
        // Initialize SQLiteClient - assumes SQLiteVerticle is already deployed by Monster.kt
        sqlClient = SQLiteClient(vertx, dbPath)
        
        // Create tables using SQLiteClient
        val createTableSQL = JsonArray()
            .add("""
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
            .add("CREATE INDEX IF NOT EXISTS ${tableName}_time_idx ON $tableName (time);")
            .add("CREATE INDEX IF NOT EXISTS ${tableName}_topic_time_idx ON $tableName (topic, time);")

        sqlClient.initDatabase(createTableSQL).onComplete { result ->
            if (result.succeeded()) {
                logger.info("SQLite Message archive [$name] is ready [start]")
                startPromise.complete()
            } else {
                logger.severe("Failed to initialize SQLite message archive: ${result.cause()?.message}")
                startPromise.fail(result.cause())
            }
        }
    }

    override fun addHistory(messages: List<BrokerMessage>) {
        if (messages.isEmpty()) return
        
        val sql = """INSERT INTO $tableName (topic, time, payload_blob, payload_json, qos, retained, client_id, message_uuid) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?) 
                     ON CONFLICT (topic, time) DO NOTHING"""

        val batchParams = JsonArray()
        messages.forEach { message ->
            val params = JsonArray()
                .add(message.topicName)
                .add(message.time.toString()) // ISO-8601 format

            // Only try JSON conversion if payloadFormat is JSON
            if (payloadFormat == at.rocworks.stores.PayloadFormat.JSON) {
                val payloadJson = message.getPayloadAsJson()
                if (payloadJson != null) {
                    params.add(null)  // payload_blob
                    params.add(payloadJson)  // payload_json
                } else {
                    // JSON format requested but payload is not valid JSON - store as binary
                    params.add(message.payload)  // payload_blob
                    params.add(null)  // payload_json
                }
            } else {
                // DEFAULT format - store as binary only
                params.add(message.payload)  // payload_blob
                params.add(null)  // payload_json
            }

            params.add(message.qosLevel)
                .add(message.isRetain)
                .add(message.clientId)
                .add(message.messageUuid)
            batchParams.add(params)
        }

        sqlClient.executeBatch(sql, batchParams).onComplete { result ->
            if (result.failed()) {
                logger.warning("Error inserting batch history data: ${result.cause()?.message}")
            } else {
                logger.fine { "Added ${messages.size} messages to archive" }
            }
        }
    }

    override fun getHistory(
        topic: String,
        startTime: Instant?,
        endTime: Instant?,
        limit: Int
    ): JsonArray {
        logger.fine { "getHistory called with: topic=$topic, startTime=$startTime, endTime=$endTime, limit=$limit" }
        
        val sql = StringBuilder("SELECT topic, time, payload_blob, payload_json, qos, retained, client_id, message_uuid FROM $tableName WHERE topic LIKE ?")
        val topicPattern = topic.replace("#", "%") // replace "#" wildcard with "%" for SQL LIKE
        val params = JsonArray().add(topicPattern)

        startTime?.let {
            sql.append(" AND time >= ?")
            // Use proper ISO-8601 format with proper precision
            val timeStr = it.toString()
            params.add(timeStr)
            logger.fine { "Added startTime parameter: $timeStr" }
        }
        endTime?.let {
            sql.append(" AND time <= ?")
            val timeStr = it.toString()
            params.add(timeStr)
            logger.fine { "Added endTime parameter: $timeStr" }
        }
        sql.append(" ORDER BY time DESC LIMIT ?")
        params.add(limit)
        
        logger.fine { "Executing SQL: ${sql.toString()}" }
        logger.fine { "With parameters: $params" }

        return try {
            logger.fine { "Starting direct JDBC query at ${System.currentTimeMillis()}..." }
            val startTime = System.currentTimeMillis()
            
            // Use direct JDBC connection to bypass event bus issues
            val connection = DriverManager.getConnection("jdbc:sqlite:$dbPath", "", "")
            val messages = JsonArray()
            
            connection.use { conn ->
                conn.prepareStatement(sql.toString()).use { preparedStatement ->
                    // Use the same parameters that were already collected during SQL building
                    for (i in 0 until params.size()) {
                        val param = params.getValue(i)
                        val paramIndex = i + 1
                        logger.fine { "Setting parameter at index $paramIndex: $param (type: ${param?.javaClass?.simpleName})" }
                        when (param) {
                            is String -> preparedStatement.setString(paramIndex, param)
                            is Int -> preparedStatement.setInt(paramIndex, param)
                            is Long -> preparedStatement.setLong(paramIndex, param)
                            else -> preparedStatement.setString(paramIndex, param.toString())
                        }
                    }
                    
                    val resultSet = preparedStatement.executeQuery()
                    val queryDuration = System.currentTimeMillis() - startTime
                    logger.fine { "Direct JDBC query completed in ${queryDuration}ms" }
                    
                    val processingStart = System.currentTimeMillis()
                    var rowCount = 0
                    while (resultSet.next()) {
                        rowCount++
                        val timeStr = resultSet.getString("time")
                        val timestamp = if (timeStr != null) {
                            try {
                                Instant.parse(timeStr).toEpochMilli()
                            } catch (e: Exception) {
                                System.currentTimeMillis() // fallback
                            }
                        } else {
                            System.currentTimeMillis()
                        }
                        
                        val messageObj = JsonObject()
                            .put("topic", resultSet.getString("topic") ?: topic)
                            .put("timestamp", timestamp)
                            .put("qos", resultSet.getInt("qos"))
                            .put("client_id", resultSet.getString("client_id") ?: "")

                        // Add payload - prefer JSON if available, otherwise use base64 blob
                        val payloadJson = resultSet.getString("payload_json")
                        if (payloadJson != null) {
                            messageObj.put("payload_json", payloadJson)
                        } else {
                            val payloadBlob = resultSet.getBytes("payload_blob")
                            if (payloadBlob != null) {
                                messageObj.put("payload_base64", Base64.getEncoder().encodeToString(payloadBlob))
                            }
                        }

                        messages.add(messageObj)
                    }
                    val processingDuration = System.currentTimeMillis() - processingStart
                    logger.fine { "Data processing took ${processingDuration}ms, processed $rowCount rows, returning ${messages.size()} messages" }
                }
            }
            
            messages
        } catch (e: Exception) {
            logger.severe("Error retrieving history for topic [$topic]: ${e.message}")
            e.printStackTrace()
            JsonArray() // Return empty array on error
        }
    }

    override fun executeQuery(sql: String): JsonArray {
        return try {
            logger.fine { "Executing SQLite query: $sql" }
            val results = sqlClient.executeQuerySync(sql, JsonArray())
            
            if (results.size() > 0) {
                val result = JsonArray()
                
                // Get column names from first row (assume all rows have same structure)
                val firstRow = results.getJsonObject(0)
                val header = JsonArray()
                firstRow.fieldNames().sorted().forEach { header.add(it) }
                result.add(header)
                
                // Add data rows
                results.forEach { row ->
                    val rowObj = row as JsonObject
                    val dataRow = JsonArray()
                    header.forEach { columnName ->
                        val value = rowObj.getValue(columnName as String)
                        // Convert ByteArray to String for JSON compatibility
                        when (value) {
                            is ByteArray -> dataRow.add(value.toString(Charsets.UTF_8))
                            else -> dataRow.add(value)
                        }
                    }
                    result.add(dataRow)
                }
                
                logger.fine { "SQLite query executed successfully with ${result.size()} rows" }
                result
            } else {
                JsonArray()
            }
        } catch (e: Exception) {
            logger.severe("Error executing SQLite query [$sql]: ${e.message}")
            JsonArray()
        }
    }

    override fun purgeOldMessages(olderThan: Instant): PurgeResult {
        val startTime = System.currentTimeMillis()
        
        logger.fine { "Starting purge for [$name] - removing messages older than $olderThan" }
        
        val sql = "DELETE FROM $tableName WHERE time < ?"
        val params = JsonArray().add(olderThan.toString())
        
        return try {
            val future = sqlClient.executeUpdate(sql, params)
            val deletedCount = future.toCompletionStage().toCompletableFuture().get(5000, java.util.concurrent.TimeUnit.MILLISECONDS)
            
            val elapsedTimeMs = System.currentTimeMillis() - startTime
            val purgeResult = PurgeResult(deletedCount, elapsedTimeMs)
            
            logger.fine { "Purge completed for [$name]: deleted ${purgeResult.deletedCount} messages in ${purgeResult.elapsedTimeMs}ms" }
            purgeResult
        } catch (e: Exception) {
            logger.severe("Error purging old messages from [$name]: ${e.message}")
            val elapsedTimeMs = System.currentTimeMillis() - startTime
            PurgeResult(0, elapsedTimeMs)
        }
    }

    override fun dropStorage(): Boolean {
        return try {
            val sql = "DROP TABLE IF EXISTS $tableName"
            val params = JsonArray()
            val future = sqlClient.executeUpdate(sql, params)
            val result = future.toCompletionStage().toCompletableFuture().get(5000, TimeUnit.MILLISECONDS)
            logger.info("Dropped table [$tableName] for message archive [$name]")
            true
        } catch (e: Exception) {
            logger.severe("Error dropping table [$tableName] for message archive [$name]: ${e.message}")
            false
        }
    }

    override fun getConnectionStatus(): Boolean {
        return try {
            ::sqlClient.isInitialized
        } catch (e: Exception) {
            false
        }
    }

    override suspend fun tableExists(): Boolean {
        return try {
            val sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?"
            val params = JsonArray().add(tableName)
            val results = sqlClient.executeQuerySync(sql, params)
            results.size() > 0
        } catch (e: Exception) {
            logger.warning("Error checking if table [$tableName] exists: ${e.message}")
            false
        }
    }

    override suspend fun createTable(): Boolean {
        return try {
            val createTableSQL = JsonArray()
                .add("""
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
                .add("CREATE INDEX IF NOT EXISTS ${tableName}_time_idx ON $tableName (time);")
                .add("CREATE INDEX IF NOT EXISTS ${tableName}_topic_time_idx ON $tableName (topic, time);")

            val result = sqlClient.initDatabase(createTableSQL).toCompletionStage().toCompletableFuture().get(5000, java.util.concurrent.TimeUnit.MILLISECONDS)
            logger.info("Message archive table [$name] initialized (created or already exists)")
            true
        } catch (e: Exception) {
            logger.warning("Error creating table: ${e.message}")
            false
        }
    }
}