package at.rocworks.stores.sqlite

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.stores.IMessageArchiveExtended
import at.rocworks.stores.MessageArchiveType
import at.rocworks.stores.PurgeResult
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
    private val dbPath: String
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

    override fun addHistory(messages: List<MqttMessage>) {
        if (messages.isEmpty()) return
        
        val sql = """INSERT INTO $tableName (topic, time, payload_blob, payload_json, qos, retained, client_id, message_uuid) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?) 
                     ON CONFLICT (topic, time) DO NOTHING"""

        val batchParams = JsonArray()
        messages.forEach { message ->
            val params = JsonArray()
                .add(message.topicName)
                .add(message.time.toString()) // ISO-8601 format
                .add(message.payload)
                .add(message.getPayloadAsJson())
                .add(message.qosLevel)
                .add(message.isRetain)
                .add(message.clientId)
                .add(message.messageUuid)
            batchParams.add(params)
        }

        sqlClient.executeBatch(sql, batchParams).onComplete { result ->
            if (result.failed()) {
                logger.warning("Error inserting batch history data: ${result.cause()?.message}")
            } else {
                logger.fine("Added ${messages.size} messages to archive")
            }
        }
    }

    override fun getHistory(
        topic: String,
        startTime: Instant?,
        endTime: Instant?,
        limit: Int
    ): JsonArray {
        logger.info("getHistory called with: topic=$topic, startTime=$startTime, endTime=$endTime, limit=$limit")
        
        val sql = StringBuilder("SELECT topic, time, payload_blob, payload_json, qos, retained, client_id, message_uuid FROM $tableName WHERE topic = ?")
        val params = JsonArray().add(topic)

        startTime?.let {
            sql.append(" AND time >= ?")
            // Use proper ISO-8601 format with proper precision
            val timeStr = it.toString()
            params.add(timeStr)
            logger.info("Added startTime parameter: $timeStr")
        }
        endTime?.let {
            sql.append(" AND time <= ?")
            val timeStr = it.toString()
            params.add(timeStr)
            logger.info("Added endTime parameter: $timeStr")
        }
        sql.append(" ORDER BY time DESC LIMIT ?")
        params.add(limit)
        
        logger.info("Executing SQL: ${sql.toString()}")
        logger.info("With parameters: $params")

        return try {
            logger.info("Starting direct JDBC query at ${System.currentTimeMillis()}...")
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
                        logger.info("Setting parameter at index $paramIndex: $param (type: ${param?.javaClass?.simpleName})")
                        when (param) {
                            is String -> preparedStatement.setString(paramIndex, param)
                            is Int -> preparedStatement.setInt(paramIndex, param)
                            is Long -> preparedStatement.setLong(paramIndex, param)
                            else -> preparedStatement.setString(paramIndex, param.toString())
                        }
                    }
                    
                    val resultSet = preparedStatement.executeQuery()
                    val queryDuration = System.currentTimeMillis() - startTime
                    logger.info("Direct JDBC query completed in ${queryDuration}ms")
                    
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
                            .put("payload_base64", Base64.getEncoder().encodeToString(resultSet.getBytes("payload_blob") ?: ByteArray(0)))
                            .put("payload_json", resultSet.getString("payload_json"))
                            .put("qos", resultSet.getInt("qos"))
                            .put("client_id", resultSet.getString("client_id") ?: "")
                            
                        messages.add(messageObj)
                    }
                    val processingDuration = System.currentTimeMillis() - processingStart
                    logger.info("Data processing took ${processingDuration}ms, processed $rowCount rows, returning ${messages.size()} messages")
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
            logger.fine("Executing SQLite query: $sql")
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
                
                logger.fine("SQLite query executed successfully with ${result.size()} rows")
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
        logger.warning("purgeOldMessages not yet implemented for SQLite message archive [$name]")
        // TODO: Implement message purging for SQLite archives
        return PurgeResult(0, 0)
    }
}