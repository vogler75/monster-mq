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
                logger.finer { "Added ${messages.size} messages to archive" }
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

    override fun getAggregatedHistory(
        topics: List<String>,
        startTime: Instant,
        endTime: Instant,
        intervalMinutes: Int,
        functions: List<String>,
        fields: List<String>
    ): JsonObject {
        if (topics.isEmpty()) {
            return JsonObject()
                .put("columns", JsonArray().add("timestamp"))
                .put("rows", JsonArray())
        }

        logger.fine { "SQLite getAggregatedHistory: topics=$topics, interval=${intervalMinutes}min, functions=$functions, fields=$fields" }

        val result = JsonObject()
        val columns = JsonArray().add("timestamp")
        val rows = JsonArray()

        try {
            // Use direct JDBC connection for complex queries
            val connection = java.sql.DriverManager.getConnection("jdbc:sqlite:$dbPath", "", "")

            connection.use { conn ->
                // Build column definitions and SELECT clauses
                val selectClauses = mutableListOf<String>()
                val columnNames = mutableListOf<String>()

                // SQLite time bucketing using strftime
                // strftime returns string, so we build bucket expressions
                val bucketExpr = when (intervalMinutes) {
                    1 -> "strftime('%Y-%m-%dT%H:%M:00Z', time)"
                    5 -> "strftime('%Y-%m-%dT%H:', time) || printf('%02d', (CAST(strftime('%M', time) AS INTEGER) / 5) * 5) || ':00Z'"
                    15 -> "strftime('%Y-%m-%dT%H:', time) || printf('%02d', (CAST(strftime('%M', time) AS INTEGER) / 15) * 15) || ':00Z'"
                    60 -> "strftime('%Y-%m-%dT%H:00:00Z', time)"
                    1440 -> "strftime('%Y-%m-%dT00:00:00Z', time)"
                    else -> "strftime('%Y-%m-%dT%H:', time) || printf('%02d', (CAST(strftime('%M', time) AS INTEGER) / $intervalMinutes) * $intervalMinutes) || ':00Z'"
                }

                // Build aggregation expressions for each topic and field combination
                for (topic in topics) {
                    val effectiveFields = if (fields.isEmpty()) listOf("") else fields

                    for (field in effectiveFields) {
                        val fieldAlias = if (field.isEmpty()) "" else ".${field.replace(".", "_")}"
                        val valueExpr = if (field.isEmpty()) {
                            // Raw value - try payload_json first, then decode payload_blob as UTF-8 text
                            // COALESCE tries payload_json first, if null tries converting payload_blob (BLOB) to text
                            // SQLite's CAST on BLOB returns the bytes as text (UTF-8)
                            "COALESCE(CAST(payload_json AS REAL), CAST(CAST(payload_blob AS TEXT) AS REAL))"
                        } else {
                            // JSON field extraction - SQLite uses json_extract
                            "CAST(json_extract(payload_json, '\$.${field.replace(".", ".")}') AS REAL)"
                        }

                        for (func in functions) {
                            val funcLower = func.lowercase()
                            val colName = "$topic$fieldAlias" + "_$funcLower"
                            columnNames.add(colName)
                            columns.add(colName)

                            // SQLite aggregate with CASE WHEN for topic filtering
                            val sqlFunc = when (func.uppercase()) {
                                "AVG" -> "AVG"
                                "MIN" -> "MIN"
                                "MAX" -> "MAX"
                                "COUNT" -> "COUNT"
                                else -> "AVG"
                            }
                            selectClauses.add("$sqlFunc(CASE WHEN topic = ? THEN $valueExpr END) AS \"${colName.replace("\"", "\"\"")}\"")
                        }
                    }
                }

                // Build the SQL query
                val topicPlaceholders = topics.joinToString(", ") { "?" }
                val sql = """
                    SELECT
                        $bucketExpr AS bucket,
                        ${selectClauses.joinToString(",\n                        ")}
                    FROM $tableName
                    WHERE topic IN ($topicPlaceholders)
                      AND time >= ? AND time <= ?
                    GROUP BY bucket
                    ORDER BY bucket ASC
                """.trimIndent()

                logger.fine { "Executing SQLite aggregation SQL: $sql" }

                conn.prepareStatement(sql).use { preparedStatement ->
                    var paramIndex = 1

                    // Set topic parameters for CASE WHEN expressions
                    val effectiveFields = if (fields.isEmpty()) listOf("") else fields
                    for (topic in topics) {
                        for (field in effectiveFields) {
                            for (func in functions) {
                                preparedStatement.setString(paramIndex++, topic)
                            }
                        }
                    }

                    // Set topic parameters for IN clause
                    for (topic in topics) {
                        preparedStatement.setString(paramIndex++, topic)
                    }

                    // Set time range parameters (SQLite stores time as ISO string)
                    preparedStatement.setString(paramIndex++, startTime.toString())
                    preparedStatement.setString(paramIndex, endTime.toString())

                    val queryStart = System.currentTimeMillis()
                    val resultSet = preparedStatement.executeQuery()
                    val queryDuration = System.currentTimeMillis() - queryStart
                    logger.fine { "SQLite aggregation query completed in ${queryDuration}ms" }

                    while (resultSet.next()) {
                        val row = JsonArray()
                        // Add timestamp
                        val bucket = resultSet.getString("bucket")
                        row.add(bucket)

                        // Add aggregated values
                        for (colName in columnNames) {
                            val value = resultSet.getObject(colName)
                            if (value == null || resultSet.wasNull()) {
                                row.addNull()
                            } else {
                                row.add((value as Number).toDouble())
                            }
                        }
                        rows.add(row)
                    }
                }
            }
        } catch (e: Exception) {
            logger.severe("Error executing SQLite aggregation query: ${e.message}")
            e.printStackTrace()
        }

        result.put("columns", columns)
        result.put("rows", rows)
        logger.fine { "SQLite aggregation returned ${rows.size()} rows with ${columns.size()} columns" }
        return result
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