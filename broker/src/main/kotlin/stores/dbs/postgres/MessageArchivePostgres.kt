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
    private val payloadFormat: at.rocworks.stores.PayloadFormat = at.rocworks.stores.PayloadFormat.DEFAULT,
    private val schema: String? = null
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
                // Just verify connection works - table creation is deferred to createTable()
                connection.autoCommit = false

                // Create and set PostgreSQL schema if specified
                if (!schema.isNullOrBlank()) {
                    connection.createStatement().use { stmt ->
                        // Create schema if it doesn't exist
                        stmt.execute("CREATE SCHEMA IF NOT EXISTS \"$schema\"")
                        // Set search_path to the specified schema
                        stmt.execute("SET search_path TO \"$schema\", public")
                    }
                }

                connection.createStatement().use { statement ->
                    statement.executeQuery("SELECT 1")
                }
                connection.commit()
                logger.fine("Message store [$name] is ready [${Utils.getCurrentFunctionName()}]")
                promise.complete()
            } catch (e: Exception) {
                logger.severe("Error initializing connection for [$name]: ${e.message} [${Utils.getCurrentFunctionName()}]")
                promise.fail(e)
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
        logger.finer("PostgreSQL getHistory called with: topic=$topic, startTime=$startTime, endTime=$endTime, limit=$limit")
        val sql = StringBuilder("SELECT topic, time, payload_blob, payload_json, qos, retained, client_id, message_uuid FROM $tableName WHERE topic LIKE ?")
        val topicPattern = topic.replace("#", "%") // replace "#" wildcard with "%" for SQL LIKE
        val params = mutableListOf<Any>()
        params.add(topicPattern)

        startTime?.let {
            sql.append(" AND time >= ?")
            params.add(Timestamp.from(it))
            logger.finer("Added startTime parameter: $it")
        }
        endTime?.let {
            sql.append(" AND time <= ?")
            params.add(Timestamp.from(it))
            logger.finer("Added endTime parameter: $it")
        }
        sql.append(" ORDER BY time DESC LIMIT ?")
        params.add(limit)
        
        logger.finer("Executing PostgreSQL SQL: ${sql.toString()} with parameters: $params")

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
                    logger.finer("PostgreSQL query completed in ${queryDuration}ms")
                    
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
                    logger.finer("PostgreSQL data processing took ${processingDuration}ms, processed $rowCount rows, returning ${messages.size()} messages")
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
                logger.fine("Dropped table [$tableName] for message archive [$name]")
                true
            } ?: false
        } catch (e: SQLException) {
            logger.severe("Error dropping table [$tableName] for message archive [$name]: ${e.message}")
            false
        }
    }

    override suspend fun tableExists(): Boolean {
        return try {
            db.connection?.let { connection ->
                val sql = "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?"
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, tableName)
                    val resultSet = preparedStatement.executeQuery()
                    resultSet.next()
                }
            } ?: false
        } catch (e: SQLException) {
            logger.warning("Error checking if table [$tableName] exists: ${e.message}")
            false
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

        logger.fine { "PostgreSQL getAggregatedHistory: topics=$topics, interval=${intervalMinutes}min, functions=$functions, fields=$fields" }

        val result = JsonObject()
        val columns = JsonArray().add("timestamp")
        val rows = JsonArray()

        try {
            db.connection?.let { connection ->
                // Build column definitions and SELECT clauses
                val selectClauses = mutableListOf<String>()
                val columnNames = mutableListOf<String>()

                // Determine if we're using TimescaleDB time_bucket or standard date_trunc
                val hasTimescaleDB = try {
                    connection.createStatement().use { stmt ->
                        val rs = stmt.executeQuery("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")
                        rs.next()
                    }
                } catch (e: Exception) {
                    false
                }

                val bucketExpr = if (hasTimescaleDB) {
                    "time_bucket('$intervalMinutes minutes', time)"
                } else {
                    // For standard PostgreSQL, use date_trunc with appropriate precision
                    when (intervalMinutes) {
                        1 -> "date_trunc('minute', time)"
                        60 -> "date_trunc('hour', time)"
                        1440 -> "date_trunc('day', time)"
                        else -> {
                            // For 5, 15 minute intervals - truncate to interval boundary
                            "(date_trunc('hour', time) + (EXTRACT(minute FROM time)::int / $intervalMinutes * $intervalMinutes) * interval '1 minute')"
                        }
                    }
                }

                // Build aggregation expressions for each topic and field combination
                for (topic in topics) {
                    val topicAlias = topic.replace("/", "_").replace(".", "_")
                    val effectiveFields = if (fields.isEmpty()) listOf("") else fields

                    for (field in effectiveFields) {
                        val fieldAlias = if (field.isEmpty()) "" else ".${field.replace(".", "_")}"
                        val valueExpr = if (field.isEmpty()) {
                            // Raw value - try payload_json first, then decode payload_blob as UTF-8 text
                            // COALESCE tries payload_json first, if null tries converting payload_blob to text
                            "COALESCE((payload_json)::NUMERIC, (convert_from(payload_blob, 'UTF8'))::NUMERIC)"
                        } else {
                            // JSON field extraction - handle nested paths
                            val pathParts = field.split(".")
                            if (pathParts.size == 1) {
                                "(payload_json->>'$field')::NUMERIC"
                            } else {
                                // Nested path: payload_json->'level1'->'level2'->>'field'
                                val jsonPath = pathParts.dropLast(1).joinToString("->") { "'$it'" }
                                val lastField = pathParts.last()
                                "(payload_json->$jsonPath->>'$lastField')::NUMERIC"
                            }
                        }

                        for (func in functions) {
                            val funcLower = func.lowercase()
                            val colName = "$topic$fieldAlias" + "_$funcLower"
                            columnNames.add(colName)
                            columns.add(colName)

                            // Add CASE WHEN for topic filtering within the aggregation
                            selectClauses.add("${func.uppercase()}(CASE WHEN topic = ? THEN $valueExpr END) AS \"${colName.replace("\"", "\"\"")}\"")
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

                logger.fine { "Executing aggregation SQL: $sql" }

                connection.prepareStatement(sql).use { preparedStatement ->
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

                    // Set time range parameters
                    preparedStatement.setTimestamp(paramIndex++, Timestamp.from(startTime))
                    preparedStatement.setTimestamp(paramIndex, Timestamp.from(endTime))

                    val queryStart = System.currentTimeMillis()
                    val resultSet = preparedStatement.executeQuery()
                    val queryDuration = System.currentTimeMillis() - queryStart
                    logger.fine { "PostgreSQL aggregation query completed in ${queryDuration}ms" }

                    while (resultSet.next()) {
                        val row = JsonArray()
                        // Add timestamp
                        val bucket = resultSet.getTimestamp("bucket")
                        row.add(bucket?.toInstant()?.toString())

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
            } ?: run {
                logger.warning("No database connection available for aggregation query")
            }
        } catch (e: SQLException) {
            logger.severe("Error executing aggregation query: ${e.message}")
            e.printStackTrace()
        }

        result.put("columns", columns)
        result.put("rows", rows)
        logger.fine { "PostgreSQL aggregation returned ${rows.size()} rows with ${columns.size()} columns" }
        return result
    }

    override suspend fun createTable(): Boolean {
        return try {
            db.connection?.let { connection ->
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
                        logger.fine("TimescaleDB extension is available [${Utils.getCurrentFunctionName()}]")
                        val hypertableCheck =
                            statement.executeQuery("SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = '$tableName';")
                        if (!hypertableCheck.next()) {
                            // Check if table is empty before converting to hypertable
                            val countResult = statement.executeQuery("SELECT COUNT(*) as cnt FROM $tableName;")
                            val isEmpty = countResult.next() && countResult.getInt("cnt") == 0

                            if (isEmpty) {
                                logger.fine("Table $tableName is empty, converting to hypertable... [${Utils.getCurrentFunctionName()}]")
                                statement.executeQuery("SELECT create_hypertable('$tableName', 'time');")
                                logger.fine("Table $tableName converted to hypertable [${Utils.getCurrentFunctionName()}]")
                            } else {
                                logger.warning("Table $tableName is not empty - skipping hypertable conversion. To convert existing data, run: SELECT create_hypertable('$tableName', 'time', migrate_data => true); [${Utils.getCurrentFunctionName()}]")
                            }
                        } else {
                            logger.fine("Table $tableName is already a hypertable [${Utils.getCurrentFunctionName()}]")
                        }
                    } else {
                        logger.warning("TimescaleDB extension is not available [${Utils.getCurrentFunctionName()}]")
                    }
                }
                connection.commit()
                logger.fine("Message archive table [$name] initialized (created or already exists) [${Utils.getCurrentFunctionName()}]")
                true
            } ?: false
        } catch (e: Exception) {
            logger.warning("Error creating table: ${e.message}")
            false
        }
    }
}