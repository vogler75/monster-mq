package at.rocworks.stores.questdb

import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.data.PurgeResult
import at.rocworks.stores.DatabaseConnection
import at.rocworks.stores.IMessageArchiveExtended
import at.rocworks.stores.MessageArchiveType
import at.rocworks.stores.PayloadDecoder
import at.rocworks.stores.PayloadFormat
import io.questdb.client.QuestDB
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.net.URI
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import java.sql.Timestamp
import java.time.Instant
import java.util.Base64
import java.util.Calendar
import java.util.TimeZone

class MessageArchiveQuestDB(
    private val name: String,
    private val url: String,
    private val username: String? = null,
    private val password: String? = null,
    private val payloadFormat: PayloadFormat = PayloadFormat.DEFAULT
) : AbstractVerticle(), IMessageArchiveExtended {

    private val logger = Utils.getLogger(this::class.java, name)
    private val tableName = name.lowercase()
    private var lastAddHistoryError: Int = 0

    private val urls = parseQuestDbUrls(url, username, password)
    private val qwpConfig = urls.first
    private val jdbcUrl = urls.second

    private var questDbClient: QuestDB? = null

    private val db = object : DatabaseConnection(logger, jdbcUrl, username ?: "admin", password ?: "quest") {
        override fun init(connection: Connection): Future<Void> {
            val promise = Promise.promise<Void>()
            try {
                connection.autoCommit = true
                connection.createStatement().use { statement ->
                    statement.executeQuery("SELECT 1")
                }
                logger.info("QuestDB PGWire connection for [$name] is ready")
                promise.complete()
            } catch (e: Exception) {
                logger.warning("QuestDB PGWire initialization for [$name]: ${e.message}")
                promise.fail(e)
            }
            return promise.future()
        }
    }

    override fun getName(): String = name
    override fun getType() = MessageArchiveType.QUESTDB

    override fun getConnectionStatus(): Boolean = db.getConnectionStatus()

    override fun start(startPromise: Promise<Void>) {
        logger.info("Starting QuestDB message archive [$name] (QWP: $qwpConfig, JDBC: $jdbcUrl)")

        // Initialize QuestDB QWP client for ingestion
        try {
            questDbClient = QuestDB.connect(qwpConfig)
            logger.info("QuestDB QWP client connected for [$name]")
        } catch (e: Exception) {
            logger.warning("Could not pre-initialize QuestDB QWP client for [$name]: ${e.message}")
        }

        // Start background JDBC connection for DDL / queries
        db.start(vertx, startPromise)
    }

    override fun stopStore(): Future<Void> {
        return try {
            questDbClient?.close()
            questDbClient = null
            db.stop()
        } catch (e: Exception) {
            logger.fine { "Error closing QuestDB client: ${e.message}" }
            db.stop()
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        stopStore().onComplete { stopPromise.complete() }
    }

    private fun getOrCreateQuestDbClient(): QuestDB {
        questDbClient?.let { return it }
        val client = QuestDB.connect(qwpConfig)
        questDbClient = client
        return client
    }

    override fun addHistory(messages: List<BrokerMessage>) {
        if (messages.isEmpty()) return

        try {
            val client = getOrCreateQuestDbClient()
            val sender = client.borrowSender()
            try {
                for (message in messages) {
                    sender.table(tableName)
                        .symbol("topic", message.topicName)
                        .symbol("client_id", message.clientId)
                        .stringColumn("message_uuid", message.messageUuid)
                        .longColumn("qos", message.qosLevel.toLong())
                        .boolColumn("retained", message.isRetain)

                    if (payloadFormat == PayloadFormat.JSON) {
                        val payloadJson = message.getPayloadAsJson()
                        if (payloadJson != null) {
                            sender.stringColumn("payload_json", payloadJson)
                            sender.stringColumn("payload_b64", "")
                        } else {
                            sender.stringColumn("payload_json", "")
                            sender.stringColumn("payload_b64", message.getPayloadAsBase64())
                        }
                    } else {
                        sender.stringColumn("payload_json", "")
                        sender.stringColumn("payload_b64", message.getPayloadAsBase64())
                    }

                    sender.at(message.time)
                }
                sender.flush()
            } finally {
                sender.close()
            }
        } catch (e: Exception) {
            val now = (System.currentTimeMillis() / 1000).toInt()
            if (now > lastAddHistoryError + 5) {
                logger.warning("Error writing batch to QuestDB archive [$name]: ${e.message}")
                lastAddHistoryError = now
            }
        }
    }

    override suspend fun createTable(): Boolean {
        return try {
            val connection = db.connection ?: return false
            val sql = """
                CREATE TABLE IF NOT EXISTS $tableName (
                    topic SYMBOL,
                    timestamp TIMESTAMP,
                    payload_b64 VARCHAR,
                    payload_json VARCHAR,
                    qos INT,
                    retained BOOLEAN,
                    client_id SYMBOL,
                    message_uuid VARCHAR
                ) TIMESTAMP(timestamp) PARTITION BY DAY WAL DEDUP UPSERT KEYS(timestamp, topic);
            """.trimIndent()

            connection.createStatement().use { statement ->
                statement.execute(sql)
            }
            logger.info("QuestDB table [$tableName] created/verified with TIMESTAMP(timestamp) PARTITION BY DAY")
            true
        } catch (e: SQLException) {
            logger.severe("Error creating QuestDB table [$tableName]: ${e.message}")
            false
        }
    }

    override suspend fun tableExists(): Boolean {
        return try {
            val connection = db.connection ?: return false
            val checkSql = "SELECT 1 FROM tables() WHERE table_name = ?"
            connection.prepareStatement(checkSql).use { stmt ->
                stmt.setString(1, tableName)
                val rs = stmt.executeQuery()
                rs.next()
            }
        } catch (e: SQLException) {
            // Fallback for older versions or information_schema
            try {
                val connection = db.connection ?: return false
                val checkSql = "SELECT 1 FROM information_schema.tables WHERE table_name = ?"
                connection.prepareStatement(checkSql).use { stmt ->
                    stmt.setString(1, tableName)
                    val rs = stmt.executeQuery()
                    rs.next()
                }
            } catch (e2: Exception) {
                logger.finer("Could not check if table exists: ${e2.message}")
                false
            }
        }
    }

    override fun dropStorage(): Boolean {
        return try {
            db.connection?.let { connection ->
                val sql = "DROP TABLE IF EXISTS $tableName"
                connection.createStatement().use { stmt ->
                    stmt.execute(sql)
                }
                logger.info("Dropped table [$tableName] for QuestDB archive [$name]")
                true
            } ?: false
        } catch (e: SQLException) {
            logger.severe("Error dropping table [$tableName]: ${e.message}")
            false
        }
    }

    private fun utcCal(): Calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))

    private fun getTimestampColumn(connection: Connection): String {
        return try {
            val sql = "SELECT \"column\" FROM table_columns(?) WHERE designated = true"
            connection.prepareStatement(sql).use { stmt ->
                stmt.setString(1, tableName)
                val rs = stmt.executeQuery()
                if (rs.next()) {
                    rs.getString(1) ?: "timestamp"
                } else {
                    "timestamp"
                }
            }
        } catch (_: Exception) {
            "timestamp"
        }
    }

    override fun getHistory(
        topic: String,
        startTime: Instant?,
        endTime: Instant?,
        limit: Int
    ): JsonArray {
        val messages = JsonArray()

        try {
            val startMs = System.currentTimeMillis()
            db.connection?.let { connection ->
                val tsCol = getTimestampColumn(connection)
                val sql = StringBuilder("SELECT topic, $tsCol, payload_b64, payload_json, qos, retained, client_id, message_uuid FROM $tableName WHERE topic LIKE ?")
                val topicPattern = topic.replace("#", "%").replace("+", "%")
                val params = mutableListOf<Any>(topicPattern)

                if (startTime != null) {
                    sql.append(" AND $tsCol >= ?")
                    params.add(Timestamp.from(startTime))
                }
                if (endTime != null) {
                    sql.append(" AND $tsCol <= ?")
                    params.add(Timestamp.from(endTime))
                }
                sql.append(" ORDER BY $tsCol DESC LIMIT ?")
                params.add(limit)

                connection.prepareStatement(sql.toString()).use { preparedStatement ->
                    for ((index, param) in params.withIndex()) {
                        when (param) {
                            is String -> preparedStatement.setString(index + 1, param)
                            is Timestamp -> preparedStatement.setTimestamp(index + 1, param, utcCal())
                            is Int -> preparedStatement.setInt(index + 1, param)
                            else -> preparedStatement.setObject(index + 1, param)
                        }
                    }
                    val resultSet = preparedStatement.executeQuery()
                    val queryDuration = System.currentTimeMillis() - startMs
                    logger.finer("QuestDB query completed in ${queryDuration}ms")

                    var rowCount = 0
                    while (resultSet.next()) {
                        rowCount++
                        val ts = resultSet.getTimestamp(tsCol, utcCal())
                        val messageObj = JsonObject()
                            .put("topic", resultSet.getString("topic") ?: topic)
                            .put("timestamp", ts?.toInstant()?.toEpochMilli() ?: 0L)
                            .put("qos", resultSet.getInt("qos"))
                            .put("client_id", resultSet.getString("client_id") ?: "")

                        val payloadJson = resultSet.getString("payload_json")
                        val payloadB64 = resultSet.getString("payload_b64")
                        val rawBytes = if (!payloadB64.isNullOrEmpty()) {
                            try { Base64.getDecoder().decode(payloadB64) } catch (_: Exception) { null }
                        } else null

                        PayloadDecoder
                            .decode(if (payloadJson.isNullOrEmpty()) null else payloadJson, rawBytes)
                            .applyTo(messageObj)

                        messages.add(messageObj)
                    }
                }
            }
        } catch (e: SQLException) {
            logger.severe("Error retrieving history from QuestDB for topic [$topic]: ${e.message}")
        }
        return messages
    }

    override fun executeQuery(sql: String): JsonArray {
        return try {
            logger.fine { "Executing QuestDB SQL query: $sql" }
            db.connection?.let { connection ->
                connection.createStatement().use { statement ->
                    statement.executeQuery(sql).use { resultSet ->
                        val metaData = resultSet.metaData
                        val columnCount = metaData.columnCount
                        val result = JsonArray()

                        val header = JsonArray()
                        for (i in 1..columnCount) {
                            header.add(metaData.getColumnName(i))
                        }
                        result.add(header)

                        while (resultSet.next()) {
                            val row = JsonArray()
                            for (i in 1..columnCount) {
                                row.add(resultSet.getObject(i))
                            }
                            result.add(row)
                        }
                        result
                    }
                }
            } ?: run {
                logger.warning("No QuestDB database connection available.")
                JsonArray().add("No database connection available.")
            }
        } catch (e: SQLException) {
            logger.severe("Error executing QuestDB query: ${e.message}")
            JsonArray().add("Error executing query: ${e.message}")
        }
    }

    override fun purgeOldMessages(olderThan: Instant): PurgeResult {
        val startTime = System.currentTimeMillis()
        logger.fine { "Purging QuestDB archive [$name] older than $olderThan" }
        return try {
            db.connection?.let { connection ->
                val tsCol = getTimestampColumn(connection)
                val sql = "DELETE FROM $tableName WHERE $tsCol < ?"
                val deleted = connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setTimestamp(1, Timestamp.from(olderThan), utcCal())
                    preparedStatement.executeUpdate()
                }
                val elapsedTime = System.currentTimeMillis() - startTime
                PurgeResult(deletedCount = deleted, elapsedTimeMs = elapsedTime)
            } ?: PurgeResult(deletedCount = 0, elapsedTimeMs = System.currentTimeMillis() - startTime)
        } catch (e: SQLException) {
            logger.severe("Error purging old messages from QuestDB: ${e.message}")
            PurgeResult(deletedCount = 0, elapsedTimeMs = System.currentTimeMillis() - startTime)
        }
    }

    override fun getArchiveStats(startTime: Instant?, endTime: Instant?): JsonObject {
        val result = JsonObject()
            .put("minTimestamp", null as String?)
            .put("dailyCounts", JsonArray())

        try {
            db.connection?.let { connection ->
                val tsCol = getTimestampColumn(connection)
                val minSql = "SELECT min($tsCol) as min_ts FROM $tableName"
                connection.createStatement().use { stmt ->
                    val rs = stmt.executeQuery(minSql)
                    if (rs.next()) {
                        val minTs = rs.getTimestamp("min_ts", utcCal())
                        if (minTs != null) {
                            result.put("minTimestamp", minTs.toInstant().toString())
                        }
                    }
                }

                val dailyCounts = JsonArray()
                val countsSql = StringBuilder(
                    "SELECT to_str($tsCol, 'yyyy-MM-dd') as day, count() as cnt FROM $tableName WHERE 1=1"
                )
                val params = mutableListOf<Timestamp>()
                if (startTime != null) {
                    countsSql.append(" AND $tsCol >= ?")
                    params.add(Timestamp.from(startTime))
                }
                if (endTime != null) {
                    countsSql.append(" AND $tsCol <= ?")
                    params.add(Timestamp.from(endTime))
                }
                countsSql.append(" GROUP BY day ORDER BY day ASC")

                connection.prepareStatement(countsSql.toString()).use { stmt ->
                    for ((idx, param) in params.withIndex()) {
                        stmt.setTimestamp(idx + 1, param, utcCal())
                    }
                    val rs = stmt.executeQuery()
                    while (rs.next()) {
                        val dayStr = rs.getString("day")
                        val count = rs.getLong("cnt")
                        if (dayStr != null) {
                            dailyCounts.add(JsonObject().put("date", dayStr).put("count", count))
                        }
                    }
                }
                result.put("dailyCounts", dailyCounts)
            }
        } catch (e: SQLException) {
            logger.warning("Error fetching QuestDB archive stats for [$name]: ${e.message}")
        }
        return result
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

        val result = JsonObject()
        val columns = JsonArray().add("timestamp")
        val rows = JsonArray()

        try {
            db.connection?.let { connection ->
                val tsCol = getTimestampColumn(connection)
                val selectClauses = mutableListOf<String>()
                val columnNames = mutableListOf<String>()

                val sampleUnit = when {
                    intervalMinutes % 1440 == 0 -> "${intervalMinutes / 1440}d"
                    intervalMinutes % 60 == 0 -> "${intervalMinutes / 60}h"
                    else -> "${intervalMinutes}m"
                }

                for (topic in topics) {
                    val effectiveFields = if (fields.isEmpty()) listOf("") else fields
                    for (field in effectiveFields) {
                        val fieldAlias = if (field.isEmpty()) "" else ".${field.replace(".", "_")}"
                        val valueExpr = if (field.isEmpty()) {
                            "payload_json::double"
                        } else {
                            "json_extract(payload_json, '$.$field')::double"
                        }

                        for (func in functions) {
                            val funcLower = func.lowercase()
                            val colName = "$topic$fieldAlias" + "_$funcLower"
                            columnNames.add(colName)
                            columns.add(colName)
                            selectClauses.add("${func.uppercase()}(CASE WHEN topic = '$topic' THEN $valueExpr END) AS \"$colName\"")
                        }
                    }
                }

                val sql = """
                    SELECT $tsCol, ${selectClauses.joinToString(", ")}
                    FROM $tableName
                    WHERE $tsCol >= ? AND $tsCol <= ?
                    SAMPLE BY $sampleUnit ALIGN TO CALENDAR
                """.trimIndent()

                connection.prepareStatement(sql).use { stmt ->
                    stmt.setTimestamp(1, Timestamp.from(startTime), utcCal())
                    stmt.setTimestamp(2, Timestamp.from(endTime), utcCal())
                    val rs = stmt.executeQuery()
                    while (rs.next()) {
                        val row = JsonArray()
                        val bucketTs = rs.getTimestamp(tsCol, utcCal())
                        row.add(bucketTs?.toInstant()?.toString())
                        for (col in columnNames) {
                            val v = rs.getObject(col)
                            row.add(if (rs.wasNull()) null else v)
                        }
                        rows.add(row)
                    }
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error in QuestDB getAggregatedHistory for [$name]: ${e.message}")
        }

        return result.put("columns", columns).put("rows", rows)
    }

    companion object {
        fun parseQuestDbUrls(rawUrl: String, user: String?, pass: String?): Pair<String, String> {
            val trimmed = rawUrl.trim()
            if (trimmed.startsWith("ws::") || trimmed.startsWith("http::") || trimmed.startsWith("tcp::")) {
                val hostPort = trimmed.substringAfter("addr=").substringBefore(";").trim()
                val host = hostPort.substringBefore(":")
                val jdbc = "jdbc:postgresql://$host:8812/qdb"
                return trimmed to jdbc
            }

            if (trimmed.startsWith("jdbc:postgresql://")) {
                val hostPart = trimmed.removePrefix("jdbc:postgresql://").substringBefore("/").substringBefore("?")
                val host = hostPart.substringBefore(":")
                val qwp = "ws::addr=$host:9000;"
                return qwp to trimmed
            }

            if (trimmed.contains("://")) {
                return try {
                    val uri = URI(trimmed)
                    val host = uri.host ?: "localhost"
                    val port = if (uri.port > 0) uri.port else 9000
                    val qwp = "ws::addr=$host:$port;"
                    val jdbc = "jdbc:postgresql://$host:8812/qdb"
                    qwp to jdbc
                } catch (_: Exception) {
                    "ws::addr=localhost:9000;" to "jdbc:postgresql://localhost:8812/qdb"
                }
            }

            val host = trimmed.substringBefore(":")
            val portStr = if (trimmed.contains(":")) trimmed.substringAfter(":") else ""
            val port = portStr.toIntOrNull() ?: 9000

            val qwpPort = if (port == 8812) 9000 else port
            val jdbcPort = if (port == 8812) 8812 else 8812

            val qwp = "ws::addr=$host:$qwpPort;"
            val jdbc = "jdbc:postgresql://$host:$jdbcPort/qdb"
            return qwp to jdbc
        }
    }
}
