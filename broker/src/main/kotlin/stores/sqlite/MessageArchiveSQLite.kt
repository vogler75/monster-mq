package at.rocworks.stores.sqlite

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.stores.IMessageArchiveExtended
import at.rocworks.stores.MessageArchiveType
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant

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
        val sql = StringBuilder("SELECT time, payload_blob FROM $tableName WHERE topic = ?")
        val params = JsonArray().add(topic)

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

        return try {
            val results = sqlClient.executeQuerySync(sql.toString(), params)
            results.forEach { row ->
                val rowObj = row as JsonObject
                val timeStr = rowObj.getString("time")
                val payloadBlob = rowObj.getBinary("payload_blob") ?: ByteArray(0)
                messages.add(JsonArray().add(timeStr).add(payloadBlob.toString(Charsets.UTF_8)))
            }
            messages
        } catch (e: Exception) {
            logger.severe("Error retrieving history for topic [$topic]: ${e.message}")
            JsonArray().add(JsonArray().add("time").add("payload")) // Return header only on error
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
}