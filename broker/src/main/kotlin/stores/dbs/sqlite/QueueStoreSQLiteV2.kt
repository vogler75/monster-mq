package at.rocworks.stores.sqlite

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.IQueueStoreSync
import at.rocworks.stores.QueueStoreType
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/**
 * Single-table queue store with visibility timeout (PGMQ-inspired).
 *
 * Key differences from V1:
 * - Single table (messagequeue) — payload duplicated per subscriber
 * - Visibility timeout (vt) replaces integer status
 * - ACK = DELETE (no mark-delivered + purge cycle)
 * - No orphan cleanup needed
 * - Uses AUTOINCREMENT msg_id for FIFO ordering
 */
class QueueStoreSQLiteV2(
    private val dbPath: String,
    private val visibilityTimeoutSeconds: Int = 30
) : AbstractVerticle(), IQueueStoreSync {
    private val logger = Utils.getLogger(this::class.java)
    private lateinit var sqlClient: SQLiteClient

    private val tableName = "messagequeue"


    override fun getType(): QueueStoreType = QueueStoreType.SQLITE_V2

    override fun start(startPromise: Promise<Void>) {
        sqlClient = SQLiteClient(vertx, dbPath)

        val createTableSQL = JsonArray()
            .add("""
            CREATE TABLE IF NOT EXISTS $tableName (
                msg_id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_uuid TEXT NOT NULL,
                client_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                payload BLOB,
                qos INTEGER NOT NULL,
                retained INTEGER NOT NULL DEFAULT 0,
                publisher_id TEXT,
                creation_time INTEGER NOT NULL,
                message_expiry_interval INTEGER,
                vt INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                read_ct INTEGER NOT NULL DEFAULT 0
            );
            """.trimIndent())
            .add("CREATE INDEX IF NOT EXISTS ${tableName}_fetch_idx ON $tableName (client_id, vt);")
            .add("CREATE INDEX IF NOT EXISTS ${tableName}_client_uuid_idx ON $tableName (client_id, message_uuid);")

        sqlClient.initDatabase(createTableSQL).onComplete { result ->
            if (result.succeeded()) {
                logger.info("SQLite V2 queue table '$tableName' is ready")
                startPromise.complete()
            } else {
                logger.severe("Failed to initialize SQLite V2 queue table: ${result.cause()?.message}")
                startPromise.fail(result.cause())
            }
        }
    }

    // -------------------------------------------------------------------------
    // Enqueue: one row per (message, client) — payload duplicated
    // -------------------------------------------------------------------------

    override fun enqueueMessages(messages: List<Pair<BrokerMessage, List<String>>>) {
        if (messages.isEmpty()) return

        val sql = """INSERT INTO $tableName
            (message_uuid, client_id, topic, payload, qos, retained, publisher_id,
             creation_time, message_expiry_interval)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""

        val batch = JsonArray()
        messages.forEach { (message, clientIds) ->
            clientIds.forEach { clientId ->
                batch.add(JsonArray()
                    .add(message.messageUuid)
                    .add(clientId)
                    .add(message.topicName)
                    .add(message.payload)
                    .add(message.qosLevel)
                    .add(if (message.isRetain) 1 else 0)
                    .add(message.clientId)
                    .add(message.time.toEpochMilli())
                    .add(message.messageExpiryInterval)
                )
            }
        }

        sqlClient.executeBatch(sql, batch)
        logger.fine { "Enqueued ${messages.size} messages for ${messages.sumOf { it.second.size }} clients" }
    }

    // -------------------------------------------------------------------------
    // Dequeue: iterate visible messages for a client
    // -------------------------------------------------------------------------

    override fun dequeueMessages(clientId: String, callback: (BrokerMessage) -> Boolean) {
        val now = System.currentTimeMillis() / 1000
        val sql = """SELECT msg_id, message_uuid, topic, payload, qos, retained, publisher_id,
                            creation_time, message_expiry_interval
                     FROM $tableName
                     WHERE client_id = ? AND vt <= ?
                       AND (message_expiry_interval IS NULL
                            OR ((? - creation_time) / 1000) < message_expiry_interval)
                     ORDER BY msg_id ASC"""

        val currentTimeMillis = System.currentTimeMillis()
        val params = JsonArray().add(clientId).add(now).add(currentTimeMillis)

        try {
            val results = sqlClient.executeQuerySync(sql, params)
            results.forEach { row ->
                val rowObj = row as JsonObject
                val continueProcessing = callback(rowToMessage(rowObj, currentTimeMillis))
                if (!continueProcessing) return
            }
        } catch (e: Exception) {
            logger.warning("Error dequeuing messages: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
    }

    // -------------------------------------------------------------------------
    // Remove messages
    // -------------------------------------------------------------------------

    override fun removeMessages(messages: List<Pair<String, String>>) {
        if (messages.isEmpty()) return

        val sql = "DELETE FROM $tableName WHERE client_id = ? AND message_uuid = ?"
        val batch = JsonArray()
        messages.forEach { (clientId, messageUuid) ->
            batch.add(JsonArray().add(clientId).add(messageUuid))
        }
        sqlClient.executeBatch(sql, batch)
    }

    // -------------------------------------------------------------------------
    // Fetch pending messages
    // -------------------------------------------------------------------------

    override fun fetchNextPendingMessage(clientId: String): BrokerMessage? {
        return fetchPendingMessages(clientId, 1).firstOrNull()
    }

    override fun fetchPendingMessages(clientId: String, limit: Int): List<BrokerMessage> {
        val now = System.currentTimeMillis() / 1000
        val currentTimeMillis = System.currentTimeMillis()
        val sql = """SELECT msg_id, message_uuid, topic, payload, qos, retained, publisher_id,
                            creation_time, message_expiry_interval
                     FROM $tableName
                     WHERE client_id = ? AND vt <= ?
                       AND (message_expiry_interval IS NULL
                            OR ((? - creation_time) / 1000) < message_expiry_interval)
                     ORDER BY msg_id ASC
                     LIMIT ?"""

        val params = JsonArray().add(clientId).add(now).add(currentTimeMillis).add(limit)

        return try {
            val results = sqlClient.executeQuerySync(sql, params)
            results.map { rowToMessage(it as JsonObject, currentTimeMillis) }
        } catch (e: Exception) {
            logger.warning("Error fetching pending messages: ${e.message} [${Utils.getCurrentFunctionName()}]")
            emptyList()
        }
    }

    // -------------------------------------------------------------------------
    // Atomic fetch-and-lock: fetch visible, then set vt into the future
    // -------------------------------------------------------------------------

    override fun fetchAndLockPendingMessages(clientId: String, limit: Int): List<BrokerMessage> {
        val now = System.currentTimeMillis() / 1000
        val currentTimeMillis = System.currentTimeMillis()
        val vtFuture = now + visibilityTimeoutSeconds

        val fetchSql = """SELECT msg_id, message_uuid, topic, payload, qos, retained, publisher_id,
                                 creation_time, message_expiry_interval
                          FROM $tableName
                          WHERE client_id = ? AND vt <= ?
                            AND (message_expiry_interval IS NULL
                                 OR ((? - creation_time) / 1000) < message_expiry_interval)
                          ORDER BY msg_id ASC
                          LIMIT ?"""

        val fetchParams = JsonArray().add(clientId).add(now).add(currentTimeMillis).add(limit)

        return try {
            val results = sqlClient.executeQuerySync(fetchSql, fetchParams)
            if (results.isEmpty) return emptyList()

            val messages = results.map { rowToMessage(it as JsonObject, currentTimeMillis) }
            val msgIds = (0 until results.size()).map { results.getJsonObject(it).getLong("msg_id") }

            // Mark as in-flight by setting vt into the future
            val placeholders = msgIds.joinToString(",") { "?" }
            val lockSql = "UPDATE $tableName SET vt = ?, read_ct = read_ct + 1 WHERE msg_id IN ($placeholders)"
            val lockParams = JsonArray().add(vtFuture)
            msgIds.forEach { lockParams.add(it) }
            sqlClient.executeUpdateSync(lockSql, lockParams)

            messages
        } catch (e: Exception) {
            logger.warning("Error in fetch-and-lock: ${e.message} [${Utils.getCurrentFunctionName()}]")
            emptyList()
        }
    }

    // -------------------------------------------------------------------------
    // Mark in-flight: extend visibility timeout
    // -------------------------------------------------------------------------

    override fun markMessageInFlight(clientId: String, messageUuid: String) {
        val now = System.currentTimeMillis() / 1000
        val vtFuture = now + visibilityTimeoutSeconds
        val sql = "UPDATE $tableName SET vt = ?, read_ct = read_ct + 1 WHERE client_id = ? AND message_uuid = ? AND vt <= ?"
        val params = JsonArray().add(vtFuture).add(clientId).add(messageUuid).add(now)
        try {
            sqlClient.executeUpdateSync(sql, params)
        } catch (e: Exception) {
            logger.warning("Error marking message in-flight: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun markMessagesInFlight(clientId: String, messageUuids: List<String>) {
        if (messageUuids.isEmpty()) return
        val now = System.currentTimeMillis() / 1000
        val vtFuture = now + visibilityTimeoutSeconds
        val placeholders = messageUuids.joinToString(",") { "?" }
        val sql = "UPDATE $tableName SET vt = ?, read_ct = read_ct + 1 WHERE client_id = ? AND message_uuid IN ($placeholders) AND vt <= ?"
        val params = JsonArray().add(vtFuture).add(clientId)
        messageUuids.forEach { params.add(it) }
        params.add(now)
        try {
            sqlClient.executeUpdateSync(sql, params)
        } catch (e: Exception) {
            logger.warning("Error marking messages in-flight: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
    }

    // -------------------------------------------------------------------------
    // ACK = DELETE
    // -------------------------------------------------------------------------

    override fun markMessageDelivered(clientId: String, messageUuid: String) {
        val sql = "DELETE FROM $tableName WHERE client_id = ? AND message_uuid = ?"
        val params = JsonArray().add(clientId).add(messageUuid)
        try {
            sqlClient.executeUpdateSync(sql, params)
        } catch (e: Exception) {
            logger.warning("Error deleting delivered message: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
    }

    // -------------------------------------------------------------------------
    // Reset: make in-flight messages visible again
    // -------------------------------------------------------------------------

    override fun resetInFlightMessages(clientId: String) {
        val now = System.currentTimeMillis() / 1000
        val sql = "UPDATE $tableName SET vt = ?, read_ct = 0 WHERE client_id = ? AND vt > ?"
        val params = JsonArray().add(now).add(clientId).add(now)
        try {
            sqlClient.executeUpdateSync(sql, params)
        } catch (e: Exception) {
            logger.warning("Error resetting in-flight messages: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
    }

    // -------------------------------------------------------------------------
    // Purge: no-ops in V2 (ACK = DELETE, no orphans)
    // -------------------------------------------------------------------------

    override fun purgeDeliveredMessages(): Int = 0

    override fun purgeExpiredMessages(): Int {
        val currentTimeMillis = System.currentTimeMillis()
        val sql = """DELETE FROM $tableName
                     WHERE message_expiry_interval IS NOT NULL
                       AND creation_time IS NOT NULL
                       AND ((? - creation_time) / 1000) >= message_expiry_interval"""
        val params = JsonArray().add(currentTimeMillis)
        return try {
            val count = sqlClient.executeUpdateSync(sql, params)
            if (count > 0) logger.fine { "Purged $count expired messages" }
            count
        } catch (e: Exception) {
            logger.warning("Error purging expired messages: ${e.message} [${Utils.getCurrentFunctionName()}]")
            0
        }
    }

    override fun purgeQueuedMessages() {
        // No-op: single-table design has no orphaned messages
    }

    // -------------------------------------------------------------------------
    // Delete all messages for a client
    // -------------------------------------------------------------------------

    override fun deleteClientMessages(clientId: String) {
        val sql = "DELETE FROM $tableName WHERE client_id = ?"
        val params = JsonArray().add(clientId)
        try {
            sqlClient.executeUpdateSync(sql, params)
        } catch (e: Exception) {
            logger.warning("Error deleting client messages: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
    }

    // -------------------------------------------------------------------------
    // Count
    // -------------------------------------------------------------------------

    override fun countQueuedMessages(): Long {
        val sql = "SELECT COUNT(*) FROM $tableName"
        return try {
            val results = sqlClient.executeQuerySync(sql)
            if (results.size() > 0) {
                results.getJsonObject(0).getLong("COUNT(*)") ?: 0L
            } else 0L
        } catch (e: Exception) {
            logger.warning("Error counting queued messages: ${e.message} [${Utils.getCurrentFunctionName()}]")
            0L
        }
    }

    override fun countQueuedMessagesForClient(clientId: String): Long {
        val now = System.currentTimeMillis() / 1000
        val sql = "SELECT COUNT(*) FROM $tableName WHERE client_id = ? AND vt <= ?"
        val params = JsonArray().add(clientId).add(now)
        return try {
            val results = sqlClient.executeQuerySync(sql, params)
            if (results.size() > 0) {
                results.getJsonObject(0).getLong("COUNT(*)") ?: 0L
            } else 0L
        } catch (e: Exception) {
            logger.warning("Error counting queued messages for client $clientId: ${e.message} [${Utils.getCurrentFunctionName()}]")
            0L
        }
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private fun rowToMessage(row: JsonObject, currentTimeMillis: Long): BrokerMessage {
        val messageExpiryIntervalRaw = row.getLong("message_expiry_interval")
        val messageExpiryInterval = if (messageExpiryIntervalRaw == null || messageExpiryIntervalRaw == -1L) null else messageExpiryIntervalRaw
        val creationTime = row.getLong("creation_time") ?: currentTimeMillis
        return BrokerMessage(
            messageUuid = row.getString("message_uuid"),
            messageId = 0,
            topicName = row.getString("topic"),
            payload = row.getBinary("payload") ?: ByteArray(0),
            qosLevel = row.getInteger("qos"),
            isRetain = row.getInteger("retained", 0) == 1,
            isDup = false,
            isQueued = true,
            clientId = row.getString("publisher_id"),
            time = java.time.Instant.ofEpochMilli(creationTime),
            messageExpiryInterval = messageExpiryInterval
        )
    }
}
