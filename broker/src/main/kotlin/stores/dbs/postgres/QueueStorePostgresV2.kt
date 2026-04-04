package at.rocworks.stores.postgres

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.DatabaseConnection
import at.rocworks.stores.IQueueStoreSync
import at.rocworks.stores.QueueStoreType
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import java.sql.*

/**
 * PGMQ-inspired single-table queue store with visibility timeout.
 *
 * Key differences from V1:
 * - Single table (no separate client mapping table)
 * - Payload duplicated per subscriber (no JOINs)
 * - Visibility timeout (vt) replaces integer status column
 * - ACK = DELETE (no mark-delivered + purge cycle)
 * - msg_id BIGINT IDENTITY for FIFO ordering (no UUID sorting)
 */
class QueueStorePostgresV2(
    private val url: String,
    private val username: String,
    private val password: String,
    private val schema: String? = null,
    private val visibilityTimeoutSeconds: Int = 30
): AbstractVerticle(), IQueueStoreSync {
    private val logger = Utils.getLogger(this::class.java)

    private val tableName = "messagequeue"


    override fun getType(): QueueStoreType = QueueStoreType.POSTGRES_V2

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun init(connection: Connection): Future<Void> {
            val promise = Promise.promise<Void>()
            try {
                connection.autoCommit = false

                if (!schema.isNullOrBlank()) {
                    connection.createStatement().use { stmt ->
                        stmt.execute("CREATE SCHEMA IF NOT EXISTS \"$schema\"")
                        stmt.execute("SET search_path TO \"$schema\", public")
                    }
                }

                connection.createStatement().use { statement ->
                    statement.executeUpdate("""
                        CREATE TABLE IF NOT EXISTS $tableName (
                            msg_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            message_uuid VARCHAR(36) NOT NULL,
                            client_id VARCHAR(65535) NOT NULL,
                            topic TEXT NOT NULL,
                            payload BYTEA,
                            qos INT NOT NULL,
                            retained BOOLEAN NOT NULL DEFAULT FALSE,
                            publisher_id VARCHAR(65535),
                            creation_time BIGINT NOT NULL,
                            message_expiry_interval BIGINT,
                            vt TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                            read_ct INT NOT NULL DEFAULT 0
                        )
                    """.trimIndent())

                    statement.executeUpdate(
                        "CREATE INDEX IF NOT EXISTS ${tableName}_fetch_idx ON $tableName (client_id, vt ASC)"
                    )
                    statement.executeUpdate(
                        "CREATE INDEX IF NOT EXISTS ${tableName}_client_uuid_idx ON $tableName (client_id, message_uuid)"
                    )
                }

                connection.commit()
                logger.fine("Queue table '$tableName' is ready [${Utils.getCurrentFunctionName()}]")
                promise.complete()
            } catch (e: Exception) {
                logger.severe("Error creating queue table: ${e.message} [${Utils.getCurrentFunctionName()}]")
                promise.fail(e)
            }
            return promise.future()
        }
    }

    override fun start(startPromise: Promise<Void>) {
        db.start(vertx, startPromise)
    }

    // -------------------------------------------------------------------------
    // Enqueue: one row per (message, client) — payload duplicated per subscriber
    // -------------------------------------------------------------------------

    override fun enqueueMessages(messages: List<Pair<BrokerMessage, List<String>>>) {
        val sql = """
            INSERT INTO $tableName
                (message_uuid, client_id, topic, payload, qos, retained, publisher_id,
                 creation_time, message_expiry_interval)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """.trimIndent()
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { ps ->
                    messages.forEach { (message, clientIds) ->
                        clientIds.forEach { clientId ->
                            ps.setString(1, message.messageUuid)
                            ps.setString(2, clientId)
                            ps.setString(3, message.topicName)
                            ps.setBytes(4, message.payload)
                            ps.setInt(5, message.qosLevel)
                            ps.setBoolean(6, message.isRetain)
                            ps.setString(7, message.clientId)
                            ps.setLong(8, message.time.toEpochMilli())
                            if (message.messageExpiryInterval != null) {
                                ps.setLong(9, message.messageExpiryInterval)
                            } else {
                                ps.setNull(9, Types.BIGINT)
                            }
                            ps.addBatch()
                        }
                    }
                    ps.executeBatch()
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            try { db.connection?.rollback() } catch (_: SQLException) {}
            logger.warning("Error enqueuing messages [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    // -------------------------------------------------------------------------
    // Dequeue: iterate visible messages for a client
    // -------------------------------------------------------------------------

    override fun dequeueMessages(clientId: String, callback: (BrokerMessage) -> Boolean) {
        val sql = """
            SELECT msg_id, message_uuid, topic, payload, qos, retained, publisher_id,
                   creation_time, message_expiry_interval
            FROM $tableName
            WHERE client_id = ? AND vt <= now()
              AND (message_expiry_interval IS NULL
                   OR ((EXTRACT(EPOCH FROM now()) * 1000 - creation_time) / 1000) < message_expiry_interval)
            ORDER BY msg_id ASC
        """.trimIndent()
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { ps ->
                    ps.setString(1, clientId)
                    val rs = ps.executeQuery()
                    while (rs.next()) {
                        val continueProcessing = callback(resultSetToBrokerMessage(rs))
                        if (!continueProcessing) break
                    }
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error dequeuing messages [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    // -------------------------------------------------------------------------
    // Remove messages (used by MsgDelQueue worker)
    // -------------------------------------------------------------------------

    override fun removeMessages(messages: List<Pair<String, String>>) {
        val sql = "DELETE FROM $tableName WHERE client_id = ? AND message_uuid = ?"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { ps ->
                    messages.forEach { (clientId, messageUuid) ->
                        ps.setString(1, clientId)
                        ps.setString(2, messageUuid)
                        ps.addBatch()
                    }
                    ps.executeBatch()
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            try { db.connection?.rollback() } catch (_: SQLException) {}
            logger.warning("Error removing messages [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    // -------------------------------------------------------------------------
    // Fetch pending messages
    // -------------------------------------------------------------------------

    override fun fetchNextPendingMessage(clientId: String): BrokerMessage? {
        return fetchPendingMessages(clientId, 1).firstOrNull()
    }

    override fun fetchPendingMessages(clientId: String, limit: Int): List<BrokerMessage> {
        val sql = """
            SELECT msg_id, message_uuid, topic, payload, qos, retained, publisher_id,
                   creation_time, message_expiry_interval
            FROM $tableName
            WHERE client_id = ? AND vt <= now()
              AND (message_expiry_interval IS NULL
                   OR ((EXTRACT(EPOCH FROM now()) * 1000 - creation_time) / 1000) < message_expiry_interval)
            ORDER BY msg_id ASC
            LIMIT ?
        """.trimIndent()
        return try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { ps ->
                    ps.setString(1, clientId)
                    ps.setInt(2, limit)
                    val rs = ps.executeQuery()
                    val messages = mutableListOf<BrokerMessage>()
                    while (rs.next()) {
                        messages.add(resultSetToBrokerMessage(rs))
                    }
                    messages
                }
            } ?: emptyList()
        } catch (e: SQLException) {
            logger.warning("Error fetching pending messages [${e.message}] [${Utils.getCurrentFunctionName()}]")
            emptyList()
        }
    }

    // -------------------------------------------------------------------------
    // Atomic fetch-and-lock using CTE with FOR UPDATE SKIP LOCKED + VT update
    // -------------------------------------------------------------------------

    override fun fetchAndLockPendingMessages(clientId: String, limit: Int): List<BrokerMessage> {
        val fetchSql = """
            SELECT msg_id, message_uuid, topic, payload, qos, retained, publisher_id,
                   creation_time, message_expiry_interval
            FROM $tableName
            WHERE client_id = ? AND vt <= clock_timestamp()
              AND (message_expiry_interval IS NULL
                   OR ((EXTRACT(EPOCH FROM clock_timestamp()) * 1000 - creation_time) / 1000) < message_expiry_interval)
            ORDER BY msg_id ASC
            LIMIT ?
            FOR UPDATE SKIP LOCKED
        """.trimIndent()
        val lockSql = """
            UPDATE $tableName SET vt = clock_timestamp() + make_interval(secs => ?), read_ct = read_ct + 1
            WHERE msg_id = ANY (?)
        """.trimIndent()
        return try {
            db.connection?.let { connection ->
                val messages = mutableListOf<BrokerMessage>()
                val msgIds = mutableListOf<Long>()

                connection.prepareStatement(fetchSql).use { ps ->
                    ps.setString(1, clientId)
                    ps.setInt(2, limit)
                    val rs = ps.executeQuery()
                    while (rs.next()) {
                        msgIds.add(rs.getLong("msg_id"))
                        messages.add(resultSetToBrokerMessage(rs))
                    }
                }

                if (msgIds.isNotEmpty()) {
                    connection.prepareStatement(lockSql).use { ps ->
                        ps.setInt(1, visibilityTimeoutSeconds)
                        ps.setArray(2, connection.createArrayOf("bigint", msgIds.toTypedArray()))
                        ps.executeUpdate()
                    }
                }

                connection.commit()
                messages
            } ?: emptyList()
        } catch (e: SQLException) {
            try { db.connection?.rollback() } catch (_: SQLException) {}
            logger.warning("Error in atomic fetch-and-lock [${e.message}] [${Utils.getCurrentFunctionName()}]")
            emptyList()
        }
    }

    // -------------------------------------------------------------------------
    // Mark in-flight: extend visibility timeout
    // -------------------------------------------------------------------------

    override fun markMessageInFlight(clientId: String, messageUuid: String) {
        val sql = "UPDATE $tableName SET vt = now() + make_interval(secs => ?), read_ct = read_ct + 1 WHERE client_id = ? AND message_uuid = ? AND vt <= now()"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { ps ->
                    ps.setInt(1, visibilityTimeoutSeconds)
                    ps.setString(2, clientId)
                    ps.setString(3, messageUuid)
                    ps.executeUpdate()
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            try { db.connection?.rollback() } catch (_: SQLException) {}
            logger.warning("Error marking message in-flight [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun markMessagesInFlight(clientId: String, messageUuids: List<String>) {
        if (messageUuids.isEmpty()) return
        val sql = "UPDATE $tableName SET vt = now() + make_interval(secs => ?), read_ct = read_ct + 1 WHERE client_id = ? AND message_uuid = ANY (?) AND vt <= now()"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { ps ->
                    ps.setInt(1, visibilityTimeoutSeconds)
                    ps.setString(2, clientId)
                    ps.setArray(3, connection.createArrayOf("text", messageUuids.toTypedArray()))
                    ps.executeUpdate()
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            try { db.connection?.rollback() } catch (_: SQLException) {}
            logger.warning("Error marking messages in-flight [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    // -------------------------------------------------------------------------
    // ACK = DELETE (no status=2 concept in V2)
    // -------------------------------------------------------------------------

    override fun markMessageDelivered(clientId: String, messageUuid: String) {
        val sql = "DELETE FROM $tableName WHERE client_id = ? AND message_uuid = ?"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { ps ->
                    ps.setString(1, clientId)
                    ps.setString(2, messageUuid)
                    ps.executeUpdate()
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            try { db.connection?.rollback() } catch (_: SQLException) {}
            logger.warning("Error deleting delivered message [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    // -------------------------------------------------------------------------
    // Reset: make in-flight messages visible again
    // -------------------------------------------------------------------------

    override fun resetInFlightMessages(clientId: String) {
        val sql = "UPDATE $tableName SET vt = now(), read_ct = 0 WHERE client_id = ? AND vt > now()"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { ps ->
                    ps.setString(1, clientId)
                    ps.executeUpdate()
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            try { db.connection?.rollback() } catch (_: SQLException) {}
            logger.warning("Error resetting in-flight messages [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    // -------------------------------------------------------------------------
    // Purge: no-ops in V2 (no status=2, no orphans)
    // -------------------------------------------------------------------------

    override fun purgeDeliveredMessages(): Int = 0  // ACK = DELETE, nothing to purge

    override fun purgeExpiredMessages(): Int {
        val sql = """
            DELETE FROM $tableName
            WHERE message_expiry_interval IS NOT NULL
              AND creation_time IS NOT NULL
              AND ((EXTRACT(EPOCH FROM now()) * 1000 - creation_time) / 1000) >= message_expiry_interval
        """.trimIndent()
        return try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { ps ->
                    val count = ps.executeUpdate()
                    connection.commit()
                    if (count > 0) logger.fine { "Purged $count expired messages" }
                    count
                }
            } ?: 0
        } catch (e: SQLException) {
            try { db.connection?.rollback() } catch (_: SQLException) {}
            logger.warning("Error purging expired messages [${e.message}] [${Utils.getCurrentFunctionName()}]")
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
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { ps ->
                    ps.setString(1, clientId)
                    ps.executeUpdate()
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            try { db.connection?.rollback() } catch (_: SQLException) {}
            logger.warning("Error deleting client messages [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    // -------------------------------------------------------------------------
    // Count
    // -------------------------------------------------------------------------

    override fun countQueuedMessages(): Long {
        val sql = "SELECT COUNT(*) FROM $tableName"
        return try {
            DriverManager.getConnection(url, username, password).use { connection ->
                connection.prepareStatement(sql).use { ps ->
                    ps.executeQuery().use { rs ->
                        if (rs.next()) rs.getLong(1) else 0L
                    }
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error counting queued messages [${e.message}] [${Utils.getCurrentFunctionName()}]")
            0L
        }
    }

    override fun countQueuedMessagesForClient(clientId: String): Long {
        val sql = "SELECT COUNT(*) FROM $tableName WHERE client_id = ? AND vt <= now()"
        return try {
            DriverManager.getConnection(url, username, password).use { connection ->
                connection.prepareStatement(sql).use { ps ->
                    ps.setString(1, clientId)
                    ps.executeQuery().use { rs ->
                        if (rs.next()) rs.getLong(1) else 0L
                    }
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error counting queued messages for client $clientId [${e.message}] [${Utils.getCurrentFunctionName()}]")
            0L
        }
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private fun resultSetToBrokerMessage(rs: ResultSet): BrokerMessage {
        val messageExpiryIntervalRaw = rs.getLong("message_expiry_interval")
        val messageExpiryInterval = if (rs.wasNull()) null else messageExpiryIntervalRaw
        return BrokerMessage(
            messageUuid = rs.getString("message_uuid"),
            messageId = 0,
            topicName = rs.getString("topic"),
            payload = rs.getBytes("payload"),
            qosLevel = rs.getInt("qos"),
            isRetain = rs.getBoolean("retained"),
            isDup = false,
            isQueued = true,
            clientId = rs.getString("publisher_id"),
            time = java.time.Instant.ofEpochMilli(rs.getLong("creation_time")),
            messageExpiryInterval = messageExpiryInterval
        )
    }
}
