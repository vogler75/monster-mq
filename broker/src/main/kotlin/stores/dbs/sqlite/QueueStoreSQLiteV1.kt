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

class QueueStoreSQLiteV1(
    private val dbPath: String
): AbstractVerticle(), IQueueStoreSync {
    private val logger = Utils.getLogger(this::class.java)
    private lateinit var sqlClient: SQLiteClient

    private val queuedMessagesTableName = "queuedmessages"
    private val queuedMessagesClientsTableName = "queuedmessagesclients"


    override fun getType(): QueueStoreType = QueueStoreType.SQLITE

    override fun start(startPromise: Promise<Void>) {
        sqlClient = SQLiteClient(vertx, dbPath)

        val createTableSQL = JsonArray()
            .add("""
            CREATE TABLE IF NOT EXISTS $queuedMessagesTableName (
                message_uuid TEXT PRIMARY KEY,
                message_id INTEGER,
                topic TEXT,
                payload BLOB,
                qos INTEGER,
                retained BOOLEAN,
                client_id TEXT,
                creation_time INTEGER,
                message_expiry_interval INTEGER
            );
            """.trimIndent())
            .add("""
            CREATE TABLE IF NOT EXISTS $queuedMessagesClientsTableName (
                client_id TEXT,
                message_uuid TEXT,
                status INTEGER DEFAULT 0,
                PRIMARY KEY (client_id, message_uuid)
            );
            """.trimIndent())
            .add("CREATE INDEX IF NOT EXISTS ${queuedMessagesClientsTableName}_status_idx ON $queuedMessagesClientsTableName (status);")
            .add("CREATE INDEX IF NOT EXISTS ${queuedMessagesClientsTableName}_message_uuid_idx ON $queuedMessagesClientsTableName (message_uuid);")
            .add("CREATE INDEX IF NOT EXISTS ${queuedMessagesClientsTableName}_client_status_idx ON $queuedMessagesClientsTableName (client_id, status);")

        sqlClient.initDatabase(createTableSQL).onComplete { result ->
            if (result.succeeded()) {
                logger.info("SQLite queue tables are ready [start]")
                startPromise.complete()
            } else {
                logger.severe("Failed to initialize SQLite queue tables: ${result.cause()?.message}")
                startPromise.fail(result.cause())
            }
        }
    }

    override fun enqueueMessages(messages: List<Pair<BrokerMessage, List<String>>>) {
        if (messages.isEmpty()) return

        val insertMessageSql = """INSERT INTO $queuedMessagesTableName
                                (message_uuid, message_id, topic, payload, qos, retained, client_id, creation_time, message_expiry_interval)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT (message_uuid) DO NOTHING"""

        val insertClientSql = """INSERT INTO $queuedMessagesClientsTableName (client_id, message_uuid, status)
                               VALUES (?, ?, 0) ON CONFLICT DO NOTHING"""

        val messageBatch = JsonArray()
        val clientBatch = JsonArray()

        messages.forEach { (message, clientIds) ->
            // Insert message once
            val messageParams = JsonArray()
                .add(message.messageUuid)
                .add(message.messageId)
                .add(message.topicName)
                .add(message.payload)
                .add(message.qosLevel)
                .add(message.isRetain)
                .add(message.clientId)
                .add(message.time.toEpochMilli())
                .add(message.messageExpiryInterval ?: -1L)  // -1 = no expiry
            messageBatch.add(messageParams)

            // Insert client mappings for each client
            clientIds.forEach { clientId ->
                val clientParams = JsonArray().add(clientId).add(message.messageUuid)
                clientBatch.add(clientParams)
            }
        }

        // Execute both batches
        sqlClient.executeBatch(insertMessageSql, messageBatch)
        sqlClient.executeBatch(insertClientSql, clientBatch)

        logger.fine { "Enqueued ${messages.size} messages for ${messages.sumOf { it.second.size }} client mappings" }
    }

    override fun dequeueMessages(clientId: String, callback: (BrokerMessage) -> Boolean) {
        val sql = """SELECT m.message_uuid, m.message_id, m.topic, m.payload, m.qos, m.retained, m.client_id, m.creation_time, m.message_expiry_interval
                    FROM $queuedMessagesTableName m
                    JOIN $queuedMessagesClientsTableName c ON m.message_uuid = c.message_uuid
                    WHERE c.client_id = ?
                    ORDER BY c.rowid"""

        val params = JsonArray().add(clientId)

        try {
            val results = sqlClient.executeQuerySync(sql, params)
            val processedUuids = mutableListOf<String>()
            val expiredUuids = mutableListOf<String>()
            val currentTimeMillis = System.currentTimeMillis()

            results.forEach { row ->
                val rowObj = row as JsonObject
                val messageUuid = rowObj.getString("message_uuid")
                val messageId = rowObj.getInteger("message_id")
                val topic = rowObj.getString("topic")
                val payload = rowObj.getBinary("payload") ?: ByteArray(0)
                val qos = rowObj.getInteger("qos")
                val retained = rowObj.getBoolean("retained", false)
                val originalClientId = rowObj.getString("client_id")
                val creationTime = rowObj.getLong("creation_time")
                val messageExpiryIntervalRaw = rowObj.getLong("message_expiry_interval", -1L)
                // Convert -1 back to null (no expiry)
                val messageExpiryInterval = if (messageExpiryIntervalRaw == -1L) null else messageExpiryIntervalRaw

                // Check if message has expired (only if expiry interval is set, i.e. not -1/null)
                if (messageExpiryInterval != null && messageExpiryInterval >= 0) {
                    val ageSeconds = (currentTimeMillis - creationTime) / 1000
                    if (ageSeconds >= messageExpiryInterval) {
                        // Message expired - mark for deletion
                        expiredUuids.add(messageUuid)
                        logger.fine { "Message [$messageUuid] expired (age: ${ageSeconds}s, limit: ${messageExpiryInterval}s)" }
                        return@forEach
                    }
                }

                val message = BrokerMessage(
                    messageUuid = messageUuid,
                    messageId = messageId,
                    topicName = topic,
                    payload = payload,
                    qosLevel = qos,
                    isRetain = retained,
                    isQueued = true,
                    clientId = originalClientId,
                    isDup = false,
                    messageExpiryInterval = messageExpiryInterval,
                    time = java.time.Instant.ofEpochMilli(creationTime ?: currentTimeMillis)
                )

                // Call callback and collect processed messages
                if (callback(message)) {
                    processedUuids.add(messageUuid)
                }
            }

            // Remove processed and expired messages for this client
            val allToRemove = processedUuids + expiredUuids
            if (allToRemove.isNotEmpty()) {
                val deleteSql = "DELETE FROM $queuedMessagesClientsTableName WHERE client_id = ? AND message_uuid = ?"
                val deleteBatch = JsonArray()
                allToRemove.forEach { uuid ->
                    deleteBatch.add(JsonArray().add(clientId).add(uuid))
                }
                sqlClient.executeBatch(deleteSql, deleteBatch)

                if (expiredUuids.isNotEmpty()) {
                    logger.fine { "Removed ${expiredUuids.size} expired messages for client [$clientId]" }
                }
            }
        } catch (e: Exception) {
            logger.warning("Error dequeuing messages for client [$clientId]: ${e.message}")
        }
    }

    override fun removeMessages(messages: List<Pair<String, String>>) {
        if (messages.isEmpty()) return

        val deleteSql = "DELETE FROM $queuedMessagesClientsTableName WHERE client_id = ? AND message_uuid = ?"
        val batchParams = JsonArray()

        messages.forEach { (clientId, messageUuid) ->
            val params = JsonArray().add(clientId).add(messageUuid)
            batchParams.add(params)
        }

        sqlClient.executeBatch(deleteSql, batchParams).onComplete { result ->
            if (result.failed()) {
                logger.warning("Error removing messages: ${result.cause()?.message}")
            } else {
                logger.fine { "Removed ${messages.size} message mappings" }
            }
        }
    }

    override fun fetchNextPendingMessage(clientId: String): BrokerMessage? {
        return fetchPendingMessages(clientId, 1).firstOrNull()
    }

    override fun fetchPendingMessages(clientId: String, limit: Int): List<BrokerMessage> {
        val sql = """SELECT m.message_uuid, m.message_id, m.topic, m.payload, m.qos, m.retained, m.client_id, m.creation_time, m.message_expiry_interval
                    FROM $queuedMessagesTableName m
                    JOIN $queuedMessagesClientsTableName c ON m.message_uuid = c.message_uuid
                    WHERE c.client_id = ? AND c.status = 0
                    ORDER BY c.rowid
                    LIMIT ?"""

        val params = JsonArray().add(clientId).add(limit)

        return try {
            val results = sqlClient.executeQuerySync(sql, params)
            val messages = mutableListOf<BrokerMessage>()
            val currentTimeMillis = System.currentTimeMillis()
            val expiredUuids = mutableListOf<String>()

            for (i in 0 until results.size()) {
                val rowObj = results.getJsonObject(i)
                val messageUuid = rowObj.getString("message_uuid")
                val creationTime = rowObj.getLong("creation_time")
                val messageExpiryIntervalRaw = rowObj.getLong("message_expiry_interval", -1L)
                // Convert -1 back to null (no expiry)
                val messageExpiryInterval = if (messageExpiryIntervalRaw == -1L) null else messageExpiryIntervalRaw

                // Check if message has expired (only if expiry interval is set, i.e. not -1/null)
                if (messageExpiryInterval != null && messageExpiryInterval >= 0) {
                    val ageSeconds = (currentTimeMillis - creationTime) / 1000
                    if (ageSeconds >= messageExpiryInterval) {
                        // Message expired - skip it and mark for deletion
                        expiredUuids.add(messageUuid)
                        logger.fine { "Message [$messageUuid] expired (age: ${ageSeconds}s, limit: ${messageExpiryInterval}s)" }
                        continue
                    }
                }

                messages.add(BrokerMessage(
                    messageUuid = messageUuid,
                    messageId = rowObj.getInteger("message_id"),
                    topicName = rowObj.getString("topic"),
                    payload = rowObj.getBinary("payload") ?: ByteArray(0),
                    qosLevel = rowObj.getInteger("qos"),
                    isRetain = rowObj.getBoolean("retained", false),
                    isDup = false,
                    isQueued = true,
                    clientId = rowObj.getString("client_id"),
                    messageExpiryInterval = messageExpiryInterval,
                    time = java.time.Instant.ofEpochMilli(creationTime ?: currentTimeMillis)
                ))
            }

            // Delete expired messages
            if (expiredUuids.isNotEmpty()) {
                val deleteSql = "DELETE FROM $queuedMessagesClientsTableName WHERE client_id = ? AND message_uuid = ?"
                val deleteBatch = JsonArray()
                expiredUuids.forEach { uuid ->
                    deleteBatch.add(JsonArray().add(clientId).add(uuid))
                }
                sqlClient.executeBatch(deleteSql, deleteBatch)
                logger.fine { "Removed ${expiredUuids.size} expired messages for client [$clientId]" }
            }

            messages
        } catch (e: Exception) {
            logger.warning("Error fetching pending messages: ${e.message}")
            emptyList()
        }
    }

    override fun fetchAndLockPendingMessages(clientId: String, limit: Int): List<BrokerMessage> {
        // SQLite single-writer model makes separate fetch + mark safe without FOR UPDATE SKIP LOCKED
        // Filter expired messages in SQL and use a single IN-clause UPDATE for marking
        val currentTimeMillis = System.currentTimeMillis()
        val sql = """SELECT m.message_uuid, m.message_id, m.topic, m.payload, m.qos, m.retained, m.client_id, m.creation_time, m.message_expiry_interval
                    FROM $queuedMessagesTableName m
                    JOIN $queuedMessagesClientsTableName c ON m.message_uuid = c.message_uuid
                    WHERE c.client_id = ? AND c.status = 0
                      AND (m.message_expiry_interval IS NULL
                           OR m.message_expiry_interval < 0
                           OR ((? - m.creation_time) / 1000) < m.message_expiry_interval)
                    ORDER BY c.rowid
                    LIMIT ?"""

        val params = JsonArray().add(clientId).add(currentTimeMillis).add(limit)

        return try {
            val results = sqlClient.executeQuerySync(sql, params)
            val messages = mutableListOf<BrokerMessage>()

            for (i in 0 until results.size()) {
                val rowObj = results.getJsonObject(i)
                val messageExpiryIntervalRaw = rowObj.getLong("message_expiry_interval", -1L)
                val messageExpiryInterval = if (messageExpiryIntervalRaw == -1L) null else messageExpiryIntervalRaw

                messages.add(BrokerMessage(
                    messageUuid = rowObj.getString("message_uuid"),
                    messageId = rowObj.getInteger("message_id"),
                    topicName = rowObj.getString("topic"),
                    payload = rowObj.getBinary("payload") ?: ByteArray(0),
                    qosLevel = rowObj.getInteger("qos"),
                    isRetain = rowObj.getBoolean("retained", false),
                    isDup = false,
                    isQueued = true,
                    clientId = rowObj.getString("client_id"),
                    messageExpiryInterval = messageExpiryInterval,
                    time = java.time.Instant.ofEpochMilli(rowObj.getLong("creation_time") ?: currentTimeMillis)
                ))
            }

            // Mark all fetched messages in-flight with a single statement using dynamic IN clause
            if (messages.isNotEmpty()) {
                val uuids = messages.map { it.messageUuid }
                val placeholders = uuids.joinToString(",") { "?" }
                val updateSql = "UPDATE $queuedMessagesClientsTableName SET status = 1 WHERE client_id = ? AND message_uuid IN ($placeholders) AND status = 0"
                val updateParams = JsonArray().add(clientId)
                uuids.forEach { updateParams.add(it) }
                sqlClient.executeUpdateSync(updateSql, updateParams)
            }

            messages
        } catch (e: Exception) {
            logger.warning("Error in atomic fetch-and-lock [${e.message}] [${Utils.getCurrentFunctionName()}]")
            emptyList()
        }
    }

    override fun markMessageInFlight(clientId: String, messageUuid: String) {
        val sql = "UPDATE $queuedMessagesClientsTableName SET status = 1 WHERE client_id = ? AND message_uuid = ? AND status = 0"
        val params = JsonArray().add(clientId).add(messageUuid)
        try {
            sqlClient.executeUpdateSync(sql, params)
        } catch (e: Exception) {
            logger.warning("Error marking message in-flight: ${e.message}")
        }
    }

    override fun markMessagesInFlight(clientId: String, messageUuids: List<String>) {
        if (messageUuids.isEmpty()) return
        // Use dynamic IN clause for single statement instead of N batch updates
        val placeholders = messageUuids.joinToString(",") { "?" }
        val sql = "UPDATE $queuedMessagesClientsTableName SET status = 1 WHERE client_id = ? AND message_uuid IN ($placeholders) AND status = 0"
        val params = JsonArray().add(clientId)
        messageUuids.forEach { params.add(it) }
        try {
            sqlClient.executeUpdateSync(sql, params)
        } catch (e: Exception) {
            logger.warning("Error marking messages in-flight: ${e.message}")
        }
    }

    override fun markMessageDelivered(clientId: String, messageUuid: String) {
        val sql = "UPDATE $queuedMessagesClientsTableName SET status = 2 WHERE client_id = ? AND message_uuid = ? AND status = 1"
        val params = JsonArray().add(clientId).add(messageUuid)
        try {
            sqlClient.executeUpdateSync(sql, params)
        } catch (e: Exception) {
            logger.warning("Error marking message delivered: ${e.message}")
        }
    }

    override fun resetInFlightMessages(clientId: String) {
        val sql = "UPDATE $queuedMessagesClientsTableName SET status = 0 WHERE client_id = ? AND status = 1"
        val params = JsonArray().add(clientId)
        try {
            sqlClient.executeUpdateSync(sql, params)
        } catch (e: Exception) {
            logger.warning("Error resetting in-flight messages: ${e.message}")
        }
    }

    override fun purgeDeliveredMessages(): Int {
        val sql = "DELETE FROM $queuedMessagesClientsTableName WHERE status = 2"
        return try {
            sqlClient.executeUpdateSync(sql, JsonArray())
        } catch (e: Exception) {
            logger.warning("Error purging delivered messages: ${e.message}")
            0
        }
    }

    override fun purgeExpiredMessages(): Int {
        val currentTimeMillis = System.currentTimeMillis()

        // Find expired messages
        val findExpiredSql = """
            SELECT message_uuid FROM $queuedMessagesTableName
            WHERE message_expiry_interval IS NOT NULL
            AND creation_time IS NOT NULL
            AND ((? - creation_time) / 1000) >= message_expiry_interval
        """.trimIndent()

        return try {
            val params = JsonArray().add(currentTimeMillis)
            val results = sqlClient.executeQuerySync(findExpiredSql, params)

            if (results.isEmpty()) {
                return 0
            }

            val expiredUuids = results.map { (it as JsonObject).getString("message_uuid") }

            // Delete expired message client mappings
            val deleteClientsSql = "DELETE FROM $queuedMessagesClientsTableName WHERE message_uuid = ?"
            val deleteBatch = JsonArray()
            expiredUuids.forEach { uuid ->
                deleteBatch.add(JsonArray().add(uuid))
            }
            sqlClient.executeBatch(deleteClientsSql, deleteBatch)

            logger.fine { "Purged ${expiredUuids.size} expired messages" }
            expiredUuids.size
        } catch (e: Exception) {
            logger.warning("Error purging expired messages: ${e.message}")
            0
        }
    }

    override fun purgeQueuedMessages() {
        val startTime = System.currentTimeMillis()
        // Delete only orphaned messages (not referenced by any client)
        val deleteSql = """
            DELETE FROM $queuedMessagesTableName
            WHERE message_uuid NOT IN (
                SELECT DISTINCT message_uuid FROM $queuedMessagesClientsTableName
            )
        """.trimIndent()
        val deleted = sqlClient.executeUpdateSync(deleteSql, JsonArray())
        val duration = (System.currentTimeMillis() - startTime) / 1000.0
        if (deleted > 0) {
            logger.info { "Purging queued messages finished: deleted $deleted orphaned messages in $duration seconds" }
        } else {
            logger.fine { "Purging queued messages finished: no orphaned messages found in $duration seconds" }
        }
    }

    override fun deleteClientMessages(clientId: String) {
        val sql = "DELETE FROM $queuedMessagesClientsTableName WHERE client_id = ?"
        val params = JsonArray().add(clientId)
        try {
            sqlClient.executeUpdateSync(sql, params)
        } catch (e: Exception) {
            logger.warning("Error deleting client messages for [$clientId]: ${e.message}")
        }
    }

    override fun countQueuedMessages(): Long {
        val sql = "SELECT COUNT(*) FROM $queuedMessagesTableName"

        return try {
            val results = sqlClient.executeQuerySync(sql)
            if (results.size() > 0) {
                val row = results.getJsonObject(0)
                row.getLong("COUNT(*)") ?: 0L
            } else {
                0L
            }
        } catch (e: Exception) {
            logger.fine { "Error counting queued messages: ${e.message}" }
            0L
        }
    }

    override fun countQueuedMessagesForClient(clientId: String): Long {
        val sql = "SELECT COUNT(*) FROM $queuedMessagesClientsTableName WHERE client_id = ? AND status < 2"
        val params = JsonArray().add(clientId)

        return try {
            val results = sqlClient.executeQuerySync(sql, params)
            if (results.size() > 0) {
                val row = results.getJsonObject(0)
                row.getLong("COUNT(*)") ?: 0L
            } else {
                0L
            }
        } catch (e: Exception) {
            logger.fine { "Error counting queued messages for client $clientId: ${e.message}" }
            0L
        }
    }
}
