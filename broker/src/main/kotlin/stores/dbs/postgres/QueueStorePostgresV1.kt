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

class QueueStorePostgresV1(
    private val url: String,
    private val username: String,
    private val password: String,
    private val schema: String? = null
): AbstractVerticle(), IQueueStoreSync {
    private val logger = Utils.getLogger(this::class.java)

    private val queuedMessagesTableName = "queuedmessages"
    private val queuedMessagesClientsTableName = "queuedmessagesclients"


    override fun getType(): QueueStoreType = QueueStoreType.POSTGRES

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun init(connection: Connection): Future<Void> {
            val promise = Promise.promise<Void>()
            try {
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

                val createTableSQL = listOf("""
                CREATE TABLE IF NOT EXISTS $queuedMessagesTableName (
                    message_uuid VARCHAR(36),
                    message_id INT,
                    topic TEXT,
                    payload BYTEA,
                    qos INT,
                    retained BOOLEAN,
                    client_id VARCHAR(65535),
                    creation_time BIGINT,
                    message_expiry_interval BIGINT,
                    PRIMARY KEY (message_uuid)
                );
                """.trimIndent(), """
                CREATE TABLE IF NOT EXISTS $queuedMessagesClientsTableName (
                    client_id VARCHAR(65535),
                    message_uuid VARCHAR(36),
                    status INTEGER DEFAULT 0,
                    PRIMARY KEY (client_id, message_uuid)
                );
                """.trimIndent())

                // Create indexes for faster queries
                val createIndexesSQL = listOf(
                    "CREATE INDEX IF NOT EXISTS ${queuedMessagesClientsTableName}_status_idx ON $queuedMessagesClientsTableName (status);",
                    "CREATE INDEX IF NOT EXISTS ${queuedMessagesClientsTableName}_message_uuid_idx ON $queuedMessagesClientsTableName (message_uuid);",
                    "CREATE INDEX IF NOT EXISTS ${queuedMessagesClientsTableName}_client_status_idx ON $queuedMessagesClientsTableName (client_id, status);"
                )

                // Execute the SQL statements
                connection.createStatement().use { statement ->
                    createTableSQL.forEach(statement::executeUpdate)
                    createIndexesSQL.forEach(statement::executeUpdate)
                }
                connection.commit()
                logger.fine("Tables are ready [${Utils.getCurrentFunctionName()}]")
                promise.complete()
            } catch (e: Exception) {
                logger.severe("Error in getting tables ready: ${e.message} [${Utils.getCurrentFunctionName()}]")
                promise.fail(e)
            }
            return promise.future()
        }
    }

    override fun start(startPromise: Promise<Void>) {
        db.start(vertx, startPromise)
    }

    override fun enqueueMessages(messages: List<Pair<BrokerMessage, List<String>>>) {
        val sql1 = "INSERT INTO $queuedMessagesTableName "+
                   "(message_uuid, message_id, topic, payload, qos, retained, client_id, creation_time, message_expiry_interval) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "+
                   "ON CONFLICT (message_uuid) DO NOTHING"
        val sql2 = "INSERT INTO $queuedMessagesClientsTableName "+
                   "(client_id, message_uuid) VALUES (?, ?) "+
                   "ON CONFLICT (client_id, message_uuid) DO NOTHING"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql1).use { preparedStatement1 ->
                   connection.prepareStatement(sql2).use { preparedStatement2 ->
                       messages.forEach { message ->
                           // Add message
                           preparedStatement1.setObject(1, message.first.messageUuid)
                           preparedStatement1.setInt(2, message.first.messageId)
                           preparedStatement1.setString(3, message.first.topicName)
                           preparedStatement1.setBytes(4, message.first.payload)
                           preparedStatement1.setInt(5, message.first.qosLevel)
                           preparedStatement1.setBoolean(6, message.first.isRetain)
                           preparedStatement1.setString(7, message.first.clientId)
                           preparedStatement1.setLong(8, message.first.time.toEpochMilli())
                           if (message.first.messageExpiryInterval != null) {
                               preparedStatement1.setLong(9, message.first.messageExpiryInterval!!)
                           } else {
                               preparedStatement1.setNull(9, java.sql.Types.BIGINT)
                           }
                           preparedStatement1.addBatch()

                           // Add client to message relation
                           message.second.forEach { clientId ->
                               preparedStatement2.setString(1, clientId)
                               preparedStatement2.setString(2, message.first.messageUuid)
                               preparedStatement2.addBatch()
                           }
                       }
                       preparedStatement1.executeBatch()
                       preparedStatement2.executeBatch()
                   }
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            try { db.connection?.rollback() } catch (_: SQLException) {}
            logger.warning("Error at inserting queued message [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun dequeueMessages(clientId: String, callback: (BrokerMessage)->Boolean) {
        val sql = "SELECT m.message_uuid, m.message_id, m.topic, m.payload, m.qos, m.retained, m.client_id, m.creation_time, m.message_expiry_interval "+
                  "FROM $queuedMessagesTableName AS m JOIN $queuedMessagesClientsTableName AS c USING (message_uuid) "+
                  "WHERE c.client_id = ? AND c.status = 0 "+
                  "ORDER BY m.message_uuid" // Time Based UUIDs
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, clientId)
                    val resultSet = preparedStatement.executeQuery()
                    val currentTimeMillis = System.currentTimeMillis()
                    while (resultSet.next()) {
                        val messageUuid = resultSet.getString(1)
                        val messageId = resultSet.getInt(2)
                        val topic = resultSet.getString(3)
                        val payload = resultSet.getBytes(4)
                        val qos = resultSet.getInt(5)
                        val retained = resultSet.getBoolean(6)
                        val clientIdPublisher = resultSet.getString(7)
                        val creationTime = resultSet.getLong(8)
                        val messageExpiryIntervalRaw = resultSet.getLong(9)
                        val messageExpiryInterval = if (resultSet.wasNull()) null else messageExpiryIntervalRaw

                        // Check if message has expired
                        if (messageExpiryInterval != null && messageExpiryInterval >= 0) {
                            val ageSeconds = (currentTimeMillis - creationTime) / 1000
                            if (ageSeconds >= messageExpiryInterval) {
                                // Skip expired message
                                continue
                            }
                        }

                        val continueProcessing = callback(
                            BrokerMessage(
                                messageUuid = messageUuid,
                                messageId = messageId,
                                topicName = topic,
                                payload = payload,
                                qosLevel = qos,
                                isRetain = retained,
                                isDup = false,
                                isQueued = true,
                                clientId = clientIdPublisher,
                                time = java.time.Instant.ofEpochMilli(creationTime),
                                messageExpiryInterval = messageExpiryInterval
                            )
                        )
                        if (!continueProcessing) break
                    }
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error at fetching queued message [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun removeMessages(messages: List<Pair<String, String>>) { // clientId, messageUuid
        val groupedMessages = messages.groupBy({ it.first }, { it.second })
        val sql = "DELETE FROM $queuedMessagesClientsTableName WHERE client_id = ? AND message_uuid = ANY (?)"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    groupedMessages.forEach { (clientId, messageUuids) ->
                        preparedStatement.setString(1, clientId)
                        preparedStatement.setArray(2, connection.createArrayOf("text", messageUuids.toTypedArray()))
                        preparedStatement.addBatch()
                    }
                    preparedStatement.executeUpdate()
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            try { db.connection?.rollback() } catch (_: SQLException) {}
            logger.warning("Error at removing dequeued message [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun fetchNextPendingMessage(clientId: String): BrokerMessage? {
        return fetchPendingMessages(clientId, 1).firstOrNull()
    }

    override fun fetchPendingMessages(clientId: String, limit: Int): List<BrokerMessage> {
        val sql = "SELECT m.message_uuid, m.message_id, m.topic, m.payload, m.qos, m.retained, m.client_id, m.creation_time, m.message_expiry_interval " +
                  "FROM $queuedMessagesTableName AS m JOIN $queuedMessagesClientsTableName AS c USING (message_uuid) " +
                  "WHERE c.client_id = ? AND c.status = 0 " +
                  "ORDER BY m.message_uuid LIMIT ?"
        return try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, clientId)
                    preparedStatement.setInt(2, limit)
                    val resultSet = preparedStatement.executeQuery()
                    val currentTimeMillis = System.currentTimeMillis()
                    val messages = mutableListOf<BrokerMessage>()
                    while (resultSet.next()) {
                        val messageUuid = resultSet.getString(1)
                        val messageId = resultSet.getInt(2)
                        val topic = resultSet.getString(3)
                        val payload = resultSet.getBytes(4)
                        val qos = resultSet.getInt(5)
                        val retained = resultSet.getBoolean(6)
                        val clientIdPublisher = resultSet.getString(7)
                        val creationTime = resultSet.getLong(8)
                        val messageExpiryIntervalRaw = resultSet.getLong(9)
                        val messageExpiryInterval = if (resultSet.wasNull()) null else messageExpiryIntervalRaw

                        // Check if message has expired
                        if (messageExpiryInterval != null && messageExpiryInterval >= 0) {
                            val ageSeconds = (currentTimeMillis - creationTime) / 1000
                            if (ageSeconds >= messageExpiryInterval) {
                                // Skip expired message
                                continue
                            }
                        }

                        messages.add(BrokerMessage(
                            messageUuid = messageUuid,
                            messageId = messageId,
                            topicName = topic,
                            payload = payload,
                            qosLevel = qos,
                            isRetain = retained,
                            isDup = false,
                            isQueued = true,
                            clientId = clientIdPublisher,
                            time = java.time.Instant.ofEpochMilli(creationTime),
                            messageExpiryInterval = messageExpiryInterval
                        ))
                    }
                    messages
                }
            } ?: emptyList()
        } catch (e: SQLException) {
            logger.warning("Error fetching pending messages [${e.message}] [${Utils.getCurrentFunctionName()}]")
            emptyList()
        }
    }

    override fun fetchAndLockPendingMessages(clientId: String, limit: Int): List<BrokerMessage> {
        val currentTimeMillis = System.currentTimeMillis()
        val fetchSql = """
            SELECT m.message_uuid, m.message_id, m.topic, m.payload, m.qos, m.retained,
                   m.client_id, m.creation_time, m.message_expiry_interval
            FROM $queuedMessagesClientsTableName c
            JOIN $queuedMessagesTableName m USING (message_uuid)
            WHERE c.client_id = ?
              AND c.status = 0
              AND (m.message_expiry_interval IS NULL
                   OR ((? - m.creation_time) / 1000) < m.message_expiry_interval)
            ORDER BY m.message_uuid ASC
            LIMIT ?
            FOR UPDATE OF c SKIP LOCKED
        """.trimIndent()
        val markSql = "UPDATE $queuedMessagesClientsTableName SET status = 1 WHERE client_id = ? AND message_uuid = ANY (?) AND status = 0"
        return try {
            db.connection?.let { connection ->
                val messages = mutableListOf<BrokerMessage>()
                connection.prepareStatement(fetchSql).use { preparedStatement ->
                    preparedStatement.setString(1, clientId)
                    preparedStatement.setLong(2, currentTimeMillis)
                    preparedStatement.setInt(3, limit)
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        messages.add(BrokerMessage(
                            messageUuid = resultSet.getString(1),
                            messageId = resultSet.getInt(2),
                            topicName = resultSet.getString(3),
                            payload = resultSet.getBytes(4),
                            qosLevel = resultSet.getInt(5),
                            isRetain = resultSet.getBoolean(6),
                            isDup = false,
                            isQueued = true,
                            clientId = resultSet.getString(7),
                            time = java.time.Instant.ofEpochMilli(resultSet.getLong(8)),
                            messageExpiryInterval = resultSet.getLong(9).let { if (resultSet.wasNull()) null else it }
                        ))
                    }
                }
                if (messages.isNotEmpty()) {
                    connection.prepareStatement(markSql).use { preparedStatement ->
                        preparedStatement.setString(1, clientId)
                        preparedStatement.setArray(2, connection.createArrayOf("text", messages.map { it.messageUuid }.toTypedArray()))
                        preparedStatement.executeUpdate()
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

    override fun markMessageInFlight(clientId: String, messageUuid: String) {
        val sql = "UPDATE $queuedMessagesClientsTableName SET status = 1 WHERE client_id = ? AND message_uuid = ? AND status = 0"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, clientId)
                    preparedStatement.setString(2, messageUuid)
                    preparedStatement.executeUpdate()
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
        val sql = "UPDATE $queuedMessagesClientsTableName SET status = 1 WHERE client_id = ? AND message_uuid = ANY (?) AND status = 0"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, clientId)
                    preparedStatement.setArray(2, connection.createArrayOf("text", messageUuids.toTypedArray()))
                    preparedStatement.executeUpdate()
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            try { db.connection?.rollback() } catch (_: SQLException) {}
            logger.warning("Error marking messages in-flight [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun markMessageDelivered(clientId: String, messageUuid: String) {
        val sql = "UPDATE $queuedMessagesClientsTableName SET status = 2 WHERE client_id = ? AND message_uuid = ? AND status = 1"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, clientId)
                    preparedStatement.setString(2, messageUuid)
                    preparedStatement.executeUpdate()
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            try { db.connection?.rollback() } catch (_: SQLException) {}
            logger.warning("Error marking message delivered [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun resetInFlightMessages(clientId: String) {
        val sql = "UPDATE $queuedMessagesClientsTableName SET status = 0 WHERE client_id = ? AND status = 1"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, clientId)
                    preparedStatement.executeUpdate()
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            try { db.connection?.rollback() } catch (_: SQLException) {}
            logger.warning("Error resetting in-flight messages [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun purgeDeliveredMessages(): Int {
        val sql = "DELETE FROM $queuedMessagesClientsTableName WHERE status = 2"
        return try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    val count = preparedStatement.executeUpdate()
                    connection.commit()
                    count
                }
            } ?: 0
        } catch (e: SQLException) {
            try { db.connection?.rollback() } catch (_: SQLException) {}
            logger.warning("Error purging delivered messages [${e.message}] [${Utils.getCurrentFunctionName()}]")
            0
        }
    }

    override fun purgeExpiredMessages(): Int {
        val currentTimeMillis = System.currentTimeMillis()

        // Delete expired message client mappings
        val sql = """
            DELETE FROM $queuedMessagesClientsTableName
            WHERE message_uuid IN (
                SELECT message_uuid FROM $queuedMessagesTableName
                WHERE message_expiry_interval IS NOT NULL
                AND creation_time IS NOT NULL
                AND (($currentTimeMillis - creation_time) / 1000) >= message_expiry_interval
            )
        """

        return try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    val count = preparedStatement.executeUpdate()
                    connection.commit()
                    if (count > 0) {
                        logger.fine { "Purged $count expired messages" }
                    }
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
        val batchSize = 5000
        val delayBetweenBatchesMs = 100L
        var totalDeleted = 0
        val startTime = System.currentTimeMillis()

        // Use batched deletion with NOT EXISTS for better performance
        val sql = """
            DELETE FROM $queuedMessagesTableName
            WHERE message_uuid IN (
                SELECT qm.message_uuid FROM $queuedMessagesTableName qm
                WHERE NOT EXISTS (
                    SELECT 1 FROM $queuedMessagesClientsTableName qmc
                    WHERE qmc.message_uuid = qm.message_uuid
                )
                LIMIT ?
            )
        """.trimIndent()

        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    var deleted: Int
                    do {
                        preparedStatement.setInt(1, batchSize)
                        deleted = preparedStatement.executeUpdate()
                        connection.commit()
                        totalDeleted += deleted

                        if (deleted > 0) {
                            logger.fine { "Purge batch: deleted $deleted orphaned messages (total: $totalDeleted) [${Utils.getCurrentFunctionName()}]" }
                            if (deleted == batchSize) {
                                Thread.sleep(delayBetweenBatchesMs)
                            }
                        }
                    } while (deleted == batchSize)
                }
            } ?: logger.warning("No database connection available for purging queued messages [${Utils.getCurrentFunctionName()}]")

            val duration = (System.currentTimeMillis() - startTime) / 1000.0
            if (totalDeleted > 0) {
                logger.info("Purging queued messages finished: deleted $totalDeleted in $duration seconds [${Utils.getCurrentFunctionName()}]")
            } else {
                logger.fine("Purging queued messages finished: no orphaned messages found in $duration seconds [${Utils.getCurrentFunctionName()}]")
            }
        } catch (e: SQLException) {
            try { db.connection?.rollback() } catch (_: SQLException) {}
            logger.warning("Error at purging queued messages [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun deleteClientMessages(clientId: String) {
        val sql = "DELETE FROM $queuedMessagesClientsTableName WHERE client_id = ?"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, clientId)
                    preparedStatement.executeUpdate()
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            try { db.connection?.rollback() } catch (_: SQLException) {}
            logger.warning("Error deleting client messages [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun countQueuedMessages(): Long {
        val sql = "SELECT COUNT(*) FROM $queuedMessagesTableName"
        return try {
            DriverManager.getConnection(url, username, password).use { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.executeQuery().use { resultSet ->
                        if (resultSet.next()) {
                            resultSet.getLong(1)
                        } else {
                            0L
                        }
                    }
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error counting queued messages [${e.message}] [${Utils.getCurrentFunctionName()}]")
            0L
        }
    }

    override fun countQueuedMessagesForClient(clientId: String): Long {
        val sql = "SELECT COUNT(*) FROM $queuedMessagesClientsTableName WHERE client_id = ? AND status < 2"
        return try {
            DriverManager.getConnection(url, username, password).use { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, clientId)
                    preparedStatement.executeQuery().use { resultSet ->
                        if (resultSet.next()) {
                            resultSet.getLong(1)
                        } else {
                            0L
                        }
                    }
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error counting queued messages for client $clientId [${e.message}] [${Utils.getCurrentFunctionName()}]")
            0L
        }
    }
}
