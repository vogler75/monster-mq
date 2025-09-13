package at.rocworks.stores.sqlite

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttSubscription
import at.rocworks.stores.ISessionStore
import at.rocworks.stores.SessionStoreType
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/**
 * Clean SQLiteVerticle-only implementation of SessionStore
 * No SharedSQLiteConnection - uses only event bus communication
 */
class SessionStoreSQLite(
    private val dbPath: String
): AbstractVerticle(), ISessionStore {
    private val logger = Utils.getLogger(this::class.java)
    private lateinit var sqlClient: SQLiteClient

    private val sessionsTableName = "sessions"
    private val subscriptionsTableName = "subscriptions"
    private val queuedMessagesTableName = "queuedmessages"
    private val queuedMessagesClientsTableName = "queuedmessagesclients"

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun getType(): SessionStoreType = SessionStoreType.SQLITE

    override fun start(startPromise: Promise<Void>) {
        // Initialize SQLiteClient - assumes SQLiteVerticle is already deployed by Monster.kt
        sqlClient = SQLiteClient(vertx, dbPath)
        
        // Initialize database tables
        val createTableSQL = JsonArray()
            .add("""
            CREATE TABLE IF NOT EXISTS $sessionsTableName (
                client_id TEXT PRIMARY KEY,
                node_id TEXT,
                clean_session BOOLEAN,
                connected BOOLEAN,
                update_time TEXT DEFAULT CURRENT_TIMESTAMP,
                information TEXT,
                last_will_topic TEXT,
                last_will_message BLOB,
                last_will_qos INTEGER,
                last_will_retain BOOLEAN
            );
            """.trimIndent())
            .add("""
            CREATE TABLE IF NOT EXISTS $subscriptionsTableName (
                client_id TEXT,
                topic TEXT,
                qos INTEGER,
                wildcard BOOLEAN,
                PRIMARY KEY (client_id, topic)
            );
            """.trimIndent())
            .add("""
            CREATE TABLE IF NOT EXISTS $queuedMessagesTableName (
                message_uuid TEXT PRIMARY KEY,
                message_id INTEGER,                    
                topic TEXT,                    
                payload BLOB,
                qos INTEGER,
                retained BOOLEAN,
                client_id TEXT
            );             
            """.trimIndent())
            .add("""
            CREATE TABLE IF NOT EXISTS $queuedMessagesClientsTableName (
                client_id TEXT,                
                message_uuid TEXT,
                PRIMARY KEY (client_id, message_uuid)
            );
            """.trimIndent())
            .add("CREATE INDEX IF NOT EXISTS ${subscriptionsTableName}_topic_idx ON $subscriptionsTableName (topic);")
            .add("CREATE INDEX IF NOT EXISTS ${subscriptionsTableName}_wildcard_idx ON $subscriptionsTableName (wildcard) WHERE wildcard = 1;")

        sqlClient.initDatabase(createTableSQL).onComplete { result ->
            if (result.succeeded()) {
                logger.info("SQLite session tables are ready [start]")
                startPromise.complete()
            } else {
                logger.severe("Failed to initialize SQLite session tables: ${result.cause()?.message}")
                startPromise.fail(result.cause())
            }
        }
    }

    override fun iterateSubscriptions(callback: (topic: String, clientId: String, qos: Int) -> Unit) {
        val sql = "SELECT client_id, topic, qos FROM $subscriptionsTableName"
        sqlClient.executeQuery(sql).onComplete { result ->
            if (result.succeeded()) {
                val results = result.result()
                results.forEach { row ->
                    val rowObj = row as JsonObject
                    val clientId = rowObj.getString("client_id")
                    val topic = rowObj.getString("topic")
                    val qos = rowObj.getInteger("qos")
                    callback(topic, clientId, qos)
                }
            } else {
                logger.warning("Error fetching subscriptions: ${result.cause()?.message} [iterateSubscriptions]")
            }
        }
    }

    override fun iterateOfflineClients(callback: (clientId: String) -> Unit) {
        val sql = "SELECT client_id FROM $sessionsTableName WHERE connected = false"
        sqlClient.executeQuery(sql).onComplete { result ->
            if (result.succeeded()) {
                val results = result.result()
                results.forEach { row ->
                    val rowObj = row as JsonObject
                    val clientId = rowObj.getString("client_id")
                    callback(clientId)
                }
            } else {
                logger.warning("Error fetching offline clients: ${result.cause()?.message}")
            }
        }
    }

    override fun iterateNodeClients(nodeId: String, callback: (clientId: String, cleanSession: Boolean, lastWill: MqttMessage) -> Unit) {
        val sql = "SELECT client_id, clean_session, last_will_topic, last_will_message, last_will_qos, last_will_retain FROM $sessionsTableName WHERE node_id = ?"
        val params = JsonArray().add(nodeId)
        sqlClient.executeQuery(sql, params).onComplete { result ->
            if (result.succeeded()) {
                val results = result.result()
                results.forEach { row ->
                    val rowObj = row as JsonObject
                    val clientId = rowObj.getString("client_id")
                    val cleanSession = rowObj.getBoolean("clean_session", false)
                    
                    // Reconstruct last will message
                    val lastWillTopic = rowObj.getString("last_will_topic")
                    val lastWill = if (lastWillTopic != null) {
                        val payload = rowObj.getBinary("last_will_message") ?: ByteArray(0)
                        val qos = rowObj.getInteger("last_will_qos", 0)
                        val retain = rowObj.getBoolean("last_will_retain", false)
                        MqttMessage(
                            messageUuid = "",
                            messageId = 0,
                            topicName = lastWillTopic,
                            payload = payload,
                            qosLevel = qos,
                            isRetain = retain,
                            isQueued = false,
                            clientId = clientId,
                            isDup = false
                        )
                    } else {
                        MqttMessage(
                            messageUuid = "",
                            messageId = 0,
                            topicName = "",
                            payload = ByteArray(0),
                            qosLevel = 0,
                            isRetain = false,
                            isQueued = false,
                            clientId = clientId,
                            isDup = false
                        )
                    }
                    
                    callback(clientId, cleanSession, lastWill)
                }
            } else {
                logger.warning("Error fetching node clients: ${result.cause()?.message}")
            }
        }
    }

    override fun setClient(clientId: String, nodeId: String, cleanSession: Boolean, connected: Boolean, information: JsonObject) {
        logger.finest { "Put client [$clientId] cleanSession [$cleanSession] connected [$connected] [setClient]" }
        val sql = "INSERT INTO $sessionsTableName (client_id, node_id, clean_session, connected, information) VALUES (?, ?, ?, ?, ?) "+
                  "ON CONFLICT (client_id) DO UPDATE "+
                  "SET node_id = excluded.node_id, "+
                  "clean_session = excluded.clean_session, "+
                  "connected = excluded.connected, "+
                  "information = excluded.information, "+
                  "update_time = CURRENT_TIMESTAMP"
        
        val params = JsonArray()
            .add(clientId)
            .add(nodeId)
            .add(cleanSession)
            .add(connected)
            .add(information.encode())
        
        sqlClient.executeUpdateAsync(sql, params)
    }

    override fun setConnected(clientId: String, connected: Boolean) {
        val sql = "UPDATE $sessionsTableName SET connected = ?, update_time = CURRENT_TIMESTAMP WHERE client_id = ?"
        val params = JsonArray().add(connected).add(clientId)
        sqlClient.executeUpdateAsync(sql, params)
    }

    override fun isConnected(clientId: String): Boolean {
        val sql = "SELECT connected FROM $sessionsTableName WHERE client_id = ?"
        val params = JsonArray().add(clientId)
        
        val results = sqlClient.executeQuerySync(sql, params)
        return if (results.size() > 0) {
            val row = results.getJsonObject(0)
            row.getBoolean("connected", false)
        } else {
            false
        }
    }

    override fun isPresent(clientId: String): Boolean {
        val sql = "SELECT client_id FROM $sessionsTableName WHERE client_id = ?"
        val params = JsonArray().add(clientId)
        
        val results = sqlClient.executeQuerySync(sql, params)
        return results.size() > 0
    }

    override fun setLastWill(clientId: String, message: MqttMessage?) {
        logger.fine { "Setting last will for client [$clientId] [setLastWill]" }
        
        val sql = """
            INSERT INTO $sessionsTableName (client_id, last_will_topic, last_will_message, last_will_qos, last_will_retain) 
            VALUES (?, ?, ?, ?, ?) 
            ON CONFLICT (client_id) DO UPDATE 
            SET last_will_topic = excluded.last_will_topic,
                last_will_message = excluded.last_will_message, 
                last_will_qos = excluded.last_will_qos,
                last_will_retain = excluded.last_will_retain,
                update_time = CURRENT_TIMESTAMP
        """.trimIndent()
        
        val params = JsonArray()
            .add(clientId)
            .add(message?.topicName)
            .add(message?.payload)
            .add(message?.qosLevel)
            .add(message?.isRetain)
        
        sqlClient.executeUpdateAsync(sql, params)
    }

    override fun addSubscriptions(subscriptions: List<MqttSubscription>) {
        if (subscriptions.isEmpty()) return
        
        val sql = "INSERT INTO $subscriptionsTableName (client_id, topic, qos, wildcard) VALUES (?, ?, ?, ?) "+
                  "ON CONFLICT (client_id, topic) DO UPDATE SET qos = excluded.qos"
        
        val batchParams = JsonArray()
        subscriptions.forEach { subscription ->
            val params = JsonArray()
                .add(subscription.clientId)
                .add(subscription.topicName)
                .add(subscription.qos.value())
                .add(isWildcardTopic(subscription.topicName))
            batchParams.add(params)
        }
        
        sqlClient.executeBatch(sql, batchParams).onComplete { result ->
            if (result.failed()) {
                logger.warning("Error adding subscriptions: ${result.cause()?.message}")
            }
        }
    }

    override fun delSubscriptions(subscriptions: List<MqttSubscription>) {
        if (subscriptions.isEmpty()) return
        
        val sql = "DELETE FROM $subscriptionsTableName WHERE client_id = ? AND topic = ?"
        val batchParams = JsonArray()
        subscriptions.forEach { subscription ->
            val params = JsonArray()
                .add(subscription.clientId)
                .add(subscription.topicName)
            batchParams.add(params)
        }
        
        sqlClient.executeBatch(sql, batchParams).onComplete { result ->
            if (result.failed()) {
                logger.warning("Error deleting subscriptions: ${result.cause()?.message}")
            }
        }
    }

    override fun delClient(clientId: String, callback: (MqttSubscription) -> Unit) {
        // First fetch all subscriptions for this client to call callback
        val selectSql = "SELECT topic, qos FROM $subscriptionsTableName WHERE client_id = ?"
        val params = JsonArray().add(clientId)
        
        sqlClient.executeQuery(selectSql, params).onComplete { result ->
            if (result.succeeded()) {
                val results = result.result()
                results.forEach { row ->
                    val rowObj = row as JsonObject
                    val topic = rowObj.getString("topic")
                    val qos = rowObj.getInteger("qos")
                    val subscription = MqttSubscription(
                        clientId = clientId,
                        topicName = topic,
                        qos = MqttQoS.valueOf(qos)
                    )
                    callback(subscription)
                }
                
                // Then delete in order: subscriptions, queued messages, then session
                val deleteSql = listOf(
                    "DELETE FROM $subscriptionsTableName WHERE client_id = ?",
                    "DELETE FROM $queuedMessagesClientsTableName WHERE client_id = ?", 
                    "DELETE FROM $sessionsTableName WHERE client_id = ?"
                )
                
                deleteSql.forEach { sql ->
                    val deleteParams = JsonArray().add(clientId)
                    sqlClient.executeUpdateAsync(sql, deleteParams)
                }
            } else {
                logger.warning("Error fetching subscriptions for client deletion: ${result.cause()?.message}")
            }
        }
    }

    // Simplified implementations for other methods - can be enhanced later
    override fun enqueueMessages(messages: List<Pair<MqttMessage, List<String>>>) {
        if (messages.isEmpty()) return
        
        val insertMessageSql = """INSERT INTO $queuedMessagesTableName 
                                (message_uuid, message_id, topic, payload, qos, retained, client_id) 
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT (message_uuid) DO NOTHING"""
        
        val insertClientSql = """INSERT INTO $queuedMessagesClientsTableName (client_id, message_uuid) 
                               VALUES (?, ?) ON CONFLICT DO NOTHING"""
        
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
        
        logger.fine("Enqueued ${messages.size} messages for ${messages.sumOf { it.second.size }} client mappings")
    }

    override fun dequeueMessages(clientId: String, callback: (MqttMessage) -> Boolean) {
        val sql = """SELECT m.message_uuid, m.message_id, m.topic, m.payload, m.qos, m.retained, m.client_id
                    FROM $queuedMessagesTableName m
                    JOIN $queuedMessagesClientsTableName c ON m.message_uuid = c.message_uuid
                    WHERE c.client_id = ?
                    ORDER BY c.rowid"""
        
        val params = JsonArray().add(clientId)
        
        sqlClient.executeQuery(sql, params).onComplete { result ->
            if (result.succeeded()) {
                val results = result.result()
                val processedUuids = mutableListOf<String>()
                
                results.forEach { row ->
                    val rowObj = row as JsonObject
                    val messageUuid = rowObj.getString("message_uuid")
                    val messageId = rowObj.getInteger("message_id")
                    val topic = rowObj.getString("topic")
                    val payload = rowObj.getBinary("payload") ?: ByteArray(0)
                    val qos = rowObj.getInteger("qos")
                    val retained = rowObj.getBoolean("retained", false)
                    val originalClientId = rowObj.getString("client_id")
                    
                    val message = MqttMessage(
                        messageUuid = messageUuid,
                        messageId = messageId,
                        topicName = topic,
                        payload = payload,
                        qosLevel = qos,
                        isRetain = retained,
                        isQueued = true,
                        clientId = originalClientId,
                        isDup = false
                    )
                    
                    // Call callback and collect processed messages
                    if (callback(message)) {
                        processedUuids.add(messageUuid)
                    }
                }
                
                // Remove processed messages for this client
                if (processedUuids.isNotEmpty()) {
                    val deleteSql = "DELETE FROM $queuedMessagesClientsTableName WHERE client_id = ? AND message_uuid = ?"
                    val deleteBatch = JsonArray()
                    processedUuids.forEach { uuid ->
                        deleteBatch.add(JsonArray().add(clientId).add(uuid))
                    }
                    sqlClient.executeBatch(deleteSql, deleteBatch)
                }
            } else {
                logger.warning("Error dequeuing messages for client [$clientId]: ${result.cause()?.message}")
            }
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
                logger.fine("Removed ${messages.size} message mappings")
            }
        }
    }

    override fun purgeQueuedMessages() {
        val deleteSql = "DELETE FROM $queuedMessagesTableName"
        sqlClient.executeUpdateAsync(deleteSql, JsonArray())
        logger.fine("Purged all queued messages")
    }

    override fun purgeSessions() {
        val deleteSql = "DELETE FROM $sessionsTableName WHERE connected = false"
        sqlClient.executeUpdateAsync(deleteSql, JsonArray())
        logger.fine("Purged disconnected sessions")
    }
    
    private fun isWildcardTopic(topicName: String): Boolean {
        return topicName.contains('+') || topicName.contains('#')
    }

    override fun countQueuedMessages(): Long {
        val sql = "SELECT COUNT(*) FROM $queuedMessagesTableName"
        return try {
            vertx.executeBlocking<Long>(java.util.concurrent.Callable {
                try {
                    java.sql.DriverManager.getConnection("jdbc:sqlite:monstermq.db").use { connection ->
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
                } catch (e: Exception) {
                    logger.warning("Error counting queued messages: ${e.message}")
                    0L
                }
            }).result()
        } catch (e: Exception) {
            logger.warning("Error counting queued messages: ${e.message}")
            0L
        }
    }

    override fun countQueuedMessagesForClient(clientId: String): Long {
        val sql = "SELECT COUNT(*) FROM $queuedMessagesClientsTableName WHERE client_id = ?"
        return try {
            vertx.executeBlocking<Long>(java.util.concurrent.Callable {
                try {
                    java.sql.DriverManager.getConnection("jdbc:sqlite:monstermq.db").use { connection ->
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
                } catch (e: Exception) {
                    logger.warning("Error counting queued messages for client $clientId: ${e.message}")
                    0L
                }
            }).result()
        } catch (e: Exception) {
            logger.warning("Error counting queued messages for client $clientId: ${e.message}")
            0L
        }
    }
}