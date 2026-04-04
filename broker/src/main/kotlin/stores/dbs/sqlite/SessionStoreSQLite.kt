package at.rocworks.stores.sqlite

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.data.MqttSubscription
import at.rocworks.stores.ISessionStoreSync
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
): AbstractVerticle(), ISessionStoreSync {
    private val logger = Utils.getLogger(this::class.java)
    private lateinit var sqlClient: SQLiteClient

    private val sessionsTableName = "sessions"
    private val subscriptionsTableName = "subscriptions"


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
                no_local INTEGER DEFAULT 0,
                retain_handling INTEGER DEFAULT 0,
                retain_as_published INTEGER DEFAULT 0,
                PRIMARY KEY (client_id, topic)
            );
            """.trimIndent())
            .add("CREATE INDEX IF NOT EXISTS ${subscriptionsTableName}_topic_idx ON $subscriptionsTableName (topic);")
            .add("CREATE INDEX IF NOT EXISTS ${subscriptionsTableName}_wildcard_idx ON $subscriptionsTableName (wildcard) WHERE wildcard = 1;")

        sqlClient.initDatabase(createTableSQL).onComplete { result ->
            if (result.succeeded()) {
                // Migration: Add retain_as_published column if it doesn't exist
                val checkColumnSql = "PRAGMA table_info($subscriptionsTableName)"
                sqlClient.executeQuery(checkColumnSql, JsonArray()).onComplete { queryResult ->
                    if (queryResult.succeeded()) {
                        val rows = queryResult.result()
                        val hasColumn = rows.any { row ->
                            (row as JsonObject).getString("name") == "retain_as_published"
                        }
                        if (!hasColumn) {
                            logger.info("Migrating subscriptions table: adding retain_as_published column")
                            sqlClient.executeUpdate(
                                "ALTER TABLE $subscriptionsTableName ADD COLUMN retain_as_published INTEGER DEFAULT 0",
                                JsonArray()
                            ).onComplete { updateResult ->
                                if (updateResult.succeeded()) {
                                    logger.info("Successfully added retain_as_published column")
                                } else {
                                    logger.warning("Migration warning (may be safe to ignore if column exists): ${updateResult.cause()?.message}")
                                }
                                logger.info("SQLite session tables are ready [start]")
                                startPromise.complete()
                            }
                        } else {
                            logger.fine("Column retain_as_published already exists, skipping migration")
                            logger.info("SQLite session tables are ready [start]")
                            startPromise.complete()
                        }
                    } else {
                        logger.warning("Could not check for retain_as_published column: ${queryResult.cause()?.message}")
                        logger.info("SQLite session tables are ready [start]")
                        startPromise.complete()
                    }
                }
            } else {
                logger.severe("Failed to initialize SQLite session tables: ${result.cause()?.message}")
                startPromise.fail(result.cause())
            }
        }
    }

    override fun iterateSubscriptions(callback: (topic: String, clientId: String, qos: Int, noLocal: Boolean, retainHandling: Int, retainAsPublished: Boolean) -> Unit) {
        val sql = "SELECT client_id, topic, qos, no_local, retain_handling, retain_as_published FROM $subscriptionsTableName"
        try {
            val results = sqlClient.executeQuerySync(sql)
            results.forEach { row ->
                val rowObj = row as JsonObject
                val clientId = rowObj.getString("client_id")
                val topic = rowObj.getString("topic")
                val qos = rowObj.getInteger("qos")
                val noLocal = rowObj.getInteger("no_local", 0) == 1  // SQLite stores as 0/1
                val retainHandling = rowObj.getInteger("retain_handling", 0)
                val retainAsPublished = rowObj.getInteger("retain_as_published", 0) == 1  // SQLite stores as 0/1
                callback(topic, clientId, qos, noLocal, retainHandling, retainAsPublished)
            }
        } catch (e: Exception) {
            logger.warning("Error fetching subscriptions: ${e.message} [iterateSubscriptions]")
        }
    }

    override fun iterateOfflineClients(callback: (clientId: String) -> Unit) {
        val sql = "SELECT client_id FROM $sessionsTableName WHERE connected = false"
        try {
            val results = sqlClient.executeQuerySync(sql)
            results.forEach { row ->
                val rowObj = row as JsonObject
                val clientId = rowObj.getString("client_id")
                callback(clientId)
            }
        } catch (e: Exception) {
            logger.warning("Error fetching offline clients: ${e.message}")
        }
    }

    override fun iterateConnectedClients(callback: (clientId: String, nodeId: String) -> Unit) {
        logger.warning("iterateConnectedClients feature not implemented yet for SQLite [${Utils.getCurrentFunctionName()}]")
    }

    override fun iterateAllSessions(callback: (clientId: String, nodeId: String, connected: Boolean, cleanSession: Boolean) -> Unit) {
        val sql = "SELECT client_id, node_id, connected, clean_session FROM $sessionsTableName"
        try {
            val results = sqlClient.executeQuerySync(sql)
            results.forEach { row ->
                val rowObj = row as JsonObject
                val clientId = rowObj.getString("client_id")
                val nodeId = rowObj.getString("node_id") ?: ""
                val connected = rowObj.getBoolean("connected") ?: false
                val cleanSession = rowObj.getBoolean("clean_session") ?: true
                callback(clientId, nodeId, connected, cleanSession)
            }
        } catch (e: Exception) {
            logger.warning("Error at fetching all sessions [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun iterateNodeClients(nodeId: String, callback: (clientId: String, cleanSession: Boolean, lastWill: BrokerMessage) -> Unit) {
        val sql = "SELECT client_id, clean_session, last_will_topic, last_will_message, last_will_qos, last_will_retain FROM $sessionsTableName WHERE node_id = ?"
        val params = JsonArray().add(nodeId)
        try {
            val results = sqlClient.executeQuerySync(sql, params)
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
                    BrokerMessage(
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
                    BrokerMessage(
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
        } catch (e: Exception) {
            logger.warning("Error fetching node clients: ${e.message}")
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

        val result = sqlClient.executeUpdateSync(sql, params)
        logger.finest { "Client [$clientId] session stored successfully. Rows affected: $result" }
    }

    override fun setConnected(clientId: String, connected: Boolean) {
        val sql = "UPDATE $sessionsTableName SET connected = ?, update_time = CURRENT_TIMESTAMP WHERE client_id = ?"
        val params = JsonArray().add(connected).add(clientId)
        val result = sqlClient.executeUpdateSync(sql, params)
        logger.finest { "Client [$clientId] connection status updated to [$connected]. Rows affected: $result" }
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

    override fun setLastWill(clientId: String, message: BrokerMessage?) {
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

        val result = sqlClient.executeUpdateSync(sql, params)
        logger.finest { "Last will for client [$clientId] stored successfully. Rows affected: $result" }
    }

    override fun addSubscriptions(subscriptions: List<MqttSubscription>) {
        if (subscriptions.isEmpty()) return

        val sql = "INSERT INTO $subscriptionsTableName (client_id, topic, qos, wildcard, no_local, retain_handling, retain_as_published) VALUES (?, ?, ?, ?, ?, ?, ?) "+
                  "ON CONFLICT (client_id, topic) DO UPDATE SET qos = excluded.qos, no_local = excluded.no_local, retain_handling = excluded.retain_handling, retain_as_published = excluded.retain_as_published"

        val batchParams = JsonArray()
        subscriptions.forEach { subscription ->
            val params = JsonArray()
                .add(subscription.clientId)
                .add(subscription.topicName)
                .add(subscription.qos.value())
                .add(isWildcardTopic(subscription.topicName))
                .add(if (subscription.noLocal) 1 else 0)
                .add(subscription.retainHandling)
                .add(if (subscription.retainAsPublished) 1 else 0)
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

        try {
            val results = sqlClient.executeQuerySync(selectSql, params)
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

            // Then delete in order: subscriptions, then session
            val deleteSql = listOf(
                "DELETE FROM $subscriptionsTableName WHERE client_id = ?",
                "DELETE FROM $sessionsTableName WHERE client_id = ?"
            )

            deleteSql.forEach { sql ->
                val deleteParams = JsonArray().add(clientId)
                sqlClient.executeUpdateAsync(sql, deleteParams)
            }
        } catch (e: Exception) {
            logger.warning("Error fetching subscriptions for client deletion: ${e.message}")
        }
    }

    override fun purgeSessions() {
        val startTime = System.currentTimeMillis()
        // Delete clean sessions (consistent with PostgreSQL/CrateDB behavior)
        sqlClient.executeUpdateSync(
            "DELETE FROM $sessionsTableName WHERE clean_session = 1",
            JsonArray()
        )
        // Delete orphaned subscriptions
        sqlClient.executeUpdateSync(
            """
            DELETE FROM $subscriptionsTableName
            WHERE client_id NOT IN (
                SELECT client_id FROM $sessionsTableName
            )
            """.trimIndent(),
            JsonArray()
        )
        // Mark all sessions as disconnected
        sqlClient.executeUpdateSync(
            "UPDATE $sessionsTableName SET connected = 0",
            JsonArray()
        )
        val duration = (System.currentTimeMillis() - startTime) / 1000.0
        logger.fine { "Purging sessions finished in $duration seconds" }
    }

    private fun isWildcardTopic(topicName: String): Boolean {
        return topicName.contains('+') || topicName.contains('#')
    }
}
