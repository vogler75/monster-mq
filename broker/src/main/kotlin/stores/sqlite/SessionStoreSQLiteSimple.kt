package at.rocworks.stores.sqlite

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttSubscription
import at.rocworks.stores.ISessionStore
import at.rocworks.stores.SessionStoreType
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import io.vertx.core.json.JsonArray
import java.sql.*
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit

class SessionStoreSQLiteSimple(
    private val dbPath: String
): AbstractVerticle(), ISessionStore {
    private val logger = Utils.getLogger(this::class.java)

    private val sessionsTableName = "Sessions"
    private val subscriptionsTableName = "Subscriptions"
    private val queuedMessagesTableName = "QueuedMessages"
    private val queuedMessagesClientsTableName = "QueuedMessagesClients"

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun getType(): SessionStoreType = SessionStoreType.SQLITE

    override fun start(startPromise: Promise<Void>) {
        // Deploy SQLiteVerticle if not already deployed
        vertx.deployVerticle(SQLiteVerticle()) { deployment ->
            if (deployment.succeeded()) {
                // Initialize database tables through event bus
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

                val request = JsonObject()
                    .put("dbPath", dbPath)
                    .put("initSql", createTableSQL)

                vertx.eventBus().request<JsonObject>(SQLiteVerticle.EB_INIT_DB, request) { result ->
                    if (result.succeeded()) {
                        logger.info("SQLite session tables are ready [start]")
                        startPromise.complete()
                    } else {
                        logger.severe("Failed to initialize SQLite session tables: ${result.cause()?.message}")
                        startPromise.fail(result.cause())
                    }
                }
            } else {
                startPromise.fail(deployment.cause())
            }
        }
    }

    override fun iterateSubscriptions(callback: (topic: String, clientId: String, qos: Int)->Unit) {
        SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
            val sql = "SELECT client_id, topic, qos FROM $subscriptionsTableName"
            connection.prepareStatement(sql).use { preparedStatement ->
                val resultSet = preparedStatement.executeQuery()
                while (resultSet.next()) {
                    val clientId = resultSet.getString(1)
                    val topic = resultSet.getString(2)
                    val qos = MqttQoS.valueOf(resultSet.getInt(3))
                    callback(topic, clientId, qos.value())
                }
            }
        }.onFailure { e ->
            logger.warning("Error fetching subscriptions: ${e.message} [iterateSubscriptions]")
        }
    }

    override fun iterateOfflineClients(callback: (clientId: String)->Unit) {
        SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
            "SELECT client_id FROM $sessionsTableName WHERE connected = 0 AND clean_session = 0".let { sql ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        callback(resultSet.getString(1))
                    }
                }
            }
        }.onFailure { e ->
            logger.warning("Error at fetching offline clients [${e.message}] [iterateOfflineClients]")
        }
    }

    override fun iterateNodeClients(nodeId: String, callback: (clientId: String, cleanSession: Boolean, lastWill: MqttMessage) -> Unit) {
        SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
            ("SELECT client_id, clean_session, last_will_topic, last_will_message, last_will_qos, last_will_retain "+
            "FROM $sessionsTableName WHERE node_id = ?").let { sql ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, nodeId)
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        val clientId = resultSet.getString(1)
                        val cleanSession = resultSet.getBoolean(2)
                        val topic = resultSet.getString(3)
                        val payload = resultSet.getBytes(4)
                        val qos = resultSet.getInt(5)
                        val retained = resultSet.getBoolean(6)
                        callback(
                            clientId,
                            cleanSession,
                            MqttMessage(
                                messageId = 0,
                                topicName = topic,
                                payload = payload,
                                qosLevel = qos,
                                isRetain = retained,
                                isDup = false,
                                isQueued = false,
                                clientId = clientId
                            )
                        )
                    }
                }
            }
        }.onFailure { e ->
            logger.warning("Error at fetching node clients [${e.message}] [iterateNodeClients]")
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
        
        val request = JsonObject()
            .put("dbPath", dbPath)
            .put("sql", sql)
            .put("params", params)
            .put("transaction", true)
        
        // Send async request via event bus
        vertx.eventBus().request<JsonObject>(SQLiteVerticle.EB_EXECUTE_UPDATE, request) { result ->
            if (result.succeeded()) {
                logger.finest { "Client [$clientId] session data stored successfully" }
            } else {
                logger.warning("Error storing client [$clientId] session data: ${result.cause()?.message}")
            }
        }
    }

    override fun setConnected(clientId: String, connected: Boolean) {
        val sql = "UPDATE $sessionsTableName SET connected = ?, update_time = CURRENT_TIMESTAMP WHERE client_id = ?"
        val params = JsonArray().add(connected).add(clientId)
        
        val request = JsonObject()
            .put("dbPath", dbPath)
            .put("sql", sql)
            .put("params", params)
            .put("transaction", true)
        
        // Send async request via event bus
        vertx.eventBus().request<JsonObject>(SQLiteVerticle.EB_EXECUTE_UPDATE, request) { result ->
            if (result.succeeded()) {
                logger.fine { "Client [$clientId] connected status set to [$connected]" }
            } else {
                logger.warning("Error setting connected status for client [$clientId]: ${result.cause()?.message}")
            }
        }
    }

    override fun isConnected(clientId: String): Boolean {
        return try {
            // Use event bus for query - this is still blocking but on worker thread
            val params = JsonArray().add(clientId)
            val request = JsonObject()
                .put("dbPath", dbPath)
                .put("sql", "SELECT connected FROM $sessionsTableName WHERE client_id = ?")
                .put("params", params)
            
            val message = vertx.eventBus()
                .request<JsonObject>(SQLiteVerticle.EB_EXECUTE_QUERY, request)
                .toCompletionStage()
                .toCompletableFuture()
                .get(1000, TimeUnit.MILLISECONDS) // 1 second timeout
            
            val response = message.body()
            if (response.getBoolean("success", false)) {
                val results = response.getJsonArray("results")
                if (results.size() > 0) {
                    val row = results.getJsonObject(0)
                    row.getBoolean("connected", false)
                } else {
                    false
                }
            } else {
                logger.warning("Failed to check if client [$clientId] is connected: query failed")
                false
            }
        } catch (e: Exception) {
            logger.warning("Error checking if client [$clientId] is connected: ${e.message}")
            false
        }
    }

    override fun isPresent(clientId: String): Boolean {
        return try {
            // Use event bus for query - this is still blocking but on worker thread
            val params = JsonArray().add(clientId)
            val request = JsonObject()
                .put("dbPath", dbPath)
                .put("sql", "SELECT client_id FROM $sessionsTableName WHERE client_id = ?")
                .put("params", params)
            
            val message = vertx.eventBus()
                .request<JsonObject>(SQLiteVerticle.EB_EXECUTE_QUERY, request)
                .toCompletionStage()
                .toCompletableFuture()
                .get(1000, TimeUnit.MILLISECONDS) // 1 second timeout
            
            val response = message.body()
            if (response.getBoolean("success", false)) {
                val results = response.getJsonArray("results")
                results.size() > 0
            } else {
                logger.warning("Failed to check if client [$clientId] is present: query failed")
                false
            }
        } catch (e: Exception) {
            logger.warning("Error checking if client [$clientId] is present: ${e.message}")
            false
        }
    }

    override fun setLastWill(clientId: String, message: MqttMessage?) {
        logger.fine { "Setting last will for client [$clientId] [setLastWill]" }
        
        // Use UPSERT pattern to ensure session exists before setting last will
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
        
        // Send request to SQLiteVerticle via event bus (fire and forget for now)
        val params = JsonArray()
            .add(clientId)
            .add(message?.topicName)
            .add(message?.payload)
            .add(message?.qosLevel)
            .add(message?.isRetain)
        
        val request = JsonObject()
            .put("dbPath", dbPath)
            .put("sql", sql)
            .put("params", params)
            .put("transaction", true)
        
        // Send async request - for now we don't wait for response since ISessionStore expects void
        vertx.eventBus().request<JsonObject>(SQLiteVerticle.EB_EXECUTE_UPDATE, request) { result ->
            if (result.succeeded()) {
                logger.fine { "Last will set successfully for client [$clientId]" }
            } else {
                logger.warning("Error setting last will for client [$clientId]: ${result.cause()?.message}")
            }
        }
    }

    override fun addSubscriptions(subscriptions: List<MqttSubscription>) {
        val sql = "INSERT INTO $subscriptionsTableName (client_id, topic, qos, wildcard) VALUES (?, ?, ?, ?) "+
                  "ON CONFLICT (client_id, topic) DO UPDATE SET qos = excluded.qos"
        
        SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
            connection.autoCommit = false
            connection.prepareStatement(sql).use { preparedStatement ->
                subscriptions.forEach { subscription ->
                    val topicJson = Utils.getTopicLevels(subscription.topicName).joinToString("/")
                    preparedStatement.setString(1, subscription.clientId)
                    preparedStatement.setString(2, topicJson)
                    preparedStatement.setInt(3, subscription.qos.value())
                    preparedStatement.setBoolean(4, Utils.isWildCardTopic(subscription.topicName))
                    preparedStatement.addBatch()
                }
                preparedStatement.executeBatch()
            }
            connection.commit()
        }.onFailure { e ->
            logger.warning("Error at inserting subscription [${e.message}] SQL: [$sql] [addSubscriptions]")
        }
    }

    override fun delSubscriptions(subscriptions: List<MqttSubscription>) {
        val sql = "DELETE FROM $subscriptionsTableName WHERE client_id = ? AND topic = ?"
        SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
            connection.autoCommit = false
            connection.prepareStatement(sql).use { preparedStatement ->
                subscriptions.forEach { subscription ->
                    val topicJson = Utils.getTopicLevels(subscription.topicName).joinToString("/")
                    preparedStatement.setString(1, subscription.clientId)
                    preparedStatement.setString(2, topicJson)
                    preparedStatement.addBatch()
                }
                preparedStatement.executeBatch()
            }
            connection.commit()
        }.onFailure { e ->
            logger.warning("Error at removing subscription [${e.message}] SQL: [$sql] [delSubscriptions]")
        }
    }

    override fun delClient(clientId: String, callback: (MqttSubscription)->Unit) {
        SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
            connection.autoCommit = false
            // SQLite doesn't support RETURNING, so we need to SELECT first
            "SELECT topic, qos FROM $subscriptionsTableName WHERE client_id = ?".let { sql ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, clientId)
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        val topic = resultSet.getString(1)
                        val qos = MqttQoS.valueOf(resultSet.getInt(2))
                        callback(MqttSubscription(clientId, topic, qos))
                    }
                }
            }

            // Then delete subscriptions
            "DELETE FROM $subscriptionsTableName WHERE client_id = ?".let { sql ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, clientId)
                    preparedStatement.executeUpdate()
                }
            }

            // Remove the client from the session table
            "DELETE FROM $sessionsTableName WHERE client_id = ?".let { sql ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, clientId)
                    preparedStatement.executeUpdate()
                }
            }

            // Remove the client from the queued clients table
            "DELETE FROM $queuedMessagesClientsTableName WHERE client_id = ?".let { sql ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, clientId)
                    preparedStatement.executeUpdate()
                }
            }

            connection.commit()
        }.onFailure { e ->
            logger.warning("Error at removing client [${e.message}] [delClient]")
        }
    }

    override fun enqueueMessages(messages: List<Pair<MqttMessage, List<String>>>) {
        val sql1 = "INSERT INTO $queuedMessagesTableName "+
                   "(message_uuid, message_id, topic, payload, qos, retained, client_id) VALUES (?, ?, ?, ?, ?, ?, ?) "+
                   "ON CONFLICT (message_uuid) DO NOTHING"
        val sql2 = "INSERT INTO $queuedMessagesClientsTableName "+
                   "(client_id, message_uuid) VALUES (?, ?) "+
                   "ON CONFLICT (client_id, message_uuid) DO NOTHING"
        
        SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
            connection.autoCommit = false
            connection.prepareStatement(sql1).use { preparedStatement1 ->
               connection.prepareStatement(sql2).use { preparedStatement2 ->
                   messages.forEach { message ->
                       // Add message
                       preparedStatement1.setString(1, message.first.messageUuid)
                       preparedStatement1.setInt(2, message.first.messageId)
                       preparedStatement1.setString(3, message.first.topicName)
                       preparedStatement1.setBytes(4, message.first.payload)
                       preparedStatement1.setInt(5, message.first.qosLevel)
                       preparedStatement1.setBoolean(6, message.first.isRetain)
                       preparedStatement1.setString(7, message.first.clientId)
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
        }.onFailure { e ->
            logger.warning("Error at inserting queued message [${e.message}] [enqueueMessages]")
        }
    }

    override fun dequeueMessages(clientId: String, callback: (MqttMessage)->Boolean) {
        val sql = "SELECT m.message_uuid, m.message_id, m.topic, m.payload, m.qos, m.retained, m.client_id "+
                  "FROM $queuedMessagesTableName AS m JOIN $queuedMessagesClientsTableName AS c ON m.message_uuid = c.message_uuid "+
                  "WHERE c.client_id = ? "+
                  "ORDER BY m.message_uuid"
        SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
            connection.prepareStatement(sql).use { preparedStatement ->
                preparedStatement.setString(1, clientId)
                val resultSet = preparedStatement.executeQuery()
                while (resultSet.next()) {
                    val messageUuid = resultSet.getString(1)
                    val messageId = resultSet.getInt(2)
                    val topic = resultSet.getString(3)
                    val payload = resultSet.getBytes(4)
                    val qos = resultSet.getInt(5)
                    val retained = resultSet.getBoolean(6)
                    val clientIdPublisher = resultSet.getString(7)
                    val continueProcessing = callback(
                        MqttMessage(
                            messageUuid = messageUuid,
                            messageId = messageId,
                            topicName = topic,
                            payload = payload,
                            qosLevel = qos,
                            isRetain = retained,
                            isDup = false,
                            isQueued = true,
                            clientId = clientIdPublisher
                        )
                    )
                    if (!continueProcessing) break
                }
            }
        }.onFailure { e ->
            logger.warning("Error at fetching queued message [${e.message}] [dequeueMessages]")
        }
    }

    override fun removeMessages(messages: List<Pair<String, String>>) { // clientId, messageUuid
        val sql = "DELETE FROM $queuedMessagesClientsTableName WHERE client_id = ? AND message_uuid = ?"
        SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
            connection.autoCommit = false
            connection.prepareStatement(sql).use { preparedStatement ->
                messages.forEach { (clientId, messageUuid) ->
                    preparedStatement.setString(1, clientId)
                    preparedStatement.setString(2, messageUuid)
                    preparedStatement.addBatch()
                }
                preparedStatement.executeBatch()
            }
            connection.commit()
        }.onFailure { e ->
            logger.warning("Error at removing dequeued message [${e.message}] [removeMessages]")
        }
    }

    override fun purgeQueuedMessages() {
        val sql = "DELETE FROM $queuedMessagesTableName WHERE message_uuid NOT IN " +
                "(SELECT message_uuid FROM $queuedMessagesClientsTableName)"
        val startTime = System.currentTimeMillis()
        SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
            connection.prepareStatement(sql).use { preparedStatement ->
                preparedStatement.executeUpdate()
                val endTime = System.currentTimeMillis()
                val duration = (endTime - startTime) / 1000.0
                logger.info("Purging queued messages finished in $duration seconds [purgeQueuedMessages]")
            }
        }.onFailure { e ->
            logger.warning("Error at purging queued messages [${e.message}] [purgeQueuedMessages]")
        }
    }

    override fun purgeSessions() {
        val startTime = System.currentTimeMillis()
        SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
            val statement = connection.createStatement()
            statement.executeUpdate(
                "DELETE FROM $sessionsTableName WHERE clean_session = 1"
            )
            statement.executeUpdate(
                "DELETE FROM $subscriptionsTableName WHERE client_id NOT IN " +
                        "(SELECT client_id FROM $sessionsTableName)"
            )
            statement.executeUpdate(
                "UPDATE $sessionsTableName SET connected = 0"
            )
            val endTime = System.currentTimeMillis()
            val duration = (endTime - startTime) / 1000.0
            logger.info("Purging sessions finished in $duration seconds [purgeSessions]")
        }.onFailure { e ->
            logger.warning("Error at purging sessions [${e.message}] [purgeSessions]")
        }
    }
}