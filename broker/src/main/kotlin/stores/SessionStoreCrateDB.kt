package at.rocworks.stores

import at.rocworks.Const
import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttSubscription
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.sql.*
import java.util.concurrent.Callable

class SessionStoreCrateDB(
    private val url: String,
    private val username: String,
    private val password: String
): AbstractVerticle(), ISessionStore {
    private val logger = Utils.getLogger(this::class.java)

    private val sessionsTableName = "Sessions".uppercase()
    private val subscriptionsTableName = "Subscriptions".uppercase()
    private val queuedMessagesTableName = "QueuedMessages".uppercase()
    private val queuedMessagesClientsTableName = "QueuedMessagesClients".uppercase()

    //private var queuedMessageId = 0L

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun getType(): SessionStoreType = SessionStoreType.CRATEDB

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun init(connection: Connection): Future<Void> {
            val promise = Promise.promise<Void>()
            try {
                connection.autoCommit = false

                // Query database type and version
                connection.metaData.let { metaData ->
                    logger.info("Connected to ${metaData.databaseProductName} ${metaData.databaseProductVersion} [${Utils.getCurrentFunctionName()}]")
                }

                // Check if it is a CrateDB
                run {
                    connection.createStatement().use { statement ->
                        statement.executeQuery("SELECT version()").use { resultSet ->
                            if (resultSet.next()) {
                                val version = resultSet.getString(1)
                                if (version.contains("CrateDB")) {
                                    logger.warning("CrateDB detected [$version] [${Utils.getCurrentFunctionName()}]")
                                    return@run
                                }
                            }
                        }
                    }
                    promise.fail("Database is not a CrateDB")
                    return promise.future()
                }

                // Create the tables
                val createTableSQL = listOf("""
                 CREATE TABLE IF NOT EXISTS $sessionsTableName (
                    client_id VARCHAR(65535) PRIMARY KEY,
                    clean_session BOOLEAN,
                    connected BOOLEAN,
                    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    information OBJECT,
                    last_will_topic VARCHAR,
                    last_will_message VARCHAR,
                    last_will_qos INT,
                    last_will_retain BOOLEAN
                );
                """.trimIndent(), """
                CREATE TABLE IF NOT EXISTS $subscriptionsTableName (
                    client_id VARCHAR(65535),
                    topic VARCHAR,
                    qos INT,
                    wildcard BOOLEAN,
                    PRIMARY KEY (client_id, topic)
                );
                """.trimIndent(), """
                CREATE TABLE IF NOT EXISTS $queuedMessagesTableName (
                    message_uuid VARCHAR(36),
                    message_id INT,                    
                    topic VARCHAR,   
                    payload VARCHAR INDEX OFF,
                    qos INT,
                    retained BOOLEAN,
                    client_id VARCHAR(65535), 
                    PRIMARY KEY (message_uuid)
                );             
                """.trimIndent(), """
                CREATE TABLE IF NOT EXISTS $queuedMessagesClientsTableName (
                    client_id VARCHAR(65535),                
                    message_uuid VARCHAR(36),
                    PRIMARY KEY (client_id, message_uuid)
                );
                """.trimIndent())

                // Execute the SQL statements
                connection.createStatement().use { statement ->
                    createTableSQL.forEach(statement::executeUpdate)
                }
                logger.info("Tables are ready [${Utils.getCurrentFunctionName()}]")
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

    override fun iterateSubscriptions(callback: (topic: String, clientId: String, qos: Int)->Unit) {
        try {
            db.connection?.let { connection ->
                var rows = 0
                val sql = "SELECT client_id, topic, qos FROM $subscriptionsTableName "
                connection.prepareStatement(sql).use { preparedStatement ->
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        val clientId = resultSet.getString(1)
                        val topic = resultSet.getString(2)
                        val qos = MqttQoS.valueOf(resultSet.getInt(3))
                        callback(topic, clientId, qos.value())
                        rows++
                    }
                }
            } ?: run {
                logger.severe("Iterating subscription table not possible without database connection! [${Utils.getCurrentFunctionName()}]")
            }
        } catch (e: SQLException) {
            logger.warning("Error fetching subscriptions: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun iterateOfflineClients(callback: (clientId: String)->Unit) {
        try {
            db.connection?.let { connection ->
                "SELECT client_id FROM $sessionsTableName WHERE connected = FALSE AND clean_session = FALSE".let { sql ->
                    connection.prepareStatement(sql).use { preparedStatement ->
                        val resultSet = preparedStatement.executeQuery()
                        while (resultSet.next()) {
                            callback(resultSet.getString(1))
                        }
                    }
                }
            } ?: run {
                logger.severe("Iterating offline clients not possible without database connection! [${Utils.getCurrentFunctionName()}]")
            }
        } catch (e: SQLException) {
            logger.warning("Error at fetching offline clients [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun setClient(clientId: String, cleanSession: Boolean, connected: Boolean, information: JsonObject) {
        logger.finest { "Put client [$clientId] cleanSession [$cleanSession] connected [$connected] [${Utils.getCurrentFunctionName()}]" }
        val sql = "INSERT INTO $sessionsTableName (client_id, clean_session, connected, information) VALUES (?, ?, ?, ?) "+
                  "ON CONFLICT (client_id) DO UPDATE "+
                  "SET clean_session = EXCLUDED.clean_session, "+
                  "connected = EXCLUDED.connected, "+
                  "information = EXCLUDED.information, "+
                  "update_time = CURRENT_TIMESTAMP"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, clientId)
                    preparedStatement.setBoolean(2, cleanSession)
                    preparedStatement.setBoolean(3, connected)
                    preparedStatement.setString(4, information.encode())
                    preparedStatement.executeUpdate()
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            logger.warning("Error at inserting client [${e.message}] SQL: [$sql] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun setConnected(clientId: String, connected: Boolean) {
        val sql = "UPDATE $sessionsTableName SET connected = ?, update_time = CURRENT_TIMESTAMP WHERE client_id = ?"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setBoolean(1, connected)
                    preparedStatement.setString(2, clientId)
                    preparedStatement.executeUpdate()
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            logger.warning("Error at updating client [${e.message}] SQL: [$sql] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun isConnected(clientId: String): Boolean {
        val sql = "SELECT connected FROM $sessionsTableName WHERE client_id = ?"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, clientId)
                    val resultSet = preparedStatement.executeQuery()
                    if (resultSet.next()) {
                        return resultSet.getBoolean(1)
                    }
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error at fetching client [${e.message}] SQL: [$sql] [${Utils.getCurrentFunctionName()}]")
        }
        return false
    }

    override fun isPresent(clientId: String): Boolean {
        val sql = "SELECT client_id FROM $sessionsTableName WHERE client_id = ?"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, clientId)
                    val resultSet = preparedStatement.executeQuery()
                    return resultSet.next()
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error at fetching client [${e.message}] SQL: [$sql] [${Utils.getCurrentFunctionName()}]")
        }
        return false
    }

    override fun setLastWill(clientId: String, message: MqttMessage?) {
        val sql = "UPDATE $sessionsTableName "+
                "SET last_will_topic = ?, last_will_message = ?, last_will_qos = ?, last_will_retain = ? "+
                "WHERE client_id = ?"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, message?.topicName)
                    preparedStatement.setString(2, message?.getPayloadAsBase64())

                    message?.qosLevel?.let { preparedStatement.setInt(3, it) } ?: preparedStatement.setNull(
                        3,
                        Types.INTEGER
                    )
                    message?.isRetain?.let { preparedStatement.setBoolean(4, it) } ?: preparedStatement.setNull(
                        4,
                        Types.BOOLEAN
                    )
                    preparedStatement.setString(5, clientId)
                    preparedStatement.executeUpdate()
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            logger.warning("Error at setting last will [${e.message}] SQL: [$sql] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun addSubscriptions(subscriptions: List<MqttSubscription>) {
        val sql = "INSERT INTO $subscriptionsTableName (client_id, topic, qos, wildcard) VALUES (?, ?, ?, ?) "+
                  "ON CONFLICT (client_id, topic) DO NOTHING"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    subscriptions.forEach { subscription ->
                        preparedStatement.setString(1, subscription.clientId)
                        preparedStatement.setString(2, subscription.topicName)
                        preparedStatement.setInt(3, subscription.qos.value())
                        preparedStatement.setBoolean(4, Utils.isWildCardTopic(subscription.topicName))
                        preparedStatement.addBatch()
                    }
                    preparedStatement.executeBatch()
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            logger.warning("Error at inserting subscription [${e.message}] SQL: [$sql] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun delSubscriptions(subscriptions: List<MqttSubscription>) {
        val sql = "DELETE FROM $subscriptionsTableName WHERE client_id = ? AND topic = ?"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    subscriptions.forEach { subscription ->
                        preparedStatement.setString(1, subscription.clientId)
                        preparedStatement.setString(2, subscription.topicName)
                        preparedStatement.addBatch()
                    }
                    preparedStatement.executeBatch()
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            logger.warning("Error at removing subscription [${e.message}] SQL: [$sql] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun delClient(clientId: String, callback: (MqttSubscription)->Unit) {
        try {
            db.connection?.let { connection ->
                // do first a select of the topics and qos
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
            }

        } catch (e: SQLException) {
            logger.warning("Error at removing client [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun enqueueMessages(messages: List<Pair<MqttMessage, List<String>>>) {
        val sql1 = "INSERT INTO $queuedMessagesTableName "+
                   "(message_uuid, message_id, topic, payload, qos, retained, client_id) VALUES (?, ?, ?, ?, ?, ?, ?) "+
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
                           preparedStatement1.setString(4, message.first.getPayloadAsBase64())
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
            }
        } catch (e: SQLException) {
            logger.warning("Error at inserting queued message [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun dequeueMessages(clientId: String, callback: (MqttMessage)->Unit) {
        val sql = "SELECT m.message_uuid, m.message_id, m.topic, m.payload, m.qos, m.retained, m.client_id "+
                  "FROM $queuedMessagesTableName AS m JOIN $queuedMessagesClientsTableName AS c USING (message_uuid) "+
                  "WHERE c.client_id = ? "+
                  "ORDER BY m.message_uuid" // Time Based UUIDs
        try {
            db.connection?.let { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("REFRESH TABLE $queuedMessagesTableName")
                    statement.execute("REFRESH TABLE $queuedMessagesClientsTableName")
                }
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, clientId)
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        val messageUuid = resultSet.getString(1)
                        val messageId = resultSet.getInt(2)
                        val topic = resultSet.getString(3)
                        val payload = MqttMessage.getPayloadFromBase64(resultSet.getString(4))
                        val qos = resultSet.getInt(5)
                        val retained = resultSet.getBoolean(6)
                        val clientIdPublisher = resultSet.getString(6)
                        callback(
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
                    }
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error at fetching queued message [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun removeMessages(messages: List<Pair<String, String>>) { // clientId, messageUuid
        val groupedMessages = messages.groupBy({ it.first }, { it.second })
        logger.finest { "Remove messages: $groupedMessages [${Utils.getCurrentFunctionName()}]" }
        val sql = "DELETE FROM $queuedMessagesClientsTableName WHERE client_id = ? AND message_uuid = ANY (?)"
        try {
            db.connection?.let { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("REFRESH TABLE $queuedMessagesTableName")
                    statement.execute("REFRESH TABLE $queuedMessagesClientsTableName")
                }
                connection.prepareStatement(sql).use { preparedStatement ->
                    groupedMessages.forEach { (clientId, messageUuids) ->
                        preparedStatement.setString(1, clientId)
                        preparedStatement.setArray(2, connection.createArrayOf("varchar", messageUuids.toTypedArray()))
                        preparedStatement.addBatch()
                    }
                    preparedStatement.executeUpdate()
                }
                connection.commit()
            }
        } catch (e: SQLException) {
            logger.warning("Error at removing dequeued message [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun purgeQueuedMessages() {
        val sql = "DELETE FROM $queuedMessagesTableName WHERE message_uuid NOT IN " +
                "(SELECT message_uuid FROM $queuedMessagesClientsTableName)"
        try {
            DriverManager.getConnection(url, username, password).use { connection ->
                val startTime = System.currentTimeMillis()
                connection.createStatement().use { statement ->
                    statement.execute("REFRESH TABLE $queuedMessagesTableName")
                    statement.execute("REFRESH TABLE $queuedMessagesClientsTableName")
                }
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.executeUpdate()
                    val endTime = System.currentTimeMillis()
                    val duration = (endTime - startTime) / 1000.0
                    logger.info("Purging queued messages finished in $duration seconds [${Utils.getCurrentFunctionName()}]")
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error at purging queued messages [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun purgeSessions() {
        try {
            DriverManager.getConnection(url, username, password).use { connection ->
                val statement = connection.createStatement()
                statement.executeUpdate("UPDATE $sessionsTableName SET connected = FALSE")
                statement.executeUpdate(
                    "DELETE FROM $subscriptionsTableName WHERE client_id IN " +
                            "(SELECT client_id FROM $sessionsTableName WHERE clean_session = TRUE)"
                )
                statement.executeUpdate("DELETE FROM $sessionsTableName WHERE clean_session = TRUE")
            }
        } catch (e: SQLException) {
            logger.warning("Error at purging sessions [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }
}