package at.rocworks.stores

import at.rocworks.Config
import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttSubscription
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.sql.*
import java.util.UUID
import java.util.concurrent.Callable

class SessionStorePostgres(
    private val url: String,
    private val username: String,
    private val password: String
): AbstractVerticle(), ISessionStore {
    private val logger = Utils.getLogger(this::class.java)

    private val sessionsTableName = "Sessions"
    private val subscriptionsTableName = "Subscriptions"
    private val queuedMessagesTableName = "QueuedMessages"
    private val queuedMessagesClientsTableName = "QueuedMessagesClients"

    //private var queuedMessageId = 0L

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    private val readyPromise: Promise<Void> = Promise.promise()
    override fun storeReady(): Future<Void> = readyPromise.future()

    override fun getType(): SessionStoreType = SessionStoreType.POSTGRES

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun init(connection: Connection) {
            try {
                val createTableSQL = listOf("""
                 CREATE TABLE IF NOT EXISTS $sessionsTableName (
                    client_id VARCHAR(65535) PRIMARY KEY,
                    clean_session BOOLEAN,
                    connected BOOLEAN,
                    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    information JSONB,
                    last_will_topic TEXT,
                    last_will_message BYTEA,
                    last_will_qos INT,
                    last_will_retain BOOLEAN
                );
                """.trimIndent(), """
                CREATE TABLE IF NOT EXISTS $subscriptionsTableName (
                    client_id VARCHAR(65535) ,
                    topic TEXT[],
                    qos INT,
                    wildcard BOOLEAN,
                    PRIMARY KEY (client_id, topic)
                );
                """.trimIndent(), """
                CREATE TABLE IF NOT EXISTS $queuedMessagesTableName (
                    message_uuid VARCHAR(36),
                    message_id INT,                    
                    topic TEXT,                    
                    payload BYTEA,
                    qos INT,
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

                // Create the index on the topic column
                val createIndexesSQL = listOf(
                    "CREATE INDEX IF NOT EXISTS idx_topic ON $subscriptionsTableName (topic);",
                    "CREATE INDEX IF NOT EXISTS idx_wildcard ON $subscriptionsTableName (wildcard) WHERE wildcard = TRUE;"
                )

                // Execute the SQL statements
                val statement: Statement = connection.createStatement()
                createTableSQL.forEach(statement::executeUpdate)
                createIndexesSQL.forEach(statement::executeUpdate)

                // if not clustered, then remove all sessions and subscriptions of clean sessions
                if (!Config.isClustered()) {
                    statement.executeUpdate(
                        "DELETE FROM $subscriptionsTableName WHERE client_id IN "+
                            "(SELECT client_id FROM $sessionsTableName WHERE clean_session = TRUE)")
                    statement.executeUpdate("DELETE FROM $sessionsTableName WHERE clean_session = TRUE")
                }

                logger.info("Tables are ready [${Utils.getCurrentFunctionName()}]")
                readyPromise.complete()
            } catch (e: Exception) {
                logger.severe("Error in getting tables ready: ${e.message} [${Utils.getCurrentFunctionName()}]")
                readyPromise.fail(e)
            }
        }
    }

    override fun start(startPromise: Promise<Void>) {
        db.start(vertx, startPromise)
        startPromise.future().onSuccess {
            vertx.executeBlocking(Callable { purgeQueuedMessages() }).onComplete {
                //vertx.setPeriodic(60_000) {
                //    vertx.executeBlocking(Callable { purgeQueuedMessages() })
                //}
            }
        }
    }

    override fun iterateSubscriptions(callback: (topic: String, clientId: String, qos: Int)->Unit) {
        try {
            db.connection?.let { connection ->
                var rows = 0
                val sql = "SELECT client_id, array_to_string(topic, '/'), qos FROM $subscriptionsTableName "
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                val resultSet = preparedStatement.executeQuery()
                while (resultSet.next()) {
                    val clientId = resultSet.getString(1)
                    val topic = resultSet.getString(2)
                    val qos = MqttQoS.valueOf(resultSet.getInt(3))
                    callback(topic, clientId, qos.value())
                    rows++
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
                    val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        callback(resultSet.getString(1))
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
        val sql = "INSERT INTO $sessionsTableName (client_id, clean_session, connected, information) VALUES (?, ?, ?, ?::jsonb) "+
                  "ON CONFLICT (client_id) DO UPDATE "+
                  "SET clean_session = EXCLUDED.clean_session, "+
                  "connected = EXCLUDED.connected, "+
                  "information = EXCLUDED.information, "+
                  "update_time = CURRENT_TIMESTAMP"
        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                preparedStatement.setString(1, clientId)
                preparedStatement.setBoolean(2, cleanSession)
                preparedStatement.setBoolean(3, connected)
                preparedStatement.setString(4, information.encode())
                preparedStatement.executeUpdate()
            }
        } catch (e: SQLException) {
            logger.warning("Error at inserting client [${e.message}] SQL: [$sql] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun setConnected(clientId: String, connected: Boolean) {
        val sql = "UPDATE $sessionsTableName SET connected = ?, update_time = CURRENT_TIMESTAMP WHERE client_id = ?"
        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                preparedStatement.setBoolean(1, connected)
                preparedStatement.setString(2, clientId)
                preparedStatement.executeUpdate()
            }
        } catch (e: SQLException) {
            logger.warning("Error at updating client [${e.message}] SQL: [$sql] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun isConnected(clientId: String): Boolean {
        val sql = "SELECT connected FROM $sessionsTableName WHERE client_id = ?"
        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                preparedStatement.setString(1, clientId)
                val resultSet = preparedStatement.executeQuery()
                if (resultSet.next()) {
                    return resultSet.getBoolean(1)
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
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                preparedStatement.setString(1, clientId)
                val resultSet = preparedStatement.executeQuery()
                return resultSet.next()
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
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                preparedStatement.setString(1, message?.topicName)
                preparedStatement.setBytes(2, message?.payload)
                message?.qosLevel?.let { preparedStatement.setInt(3, it) } ?: preparedStatement.setNull(3, Types.INTEGER)
                message?.isRetain?.let { preparedStatement.setBoolean(4, it) } ?: preparedStatement.setNull(4, Types.BOOLEAN)
                preparedStatement.setString(5, clientId)
                preparedStatement.executeUpdate()
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
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                subscriptions.forEach { subscription ->
                    val levels = Utils.getTopicLevels(subscription.topicName).toTypedArray()
                    preparedStatement.setString(1, subscription.clientId)
                    preparedStatement.setArray(2, connection.createArrayOf("text", levels))
                    preparedStatement.setInt(3, subscription.qos.value())
                    preparedStatement.setBoolean(4, Utils.isWildCardTopic(subscription.topicName))
                    preparedStatement.addBatch()
                }
                preparedStatement.executeBatch()
            }
        } catch (e: SQLException) {
            logger.warning("Error at inserting subscription [${e.message}] SQL: [$sql] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun delSubscriptions(subscriptions: List<MqttSubscription>) {
        val sql = "DELETE FROM $subscriptionsTableName WHERE client_id = ? AND topic = ?"
        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                subscriptions.forEach { subscription ->
                    val levels = Utils.getTopicLevels(subscription.topicName).toTypedArray()
                    preparedStatement.setString(1, subscription.clientId)
                    preparedStatement.setArray(2, connection.createArrayOf("text", levels))
                    preparedStatement.addBatch()
                }
                preparedStatement.executeBatch()
            }
        } catch (e: SQLException) {
            logger.warning("Error at removing subscription [${e.message}] SQL: [$sql] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun delClient(clientId: String, callback: (MqttSubscription)->Unit) {
        try {
            db.connection?.let { connection ->
                "DELETE FROM $subscriptionsTableName WHERE client_id = ? RETURNING topic, qos".let { sql ->
                    val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                    preparedStatement.setString(1, clientId)
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        val topic = resultSet.getArray(1).array
                        val qos = MqttQoS.valueOf(resultSet.getInt(2))
                        if (topic is Array<*>)
                            callback(MqttSubscription(clientId, topic.joinToString("/"), qos))
                    }
                }

                // Remove the client from the session table
                "DELETE FROM $sessionsTableName WHERE client_id = ?".let { sql ->
                    val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                    preparedStatement.setString(1, clientId)
                    preparedStatement.executeUpdate()
                }

                // Remove the client from the queued clients table
                "DELETE FROM $queuedMessagesClientsTableName WHERE client_id = ?".let { sql ->
                    val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                    preparedStatement.setString(1, clientId)
                    preparedStatement.executeUpdate()
                }
            }

        } catch (e: SQLException) {
            logger.warning("Error at removing client [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun enqueueMessages(messages: List<Pair<MqttMessage, List<String>>>) {
        val sql1 = "INSERT INTO $queuedMessagesTableName "+
                   "(message_uuid, message_id, topic, payload, qos, client_id) VALUES (?, ?, ?, ?, ?, ?) "+
                   "ON CONFLICT (message_uuid) DO NOTHING"
        val sql2 = "INSERT INTO $queuedMessagesClientsTableName "+
                   "(client_id, message_uuid) VALUES (?, ?) "+
                   "ON CONFLICT (client_id, message_uuid) DO NOTHING"
        try {
            db.connection?.let { connection ->
                val preparedStatement1: PreparedStatement = connection.prepareStatement(sql1)
                val preparedStatement2: PreparedStatement = connection.prepareStatement(sql2)
                messages.forEach { message ->
                    // Add message
                    preparedStatement1.setObject(1, message.first.messageUuid)
                    preparedStatement1.setInt(2, message.first.messageId)
                    preparedStatement1.setString(3, message.first.topicName)
                    preparedStatement1.setBytes(4, message.first.payload)
                    preparedStatement1.setInt(5, message.first.qosLevel)
                    preparedStatement1.setString(6, message.first.clientId)
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
        } catch (e: SQLException) {
            logger.warning("Error at inserting queued message [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun dequeueMessages(clientId: String, callback: (MqttMessage)->Unit) {
        /*
        val sql = "DELETE FROM $queuedMessagesClientsTableName USING $queuedMessagesTableName "+
                  "WHERE $queuedMessagesClientsTableName.message_uuid = $queuedMessagesTableName.message_uuid "+
                  "AND client_id = ? "+
                  "RETURNING message_id, topic, payload, qos"
        */
        val sql = "SELECT m.message_uuid, m.message_id, m.topic, m.payload, m.qos, m.client_id "+
                  "FROM $queuedMessagesTableName AS m JOIN $queuedMessagesClientsTableName AS c USING (message_uuid) "+
                  "WHERE c.client_id = ? "+
                  "ORDER BY m.message_uuid" // Time Based UUIDs
        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                preparedStatement.setString(1, clientId)
                val resultSet = preparedStatement.executeQuery()
                while (resultSet.next()) {
                    val messageUuid = resultSet.getString(1)
                    val messageId = resultSet.getInt(2)
                    val topic = resultSet.getString(3)
                    val payload = resultSet.getBytes(4)
                    val qos = resultSet.getInt(5)
                    val clientId = resultSet.getString(6)
                    callback(MqttMessage(
                        messageUuid = messageUuid,
                        messageId = messageId,
                        topicName = topic,
                        payload = payload,
                        qosLevel = qos,
                        isRetain = false,
                        isDup = false,
                        clientId = clientId
                    ))
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
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                groupedMessages.forEach { (clientId, messageUuids) ->
                    preparedStatement.setString(1, clientId)
                    preparedStatement.setArray(2, connection.createArrayOf("text", messageUuids.toTypedArray()))
                    preparedStatement.addBatch()
                }
                preparedStatement.executeUpdate()

            }
        } catch (e: SQLException) {
            logger.warning("Error at removing dequeued message [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    private fun purgeQueuedMessages() {
        val sql = "DELETE FROM $queuedMessagesTableName WHERE message_id NOT IN " +
                "(SELECT message_id FROM $queuedMessagesClientsTableName)"
        try {
            DriverManager.getConnection(url, username, password).use { connection ->
                val startTime = System.currentTimeMillis()
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                preparedStatement.executeUpdate()
                val endTime = System.currentTimeMillis()
                val duration = (endTime - startTime) / 1000.0
                logger.info("Purging queued messages finished in $duration seconds [${Utils.getCurrentFunctionName()}]")
            }
        } catch (e: SQLException) {
            logger.warning("Error at purging queued messages [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }
}