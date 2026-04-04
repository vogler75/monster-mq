package at.rocworks.stores.postgres

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.data.MqttSubscription
import at.rocworks.stores.DatabaseConnection
import at.rocworks.stores.ISessionStoreSync
import at.rocworks.stores.SessionStoreType
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.sql.*

class SessionStorePostgres(
    private val url: String,
    private val username: String,
    private val password: String,
    private val schema: String? = null
): AbstractVerticle(), ISessionStoreSync {
    private val logger = Utils.getLogger(this::class.java)

    private val sessionsTableName = "sessions"
    private val subscriptionsTableName = "subscriptions"

    override fun getType(): SessionStoreType = SessionStoreType.POSTGRES

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
                CREATE TABLE IF NOT EXISTS $sessionsTableName (
                    client_id VARCHAR(65535) PRIMARY KEY,
                    node_id VARCHAR(65535),
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
                    no_local BOOLEAN DEFAULT false,
                    retain_handling INT DEFAULT 0,
                    retain_as_published BOOLEAN DEFAULT false,
                    PRIMARY KEY (client_id, topic)
                );
                """.trimIndent())

                // Create indexes for faster queries
                val createIndexesSQL = listOf(
                    "CREATE INDEX IF NOT EXISTS ${subscriptionsTableName}_topic_idx ON $subscriptionsTableName (topic);",
                    "CREATE INDEX IF NOT EXISTS ${subscriptionsTableName}_wildcard_idx ON $subscriptionsTableName (wildcard) WHERE wildcard = TRUE;"
                )

                // Execute the SQL statements
                connection.createStatement().use { statement ->
                    createTableSQL.forEach(statement::executeUpdate)
                    createIndexesSQL.forEach(statement::executeUpdate)
                    
                    // Migration: Add retain_as_published column if it doesn't exist
                    try {
                        statement.executeUpdate("""
                        ALTER TABLE $subscriptionsTableName ADD COLUMN IF NOT EXISTS retain_as_published BOOLEAN DEFAULT false
                        """.trimIndent())
                        logger.fine("Subscription migration: Added retain_as_published column")
                    } catch (e: Exception) {
                        logger.fine("Subscription migration: Column may already exist: ${e.message}")
                    }
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

    override fun iterateSubscriptions(callback: (topic: String, clientId: String, qos: Int, noLocal: Boolean, retainHandling: Int, retainAsPublished: Boolean)->Unit) {
        try {
            db.connection?.let { connection ->
                var rows = 0
                val sql = "SELECT client_id, array_to_string(topic, '/'), qos, no_local, retain_handling, retain_as_published FROM $subscriptionsTableName "
                connection.prepareStatement(sql).use { preparedStatement ->
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        val clientId = resultSet.getString(1)
                        val topic = resultSet.getString(2)
                        val qos = MqttQoS.valueOf(resultSet.getInt(3))
                        val noLocal = resultSet.getBoolean(4)
                        val retainHandling = resultSet.getInt(5)
                        val retainAsPublished = resultSet.getBoolean(6)
                        callback(topic, clientId, qos.value(), noLocal, retainHandling, retainAsPublished)
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

    override fun iterateConnectedClients(callback: (clientId: String, nodeId: String) -> Unit) {
        try {
            db.connection?.let { connection ->
                "SELECT client_id, node_id FROM $sessionsTableName WHERE connected = TRUE".let { sql ->
                    connection.prepareStatement(sql).use { preparedStatement ->
                        val resultSet = preparedStatement.executeQuery()
                        while (resultSet.next()) {
                            val clientId = resultSet.getString(1)
                            val nodeId = resultSet.getString(2) ?: ""
                            callback(clientId, nodeId)
                        }
                    }
                }
            } ?: run {
                logger.severe("Iterating connected clients not possible without database connection! [${Utils.getCurrentFunctionName()}]")
            }
        } catch (e: SQLException) {
            logger.warning("Error at fetching connected clients [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun iterateAllSessions(callback: (clientId: String, nodeId: String, connected: Boolean, cleanSession: Boolean) -> Unit) {
        try {
            db.connection?.let { connection ->
                "SELECT client_id, node_id, connected, clean_session FROM $sessionsTableName".let { sql ->
                    connection.prepareStatement(sql).use { preparedStatement ->
                        val resultSet = preparedStatement.executeQuery()
                        while (resultSet.next()) {
                            val clientId = resultSet.getString(1)
                            val nodeId = resultSet.getString(2) ?: ""
                            val connected = resultSet.getBoolean(3)
                            val cleanSession = resultSet.getBoolean(4)
                            callback(clientId, nodeId, connected, cleanSession)
                        }
                    }
                }
            } ?: run {
                logger.severe("Iterating all sessions not possible without database connection! [${Utils.getCurrentFunctionName()}]")
            }
        } catch (e: SQLException) {
            logger.warning("Error at fetching all sessions [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun iterateNodeClients(nodeId: String, callback: (clientId: String, cleanSession: Boolean, lastWill: BrokerMessage) -> Unit) {
        try {
            db.connection?.let { connection ->
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
                                BrokerMessage(
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
            } ?: run {
                logger.severe("Iterating node clients not possible without database connection! [${Utils.getCurrentFunctionName()}]")
            }
        } catch (e: SQLException) {
            logger.warning("Error at fetching node clients [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun setClient(clientId: String, nodeId: String, cleanSession: Boolean, connected: Boolean, information: JsonObject) {
        logger.finest { "Put client [$clientId] cleanSession [$cleanSession] connected [$connected] [${Utils.getCurrentFunctionName()}]" }
        val sql = "INSERT INTO $sessionsTableName (client_id, node_id, clean_session, connected, information) VALUES (?, ?, ?, ?, ?::jsonb) "+
                  "ON CONFLICT (client_id) DO UPDATE "+
                  "SET node_id = EXCLUDED.node_id, "+
                  "clean_session = EXCLUDED.clean_session, "+
                  "connected = EXCLUDED.connected, "+
                  "information = EXCLUDED.information, "+
                  "update_time = CURRENT_TIMESTAMP"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, clientId)
                    preparedStatement.setString(2, nodeId)
                    preparedStatement.setBoolean(3, cleanSession)
                    preparedStatement.setBoolean(4, connected)
                    preparedStatement.setString(5, information.encode())
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

    override fun setLastWill(clientId: String, message: BrokerMessage?) {
        val sql = "UPDATE $sessionsTableName "+
                "SET last_will_topic = ?, last_will_message = ?, last_will_qos = ?, last_will_retain = ? "+
                "WHERE client_id = ?"
        try {
            logger.fine { "Setting last will for client [$clientId] [${Utils.getCurrentFunctionName()}]" }
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, message?.topicName)
                    preparedStatement.setBytes(2, message?.payload)
                    message?.qosLevel?.let { preparedStatement.setInt(3, it) }
                        ?: preparedStatement.setNull(3, Types.INTEGER)
                    message?.isRetain?.let { preparedStatement.setBoolean(4, it) }
                        ?: preparedStatement.setNull(4, Types.BOOLEAN)
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
        val sql = "INSERT INTO $subscriptionsTableName (client_id, topic, qos, wildcard, no_local, retain_handling, retain_as_published) VALUES (?, ?, ?, ?, ?, ?, ?) "+
                  "ON CONFLICT (client_id, topic) DO UPDATE SET qos = EXCLUDED.qos, no_local = EXCLUDED.no_local, retain_handling = EXCLUDED.retain_handling, retain_as_published = EXCLUDED.retain_as_published"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    subscriptions.forEach { subscription ->
                        val levels = Utils.getTopicLevels(subscription.topicName).toTypedArray()
                        preparedStatement.setString(1, subscription.clientId)
                        preparedStatement.setArray(2, connection.createArrayOf("text", levels))
                        preparedStatement.setInt(3, subscription.qos.value())
                        preparedStatement.setBoolean(4, Utils.isWildCardTopic(subscription.topicName))
                        preparedStatement.setBoolean(5, subscription.noLocal)
                        preparedStatement.setInt(6, subscription.retainHandling)
                        preparedStatement.setBoolean(7, subscription.retainAsPublished)
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
                        val levels = Utils.getTopicLevels(subscription.topicName).toTypedArray()
                        preparedStatement.setString(1, subscription.clientId)
                        preparedStatement.setArray(2, connection.createArrayOf("text", levels))
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
                "DELETE FROM $subscriptionsTableName WHERE client_id = ? RETURNING topic, qos".let { sql ->
                    connection.prepareStatement(sql).use { preparedStatement ->
                        preparedStatement.setString(1, clientId)
                        val resultSet = preparedStatement.executeQuery()
                        while (resultSet.next()) {
                            val topic = resultSet.getArray(1).array
                            val qos = MqttQoS.valueOf(resultSet.getInt(2))
                            if (topic is Array<*>)
                                callback(MqttSubscription(clientId, topic.joinToString("/"), qos))
                        }
                    }
                }

                // Remove the client from the session table
                "DELETE FROM $sessionsTableName WHERE client_id = ?".let { sql ->
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

    override fun purgeSessions() {
        try {
            val startTime = System.currentTimeMillis()
            db.connection?.let { connection ->
                connection.createStatement().use { statement ->
                    // Delete clean sessions
                    statement.executeUpdate(
                        "DELETE FROM $sessionsTableName WHERE clean_session = TRUE"
                    )
                    // Delete orphaned subscriptions using NOT EXISTS for better performance
                    statement.executeUpdate(
                        """
                        DELETE FROM $subscriptionsTableName s
                        WHERE NOT EXISTS (
                            SELECT 1 FROM $sessionsTableName sess
                            WHERE sess.client_id = s.client_id
                        )
                        """.trimIndent()
                    )
                    // Mark all sessions as disconnected
                    statement.executeUpdate(
                        "UPDATE $sessionsTableName SET connected = FALSE"
                    )
                }
                connection.commit()
                val duration = (System.currentTimeMillis() - startTime) / 1000.0
                logger.fine("Purging sessions finished in $duration seconds [${Utils.getCurrentFunctionName()}]")
            } ?: logger.warning("No database connection available for purging sessions [${Utils.getCurrentFunctionName()}]")
        } catch (e: SQLException) {
            logger.warning("Error at purging sessions [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

}