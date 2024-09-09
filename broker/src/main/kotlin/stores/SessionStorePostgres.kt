package at.rocworks.stores

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttSubscription
import at.rocworks.data.TopicTree
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import java.sql.*
import java.util.logging.Logger

class SessionStorePostgres(
    private val url: String,
    private val username: String,
    private val password: String
): AbstractVerticle(), ISessionStore {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    private val sessionTableName = "Sessions"
    private val subscriptionTableName = "Subscriptions"
    private val queuedMessageTableName = "QueuedMessages"
    private val queuedClientsTableName = "QueuedClients"

    private var queuedMessageId = 0L

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun getType(): SessionStoreType = SessionStoreType.POSTGRES

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun checkTable(connection: Connection) {
            try {
                val createTableSQL = listOf("""
                   CREATE TABLE IF NOT EXISTS $sessionTableName (
                    client TEXT PRIMARY KEY,
                    clean_session BOOLEAN,
                    connected BOOLEAN,
                    last_will_topic TEXT,
                    last_will_message BYTEA,
                    last_will_qos INT,
                    last_will_retain BOOLEAN
                );
                """.trimIndent(), """
                CREATE TABLE IF NOT EXISTS $subscriptionTableName (
                    client TEXT,
                    topic TEXT[],
                    wildcard BOOLEAN,
                    PRIMARY KEY (client, topic)
                );
                """.trimIndent(), """
                CREATE TABLE IF NOT EXISTS $queuedMessageTableName (
                    message_id BIGINT,
                    topic TEXT,
                    payload BYTEA,
                    PRIMARY KEY (message_id)
                );
                """.trimIndent(), """
                CREATE TABLE IF NOT EXISTS $queuedClientsTableName (
                    client TEXT,                
                    message_id BIGINT,
                    PRIMARY KEY (client, message_id)
                );
                """.trimIndent())

                // Create the index on the topic column
                val createIndexesSQL = listOf(
                    "CREATE INDEX IF NOT EXISTS idx_topic ON $subscriptionTableName (topic);",
                    "CREATE INDEX IF NOT EXISTS idx_wildcard ON $subscriptionTableName (wildcard) WHERE wildcard = TRUE;"
                )

                // Execute the SQL statements
                val statement: Statement = connection.createStatement()
                createTableSQL.forEach(statement::executeUpdate)
                createIndexesSQL.forEach(statement::executeUpdate)
                logger.info("Table [$subscriptionTableName] is ready.")
            } catch (e: Exception) {
                logger.severe("Error in creating table: ${e.message}")
            }
        }
    }

    override fun start(startPromise: Promise<Void>) {
        db.start(vertx, startPromise)
    }

    override fun buildIndex(index: TopicTree) {
        try {
            logger.info("Indexing subscription table [$subscriptionTableName].")
            db.connection?.let { connection ->
                val sql = "SELECT array_to_string(topic, '/') FROM $subscriptionTableName "
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                val resultSet = preparedStatement.executeQuery()
                if (resultSet.next()) {
                    index.add(resultSet.getString(1))
                }
                logger.info("Indexing subscription table [$subscriptionTableName] finished.")
            } ?: run {
                logger.severe("Indexing subscription table not possible without database connection!")
            }
        } catch (e: SQLException) {
            logger.warning("CreateLocalIndex: Error fetching data: ${e.message}")
        }
    }

    override fun putClient(clientId: String, cleanSession: Boolean, connected: Boolean) {
        val sql = "INSERT INTO $sessionTableName (client, clean_session, connected) VALUES (?, ?, ?) "+
                  "ON CONFLICT (client) DO UPDATE SET clean_session = EXCLUDED.clean_session, connected = EXCLUDED.connected"
        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                preparedStatement.setString(1, clientId)
                preparedStatement.setBoolean(2, cleanSession)
                preparedStatement.setBoolean(3, connected)
                preparedStatement.executeUpdate()
            }
        } catch (e: SQLException) {
            logger.warning("Error at inserting client [${e.message}] SQL: [$sql]")
        }
    }

    override fun setConnected(clientId: String, connected: Boolean) {
        val sql = "UPDATE $sessionTableName SET connected = ? WHERE client = ?"
        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                preparedStatement.setBoolean(1, connected)
                preparedStatement.setString(2, clientId)
                preparedStatement.executeUpdate()
            }
        } catch (e: SQLException) {
            logger.warning("Error at updating client [${e.message}] SQL: [$sql]")
        }
    }

    override fun isConnected(clientId: String): Boolean {
        val sql = "SELECT connected FROM $sessionTableName WHERE client = ?"
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
            logger.warning("Error at fetching client [${e.message}] SQL: [$sql]")
        }
        return false
    }

    override fun setLastWill(clientId: String, topic: String, message: MqttMessage) {
        val sql = "UPDATE $sessionTableName SET last_will_topic = ?, last_will_message = ?, last_will_qos = ?, last_will_retain = ? WHERE client = ?"
        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                preparedStatement.setString(1, topic)
                preparedStatement.setBytes(2, message.payload)
                preparedStatement.setInt(3, message.qosLevel)
                preparedStatement.setBoolean(4, message.isRetain)
                preparedStatement.setString(5, clientId)
                preparedStatement.executeUpdate()
            }
        } catch (e: SQLException) {
            logger.warning("Error at setting last will [${e.message}] SQL: [$sql]")
        }
    }

    override fun addSubscriptions(subscriptions: List<MqttSubscription>) {
        val sql = "INSERT INTO $subscriptionTableName (client, topic, wildcard) VALUES (?, ?, ?) "+
                  "ON CONFLICT (client, topic) DO NOTHING"
        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                subscriptions.forEach { subscription ->
                    val levels = Utils.getTopicLevels(subscription.topicName).toTypedArray()
                    preparedStatement.setString(1, subscription.clientId)
                    preparedStatement.setArray(2, connection.createArrayOf("text", levels))
                    preparedStatement.setBoolean(3, Utils.isWildCardTopic(subscription.topicName))
                    preparedStatement.addBatch()
                }
                preparedStatement.executeBatch()
            }
        } catch (e: SQLException) {
            logger.warning("Error at inserting subscription [${e.message}] SQL: [$sql]")
        }
    }

    override fun delSubscriptions(subscriptions: List<MqttSubscription>) {
        val sql = "DELETE FROM $subscriptionTableName WHERE client = ? AND topic = ?"
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
            logger.warning("Error at removing subscription [${e.message}] SQL: [$sql]")
        }
    }

    override fun delClient(clientId: String, callback: (MqttSubscription)->Unit) {
        try {
            db.connection?.let { connection ->
                "DELETE FROM $subscriptionTableName WHERE client = ? RETURNING topic".let { sql ->
                    val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                    preparedStatement.setString(1, clientId)
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        val topic = resultSet.getArray(1).array
                        if (topic is Array<*>) callback(MqttSubscription(clientId, topic.joinToString("/")))
                    }
                }

                // Remove the client from the session table
                "DELETE FROM $sessionTableName WHERE client = ?".let { sql ->
                    val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                    preparedStatement.setString(1, clientId)
                    preparedStatement.executeUpdate()
                }
            }

        } catch (e: SQLException) {
            logger.warning("Error at removing client [${e.message}]")
        }
    }

    override fun enqueueMessages(clientIds: List<String>, messages: List<MqttMessage>) {
        val sql1 = "INSERT INTO $queuedMessageTableName (message_id, topic, payload) VALUES (?, ?, ?) "+
                  "ON CONFLICT (message_id) DO NOTHING"
        val sql2 = "INSERT INTO $queuedClientsTableName (client, message_id) VALUES (?, ?) "+
                   "ON CONFLICT (client, message_id) DO NOTHING"
        try {
            db.connection?.let { connection ->
                val preparedStatement1: PreparedStatement = connection.prepareStatement(sql1)
                val preparedStatement2: PreparedStatement = connection.prepareStatement(sql2)
                messages.forEach { message ->
                    val messageId = queuedMessageId++

                    // Add message
                    preparedStatement1.setLong(1, messageId)
                    preparedStatement1.setString(2, message.topicName)
                    preparedStatement1.setBytes(3, message.payload)
                    preparedStatement1.addBatch()

                    // Add client to message relation
                    clientIds.forEach { clientId ->
                        preparedStatement2.setString(1, clientId)
                        preparedStatement2.setLong(2, messageId)
                        preparedStatement2.addBatch()
                    }
                }
                preparedStatement1.executeBatch()
                preparedStatement2.executeBatch()
            }
        } catch (e: SQLException) {
            logger.warning("Error at inserting queued message [${e.message}]")
        }
    }

    override fun dequeueMessages(clientId: String, callback: (MqttMessage)->Unit) {
        // turn the sql to a delete statement with returning topic and payload
        val sql = "DELETE FROM $queuedClientsTableName USING $queuedMessageTableName WHERE $queuedClientsTableName.message_id = $queuedMessageTableName.message_id AND client = ? RETURNING topic, payload"
        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                preparedStatement.setString(1, clientId)
                val resultSet = preparedStatement.executeQuery()
                while (resultSet.next()) {
                    val topic = resultSet.getString(1)
                    val payload = resultSet.getBytes(2)
                    callback(MqttMessage(
                        messageId = 0,
                        topicName = topic,
                        payload = payload,
                        qosLevel = 0,
                        isRetain = false,
                        isDup = false
                    ))
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error at fetching queued message [${e.message}]")
        }
        /*
        val sql = "SELECT topic, payload FROM $queuedMessageTableName JOIN $queuedClientsTableName USING (message_id) WHERE client = ?"
        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                preparedStatement.setString(1, clientId)
                val resultSet = preparedStatement.executeQuery()
                while (resultSet.next()) {
                    val topic = resultSet.getString(1)
                    val payload = resultSet.getBytes(2)
                    callback(MqttMessage(
                        messageId = 0,
                        topicName = topic,
                        payload = payload,
                        qosLevel = 0,
                        isRetain = false,
                        isDup = false
                    ))
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error at fetching queued message [${e.message}]")
        }
         */
    }
}