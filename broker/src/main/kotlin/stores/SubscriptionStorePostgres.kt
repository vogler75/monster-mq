package at.rocworks.stores

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttSubscription
import at.rocworks.data.TopicTree
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import java.sql.*
import java.util.logging.Logger

class SubscriptionStorePostgres(
    private val url: String,
    private val username: String,
    private val password: String
): AbstractVerticle(), ISubscriptionStore {
    private val logger = Logger.getLogger(this.javaClass.simpleName)
    private val tableName = "Subscriptions"

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun getType(): SubscriptionStoreType = SubscriptionStoreType.POSTGRES

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun checkTable(connection: Connection) {
            try {
                val createTableSQL = """
                CREATE TABLE IF NOT EXISTS $tableName (
                    client TEXT,
                    topic TEXT[],
                    wildcard BOOLEAN,
                    PRIMARY KEY (client, topic)
                );
                """.trimIndent()

                // Create the index on the topic column
                val createIndexesSQL = listOf(
                    "CREATE INDEX IF NOT EXISTS idx_topic ON $tableName (topic);",
                    "CREATE INDEX IF NOT EXISTS idx_wildcard ON $tableName (wildcard) WHERE wildcard = TRUE;"
                )

                // Execute the SQL statements
                val statement: Statement = connection.createStatement()
                statement.executeUpdate(createTableSQL)
                createIndexesSQL.forEach(statement::executeUpdate)
                logger.info("Table [$tableName] is ready.")
            } catch (e: Exception) {
                logger.severe("Error in creating table [$tableName]: ${e.message}")
            }
        }
    }

    override fun start(startPromise: Promise<Void>) {
        db.start(vertx, startPromise)
    }

    override fun populateIndex(index: TopicTree) {
        try {
            logger.info("Indexing subscription table [$tableName].")
            db.connection?.let { connection ->
                val sql = "SELECT array_to_string(topic, '/') FROM $tableName "
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                val resultSet = preparedStatement.executeQuery()
                if (resultSet.next()) {
                    index.add(resultSet.getString(1))
                }
                logger.info("Indexing subscription table [$tableName] finished.")
            } ?: run {
                logger.severe("Indexing subscription table not possible without database connection!")
            }
        } catch (e: SQLException) {
            logger.warning("CreateLocalIndex: Error fetching data: ${e.message}")
        }
    }

    override fun addSubscriptions(subscriptions: List<MqttSubscription>) {
        val sql = "INSERT INTO $tableName (client, topic, wildcard) VALUES (?, ?, ?) "+
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

    override fun removeSubscriptions(subscriptions: List<MqttSubscription>) {
        val sql = "DELETE FROM $tableName WHERE client = ? AND topic = ?"
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

    override fun removeClient(clientId: String, callback: (MqttSubscription)->Unit) {
        val sql = "DELETE FROM $tableName WHERE client = ? RETURNING topic"
        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                preparedStatement.setString(1, clientId)
                val resultSet = preparedStatement.executeQuery()
                while (resultSet.next()) {
                    val topic = resultSet.getArray(1).array as Array<String>
                    callback(MqttSubscription(clientId, topic.joinToString("/")))
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error at removing client [${e.message}] SQL: [$sql]")
        }
    }
}