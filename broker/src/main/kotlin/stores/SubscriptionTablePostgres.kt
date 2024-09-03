package at.rocworks.stores

import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttSubscription
import io.vertx.core.AbstractVerticle
import java.sql.*
import java.time.Instant
import java.util.logging.Logger

class SubscriptionTablePostgres(
    private val url: String,
    private val username: String,
    private val password: String
): AbstractVerticle(), ISubscriptionTable {
    private val logger = Logger.getLogger(this.javaClass.simpleName)
    private val tableName = "SubscriptionTable"

    val db = object : PostgresConnection(logger, url, username, password) {
        override fun checkTable(connection: Connection) {
            try {
                val createTableSQL = """
                CREATE TABLE IF NOT EXISTS $tableName (
                    client TEXT,
                    topic TEXT[],
                    PRIMARY KEY (client, topic)
                );
                """.trimIndent()

                // Create the index on the topic column
                val createIndexSQL = "CREATE INDEX IF NOT EXISTS idx_topic ON SubscriptionTable (topic)"

                // Execute the SQL statements
                val statement: Statement = connection.createStatement()
                statement.executeUpdate(createTableSQL)
                statement.executeUpdate(createIndexSQL)
                logger.info("Table [$tableName] is ready.")
            } catch (e: Exception) {
                logger.severe("Error in creating table [$tableName]: ${e.message}")
            }
        }
    }

    override fun start() {
        db.connectDatabase()
        vertx.setPeriodic(5000) { // TODO: configurable
            if (!db.checkConnection())
                db.connectDatabase()
        }
    }

    override fun addSubscription(subscription: MqttSubscription) {
        val sql = "INSERT INTO $tableName (client, topic) VALUES (?, ?) "+
                  "ON CONFLICT (client, topic) DO NOTHING"
        val levels = subscription.topicName.split("/").toTypedArray()
        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                preparedStatement.setString(1, subscription.clientId)
                preparedStatement.setArray(2, connection.createArrayOf("text", levels))
                preparedStatement.executeUpdate()
            }
        } catch (e: SQLException) {
            logger.warning("AddSubscription: Error inserting data [${e.message}]")
        }
    }

    override fun removeSubscription(subscription: MqttSubscription) {
        val sql = "DELETE FROM $tableName WHERE client = ? AND topic = ?"
        val levels = subscription.topicName.split("/").toTypedArray()
        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                preparedStatement.setString(1, subscription.clientId)
                preparedStatement.setArray(2, connection.createArrayOf("text", levels))
                preparedStatement.executeUpdate()
            }
        } catch (e: SQLException) {
            logger.warning("RemoveSubscription: Error deleting data [${e.message}]")
        }
    }

    override fun removeClient(clientId: String) {
        val sql = "DELETE FROM $tableName WHERE client = ?"
        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                preparedStatement.setString(1, clientId)
                preparedStatement.executeUpdate()
            }
        } catch (e: SQLException) {
            logger.warning("RemoveSubscription: Error deleting data [${e.message}]")
        }
    }

    override fun findClients(topicName: String): Set<String> {
        val result = hashSetOf<String>()

        return result
    }
}