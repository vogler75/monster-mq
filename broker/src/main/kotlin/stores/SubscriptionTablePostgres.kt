package at.rocworks.stores

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttSubscription
import at.rocworks.data.TopicTree
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import java.sql.*
import java.util.logging.Level
import java.util.logging.Logger

class SubscriptionTablePostgres(
    private val url: String,
    private val username: String,
    private val password: String
): AbstractVerticle(), ISubscriptionTable {
    private val logger = Logger.getLogger(this.javaClass.simpleName)
    private val tableName = "SubscriptionTable"
    private val wildCardIndex = TopicTree()

    private val addAddress = Const.GLOBAL_SUBSCRIPTION_TABLE_NAMESPACE +"/A"
    private val delAddress = Const.GLOBAL_SUBSCRIPTION_TABLE_NAMESPACE +"/D"

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun getType(): SubscriptionTableType = SubscriptionTableType.POSTGRES

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
                    "CREATE INDEX IF NOT EXISTS idx_topic ON SubscriptionTable (topic);",
                    "CREATE INDEX IF NOT EXISTS idx_wildcard ON SubscriptionTable (wildcard) WHERE wildcard = TRUE;"
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
        vertx.eventBus().consumer<MqttSubscription>(addAddress) {
            wildCardIndex.add(it.body().topicName, it.body().clientId)
        }

        vertx.eventBus().consumer<MqttSubscription>(delAddress) {
            wildCardIndex.del(it.body().topicName, it.body().clientId)
        }

        db.start(vertx, startPromise)

        startPromise.future().onSuccess {
            createWildCardIndex()
        }
    }

    private fun createWildCardIndex() {
        try {
            logger.info("Indexing subscription table [$tableName].")
            db.connection?.let { connection ->
                val sql = "SELECT array_to_string(topic, '/') FROM $tableName WHERE wildcard = TRUE"
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                val resultSet = preparedStatement.executeQuery()
                if (resultSet.next()) {
                    wildCardIndex.add(resultSet.getString(1))
                }
                logger.info("Indexing subscription table [$tableName] finished.")
            } ?: run {
                logger.severe("Indexing subscription table not possible without database connection!")
            }
        } catch (e: SQLException) {
            logger.warning("CreateLocalIndex: Error fetching data: ${e.message}")
        }
    }

    override fun addSubscription(subscription: MqttSubscription) {
        if (Utils.isWildCardTopic(subscription.topicName)) vertx.eventBus().publish(addAddress, subscription)
        val sql = "INSERT INTO $tableName (client, topic, wildcard) VALUES (?, ?, ?) "+
                  "ON CONFLICT (client, topic) DO NOTHING"
        val levels = Utils.getTopicLevels(subscription.topicName).toTypedArray()
        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                preparedStatement.setString(1, subscription.clientId)
                preparedStatement.setArray(2, connection.createArrayOf("text", levels))
                preparedStatement.setBoolean(3, Utils.isWildCardTopic(subscription.topicName))
                preparedStatement.executeUpdate()
            }
        } catch (e: SQLException) {
            logger.warning("Error at inserting subscription [${e.message}] SQL: [$sql]")
        }
    }

    override fun removeSubscription(subscription: MqttSubscription) {
        if (Utils.isWildCardTopic(subscription.topicName)) vertx.eventBus().publish(delAddress, subscription)
        val sql = "DELETE FROM $tableName WHERE client = ? AND topic = ?"
        val levels = Utils.getTopicLevels(subscription.topicName).toTypedArray()
        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                preparedStatement.setString(1, subscription.clientId)
                preparedStatement.setArray(2, connection.createArrayOf("text", levels))
                preparedStatement.executeUpdate()
            }
        } catch (e: SQLException) {
            logger.warning("Error at removing subscription [${e.message}] SQL: [$sql]")
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
            logger.warning("Error at removing client [${e.message}] SQL: [$sql]")
        }
    }

    override fun findClients(topicName: String): Set<String> {
        val wildCardResult = wildCardIndex.findDataOfTopicName(topicName).toSet()
        val nonWildCardResult = findNonWildcardClientsForTopic(topicName) // TODO: start in parallel?
        logger.finest { "Found [${nonWildCardResult.size}] NonWildCard and [${wildCardResult.size}] WildCard clients." }
        return (wildCardResult + nonWildCardResult).distinct().toSet()
    }

    private fun findNonWildcardClientsForTopic(topicName: String): HashSet<String> {
        val nonWildCardResult = hashSetOf<String>()
        val topicLevels = Utils.getTopicLevels(topicName).toTypedArray()
        val sql = "SELECT client FROM $tableName WHERE topic = ?"
        try {
            db.connection?.let { connection ->
                val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
                preparedStatement.setArray(1, connection.createArrayOf("text", topicLevels))
                val resultSet = preparedStatement.executeQuery()
                while (resultSet.next()) {
                    val client = resultSet.getString(1)
                    nonWildCardResult.add(client)
                }
            }
        } catch (e: SQLException) {
            logger.warning("Error at fetching data [${e.message}] SQL: [$sql]")
        }
        return nonWildCardResult
    }
}