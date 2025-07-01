package at.rocworks.stores.postgres

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.stores.DatabaseConnection
import at.rocworks.stores.IMessageStoreExtended
import at.rocworks.stores.MessageStoreType
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import java.sql.*

class MessageStorePostgres(
    private val name: String,
    private val url: String,
    private val username: String,
    private val password: String
): AbstractVerticle(), IMessageStoreExtended {
    private val logger = Utils.getLogger(this::class.java, name)
    private val tableName = name.lowercase()

    private companion object {
        const val MAX_FIXED_TOPIC_LEVELS = 9
        val FIXED_TOPIC_COLUMN_NAMES = (0 until MAX_FIXED_TOPIC_LEVELS).map { "topic_${it+1}" }
        val ALL_PK_COLUMNS = FIXED_TOPIC_COLUMN_NAMES + "topic_r"

        fun splitTopic(topicName: String): Triple<List<String>, List<String>, String> {
            val levels = Utils.getTopicLevels(topicName)
            val first = levels.take(MAX_FIXED_TOPIC_LEVELS)
            val rest = levels.drop(MAX_FIXED_TOPIC_LEVELS)
            val last = levels.last()
            return Triple(first, rest, last)
        }
    }

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun init(connection: Connection): Future<Void> {
            val promise = Promise.promise<Void>()
            try {
                connection.createStatement().use { statement ->
                    val pkColumnsString = ALL_PK_COLUMNS.joinToString(", ")
                    val fixedTopicColumns = FIXED_TOPIC_COLUMN_NAMES.joinToString(", ") { "$it text NOT NULL" }
                    statement.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS $tableName (
                        topic text, -- full topic for                     
                        ${fixedTopicColumns}, -- topic levels for wildcard matching                        
                        topic_r text[] NOT NULL, -- remaining topic levels
                        topic_l text NOT NULL, -- last level of the topic
                        time TIMESTAMPTZ,                    
                        payload_blob BYTEA,
                        payload_json JSONB,
                        qos INT,
                        retained BOOLEAN,
                        client_id VARCHAR(65535), 
                        message_uuid VARCHAR(36),
                        PRIMARY KEY (topic)
                    )
                    """.trimIndent())
                    statement.executeUpdate("""
                    CREATE INDEX IF NOT EXISTS ${tableName}_topic ON $tableName ($pkColumnsString)
                    """.trimIndent())
                    logger.info("Message store [$name] is ready [${Utils.getCurrentFunctionName()}]")
                    promise.complete()
                }
            } catch (e: Exception) {
                logger.severe("Error in creating table [$name]: ${e.message} [${Utils.getCurrentFunctionName()}]")
                promise.fail(e)
            }
            return promise.future()
        }
    }

    override fun getName(): String = name
    override fun getType(): MessageStoreType = MessageStoreType.POSTGRES

    override fun start(startPromise: Promise<Void>) {
        db.start(vertx, startPromise)
    }

    override fun get(topicName: String): MqttMessage? {
        try {
            db.connection?.let { connection ->
                val sql = """SELECT payload_blob, qos, retained, client_id, message_uuid FROM $tableName 
                             WHERE topic = ?"""
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, topicName)
                    val resultSet = preparedStatement.executeQuery()
                    if (resultSet.next()) {
                        val payload = resultSet.getBytes(1)
                        val qos = resultSet.getInt(2)
                        val retained = resultSet.getBoolean(3)
                        val clientId = resultSet.getString(4)
                        val messageUuid = resultSet.getString(5)

                        return MqttMessage(
                            messageUuid = messageUuid,
                            messageId = 0,
                            topicName = topicName,
                            payload = payload,
                            qosLevel = qos,
                            isRetain = retained,
                            isQueued = false,
                            clientId = clientId,
                            isDup = false
                        )
                    }
                }
            }
        } catch (e: SQLException) {
            logger.severe("Error fetching data for topic [$topicName]: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
        return null
    }

    override fun addAll(messages: List<MqttMessage>) {
        val sql = """INSERT INTO $tableName (topic, ${FIXED_TOPIC_COLUMN_NAMES.joinToString(", ")}, topic_r, topic_l,
                   time, payload_blob, payload_json, qos, retained, client_id, message_uuid) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::JSONB, ?, ?, ?, ?) 
                   ON CONFLICT (topic) DO UPDATE 
                   SET time = EXCLUDED.time, 
                   payload_blob = EXCLUDED.payload_blob, 
                   payload_json = EXCLUDED.payload_json,
                   qos = EXCLUDED.qos, 
                   retained = EXCLUDED.retained, 
                   client_id = EXCLUDED.client_id, 
                   message_uuid = EXCLUDED.message_uuid 
                   """

        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    messages.forEach { message ->
                        val (first, rest, last) = splitTopic(message.topicName)
                        preparedStatement.setString(1, message.topicName)
                        repeat(MAX_FIXED_TOPIC_LEVELS) { preparedStatement.setString(it + 2, first.getOrNull(it) ?: "") }
                        preparedStatement.setArray(MAX_FIXED_TOPIC_LEVELS + 2, connection.createArrayOf("text", rest.toTypedArray()))
                        preparedStatement.setString(MAX_FIXED_TOPIC_LEVELS + 3, last)
                        preparedStatement.setTimestamp(MAX_FIXED_TOPIC_LEVELS + 4, Timestamp.from(message.time))
                        preparedStatement.setBytes(MAX_FIXED_TOPIC_LEVELS + 5, message.payload)
                        preparedStatement.setString(MAX_FIXED_TOPIC_LEVELS + 6, message.getPayloadAsJson())
                        preparedStatement.setInt(MAX_FIXED_TOPIC_LEVELS + 7, message.qosLevel)
                        preparedStatement.setBoolean(MAX_FIXED_TOPIC_LEVELS + 8, message.isRetain)
                        preparedStatement.setString(MAX_FIXED_TOPIC_LEVELS + 9, message.clientId)
                        preparedStatement.setString(MAX_FIXED_TOPIC_LEVELS + 10, message.messageUuid)
                        preparedStatement.addBatch()
                    }
                    preparedStatement.executeBatch()
                }
            }
        } catch (e: SQLException) {
            logger.severe("Error inserting batch data [${e.errorCode}] [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun delAll(topics: List<String>) {
        val sql = "DELETE FROM $tableName WHERE topic = ?"
        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    for (topic in topics) {
                        preparedStatement.setString(1, topic)
                        preparedStatement.addBatch()
                    }
                    preparedStatement.executeBatch()
                }
            }
        } catch (e: SQLException) {
            logger.severe("Error deleting batch data [${e.message}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun findMatchingMessages(topicName: String, callback: (MqttMessage) -> Boolean) {
        val filter = Utils.getTopicLevels(topicName).mapIndexed { index, level ->
            when (level) {
                "+", "#" -> null
                else -> {
                    if (index >= MAX_FIXED_TOPIC_LEVELS) {
                        Pair("topic_r[${index - MAX_FIXED_TOPIC_LEVELS + 1}] = ?", level)
                    } else {
                        Pair(FIXED_TOPIC_COLUMN_NAMES[index] + " = ?", level)
                    }
                }
            }
        }.filterNotNull()
        try {
            db.connection?.let { connection ->
                val where = filter.joinToString(" AND ") { it.first }.ifEmpty { "1=1" }
                val sql = "SELECT ${FIXED_TOPIC_COLUMN_NAMES.joinToString(", ")}, topic_r, " +
                          "payload_blob, qos, client_id, message_uuid "+
                          "FROM $tableName WHERE $where"
                logger.finest { "SQL: $sql [${Utils.getCurrentFunctionName()}]" }
                connection.prepareStatement(sql).use { preparedStatement ->
                    filter.forEachIndexed { i, x -> preparedStatement.setString(i + 1, x.second) }

                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        val topic = (((1 until MAX_FIXED_TOPIC_LEVELS + 1).map { resultSet.getString(it) }.filterNot { it.isNullOrEmpty() }) +
                                (resultSet.getArray(MAX_FIXED_TOPIC_LEVELS + 1).array as Array<String>)).joinToString("/")

                        val payload = resultSet.getBytes(11)
                        val qos = resultSet.getInt(12)
                        val clientId = resultSet.getString(13)
                        val messageUuid = resultSet.getString(14)
                        val message = MqttMessage(
                            messageUuid = messageUuid,
                            messageId = 0,
                            topicName = topic,
                            payload = payload,
                            qosLevel = qos,
                            isRetain = true,
                            isDup = false,
                            isQueued = false,
                            clientId = clientId
                        )
                        callback(message)
                    }
                }
            }
        } catch (e: SQLException) {
            logger.severe("Error finding data for topic [$topicName]: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun findTopicsByName(name: String, ignoreCase: Boolean, namespace: String): List<String> {
        val resultTopics = mutableListOf<String>()
        val sqlSearchPattern = name.replace("*", "%").replace("+", "_") // Also handle MQTT single level wildcard for LIKE
        val sqlNamespacePattern = if (namespace.isEmpty()) "%" else "$namespace/%"

        val sql = """
        SELECT topic
        FROM $tableName AS t 
        WHERE topic_l <> '${Const.MCP_CONFIG_TOPIC}'
        AND ${if (ignoreCase) "LOWER(topic)" else "topic"} LIKE ${if (ignoreCase) "LOWER(?)" else "?"}        
        AND ${if (ignoreCase) "LOWER(topic)" else "topic"} LIKE ${if (ignoreCase) "LOWER(?)" else "?"} 
        ORDER BY topic
        """.trimIndent()

        logger.fine { "findTopicsByName SQL: $sql with pattern '$sqlSearchPattern' [${Utils.getCurrentFunctionName()}]" }

        db.connection?.let { connection ->
            connection.prepareStatement(sql).use { preparedStatement ->
                preparedStatement.setString(1, sqlNamespacePattern)
                preparedStatement.setString(2, sqlSearchPattern)
                val resultSet = preparedStatement.executeQuery()
                while (resultSet.next()) {
                    val fullTopic = resultSet.getString("topic") ?: ""
                    resultTopics.add(fullTopic)
                }
            }
        }

        logger.fine("findTopicsByName result: ${resultTopics.size} topics found [${Utils.getCurrentFunctionName()}]")
        return resultTopics
    }

    override fun findTopicsByConfig(config: String, description: String, ignoreCase: Boolean, namespace: String): List<Pair<String, String>> {
        val resultTopics = mutableListOf<Pair<String, String>>()
        val sqlSearchPattern = description
        val sqlNamespacePattern = if (namespace.isEmpty()) "%" else "$namespace/%"

        val sql = """
        SELECT RTRIM(topic, '/${Const.MCP_CONFIG_TOPIC}') AS topic, payload_json AS config, payload_json AS config
        FROM $tableName
        WHERE topic_l = '${Const.MCP_CONFIG_TOPIC}' 
        AND ${if (ignoreCase) "LOWER(topic)" else "topic"} LIKE ${if (ignoreCase) "LOWER(?)" else "?"}
        AND payload_json->>'${config}' ${if (ignoreCase) "~* ?" else "~ ?"}   
        ORDER BY topic
        """.trimIndent()

        logger.fine { "findTopicsByDescription SQL: $sql with pattern '$sqlSearchPattern' [${Utils.getCurrentFunctionName()}]" }

        db.connection?.let { connection ->
            connection.prepareStatement(sql).use { preparedStatement ->
                preparedStatement.setString(1, sqlNamespacePattern)
                preparedStatement.setString(2, sqlSearchPattern)
                val resultSet = preparedStatement.executeQuery()
                while (resultSet.next()) {
                    val fullTopic = resultSet.getString("topic") ?: ""
                    val configJson = resultSet.getString("config") ?: ""
                    resultTopics.add(Pair(fullTopic, configJson))
                }
            }
        }

        logger.fine("findTopicsByConfig result: ${resultTopics.size} topics found [${Utils.getCurrentFunctionName()}]")
        return resultTopics
    }
}