package at.rocworks.stores.sqlite

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

class MessageStoreSQLite(
    private val name: String,
    private val dbPath: String
): AbstractVerticle(), IMessageStoreExtended {
    private val logger = Utils.getLogger(this::class.java, name)
    private val tableName = name.lowercase()

    private companion object {
        const val MAX_FIXED_TOPIC_LEVELS = 9
        val FIXED_TOPIC_COLUMN_NAMES = (0 until MAX_FIXED_TOPIC_LEVELS).map { "topic_${it+1}" }

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

    override fun getName(): String = name
    override fun getType(): MessageStoreType = MessageStoreType.SQLITE

    override fun start(startPromise: Promise<Void>) {
        // Initialize shared connection
        SharedSQLiteConnection.initialize(vertx)
        
        // Create tables using shared connection
        SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
            connection.createStatement().use { statement ->
                val fixedTopicColumns = FIXED_TOPIC_COLUMN_NAMES.joinToString(", ") { "$it TEXT NOT NULL DEFAULT ''" }
                statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS $tableName (
                    topic TEXT PRIMARY KEY, -- full topic                   
                    ${fixedTopicColumns}, -- topic levels for wildcard matching                        
                    topic_r TEXT, -- remaining topic levels as JSON
                    topic_l TEXT NOT NULL, -- last level of the topic
                    time TEXT,                    
                    payload_blob BLOB,
                    payload_json TEXT,
                    qos INTEGER,
                    retained BOOLEAN,
                    client_id TEXT, 
                    message_uuid TEXT
                )
                """.trimIndent())
                val allPkColumns = FIXED_TOPIC_COLUMN_NAMES + "topic_r"
                statement.executeUpdate("""
                CREATE INDEX IF NOT EXISTS ${tableName}_topic ON $tableName (${allPkColumns.joinToString(", ")})
                """.trimIndent())
                logger.info("SQLite Message store [$name] is ready [start]")
            }
        }.onComplete(startPromise)
    }

    override fun get(topicName: String): MqttMessage? {
        return try {
            SharedSQLiteConnection.executeBlocking(dbPath) { connection ->
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

                        MqttMessage(
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
                    } else {
                        null
                    }
                }
            }.result()
        } catch (e: Exception) {
            logger.severe("Error fetching data for topic [$topicName]: ${e.message} [get]")
            null
        }
    }

    override fun addAll(messages: List<MqttMessage>) {
        val fixedColumns = FIXED_TOPIC_COLUMN_NAMES.joinToString(", ")
        val placeholders = FIXED_TOPIC_COLUMN_NAMES.joinToString(", ") { "?" }
        val sql = """INSERT INTO $tableName (topic, $fixedColumns, topic_r, topic_l,
                   time, payload_blob, payload_json, qos, retained, client_id, message_uuid) 
                   VALUES (?, $placeholders, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
                   ON CONFLICT (topic) DO UPDATE 
                   SET time = excluded.time, 
                   payload_blob = excluded.payload_blob, 
                   payload_json = excluded.payload_json,
                   qos = excluded.qos, 
                   retained = excluded.retained, 
                   client_id = excluded.client_id, 
                   message_uuid = excluded.message_uuid 
                   """

        try {
            SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    messages.forEach { message ->
                        val (first, rest, last) = splitTopic(message.topicName)
                        var paramIndex = 1
                        preparedStatement.setString(paramIndex++, message.topicName)
                        repeat(MAX_FIXED_TOPIC_LEVELS) { 
                            preparedStatement.setString(paramIndex++, first.getOrNull(it) ?: "") 
                        }
                        // Store remaining levels as JSON string instead of PostgreSQL array
                        val restJson = if (rest.isNotEmpty()) {
                            "[\"${rest.joinToString("\",\"")}\"]"
                        } else {
                            "[]"
                        }
                        preparedStatement.setString(paramIndex++, restJson)
                        preparedStatement.setString(paramIndex++, last)
                        preparedStatement.setString(paramIndex++, message.time.toString())
                        preparedStatement.setBytes(paramIndex++, message.payload)
                        preparedStatement.setString(paramIndex++, message.getPayloadAsJson())
                        preparedStatement.setInt(paramIndex++, message.qosLevel)
                        preparedStatement.setBoolean(paramIndex++, message.isRetain)
                        preparedStatement.setString(paramIndex++, message.clientId)
                        preparedStatement.setString(paramIndex, message.messageUuid)
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
            SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
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
        val levels = Utils.getTopicLevels(topicName)
        val filter = levels.mapIndexed { index, level ->
            when (level) {
                "+", "#" -> null
                else -> {
                    if (index >= MAX_FIXED_TOPIC_LEVELS) {
                        // For SQLite, we need to use JSON functions to extract array elements
                        val jsonIndex = index - MAX_FIXED_TOPIC_LEVELS
                        Pair("json_extract(topic_r, '\$[$jsonIndex]') = ?", level)
                    } else {
                        Pair(FIXED_TOPIC_COLUMN_NAMES[index] + " = ?", level)
                    }
                }
            }
        }.filterNotNull()
        
        try {
            SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
                val where = filter.joinToString(" AND ") { it.first }.ifEmpty { "1=1" } +
                        (if (topicName.endsWith("#")) ""
                        else {
                            if (levels.size < MAX_FIXED_TOPIC_LEVELS) {
                                " AND " + FIXED_TOPIC_COLUMN_NAMES[levels.size] + " = ''"
                            } else {
                                // For SQLite, check JSON array length
                                " AND json_array_length(topic_r) = " + (levels.size - MAX_FIXED_TOPIC_LEVELS)
                            }
                        })
                val sql = "SELECT topic, payload_blob, qos, client_id, message_uuid " +
                          "FROM $tableName WHERE $where"
                logger.fine { "SQL: $sql [${Utils.getCurrentFunctionName()}]" }
                connection.prepareStatement(sql).use { preparedStatement ->
                    filter.forEachIndexed { i, x -> preparedStatement.setString(i + 1, x.second) }
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        val topic = resultSet.getString(1)
                        val payload = resultSet.getBytes(2)
                        val qos = resultSet.getInt(3)
                        val clientId = resultSet.getString(4)
                        val messageUuid = resultSet.getString(5)
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
        val sqlSearchPattern = name.replace("*", "%").replace("+", "_")
        val sqlNamespacePattern = if (namespace.isEmpty()) "%" else "$namespace/%"

        val sql = """
        SELECT topic
        FROM $tableName AS t 
        WHERE topic_l != '${Const.MCP_CONFIG_TOPIC}'
        AND ${if (ignoreCase) "LOWER(topic)" else "topic"} LIKE ${if (ignoreCase) "LOWER(?)" else "?"}        
        AND ${if (ignoreCase) "LOWER(topic)" else "topic"} LIKE ${if (ignoreCase) "LOWER(?)" else "?"} 
        ORDER BY topic
        """.trimIndent()

        logger.fine { "findTopicsByName SQL: $sql with pattern '$sqlSearchPattern' [${Utils.getCurrentFunctionName()}]" }

        SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
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

        // SQLite uses different JSON syntax than PostgreSQL
        val sql = """
        SELECT REPLACE(topic, '/${Const.MCP_CONFIG_TOPIC}', '') AS topic, payload_json AS config
        FROM $tableName
        WHERE topic_l = '${Const.MCP_CONFIG_TOPIC}' 
        AND ${if (ignoreCase) "LOWER(topic)" else "topic"} LIKE ${if (ignoreCase) "LOWER(?)" else "?"}
        AND ${if (ignoreCase) "LOWER(json_extract(payload_json, '\$.$config'))" else "json_extract(payload_json, '\$.$config')"} LIKE ${if (ignoreCase) "LOWER(?)" else "?"}
        ORDER BY topic
        """.trimIndent()

        logger.fine { "findTopicsByDescription SQL: $sql with pattern '$sqlSearchPattern' [${Utils.getCurrentFunctionName()}]" }

        SharedSQLiteConnection.executeBlockingVoid(dbPath) { connection ->
            connection.prepareStatement(sql).use { preparedStatement ->
                preparedStatement.setString(1, sqlNamespacePattern)
                preparedStatement.setString(2, "%$sqlSearchPattern%")
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