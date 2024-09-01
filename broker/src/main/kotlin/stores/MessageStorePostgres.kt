package at.rocworks.stores

import at.rocworks.data.MqttMessage
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.SQLException
import java.util.logging.Logger

class MessageStorePostgres(
    private val name: String,
    private val url: String,
    private val username: String,
    private val password: String
): AbstractVerticle(), IMessageStore {
    private val logger = Logger.getLogger(this.javaClass.simpleName+"/"+name)

    private val connection = DriverManager.getConnection(url, username, password)

    /*
        CREATE TABLE retained (topic text[] NOT NULL, payload BYTEA)

        CREATE TABLE retained (
            topic text PRIMARY KEY,
            levels text[] NOT NULL,
            payload BYTEA)
        CREATE INDEX idx_retained_levels ON retained USING BTREE (levels);
    */

    init {
        connection.autoCommit = true
    }

    override fun start(startPromise: Promise<Void>) {
        // Function to get a connection to the PostgreSQL database
        startPromise.complete()
    }

    override fun get(topic: String): MqttMessage? {
        TODO("Not yet implemented")
    }

    override fun addAll(messages: List<MqttMessage>) {
        val rows: MutableList<Pair<Array<String>, ByteArray>> = ArrayList()
        messages.forEach { message ->
            val levels = message.topicName.split("/").toTypedArray()
            rows.add(Pair(levels, message.payload))
        }

        val sql = "INSERT INTO retained (topic, payload) VALUES (?, ?) "+
                    "ON CONFLICT (topic) DO UPDATE SET payload = EXCLUDED.payload"

        try {
            val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

            for ((topic, payload) in rows) {
                preparedStatement.setArray(1, connection.createArrayOf("text", topic))
                preparedStatement.setBytes(2, payload)
                preparedStatement.addBatch()
            }

            preparedStatement.executeBatch()
            logger.finest("Batch insert of [${rows.count()} rows successful.")
        } catch (e: SQLException) {
            e.printStackTrace()
        }
    }

    override fun delAll(messages: List<MqttMessage>) {
        TODO("Not yet implemented")
    }

    override fun findMatchingTopicNames(topicName: String, callback: (String) -> Boolean) {
        TODO("Not yet implemented")
    }


}