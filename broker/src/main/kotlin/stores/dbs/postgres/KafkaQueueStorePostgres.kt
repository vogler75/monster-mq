package at.rocworks.stores.postgres

import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.DatabaseConnection
import at.rocworks.stores.IKafkaQueueStore
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.time.Instant

class KafkaQueueStorePostgres(
    private val url: String,
    private val username: String,
    private val password: String,
    private val schema: String? = null,
    private val tableNameSuffix: String = ""
) : AbstractVerticle(), IKafkaQueueStore {
    private val logger = Utils.getLogger(this::class.java)

    private val queueTable = if (tableNameSuffix.isEmpty()) "kafka_queue" else "kafka_queue_$tableNameSuffix"
    private val offsetTable = if (tableNameSuffix.isEmpty()) "kafka_offsets" else "kafka_offsets_$tableNameSuffix"

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun init(connection: Connection): Future<Void> {
            val promise = Promise.promise<Void>()
            try {
                connection.autoCommit = false

                if (!schema.isNullOrBlank()) {
                    connection.createStatement().use { stmt ->
                        stmt.execute("CREATE SCHEMA IF NOT EXISTS \"$schema\"")
                        stmt.execute("SET search_path TO \"$schema\", public")
                    }
                }

                connection.createStatement().use { statement ->
                    statement.executeUpdate("""
                        CREATE TABLE IF NOT EXISTS $queueTable (
                            offset_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            topic TEXT NOT NULL,
                            payload BYTEA,
                            qos INT NOT NULL DEFAULT 1,
                            publisher_id VARCHAR(65535),
                            creation_time BIGINT NOT NULL,
                            message_uuid VARCHAR(36) NOT NULL
                        )
                    """.trimIndent())

                    statement.executeUpdate("""
                        CREATE TABLE IF NOT EXISTS $offsetTable (
                            group_id VARCHAR(255) NOT NULL,
                            topic TEXT NOT NULL,
                            partition_id INT NOT NULL DEFAULT 0,
                            committed_offset BIGINT NOT NULL,
                            last_commit_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                            PRIMARY KEY (group_id, topic, partition_id)
                        )
                    """.trimIndent())

                    statement.executeUpdate(
                        "CREATE INDEX IF NOT EXISTS ${queueTable}_topic_offset_idx ON $queueTable (topic, offset_id ASC)"
                    )
                    statement.executeUpdate(
                        "CREATE INDEX IF NOT EXISTS ${queueTable}_creation_time_idx ON $queueTable (creation_time)"
                    )
                }

                connection.commit()
                logger.info("PostgreSQL Kafka tables '$queueTable' and '$offsetTable' are ready")
                promise.complete()
            } catch (e: Exception) {
                logger.severe("Error creating Kafka tables: ${e.message}")
                promise.fail(e)
            }
            return promise.future()
        }
    }

    override fun start(startPromise: Promise<Void>) {
        db.start(vertx, startPromise)
    }

    override fun enqueue(messages: List<BrokerMessage>): Future<Void> {
        if (messages.isEmpty()) return Future.succeededFuture()

        return vertx.executeBlocking(java.util.concurrent.Callable<Void> {
            val sql = """
                INSERT INTO $queueTable (topic, payload, qos, publisher_id, creation_time, message_uuid)
                VALUES (?, ?, ?, ?, ?, ?)
            """.trimIndent()

            try {
                db.connection?.let { connection ->
                    connection.prepareStatement(sql).use { ps ->
                        messages.forEach { message ->
                            ps.setString(1, message.topicName)
                            ps.setBytes(2, message.payload)
                            ps.setInt(3, message.qosLevel)
                            ps.setString(4, message.clientId)
                            ps.setLong(5, message.time.toEpochMilli())
                            ps.setString(6, message.messageUuid)
                            ps.addBatch()
                        }
                        ps.executeBatch()
                    }
                    connection.commit()
                } ?: throw SQLException("No database connection available")
            } catch (e: SQLException) {
                try { db.connection?.rollback() } catch (_: SQLException) {}
                logger.warning("Error enqueuing Kafka messages: ${e.message}")
                throw e
            }
            null
        })
    }

    override fun fetch(topic: String, startOffset: Long, limit: Int): Future<List<Pair<Long, BrokerMessage>>> {
        return vertx.executeBlocking(java.util.concurrent.Callable<List<Pair<Long, BrokerMessage>>> {
            val sql = if (tableNameSuffix.isEmpty()) {
                """
                SELECT offset_id, topic, payload, qos, publisher_id, creation_time, message_uuid
                FROM $queueTable
                WHERE topic = ? AND offset_id >= ?
                ORDER BY offset_id ASC
                LIMIT ?
                """.trimIndent()
            } else {
                """
                SELECT offset_id, topic, payload, qos, publisher_id, creation_time, message_uuid
                FROM $queueTable
                WHERE offset_id >= ?
                ORDER BY offset_id ASC
                LIMIT ?
                """.trimIndent()
            }

            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { ps ->
                    if (tableNameSuffix.isEmpty()) {
                        ps.setString(1, topic)
                        ps.setLong(2, startOffset)
                        ps.setInt(3, limit)
                    } else {
                        ps.setLong(1, startOffset)
                        ps.setInt(2, limit)
                    }
                    val rs = ps.executeQuery()
                    val list = mutableListOf<Pair<Long, BrokerMessage>>()
                    while (rs.next()) {
                        list.add(resultSetToRecord(rs))
                    }
                    list
                }
            } ?: throw SQLException("No database connection available")
        })
    }

    override fun getOffset(groupId: String, topic: String, partition: Int): Future<Long?> {
        return vertx.executeBlocking(java.util.concurrent.Callable<Long?> {
            val sql = """
                SELECT committed_offset 
                FROM $offsetTable 
                WHERE group_id = ? AND topic = ? AND partition_id = ?
            """.trimIndent()

            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { ps ->
                    ps.setString(1, groupId)
                    ps.setString(2, topic)
                    ps.setInt(3, partition)
                    val rs = ps.executeQuery()
                    if (rs.next()) {
                        rs.getLong("committed_offset")
                    } else {
                        null
                    }
                }
            } ?: throw SQLException("No database connection available")
        })
    }

    override fun commitOffset(groupId: String, topic: String, partition: Int, offset: Long): Future<Void> {
        return vertx.executeBlocking(java.util.concurrent.Callable<Void> {
            val sql = """
                INSERT INTO $offsetTable (group_id, topic, partition_id, committed_offset, last_commit_time)
                VALUES (?, ?, ?, ?, now())
                ON CONFLICT (group_id, topic, partition_id) DO UPDATE SET 
                    committed_offset = excluded.committed_offset,
                    last_commit_time = now()
            """.trimIndent()

            try {
                db.connection?.let { connection ->
                    connection.prepareStatement(sql).use { ps ->
                        ps.setString(1, groupId)
                        ps.setString(2, topic)
                        ps.setInt(3, partition)
                        ps.setLong(4, offset)
                        ps.executeUpdate()
                    }
                    connection.commit()
                } ?: throw SQLException("No database connection available")
            } catch (e: SQLException) {
                try { db.connection?.rollback() } catch (_: SQLException) {}
                logger.warning("Error committing offset: ${e.message}")
                throw e
            }
            null
        })
    }

    override fun getLatestOffset(topic: String): Future<Long> {
        return vertx.executeBlocking(java.util.concurrent.Callable<Long> {
            val sql = if (tableNameSuffix.isEmpty()) {
                "SELECT MAX(offset_id) as max_offset FROM $queueTable WHERE topic = ?"
            } else {
                "SELECT MAX(offset_id) as max_offset FROM $queueTable"
            }
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { ps ->
                    if (tableNameSuffix.isEmpty()) {
                        ps.setString(1, topic)
                    }
                    val rs = ps.executeQuery()
                    if (rs.next()) {
                        val maxOffset = rs.getLong("max_offset")
                        if (rs.wasNull()) 0L else maxOffset
                    } else {
                        0L
                    }
                }
            } ?: throw SQLException("No database connection available")
        })
    }

    override fun getEarliestOffset(topic: String): Future<Long> {
        return vertx.executeBlocking(java.util.concurrent.Callable<Long> {
            val sql = if (tableNameSuffix.isEmpty()) {
                "SELECT MIN(offset_id) as min_offset FROM $queueTable WHERE topic = ?"
            } else {
                "SELECT MIN(offset_id) as min_offset FROM $queueTable"
            }
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { ps ->
                    if (tableNameSuffix.isEmpty()) {
                        ps.setString(1, topic)
                    }
                    val rs = ps.executeQuery()
                    if (rs.next()) {
                        val minOffset = rs.getLong("min_offset")
                        if (rs.wasNull()) 0L else minOffset
                    } else {
                        0L
                    }
                }
            } ?: throw SQLException("No database connection available")
        })
    }

    override fun pruneExpired(olderThanMs: Long): Future<Int> {
        return vertx.executeBlocking(java.util.concurrent.Callable<Int> {
            val sql = "DELETE FROM $queueTable WHERE creation_time < ?"
            try {
                db.connection?.let { connection ->
                    connection.prepareStatement(sql).use { ps ->
                        ps.setLong(1, olderThanMs)
                        val count = ps.executeUpdate()
                        connection.commit()
                        count
                    }
                } ?: throw SQLException("No database connection available")
            } catch (e: SQLException) {
                try { db.connection?.rollback() } catch (_: SQLException) {}
                logger.warning("Error pruning expired Kafka messages: ${e.message}")
                throw e
            }
        })
    }

    private fun resultSetToRecord(rs: ResultSet): Pair<Long, BrokerMessage> {
        val offset = rs.getLong("offset_id")
        val creationTime = rs.getLong("creation_time")
        val msg = BrokerMessage(
            messageUuid = rs.getString("message_uuid"),
            messageId = 0,
            topicName = rs.getString("topic"),
            payload = rs.getBytes("payload"),
            qosLevel = rs.getInt("qos"),
            isRetain = false,
            isDup = false,
            isQueued = true,
            clientId = rs.getString("publisher_id"),
            time = Instant.ofEpochMilli(creationTime),
            messageExpiryInterval = null
        )
        return Pair(offset, msg)
    }
}
