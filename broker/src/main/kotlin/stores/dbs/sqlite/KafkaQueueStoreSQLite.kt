package at.rocworks.stores.sqlite

import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.IKafkaQueueStore
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant

class KafkaQueueStoreSQLite(
    private val dbPath: String,
    private val tableNameSuffix: String = ""
) : AbstractVerticle(), IKafkaQueueStore {
    private val logger = Utils.getLogger(this::class.java)
    private lateinit var sqlClient: SQLiteClient

    private val queueTable = if (tableNameSuffix.isEmpty()) "kafka_queue" else "kafka_queue_$tableNameSuffix"
    private val offsetTable = if (tableNameSuffix.isEmpty()) "kafka_offsets" else "kafka_offsets_$tableNameSuffix"

    override fun start(startPromise: Promise<Void>) {
        sqlClient = SQLiteClient(vertx, dbPath)

        val createTablesSQL = JsonArray()
            .add("""
            CREATE TABLE IF NOT EXISTS $queueTable (
                offset_id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                payload BLOB,
                qos INTEGER NOT NULL DEFAULT 1,
                publisher_id TEXT,
                creation_time INTEGER NOT NULL,
                message_uuid TEXT NOT NULL
            );
            """.trimIndent())
            .add("CREATE INDEX IF NOT EXISTS ${queueTable}_topic_offset_idx ON $queueTable (topic, offset_id ASC);")
            .add("CREATE INDEX IF NOT EXISTS ${queueTable}_creation_idx ON $queueTable (creation_time);")
            .add("""
            CREATE TABLE IF NOT EXISTS $offsetTable (
                group_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                partition_id INTEGER NOT NULL DEFAULT 0,
                committed_offset INTEGER NOT NULL,
                last_commit_time INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                PRIMARY KEY (group_id, topic, partition_id)
            );
            """.trimIndent())

        sqlClient.initDatabase(createTablesSQL).onComplete { result ->
            if (result.succeeded()) {
                logger.info("SQLite Kafka tables '$queueTable' and '$offsetTable' are ready")
                startPromise.complete()
            } else {
                logger.severe("Failed to initialize SQLite Kafka tables: ${result.cause()?.message}")
                startPromise.fail(result.cause())
            }
        }
    }

    override fun enqueue(messages: List<BrokerMessage>): Future<Void> {
        if (messages.isEmpty()) return Future.succeededFuture()

        val sql = """
            INSERT INTO $queueTable (topic, payload, qos, publisher_id, creation_time, message_uuid)
            VALUES (?, ?, ?, ?, ?, ?)
        """.trimIndent()

        val batch = JsonArray()
        messages.forEach { message ->
            batch.add(JsonArray()
                .add(message.topicName)
                .add(message.payload)
                .add(message.qosLevel)
                .add(message.clientId)
                .add(message.time.toEpochMilli())
                .add(message.messageUuid)
            )
        }

        return sqlClient.executeBatch(sql, batch).mapEmpty()
    }

    override fun fetch(topic: String, startOffset: Long, limit: Int): Future<List<Pair<Long, BrokerMessage>>> {
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

        val params = if (tableNameSuffix.isEmpty()) {
            JsonArray().add(topic).add(startOffset).add(limit)
        } else {
            JsonArray().add(startOffset).add(limit)
        }

        return sqlClient.executeQuery(sql, params).map { results ->
            val list = mutableListOf<Pair<Long, BrokerMessage>>()
            results.forEach { row ->
                val rowObj = row as JsonObject
                val offset = rowObj.getLong("offset_id")
                val creationTime = rowObj.getLong("creation_time") ?: System.currentTimeMillis()
                val msg = BrokerMessage(
                    messageUuid = rowObj.getString("message_uuid"),
                    messageId = 0,
                    topicName = rowObj.getString("topic"),
                    payload = rowObj.getBinary("payload") ?: ByteArray(0),
                    qosLevel = rowObj.getInteger("qos", 1),
                    isRetain = false,
                    isDup = false,
                    isQueued = true,
                    clientId = rowObj.getString("publisher_id"),
                    time = Instant.ofEpochMilli(creationTime),
                    messageExpiryInterval = null
                )
                list.add(Pair(offset, msg))
            }
            list
        }
    }

    override fun getOffset(groupId: String, topic: String, partition: Int): Future<Long?> {
        val sql = """
            SELECT committed_offset 
            FROM $offsetTable 
            WHERE group_id = ? AND topic = ? AND partition_id = ?
        """.trimIndent()

        val params = JsonArray().add(groupId).add(topic).add(partition)

        return sqlClient.executeQuery(sql, params).map { results ->
            if (results.size() > 0) {
                results.getJsonObject(0).getLong("committed_offset")
            } else null
        }
    }

    override fun commitOffset(groupId: String, topic: String, partition: Int, offset: Long): Future<Void> {
        val now = System.currentTimeMillis() / 1000
        val sql = """
            INSERT INTO $offsetTable (group_id, topic, partition_id, committed_offset, last_commit_time)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (group_id, topic, partition_id) DO UPDATE SET 
                committed_offset = excluded.committed_offset,
                last_commit_time = excluded.last_commit_time
        """.trimIndent()

        val params = JsonArray().add(groupId).add(topic).add(partition).add(offset).add(now)

        return sqlClient.executeUpdate(sql, params).mapEmpty()
    }

    override fun getLatestOffset(topic: String): Future<Long> {
        val sql = if (tableNameSuffix.isEmpty()) {
            "SELECT MAX(offset_id) as max_offset FROM $queueTable WHERE topic = ?"
        } else {
            "SELECT MAX(offset_id) as max_offset FROM $queueTable"
        }
        val params = if (tableNameSuffix.isEmpty()) JsonArray().add(topic) else JsonArray()

        return sqlClient.executeQuery(sql, params).map { results ->
            if (results.size() > 0) {
                val maxOffset = results.getJsonObject(0).getLong("max_offset")
                maxOffset ?: 0L
            } else 0L
        }
    }

    override fun getEarliestOffset(topic: String): Future<Long> {
        val sql = if (tableNameSuffix.isEmpty()) {
            "SELECT MIN(offset_id) as min_offset FROM $queueTable WHERE topic = ?"
        } else {
            "SELECT MIN(offset_id) as min_offset FROM $queueTable"
        }
        val params = if (tableNameSuffix.isEmpty()) JsonArray().add(topic) else JsonArray()

        return sqlClient.executeQuery(sql, params).map { results ->
            if (results.size() > 0) {
                val minOffset = results.getJsonObject(0).getLong("min_offset")
                minOffset ?: 0L
            } else 0L
        }
    }

    override fun pruneExpired(olderThanMs: Long): Future<Int> {
        val sql = "DELETE FROM $queueTable WHERE creation_time < ?"
        val params = JsonArray().add(olderThanMs)

        return sqlClient.executeUpdate(sql, params)
    }
}
