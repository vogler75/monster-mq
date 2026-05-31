package at.rocworks.stores.mongodb

import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.IKafkaQueueStore
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters.*
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.ReturnDocument
import com.mongodb.client.model.UpdateOptions
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import org.bson.Document
import org.bson.types.Binary
import java.time.Instant
import java.util.Date
import java.util.concurrent.Callable

class KafkaQueueStoreMongoDB(
    private val connectionString: String,
    private val databaseName: String,
    private val tableNameSuffix: String = ""
) : AbstractVerticle(), IKafkaQueueStore {
    private val logger = Utils.getLogger(this::class.java)

    private lateinit var mongoClient: MongoClient
    private lateinit var database: MongoDatabase
    private lateinit var queueCollection: MongoCollection<Document>
    private lateinit var offsetCollection: MongoCollection<Document>
    private lateinit var countersCollection: MongoCollection<Document>

    private val queueName = if (tableNameSuffix.isEmpty()) "kafka_queue" else "kafka_queue_$tableNameSuffix"
    private val offsetName = if (tableNameSuffix.isEmpty()) "kafka_offsets" else "kafka_offsets_$tableNameSuffix"

    override fun start(startPromise: Promise<Void>) {
        try {
            mongoClient = MongoClientPool.getClient(connectionString)
            database = mongoClient.getDatabase(databaseName)
            queueCollection = database.getCollection(queueName)
            offsetCollection = database.getCollection(offsetName)
            countersCollection = database.getCollection("counters")

            vertx.executeBlocking(Callable<Void?> {
                try {
                    queueCollection.createIndex(
                        Document("topic", 1).append("offset_id", 1),
                        IndexOptions().name("${queueName}_topic_offset_idx")
                    )
                    queueCollection.createIndex(
                        Document("creation_time", 1),
                        IndexOptions().name("${queueName}_creation_idx")
                    )
                    logger.info("MongoDB Kafka queue indexes created successfully")
                } catch (e: Exception) {
                    logger.warning("Failed to create indexes for Kafka queue: ${e.message}")
                }
                null
            })

            logger.info("MongoDB Kafka queue store initialized.")
            startPromise.complete()
        } catch (e: Exception) {
            logger.severe("Error starting MongoDB Kafka queue store: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun stop() {
        MongoClientPool.releaseClient(connectionString)
    }

    private fun nextOffsetId(): Long {
        val result = countersCollection.findOneAndUpdate(
            eq("_id", queueName),
            Document("\$inc", Document("seq", 1L)),
            FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER)
        )
        return result?.getLong("seq") ?: 1L
    }

    override fun enqueue(messages: List<BrokerMessage>): Future<Void> {
        if (messages.isEmpty()) return Future.succeededFuture()

        return vertx.executeBlocking(Callable<Void> {
            try {
                val docs = messages.map { message ->
                    Document(mapOf(
                        "offset_id" to nextOffsetId(),
                        "topic" to message.topicName,
                        "payload" to Binary(message.payload),
                        "qos" to message.qosLevel,
                        "publisher_id" to message.clientId,
                        "creation_time" to message.time.toEpochMilli(),
                        "message_uuid" to message.messageUuid
                    ))
                }
                queueCollection.insertMany(docs)
            } catch (e: Exception) {
                logger.warning("Error enqueuing MongoDB Kafka messages: ${e.message}")
                throw e
            }
            null
        })
    }

    override fun fetch(topic: String, startOffset: Long, limit: Int): Future<List<Pair<Long, BrokerMessage>>> {
        return vertx.executeBlocking(Callable<List<Pair<Long, BrokerMessage>>> {
            try {
                val filter = if (tableNameSuffix.isEmpty()) {
                    and(
                        eq("topic", topic),
                        gte("offset_id", startOffset)
                    )
                } else {
                    gte("offset_id", startOffset)
                }
                val results = queueCollection.find(filter).sort(Document("offset_id", 1)).limit(limit)

                results.map { doc ->
                    val offset = doc.getLong("offset_id")
                    val creationTime = doc.getLong("creation_time") ?: System.currentTimeMillis()
                    val msg = BrokerMessage(
                        messageUuid = doc.getString("message_uuid"),
                        messageId = 0,
                        topicName = doc.getString("topic"),
                        payload = doc.get("payload", Binary::class.java).data,
                        qosLevel = doc.getInteger("qos", 1),
                        isRetain = false,
                        isDup = false,
                        isQueued = true,
                        clientId = doc.getString("publisher_id"),
                        time = Instant.ofEpochMilli(creationTime),
                        messageExpiryInterval = null
                    )
                    Pair(offset, msg)
                }.toList()
            } catch (e: Exception) {
                logger.warning("Error fetching MongoDB Kafka messages: ${e.message}")
                throw e
            }
        })
    }

    override fun getOffset(groupId: String, topic: String, partition: Int): Future<Long?> {
        return vertx.executeBlocking(Callable<Long?> {
            try {
                val id = "$groupId:$topic:$partition"
                val doc = offsetCollection.find(eq("_id", id)).first()
                doc?.getLong("committed_offset")
            } catch (e: Exception) {
                logger.warning("Error getting committed offset from MongoDB: ${e.message}")
                throw e
            }
        })
    }

    override fun commitOffset(groupId: String, topic: String, partition: Int, offset: Long): Future<Void> {
        return vertx.executeBlocking(Callable<Void> {
            try {
                val id = "$groupId:$topic:$partition"
                offsetCollection.updateOne(
                    eq("_id", id),
                    Document("\$set", Document(mapOf(
                        "group_id" to groupId,
                        "topic" to topic,
                        "partition_id" to partition,
                        "committed_offset" to offset,
                        "last_commit_time" to Date()
                    ))),
                    UpdateOptions().upsert(true)
                )
            } catch (e: Exception) {
                logger.warning("Error committing MongoDB offset: ${e.message}")
                throw e
            }
            null
        })
    }

    override fun getLatestOffset(topic: String): Future<Long> {
        return vertx.executeBlocking(Callable<Long> {
            try {
                val filter = if (tableNameSuffix.isEmpty()) eq("topic", topic) else Document()
                val doc = queueCollection.find(filter)
                    .sort(Document("offset_id", -1))
                    .first()
                doc?.getLong("offset_id") ?: 0L
            } catch (e: Exception) {
                logger.warning("Error getting latest MongoDB offset: ${e.message}")
                throw e
            }
        })
    }

    override fun getEarliestOffset(topic: String): Future<Long> {
        return vertx.executeBlocking(Callable<Long> {
            try {
                val filter = if (tableNameSuffix.isEmpty()) eq("topic", topic) else Document()
                val doc = queueCollection.find(filter)
                    .sort(Document("offset_id", 1))
                    .first()
                doc?.getLong("offset_id") ?: 0L
            } catch (e: Exception) {
                logger.warning("Error getting earliest MongoDB offset: ${e.message}")
                throw e
            }
        })
    }

    override fun pruneExpired(olderThanMs: Long): Future<Int> {
        return vertx.executeBlocking(Callable<Int> {
            try {
                val result = queueCollection.deleteMany(lt("creation_time", olderThanMs))
                result.deletedCount.toInt()
            } catch (e: Exception) {
                logger.warning("Error pruning expired MongoDB Kafka messages: ${e.message}")
                throw e
            }
        })
    }

    override fun getConsumerGroups(): Future<List<at.rocworks.stores.KafkaConsumerGroup>> {
        return vertx.executeBlocking(Callable<List<at.rocworks.stores.KafkaConsumerGroup>> {
            try {
                val groupsMap = mutableMapOf<String, MutableList<Document>>()
                offsetCollection.find().forEach { doc ->
                    val groupId = doc.getString("group_id") ?: ""
                    if (groupId.isNotEmpty()) {
                        groupsMap.computeIfAbsent(groupId) { mutableListOf() }.add(doc)
                    }
                }
                groupsMap.map { (groupId, docs) ->
                    val topics = docs.mapNotNull { it.getString("topic") }.distinct()
                    val maxTime = docs.mapNotNull { it.getDate("last_commit_time")?.time }.maxOrNull() ?: 0L
                    at.rocworks.stores.KafkaConsumerGroup(groupId, topics, maxTime)
                }
            } catch (e: Exception) {
                logger.warning("Error getting consumer groups from MongoDB: ${e.message}")
                emptyList()
            }
        })
    }

    override fun deleteConsumerGroup(groupId: String): Future<Boolean> {
        return vertx.executeBlocking(Callable<Boolean> {
            try {
                val result = offsetCollection.deleteMany(eq("group_id", groupId))
                result.deletedCount > 0
            } catch (e: Exception) {
                logger.warning("Error deleting consumer group from MongoDB: ${e.message}")
                false
            }
        })
    }
}
