package at.rocworks.stores.mongodb

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.IQueueStoreSync
import at.rocworks.stores.QueueStoreType
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.*
import com.mongodb.client.model.Filters.*
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import org.bson.Document
import org.bson.types.Binary
import java.util.concurrent.Callable

class QueueStoreMongoDBV1(
    private val connectionString: String,
    private val databaseName: String
) : AbstractVerticle(), IQueueStoreSync {
    private val logger = Utils.getLogger(this::class.java)
    private lateinit var mongoClient: MongoClient
    private lateinit var database: MongoDatabase

    private lateinit var queuedMessagesCollection: MongoCollection<Document>
    private lateinit var queuedMessagesClientsCollection: MongoCollection<Document>


    override fun getType(): QueueStoreType = QueueStoreType.MONGODB

    override fun start(startPromise: Promise<Void>) {
        try {
            mongoClient = MongoClientPool.getClient(connectionString)
            database = mongoClient.getDatabase(databaseName)

            // Initialize collections
            queuedMessagesCollection = database.getCollection("queuedmessages")
            queuedMessagesClientsCollection = database.getCollection("queuedmessagesclients")

            // Create indexes in background to avoid blocking the event loop
            vertx.executeBlocking(Callable {
                try {
                    queuedMessagesCollection.createIndex(Document("client_id", 1))
                    queuedMessagesCollection.createIndex(Document("message_uuid", 1))  // For $lookup joins
                    queuedMessagesClientsCollection.createIndex(Document("client_id", 1))
                    queuedMessagesClientsCollection.createIndex(Document("client_id", 1).append("status", 1).append("message_uuid", 1))
                    logger.info("MongoDB queue store indexes created successfully")
                } catch (e: Exception) {
                    logger.warning("Failed to create indexes: ${e.message}")
                }
                null
            })

            logger.fine("MongoDB connection established successfully.")
            startPromise.complete()
        } catch (e: Exception) {
            logger.severe("Error while starting MongoDB connection: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun stop() {
        MongoClientPool.releaseClient(connectionString)
        logger.info("MongoDB connection released.")
    }

    override fun enqueueMessages(messages: List<Pair<BrokerMessage, List<String>>>) {
        try {
            // Batch message upserts
            val messageOps = messages.map { (message, _) ->
                val messageDocument = Document(mapOf(
                    "message_uuid" to message.messageUuid,
                    "message_id" to message.messageId,
                    "topic" to message.topicName,
                    "payload" to Binary(message.payload),
                    "qos" to message.qosLevel,
                    "retained" to message.isRetain,
                    "client_id" to message.clientId,
                    "creation_time" to message.time.toEpochMilli(),
                    "message_expiry_interval" to message.messageExpiryInterval
                ))
                UpdateOneModel<Document>(
                    eq("message_uuid", message.messageUuid),
                    Document("\$setOnInsert", messageDocument),
                    UpdateOptions().upsert(true)
                )
            }

            // Batch client mapping upserts
            val clientOps = messages.flatMap { (message, clientIds) ->
                clientIds.map { clientId ->
                    val clientMessageDocument = Document(mapOf(
                        "client_id" to clientId,
                        "message_uuid" to message.messageUuid,
                        "status" to 0
                    ))
                    UpdateOneModel<Document>(
                        and(
                            eq("client_id", clientId),
                            eq("message_uuid", message.messageUuid)
                        ),
                        Document("\$setOnInsert", clientMessageDocument),
                        UpdateOptions().upsert(true)
                    )
                }
            }

            if (messageOps.isNotEmpty()) {
                queuedMessagesCollection.bulkWrite(messageOps, BulkWriteOptions().ordered(false))
            }
            if (clientOps.isNotEmpty()) {
                queuedMessagesClientsCollection.bulkWrite(clientOps, BulkWriteOptions().ordered(false))
            }
        } catch (e: Exception) {
            logger.warning("Error while enqueuing messages: ${e.message}")
        }
    }

    override fun dequeueMessages(clientId: String, callback: (BrokerMessage) -> Boolean) {
        try {
            val pipeline = listOf(
                Document("\$match", Document("client_id", clientId)),
                Document(
                    "\$lookup", Document(mapOf(
                        "from" to "QueuedMessages",
                        "localField" to "message_uuid",
                        "foreignField" to "message_uuid",
                        "as" to "message"
                    ))
                ),
                Document("\$unwind", "\$message"),
                Document("\$sort", Document("message.message_uuid", 1))
            )

            val currentTimeMillis = System.currentTimeMillis()
            val results = queuedMessagesClientsCollection.aggregate(pipeline)
            for (doc in results) {
                val messageDoc = doc.get("message", Document::class.java)
                val messageUuid = messageDoc.getString("message_uuid")
                val messageId = messageDoc.getInteger("message_id")
                val topic = messageDoc.getString("topic")
                val payload = messageDoc.get("payload", Binary::class.java).data
                val qos = messageDoc.getInteger("qos")
                val retained = messageDoc.getBoolean("retained")
                val clientIdPublisher = messageDoc.getString("client_id")
                val creationTime = messageDoc.getLong("creation_time") ?: currentTimeMillis
                val messageExpiryInterval = messageDoc.getLong("message_expiry_interval")

                // Check if message has expired
                if (messageExpiryInterval != null && messageExpiryInterval >= 0) {
                    val ageSeconds = (currentTimeMillis - creationTime) / 1000
                    if (ageSeconds >= messageExpiryInterval) {
                        // Skip expired message
                        continue
                    }
                }

                val continueProcessing = callback(
                    BrokerMessage(
                        messageUuid = messageUuid,
                        messageId = messageId,
                        topicName = topic,
                        payload = payload,
                        qosLevel = qos,
                        isRetain = retained,
                        isDup = false,
                        isQueued = true,
                        clientId = clientIdPublisher,
                        time = java.time.Instant.ofEpochMilli(creationTime),
                        messageExpiryInterval = messageExpiryInterval
                    )
                )
                if (!continueProcessing) break
            }
        } catch (e: Exception) {
            logger.warning("Error while fetching queued messages: ${e.message}")
        }
    }

    override fun removeMessages(messages: List<Pair<String, String>>) { // clientId, messageUuid
        try {
            val groupedMessages = messages.groupBy({ it.first }, { it.second })
            groupedMessages.forEach { (clientId, messageUuids) ->
                queuedMessagesClientsCollection.deleteMany(
                    and(
                        eq("client_id", clientId),
                        `in`("message_uuid", messageUuids)
                    )
                )
            }
        } catch (e: Exception) {
            logger.warning("Error while removing dequeued messages: ${e.message}")
        }
    }

    override fun fetchNextPendingMessage(clientId: String): BrokerMessage? {
        return fetchPendingMessages(clientId, 1).firstOrNull()
    }

    override fun fetchPendingMessages(clientId: String, limit: Int): List<BrokerMessage> {
        return try {
            val pipeline = listOf(
                Document("\$match", Document(mapOf("client_id" to clientId, "status" to 0))),
                Document(
                    "\$lookup", Document(mapOf(
                        "from" to "queuedmessages",
                        "localField" to "message_uuid",
                        "foreignField" to "message_uuid",
                        "as" to "message"
                    ))
                ),
                Document("\$unwind", "\$message"),
                Document("\$sort", Document("message.message_uuid", 1)),
                Document("\$limit", limit)
            )

            val currentTimeMillis = System.currentTimeMillis()
            val results = queuedMessagesClientsCollection.aggregate(pipeline)
            results.mapNotNull { doc ->
                val messageDoc = doc.get("message", Document::class.java)
                if (messageDoc != null) {
                    val creationTime = messageDoc.getLong("creation_time") ?: currentTimeMillis
                    val messageExpiryInterval = messageDoc.getLong("message_expiry_interval")

                    // Check if message has expired
                    if (messageExpiryInterval != null && messageExpiryInterval >= 0) {
                        val ageSeconds = (currentTimeMillis - creationTime) / 1000
                        if (ageSeconds >= messageExpiryInterval) {
                            // Skip expired message
                            return@mapNotNull null
                        }
                    }

                    BrokerMessage(
                        messageUuid = messageDoc.getString("message_uuid"),
                        messageId = messageDoc.getInteger("message_id"),
                        topicName = messageDoc.getString("topic"),
                        payload = messageDoc.get("payload", Binary::class.java).data,
                        qosLevel = messageDoc.getInteger("qos"),
                        isRetain = messageDoc.getBoolean("retained"),
                        isDup = false,
                        isQueued = true,
                        clientId = messageDoc.getString("client_id"),
                        time = java.time.Instant.ofEpochMilli(creationTime),
                        messageExpiryInterval = messageExpiryInterval
                    )
                } else null
            }.toList()
        } catch (e: Exception) {
            logger.warning("Error fetching pending messages: ${e.message}")
            emptyList()
        }
    }

    override fun fetchAndLockPendingMessages(clientId: String, limit: Int): List<BrokerMessage> {
        return try {
            val currentTimeMillis = System.currentTimeMillis()

            // Step 1: Find pending client mappings (single query)
            val pendingDocs = queuedMessagesClientsCollection.find(
                and(eq("client_id", clientId), eq("status", 0))
            ).sort(Document("_id", 1)).limit(limit).toList()

            if (pendingDocs.isEmpty()) return emptyList()

            val uuids = pendingDocs.map { it.getString("message_uuid") }

            // Step 2: Atomically mark as in-flight with status=0 re-check (single updateMany)
            queuedMessagesClientsCollection.updateMany(
                and(
                    eq("client_id", clientId),
                    `in`("message_uuid", uuids),
                    eq("status", 0)
                ),
                Document("\$set", Document("status", 1))
            )

            // Step 3: Batch-fetch message content (single query)
            val messageDocs = queuedMessagesCollection.find(`in`("message_uuid", uuids))
            val messageMap = mutableMapOf<String, Document>()
            for (doc in messageDocs) {
                messageMap[doc.getString("message_uuid")] = doc
            }

            // Build result in order, filtering expired messages
            val messages = mutableListOf<BrokerMessage>()
            for (uuid in uuids) {
                val messageDoc = messageMap[uuid] ?: continue
                val creationTime = messageDoc.getLong("creation_time") ?: currentTimeMillis
                val messageExpiryInterval = messageDoc.getLong("message_expiry_interval")

                if (messageExpiryInterval != null && messageExpiryInterval >= 0) {
                    val ageSeconds = (currentTimeMillis - creationTime) / 1000
                    if (ageSeconds >= messageExpiryInterval) continue
                }

                messages.add(BrokerMessage(
                    messageUuid = messageDoc.getString("message_uuid"),
                    messageId = messageDoc.getInteger("message_id"),
                    topicName = messageDoc.getString("topic"),
                    payload = messageDoc.get("payload", Binary::class.java).data,
                    qosLevel = messageDoc.getInteger("qos"),
                    isRetain = messageDoc.getBoolean("retained"),
                    isDup = false,
                    isQueued = true,
                    clientId = messageDoc.getString("client_id"),
                    time = java.time.Instant.ofEpochMilli(creationTime),
                    messageExpiryInterval = messageExpiryInterval
                ))
            }

            messages
        } catch (e: Exception) {
            logger.warning("Error in atomic fetch-and-lock [${e.message}] [${Utils.getCurrentFunctionName()}]")
            emptyList()
        }
    }

    override fun markMessageInFlight(clientId: String, messageUuid: String) {
        try {
            queuedMessagesClientsCollection.updateOne(
                and(
                    eq("client_id", clientId),
                    eq("message_uuid", messageUuid),
                    eq("status", 0)
                ),
                Document("\$set", Document("status", 1))
            )
        } catch (e: Exception) {
            logger.warning("Error marking message in-flight: ${e.message}")
        }
    }

    override fun markMessagesInFlight(clientId: String, messageUuids: List<String>) {
        if (messageUuids.isEmpty()) return
        try {
            queuedMessagesClientsCollection.updateMany(
                and(
                    eq("client_id", clientId),
                    `in`("message_uuid", messageUuids),
                    eq("status", 0)
                ),
                Document("\$set", Document("status", 1))
            )
        } catch (e: Exception) {
            logger.warning("Error marking messages in-flight: ${e.message}")
        }
    }

    override fun markMessageDelivered(clientId: String, messageUuid: String) {
        try {
            queuedMessagesClientsCollection.updateOne(
                and(
                    eq("client_id", clientId),
                    eq("message_uuid", messageUuid),
                    eq("status", 1)
                ),
                Document("\$set", Document("status", 2))
            )
        } catch (e: Exception) {
            logger.warning("Error marking message delivered: ${e.message}")
        }
    }

    override fun resetInFlightMessages(clientId: String) {
        try {
            queuedMessagesClientsCollection.updateMany(
                and(
                    eq("client_id", clientId),
                    eq("status", 1)
                ),
                Document("\$set", Document("status", 0))
            )
        } catch (e: Exception) {
            logger.warning("Error resetting in-flight messages: ${e.message}")
        }
    }

    override fun purgeDeliveredMessages(): Int {
        return try {
            val result = queuedMessagesClientsCollection.deleteMany(eq("status", 2))
            result.deletedCount.toInt()
        } catch (e: Exception) {
            logger.warning("Error purging delivered messages: ${e.message}")
            0
        }
    }

    override fun purgeExpiredMessages(): Int {
        val currentTimeMillis = System.currentTimeMillis()

        return try {
            // Find expired messages
            val expiredMessages = queuedMessagesCollection.find(
                and(
                    exists("message_expiry_interval"),
                    exists("creation_time"),
                    Document("\$expr", Document("\$gte", listOf(
                        Document("\$divide", listOf(
                            Document("\$subtract", listOf(currentTimeMillis, "\$creation_time")),
                            1000
                        )),
                        "\$message_expiry_interval"
                    )))
                )
            ).projection(Document("message_uuid", 1))

            val expiredUuids = mutableListOf<String>()
            expiredMessages.forEach { doc ->
                expiredUuids.add(doc.getString("message_uuid"))
            }

            if (expiredUuids.isEmpty()) {
                return 0
            }

            // Delete expired message client mappings
            val result = queuedMessagesClientsCollection.deleteMany(
                `in`("message_uuid", expiredUuids)
            )

            val count = result.deletedCount.toInt()
            if (count > 0) {
                logger.fine { "Purged $count expired messages" }
            }
            count
        } catch (e: Exception) {
            logger.warning("Error purging expired messages: ${e.message}")
            0
        }
    }

    override fun purgeQueuedMessages() {
        val batchSize = 5000
        val delayBetweenBatchesMs = 100L
        var totalDeleted = 0L
        val startTime = System.currentTimeMillis()

        try {
            // Use batched aggregation with $lookup to find orphaned messages
            // This avoids loading all UUIDs into memory at once
            var deleted: Long
            do {
                // Find orphaned message UUIDs in batches using $lookup
                val orphanedUuids = queuedMessagesCollection.aggregate(listOf(
                    Document("\$lookup", Document(mapOf(
                        "from" to "queuedmessagesclients",
                        "localField" to "message_uuid",
                        "foreignField" to "message_uuid",
                        "as" to "clients"
                    ))),
                    Document("\$match", Document("clients", Document("\$size", 0))),
                    Document("\$limit", batchSize),
                    Document("\$project", Document("message_uuid", 1))
                )).map { it.getString("message_uuid") }.toList()

                if (orphanedUuids.isNotEmpty()) {
                    val result = queuedMessagesCollection.deleteMany(`in`("message_uuid", orphanedUuids))
                    deleted = result.deletedCount
                    totalDeleted += deleted

                    logger.fine { "Purge batch: deleted $deleted orphaned messages (total: $totalDeleted)" }

                    if (deleted >= batchSize) {
                        Thread.sleep(delayBetweenBatchesMs)
                    }
                } else {
                    deleted = 0
                }
            } while (deleted >= batchSize)

            val duration = (System.currentTimeMillis() - startTime) / 1000.0
            if (totalDeleted > 0) {
                logger.info("Purging queued messages finished: deleted $totalDeleted in $duration seconds")
            } else {
                logger.fine { "Purging queued messages finished: no orphaned messages found in $duration seconds" }
            }
        } catch (e: Exception) {
            logger.warning("Error while purging queued messages: ${e.message}")
        }
    }

    override fun deleteClientMessages(clientId: String) {
        try {
            queuedMessagesClientsCollection.deleteMany(eq("client_id", clientId))
        } catch (e: Exception) {
            logger.warning("Error while deleting client messages: ${e.message}")
        }
    }

    override fun countQueuedMessages(): Long {
        return try {
            queuedMessagesCollection.countDocuments()
        } catch (e: Exception) {
            logger.warning("Error counting queued messages: ${e.message}")
            0L
        }
    }

    override fun countQueuedMessagesForClient(clientId: String): Long {
        return try {
            queuedMessagesClientsCollection.countDocuments(and(eq("client_id", clientId), lt("status", 2)))
        } catch (e: Exception) {
            logger.warning("Error counting queued messages for client $clientId: ${e.message}")
            0L
        }
    }
}
