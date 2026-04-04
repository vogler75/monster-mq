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
import java.util.Date
import java.util.concurrent.Callable

/**
 * Single-collection queue store with visibility timeout (PGMQ-inspired).
 *
 * Key differences from V1:
 * - Single collection (messagequeue) — payload duplicated per subscriber
 * - Visibility timeout (vt) replaces integer status
 * - ACK = DELETE (no mark-delivered + purge cycle)
 * - No orphan cleanup needed
 * - Uses auto-incrementing sequence for FIFO ordering
 */
class QueueStoreMongoDBV2(
    private val connectionString: String,
    private val databaseName: String,
    private val visibilityTimeoutSeconds: Int = 30
) : AbstractVerticle(), IQueueStoreSync {
    private val logger = Utils.getLogger(this::class.java)
    private lateinit var mongoClient: MongoClient
    private lateinit var database: MongoDatabase
    private lateinit var collection: MongoCollection<Document>
    private lateinit var countersCollection: MongoCollection<Document>

    private val collectionName = "messagequeue"


    override fun getType(): QueueStoreType = QueueStoreType.MONGODB_V2

    override fun start(startPromise: Promise<Void>) {
        try {
            mongoClient = MongoClientPool.getClient(connectionString)
            database = mongoClient.getDatabase(databaseName)
            collection = database.getCollection(collectionName)
            countersCollection = database.getCollection("counters")

            vertx.executeBlocking(Callable {
                try {
                    collection.createIndex(
                        Document("client_id", 1).append("vt", 1),
                        IndexOptions().name("${collectionName}_fetch_idx")
                    )
                    collection.createIndex(
                        Document("client_id", 1).append("message_uuid", 1),
                        IndexOptions().name("${collectionName}_client_uuid_idx")
                    )
                    logger.info("MongoDB V2 queue store indexes created successfully")
                } catch (e: Exception) {
                    logger.warning("Failed to create indexes: ${e.message}")
                }
                null
            })

            logger.fine("MongoDB V2 queue store initialized.")
            startPromise.complete()
        } catch (e: Exception) {
            logger.severe("Error while starting MongoDB V2 queue store: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun stop() {
        MongoClientPool.releaseClient(connectionString)
        logger.info("MongoDB connection released.")
    }

    private fun nextMsgId(): Long {
        val result = countersCollection.findOneAndUpdate(
            eq("_id", collectionName),
            Document("\$inc", Document("seq", 1L)),
            FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER)
        )
        return result?.getLong("seq") ?: 1L
    }

    // -------------------------------------------------------------------------
    // Enqueue: one document per (message, client) — payload duplicated
    // -------------------------------------------------------------------------

    override fun enqueueMessages(messages: List<Pair<BrokerMessage, List<String>>>) {
        try {
            val docs = mutableListOf<Document>()
            messages.forEach { (message, clientIds) ->
                clientIds.forEach { clientId ->
                    docs.add(Document(mapOf(
                        "msg_id" to nextMsgId(),
                        "message_uuid" to message.messageUuid,
                        "client_id" to clientId,
                        "topic" to message.topicName,
                        "payload" to Binary(message.payload),
                        "qos" to message.qosLevel,
                        "retained" to message.isRetain,
                        "publisher_id" to message.clientId,
                        "creation_time" to message.time.toEpochMilli(),
                        "message_expiry_interval" to message.messageExpiryInterval,
                        "vt" to Date(),
                        "read_ct" to 0
                    )))
                }
            }
            if (docs.isNotEmpty()) {
                collection.insertMany(docs)
            }
        } catch (e: Exception) {
            logger.warning("Error enqueuing messages: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
    }

    // -------------------------------------------------------------------------
    // Dequeue: iterate visible messages for a client
    // -------------------------------------------------------------------------

    override fun dequeueMessages(clientId: String, callback: (BrokerMessage) -> Boolean) {
        try {
            val now = Date()
            val currentTimeMillis = now.time
            val cursor = collection.find(
                and(
                    eq("client_id", clientId),
                    lte("vt", now)
                )
            ).sort(Document("msg_id", 1))

            for (doc in cursor) {
                if (isExpired(doc, currentTimeMillis)) continue
                val continueProcessing = callback(documentToBrokerMessage(doc))
                if (!continueProcessing) break
            }
        } catch (e: Exception) {
            logger.warning("Error dequeuing messages: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
    }

    // -------------------------------------------------------------------------
    // Remove messages
    // -------------------------------------------------------------------------

    override fun removeMessages(messages: List<Pair<String, String>>) {
        try {
            val ops = messages.map { (clientId, messageUuid) ->
                DeleteOneModel<Document>(
                    and(eq("client_id", clientId), eq("message_uuid", messageUuid))
                )
            }
            if (ops.isNotEmpty()) {
                collection.bulkWrite(ops, BulkWriteOptions().ordered(false))
            }
        } catch (e: Exception) {
            logger.warning("Error removing messages: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
    }

    // -------------------------------------------------------------------------
    // Fetch pending messages
    // -------------------------------------------------------------------------

    override fun fetchNextPendingMessage(clientId: String): BrokerMessage? {
        return fetchPendingMessages(clientId, 1).firstOrNull()
    }

    override fun fetchPendingMessages(clientId: String, limit: Int): List<BrokerMessage> {
        return try {
            val now = Date()
            val currentTimeMillis = now.time
            val results = collection.find(
                and(
                    eq("client_id", clientId),
                    lte("vt", now)
                )
            ).sort(Document("msg_id", 1)).limit(limit)

            results.mapNotNull { doc ->
                if (isExpired(doc, currentTimeMillis)) null
                else documentToBrokerMessage(doc)
            }.toList()
        } catch (e: Exception) {
            logger.warning("Error fetching pending messages: ${e.message} [${Utils.getCurrentFunctionName()}]")
            emptyList()
        }
    }

    // -------------------------------------------------------------------------
    // Atomic fetch-and-lock: find visible messages, then set vt into the future
    // -------------------------------------------------------------------------

    override fun fetchAndLockPendingMessages(clientId: String, limit: Int): List<BrokerMessage> {
        return try {
            val now = Date()
            val currentTimeMillis = now.time
            val vtFuture = Date(currentTimeMillis + visibilityTimeoutSeconds * 1000L)

            // Find visible pending messages
            val docs = collection.find(
                and(
                    eq("client_id", clientId),
                    lte("vt", now)
                )
            ).sort(Document("msg_id", 1)).limit(limit).toList()

            if (docs.isEmpty()) return emptyList()

            // Filter expired and collect IDs
            val validDocs = docs.filter { !isExpired(it, currentTimeMillis) }
            if (validDocs.isEmpty()) return emptyList()

            val docIds = validDocs.map { it.getObjectId("_id") }

            // Atomically set vt into the future
            collection.updateMany(
                `in`("_id", docIds),
                Document("\$set", Document("vt", vtFuture))
                    .append("\$inc", Document("read_ct", 1))
            )

            validDocs.map { documentToBrokerMessage(it) }
        } catch (e: Exception) {
            logger.warning("Error in atomic fetch-and-lock: ${e.message} [${Utils.getCurrentFunctionName()}]")
            emptyList()
        }
    }

    // -------------------------------------------------------------------------
    // Mark in-flight: extend visibility timeout
    // -------------------------------------------------------------------------

    override fun markMessageInFlight(clientId: String, messageUuid: String) {
        try {
            val now = Date()
            val vtFuture = Date(now.time + visibilityTimeoutSeconds * 1000L)
            collection.updateOne(
                and(
                    eq("client_id", clientId),
                    eq("message_uuid", messageUuid),
                    lte("vt", now)
                ),
                Document("\$set", Document("vt", vtFuture))
                    .append("\$inc", Document("read_ct", 1))
            )
        } catch (e: Exception) {
            logger.warning("Error marking message in-flight: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
    }

    override fun markMessagesInFlight(clientId: String, messageUuids: List<String>) {
        if (messageUuids.isEmpty()) return
        try {
            val now = Date()
            val vtFuture = Date(now.time + visibilityTimeoutSeconds * 1000L)
            collection.updateMany(
                and(
                    eq("client_id", clientId),
                    `in`("message_uuid", messageUuids),
                    lte("vt", now)
                ),
                Document("\$set", Document("vt", vtFuture))
                    .append("\$inc", Document("read_ct", 1))
            )
        } catch (e: Exception) {
            logger.warning("Error marking messages in-flight: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
    }

    // -------------------------------------------------------------------------
    // ACK = DELETE
    // -------------------------------------------------------------------------

    override fun markMessageDelivered(clientId: String, messageUuid: String) {
        try {
            collection.deleteOne(
                and(eq("client_id", clientId), eq("message_uuid", messageUuid))
            )
        } catch (e: Exception) {
            logger.warning("Error deleting delivered message: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
    }

    // -------------------------------------------------------------------------
    // Reset: make in-flight messages visible again
    // -------------------------------------------------------------------------

    override fun resetInFlightMessages(clientId: String) {
        try {
            val now = Date()
            collection.updateMany(
                and(
                    eq("client_id", clientId),
                    gt("vt", now)
                ),
                Document("\$set", Document("vt", now).append("read_ct", 0))
            )
        } catch (e: Exception) {
            logger.warning("Error resetting in-flight messages: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
    }

    // -------------------------------------------------------------------------
    // Purge: no-ops in V2 (ACK = DELETE, no orphans)
    // -------------------------------------------------------------------------

    override fun purgeDeliveredMessages(): Int = 0

    override fun purgeExpiredMessages(): Int {
        val currentTimeMillis = System.currentTimeMillis()
        return try {
            val result = collection.deleteMany(
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
            )
            val count = result.deletedCount.toInt()
            if (count > 0) logger.fine { "Purged $count expired messages" }
            count
        } catch (e: Exception) {
            logger.warning("Error purging expired messages: ${e.message} [${Utils.getCurrentFunctionName()}]")
            0
        }
    }

    override fun purgeQueuedMessages() {
        // No-op: single-collection design has no orphaned messages
    }

    // -------------------------------------------------------------------------
    // Delete all messages for a client
    // -------------------------------------------------------------------------

    override fun deleteClientMessages(clientId: String) {
        try {
            collection.deleteMany(eq("client_id", clientId))
        } catch (e: Exception) {
            logger.warning("Error deleting client messages: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }
    }

    // -------------------------------------------------------------------------
    // Count
    // -------------------------------------------------------------------------

    override fun countQueuedMessages(): Long {
        return try {
            collection.countDocuments()
        } catch (e: Exception) {
            logger.warning("Error counting queued messages: ${e.message} [${Utils.getCurrentFunctionName()}]")
            0L
        }
    }

    override fun countQueuedMessagesForClient(clientId: String): Long {
        return try {
            collection.countDocuments(
                and(eq("client_id", clientId), lte("vt", Date()))
            )
        } catch (e: Exception) {
            logger.warning("Error counting queued messages for client $clientId: ${e.message} [${Utils.getCurrentFunctionName()}]")
            0L
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private fun isExpired(doc: Document, currentTimeMillis: Long): Boolean {
        val expiryInterval = doc.getLong("message_expiry_interval") ?: return false
        val creationTime = doc.getLong("creation_time") ?: return false
        val ageSeconds = (currentTimeMillis - creationTime) / 1000
        return ageSeconds >= expiryInterval
    }

    private fun documentToBrokerMessage(doc: Document): BrokerMessage {
        val currentTimeMillis = System.currentTimeMillis()
        val creationTime = doc.getLong("creation_time") ?: currentTimeMillis
        return BrokerMessage(
            messageUuid = doc.getString("message_uuid"),
            messageId = 0,
            topicName = doc.getString("topic"),
            payload = doc.get("payload", Binary::class.java).data,
            qosLevel = doc.getInteger("qos"),
            isRetain = doc.getBoolean("retained"),
            isDup = false,
            isQueued = true,
            clientId = doc.getString("publisher_id"),
            time = java.time.Instant.ofEpochMilli(creationTime),
            messageExpiryInterval = doc.getLong("message_expiry_interval")
        )
    }
}
