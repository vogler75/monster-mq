package at.rocworks.stores.mongodb

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.data.MqttSubscription
import at.rocworks.stores.ISessionStoreSync
import at.rocworks.stores.SessionStoreType
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters.*
import com.mongodb.client.model.UpdateOptions
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import org.bson.Document
import org.bson.types.Binary

class SessionStoreMongoDB(
    private val connectionString: String,
    private val databaseName: String
) : AbstractVerticle(), ISessionStoreSync {
    private val logger = Utils.getLogger(this::class.java)
    private lateinit var mongoClient: MongoClient
    private lateinit var database: MongoDatabase

    private lateinit var sessionsCollection: MongoCollection<Document>
    private lateinit var subscriptionsCollection: MongoCollection<Document>
    private lateinit var queuedMessagesCollection: MongoCollection<Document>
    private lateinit var queuedMessagesClientsCollection: MongoCollection<Document>

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun getType(): SessionStoreType = SessionStoreType.MONGODB

    override fun start(startPromise: Promise<Void>) {
        try {
            mongoClient = MongoClients.create(connectionString)
            database = mongoClient.getDatabase(databaseName)

            // Initialize collections
            sessionsCollection = database.getCollection("sessions")
            subscriptionsCollection = database.getCollection("subscriptions")
            queuedMessagesCollection = database.getCollection("queuedmessages")
            queuedMessagesClientsCollection = database.getCollection("queuedmessagesclients")

            // Create indexes for faster queries
            sessionsCollection.createIndex(Document("client_id", 1))
            subscriptionsCollection.createIndex(Document("client_id", 1).append("topic", 1))
            queuedMessagesCollection.createIndex(Document("client_id", 1))
            queuedMessagesCollection.createIndex(Document("message_uuid", 1))  // For $lookup joins
            queuedMessagesClientsCollection.createIndex(Document("client_id", 1))
            queuedMessagesClientsCollection.createIndex(Document("client_id", 1).append("status", 1))

            logger.fine("MongoDB connection established successfully.")
            startPromise.complete()
        } catch (e: Exception) {
            logger.severe("Error while starting MongoDB connection: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun iterateSubscriptions(callback: (topic: String, clientId: String, qos: Int, noLocal: Boolean, retainHandling: Int) -> Unit) {
        try {
            val subscriptions = subscriptionsCollection.find()
            for (doc in subscriptions) {
                val clientId = doc.getString("client_id")
                val topic = doc.getString("topic")
                val qos = doc.getInteger("qos")
                val noLocal = doc.getBoolean("no_local", false)
                val retainHandling = doc.getInteger("retain_handling", 0)
                callback(topic, clientId, qos, noLocal, retainHandling)
            }
        } catch (e: Exception) {
            logger.warning("Error while retrieving subscriptions: ${e.message}")
        }
    }

    override fun iterateOfflineClients(callback: (clientId: String) -> Unit) {
        try {
            val clients = sessionsCollection.find(and(eq("connected", false), eq("clean_session", false)))
            for (doc in clients) {
                callback(doc.getString("client_id"))
            }
        } catch (e: Exception) {
            logger.warning("Error while retrieving offline clients: ${e.message}")
        }
    }

    override fun iterateConnectedClients(callback: (clientId: String, nodeId: String) -> Unit) {
        try {
            val filter = Document("connected", true)
            val sessions = sessionsCollection.find(filter)
            for (doc in sessions) {
                val clientId = doc.getString("client_id")
                val nodeId = doc.getString("node_id") ?: ""
                callback(clientId, nodeId)
            }
        } catch (e: Exception) {
            logger.warning("Error while retrieving connected clients: ${e.message}")
        }
    }

    override fun iterateAllSessions(callback: (clientId: String, nodeId: String, connected: Boolean, cleanSession: Boolean) -> Unit) {
        try {
            val sessions = sessionsCollection.find()
            for (doc in sessions) {
                val clientId = doc.getString("client_id")
                val nodeId = doc.getString("node_id") ?: ""
                val connected = doc.getBoolean("connected") ?: false
                val cleanSession = doc.getBoolean("clean_session") ?: true
                callback(clientId, nodeId, connected, cleanSession)
            }
        } catch (e: Exception) {
            logger.warning("Error while retrieving all sessions: ${e.message}")
        }
    }

    override fun iterateNodeClients(nodeId: String, callback: (clientId: String, cleanSession: Boolean, lastWill: BrokerMessage) -> Unit) {
        try {
            val clients = sessionsCollection.find(eq("node_id", nodeId))
            for (doc in clients) {
                val clientId = doc.getString("client_id")
                val cleanSession = doc.getBoolean("clean_session")
                // create the lastWill message like in file sessionStorePostgres
                val lastWill = if (doc.containsKey("last_will")) {
                    val lastWillDoc = doc.get("last_will", Document::class.java)
                    val topic = lastWillDoc.getString("topic")
                    val payload = lastWillDoc.get("payload", Binary::class.java).data
                    val qos = lastWillDoc.getInteger("qos")
                    val retain = lastWillDoc.getBoolean("retain")
                    BrokerMessage(
                        messageId = 0,
                        topicName = topic,
                        payload = payload,
                        qosLevel = qos,
                        isRetain = retain,
                        isDup = false,
                        isQueued = false,
                        clientId = clientId
                    )
                } else {
                    BrokerMessage(
                        messageId = 0,
                        topicName = "",
                        payload = ByteArray(0),
                        qosLevel = 0,
                        isRetain = false,
                        isDup = false,
                        isQueued = false,
                        clientId = clientId
                    )
                }
                callback(clientId, cleanSession, lastWill)
            }
        } catch (e: Exception) {
            logger.warning("Error while retrieving node clients: ${e.message}")
        }
    }

    override fun setClient(clientId: String, nodeId: String, cleanSession: Boolean, connected: Boolean, information: JsonObject) {
        try {
            val update = Document("\$set", Document(mapOf(
                "node_id" to nodeId,
                "clean_session" to cleanSession,
                "connected" to connected,
                "information" to Document.parse(information.encode()),
                "update_time" to System.currentTimeMillis()
            )))
            sessionsCollection.updateOne(eq("client_id", clientId), update, UpdateOptions().upsert(true))
        } catch (e: Exception) {
            logger.warning("Error while setting client: ${e.message}")
        }
    }

    override fun setConnected(clientId: String, connected: Boolean) {
        try {
            val update = Document("\$set", Document(mapOf(
                "connected" to connected,
                "update_time" to System.currentTimeMillis()
            )))
            sessionsCollection.updateOne(eq("client_id", clientId), update)
        } catch (e: Exception) {
            logger.warning("Error while updating client: ${e.message}")
        }
    }

    override fun isConnected(clientId: String): Boolean {
        try {
            val document = sessionsCollection.find(eq("client_id", clientId)).first()
            document?.let {
                return it.getBoolean("connected")
            }
        } catch (e: Exception) {
            logger.warning("Error while fetching client: ${e.message}")
        }
        return false
    }

    override fun isPresent(clientId: String): Boolean {
        try {
            val document = sessionsCollection.find(eq("client_id", clientId)).first()
            return document != null
        } catch (e: Exception) {
            logger.warning("Error while fetching client: ${e.message}")
            return false
        }
    }

    override fun setLastWill(clientId: String, message: BrokerMessage?) {
        try {
            val update = if (message != null) {
                Document("\$set", Document(mapOf(
                    "last_will.topic" to message.topicName,
                    "last_will.payload" to Binary(message.payload),
                    "last_will.qos" to message.qosLevel,
                    "last_will.retain" to message.isRetain
                )))
            } else {
                Document("\$unset", Document(mapOf(
                    "last_will" to ""
                )))
            }
            sessionsCollection.updateOne(eq("client_id", clientId), update, UpdateOptions().upsert(true))
        } catch (e: Exception) {
            logger.warning("Error while setting last will: ${e.message}")
        }
    }

    override fun addSubscriptions(subscriptions: List<MqttSubscription>) {
        try {
            subscriptions.forEach { subscription ->
                val update = Document("\$set", Document(mapOf(
                    "qos" to subscription.qos.value(),
                    "wildcard" to Utils.isWildCardTopic(subscription.topicName),
                    "no_local" to subscription.noLocal,
                    "retain_handling" to subscription.retainHandling
                )))
                subscriptionsCollection.updateOne(
                    and(
                        eq("client_id", subscription.clientId),
                        eq("topic", subscription.topicName)
                    ),
                    update,
                    UpdateOptions().upsert(true)
                )
            }
        } catch (e: Exception) {
            logger.warning("Error while adding subscriptions: ${e.message}")
        }
    }

    override fun delSubscriptions(subscriptions: List<MqttSubscription>) {
        try {
            subscriptions.forEach { subscription ->
                subscriptionsCollection.deleteOne(
                    and(
                        eq("client_id", subscription.clientId),
                        eq("topic", subscription.topicName)
                    )
                )
            }
        } catch (e: Exception) {
            logger.warning("Error while deleting subscriptions: ${e.message}")
        }
    }


    override fun delClient(clientId: String, callback: (MqttSubscription) -> Unit) {
        try {
            val subscriptions = subscriptionsCollection.find(eq("client_id", clientId))
            for (doc in subscriptions) {
                val topic = doc.getString("topic")
                val qos = MqttQoS.valueOf(doc.getInteger("qos"))
                callback(MqttSubscription(clientId, topic, qos))
            }
            subscriptionsCollection.deleteMany(eq("client_id", clientId))
            sessionsCollection.deleteOne(eq("client_id", clientId))
            queuedMessagesClientsCollection.deleteMany(eq("client_id", clientId))
        } catch (e: Exception) {
            logger.warning("Error while deleting client: ${e.message}")
        }
    }

    override fun enqueueMessages(messages: List<Pair<BrokerMessage, List<String>>>) {
        try {
            messages.forEach { (message, clientIds) ->
                val messageDocument = Document(mapOf(
                    "message_uuid" to message.messageUuid,
                    "message_id" to message.messageId,
                    "topic" to message.topicName,
                    "payload" to Binary(message.payload),
                    "qos" to message.qosLevel,
                    "retained" to message.isRetain,
                    "client_id" to message.clientId
                ))
                queuedMessagesCollection.updateOne(
                    eq("message_uuid", message.messageUuid),
                    Document("\$setOnInsert", messageDocument),
                    UpdateOptions().upsert(true)
                )

                clientIds.forEach { clientId ->
                    val clientMessageDocument = Document(mapOf(
                        "client_id" to clientId,
                        "message_uuid" to message.messageUuid,
                        "status" to 0
                    ))
                    queuedMessagesClientsCollection.updateOne(
                        and(
                            eq("client_id", clientId),
                            eq("message_uuid", message.messageUuid)
                        ),
                        Document("\$setOnInsert", clientMessageDocument),
                        UpdateOptions().upsert(true)
                    )
                }
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
                        clientId = clientIdPublisher
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

            val results = queuedMessagesClientsCollection.aggregate(pipeline)
            results.mapNotNull { doc ->
                val messageDoc = doc.get("message", Document::class.java)
                if (messageDoc != null) {
                    BrokerMessage(
                        messageUuid = messageDoc.getString("message_uuid"),
                        messageId = messageDoc.getInteger("message_id"),
                        topicName = messageDoc.getString("topic"),
                        payload = messageDoc.get("payload", Binary::class.java).data,
                        qosLevel = messageDoc.getInteger("qos"),
                        isRetain = messageDoc.getBoolean("retained"),
                        isDup = false,
                        isQueued = true,
                        clientId = messageDoc.getString("client_id")
                    )
                } else null
            }.toList()
        } catch (e: Exception) {
            logger.warning("Error fetching pending messages: ${e.message}")
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
                    eq("message_uuid", messageUuid)
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
                    gte("\$expr", Document("\$gte", listOf(
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

    override fun purgeSessions() {
        try {
            val startTime = System.currentTimeMillis()

            // Delete clean sessions
            sessionsCollection.deleteMany(eq("clean_session", true))

            // Delete orphaned subscriptions using $lookup aggregation
            // This avoids loading all client_ids into memory
            val orphanedClientIds = subscriptionsCollection.aggregate(listOf(
                Document("\$lookup", Document(mapOf(
                    "from" to "sessions",
                    "localField" to "client_id",
                    "foreignField" to "client_id",
                    "as" to "session"
                ))),
                Document("\$match", Document("session", Document("\$size", 0))),
                Document("\$project", Document("client_id", 1))
            )).map { it.getString("client_id") }.toList()

            if (orphanedClientIds.isNotEmpty()) {
                subscriptionsCollection.deleteMany(`in`("client_id", orphanedClientIds))
            }

            // Mark all sessions as disconnected
            sessionsCollection.updateMany(
                Document(),
                Document("\$set", Document("connected", false))
            )

            val duration = (System.currentTimeMillis() - startTime) / 1000.0
            logger.fine { "Purging sessions finished in $duration seconds" }
        } catch (e: Exception) {
            logger.warning("Error while purging sessions: ${e.message}")
        }
    }

    override fun stop() {
        mongoClient.close()
        logger.info("MongoDB connection closed.")
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