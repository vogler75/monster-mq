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
            queuedMessagesClientsCollection.createIndex(Document("client_id", 1))

            logger.info("MongoDB connection established successfully.")
            startPromise.complete()
        } catch (e: Exception) {
            logger.severe("Error while starting MongoDB connection: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun iterateSubscriptions(callback: (topic: String, clientId: String, qos: Int) -> Unit) {
        try {
            val subscriptions = subscriptionsCollection.find()
            for (doc in subscriptions) {
                val clientId = doc.getString("client_id")
                val topic = doc.getString("topic")
                val qos = doc.getInteger("qos")
                callback(topic, clientId, qos)
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
                    "wildcard" to Utils.isWildCardTopic(subscription.topicName)
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
                        "message_uuid" to message.messageUuid
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

    override fun purgeQueuedMessages() {
        try {
            val startTime = System.currentTimeMillis()
            val unusedMessagesFilter = Document("\$expr", Document(
                "\$not", Document(
                    "\$in", listOf("\$message_uuid",
                        queuedMessagesClientsCollection.distinct("message_uuid", String::class.java).into(mutableListOf())
                    )
                )
            ))
            val deleteResult = queuedMessagesCollection.deleteMany(unusedMessagesFilter)
            val endTime = System.currentTimeMillis()
            val duration = (endTime - startTime) / 1000.0
            logger.info("Purging queued messages finished in $duration seconds. Deleted ${deleteResult.deletedCount} messages.")
        } catch (e: Exception) {
            logger.warning("Error while purging queued messages: ${e.message}")
        }
    }

    override fun purgeSessions() {
        try {
            val startTime = System.currentTimeMillis()
            sessionsCollection.deleteMany(eq("clean_session", true))
            subscriptionsCollection.deleteMany(
                Document("\$expr", Document(
                    "\$not", Document(
                        "\$in", listOf("\$client_id", sessionsCollection.distinct("client_id", String::class.java).into(mutableListOf()))
                    )
                ))
            )

            sessionsCollection.updateMany(
                Document(),
                Document("\$set", Document("connected", false))
            )

            val endTime = System.currentTimeMillis()
            val duration = (endTime - startTime) / 1000.0
            logger.info("Purging sessions finished in $duration seconds.")
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
            queuedMessagesClientsCollection.countDocuments(eq("client_id", clientId))
        } catch (e: Exception) {
            logger.warning("Error counting queued messages for client $clientId: ${e.message}")
            0L
        }
    }
}