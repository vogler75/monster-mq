package at.rocworks.stores.mongodb

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.data.MqttSubscription
import at.rocworks.stores.ISessionStoreSync
import at.rocworks.stores.SessionStoreType
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Filters.*
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import org.bson.Document
import org.bson.types.Binary
import java.util.concurrent.Callable

class SessionStoreMongoDB(
    private val connectionString: String,
    private val databaseName: String
) : AbstractVerticle(), ISessionStoreSync {
    private val logger = Utils.getLogger(this::class.java)
    private lateinit var mongoClient: MongoClient
    private lateinit var database: MongoDatabase

    private lateinit var sessionsCollection: MongoCollection<Document>
    private lateinit var subscriptionsCollection: MongoCollection<Document>


    override fun getType(): SessionStoreType = SessionStoreType.MONGODB

    override fun start(startPromise: Promise<Void>) {
        try {
            mongoClient = MongoClientPool.getClient(connectionString)
            database = mongoClient.getDatabase(databaseName)

            // Initialize collections
            sessionsCollection = database.getCollection("sessions")
            subscriptionsCollection = database.getCollection("subscriptions")

            // Create indexes in background to avoid blocking the event loop
            vertx.executeBlocking(Callable {
                try {
                    sessionsCollection.createIndex(Document("client_id", 1))
                    subscriptionsCollection.createIndex(Document("client_id", 1).append("topic", 1))
                    logger.info("MongoDB session store indexes created successfully")
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

    override fun iterateSubscriptions(callback: (topic: String, clientId: String, qos: Int, noLocal: Boolean, retainHandling: Int, retainAsPublished: Boolean) -> Unit) {
        try {
            val subscriptions = subscriptionsCollection.find()
            for (doc in subscriptions) {
                val clientId = doc.getString("client_id")
                val topic = doc.getString("topic")
                val qos = doc.getInteger("qos")
                val noLocal = doc.getBoolean("no_local", false)
                val retainHandling = doc.getInteger("retain_handling", 0)
                val retainAsPublished = doc.getBoolean("retain_as_published", false)
                callback(topic, clientId, qos, noLocal, retainHandling, retainAsPublished)
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
                    "retain_handling" to subscription.retainHandling,
                    "retain_as_published" to subscription.retainAsPublished
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
        } catch (e: Exception) {
            logger.warning("Error while deleting client: ${e.message}")
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
        MongoClientPool.releaseClient(connectionString)
        logger.info("MongoDB connection released.")
    }
}
