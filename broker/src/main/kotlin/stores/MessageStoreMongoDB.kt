package at.rocworks.stores

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters.*
import com.mongodb.client.model.IndexOptions
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import org.bson.Document
import java.time.Instant

class MessageStoreMongoDB(
    private val name: String,
    private val connectionString: String,
    private val databaseName: String
) : AbstractVerticle(), IMessageStore {

    private val logger = Utils.getLogger(this::class.java, name)
    private val tableName = name
    private lateinit var mongoClient: MongoClient
    private lateinit var database: com.mongodb.client.MongoDatabase
    private lateinit var collection: MongoCollection<Document>
    private var lastAddAllError: Int = 0
    private var lastGetError: Int = 0
    private var lastDelAllError: Int = 0
    private var lastFetchError: Int = 0

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun getName(): String = name
    override fun getType(): MessageStoreType = MessageStoreType.MONGODB

    override fun start(startPromise: Promise<Void>) {
        try {
            mongoClient = MongoClients.create(connectionString)
            database = mongoClient.getDatabase(databaseName)

            // Check if the collection exists, if not create it
            if (!database.listCollectionNames().contains(tableName)) {
                logger.info("Collection [$tableName] will be created in MongoDB.")
                database.createCollection(tableName)
                collection = database.getCollection(tableName)
                collection.createIndex(Document("topic", 1), IndexOptions().unique(true))
                collection.createIndex(Document("topic_levels", 1))
            } else {
                logger.info("Collection [$tableName] already exists in MongoDB.")
                collection = database.getCollection(tableName)
            }

            logger.info("Message store [$name] is ready [${Utils.getCurrentFunctionName()}]")
            startPromise.complete()
        } catch (e: Exception) {
            logger.severe("Error in starting MongoDB connection: ${e.message} [${Utils.getCurrentFunctionName()}]")
            startPromise.fail(e)
        }
    }

    override fun get(topicName: String): MqttMessage? {
        try {
            val document = collection.find(eq("topic", topicName)).first()
            document?.let {
                val payload = it.get("payload", ByteArray::class.java)
                val qos = it.getInteger("qos")
                val retained = it.getBoolean("retained")
                val clientId = it.getString("client_id")
                val messageUuid = it.getString("message_uuid")

                if (lastGetError != 0) {
                    logger.info("Read successful after error [${Utils.getCurrentFunctionName()}]")
                    lastGetError = 0
                }

                return MqttMessage(
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
            }
        } catch (e: Exception) {
            if (lastGetError != e.hashCode()) {
                logger.warning("Error fetching data for topic [$topicName]: ${e.message} [${Utils.getCurrentFunctionName()}]")
                lastGetError = e.hashCode()
            }
        }
        return null
    }

    override fun addAll(messages: List<MqttMessage>) {
        try {
            val bulkOperations = messages.map { message ->
                val filter = Document("topic", message.topicName)
                val json = message.getPayloadAsJson()
                val doc = if (json != null)
                    mapOf(
                        "topic_levels" to Utils.getTopicLevels(message.topicName),
                        "time" to Instant.ofEpochMilli(message.time.toEpochMilli()),
                        "payload" to message.getPayloadAsJson(),
                        "qos" to message.qosLevel,
                        "retained" to message.isRetain,
                        "client_id" to message.clientId,
                        "message_uuid" to message.messageUuid
                    )
                else
                    mapOf(
                        "topic_levels" to Utils.getTopicLevels(message.topicName),
                        "time" to Instant.ofEpochMilli(message.time.toEpochMilli()),
                        "payload" to message.payload,
                        "qos" to message.qosLevel,
                        "retained" to message.isRetain,
                        "client_id" to message.clientId,
                        "message_uuid" to message.messageUuid
                    )
                val update = Document("\$set", doc)
                com.mongodb.client.model.UpdateOneModel<Document>(
                    filter,
                    update,
                    com.mongodb.client.model.UpdateOptions().upsert(true)
                )
            }
            collection.bulkWrite(bulkOperations)

            if (lastAddAllError != 0) {
                logger.info("Batch insert successful after error [${Utils.getCurrentFunctionName()}]")
                lastAddAllError = 0
            }

        } catch (e: Exception) {
            if (lastAddAllError != e.hashCode()) {
                logger.warning("Error inserting batch data: ${e.message} [${Utils.getCurrentFunctionName()}]")
                lastAddAllError = e.hashCode()
            }
        }
    }

    override fun delAll(topics: List<String>) {
        try {
            val deleteFilters = topics.map { eq("topic", it) }
            if (deleteFilters.isNotEmpty()) {
                val combinedFilter = or(deleteFilters)
                collection.deleteMany(combinedFilter)
            }

            if (lastDelAllError != 0) {
                logger.info("Batch delete successful after error [${Utils.getCurrentFunctionName()}]")
                lastDelAllError = 0
            }

        } catch (e: Exception) {
            if (lastDelAllError != e.hashCode()) {
                logger.warning("Error deleting batch data: ${e.message} [${Utils.getCurrentFunctionName()}]")
                lastDelAllError = e.hashCode()
            }
        }
    }

    override fun findMatchingMessages(topicName: String, callback: (MqttMessage) -> Boolean) {
        try {
            val levels = Utils.getTopicLevels(topicName)
            val filters = levels.map { if (it == "+" || it == "#") null else it }.mapIndexed { index, value ->
                if (value != null) eq("topic_levels.$index", value)
                else null
            }.filterNotNull()
            val filter = if (filters.isNotEmpty()) and(filters) else Document()
            collection.find(filter).forEach { document ->
                val topic = document.getString("topic")
                val payload = when (val rawPayload = document["payload"]) {
                    is org.bson.types.Binary -> rawPayload.data
                    is String -> rawPayload.toByteArray()
                    else -> logger.severe("Unknown payload type: ${rawPayload?.javaClass}").let { "".toByteArray() }
                }
                val qos = document.getInteger("qos")
                val clientId = document.getString("client_id")
                val messageUuid = document.getString("message_uuid")
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

            if (lastFetchError != 0) {
                logger.info("Read successful after error [${Utils.getCurrentFunctionName()}]")
                lastFetchError = 0
            }

        } catch (e: Exception) {
            if (lastFetchError != e.hashCode()) {
                logger.warning("Error finding data for topic [$topicName]: ${e.message} [${Utils.getCurrentFunctionName()}]")
                lastFetchError = e.hashCode()
            }
        }
    }

    override fun stop() {
        mongoClient.close()
        logger.info("MongoDB connection closed.")
    }
}