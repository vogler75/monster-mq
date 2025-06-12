package at.rocworks.stores.mongodb

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.stores.IMessageArchive
import at.rocworks.stores.MessageArchiveType
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.TimeSeriesGranularity
import com.mongodb.client.model.TimeSeriesOptions
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import org.bson.Document
import java.time.Instant
import java.util.*

class MessageArchiveMongoDB(
    private val name: String,
    private val connectionString: String,
    private val databaseName: String
): AbstractVerticle(), IMessageArchive {

    private val logger = Utils.getLogger(this::class.java)
    private val tableName = name

    private lateinit var mongoClient: MongoClient
    private lateinit var database: MongoDatabase
    private lateinit var collection: MongoCollection<Document>

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun start(startPromise: Promise<Void>) {
        try {
            mongoClient = MongoClients.create(connectionString)
            database = mongoClient.getDatabase(databaseName)

            val timeSeriesOptions = TimeSeriesOptions("time")
                .metaField("metadata")
                .granularity(TimeSeriesGranularity.MINUTES)
            val createCollectionOptions = CreateCollectionOptions().timeSeriesOptions(timeSeriesOptions)

            if (!database.listCollectionNames().into(mutableListOf()).contains(tableName)) {
                database.createCollection(tableName, createCollectionOptions)
            }

            collection = database.getCollection(tableName)
            startPromise.complete()
        } catch (e: Exception) {
            logger.severe("Error starting MongoDB message archive: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun addHistory(messages: List<MqttMessage>) {
        val documents = messages.map { message ->
            val json = message.getPayloadAsJson()
            if (json != null)
                Document(
                    mapOf(
                        "meta.topic" to message.topicName,
                        "time" to Date(message.time.toEpochMilli()),
                        "payload" to json,
                        "qos" to message.qosLevel,
                        "retained" to message.isRetain,
                        "client_id" to message.clientId,
                        "message_uuid" to message.messageUuid
                    )
                )
            else
                Document(
                    mapOf(
                        "meta.topic" to Utils.getTopicLevels(message.topicName).toList(),
                        "time" to Date(message.time.toEpochMilli()),
                        "payload" to message.payload,
                        "qos" to message.qosLevel,
                        "retained" to message.isRetain,
                        "client_id" to message.clientId,
                        "message_uuid" to message.messageUuid
                    )
                )
        }

        try {
            collection.insertMany(documents)
        } catch (e: Exception) {
            logger.warning("Error inserting batch data: ${e.message}")
        }
    }

    override fun getHistory(
        topic: String,
        startTime: Instant?,
        endTime: Instant?,
        limit: Int
    ): List<MqttMessage> {
        TODO("Not yet implemented")
    }

    override fun getName(): String = name

    override fun getType(): MessageArchiveType = MessageArchiveType.MONGODB

}