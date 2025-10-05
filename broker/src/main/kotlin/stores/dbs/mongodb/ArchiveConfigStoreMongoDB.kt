package at.rocworks.stores.mongodb

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.stores.*
import at.rocworks.utils.DurationParser
import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters
import com.mongodb.client.model.ReplaceOptions
import at.rocworks.handlers.ArchiveGroup
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import org.bson.Document
import java.time.Instant
import java.util.logging.Logger
import java.util.concurrent.Callable

/**
 * MongoDB implementation of IConfigStore for archive group configuration management
 */
class ArchiveConfigStoreMongoDB(
    private val connectionString: String,
    private val databaseName: String
) : AbstractVerticle(), IArchiveConfigStore {

    private val logger: Logger = Utils.getLogger(this::class.java)
    private val collectionName = "archiveconfigs"

    private lateinit var mongoClient: MongoClient
    private lateinit var database: MongoDatabase
    private lateinit var collection: MongoCollection<Document>

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun start(startPromise: Promise<Void>) {
        try {
            val settings = MongoClientSettings.builder()
                .applyConnectionString(ConnectionString(connectionString))
                .build()

            mongoClient = MongoClients.create(settings)
            database = mongoClient.getDatabase(databaseName)
            collection = database.getCollection(collectionName)

            // Create index on name field for faster lookups
            collection.createIndex(Document("name", 1))

            // Insert default archive config if it doesn't exist
            insertDefaultArchiveGroup()

            logger.info("MongoDB ConfigStore connected successfully to $databaseName.$collectionName with default entry")
            startPromise.complete()
        } catch (e: Exception) {
            logger.severe("Failed to connect to MongoDB: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        try {
            if (::mongoClient.isInitialized) {
                mongoClient.close()
                logger.info("MongoDB ConfigStore connection closed")
            }
            stopPromise.complete()
        } catch (e: Exception) {
            logger.warning("Error closing MongoDB connection: ${e.message}")
            stopPromise.complete() // Complete anyway
        }
    }

    override fun getAllArchiveGroups(): io.vertx.core.Future<List<ArchiveGroupConfig>> {
        val promise = Promise.promise<List<ArchiveGroupConfig>>()

        vertx.executeBlocking(Callable {
            try {
                val results = mutableListOf<ArchiveGroupConfig>()

                collection.find().forEach { document ->
                    try {
                        val archiveGroup = documentToArchiveGroup(document)
                        val enabled = document.getBoolean("enabled", false)
                        results.add(ArchiveGroupConfig(archiveGroup, enabled))
                    } catch (e: Exception) {
                        logger.warning("Error parsing archive group document: ${e.message}")
                    }
                }

                logger.info("Retrieved ${results.size} archive groups from MongoDB")
                results
            } catch (e: Exception) {
                logger.severe("Error retrieving archive groups from MongoDB: ${e.message}")
                emptyList()
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in getAllArchiveGroups: ${result.cause()?.message}")
                promise.complete(emptyList())
            }
        }

        return promise.future()
    }

    override fun getArchiveGroup(name: String): io.vertx.core.Future<ArchiveGroupConfig?> {
        val promise = Promise.promise<ArchiveGroupConfig?>()

        vertx.executeBlocking(Callable {
            try {
                val document = collection.find(Filters.eq("name", name)).first()

                if (document != null) {
                    val archiveGroup = documentToArchiveGroup(document)
                    val enabled = document.getBoolean("enabled", false)
                    ArchiveGroupConfig(archiveGroup, enabled)
                } else {
                    null
                }
            } catch (e: Exception) {
                logger.severe("Error retrieving archive group '$name' from MongoDB: ${e.message}")
                null
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in getArchiveGroup: ${result.cause()?.message}")
                promise.complete(null)
            }
        }

        return promise.future()
    }

    override fun saveArchiveGroup(archiveGroup: ArchiveGroup, enabled: Boolean): io.vertx.core.Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        vertx.executeBlocking(Callable {
            try {
                val document = archiveGroupToDocument(archiveGroup, enabled)

                val options = ReplaceOptions().upsert(true)
                val result = collection.replaceOne(
                    Filters.eq("name", archiveGroup.name),
                    document,
                    options
                )

                val success = result.wasAcknowledged()
                if (success) {
                    logger.info("Archive group '${archiveGroup.name}' saved successfully to MongoDB")
                } else {
                    logger.warning("Failed to save archive group '${archiveGroup.name}' to MongoDB")
                }
                success
            } catch (e: Exception) {
                logger.severe("Error saving archive group '${archiveGroup.name}' to MongoDB: ${e.message}")
                false
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in saveArchiveGroup: ${result.cause()?.message}")
                promise.complete(false)
            }
        }

        return promise.future()
    }

    override fun updateArchiveGroup(archiveGroup: ArchiveGroup, enabled: Boolean): io.vertx.core.Future<Boolean> {
        return saveArchiveGroup(archiveGroup, enabled) // MongoDB upsert handles both insert and update
    }

    override fun deleteArchiveGroup(name: String): io.vertx.core.Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        vertx.executeBlocking(Callable {
            try {
                val result = collection.deleteOne(Filters.eq("name", name))
                val success = result.deletedCount > 0

                if (success) {
                    logger.info("Archive group '$name' deleted successfully from MongoDB")
                } else {
                    logger.warning("Archive group '$name' not found in MongoDB for deletion")
                }
                success
            } catch (e: Exception) {
                logger.severe("Error deleting archive group '$name' from MongoDB: ${e.message}")
                false
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in deleteArchiveGroup: ${result.cause()?.message}")
                promise.complete(false)
            }
        }

        return promise.future()
    }

    private fun insertDefaultArchiveGroup() {
        try {
            // Check if Default archive group already exists
            val existing = collection.find(Filters.eq("name", "Default")).first()
            if (existing == null) {
                // Create default archive config document
                val defaultDoc = Document()
                    .append("name", "Default")
                    .append("enabled", true)
                    .append("topic_filter", listOf("#"))
                    .append("retained_only", false)
                    .append("last_val_type", "MEMORY")
                    .append("archive_type", "NONE")
                    .append("last_val_retention", "1h")
                    .append("archive_retention", "1h")
                    .append("purge_interval", "1h")
                    .append("payload_format", "JAVA")
                    .append("created_at", Instant.now())
                    .append("updated_at", Instant.now())

                collection.insertOne(defaultDoc)
                logger.info("Default archive group created in MongoDB")
            } else {
                logger.fine("Default archive group already exists in MongoDB")
            }
        } catch (e: Exception) {
            logger.warning("Error creating default archive group in MongoDB: ${e.message}")
        }
    }

    private fun documentToArchiveGroup(document: Document): ArchiveGroup {
        val name = document.getString("name") ?: throw IllegalArgumentException("Missing name field")

        // Parse topic filter - handle both array and legacy object formats
        val topicFilter = when (val topicFilterObj = document.get("topic_filter")) {
            is List<*> -> topicFilterObj.map { it.toString() }
            is Document -> {
                val filters = topicFilterObj.get("filters") as? List<*>
                filters?.map { it.toString() } ?: emptyList()
            }
            else -> emptyList()
        }

        val retainedOnly = document.getBoolean("retained_only", false)
        val lastValType = MessageStoreType.valueOf(document.getString("last_val_type") ?: "MONGODB")
        val archiveType = MessageArchiveType.valueOf(document.getString("archive_type") ?: "MONGODB")

        val lastValRetention = document.getString("last_val_retention")
        val archiveRetention = document.getString("archive_retention")
        val purgeInterval = document.getString("purge_interval")

        val payloadFormatStr = document.getString("payload_format")
        val payloadFormat = try { if (payloadFormatStr != null) PayloadFormat.valueOf(payloadFormatStr) else PayloadFormat.JAVA } catch (e: Exception) { PayloadFormat.JAVA }

        return ArchiveGroup(
            name = name,
            topicFilter = topicFilter,
            retainedOnly = retainedOnly,
            lastValType = lastValType,
            archiveType = archiveType,
            payloadFormat = payloadFormat,
            lastValRetentionMs = lastValRetention?.let { DurationParser.parse(it) },
            archiveRetentionMs = archiveRetention?.let { DurationParser.parse(it) },
            purgeIntervalMs = purgeInterval?.let { DurationParser.parse(it) },
            lastValRetentionStr = lastValRetention,
            archiveRetentionStr = archiveRetention,
            purgeIntervalStr = purgeInterval,
            databaseConfig = JsonObject() // Will be populated from config
        )
    }

    private fun archiveGroupToDocument(archiveGroup: ArchiveGroup, enabled: Boolean): Document {
        val document = Document()
            .append("name", archiveGroup.name)
            .append("enabled", enabled)
            .append("topic_filter", archiveGroup.topicFilter)
            .append("retained_only", archiveGroup.retainedOnly)
            .append("last_val_type", archiveGroup.getLastValType().name)
            .append("archive_type", archiveGroup.getArchiveType().name)
            .append("payload_format", archiveGroup.payloadFormat.name)
            .append("updated_at", Instant.now())

        // Add optional duration fields
        archiveGroup.getLastValRetentionMs()?.let { ms ->
            document.append("last_val_retention", DurationParser.formatDuration(ms))
        }
        archiveGroup.getArchiveRetentionMs()?.let { ms ->
            document.append("archive_retention", DurationParser.formatDuration(ms))
        }
        archiveGroup.getPurgeIntervalMs()?.let { ms ->
            document.append("purge_interval", DurationParser.formatDuration(ms))
        }

        return document
    }

    override fun getType(): String = "ConfigStoreMongoDB"
}