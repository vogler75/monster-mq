package at.rocworks.stores.mongodb

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.stores.*
import com.mongodb.client.MongoClient
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
    private val connectionCollectionName = "databaseconnections"

    private lateinit var mongoClient: MongoClient
    private lateinit var database: MongoDatabase
    private lateinit var collection: MongoCollection<Document>
    private lateinit var connectionCollection: MongoCollection<Document>


    override fun start(startPromise: Promise<Void>) {
        try {
            mongoClient = MongoClientPool.getClient(connectionString)
            database = mongoClient.getDatabase(databaseName)
            collection = database.getCollection(collectionName)
            connectionCollection = database.getCollection(connectionCollectionName)

            // Create index in background to avoid blocking the event loop
            vertx.executeBlocking(Callable {
                try {
                    collection.createIndex(Document("name", 1))
                    connectionCollection.createIndex(Document("name", 1))
                    logger.info("MongoDB ConfigStore indexes created successfully")
                } catch (e: Exception) {
                    logger.warning("Failed to create indexes: ${e.message}")
                }
                null
            })

            logger.info("MongoDB ConfigStore connected successfully to $databaseName.$collectionName")
            startPromise.complete()
        } catch (e: Exception) {
            logger.severe("Failed to connect to MongoDB: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        try {
            if (::mongoClient.isInitialized) {
                MongoClientPool.releaseClient(connectionString)
                logger.info("MongoDB ConfigStore connection released")
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

    override fun getAllDatabaseConnections(): io.vertx.core.Future<List<DatabaseConnectionConfig>> {
        val promise = Promise.promise<List<DatabaseConnectionConfig>>()
        vertx.executeBlocking(Callable {
            try {
                val results = mutableListOf<DatabaseConnectionConfig>()
                connectionCollection.find().forEach { document ->
                    results.add(documentToDatabaseConnection(document))
                }
                results.sortedBy { it.name }
            } catch (e: Exception) {
                logger.severe("Error retrieving database connections from MongoDB: ${e.message}")
                emptyList()
            }
        }).onComplete { result ->
            if (result.succeeded()) promise.complete(result.result()) else promise.complete(emptyList())
        }
        return promise.future()
    }

    override fun getDatabaseConnection(name: String): io.vertx.core.Future<DatabaseConnectionConfig?> {
        val promise = Promise.promise<DatabaseConnectionConfig?>()
        vertx.executeBlocking(Callable {
            try {
                connectionCollection.find(Filters.eq("name", name)).first()?.let { documentToDatabaseConnection(it) }
            } catch (e: Exception) {
                logger.severe("Error retrieving database connection '$name' from MongoDB: ${e.message}")
                null
            }
        }).onComplete { result ->
            if (result.succeeded()) promise.complete(result.result()) else promise.complete(null)
        }
        return promise.future()
    }

    override fun saveDatabaseConnection(connection: DatabaseConnectionConfig): io.vertx.core.Future<Boolean> {
        val promise = Promise.promise<Boolean>()
        vertx.executeBlocking(Callable {
            try {
                val now = Instant.now()
                val existing = connectionCollection.find(Filters.eq("name", connection.name)).first()
                val document = Document()
                    .append("name", connection.name)
                    .append("type", connection.type.name)
                    .append("url", connection.url)
                    .append("username", connection.username)
                    .append("password", connection.password)
                    .append("database", connection.database)
                    .append("schema", connection.schema)
                    .append("created_at", existing?.get("created_at") ?: now)
                    .append("updated_at", now)
                val result = connectionCollection.replaceOne(
                    Filters.eq("name", connection.name),
                    document,
                    ReplaceOptions().upsert(true)
                )
                result.wasAcknowledged()
            } catch (e: Exception) {
                logger.severe("Error saving database connection '${connection.name}' to MongoDB: ${e.message}")
                false
            }
        }).onComplete { result ->
            if (result.succeeded()) promise.complete(result.result()) else promise.complete(false)
        }
        return promise.future()
    }

    override fun deleteDatabaseConnection(name: String): io.vertx.core.Future<Boolean> {
        val promise = Promise.promise<Boolean>()
        vertx.executeBlocking(Callable {
            try {
                connectionCollection.deleteOne(Filters.eq("name", name)).deletedCount > 0
            } catch (e: Exception) {
                logger.severe("Error deleting database connection '$name' from MongoDB: ${e.message}")
                false
            }
        }).onComplete { result ->
            if (result.succeeded()) promise.complete(result.result()) else promise.complete(false)
        }
        return promise.future()
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
        val payloadFormat = PayloadFormat.parse(payloadFormatStr)

        return ArchiveGroup(
            name = name,
            topicFilter = topicFilter,
            retainedOnly = retainedOnly,
            lastValType = lastValType,
            archiveType = archiveType,
            payloadFormat = payloadFormat,
            lastValRetentionMs = lastValRetention?.let { Utils.parseDuration(it) },
            archiveRetentionMs = archiveRetention?.let { Utils.parseDuration(it) },
            purgeIntervalMs = purgeInterval?.let { Utils.parseDuration(it) },
            lastValRetentionStr = lastValRetention,
            archiveRetentionStr = archiveRetention,
            purgeIntervalStr = purgeInterval,
            databaseConnectionName = document.getString("database_connection_name"),
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
            .append("database_connection_name", archiveGroup.getDatabaseConnectionName())
            .append("updated_at", Instant.now())

        // Add optional duration fields (preserve original string to avoid lossy roundtrip via formatDuration)
        archiveGroup.getLastValRetention()?.let { document.append("last_val_retention", it) }
        archiveGroup.getArchiveRetention()?.let { document.append("archive_retention", it) }
        archiveGroup.getPurgeInterval()?.let { document.append("purge_interval", it) }

        return document
    }

    private fun documentToDatabaseConnection(document: Document): DatabaseConnectionConfig {
        return DatabaseConnectionConfig(
            name = document.getString("name"),
            type = DatabaseConnectionType.valueOf(document.getString("type")),
            url = document.getString("url"),
            username = document.getString("username"),
            password = document.getString("password"),
            database = document.getString("database"),
            schema = document.getString("schema"),
            createdAt = document.get("created_at")?.toString(),
            updatedAt = document.get("updated_at")?.toString()
        )
    }

    override fun getType(): String = "ConfigStoreMongoDB"

    override suspend fun tableExists(): Boolean {
        return try {
            if (!::database.isInitialized) {
                logger.warning("MongoDB not initialized, cannot check collection existence for [$collectionName]")
                return false
            }
            val collections = database.listCollectionNames().into(mutableListOf())
            collections.contains(collectionName)
        } catch (e: Exception) {
            logger.warning("Error checking if collection [$collectionName] exists: ${e.message}")
            false
        }
    }
}
