package at.rocworks.stores.mongodb

import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigException
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.ImportDeviceConfigResult
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters.*
import com.mongodb.client.model.ReplaceOptions
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import org.bson.Document
import java.time.Instant
import java.util.*
import java.util.concurrent.Callable
import java.util.logging.Logger

/**
 * MongoDB implementation of DeviceConfigStore
 */
class DeviceConfigStoreMongoDB(
    private val connectionString: String,
    private val databaseName: String
) : IDeviceConfigStore {

    private val logger: Logger = Utils.getLogger(DeviceConfigStoreMongoDB::class.java)
    private lateinit var mongoClient: MongoClient
    private lateinit var database: MongoDatabase
    private lateinit var deviceConfigsCollection: MongoCollection<Document>
    private var vertx: Vertx? = null
    private var reconnectOngoing = false
    private val defaultRetryWaitTime = 3000L
    private var healthCheckTimer: Long? = null

    companion object {
        private const val COLLECTION_NAME = "deviceconfigs"
    }

    override fun initialize(): Future<Void> {
        val promise = Promise.promise<Void>()

        try {
            mongoClient = MongoClients.create(connectionString)
            database = mongoClient.getDatabase(databaseName)

            // Initialize collection
            deviceConfigsCollection = database.getCollection(COLLECTION_NAME)

            // Create indexes for faster queries
            deviceConfigsCollection.createIndex(Document("name", 1)) // Unique index on name
            deviceConfigsCollection.createIndex(Document("node_id", 1))
            deviceConfigsCollection.createIndex(Document("enabled", 1))
            deviceConfigsCollection.createIndex(Document("namespace", 1))
            deviceConfigsCollection.createIndex(Document("type", 1))

            logger.info("DeviceConfigStoreMongoDB initialized successfully")
            promise.complete()
        } catch (e: Exception) {
            logger.severe("Failed to initialize DeviceConfigStoreMongoDB: ${e.message}")
            promise.fail(DeviceConfigException("Failed to initialize database", e))
        }

        return promise.future()
    }

    override fun getAllDevices(): Future<List<DeviceConfig>> {
        val promise = Promise.promise<List<DeviceConfig>>()

        try {
            val documents = deviceConfigsCollection.find().sort(Document("name", 1))
            val devices = mutableListOf<DeviceConfig>()

            for (doc in documents) {
                try {
                    devices.add(mapDocumentToDevice(doc))
                } catch (e: DeviceConfigException) {
                    logger.warning("Skipping invalid device record: ${e.message}")
                    // Continue processing other records instead of failing completely
                }
            }

            promise.complete(devices)
        } catch (e: Exception) {
            logger.severe("Failed to get all devices: ${e.message}")
            promise.fail(DeviceConfigException("Failed to get all devices", e))
        }

        return promise.future()
    }

    override fun getDevicesByNode(nodeId: String): Future<List<DeviceConfig>> {
        val promise = Promise.promise<List<DeviceConfig>>()

        try {
            val documents = deviceConfigsCollection.find(eq("node_id", nodeId)).sort(Document("name", 1))
            val devices = mutableListOf<DeviceConfig>()

            for (doc in documents) {
                try {
                    devices.add(mapDocumentToDevice(doc))
                } catch (e: DeviceConfigException) {
                    logger.warning("Skipping invalid device record: ${e.message}")
                    // Continue processing other records instead of failing completely
                }
            }

            promise.complete(devices)
        } catch (e: Exception) {
            logger.severe("Failed to get devices by node $nodeId: ${e.message}")
            promise.fail(DeviceConfigException("Failed to get devices by node", e))
        }

        return promise.future()
    }

    override fun getEnabledDevicesByNode(nodeId: String): Future<List<DeviceConfig>> {
        val promise = Promise.promise<List<DeviceConfig>>()

        try {
            val filter = and(eq("node_id", nodeId), eq("enabled", true))
            val documents = deviceConfigsCollection.find(filter).sort(Document("name", 1))
            val devices = mutableListOf<DeviceConfig>()

            for (doc in documents) {
                try {
                    devices.add(mapDocumentToDevice(doc))
                } catch (e: DeviceConfigException) {
                    logger.warning("Skipping invalid device record: ${e.message}")
                    // Continue processing other records instead of failing completely
                }
            }

            promise.complete(devices)
        } catch (e: Exception) {
            logger.severe("Failed to get enabled devices by node $nodeId: ${e.message}")
            promise.fail(DeviceConfigException("Failed to get enabled devices by node", e))
        }

        return promise.future()
    }

    override fun getDevice(name: String): Future<DeviceConfig?> {
        val promise = Promise.promise<DeviceConfig?>()

        try {
            val document = deviceConfigsCollection.find(eq("name", name)).first()
            if (document != null) {
                try {
                    promise.complete(mapDocumentToDevice(document))
                } catch (e: DeviceConfigException) {
                    logger.warning("Invalid device record for name $name: ${e.message}")
                    promise.complete(null) // Return null for invalid records
                }
            } else {
                promise.complete(null)
            }
        } catch (e: Exception) {
            logger.severe("Failed to get device $name: ${e.message}")
            promise.fail(DeviceConfigException("Failed to get device", e))
        }

        return promise.future()
    }

    override fun isNamespaceInUse(namespace: String, excludeName: String?): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        try {
            val filter = if (excludeName != null) {
                and(eq("namespace", namespace), ne("name", excludeName))
            } else {
                eq("namespace", namespace)
            }

            val count = deviceConfigsCollection.countDocuments(filter)
            promise.complete(count > 0)
        } catch (e: Exception) {
            logger.severe("Failed to check namespace usage: ${e.message}")
            promise.fail(DeviceConfigException("Failed to check namespace usage", e))
        }

        return promise.future()
    }

    override fun saveDevice(device: DeviceConfig): Future<DeviceConfig> {
        val promise = Promise.promise<DeviceConfig>()

        try {
            val now = Instant.now()
            val updatedDevice = device.copy(updatedAt = now)

            // Check if document exists
            val existingDoc = deviceConfigsCollection.find(eq("name", device.name)).first()

            val document = mapDeviceToDocument(updatedDevice)

            if (existingDoc == null) {
                // New document - let MongoDB generate _id
                deviceConfigsCollection.insertOne(document)
            } else {
                // Update existing document
                deviceConfigsCollection.updateOne(
                    eq("name", device.name),
                    Document("\$set", document)
                )
            }

            promise.complete(updatedDevice)
        } catch (e: Exception) {
            logger.severe("Failed to save device ${device.name}: ${e.message}")
            promise.fail(DeviceConfigException("Failed to save device", e))
        }

        return promise.future()
    }

    override fun deleteDevice(name: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        try {
            val result = deviceConfigsCollection.deleteOne(eq("name", name))
            promise.complete(result.deletedCount > 0)
        } catch (e: Exception) {
            logger.severe("Failed to delete device $name: ${e.message}")
            promise.fail(DeviceConfigException("Failed to delete device", e))
        }

        return promise.future()
    }

    override fun toggleDevice(name: String, enabled: Boolean): Future<DeviceConfig?> {
        val promise = Promise.promise<DeviceConfig?>()

        try {
            val update = Updates.combine(
                Updates.set("enabled", enabled),
                Updates.set("updated_at", Date.from(Instant.now()))
            )

            val result = deviceConfigsCollection.updateOne(eq("name", name), update)

            if (result.modifiedCount > 0) {
                // Return updated device
                getDevice(name).onComplete { deviceResult ->
                    if (deviceResult.succeeded()) {
                        promise.complete(deviceResult.result())
                    } else {
                        promise.fail(deviceResult.cause())
                    }
                }
            } else {
                promise.complete(null)
            }
        } catch (e: Exception) {
            logger.severe("Failed to toggle device $name: ${e.message}")
            promise.fail(DeviceConfigException("Failed to toggle device", e))
        }

        return promise.future()
    }

    override fun reassignDevice(name: String, nodeId: String): Future<DeviceConfig?> {
        val promise = Promise.promise<DeviceConfig?>()

        try {
            val update = Updates.combine(
                Updates.set("node_id", nodeId),
                Updates.set("updated_at", Date.from(Instant.now()))
            )

            val result = deviceConfigsCollection.updateOne(eq("name", name), update)

            if (result.modifiedCount > 0) {
                // Return updated device
                getDevice(name).onComplete { deviceResult ->
                    if (deviceResult.succeeded()) {
                        promise.complete(deviceResult.result())
                    } else {
                        promise.fail(deviceResult.cause())
                    }
                }
            } else {
                promise.complete(null)
            }
        } catch (e: Exception) {
            logger.severe("Failed to reassign device $name: ${e.message}")
            promise.fail(DeviceConfigException("Failed to reassign device", e))
        }

        return promise.future()
    }

    override fun close(): Future<Void> {
        val promise = Promise.promise<Void>()

        try {
            mongoClient.close()
            promise.complete()
        } catch (e: Exception) {
            logger.warning("Error closing MongoDB connection: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    fun startHealthChecks(vertxInstance: Vertx) {
        if (vertx != null) return // Already started
        this.vertx = vertxInstance
        healthCheckTimer = vertxInstance.setPeriodic(5000) { // Check every 5 seconds
            vertxInstance.executeBlocking<Boolean>(Callable {
                checkConnection()
            }).onSuccess { isHealthy ->
                if (!isHealthy && !reconnectOngoing) {
                    logger.warning("Connection health check failed, attempting reconnection...")
                    reconnect()
                }
            }.onFailure { error ->
                logger.warning("Health check error: ${error.message}")
                if (!reconnectOngoing) {
                    reconnect()
                }
            }
        }
        logger.info("DeviceConfigStore health checks started (5 second interval)")
    }

    private fun checkConnection(): Boolean {
        return try {
            // Use admin database to send a ping command
            val adminDb = mongoClient.getDatabase("admin")
            adminDb.runCommand(Document("ping", 1))
            true // Connection is good
        } catch (e: Exception) {
            logger.finer("Connection check failed: ${e.message}")
            false
        }
    }

    private fun reconnect() {
        if (!reconnectOngoing && vertx != null) {
            reconnectOngoing = true
            vertx!!.executeBlocking(Callable {
                try {
                    logger.info("Attempting to reconnect to MongoDB...")
                    // Close old client
                    try {
                        mongoClient.close()
                    } catch (e: Exception) {
                        logger.finer("Error closing old MongoDB connection: ${e.message}")
                    }
                    // Create new client
                    mongoClient = MongoClients.create(connectionString)
                    database = mongoClient.getDatabase(databaseName)
                    deviceConfigsCollection = database.getCollection(COLLECTION_NAME)
                    logger.info("Successfully reconnected to MongoDB")
                    reconnectOngoing = false
                } catch (e: Exception) {
                    logger.warning("Reconnection failed: ${e.message}. Will retry in ${defaultRetryWaitTime}ms")
                    reconnectOngoing = false
                    vertx!!.setTimer(defaultRetryWaitTime) {
                        reconnect()
                    }
                }
            })
        }
    }

    private fun mapDeviceToDocument(device: DeviceConfig): Document {
        val configJson = device.config

        return Document().apply {
            put("name", device.name)
            put("namespace", device.namespace)
            put("node_id", device.nodeId)
            put("config", Document.parse(configJson.toString()))
            put("enabled", device.enabled)
            put("type", device.type)
            put("created_at", Date.from(device.createdAt))
            put("updated_at", Date.from(device.updatedAt))
        }
    }

    private fun mapDocumentToDevice(document: Document): DeviceConfig {
        val configDoc = document.get("config", Document::class.java)
        if (configDoc == null) {
            throw DeviceConfigException("Config field is null for device")
        }
        val configJson = JsonObject(configDoc.toJson())

        val name = document.getString("name")
        if (name == null) {
            throw DeviceConfigException("Name field is null for device")
        }

        val namespace = document.getString("namespace")
        if (namespace == null) {
            throw DeviceConfigException("Namespace field is null for device")
        }

        val nodeId = document.getString("node_id")
        if (nodeId == null) {
            throw DeviceConfigException("Node ID field is null for device")
        }

        return DeviceConfig(
            name = name,
            namespace = namespace,
            nodeId = nodeId,
            config = configJson,
            enabled = document.getBoolean("enabled", true),
            type = document.getString("type") ?: DeviceConfig.DEVICE_TYPE_OPCUA_CLIENT,
            createdAt = document.getDate("created_at")?.toInstant() ?: Instant.now(),
            updatedAt = document.getDate("updated_at")?.toInstant() ?: Instant.now()
        )
    }

    override fun exportConfigs(names: List<String>?): Future<List<Map<String, Any?>>> {
        val promise = Promise.promise<List<Map<String, Any?>>>()

        try {
            val documents = if (names.isNullOrEmpty()) {
                deviceConfigsCollection.find().sort(Document("name", 1)).toList()
            } else {
                deviceConfigsCollection.find(`in`("name", names)).sort(Document("name", 1)).toList()
            }

            val configs = documents.map { doc ->
                val configObj = doc.get("config")
                val configMap = when (configObj) {
                    is Document -> configObj.toMap()
                    is Map<*, *> -> @Suppress("UNCHECKED_CAST") (configObj as Map<String, Any?>)
                    else -> emptyMap<String, Any?>()
                }

                mapOf(
                    "name" to doc.getString("name"),
                    "namespace" to doc.getString("namespace"),
                    "nodeId" to doc.getString("node_id"),
                    "config" to configMap,
                    "enabled" to doc.getBoolean("enabled", true),
                    "type" to (doc.getString("type") ?: DeviceConfig.DEVICE_TYPE_OPCUA_CLIENT),
                    "createdAt" to (doc.getDate("created_at")?.toInstant()?.toString() ?: ""),
                    "updatedAt" to (doc.getDate("updated_at")?.toInstant()?.toString() ?: "")
                )
            }

            promise.complete(configs)
        } catch (e: Exception) {
            logger.severe("Failed to export device configs: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    override fun importConfigs(configs: List<Map<String, Any?>>): Future<ImportDeviceConfigResult> {
        val promise = Promise.promise<ImportDeviceConfigResult>()

        try {
            var imported = 0
            val errors = mutableListOf<String>()

            for ((index, configMap) in configs.withIndex()) {
                try {
                    val name = (configMap["name"] as? String)?.takeIf { it.isNotBlank() }
                        ?: throw IllegalArgumentException("Device name is required at index $index")
                    val namespace = (configMap["namespace"] as? String)?.takeIf { it.isNotBlank() }
                        ?: throw IllegalArgumentException("Namespace is required for device $name")
                    val nodeId = (configMap["nodeId"] as? String)?.takeIf { it.isNotBlank() }
                        ?: throw IllegalArgumentException("NodeId is required for device $name")
                    val type = (configMap["type"] as? String) ?: DeviceConfig.DEVICE_TYPE_OPCUA_CLIENT
                    val enabled = (configMap["enabled"] as? Boolean) ?: true

                    @Suppress("UNCHECKED_CAST")
                    val configObj = when (val cfg = configMap["config"]) {
                        is Map<*, *> -> cfg as Map<String, Any?>
                        else -> throw IllegalArgumentException("Config must be a JSON object for device $name")
                    }

                    val document = Document()
                        .append("name", name)
                        .append("namespace", namespace)
                        .append("node_id", nodeId)
                        .append("config", configObj)
                        .append("enabled", enabled)
                        .append("type", type)
                        .append("created_at", Date())
                        .append("updated_at", Date())

                    // Check if document with this name exists
                    val existingDoc = deviceConfigsCollection.find(eq("name", name)).first()
                    if (existingDoc == null) {
                        // New document - let MongoDB generate _id
                        deviceConfigsCollection.insertOne(document)
                    } else {
                        // Update existing document by name
                        deviceConfigsCollection.updateOne(
                            eq("name", name),
                            Document("\$set", document)
                        )
                    }

                    imported++
                } catch (e: Exception) {
                    val errorMsg = "Failed to import config at index $index: ${e.message}"
                    logger.warning(errorMsg)
                    errors.add(errorMsg)
                }
            }

            val failed = configs.size - imported
            val result = if (errors.isEmpty()) {
                ImportDeviceConfigResult.success(imported)
            } else if (imported > 0) {
                ImportDeviceConfigResult.partial(imported, failed, errors)
            } else {
                ImportDeviceConfigResult.failure(errors)
            }

            promise.complete(result)
        } catch (e: Exception) {
            logger.severe("Failed to import device configs: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }
}