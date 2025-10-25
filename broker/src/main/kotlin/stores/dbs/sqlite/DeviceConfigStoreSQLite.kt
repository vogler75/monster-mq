package at.rocworks.stores.sqlite

import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigException
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.ImportDeviceConfigResult
import at.rocworks.stores.devices.OpcUaAddress
import at.rocworks.stores.devices.OpcUaConnectionConfig
import at.rocworks.stores.sqlite.SQLiteClient
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.Callable
import java.util.logging.Logger

/**
 * SQLite implementation of DeviceConfigStore using the shared SQLiteVerticle
 */
class DeviceConfigStoreSQLite(
    private val vertx: Vertx,
    private val dbPath: String
) : IDeviceConfigStore {

    private val logger: Logger = Utils.getLogger(DeviceConfigStoreSQLite::class.java)
    private lateinit var sqliteClient: SQLiteClient
    private var reconnectOngoing = false
    private val defaultRetryWaitTime = 3000L
    private var healthCheckTimer: Long? = null

    companion object {
        private const val TABLE_NAME = "deviceconfigs"

        private val CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS $TABLE_NAME (
                name TEXT PRIMARY KEY,
                namespace TEXT NOT NULL,
                node_id TEXT NOT NULL,
                config TEXT NOT NULL,
                enabled INTEGER DEFAULT 1,
                type TEXT DEFAULT '${DeviceConfig.DEVICE_TYPE_OPCUA_CLIENT}',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """

        private const val CREATE_INDEX_NODE_ID = """
            CREATE INDEX IF NOT EXISTS idx_deviceconfigs_node_id ON $TABLE_NAME (node_id)
        """

        private const val CREATE_INDEX_ENABLED = """
            CREATE INDEX IF NOT EXISTS idx_deviceconfigs_enabled ON $TABLE_NAME (enabled)
        """

        private const val CREATE_INDEX_NAMESPACE = """
            CREATE INDEX IF NOT EXISTS idx_deviceconfigs_namespace ON $TABLE_NAME (namespace)
        """

        private const val SELECT_ALL = """
            SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at
            FROM $TABLE_NAME
            ORDER BY name
        """

        private const val SELECT_BY_NODE = """
            SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at
            FROM $TABLE_NAME
            WHERE node_id = ?
            ORDER BY name
        """

        private const val SELECT_ENABLED_BY_NODE = """
            SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at
            FROM $TABLE_NAME
            WHERE node_id = ? AND enabled = 1
            ORDER BY name
        """

        private const val SELECT_BY_NAME = """
            SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at
            FROM $TABLE_NAME
            WHERE name = ?
        """

        private const val INSERT_DEVICE = """
            INSERT INTO $TABLE_NAME (name, namespace, node_id, config, enabled, type, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        """

        private const val UPDATE_DEVICE = """
            UPDATE $TABLE_NAME SET namespace = ?, node_id = ?, config = ?, enabled = ?, type = ?, updated_at = datetime('now')
            WHERE name = ?
        """

        private const val DELETE_DEVICE = """
            DELETE FROM $TABLE_NAME WHERE name = ?
        """

        private const val UPDATE_ENABLED = """
            UPDATE $TABLE_NAME SET enabled = ?, updated_at = datetime('now') WHERE name = ?
        """

        private const val UPDATE_NODE_ID = """
            UPDATE $TABLE_NAME SET node_id = ?, updated_at = datetime('now') WHERE name = ?
        """

        private const val COUNT_NAMESPACE = """
            SELECT COUNT(*) as count FROM $TABLE_NAME WHERE namespace = ?
        """

        private const val COUNT_NAMESPACE_EXCLUDE = """
            SELECT COUNT(*) as count FROM $TABLE_NAME WHERE namespace = ? AND name != ?
        """

    }

    override fun initialize(): Future<Void> {
        val promise = Promise.promise<Void>()

        try {
            sqliteClient = SQLiteClient(vertx, dbPath)

            // Initialize database with table creation and indexes
            val initSql = JsonArray()
                .add(CREATE_TABLE)
                .add(CREATE_INDEX_NODE_ID)
                .add(CREATE_INDEX_ENABLED)
                .add(CREATE_INDEX_NAMESPACE)

            sqliteClient.initDatabase(initSql)
                .compose {
                    // Check if backup_node_id column exists and migrate if needed
                    migrateDropBackupNodeId()
                }
                .onSuccess {
                    logger.info("DeviceConfigStoreSQLite initialized successfully")
                    promise.complete()
                }
                .onFailure { error ->
                    logger.severe("Failed to initialize DeviceConfigStoreSQLite: ${error.message}")
                    promise.fail(DeviceConfigException("Failed to initialize database", error))
                }
        } catch (e: Exception) {
            logger.severe("Failed to create SQLiteClient: ${e.message}")
            promise.fail(DeviceConfigException("Failed to initialize database", e))
        }

        return promise.future()
    }

    private fun migrateDropBackupNodeId(): Future<Void> {
        val promise = Promise.promise<Void>()

        // Check if backup_node_id column exists
        val checkColumnSql = "PRAGMA table_info(deviceconfigs)"

        sqliteClient.executeQuery(checkColumnSql, JsonArray())
            .onSuccess { result ->
                var hasBackupNodeId = false
                for (i in 0 until result.size()) {
                    val row = result.getJsonObject(i)
                    val columnName = row.getString("name")
                    if (columnName == "backup_node_id") {
                        hasBackupNodeId = true
                        break
                    }
                }

                if (hasBackupNodeId) {
                    // Run migration step by step
                    logger.info("Migrating deviceconfigs table to remove backup_node_id column")

                    val createNewTable = """
                        CREATE TABLE deviceconfigs_new (
                            name TEXT PRIMARY KEY,
                            namespace TEXT NOT NULL,
                            node_id TEXT NOT NULL,
                            config TEXT NOT NULL,
                            enabled INTEGER DEFAULT 1,
                            type TEXT DEFAULT '${DeviceConfig.DEVICE_TYPE_OPCUA_CLIENT}',
                            created_at TEXT DEFAULT (datetime('now')),
                            updated_at TEXT DEFAULT (datetime('now'))
                        )
                    """

                    sqliteClient.executeUpdate(createNewTable, JsonArray())
                        .compose {
                            // Copy data
                            val copyData = """
                                INSERT INTO deviceconfigs_new (name, namespace, node_id, config, enabled, type, created_at, updated_at)
                                SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at FROM deviceconfigs
                            """
                            sqliteClient.executeUpdate(copyData, JsonArray())
                        }
                        .compose {
                            // Drop old table
                            sqliteClient.executeUpdate("DROP TABLE deviceconfigs", JsonArray())
                        }
                        .compose {
                            // Rename new table
                            sqliteClient.executeUpdate("ALTER TABLE deviceconfigs_new RENAME TO deviceconfigs", JsonArray())
                        }
                        .onComplete { migrationResult ->
                            if (migrationResult.succeeded()) {
                                logger.info("Successfully migrated deviceconfigs table")
                                promise.complete()
                            } else {
                                logger.severe("Failed to migrate deviceconfigs table: ${migrationResult.cause()?.message}")
                                promise.fail(migrationResult.cause())
                            }
                        }
                } else {
                    // No migration needed
                    promise.complete()
                }
            }
            .onFailure { error ->
                logger.severe("Failed to check table schema: ${error.message}")
                promise.fail(error)
            }

        return promise.future()
    }

    override fun getAllDevices(): Future<List<DeviceConfig>> {
        val promise = Promise.promise<List<DeviceConfig>>()

        sqliteClient.executeQuery(SELECT_ALL)
            .onSuccess { results ->
                try {
                    val devices = results.map { row ->
                        mapJsonToDeviceConfig(row as JsonObject)
                    }
                    promise.complete(devices)
                } catch (e: Exception) {
                    logger.severe("Failed to map results: ${e.message}")
                    promise.fail(DeviceConfigException("Failed to process results", e))
                }
            }
            .onFailure { error ->
                logger.severe("Failed to get all devices: ${error.message}")
                promise.fail(DeviceConfigException("Failed to retrieve devices", error))
            }

        return promise.future()
    }

    override fun getDevicesByNode(nodeId: String): Future<List<DeviceConfig>> {
        val promise = Promise.promise<List<DeviceConfig>>()

        sqliteClient.executeQuery(SELECT_BY_NODE, JsonArray().add(nodeId))
            .onSuccess { results ->
                try {
                    val devices = results.map { row ->
                        mapJsonToDeviceConfig(row as JsonObject)
                    }
                    promise.complete(devices)
                } catch (e: Exception) {
                    logger.severe("Failed to map results: ${e.message}")
                    promise.fail(DeviceConfigException("Failed to process results", e))
                }
            }
            .onFailure { error ->
                logger.severe("Failed to get devices by node $nodeId: ${error.message}")
                promise.fail(DeviceConfigException("Failed to retrieve devices for node", error))
            }

        return promise.future()
    }

    override fun getEnabledDevicesByNode(nodeId: String): Future<List<DeviceConfig>> {
        val promise = Promise.promise<List<DeviceConfig>>()

        sqliteClient.executeQuery(SELECT_ENABLED_BY_NODE, JsonArray().add(nodeId))
            .onSuccess { results ->
                try {
                    val devices = results.map { row ->
                        mapJsonToDeviceConfig(row as JsonObject)
                    }
                    promise.complete(devices)
                } catch (e: Exception) {
                    logger.severe("Failed to map results: ${e.message}")
                    promise.fail(DeviceConfigException("Failed to process results", e))
                }
            }
            .onFailure { error ->
                logger.severe("Failed to get enabled devices by node $nodeId: ${error.message}")
                promise.fail(DeviceConfigException("Failed to retrieve enabled devices for node", error))
            }

        return promise.future()
    }

    override fun getDevice(name: String): Future<DeviceConfig?> {
        val promise = Promise.promise<DeviceConfig?>()

        sqliteClient.executeQuery(SELECT_BY_NAME, JsonArray().add(name))
            .onSuccess { results ->
                try {
                    if (results.size() > 0) {
                        val device = mapJsonToDeviceConfig(results.getJsonObject(0))
                        promise.complete(device)
                    } else {
                        promise.complete(null)
                    }
                } catch (e: Exception) {
                    logger.severe("Failed to map result: ${e.message}")
                    promise.fail(DeviceConfigException("Failed to process result", e))
                }
            }
            .onFailure { error ->
                logger.severe("Failed to get device $name: ${error.message}")
                promise.fail(DeviceConfigException("Failed to retrieve device", error))
            }

        return promise.future()
    }

    override fun saveDevice(device: DeviceConfig): Future<DeviceConfig> {
        val promise = Promise.promise<DeviceConfig>()

        // Check if device exists first
        getDevice(device.name).onComplete { result ->
            if (result.succeeded()) {
                val exists = result.result() != null

                try {
                    // Config is already a JsonObject, use it directly
                    val configJson = device.config

                    val sql = if (exists) UPDATE_DEVICE else INSERT_DEVICE
                    val params = if (exists) {
                        // UPDATE: namespace, node_id, config, enabled, type WHERE name
                        JsonArray()
                            .add(device.namespace)
                            .add(device.nodeId)
                            .add(configJson.encode())
                            .add(if (device.enabled) 1 else 0)
                            .add(device.type)
                            .add(device.name)
                    } else {
                        // INSERT: name, namespace, node_id, config, enabled, type
                        JsonArray()
                            .add(device.name)
                            .add(device.namespace)
                            .add(device.nodeId)
                            .add(configJson.encode())
                            .add(if (device.enabled) 1 else 0)
                            .add(device.type)
                    }

                    sqliteClient.executeUpdate(sql, params)
                        .onSuccess {
                            logger.info("${if (exists) "Updated" else "Created"} device: ${device.name}")
                            val updatedDevice = device.copy(updatedAt = Instant.now())
                            promise.complete(updatedDevice)
                        }
                        .onFailure { error ->
                            logger.severe("Failed to save device ${device.name}: ${error.message}")
                            promise.fail(DeviceConfigException("Failed to save device", error))
                        }
                } catch (e: Exception) {
                    logger.severe("Failed to prepare device save ${device.name}: ${e.message}")
                    promise.fail(DeviceConfigException("Failed to save device", e))
                }
            } else {
                promise.fail(result.cause())
            }
        }

        return promise.future()
    }

    override fun deleteDevice(name: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        sqliteClient.executeUpdate(DELETE_DEVICE, JsonArray().add(name))
            .onSuccess { rowsAffected ->
                if (rowsAffected > 0) {
                    logger.info("Deleted device: $name")
                    promise.complete(true)
                } else {
                    logger.warning("Device not found for deletion: $name")
                    promise.complete(false)
                }
            }
            .onFailure { error ->
                logger.severe("Failed to delete device $name: ${error.message}")
                promise.fail(DeviceConfigException("Failed to delete device", error))
            }

        return promise.future()
    }

    override fun isNamespaceInUse(namespace: String, excludeName: String?): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        val sql = if (excludeName != null) COUNT_NAMESPACE_EXCLUDE else COUNT_NAMESPACE
        val params = if (excludeName != null) {
            JsonArray().add(namespace).add(excludeName)
        } else {
            JsonArray().add(namespace)
        }

        sqliteClient.executeQuery(sql, params)
            .onSuccess { results ->
                try {
                    if (results.size() > 0) {
                        val count = results.getJsonObject(0).getInteger("count", 0)
                        promise.complete(count > 0)
                    } else {
                        promise.complete(false)
                    }
                } catch (e: Exception) {
                    logger.severe("Failed to process namespace check: ${e.message}")
                    promise.fail(DeviceConfigException("Failed to check namespace", e))
                }
            }
            .onFailure { error ->
                logger.severe("Failed to check namespace usage: ${error.message}")
                promise.fail(DeviceConfigException("Failed to check namespace", error))
            }

        return promise.future()
    }

    override fun toggleDevice(name: String, enabled: Boolean): Future<DeviceConfig?> {
        val promise = Promise.promise<DeviceConfig?>()

        sqliteClient.executeUpdate(UPDATE_ENABLED, JsonArray().add(if (enabled) 1 else 0).add(name))
            .onSuccess { rowsAffected ->
                if (rowsAffected > 0) {
                    logger.info("Updated device $name enabled status to $enabled")
                    // Retrieve and return the updated device
                    getDevice(name).onComplete { result ->
                        if (result.succeeded()) {
                            promise.complete(result.result())
                        } else {
                            promise.fail(result.cause())
                        }
                    }
                } else {
                    logger.warning("Device not found for enabled update: $name")
                    promise.complete(null)
                }
            }
            .onFailure { error ->
                logger.severe("Failed to toggle device $name: ${error.message}")
                promise.fail(DeviceConfigException("Failed to toggle device", error))
            }

        return promise.future()
    }

    override fun reassignDevice(name: String, nodeId: String): Future<DeviceConfig?> {
        val promise = Promise.promise<DeviceConfig?>()

        sqliteClient.executeUpdate(UPDATE_NODE_ID, JsonArray().add(nodeId).add(name))
            .onSuccess { rowsAffected ->
                if (rowsAffected > 0) {
                    logger.info("Updated device $name nodeId to $nodeId")
                    // Retrieve and return the updated device
                    getDevice(name).onComplete { result ->
                        if (result.succeeded()) {
                            promise.complete(result.result())
                        } else {
                            promise.fail(result.cause())
                        }
                    }
                } else {
                    logger.warning("Device not found for nodeId update: $name")
                    promise.complete(null)
                }
            }
            .onFailure { error ->
                logger.severe("Failed to reassign device $name: ${error.message}")
                promise.fail(DeviceConfigException("Failed to reassign device", error))
            }

        return promise.future()
    }

    override fun close(): Future<Void> {
        val promise = Promise.promise<Void>()
        // SQLiteClient doesn't need explicit closing - the SQLiteVerticle manages connections
        logger.info("DeviceConfigStoreSQLite closed")
        promise.complete()
        return promise.future()
    }

    fun startHealthChecks(vertxInstance: Vertx = this.vertx) {
        if (healthCheckTimer != null) return // Already started
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
            // Test connection with a simple query
            sqliteClient.executeQuery("SELECT 1", JsonArray()).result()
            true // Connection is good
        } catch (e: Exception) {
            logger.fine("Connection check failed: ${e.message}")
            false
        }
    }

    private fun reconnect() {
        if (!reconnectOngoing) {
            reconnectOngoing = true
            vertx.executeBlocking(Callable {
                try {
                    logger.info("Attempting to reconnect to SQLite...")
                    // Recreate the SQLiteClient
                    sqliteClient = SQLiteClient(vertx, dbPath)
                    logger.info("Successfully reconnected to SQLite")
                    reconnectOngoing = false
                } catch (e: Exception) {
                    logger.warning("Reconnection failed: ${e.message}. Will retry in ${defaultRetryWaitTime}ms")
                    reconnectOngoing = false
                    vertx.setTimer(defaultRetryWaitTime) {
                        reconnect()
                    }
                }
            })
        }
    }

    private fun mapJsonToDeviceConfig(row: JsonObject): DeviceConfig {
        val configJson = JsonObject(row.getString("config"))

        // Parse SQLite datetime strings to Instant
        val createdAtStr = row.getString("created_at")
        val updatedAtStr = row.getString("updated_at")

        val createdAt = try {
            if (createdAtStr != null) {
                // Convert SQLite datetime to ISO format and parse
                val isoFormat = "${createdAtStr.replace(' ', 'T')}Z"
                Instant.parse(isoFormat)
            } else {
                Instant.now()
            }
        } catch (e: Exception) {
            Instant.now()
        }

        val updatedAt = try {
            if (updatedAtStr != null) {
                // Convert SQLite datetime to ISO format and parse
                val isoFormat = "${updatedAtStr.replace(' ', 'T')}Z"
                Instant.parse(isoFormat)
            } else {
                Instant.now()
            }
        } catch (e: Exception) {
            Instant.now()
        }

        return DeviceConfig(
            name = row.getString("name"),
            namespace = row.getString("namespace"),
            nodeId = row.getString("node_id"),
            config = configJson,
            enabled = row.getInteger("enabled") == 1,
            type = row.getString("type") ?: DeviceConfig.DEVICE_TYPE_OPCUA_CLIENT,
            createdAt = createdAt,
            updatedAt = updatedAt
        )
    }

    override fun exportConfigs(names: List<String>?): Future<List<Map<String, Any?>>> {
        val promise = Promise.promise<List<Map<String, Any?>>>()

        try {
            val query = if (names.isNullOrEmpty()) {
                "SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at FROM $TABLE_NAME ORDER BY name"
            } else {
                val placeholders = names.joinToString(",") { "?" }
                "SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at FROM $TABLE_NAME WHERE name IN ($placeholders) ORDER BY name"
            }

            val params = JsonArray((names ?: emptyList()).map { it as Any })
            sqliteClient.executeQuery(query, params).onComplete { result ->
                if (result.succeeded()) {
                    val rowArray = result.result()
                    val configs = rowArray.map { row ->
                        val rowObj = row as? JsonObject ?: JsonObject()
                        val configStr = rowObj.getString("config", "{}")
                        val configObj = try {
                            JsonObject(configStr).map
                        } catch (e: Exception) {
                            emptyMap<String, Any?>()
                        }

                        mapOf(
                            "name" to rowObj.getString("name"),
                            "namespace" to rowObj.getString("namespace"),
                            "nodeId" to rowObj.getString("node_id"),
                            "config" to configObj,
                            "enabled" to (rowObj.getInteger("enabled", 1) == 1),
                            "type" to (rowObj.getString("type") ?: DeviceConfig.DEVICE_TYPE_OPCUA_CLIENT),
                            "createdAt" to rowObj.getString("created_at", ""),
                            "updatedAt" to rowObj.getString("updated_at", "")
                        )
                    }
                    promise.complete(configs)
                } else {
                    logger.severe("Failed to export device configs: ${result.cause().message}")
                    promise.fail(result.cause())
                }
            }
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
                    val enabled = if ((configMap["enabled"] as? Boolean) == true) 1 else 0

                    @Suppress("UNCHECKED_CAST")
                    val configObj = when (val cfg = configMap["config"]) {
                        is JsonObject -> cfg
                        is Map<*, *> -> JsonObject(cfg as Map<String, Any?>)
                        else -> throw IllegalArgumentException("Config must be a JSON object for device $name")
                    }

                    val query = """
                        INSERT OR REPLACE INTO $TABLE_NAME
                        (name, namespace, node_id, config, enabled, type, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                    """

                    val params = JsonArray()
                        .add(name)
                        .add(namespace)
                        .add(nodeId)
                        .add(configObj.encode())
                        .add(enabled)
                        .add(type)

                    sqliteClient.executeUpdate(query, params).onComplete { result ->
                        if (result.succeeded()) {
                            imported++
                        } else {
                            val errorMsg = "Failed to import config at index $index: ${result.cause().message}"
                            logger.warning(errorMsg)
                            errors.add(errorMsg)
                        }
                    }
                } catch (e: Exception) {
                    val errorMsg = "Failed to import config at index $index: ${e.message}"
                    logger.warning(errorMsg)
                    errors.add(errorMsg)
                }
            }

            // Wait a bit for all inserts to complete
            vertx.setTimer(100) {
                val failed = configs.size - imported
                val result = if (errors.isEmpty()) {
                    ImportDeviceConfigResult.success(imported)
                } else if (imported > 0) {
                    ImportDeviceConfigResult.partial(imported, failed, errors)
                } else {
                    ImportDeviceConfigResult.failure(errors)
                }
                promise.complete(result)
            }
        } catch (e: Exception) {
            logger.severe("Failed to import device configs: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }
}