package at.rocworks.stores.cratedb

import at.rocworks.Utils
import at.rocworks.stores.DatabaseConnection
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigException
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.ImportDeviceConfigResult
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant
import java.util.logging.Logger

/**
 * CrateDB implementation of DeviceConfigStore with automatic reconnection on connection loss
 */
class DeviceConfigStoreCrateDB(
    private val url: String,
    private val user: String,
    private val password: String
) : DatabaseConnection(Utils.getLogger(DeviceConfigStoreCrateDB::class.java), url, user, password), IDeviceConfigStore {

    private val logger: Logger = Utils.getLogger(DeviceConfigStoreCrateDB::class.java)

    companion object {
        private const val TABLE_NAME = "deviceconfigs"

        private val CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS $TABLE_NAME (
                name STRING PRIMARY KEY,
                namespace STRING NOT NULL,
                node_id STRING NOT NULL,
                config VARCHAR NOT NULL,
                enabled BOOLEAN DEFAULT TRUE,
                type STRING DEFAULT '${DeviceConfig.DEVICE_TYPE_OPCUA_CLIENT}',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """.trimIndent()

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
            WHERE node_id = ? AND enabled = TRUE
            ORDER BY name
        """

        private const val SELECT_BY_NAME = """
            SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at
            FROM $TABLE_NAME
            WHERE name = ?
        """

        private const val CHECK_NAMESPACE = """
            SELECT COUNT(*) FROM $TABLE_NAME
            WHERE namespace = ? AND name != COALESCE(?, '')
        """

        private const val INSERT_OR_UPDATE = """
            INSERT INTO $TABLE_NAME (name, namespace, node_id, config, enabled, type, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (name) DO UPDATE SET
                namespace = EXCLUDED.namespace,
                node_id = EXCLUDED.node_id,
                config = EXCLUDED.config,
                enabled = EXCLUDED.enabled,
                type = EXCLUDED.type,
                updated_at = EXCLUDED.updated_at
        """

        private const val DELETE_BY_NAME = """
            DELETE FROM $TABLE_NAME WHERE name = ?
        """

        private const val UPDATE_ENABLED = """
            UPDATE $TABLE_NAME SET enabled = ?, updated_at = ? WHERE name = ?
        """

        private const val UPDATE_NODE_ID = """
            UPDATE $TABLE_NAME SET node_id = ?, updated_at = ? WHERE name = ?
        """
    }

    override fun initialize(): Future<Void> {
        val promise = Promise.promise<Void>()
        start(Vertx.currentContext().owner(), promise)
        return promise.future()
    }

    override fun init(connection: Connection): Future<Void> {
        val promise = Promise.promise<Void>()
        try {
            connection.createStatement().use { stmt ->
                stmt.execute(CREATE_TABLE)
            }
            logger.info("DeviceConfigStoreCrateDB initialized successfully")
            promise.complete()
        } catch (e: Exception) {
            logger.severe("Failed to initialize DeviceConfigStoreCrateDB: ${e.message}")
            promise.fail(DeviceConfigException("Failed to initialize database", e))
        }
        return promise.future()
    }

    override fun getAllDevices(): Future<List<DeviceConfig>> {
        val promise = Promise.promise<List<DeviceConfig>>()

        try {
            connection!!.prepareStatement(SELECT_ALL).use { stmt ->
                stmt.executeQuery().use { rs ->
                    val devices = mutableListOf<DeviceConfig>()
                    while (rs.next()) {
                        try {
                            devices.add(mapResultSetToDevice(rs))
                        } catch (e: DeviceConfigException) {
                            logger.warning("Skipping invalid device record: ${e.message}")
                            // Continue processing other records instead of failing completely
                        }
                    }
                    promise.complete(devices)
                }
            }
        } catch (e: Exception) {
            logger.severe("Failed to get all devices: ${e.message}")
            promise.fail(DeviceConfigException("Failed to get all devices", e))
        }

        return promise.future()
    }

    override fun getDevicesByNode(nodeId: String): Future<List<DeviceConfig>> {
        val promise = Promise.promise<List<DeviceConfig>>()

        try {
            connection!!.prepareStatement(SELECT_BY_NODE).use { stmt ->
                stmt.setString(1, nodeId)
                stmt.executeQuery().use { rs ->
                    val devices = mutableListOf<DeviceConfig>()
                    while (rs.next()) {
                        try {
                            devices.add(mapResultSetToDevice(rs))
                        } catch (e: DeviceConfigException) {
                            logger.warning("Skipping invalid device record for node $nodeId: ${e.message}")
                            // Continue processing other records instead of failing completely
                        }
                    }
                    promise.complete(devices)
                }
            }
        } catch (e: Exception) {
            logger.severe("Failed to get devices by node $nodeId: ${e.message}")
            promise.fail(DeviceConfigException("Failed to get devices by node", e))
        }

        return promise.future()
    }

    override fun getEnabledDevicesByNode(nodeId: String): Future<List<DeviceConfig>> {
        val promise = Promise.promise<List<DeviceConfig>>()

        try {
            connection!!.prepareStatement(SELECT_ENABLED_BY_NODE).use { stmt ->
                stmt.setString(1, nodeId)
                stmt.executeQuery().use { rs ->
                    val devices = mutableListOf<DeviceConfig>()
                    while (rs.next()) {
                        try {
                            devices.add(mapResultSetToDevice(rs))
                        } catch (e: DeviceConfigException) {
                            logger.warning("Skipping invalid enabled device record for node $nodeId: ${e.message}")
                            // Continue processing other records instead of failing completely
                        }
                    }
                    promise.complete(devices)
                }
            }
        } catch (e: Exception) {
            logger.severe("Failed to get enabled devices by node $nodeId: ${e.message}")
            promise.fail(DeviceConfigException("Failed to get enabled devices by node", e))
        }

        return promise.future()
    }

    override fun getDevice(name: String): Future<DeviceConfig?> {
        val promise = Promise.promise<DeviceConfig?>()

        try {
            connection!!.prepareStatement(SELECT_BY_NAME).use { stmt ->
                stmt.setString(1, name)
                stmt.executeQuery().use { rs ->
                    if (rs.next()) {
                        try {
                            promise.complete(mapResultSetToDevice(rs))
                        } catch (e: DeviceConfigException) {
                            logger.warning("Invalid device record for name $name: ${e.message}")
                            promise.complete(null) // Return null for invalid records
                        }
                    } else {
                        promise.complete(null)
                    }
                }
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
            connection!!.prepareStatement(CHECK_NAMESPACE).use { stmt ->
                stmt.setString(1, namespace)
                stmt.setString(2, excludeName)
                stmt.executeQuery().use { rs ->
                    if (rs.next()) {
                        val count = rs.getInt(1)
                        promise.complete(count > 0)
                    } else {
                        promise.complete(false)
                    }
                }
            }
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

            connection!!.prepareStatement(INSERT_OR_UPDATE).use { stmt ->
                stmt.setString(1, device.name)
                stmt.setString(2, device.namespace)
                stmt.setString(3, device.nodeId)
                stmt.setString(4, device.config.toString())
                stmt.setBoolean(5, device.enabled)
                stmt.setString(6, device.type)
                stmt.setTimestamp(7, Timestamp.from(device.createdAt))
                stmt.setTimestamp(8, Timestamp.from(now))

                val rowsAffected = stmt.executeUpdate()
                if (rowsAffected > 0) {
                    promise.complete(updatedDevice)
                } else {
                    promise.fail(DeviceConfigException("No rows affected when saving device"))
                }
            }
        } catch (e: Exception) {
            logger.severe("Failed to save device ${device.name}: ${e.message}")
            promise.fail(DeviceConfigException("Failed to save device", e))
        }

        return promise.future()
    }

    override fun deleteDevice(name: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        try {
            connection!!.prepareStatement(DELETE_BY_NAME).use { stmt ->
                stmt.setString(1, name)
                val rowsAffected = stmt.executeUpdate()
                promise.complete(rowsAffected > 0)
            }
        } catch (e: Exception) {
            logger.severe("Failed to delete device $name: ${e.message}")
            promise.fail(DeviceConfigException("Failed to delete device", e))
        }

        return promise.future()
    }

    override fun toggleDevice(name: String, enabled: Boolean): Future<DeviceConfig?> {
        val promise = Promise.promise<DeviceConfig?>()

        try {
            connection!!.prepareStatement(UPDATE_ENABLED).use { stmt ->
                stmt.setBoolean(1, enabled)
                stmt.setTimestamp(2, Timestamp.from(Instant.now()))
                stmt.setString(3, name)

                val rowsAffected = stmt.executeUpdate()
                if (rowsAffected > 0) {
                    // Return updated device
                    getDevice(name).onComplete { result ->
                        if (result.succeeded()) {
                            promise.complete(result.result())
                        } else {
                            promise.fail(result.cause())
                        }
                    }
                } else {
                    promise.complete(null)
                }
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
            connection!!.prepareStatement(UPDATE_NODE_ID).use { stmt ->
                stmt.setString(1, nodeId)
                stmt.setTimestamp(2, Timestamp.from(Instant.now()))
                stmt.setString(3, name)

                val rowsAffected = stmt.executeUpdate()
                if (rowsAffected > 0) {
                    // Return updated device
                    getDevice(name).onComplete { result ->
                        if (result.succeeded()) {
                            promise.complete(result.result())
                        } else {
                            promise.fail(result.cause())
                        }
                    }
                } else {
                    promise.complete(null)
                }
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
            connection?.close()
            promise.complete()
        } catch (e: Exception) {
            logger.warning("Error closing database connection: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    private fun mapResultSetToDevice(rs: ResultSet): DeviceConfig {
        val configString = rs.getString("config")
        if (configString == null) {
            throw DeviceConfigException("Config column is null for device")
        }
        val configJson = JsonObject(configString)

        val name = rs.getString("name")
        if (name == null) {
            throw DeviceConfigException("Name column is null for device")
        }

        val namespace = rs.getString("namespace")
        if (namespace == null) {
            throw DeviceConfigException("Namespace column is null for device")
        }

        val nodeId = rs.getString("node_id")
        if (nodeId == null) {
            throw DeviceConfigException("Node ID column is null for device")
        }

        return DeviceConfig(
            name = name,
            namespace = namespace,
            nodeId = nodeId,
            config = configJson,
            enabled = rs.getBoolean("enabled"),
            type = rs.getString("type") ?: DeviceConfig.DEVICE_TYPE_OPCUA_CLIENT,
            createdAt = rs.getTimestamp("created_at").toInstant(),
            updatedAt = rs.getTimestamp("updated_at").toInstant()
        )
    }

    override fun exportConfigs(names: List<String>?): Future<List<Map<String, Any?>>> {
        val promise = Promise.promise<List<Map<String, Any?>>>()

        try {
            val conn = connection ?: throw DeviceConfigException("Database connection not available")

            val query = if (names.isNullOrEmpty()) {
                "SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at FROM $TABLE_NAME ORDER BY name"
            } else {
                val placeholders = names.joinToString(",") { "?" }
                "SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at FROM $TABLE_NAME WHERE name IN ($placeholders) ORDER BY name"
            }

            val stmt = conn.prepareStatement(query)
            names?.forEachIndexed { index, name ->
                stmt.setString(index + 1, name)
            }

            val rs = stmt.executeQuery()
            val configs = mutableListOf<Map<String, Any?>>()

            while (rs.next()) {
                val configStr = rs.getString("config")
                val configMap = mapOf(
                    "name" to rs.getString("name"),
                    "namespace" to rs.getString("namespace"),
                    "nodeId" to rs.getString("node_id"),
                    "config" to if (configStr.startsWith("{")) JsonObject(configStr).map else configStr,
                    "enabled" to rs.getBoolean("enabled"),
                    "type" to rs.getString("type"),
                    "createdAt" to rs.getTimestamp("created_at").toInstant().toString(),
                    "updatedAt" to rs.getTimestamp("updated_at").toInstant().toString()
                )
                configs.add(configMap)
            }

            rs.close()
            stmt.close()

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
            val conn = connection ?: throw DeviceConfigException("Database connection not available")
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
                        is JsonObject -> cfg
                        is Map<*, *> -> JsonObject(cfg as Map<String, Any?>)
                        else -> throw IllegalArgumentException("Config must be a JSON object for device $name")
                    }

                    // Try to insert, update if exists
                    val stmt = conn.prepareStatement(
                        """INSERT INTO $TABLE_NAME (name, namespace, node_id, config, enabled, type, created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                           ON CONFLICT (name) DO UPDATE SET
                           namespace = ?, node_id = ?, config = ?, enabled = ?, type = ?, updated_at = ?"""
                    )

                    val now = Timestamp.from(Instant.now())

                    stmt.setString(1, name)
                    stmt.setString(2, namespace)
                    stmt.setString(3, nodeId)
                    stmt.setString(4, configObj.encode())
                    stmt.setBoolean(5, enabled)
                    stmt.setString(6, type)
                    stmt.setTimestamp(7, now)
                    stmt.setTimestamp(8, now)

                    // Conflict update values
                    stmt.setString(9, namespace)
                    stmt.setString(10, nodeId)
                    stmt.setString(11, configObj.encode())
                    stmt.setBoolean(12, enabled)
                    stmt.setString(13, type)
                    stmt.setTimestamp(14, now)

                    stmt.executeUpdate()
                    stmt.close()

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