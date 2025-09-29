package at.rocworks.stores.postgres

import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigException
import at.rocworks.stores.IDeviceConfigStore
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant
import java.util.logging.Logger

/**
 * PostgreSQL implementation of DeviceConfigStore
 */
class DeviceConfigStorePostgres(
    private val url: String,
    private val user: String,
    private val password: String
) : IDeviceConfigStore {

    private val logger: Logger = Utils.getLogger(DeviceConfigStorePostgres::class.java)
    private var connection: Connection? = null

    companion object {
        private const val TABLE_NAME = "deviceconfigs"

        private val CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS $TABLE_NAME (
                name VARCHAR(255) PRIMARY KEY,
                namespace VARCHAR(255) NOT NULL,
                node_id VARCHAR(255) NOT NULL,
                config JSONB NOT NULL,
                enabled BOOLEAN DEFAULT true,
                type VARCHAR(255) DEFAULT '${DeviceConfig.DEVICE_TYPE_OPCUA_CLIENT}',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),

                CONSTRAINT deviceconfigs_namespace_format CHECK (namespace ~ '^[a-zA-Z0-9_/-]+$'),
                CONSTRAINT deviceconfigs_name_format CHECK (name ~ '^[a-zA-Z0-9_-]+$')
            )
        """

        private const val CREATE_INDEXES = """
            CREATE INDEX IF NOT EXISTS idx_deviceconfigs_node_id ON $TABLE_NAME (node_id);
            CREATE INDEX IF NOT EXISTS idx_deviceconfigs_enabled ON $TABLE_NAME (enabled);
            CREATE INDEX IF NOT EXISTS idx_deviceconfigs_namespace ON $TABLE_NAME (namespace);
        """

        private val MIGRATE_SCHEMA = """
            DO ${'$'}${'$'}
            BEGIN
                -- Add type column if it doesn't exist
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='$TABLE_NAME' AND column_name='type') THEN
                    ALTER TABLE $TABLE_NAME ADD COLUMN type VARCHAR(255) DEFAULT '${DeviceConfig.DEVICE_TYPE_OPCUA_CLIENT}';
                END IF;

                -- Drop unique constraint on namespace if it exists
                IF EXISTS (SELECT 1 FROM information_schema.constraint_column_usage WHERE table_name='$TABLE_NAME' AND constraint_name LIKE '%namespace%') THEN
                    ALTER TABLE $TABLE_NAME DROP CONSTRAINT IF EXISTS deviceconfigs_namespace_key;
                END IF;

                -- Drop backup_node_id column if it exists
                IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='$TABLE_NAME' AND column_name='backup_node_id') THEN
                    ALTER TABLE $TABLE_NAME DROP COLUMN backup_node_id;
                END IF;
            END
            ${'$'}${'$'};
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
            WHERE node_id = ? AND enabled = true
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
            VALUES (?, ?, ?, ?::jsonb, ?, ?, ?, ?)
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

        try {
            connection = DriverManager.getConnection(url, user, password)
            connection!!.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute(CREATE_TABLE)
                    stmt.execute(MIGRATE_SCHEMA)
                    stmt.execute(CREATE_INDEXES)
                }
            }
            // Reconnect for ongoing operations
            connection = DriverManager.getConnection(url, user, password)
            logger.info("DeviceConfigStorePostgres initialized successfully")
            promise.complete()
        } catch (e: Exception) {
            logger.severe("Failed to initialize DeviceConfigStorePostgres: ${e.message}")
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
        // Get all fields first to identify which one is null - using wasNull() to detect nulls safely
        val name = rs.getString("name")
        val nameWasNull = rs.wasNull()

        val namespace = rs.getString("namespace")
        val namespaceWasNull = rs.wasNull()

        val nodeId = rs.getString("node_id")
        val nodeIdWasNull = rs.wasNull()

        val configString = rs.getString("config")
        val configWasNull = rs.wasNull()

        val type = rs.getString("type")
        val typeWasNull = rs.wasNull()

        val createdAt = rs.getTimestamp("created_at")
        val createdAtWasNull = rs.wasNull()

        val updatedAt = rs.getTimestamp("updated_at")
        val updatedAtWasNull = rs.wasNull()

        // Log debug info for problematic records
        logger.info("Device record debug - name: $name (null: $nameWasNull), namespace: $namespace (null: $namespaceWasNull), nodeId: $nodeId (null: $nodeIdWasNull), config: ${if (configWasNull) "NULL" else "present"}, type: $type (null: $typeWasNull), createdAt: ${if (createdAtWasNull) "NULL" else createdAt}, updatedAt: ${if (updatedAtWasNull) "NULL" else updatedAt}")

        // Check for null values in critical fields

        if (nameWasNull) {
            throw DeviceConfigException("Name column is null for device")
        }

        if (namespaceWasNull) {
            throw DeviceConfigException("Namespace column is null for device name: $name")
        }

        if (nodeIdWasNull) {
            throw DeviceConfigException("Node ID column is null for device name: $name")
        }

        if (configWasNull) {
            throw DeviceConfigException("Config column is null for device name: $name")
        }

        if (createdAtWasNull) {
            throw DeviceConfigException("Created timestamp is null for device name: $name")
        }

        if (updatedAtWasNull) {
            throw DeviceConfigException("Updated timestamp is null for device name: $name")
        }

        val configJson = try {
            JsonObject(configString)
        } catch (e: Exception) {
            logger.severe("Failed to parse config JSON for device $name: ${e.message}")
            logger.severe("Config JSON content: $configString")
            throw DeviceConfigException("Failed to parse config JSON for device $name", e)
        }

        logger.info("Config JSON for device $name: $configString")

        return DeviceConfig(
            name = name!!,
            namespace = namespace!!,
            nodeId = nodeId!!,
            config = configJson,
            enabled = rs.getBoolean("enabled"),
            type = type ?: DeviceConfig.DEVICE_TYPE_OPCUA_CLIENT,
            createdAt = createdAt!!.toInstant(),
            updatedAt = updatedAt!!.toInstant()
        )
    }
}