package at.rocworks.stores.postgres

import at.rocworks.Utils
import at.rocworks.devices.opcuaserver.OpcUaServerConfig
import at.rocworks.stores.IOpcUaServerConfigStore
import io.vertx.core.Future
import io.vertx.core.Promise
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant
import java.util.logging.Logger

/**
 * PostgreSQL implementation of OPC UA Server configuration store
 */
class OpcUaServerConfigStorePostgres(
    private val url: String,
    private val user: String,
    private val password: String
) : IOpcUaServerConfigStore {

    private val logger: Logger = Utils.getLogger(OpcUaServerConfigStorePostgres::class.java)
    private var connection: Connection? = null

    companion object {
        private const val TABLE_NAME = "opcua_server_configs"

        private val CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS $TABLE_NAME (
                name VARCHAR(255) PRIMARY KEY,
                namespace VARCHAR(255) NOT NULL,
                node_id VARCHAR(255) NOT NULL,
                enabled BOOLEAN DEFAULT true,
                port INTEGER NOT NULL DEFAULT 4840,
                path VARCHAR(255) NOT NULL DEFAULT 'server',
                namespace_index INTEGER NOT NULL DEFAULT 1,
                namespace_uri VARCHAR(255) NOT NULL DEFAULT 'urn:MonsterMQ:OpcUaServer',
                addresses JSONB NOT NULL DEFAULT '[]',
                security JSONB NOT NULL DEFAULT '{}',
                buffer_size INTEGER NOT NULL DEFAULT 1000,
                update_interval BIGINT NOT NULL DEFAULT 100,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),

                CONSTRAINT opcua_server_configs_namespace_format CHECK (namespace ~ '^[a-zA-Z0-9_/-]+$'),
                CONSTRAINT opcua_server_configs_name_format CHECK (name ~ '^[a-zA-Z0-9_-]+$')
            )
        """

        private const val CREATE_INDEXES = """
            CREATE INDEX IF NOT EXISTS idx_opcua_server_configs_node_id ON $TABLE_NAME (node_id);
            CREATE INDEX IF NOT EXISTS idx_opcua_server_configs_enabled ON $TABLE_NAME (enabled);
            CREATE INDEX IF NOT EXISTS idx_opcua_server_configs_namespace ON $TABLE_NAME (namespace);
        """

        private const val SELECT_ALL = """
            SELECT name, namespace, node_id, enabled, port, path, namespace_index, namespace_uri,
                   addresses, security, buffer_size, update_interval, created_at, updated_at
            FROM $TABLE_NAME
            ORDER BY name
        """

        private const val SELECT_BY_NODE = """
            SELECT name, namespace, node_id, enabled, port, path, namespace_index, namespace_uri,
                   addresses, security, buffer_size, update_interval, created_at, updated_at
            FROM $TABLE_NAME
            WHERE node_id = ?
            ORDER BY name
        """

        private const val SELECT_ENABLED_BY_NODE = """
            SELECT name, namespace, node_id, enabled, port, path, namespace_index, namespace_uri,
                   addresses, security, buffer_size, update_interval, created_at, updated_at
            FROM $TABLE_NAME
            WHERE node_id = ? AND enabled = true
            ORDER BY name
        """

        private const val SELECT_BY_NAME = """
            SELECT name, namespace, node_id, enabled, port, path, namespace_index, namespace_uri,
                   addresses, security, buffer_size, update_interval, created_at, updated_at
            FROM $TABLE_NAME
            WHERE name = ?
        """

        private const val INSERT_OR_UPDATE = """
            INSERT INTO $TABLE_NAME (name, namespace, node_id, enabled, port, path, namespace_index,
                                    namespace_uri, addresses, security, buffer_size, update_interval,
                                    created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?, ?, ?)
            ON CONFLICT (name) DO UPDATE SET
                namespace = EXCLUDED.namespace,
                node_id = EXCLUDED.node_id,
                enabled = EXCLUDED.enabled,
                port = EXCLUDED.port,
                path = EXCLUDED.path,
                namespace_index = EXCLUDED.namespace_index,
                namespace_uri = EXCLUDED.namespace_uri,
                addresses = EXCLUDED.addresses,
                security = EXCLUDED.security,
                buffer_size = EXCLUDED.buffer_size,
                update_interval = EXCLUDED.update_interval,
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
                    stmt.execute(CREATE_INDEXES)
                }
            }
            // Reconnect for ongoing operations
            connection = DriverManager.getConnection(url, user, password)
            logger.info("OpcUaServerConfigStorePostgres initialized successfully")
            promise.complete()
        } catch (e: Exception) {
            logger.severe("Failed to initialize OpcUaServerConfigStorePostgres: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    override fun getAllConfigs(): Future<List<OpcUaServerConfig>> {
        val promise = Promise.promise<List<OpcUaServerConfig>>()

        try {
            connection!!.prepareStatement(SELECT_ALL).use { stmt ->
                stmt.executeQuery().use { rs ->
                    val configs = mutableListOf<OpcUaServerConfig>()
                    while (rs.next()) {
                        try {
                            configs.add(mapResultSetToConfig(rs))
                        } catch (e: Exception) {
                            logger.warning("Skipping invalid OPC UA server config record: ${e.message}")
                        }
                    }
                    promise.complete(configs)
                }
            }
        } catch (e: Exception) {
            logger.severe("Failed to get all OPC UA server configs: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    override fun getConfigsByNode(nodeId: String): Future<List<OpcUaServerConfig>> {
        val promise = Promise.promise<List<OpcUaServerConfig>>()

        try {
            connection!!.prepareStatement(SELECT_BY_NODE).use { stmt ->
                stmt.setString(1, nodeId)
                stmt.executeQuery().use { rs ->
                    val configs = mutableListOf<OpcUaServerConfig>()
                    while (rs.next()) {
                        try {
                            configs.add(mapResultSetToConfig(rs))
                        } catch (e: Exception) {
                            logger.warning("Skipping invalid OPC UA server config record for node $nodeId: ${e.message}")
                        }
                    }
                    promise.complete(configs)
                }
            }
        } catch (e: Exception) {
            logger.severe("Failed to get OPC UA server configs by node $nodeId: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    override fun getEnabledConfigsByNode(nodeId: String): Future<List<OpcUaServerConfig>> {
        val promise = Promise.promise<List<OpcUaServerConfig>>()

        try {
            connection!!.prepareStatement(SELECT_ENABLED_BY_NODE).use { stmt ->
                stmt.setString(1, nodeId)
                stmt.executeQuery().use { rs ->
                    val configs = mutableListOf<OpcUaServerConfig>()
                    while (rs.next()) {
                        try {
                            configs.add(mapResultSetToConfig(rs))
                        } catch (e: Exception) {
                            logger.warning("Skipping invalid enabled OPC UA server config for node $nodeId: ${e.message}")
                        }
                    }
                    promise.complete(configs)
                }
            }
        } catch (e: Exception) {
            logger.severe("Failed to get enabled OPC UA server configs by node $nodeId: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    override fun getConfig(name: String): Future<OpcUaServerConfig?> {
        val promise = Promise.promise<OpcUaServerConfig?>()

        try {
            connection!!.prepareStatement(SELECT_BY_NAME).use { stmt ->
                stmt.setString(1, name)
                stmt.executeQuery().use { rs ->
                    if (rs.next()) {
                        try {
                            promise.complete(mapResultSetToConfig(rs))
                        } catch (e: Exception) {
                            logger.warning("Invalid OPC UA server config for name $name: ${e.message}")
                            promise.complete(null)
                        }
                    } else {
                        promise.complete(null)
                    }
                }
            }
        } catch (e: Exception) {
            logger.severe("Failed to get OPC UA server config $name: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    override fun saveConfig(config: OpcUaServerConfig): Future<OpcUaServerConfig> {
        val promise = Promise.promise<OpcUaServerConfig>()

        try {
            val now = Instant.now()
            val updatedConfig = config.copy(updatedAt = now)

            connection!!.prepareStatement(INSERT_OR_UPDATE).use { stmt ->
                stmt.setString(1, config.name)
                stmt.setString(2, config.namespace)
                stmt.setString(3, config.nodeId)
                stmt.setBoolean(4, config.enabled)
                stmt.setInt(5, config.port)
                stmt.setString(6, config.path)
                stmt.setInt(7, config.namespaceIndex)
                stmt.setString(8, config.namespaceUri)
                stmt.setString(9, config.addresses.map { it.toJsonObject() }.toString())
                stmt.setString(10, config.security.toJsonObject().toString())
                stmt.setInt(11, config.bufferSize)
                stmt.setLong(12, config.updateInterval)
                stmt.setTimestamp(13, Timestamp.from(config.createdAt))
                stmt.setTimestamp(14, Timestamp.from(now))

                val rowsAffected = stmt.executeUpdate()
                if (rowsAffected > 0) {
                    promise.complete(updatedConfig)
                } else {
                    promise.fail(Exception("No rows affected when saving OPC UA server config"))
                }
            }
        } catch (e: Exception) {
            logger.severe("Failed to save OPC UA server config ${config.name}: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    override fun deleteConfig(name: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        try {
            connection!!.prepareStatement(DELETE_BY_NAME).use { stmt ->
                stmt.setString(1, name)
                val rowsAffected = stmt.executeUpdate()
                promise.complete(rowsAffected > 0)
            }
        } catch (e: Exception) {
            logger.severe("Failed to delete OPC UA server config $name: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    override fun toggleConfig(name: String, enabled: Boolean): Future<OpcUaServerConfig?> {
        val promise = Promise.promise<OpcUaServerConfig?>()

        try {
            connection!!.prepareStatement(UPDATE_ENABLED).use { stmt ->
                stmt.setBoolean(1, enabled)
                stmt.setTimestamp(2, Timestamp.from(Instant.now()))
                stmt.setString(3, name)

                val rowsAffected = stmt.executeUpdate()
                if (rowsAffected > 0) {
                    getConfig(name).onComplete { result ->
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
            logger.severe("Failed to toggle OPC UA server config $name: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    override fun reassignConfig(name: String, nodeId: String): Future<OpcUaServerConfig?> {
        val promise = Promise.promise<OpcUaServerConfig?>()

        try {
            connection!!.prepareStatement(UPDATE_NODE_ID).use { stmt ->
                stmt.setString(1, nodeId)
                stmt.setTimestamp(2, Timestamp.from(Instant.now()))
                stmt.setString(3, name)

                val rowsAffected = stmt.executeUpdate()
                if (rowsAffected > 0) {
                    getConfig(name).onComplete { result ->
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
            logger.severe("Failed to reassign OPC UA server config $name: ${e.message}")
            promise.fail(e)
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

    private fun mapResultSetToConfig(rs: ResultSet): OpcUaServerConfig {
        return OpcUaServerConfig(
            name = rs.getString("name"),
            namespace = rs.getString("namespace"),
            nodeId = rs.getString("node_id"),
            enabled = rs.getBoolean("enabled"),
            port = rs.getInt("port"),
            path = rs.getString("path"),
            namespaceIndex = rs.getInt("namespace_index"),
            namespaceUri = rs.getString("namespace_uri"),
            addresses = emptyList(), // TODO: Parse addresses from JSONB
            security = at.rocworks.devices.opcuaserver.OpcUaServerSecurity(), // TODO: Parse security from JSONB
            bufferSize = rs.getInt("buffer_size"),
            updateInterval = rs.getLong("update_interval"),
            createdAt = rs.getTimestamp("created_at").toInstant(),
            updatedAt = rs.getTimestamp("updated_at").toInstant()
        )
    }
}