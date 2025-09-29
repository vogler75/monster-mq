package at.rocworks.stores

import at.rocworks.devices.opcuaserver.OpcUaServerConfig
import io.vertx.core.Future

/**
 * Interface for OPC UA Server configuration storage
 */
interface IOpcUaServerConfigStore {

    /**
     * Initialize the store
     */
    fun initialize(): Future<Void>

    /**
     * Get all OPC UA server configurations
     */
    fun getAllConfigs(): Future<List<OpcUaServerConfig>>

    /**
     * Get configurations by node ID
     */
    fun getConfigsByNode(nodeId: String): Future<List<OpcUaServerConfig>>

    /**
     * Get enabled configurations by node ID
     */
    fun getEnabledConfigsByNode(nodeId: String): Future<List<OpcUaServerConfig>>

    /**
     * Get configuration by name
     */
    fun getConfig(name: String): Future<OpcUaServerConfig?>

    /**
     * Save configuration
     */
    fun saveConfig(config: OpcUaServerConfig): Future<OpcUaServerConfig>

    /**
     * Delete configuration
     */
    fun deleteConfig(name: String): Future<Boolean>

    /**
     * Toggle enabled status
     */
    fun toggleConfig(name: String, enabled: Boolean): Future<OpcUaServerConfig?>

    /**
     * Reassign configuration to different node
     */
    fun reassignConfig(name: String, nodeId: String): Future<OpcUaServerConfig?>

    /**
     * Close the store
     */
    fun close(): Future<Void>
}