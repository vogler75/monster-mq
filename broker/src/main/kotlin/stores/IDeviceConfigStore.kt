package at.rocworks.devices.opcua

import io.vertx.core.Future
import at.rocworks.stores.DeviceConfig

/**
 * Interface for OPC UA device configuration storage
 */
interface IDeviceConfigStore {
    /**
     * Initialize the store (create tables, etc.)
     */
    fun initialize(): Future<Void>

    /**
     * Get all device configurations
     */
    fun getAllDevices(): Future<List<DeviceConfig>>

    /**
     * Get devices assigned to a specific cluster node
     */
    fun getDevicesByNode(nodeId: String): Future<List<DeviceConfig>>

    /**
     * Get a specific device configuration by name
     */
    fun getDevice(name: String): Future<DeviceConfig?>

    /**
     * Check if a namespace is already in use
     */
    fun isNamespaceInUse(namespace: String, excludeName: String? = null): Future<Boolean>

    /**
     * Add or update a device configuration
     */
    fun saveDevice(device: DeviceConfig): Future<DeviceConfig>

    /**
     * Delete a device configuration
     */
    fun deleteDevice(name: String): Future<Boolean>

    /**
     * Enable or disable a device
     */
    fun toggleDevice(name: String, enabled: Boolean): Future<DeviceConfig?>

    /**
     * Reassign device to a different cluster node
     */
    fun reassignDevice(name: String, nodeId: String): Future<DeviceConfig?>

    /**
     * Get enabled devices for a specific node
     */
    fun getEnabledDevicesByNode(nodeId: String): Future<List<DeviceConfig>>

    /**
     * Close the store and cleanup resources
     */
    fun close(): Future<Void>
}

/**
 * Exception thrown when device configuration operations fail
 */
class DeviceConfigException(message: String, cause: Throwable? = null) : Exception(message, cause)

/**
 * Result wrapper for device operations
 */
data class DeviceConfigResult(
    val success: Boolean,
    val device: DeviceConfig? = null,
    val errors: List<String> = emptyList()
) {
    companion object {
        fun success(device: DeviceConfig): DeviceConfigResult {
            return DeviceConfigResult(true, device)
        }

        fun failure(errors: List<String>): DeviceConfigResult {
            return DeviceConfigResult(false, null, errors)
        }

        fun failure(error: String): DeviceConfigResult {
            return DeviceConfigResult(false, null, listOf(error))
        }
    }
}