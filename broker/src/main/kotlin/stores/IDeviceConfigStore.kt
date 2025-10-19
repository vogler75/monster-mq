package at.rocworks.stores

import io.vertx.core.Future

/**
 * Interface for device configuration storage
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

    /**
     * Export device configurations
     * @param names Optional list of device names to export. If null/empty, export all devices.
     * @return List of device configurations as maps
     */
    fun exportConfigs(names: List<String>? = null): Future<List<Map<String, Any?>>>

    /**
     * Import device configurations
     * @param configs List of device configuration maps to import
     * @return ImportResult containing success status, counts, and error messages
     */
    fun importConfigs(configs: List<Map<String, Any?>>): Future<ImportDeviceConfigResult>
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

/**
 * Result wrapper for device configuration import operations
 */
data class ImportDeviceConfigResult(
    val success: Boolean,
    val imported: Int = 0,
    val failed: Int = 0,
    val errors: List<String> = emptyList()
) {
    companion object {
        fun success(imported: Int): ImportDeviceConfigResult {
            return ImportDeviceConfigResult(true, imported, 0)
        }

        fun partial(imported: Int, failed: Int, errors: List<String>): ImportDeviceConfigResult {
            return ImportDeviceConfigResult(false, imported, failed, errors)
        }

        fun failure(errors: List<String>): ImportDeviceConfigResult {
            return ImportDeviceConfigResult(false, 0, 0, errors)
        }

        fun failure(error: String): ImportDeviceConfigResult {
            return ImportDeviceConfigResult(false, 0, 0, listOf(error))
        }
    }
}