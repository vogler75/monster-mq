package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.devices.winccua.WinCCUaExtension
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigRequest
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.WinCCUaAddress
import at.rocworks.stores.devices.WinCCUaAddressType
import at.rocworks.stores.devices.WinCCUaConnectionConfig
import at.rocworks.stores.devices.WinCCUaTransformConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL mutations for WinCC Unified client configuration management
 */
class WinCCUaClientConfigMutations(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(WinCCUaClientConfigMutations::class.java)

    fun createWinCCUaClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val input = env.getArgument<Map<String, Any>>("input")
                    ?: return@DataFetcher future.apply {
                        complete(mapOf("success" to false, "errors" to listOf("Input is required")))
                    }

                // Parse input
                val request = parseDeviceConfigRequest(input)
                val validationErrors = request.validate()

                if (validationErrors.isNotEmpty()) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to validationErrors
                        )
                    )
                    return@DataFetcher future
                }

                // Check if name already exists
                deviceStore.getDevice(request.name).onComplete { existingResult ->
                    if (existingResult.failed()) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Database error: ${existingResult.cause()?.message}")
                            )
                        )
                        return@onComplete
                    }

                    if (existingResult.result() != null) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Device with name '${request.name}' already exists")
                            )
                        )
                        return@onComplete
                    }

                    // Save device
                    val device = request.toDeviceConfig()
                    deviceStore.saveDevice(device).onComplete { saveResult ->
                        if (saveResult.succeeded()) {
                            val savedDevice = saveResult.result()

                            // Notify extension about the change
                            notifyDeviceConfigChange("add", savedDevice)

                            future.complete(
                                mapOf(
                                    "success" to true,
                                    "client" to deviceToMap(savedDevice),
                                    "errors" to emptyList<String>()
                                )
                            )
                        } else {
                            future.complete(
                                mapOf(
                                    "success" to false,
                                    "errors" to listOf("Failed to save device: ${saveResult.cause()?.message}")
                                )
                            )
                        }
                    }
                }

            } catch (e: Exception) {
                logger.severe("Error creating WinCC Unified client: ${e.message}")
                future.complete(
                    mapOf(
                        "success" to false,
                        "errors" to listOf("Failed to create client: ${e.message}")
                    )
                )
            }

            future
        }
    }

    fun updateWinCCUaClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val name = env.getArgument<String>("name")
                val input = env.getArgument<Map<String, Any>>("input")

                if (name == null || input == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("Name and input are required")
                        )
                    )
                    return@DataFetcher future
                }

                // Check if device exists
                deviceStore.getDevice(name).onComplete { existingResult ->
                    if (existingResult.failed()) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Database error: ${existingResult.cause()?.message}")
                            )
                        )
                        return@onComplete
                    }

                    val existingDevice = existingResult.result()
                    if (existingDevice == null) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Device '$name' not found")
                            )
                        )
                        return@onComplete
                    }

                    // Parse input
                    val request = parseDeviceConfigRequest(input)
                    val validationErrors = request.validate()

                    if (validationErrors.isNotEmpty()) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to validationErrors
                            )
                        )
                        return@onComplete
                    }

                    // Parse existing config from JsonObject
                    val existingConfig = WinCCUaConnectionConfig.fromJsonObject(existingDevice.config)
                    val requestConfig = WinCCUaConnectionConfig.fromJsonObject(request.config)

                    // Update device (preserve creation time, existing addresses, and passwords if not provided)
                    val newConfig = requestConfig.copy(
                        addresses = existingConfig.addresses,
                        // Preserve existing password if not provided in update
                        password = requestConfig.password.ifBlank { existingConfig.password }
                    )
                    val updatedDevice = request.toDeviceConfig().copy(
                        createdAt = existingDevice.createdAt,
                        config = newConfig.toJsonObject()
                    )
                    deviceStore.saveDevice(updatedDevice).onComplete { saveResult ->
                        if (saveResult.succeeded()) {
                            val savedDevice = saveResult.result()

                            // Notify extension about the change
                            notifyDeviceConfigChange("update", savedDevice)

                            future.complete(
                                mapOf(
                                    "success" to true,
                                    "client" to deviceToMap(savedDevice),
                                    "errors" to emptyList<String>()
                                )
                            )
                        } else {
                            future.complete(
                                mapOf(
                                    "success" to false,
                                    "errors" to listOf("Failed to update device: ${saveResult.cause()?.message}")
                                )
                            )
                        }
                    }
                }

            } catch (e: Exception) {
                logger.severe("Error updating WinCC Unified client: ${e.message}")
                future.complete(
                    mapOf(
                        "success" to false,
                        "errors" to listOf("Failed to update client: ${e.message}")
                    )
                )
            }

            future
        }
    }

    fun deleteWinCCUaClient(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Boolean>()

            try {
                val name = env.getArgument<String>("name")
                if (name == null) {
                    future.complete(false)
                    return@DataFetcher future
                }

                // Check if device exists
                deviceStore.getDevice(name).onComplete { existingResult ->
                    if (existingResult.failed()) {
                        future.complete(false)
                        return@onComplete
                    }

                    val existingDevice = existingResult.result()
                    if (existingDevice == null) {
                        future.complete(false)
                        return@onComplete
                    }

                    // Delete device
                    deviceStore.deleteDevice(name).onComplete { deleteResult ->
                        if (deleteResult.succeeded() && deleteResult.result()) {
                            // Notify extension about the change
                            val changeData = JsonObject()
                                .put("operation", "delete")
                                .put("deviceName", name)

                            vertx.eventBus().publish(WinCCUaExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                            logger.info("Deleted WinCC Unified client: $name")

                            future.complete(true)
                        } else {
                            future.complete(false)
                        }
                    }
                }

            } catch (e: Exception) {
                logger.severe("Error deleting WinCC Unified client: ${e.message}")
                future.complete(false)
            }

            future
        }
    }

    fun startWinCCUaClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val name = env.getArgument<String>("name")
            toggleWinCCUaClient(name, true)
        }
    }

    fun stopWinCCUaClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val name = env.getArgument<String>("name")
            toggleWinCCUaClient(name, false)
        }
    }

    fun toggleWinCCUaClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val name = env.getArgument<String>("name")
            val enabled = env.getArgument<Boolean>("enabled")
            toggleWinCCUaClient(name, enabled)
        }
    }

    private fun toggleWinCCUaClient(name: String?, enabled: Boolean?): CompletableFuture<Map<String, Any>> {
        val future = CompletableFuture<Map<String, Any>>()

        try {
            if (name == null || enabled == null) {
                future.complete(
                    mapOf(
                        "success" to false,
                        "errors" to listOf("Name and enabled are required")
                    )
                )
                return future
            }

            deviceStore.toggleDevice(name, enabled).onComplete { result ->
                if (result.succeeded()) {
                    val updatedDevice = result.result()
                    if (updatedDevice != null) {
                        // Notify extension about the change
                        val changeData = JsonObject()
                            .put("operation", "toggle")
                            .put("deviceName", name)
                            .put("enabled", enabled)

                        vertx.eventBus().publish(WinCCUaExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                        logger.info("Toggled WinCC Unified client $name to enabled=$enabled")

                        future.complete(
                            mapOf(
                                "success" to true,
                                "client" to deviceToMap(updatedDevice),
                                "errors" to emptyList<String>()
                            )
                        )
                    } else {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Device '$name' not found")
                            )
                        )
                    }
                } else {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("Failed to toggle device: ${result.cause()?.message}")
                        )
                    )
                }
            }

        } catch (e: Exception) {
            logger.severe("Error toggling WinCC Unified client: ${e.message}")
            future.complete(
                mapOf(
                    "success" to false,
                    "errors" to listOf("Failed to toggle client: ${e.message}")
                )
            )
        }

        return future
    }

    fun reassignWinCCUaClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val name = env.getArgument<String>("name")
                val nodeId = env.getArgument<String>("nodeId")

                if (name == null || nodeId == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("Name and nodeId are required")
                        )
                    )
                    return@DataFetcher future
                }

                // Validate node ID exists in cluster
                val clusterNodes = Monster.getClusterNodeIds(vertx)
                if (!clusterNodes.contains(nodeId)) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf(
                                "Cluster node '$nodeId' not found. Available nodes: ${clusterNodes.joinToString(", ")}"
                            )
                        )
                    )
                    return@DataFetcher future
                }

                deviceStore.reassignDevice(name, nodeId).onComplete { result ->
                    if (result.succeeded()) {
                        val updatedDevice = result.result()
                        if (updatedDevice != null) {
                            // Notify extension about the change
                            val changeData = JsonObject()
                                .put("operation", "reassign")
                                .put("deviceName", name)
                                .put("nodeId", nodeId)

                            vertx.eventBus().publish(WinCCUaExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                            logger.info("Reassigned WinCC Unified client $name to node $nodeId")

                            future.complete(
                                mapOf(
                                    "success" to true,
                                    "client" to deviceToMap(updatedDevice),
                                    "errors" to emptyList<String>()
                                )
                            )
                        } else {
                            future.complete(
                                mapOf(
                                    "success" to false,
                                    "errors" to listOf("Device '$name' not found")
                                )
                            )
                        }
                    } else {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Failed to reassign device: ${result.cause()?.message}")
                            )
                        )
                    }
                }

            } catch (e: Exception) {
                logger.severe("Error reassigning WinCC Unified client: ${e.message}")
                future.complete(
                    mapOf(
                        "success" to false,
                        "errors" to listOf("Failed to reassign client: ${e.message}")
                    )
                )
            }

            future
        }
    }

    fun addWinCCUaClientAddress(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val deviceName = env.getArgument<String>("deviceName")
                val inputMap = env.getArgument<Map<String, Any>>("input")

                if (deviceName == null || inputMap == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("Device name and input are required")
                        )
                    )
                    return@DataFetcher future
                }

                val typeStr = inputMap["type"] as? String ?: "TAG_VALUES"
                val type = try {
                    WinCCUaAddressType.valueOf(typeStr)
                } catch (e: Exception) {
                    WinCCUaAddressType.TAG_VALUES
                }
                val topic = inputMap["topic"] as? String
                val description = inputMap["description"] as? String ?: ""
                val retained = inputMap["retained"] as? Boolean ?: false

                if (topic == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("Topic is required")
                        )
                    )
                    return@DataFetcher future
                }

                // Parse type-specific fields
                val nameFilters = (inputMap["nameFilters"] as? List<*>)?.mapNotNull { it as? String }
                val includeQuality = inputMap["includeQuality"] as? Boolean ?: false
                val systemNames = (inputMap["systemNames"] as? List<*>)?.mapNotNull { it as? String }
                val filterString = inputMap["filterString"] as? String

                // Create the address object
                val address = WinCCUaAddress(
                    type = type,
                    topic = topic,
                    description = description,
                    retained = retained,
                    nameFilters = nameFilters,
                    includeQuality = includeQuality,
                    systemNames = systemNames,
                    filterString = filterString
                )

                val validationErrors = address.validate()
                if (validationErrors.isNotEmpty()) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to validationErrors
                        )
                    )
                    return@DataFetcher future
                }

                // Get the existing device
                deviceStore.getDevice(deviceName).onComplete { existingResult ->
                    if (existingResult.failed()) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Database error: ${existingResult.cause()?.message}")
                            )
                        )
                        return@onComplete
                    }

                    val existingDevice = existingResult.result()
                    if (existingDevice == null) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Device '$deviceName' not found")
                            )
                        )
                        return@onComplete
                    }

                    // Parse existing config from JsonObject
                    val existingConfig = WinCCUaConnectionConfig.fromJsonObject(existingDevice.config)

                    // Check if address already exists (by topic)
                    if (existingConfig.addresses.any { it.topic == address.topic }) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Address with topic '${address.topic}' already exists for device '$deviceName'")
                            )
                        )
                        return@onComplete
                    }

                    // Add the new address
                    val updatedAddresses = existingConfig.addresses + address
                    val updatedConfig = existingConfig.copy(addresses = updatedAddresses)
                    val updatedDevice = existingDevice.copy(config = updatedConfig.toJsonObject(), updatedAt = Instant.now())

                    deviceStore.saveDevice(updatedDevice).onComplete { saveResult ->
                        if (saveResult.succeeded()) {
                            val savedDevice = saveResult.result()

                            // Notify extension about the change
                            notifyDeviceConfigChange("addAddress", savedDevice)

                            future.complete(
                                mapOf(
                                    "success" to true,
                                    "client" to deviceToMap(savedDevice),
                                    "errors" to emptyList<String>()
                                )
                            )
                        } else {
                            future.complete(
                                mapOf(
                                    "success" to false,
                                    "errors" to listOf("Failed to add address: ${saveResult.cause()?.message}")
                                )
                            )
                        }
                    }
                }

            } catch (e: Exception) {
                logger.severe("Error adding WinCC Unified client address: ${e.message}")
                future.complete(
                    mapOf(
                        "success" to false,
                        "errors" to listOf("Failed to add address: ${e.message}")
                    )
                )
            }

            future
        }
    }

    fun deleteWinCCUaClientAddress(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val deviceName = env.getArgument<String>("deviceName")
                val topic = env.getArgument<String>("topic")

                if (deviceName == null || topic == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("Device name and topic are required")
                        )
                    )
                    return@DataFetcher future
                }

                // Get the existing device
                deviceStore.getDevice(deviceName).onComplete { existingResult ->
                    if (existingResult.failed()) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Database error: ${existingResult.cause()?.message}")
                            )
                        )
                        return@onComplete
                    }

                    val existingDevice = existingResult.result()
                    if (existingDevice == null) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Device '$deviceName' not found")
                            )
                        )
                        return@onComplete
                    }

                    // Parse existing config from JsonObject
                    val existingConfig = WinCCUaConnectionConfig.fromJsonObject(existingDevice.config)

                    // Check if address exists
                    if (!existingConfig.addresses.any { it.topic == topic }) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Address with topic '$topic' not found for device '$deviceName'")
                            )
                        )
                        return@onComplete
                    }

                    // Remove the address
                    val updatedAddresses = existingConfig.addresses.filter { it.topic != topic }
                    val updatedConfig = existingConfig.copy(addresses = updatedAddresses)
                    val updatedDevice = existingDevice.copy(config = updatedConfig.toJsonObject(), updatedAt = Instant.now())

                    deviceStore.saveDevice(updatedDevice).onComplete { saveResult ->
                        if (saveResult.succeeded()) {
                            val savedDevice = saveResult.result()

                            // Notify extension about the change
                            notifyDeviceConfigChange("deleteAddress", savedDevice)

                            future.complete(
                                mapOf(
                                    "success" to true,
                                    "client" to deviceToMap(savedDevice),
                                    "errors" to emptyList<String>()
                                )
                            )
                        } else {
                            future.complete(
                                mapOf(
                                    "success" to false,
                                    "errors" to listOf("Failed to delete address: ${saveResult.cause()?.message}")
                                )
                            )
                        }
                    }
                }

            } catch (e: Exception) {
                logger.severe("Error deleting WinCC Unified client address: ${e.message}")
                future.complete(
                    mapOf(
                        "success" to false,
                        "errors" to listOf("Failed to delete address: ${e.message}")
                    )
                )
            }

            future
        }
    }

    private fun notifyDeviceConfigChange(operation: String, device: DeviceConfig) {
        val changeData = JsonObject()
            .put("operation", operation)
            .put("deviceName", device.name)
            .put("device", deviceToJson(device))

        vertx.eventBus().publish(WinCCUaExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
        logger.info("Notified device config change: $operation for device ${device.name}")
    }

    private fun parseDeviceConfigRequest(input: Map<String, Any>): DeviceConfigRequest {
        val configMap = (input["config"] as? Map<*, *>)?.let { map ->
            @Suppress("UNCHECKED_CAST")
            map as Map<String, Any>
        } ?: throw IllegalArgumentException("Invalid or missing 'config' field")

        val transformConfigMap = configMap["transformConfig"] as? Map<*, *>
        val transformConfig = if (transformConfigMap != null) {
            @Suppress("UNCHECKED_CAST")
            val tcMap = transformConfigMap as Map<String, Any>
            WinCCUaTransformConfig(
                convertDotToSlash = tcMap["convertDotToSlash"] as? Boolean ?: true,
                convertUnderscoreToSlash = tcMap["convertUnderscoreToSlash"] as? Boolean ?: false,
                regexPattern = tcMap["regexPattern"] as? String,
                regexReplacement = tcMap["regexReplacement"] as? String
            )
        } else {
            WinCCUaTransformConfig()
        }

        val config = WinCCUaConnectionConfig(
            graphqlEndpoint = configMap["graphqlEndpoint"] as? String ?: "http://winccua:4000/graphql",
            websocketEndpoint = configMap["websocketEndpoint"] as? String,
            username = configMap["username"] as? String ?: throw IllegalArgumentException("username is required"),
            password = configMap["password"] as? String ?: "",
            reconnectDelay = (configMap["reconnectDelay"] as? Number)?.toLong() ?: 5000L,
            connectionTimeout = (configMap["connectionTimeout"] as? Number)?.toLong() ?: 10000L,
            addresses = emptyList(), // Addresses are managed separately
            transformConfig = transformConfig,
            messageFormat = configMap["messageFormat"] as? String ?: WinCCUaConnectionConfig.FORMAT_JSON_ISO
        )

        return DeviceConfigRequest(
            name = input["name"] as String,
            namespace = input["namespace"] as String,
            nodeId = input["nodeId"] as String,
            config = config.toJsonObject(),
            enabled = input["enabled"] as? Boolean ?: true,
            type = DeviceConfig.DEVICE_TYPE_WINCCUA_CLIENT
        )
    }

    private fun deviceToJson(device: DeviceConfig): JsonObject {
        return JsonObject()
            .put("name", device.name)
            .put("namespace", device.namespace)
            .put("nodeId", device.nodeId)
            .put("config", device.config)
            .put("enabled", device.enabled)
            .put("type", device.type)
            .put("createdAt", device.createdAt.toString())
            .put("updatedAt", device.updatedAt.toString())
    }

    private fun deviceToMap(device: DeviceConfig): Map<String, Any?> {
        val currentNodeId = Monster.getClusterNodeId(vertx)

        // Parse config from JsonObject for WinCC Unified Client devices
        val config = WinCCUaConnectionConfig.fromJsonObject(device.config)

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "config" to mapOf(
                "graphqlEndpoint" to config.graphqlEndpoint,
                "websocketEndpoint" to config.websocketEndpoint,
                "username" to config.username,
                "reconnectDelay" to config.reconnectDelay,
                "connectionTimeout" to config.connectionTimeout,
                "messageFormat" to config.messageFormat,
                "transformConfig" to mapOf(
                    "convertDotToSlash" to config.transformConfig.convertDotToSlash,
                    "convertUnderscoreToSlash" to config.transformConfig.convertUnderscoreToSlash,
                    "regexPattern" to config.transformConfig.regexPattern,
                    "regexReplacement" to config.transformConfig.regexReplacement
                ),
                "addresses" to config.addresses.map { address ->
                    val addressMap = mutableMapOf<String, Any?>(
                        "type" to address.type.name,
                        "topic" to address.topic,
                        "description" to address.description,
                        "retained" to address.retained
                    )
                    if (address.nameFilters != null) {
                        addressMap["nameFilters"] = address.nameFilters
                    }
                    if (address.type == WinCCUaAddressType.TAG_VALUES) {
                        addressMap["includeQuality"] = address.includeQuality
                    }
                    if (address.systemNames != null) {
                        addressMap["systemNames"] = address.systemNames
                    }
                    if (address.filterString != null) {
                        addressMap["filterString"] = address.filterString
                    }
                    addressMap
                }
            ),
            "enabled" to device.enabled,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString(),
            "isOnCurrentNode" to device.isAssignedToNode(currentNodeId)
        )
    }
}
