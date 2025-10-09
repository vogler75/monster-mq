package at.rocworks.graphql

import at.rocworks.Utils
import at.rocworks.Monster
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigRequest
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.Plc4xAddress
import at.rocworks.stores.devices.Plc4xAddressMode
import at.rocworks.stores.devices.Plc4xConnectionConfig
import at.rocworks.devices.plc4x.Plc4xExtension
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL mutations for PLC4X client configuration management
 */
class Plc4xClientConfigMutations(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(Plc4xClientConfigMutations::class.java)

    fun create(): DataFetcher<CompletableFuture<Map<String, Any>>> {
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

                    // Check if namespace is already in use
                    deviceStore.isNamespaceInUse(request.namespace).onComplete { namespaceResult ->
                        if (namespaceResult.failed()) {
                            future.complete(
                                mapOf(
                                    "success" to false,
                                    "errors" to listOf("Database error: ${namespaceResult.cause()?.message}")
                                )
                            )
                            return@onComplete
                        }

                        if (namespaceResult.result()) {
                            future.complete(
                                mapOf(
                                    "success" to false,
                                    "errors" to listOf("Namespace '${request.namespace}' is already in use")
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
                }

            } catch (e: Exception) {
                logger.severe("Error adding PLC4X device: ${e.message}")
                future.complete(
                    mapOf(
                        "success" to false,
                        "errors" to listOf("Failed to add device: ${e.message}")
                    )
                )
            }

            future
        }
    }

    fun update(): DataFetcher<CompletableFuture<Map<String, Any>>> {
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

                    // Check namespace conflict (exclude current device)
                    deviceStore.isNamespaceInUse(request.namespace, name).onComplete { namespaceResult ->
                        if (namespaceResult.failed()) {
                            future.complete(
                                mapOf(
                                    "success" to false,
                                    "errors" to listOf("Database error: ${namespaceResult.cause()?.message}")
                                )
                            )
                            return@onComplete
                        }

                        if (namespaceResult.result()) {
                            future.complete(
                                mapOf(
                                    "success" to false,
                                    "errors" to listOf("Namespace '${request.namespace}' is already in use by another device")
                                )
                            )
                            return@onComplete
                        }

                        // Parse existing config from JsonObject
                        val existingConfig = Plc4xConnectionConfig.fromJsonObject(existingDevice.config)
                        val requestConfig = Plc4xConnectionConfig.fromJsonObject(request.config)

                        // Update device (preserve creation time and existing addresses)
                        val newConfig = requestConfig.copy(
                            addresses = existingConfig.addresses
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
                }

            } catch (e: Exception) {
                logger.severe("Error updating PLC4X device: ${e.message}")
                future.complete(
                    mapOf(
                        "success" to false,
                        "errors" to listOf("Failed to update device: ${e.message}")
                    )
                )
            }

            future
        }
    }

    fun delete(): DataFetcher<CompletableFuture<Boolean>> {
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

                            vertx.eventBus().publish(Plc4xExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                            logger.info("Deleted PLC4X device: $name")

                            future.complete(true)
                        } else {
                            future.complete(false)
                        }
                    }
                }

            } catch (e: Exception) {
                logger.severe("Error deleting PLC4X device: ${e.message}")
                future.complete(false)
            }

            future
        }
    }

    fun start(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            toggle().get(env)
        }
    }

    fun stop(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            toggle().get(env)
        }
    }

    fun toggle(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val name = env.getArgument<String>("name")
                val enabled = env.getArgument<Boolean>("enabled")

                if (name == null || enabled == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("Name and enabled are required")
                        )
                    )
                    return@DataFetcher future
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

                            vertx.eventBus().publish(Plc4xExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                            logger.info("Toggled PLC4X device $name to enabled=$enabled")

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
                logger.severe("Error toggling PLC4X device: ${e.message}")
                future.complete(
                    mapOf(
                        "success" to false,
                        "errors" to listOf("Failed to toggle device: ${e.message}")
                    )
                )
            }

            future
        }
    }

    fun reassign(): DataFetcher<CompletableFuture<Map<String, Any>>> {
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
                val clusterNodes = Monster.Companion.getClusterNodeIds(vertx)
                if (!clusterNodes.contains(nodeId)) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf(
                                "Cluster node '$nodeId' not found. Available nodes: ${
                                    clusterNodes.joinToString(
                                        ", "
                                    )
                                }"
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

                            vertx.eventBus().publish(Plc4xExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                            logger.info("Reassigned PLC4X device $name to node $nodeId")

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
                logger.severe("Error reassigning PLC4X device: ${e.message}")
                future.complete(
                    mapOf(
                        "success" to false,
                        "errors" to listOf("Failed to reassign device: ${e.message}")
                    )
                )
            }

            future
        }
    }

    fun addAddress(): DataFetcher<CompletableFuture<Map<String, Any>>> {
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

                val addressName = inputMap["name"] as? String
                val addressStr = inputMap["address"] as? String
                val topic = inputMap["topic"] as? String
                val qos = (inputMap["qos"] as? Number)?.toInt() ?: 0
                val retained = inputMap["retained"] as? Boolean ?: false
                val scalingFactor = (inputMap["scalingFactor"] as? Number)?.toDouble()
                val offset = (inputMap["offset"] as? Number)?.toDouble()
                val deadband = (inputMap["deadband"] as? Number)?.toDouble()
                val publishOnChange = inputMap["publishOnChange"] as? Boolean ?: true
                val modeStr = inputMap["mode"] as? String
                val mode = Plc4xAddressMode.fromString(modeStr)
                val enabled = inputMap["enabled"] as? Boolean ?: true

                if (addressName == null || addressStr == null || topic == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("Name, address, and topic are required")
                        )
                    )
                    return@DataFetcher future
                }

                // Create the address object
                val address = Plc4xAddress(
                    name = addressName,
                    address = addressStr,
                    topic = topic,
                    qos = qos,
                    retained = retained,
                    scalingFactor = scalingFactor,
                    offset = offset,
                    deadband = deadband,
                    publishOnChange = publishOnChange,
                    mode = mode,
                    enabled = enabled
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
                    val existingConfig = Plc4xConnectionConfig.fromJsonObject(existingDevice.config)

                    // Check if address name already exists
                    if (existingConfig.addresses.any { it.name == address.name }) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Address name '${address.name}' already exists for device '$deviceName'")
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
                logger.severe("Error adding PLC4X address: ${e.message}")
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

    fun deleteAddress(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val deviceName = env.getArgument<String>("deviceName")
                val addressName = env.getArgument<String>("addressName")

                if (deviceName == null || addressName == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("Device name and address name are required")
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
                    val existingConfig = Plc4xConnectionConfig.fromJsonObject(existingDevice.config)

                    // Check if address exists
                    if (!existingConfig.addresses.any { it.name == addressName }) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Address '$addressName' not found for device '$deviceName'")
                            )
                        )
                        return@onComplete
                    }

                    // Remove the address
                    val updatedAddresses = existingConfig.addresses.filter { it.name != addressName }
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
                logger.severe("Error deleting PLC4X address: ${e.message}")
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

        vertx.eventBus().publish(Plc4xExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
        logger.info("Notified device config change: $operation for device ${device.name}")
    }

    private fun parseDeviceConfigRequest(input: Map<String, Any>): DeviceConfigRequest {
        val configMap = (input["config"] as? Map<*, *>)?.let { map ->
            @Suppress("UNCHECKED_CAST")
            map as Map<String, Any>
        } ?: throw IllegalArgumentException("Invalid or missing 'config' field")

        val config = Plc4xConnectionConfig(
            protocol = (configMap["protocol"] as? String) ?: throw IllegalArgumentException("Protocol is required"),
            connectionString = (configMap["connectionString"] as? String) ?: throw IllegalArgumentException("Connection string is required"),
            pollingInterval = (configMap["pollingInterval"] as? Number)?.toLong() ?: 1000L,
            reconnectDelay = (configMap["reconnectDelay"] as? Number)?.toLong() ?: 5000L,
            addresses = emptyList(), // Addresses are managed separately
            enabled = configMap["enabled"] as? Boolean ?: true
        )

        return DeviceConfigRequest(
            name = input["name"] as String,
            namespace = input["namespace"] as String,
            nodeId = input["nodeId"] as String,
            config = config.toJsonObject(),
            enabled = input["enabled"] as? Boolean ?: true,
            type = DeviceConfig.DEVICE_TYPE_PLC4X_CLIENT
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
        val currentNodeId = Monster.Companion.getClusterNodeId(vertx) ?: "local"

        // Parse config from JsonObject for PLC4X Client devices
        val config = Plc4xConnectionConfig.fromJsonObject(device.config)

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "config" to mapOf(
                "protocol" to config.protocol.uppercase(),
                "connectionString" to config.connectionString,
                "pollingInterval" to config.pollingInterval,
                "reconnectDelay" to config.reconnectDelay,
                "enabled" to config.enabled,
                "addresses" to config.addresses.map { address ->
                    mapOf(
                        "name" to address.name,
                        "address" to address.address,
                        "topic" to address.topic,
                        "qos" to address.qos,
                        "retained" to address.retained,
                        "scalingFactor" to address.scalingFactor,
                        "offset" to address.offset,
                        "deadband" to address.deadband,
                        "publishOnChange" to address.publishOnChange,
                        "mode" to address.mode.name,
                        "enabled" to address.enabled
                    )
                }
            ),
            "enabled" to device.enabled,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString(),
            "isOnCurrentNode" to device.isAssignedToNode(currentNodeId),
            "metrics" to emptyList<Map<String, Any>>(),
            "metricsHistory" to emptyList<Map<String, Any>>()
        )
    }
}
