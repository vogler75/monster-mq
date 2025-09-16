package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigRequest
import at.rocworks.devices.opcua.IDeviceConfigStore
import at.rocworks.stores.MonitoringParameters
import at.rocworks.stores.OpcUaAddress
import at.rocworks.stores.OpcUaConnectionConfig
import at.rocworks.devices.opcua.OpcUaExtension
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL mutations for OPC UA device configuration management
 */
class DeviceConfigMutations(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Logger.getLogger(DeviceConfigMutations::class.java.name)

    fun addOpcUaDevice(): DataFetcher<CompletableFuture<Map<String, Any>>> {
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
                                        "device" to deviceToMap(savedDevice),
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
                logger.severe("Error adding OPC UA device: ${e.message}")
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

    fun updateOpcUaDevice(): DataFetcher<CompletableFuture<Map<String, Any>>> {
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

                        // Update device (preserve creation time and existing addresses)
                        val updatedDevice = request.toDeviceConfig().copy(
                            createdAt = existingDevice.createdAt,
                            config = request.config.copy(addresses = existingDevice.config.addresses)
                        )
                        deviceStore.saveDevice(updatedDevice).onComplete { saveResult ->
                            if (saveResult.succeeded()) {
                                val savedDevice = saveResult.result()

                                // Notify extension about the change
                                notifyDeviceConfigChange("update", savedDevice)

                                future.complete(
                                    mapOf(
                                        "success" to true,
                                        "device" to deviceToMap(savedDevice),
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
                logger.severe("Error updating OPC UA device: ${e.message}")
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

    fun deleteOpcUaDevice(): DataFetcher<CompletableFuture<Boolean>> {
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

                            vertx.eventBus().publish(OpcUaExtension.Companion.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                            logger.info("Deleted OPC UA device: $name")

                            future.complete(true)
                        } else {
                            future.complete(false)
                        }
                    }
                }

            } catch (e: Exception) {
                logger.severe("Error deleting OPC UA device: ${e.message}")
                future.complete(false)
            }

            future
        }
    }

    fun toggleOpcUaDevice(): DataFetcher<CompletableFuture<Map<String, Any>>> {
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

                            vertx.eventBus().publish(OpcUaExtension.Companion.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                            logger.info("Toggled OPC UA device $name to enabled=$enabled")

                            future.complete(
                                mapOf(
                                    "success" to true,
                                    "device" to deviceToMap(updatedDevice),
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
                logger.severe("Error toggling OPC UA device: ${e.message}")
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

    fun reassignOpcUaDevice(): DataFetcher<CompletableFuture<Map<String, Any>>> {
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

                            vertx.eventBus().publish(OpcUaExtension.Companion.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                            logger.info("Reassigned OPC UA device $name to node $nodeId")

                            future.complete(
                                mapOf(
                                    "success" to true,
                                    "device" to deviceToMap(updatedDevice),
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
                logger.severe("Error reassigning OPC UA device: ${e.message}")
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

    private fun notifyDeviceConfigChange(operation: String, device: DeviceConfig) {
        val changeData = JsonObject()
            .put("operation", operation)
            .put("deviceName", device.name)
            .put("device", device.toJsonObject())

        vertx.eventBus().publish(OpcUaExtension.Companion.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
        logger.info("Notified device config change: $operation for device ${device.name}")
    }

    private fun parseDeviceConfigRequest(input: Map<String, Any>): DeviceConfigRequest {
        val configMap = (input["config"] as? Map<*, *>)?.let { map ->
            @Suppress("UNCHECKED_CAST")
            map as Map<String, Any>
        } ?: throw IllegalArgumentException("Invalid or missing 'config' field")

        // Parse monitoring parameters
        val monitoringParams = (configMap["monitoringParameters"] as? Map<*, *>)?.let { params ->
            @Suppress("UNCHECKED_CAST")
            val paramsMap = params as Map<String, Any>
            MonitoringParameters(
                bufferSize = (paramsMap["bufferSize"] as? Number)?.toInt() ?: 100,
                samplingInterval = (paramsMap["samplingInterval"] as? Number)?.toDouble() ?: 0.0,
                discardOldest = paramsMap["discardOldest"] as? Boolean ?: false
            )
        } ?: MonitoringParameters()

        val config = OpcUaConnectionConfig(
            endpointUrl = configMap["endpointUrl"] as String,
            securityPolicy = configMap["securityPolicy"] as? String ?: "None",
            username = configMap["username"] as? String,
            password = configMap["password"] as? String,
            subscriptionSamplingInterval = (configMap["subscriptionSamplingInterval"] as? Number)?.toDouble() ?: 0.0,
            keepAliveFailuresAllowed = (configMap["keepAliveFailuresAllowed"] as? Number)?.toInt() ?: 3,
            reconnectDelay = (configMap["reconnectDelay"] as? Number)?.toLong() ?: 5000L,
            connectionTimeout = (configMap["connectionTimeout"] as? Number)?.toLong() ?: 10000L,
            requestTimeout = (configMap["requestTimeout"] as? Number)?.toLong() ?: 5000L,
            monitoringParameters = monitoringParams,
            addresses = emptyList() // Addresses are managed separately now
        )

        return DeviceConfigRequest(
            name = input["name"] as String,
            namespace = input["namespace"] as String,
            nodeId = input["nodeId"] as String,
            backupNodeId = input["backupNodeId"] as? String,
            config = config,
            enabled = input["enabled"] as? Boolean ?: true
        )
    }

    fun addOpcUaAddress(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val deviceName = env.getArgument<String>("deviceName")
                val addressStr = env.getArgument<String>("address")
                val topic = env.getArgument<String>("topic")
                val publishMode = env.getArgument<String>("publishMode") ?: "SINGLE"
                val removePath = env.getArgument<Boolean>("removePath") ?: true

                if (deviceName == null || addressStr == null || topic == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("Device name, address, and topic are required")
                        )
                    )
                    return@DataFetcher future
                }

                // Create the address object
                val address = OpcUaAddress(
                    address = addressStr,
                    topic = topic,
                    publishMode = publishMode,
                    removePath = removePath
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

                    // Check if address already exists
                    if (existingDevice.config.addresses.any { it.address == address.address }) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Address '${address.address}' already exists for device '$deviceName'")
                            )
                        )
                        return@onComplete
                    }

                    // Add the new address
                    val updatedAddresses = existingDevice.config.addresses + address
                    val updatedConfig = existingDevice.config.copy(addresses = updatedAddresses)
                    val updatedDevice = existingDevice.copy(config = updatedConfig, updatedAt = Instant.now())

                    deviceStore.saveDevice(updatedDevice).onComplete { saveResult ->
                        if (saveResult.succeeded()) {
                            val savedDevice = saveResult.result()

                            // Notify extension about the change
                            notifyDeviceConfigChange("addAddress", savedDevice)

                            future.complete(
                                mapOf(
                                    "success" to true,
                                    "device" to deviceToMap(savedDevice),
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
                logger.severe("Error adding OPC UA address: ${e.message}")
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

    fun deleteOpcUaAddress(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val deviceName = env.getArgument<String>("deviceName")
                val address = env.getArgument<String>("address")

                if (deviceName == null || address == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("Device name and address are required")
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

                    // Check if address exists
                    if (!existingDevice.config.addresses.any { it.address == address }) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Address '$address' not found for device '$deviceName'")
                            )
                        )
                        return@onComplete
                    }

                    // Remove the address
                    val updatedAddresses = existingDevice.config.addresses.filter { it.address != address }
                    val updatedConfig = existingDevice.config.copy(addresses = updatedAddresses)
                    val updatedDevice = existingDevice.copy(config = updatedConfig, updatedAt = Instant.now())

                    deviceStore.saveDevice(updatedDevice).onComplete { saveResult ->
                        if (saveResult.succeeded()) {
                            val savedDevice = saveResult.result()

                            // Notify extension about the change
                            notifyDeviceConfigChange("deleteAddress", savedDevice)

                            future.complete(
                                mapOf(
                                    "success" to true,
                                    "device" to deviceToMap(savedDevice),
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
                logger.severe("Error deleting OPC UA address: ${e.message}")
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

    private fun deviceToMap(device: DeviceConfig): Map<String, Any?> {
        val currentNodeId = Monster.Companion.getClusterNodeId(vertx) ?: "local"

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "backupNodeId" to device.backupNodeId,
            "config" to mapOf(
                "endpointUrl" to device.config.endpointUrl,
                "securityPolicy" to device.config.securityPolicy,
                "username" to device.config.username,
                "subscriptionSamplingInterval" to device.config.subscriptionSamplingInterval,
                "keepAliveFailuresAllowed" to device.config.keepAliveFailuresAllowed,
                "reconnectDelay" to device.config.reconnectDelay,
                "connectionTimeout" to device.config.connectionTimeout,
                "requestTimeout" to device.config.requestTimeout,
                "monitoringParameters" to mapOf(
                    "bufferSize" to device.config.monitoringParameters.bufferSize,
                    "samplingInterval" to device.config.monitoringParameters.samplingInterval,
                    "discardOldest" to device.config.monitoringParameters.discardOldest
                ),
                "addresses" to device.config.addresses.map { address ->
                    mapOf(
                        "address" to address.address,
                        "topic" to address.topic,
                        "publishMode" to address.publishMode,
                        "removePath" to address.removePath
                    )
                }
            ),
            "enabled" to device.enabled,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString(),
            "isOnCurrentNode" to device.isAssignedToNode(currentNodeId)
        )
    }
}