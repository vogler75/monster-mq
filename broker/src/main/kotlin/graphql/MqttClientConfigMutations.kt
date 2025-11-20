package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.devices.mqttclient.MqttClientExtension
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigRequest
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.MqttClientAddress
import at.rocworks.stores.devices.MqttClientConnectionConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL mutations for MQTT client configuration management
 */
class MqttClientConfigMutations(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(MqttClientConfigMutations::class.java)

    fun createMqttClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
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
                logger.severe("Error creating MQTT client: ${e.message}")
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

    fun updateMqttClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
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
                    val existingConfig = MqttClientConnectionConfig.fromJsonObject(existingDevice.config)
                    val requestConfig = MqttClientConnectionConfig.fromJsonObject(request.config)

                    // Update device (preserve creation time, existing addresses, and passwords if not provided)
                    val newConfig = requestConfig.copy(
                        addresses = existingConfig.addresses,
                        // Preserve existing password if not provided in update
                        password = requestConfig.password ?: existingConfig.password
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
                logger.severe("Error updating MQTT client: ${e.message}")
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

    fun deleteMqttClient(): DataFetcher<CompletableFuture<Boolean>> {
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

                            vertx.eventBus().publish(MqttClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                            logger.info("Deleted MQTT client: $name")

                            future.complete(true)
                        } else {
                            future.complete(false)
                        }
                    }
                }

            } catch (e: Exception) {
                logger.severe("Error deleting MQTT client: ${e.message}")
                future.complete(false)
            }

            future
        }
    }

    fun startMqttClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val name = env.getArgument<String>("name")
            toggleMqttClient(name, true)
        }
    }

    fun stopMqttClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val name = env.getArgument<String>("name")
            toggleMqttClient(name, false)
        }
    }

    fun toggleMqttClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val name = env.getArgument<String>("name")
            val enabled = env.getArgument<Boolean>("enabled")
            toggleMqttClient(name, enabled)
        }
    }

    private fun toggleMqttClient(name: String?, enabled: Boolean?): CompletableFuture<Map<String, Any>> {
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

                        vertx.eventBus().publish(MqttClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                        logger.info("Toggled MQTT client $name to enabled=$enabled")

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
            logger.severe("Error toggling MQTT client: ${e.message}")
            future.complete(
                mapOf(
                    "success" to false,
                    "errors" to listOf("Failed to toggle client: ${e.message}")
                )
            )
        }

        return future
    }

    fun reassignMqttClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
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

                            vertx.eventBus().publish(MqttClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                            logger.info("Reassigned MQTT client $name to node $nodeId")

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
                logger.severe("Error reassigning MQTT client: ${e.message}")
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

    fun addMqttClientAddress(): DataFetcher<CompletableFuture<Map<String, Any>>> {
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

                val mode = inputMap["mode"] as? String
                val remoteTopic = inputMap["remoteTopic"] as? String
                val localTopic = inputMap["localTopic"] as? String
                val removePath = inputMap["removePath"] as? Boolean ?: true
                val qos = inputMap["qos"] as? Int ?: 0

                if (mode == null || remoteTopic == null || localTopic == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("Mode, remoteTopic, and localTopic are required")
                        )
                    )
                    return@DataFetcher future
                }

                // Create the address object
                val address = MqttClientAddress(
                    mode = mode,
                    remoteTopic = remoteTopic,
                    localTopic = localTopic,
                    removePath = removePath,
                    qos = qos
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
                    val existingConfig = MqttClientConnectionConfig.fromJsonObject(existingDevice.config)

                    // Check if address already exists (by remoteTopic)
                    if (existingConfig.addresses.any { it.remoteTopic == address.remoteTopic }) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Address with remoteTopic '${address.remoteTopic}' already exists for device '$deviceName'")
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
                logger.severe("Error adding MQTT client address: ${e.message}")
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

    fun updateMqttClientAddress(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val deviceName = env.getArgument<String>("deviceName")
                val remoteTopic = env.getArgument<String>("remoteTopic")
                val inputMap = env.getArgument<Map<String, Any>>("input")

                if (deviceName == null || remoteTopic == null || inputMap == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("Device name, remoteTopic, and input are required")
                        )
                    )
                    return@DataFetcher future
                }

                val mode = inputMap["mode"] as? String
                val newRemoteTopic = inputMap["remoteTopic"] as? String
                val localTopic = inputMap["localTopic"] as? String
                val removePath = inputMap["removePath"] as? Boolean ?: true
                val qos = inputMap["qos"] as? Int ?: 0

                if (mode == null || newRemoteTopic == null || localTopic == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("Mode, remoteTopic, and localTopic are required")
                        )
                    )
                    return@DataFetcher future
                }

                // Create the updated address object
                val updatedAddress = MqttClientAddress(
                    mode = mode,
                    remoteTopic = newRemoteTopic,
                    localTopic = localTopic,
                    removePath = removePath,
                    qos = qos
                )

                val validationErrors = updatedAddress.validate()
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
                    val existingConfig = MqttClientConnectionConfig.fromJsonObject(existingDevice.config)

                    // Check if the address to update exists
                    if (!existingConfig.addresses.any { it.remoteTopic == remoteTopic }) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Address with remoteTopic '$remoteTopic' not found for device '$deviceName'")
                            )
                        )
                        return@onComplete
                    }

                    // If remoteTopic changed, check that the new one doesn't already exist
                    if (remoteTopic != newRemoteTopic && existingConfig.addresses.any { it.remoteTopic == newRemoteTopic }) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Address with remoteTopic '$newRemoteTopic' already exists for device '$deviceName'")
                            )
                        )
                        return@onComplete
                    }

                    // Update the address
                    val updatedAddresses = existingConfig.addresses.map {
                        if (it.remoteTopic == remoteTopic) updatedAddress else it
                    }
                    val updatedConfig = existingConfig.copy(addresses = updatedAddresses)
                    val updatedDevice = existingDevice.copy(config = updatedConfig.toJsonObject(), updatedAt = Instant.now())

                    deviceStore.saveDevice(updatedDevice).onComplete { saveResult ->
                        if (saveResult.succeeded()) {
                            val savedDevice = saveResult.result()

                            // Notify extension about the change
                            notifyDeviceConfigChange("updateAddress", savedDevice)

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
                                    "errors" to listOf("Failed to update address: ${saveResult.cause()?.message}")
                                )
                            )
                        }
                    }
                }

            } catch (e: Exception) {
                logger.severe("Error updating MQTT client address: ${e.message}")
                future.complete(
                    mapOf(
                        "success" to false,
                        "errors" to listOf("Failed to update address: ${e.message}")
                    )
                )
            }

            future
        }
    }

    fun deleteMqttClientAddress(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val deviceName = env.getArgument<String>("deviceName")
                val remoteTopic = env.getArgument<String>("remoteTopic")

                if (deviceName == null || remoteTopic == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("Device name and remoteTopic are required")
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
                    val existingConfig = MqttClientConnectionConfig.fromJsonObject(existingDevice.config)

                    // Check if address exists
                    if (!existingConfig.addresses.any { it.remoteTopic == remoteTopic }) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Address with remoteTopic '$remoteTopic' not found for device '$deviceName'")
                            )
                        )
                        return@onComplete
                    }

                    // Remove the address
                    val updatedAddresses = existingConfig.addresses.filter { it.remoteTopic != remoteTopic }
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
                logger.severe("Error deleting MQTT client address: ${e.message}")
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

        vertx.eventBus().publish(MqttClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
        logger.info("Notified device config change: $operation for device ${device.name}")
    }

    private fun parseDeviceConfigRequest(input: Map<String, Any>): DeviceConfigRequest {
        val configMap = (input["config"] as? Map<*, *>)?.let { map ->
            @Suppress("UNCHECKED_CAST")
            map as Map<String, Any>
        } ?: throw IllegalArgumentException("Invalid or missing 'config' field")

        val config = MqttClientConnectionConfig(
            brokerUrl = configMap["brokerUrl"] as? String ?: throw IllegalArgumentException("brokerUrl is required"),
            username = configMap["username"] as? String,
            password = configMap["password"] as? String,
            clientId = configMap["clientId"] as? String ?: "monstermq-client",
            cleanSession = configMap["cleanSession"] as? Boolean ?: true,
            keepAlive = (configMap["keepAlive"] as? Number)?.toInt() ?: 60,
            reconnectDelay = (configMap["reconnectDelay"] as? Number)?.toLong() ?: 5000L,
            connectionTimeout = (configMap["connectionTimeout"] as? Number)?.toLong() ?: 30000L,
            addresses = emptyList(), // Addresses are managed separately
            bufferEnabled = configMap["bufferEnabled"] as? Boolean ?: false,
            bufferSize = (configMap["bufferSize"] as? Number)?.toInt() ?: 5000,
            persistBuffer = configMap["persistBuffer"] as? Boolean ?: false,
            deleteOldestMessages = configMap["deleteOldestMessages"] as? Boolean ?: true,
            loopPrevention = configMap["loopPrevention"] as? Boolean ?: true,
            sslVerifyCertificate = configMap["sslVerifyCertificate"] as? Boolean ?: true
        )

        return DeviceConfigRequest(
            name = input["name"] as String,
            namespace = input["namespace"] as String,
            nodeId = input["nodeId"] as String,
            config = config.toJsonObject(),
            enabled = input["enabled"] as? Boolean ?: true,
            type = DeviceConfig.DEVICE_TYPE_MQTT_CLIENT
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
        val currentNodeId = Monster.getClusterNodeId(vertx) ?: "local"

        // Parse config from JsonObject for MQTT Client devices
        val config = MqttClientConnectionConfig.fromJsonObject(device.config)

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "config" to mapOf(
                "brokerUrl" to config.brokerUrl,
                "username" to config.username,
                "clientId" to config.clientId,
                "cleanSession" to config.cleanSession,
                "keepAlive" to config.keepAlive,
                "reconnectDelay" to config.reconnectDelay,
                "connectionTimeout" to config.connectionTimeout,
                "bufferEnabled" to config.bufferEnabled,
                "bufferSize" to config.bufferSize,
                "persistBuffer" to config.persistBuffer,
                "deleteOldestMessages" to config.deleteOldestMessages,
                "sslVerifyCertificate" to config.sslVerifyCertificate,
                "addresses" to config.addresses.map { address ->
                    mapOf(
                        "mode" to address.mode,
                        "remoteTopic" to address.remoteTopic,
                        "localTopic" to address.localTopic,
                        "removePath" to address.removePath,
                        "qos" to address.qos
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