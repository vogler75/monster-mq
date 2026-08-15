package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Features
import at.rocworks.Utils
import at.rocworks.devices.i3xclient.I3xClientExtension
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigRequest
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.I3xAddress
import at.rocworks.stores.devices.I3xConnectionConfig
import at.rocworks.stores.devices.I3xHeader
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL mutations for i3X Client configuration management
 */
class I3xClientConfigMutations(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(I3xClientConfigMutations::class.java)

    fun createI3xClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            if (!Monster.isFeatureEnabled(Features.I3xClient))
                return@DataFetcher future.apply {
                    complete(mapOf("success" to false, "errors" to listOf("I3xClient feature is not enabled on this node")))
                }

            try {
                val input = env.getArgument<Map<String, Any>>("input")
                    ?: return@DataFetcher future.apply {
                        complete(mapOf("success" to false, "errors" to listOf("Input is required")))
                    }

                val request = parseDeviceConfigRequest(input)
                if (!Monster.getEnabledFeaturesForNode(request.nodeId).contains(Features.I3xClient))
                    return@DataFetcher future.apply {
                        complete(mapOf("success" to false, "errors" to listOf("I3xClient feature is not enabled on node ${request.nodeId}")))
                    }

                val validationErrors = request.validate()
                if (validationErrors.isNotEmpty()) {
                    future.complete(mapOf("success" to false, "errors" to validationErrors))
                    return@DataFetcher future
                }

                deviceStore.getDevice(request.name).onComplete { existingResult ->
                    if (existingResult.failed()) {
                        future.complete(mapOf("success" to false, "errors" to listOf("Database error: ${existingResult.cause()?.message}")))
                        return@onComplete
                    }

                    if (existingResult.result() != null) {
                        future.complete(mapOf("success" to false, "errors" to listOf("Device with name '${request.name}' already exists")))
                        return@onComplete
                    }

                    val device = request.toDeviceConfig()
                    deviceStore.saveDevice(device).onComplete { saveResult ->
                        if (saveResult.succeeded()) {
                            val savedDevice = saveResult.result()
                            notifyDeviceConfigChange("add", savedDevice)
                            val currentNodeId = Monster.getClusterNodeId(vertx)
                            future.complete(
                                mapOf(
                                    "success" to true,
                                    "client" to I3xClientConfigQueries.deviceToMap(savedDevice, currentNodeId),
                                    "errors" to emptyList<String>()
                                )
                            )
                        } else {
                            future.complete(mapOf("success" to false, "errors" to listOf("Failed to save device: ${saveResult.cause()?.message}")))
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error creating i3X client: ${e.message}")
                future.complete(mapOf("success" to false, "errors" to listOf("Internal error: ${e.message}")))
            }

            future
        }
    }

    fun updateI3xClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            if (!Monster.isFeatureEnabled(Features.I3xClient))
                return@DataFetcher future.apply {
                    complete(mapOf("success" to false, "errors" to listOf("I3xClient feature is not enabled on this node")))
                }

            try {
                val name = env.getArgument<String>("name")
                    ?: return@DataFetcher future.apply {
                        complete(mapOf("success" to false, "errors" to listOf("Name is required")))
                    }
                val input = env.getArgument<Map<String, Any>>("input")
                    ?: return@DataFetcher future.apply {
                        complete(mapOf("success" to false, "errors" to listOf("Input is required")))
                    }

                deviceStore.getDevice(name).onComplete { existingResult ->
                    if (existingResult.failed()) {
                        future.complete(mapOf("success" to false, "errors" to listOf("Database error: ${existingResult.cause()?.message}")))
                        return@onComplete
                    }

                    val existingDevice = existingResult.result()
                    if (existingDevice == null || existingDevice.type != DeviceConfig.DEVICE_TYPE_I3X_CLIENT) {
                        future.complete(mapOf("success" to false, "errors" to listOf("i3X client '$name' not found")))
                        return@onComplete
                    }

                    val existingConfig = I3xConnectionConfig.fromJsonObject(existingDevice.config)
                    val request = parseDeviceConfigRequest(input, existingConfig)
                    if (!Monster.getEnabledFeaturesForNode(request.nodeId).contains(Features.I3xClient)) {
                        future.complete(mapOf("success" to false, "errors" to listOf("I3xClient feature is not enabled on node ${request.nodeId}")))
                        return@onComplete
                    }

                    val validationErrors = request.validate()
                    if (validationErrors.isNotEmpty()) {
                        future.complete(mapOf("success" to false, "errors" to validationErrors))
                        return@onComplete
                    }

                    val updatedDevice = DeviceConfig(
                        name = name,
                        namespace = request.namespace,
                        nodeId = request.nodeId,
                        config = request.config,
                        enabled = request.enabled,
                        type = DeviceConfig.DEVICE_TYPE_I3X_CLIENT,
                        createdAt = existingDevice.createdAt,
                        updatedAt = Instant.now()
                    )

                    deviceStore.saveDevice(updatedDevice).onComplete { saveResult ->
                        if (saveResult.succeeded()) {
                            val savedDevice = saveResult.result()
                            notifyDeviceConfigChange("update", savedDevice)
                            val currentNodeId = Monster.getClusterNodeId(vertx)
                            future.complete(
                                mapOf(
                                    "success" to true,
                                    "client" to I3xClientConfigQueries.deviceToMap(savedDevice, currentNodeId),
                                    "errors" to emptyList<String>()
                                )
                            )
                        } else {
                            future.complete(mapOf("success" to false, "errors" to listOf("Failed to update device: ${saveResult.cause()?.message}")))
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error updating i3X client: ${e.message}")
                future.complete(mapOf("success" to false, "errors" to listOf("Internal error: ${e.message}")))
            }

            future
        }
    }

    fun deleteI3xClient(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Boolean>()
            if (!Monster.isFeatureEnabled(Features.I3xClient))
                return@DataFetcher future.apply { complete(false) }

            try {
                val name = env.getArgument<String>("name")
                    ?: return@DataFetcher future.apply { complete(false) }

                deviceStore.getDevice(name).onComplete { existingResult ->
                    if (existingResult.succeeded() && existingResult.result() != null) {
                        val device = existingResult.result()!!
                        if (device.type == DeviceConfig.DEVICE_TYPE_I3X_CLIENT) {
                            deviceStore.deleteDevice(name).onComplete { deleteResult ->
                                if (deleteResult.succeeded()) {
                                    notifyDeviceConfigChange("delete", device)
                                    future.complete(true)
                                } else {
                                    future.complete(false)
                                }
                            }
                        } else {
                            future.complete(false)
                        }
                    } else {
                        future.complete(false)
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error deleting i3X client: ${e.message}")
                future.complete(false)
            }

            future
        }
    }

    fun startI3xClient(): DataFetcher<CompletableFuture<Map<String, Any>>> = toggleI3xClientInternal(true)
    fun stopI3xClient(): DataFetcher<CompletableFuture<Map<String, Any>>> = toggleI3xClientInternal(false)

    fun toggleI3xClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val name = env.getArgument<String>("name")
            val enabled = env.getArgument<Boolean>("enabled") ?: true
            toggleDevice(name, enabled)
        }
    }

    private fun toggleI3xClientInternal(enabled: Boolean): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val name = env.getArgument<String>("name")
            toggleDevice(name, enabled)
        }
    }

    private fun toggleDevice(name: String?, enabled: Boolean): CompletableFuture<Map<String, Any>> {
        val future = CompletableFuture<Map<String, Any>>()
        if (!Monster.isFeatureEnabled(Features.I3xClient))
            return future.apply { complete(mapOf("success" to false, "errors" to listOf("I3xClient feature is not enabled on this node"))) }

        if (name == null) {
            future.complete(mapOf("success" to false, "errors" to listOf("Name is required")))
            return future
        }

        deviceStore.getDevice(name).onComplete { existingResult ->
            if (existingResult.failed()) {
                future.complete(mapOf("success" to false, "errors" to listOf("Database error: ${existingResult.cause()?.message}")))
                return@onComplete
            }

            val device = existingResult.result()
            if (device == null || device.type != DeviceConfig.DEVICE_TYPE_I3X_CLIENT) {
                future.complete(mapOf("success" to false, "errors" to listOf("i3X client '$name' not found")))
                return@onComplete
            }

            val updatedDevice = device.copy(enabled = enabled, updatedAt = Instant.now())
            deviceStore.saveDevice(updatedDevice).onComplete { saveResult ->
                if (saveResult.succeeded()) {
                    val saved = saveResult.result()
                    notifyDeviceConfigChange("toggle", saved, enabled)
                    val currentNodeId = Monster.getClusterNodeId(vertx)
                    future.complete(
                        mapOf(
                            "success" to true,
                            "client" to I3xClientConfigQueries.deviceToMap(saved, currentNodeId),
                            "errors" to emptyList<String>()
                        )
                    )
                } else {
                    future.complete(mapOf("success" to false, "errors" to listOf("Failed to update device state: ${saveResult.cause()?.message}")))
                }
            }
        }

        return future
    }

    fun reassignI3xClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            if (!Monster.isFeatureEnabled(Features.I3xClient))
                return@DataFetcher future.apply { complete(mapOf("success" to false, "errors" to listOf("I3xClient feature is not enabled on this node"))) }

            val name = env.getArgument<String>("name")
            val nodeId = env.getArgument<String>("nodeId")

            if (name == null || nodeId == null) {
                future.complete(mapOf("success" to false, "errors" to listOf("Name and nodeId are required")))
                return@DataFetcher future
            }

            deviceStore.getDevice(name).onComplete { existingResult ->
                if (existingResult.failed()) {
                    future.complete(mapOf("success" to false, "errors" to listOf("Database error: ${existingResult.cause()?.message}")))
                    return@onComplete
                }

                val device = existingResult.result()
                if (device == null || device.type != DeviceConfig.DEVICE_TYPE_I3X_CLIENT) {
                    future.complete(mapOf("success" to false, "errors" to listOf("i3X client '$name' not found")))
                    return@onComplete
                }

                val updatedDevice = device.copy(nodeId = nodeId, updatedAt = Instant.now())
                deviceStore.saveDevice(updatedDevice).onComplete { saveResult ->
                    if (saveResult.succeeded()) {
                        val saved = saveResult.result()
                        notifyDeviceConfigChange("reassign", saved, nodeId = nodeId)
                        val currentNodeId = Monster.getClusterNodeId(vertx)
                        future.complete(
                            mapOf(
                                "success" to true,
                                "client" to I3xClientConfigQueries.deviceToMap(saved, currentNodeId),
                                "errors" to emptyList<String>()
                            )
                        )
                    } else {
                        future.complete(mapOf("success" to false, "errors" to listOf("Failed to reassign device: ${saveResult.cause()?.message}")))
                    }
                }
            }

            future
        }
    }

    fun addI3xClientAddress(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            if (!Monster.isFeatureEnabled(Features.I3xClient))
                return@DataFetcher future.apply { complete(mapOf("success" to false, "errors" to listOf("I3xClient feature is not enabled on this node"))) }

            val deviceName = env.getArgument<String>("deviceName")
            val input = env.getArgument<Map<String, Any>>("input")

            if (deviceName == null || input == null) {
                future.complete(mapOf("success" to false, "errors" to listOf("deviceName and input are required")))
                return@DataFetcher future
            }

            val address = parseAddress(input)
            val validationErrors = address.validate()
            if (validationErrors.isNotEmpty()) {
                future.complete(mapOf("success" to false, "errors" to validationErrors))
                return@DataFetcher future
            }

            deviceStore.getDevice(deviceName).onComplete { existingResult ->
                if (existingResult.failed()) {
                    future.complete(mapOf("success" to false, "errors" to listOf("Database error: ${existingResult.cause()?.message}")))
                    return@onComplete
                }

                val device = existingResult.result()
                if (device == null || device.type != DeviceConfig.DEVICE_TYPE_I3X_CLIENT) {
                    future.complete(mapOf("success" to false, "errors" to listOf("i3X client '$deviceName' not found")))
                    return@onComplete
                }

                val config = I3xConnectionConfig.fromJsonObject(device.config)
                val updatedAddresses = config.addresses.filter { it.elementId != address.elementId }.toMutableList()
                updatedAddresses.add(address)

                val updatedConfig = config.copy(addresses = updatedAddresses)
                val updatedDevice = device.copy(config = updatedConfig.toJsonObject(), updatedAt = Instant.now())

                deviceStore.saveDevice(updatedDevice).onComplete { saveResult ->
                    if (saveResult.succeeded()) {
                        val saved = saveResult.result()
                        notifyDeviceConfigChange("addAddress", saved)
                        val currentNodeId = Monster.getClusterNodeId(vertx)
                        future.complete(
                            mapOf(
                                "success" to true,
                                "client" to I3xClientConfigQueries.deviceToMap(saved, currentNodeId),
                                "errors" to emptyList<String>()
                            )
                        )
                    } else {
                        future.complete(mapOf("success" to false, "errors" to listOf("Failed to add address: ${saveResult.cause()?.message}")))
                    }
                }
            }

            future
        }
    }

    fun updateI3xClientAddress(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            if (!Monster.isFeatureEnabled(Features.I3xClient))
                return@DataFetcher future.apply { complete(mapOf("success" to false, "errors" to listOf("I3xClient feature is not enabled on this node"))) }

            val deviceName = env.getArgument<String>("deviceName")
            val elementId = env.getArgument<String>("elementId")
            val input = env.getArgument<Map<String, Any>>("input")

            if (deviceName == null || elementId == null || input == null) {
                future.complete(mapOf("success" to false, "errors" to listOf("deviceName, elementId and input are required")))
                return@DataFetcher future
            }

            val address = parseAddress(input)
            val validationErrors = address.validate()
            if (validationErrors.isNotEmpty()) {
                future.complete(mapOf("success" to false, "errors" to validationErrors))
                return@DataFetcher future
            }

            deviceStore.getDevice(deviceName).onComplete { existingResult ->
                if (existingResult.failed()) {
                    future.complete(mapOf("success" to false, "errors" to listOf("Database error: ${existingResult.cause()?.message}")))
                    return@onComplete
                }

                val device = existingResult.result()
                if (device == null || device.type != DeviceConfig.DEVICE_TYPE_I3X_CLIENT) {
                    future.complete(mapOf("success" to false, "errors" to listOf("i3X client '$deviceName' not found")))
                    return@onComplete
                }

                val config = I3xConnectionConfig.fromJsonObject(device.config)
                val updatedAddresses = config.addresses.map {
                    if (it.elementId == elementId) address else it
                }

                val updatedConfig = config.copy(addresses = updatedAddresses)
                val updatedDevice = device.copy(config = updatedConfig.toJsonObject(), updatedAt = Instant.now())

                deviceStore.saveDevice(updatedDevice).onComplete { saveResult ->
                    if (saveResult.succeeded()) {
                        val saved = saveResult.result()
                        notifyDeviceConfigChange("updateAddress", saved)
                        val currentNodeId = Monster.getClusterNodeId(vertx)
                        future.complete(
                            mapOf(
                                "success" to true,
                                "client" to I3xClientConfigQueries.deviceToMap(saved, currentNodeId),
                                "errors" to emptyList<String>()
                            )
                        )
                    } else {
                        future.complete(mapOf("success" to false, "errors" to listOf("Failed to update address: ${saveResult.cause()?.message}")))
                    }
                }
            }

            future
        }
    }

    fun deleteI3xClientAddress(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            if (!Monster.isFeatureEnabled(Features.I3xClient))
                return@DataFetcher future.apply { complete(mapOf("success" to false, "errors" to listOf("I3xClient feature is not enabled on this node"))) }

            val deviceName = env.getArgument<String>("deviceName")
            val elementId = env.getArgument<String>("elementId")

            if (deviceName == null || elementId == null) {
                future.complete(mapOf("success" to false, "errors" to listOf("deviceName and elementId are required")))
                return@DataFetcher future
            }

            deviceStore.getDevice(deviceName).onComplete { existingResult ->
                if (existingResult.failed()) {
                    future.complete(mapOf("success" to false, "errors" to listOf("Database error: ${existingResult.cause()?.message}")))
                    return@onComplete
                }

                val device = existingResult.result()
                if (device == null || device.type != DeviceConfig.DEVICE_TYPE_I3X_CLIENT) {
                    future.complete(mapOf("success" to false, "errors" to listOf("i3X client '$deviceName' not found")))
                    return@onComplete
                }

                val config = I3xConnectionConfig.fromJsonObject(device.config)
                val updatedAddresses = config.addresses.filter { it.elementId != elementId }

                val updatedConfig = config.copy(addresses = updatedAddresses)
                val updatedDevice = device.copy(config = updatedConfig.toJsonObject(), updatedAt = Instant.now())

                deviceStore.saveDevice(updatedDevice).onComplete { saveResult ->
                    if (saveResult.succeeded()) {
                        val saved = saveResult.result()
                        notifyDeviceConfigChange("deleteAddress", saved)
                        val currentNodeId = Monster.getClusterNodeId(vertx)
                        future.complete(
                            mapOf(
                                "success" to true,
                                "client" to I3xClientConfigQueries.deviceToMap(saved, currentNodeId),
                                "errors" to emptyList<String>()
                            )
                        )
                    } else {
                        future.complete(mapOf("success" to false, "errors" to listOf("Failed to delete address: ${saveResult.cause()?.message}")))
                    }
                }
            }

            future
        }
    }

    private fun parseAddress(input: Map<String, Any>): I3xAddress {
        return I3xAddress(
            elementId = input["elementId"] as? String ?: "",
            topic = input["topic"] as? String ?: "",
            maxDepth = (input["maxDepth"] as? Number)?.toInt() ?: 1,
            retained = input["retained"] as? Boolean ?: false,
            qos = (input["qos"] as? Number)?.toInt() ?: 0,
            messageFormat = input["messageFormat"] as? String ?: I3xAddress.FORMAT_RAW_VALUE,
            removePath = input["removePath"] as? Boolean ?: false,
            description = input["description"] as? String ?: ""
        )
    }

    private fun parseDeviceConfigRequest(
        input: Map<String, Any>,
        existingConfig: I3xConnectionConfig? = null
    ): DeviceConfigRequest {
        val name = input["name"] as? String ?: ""
        val namespace = input["namespace"] as? String ?: ""
        val nodeId = input["nodeId"] as? String ?: "*"
        val enabled = input["enabled"] as? Boolean ?: true

        @Suppress("UNCHECKED_CAST")
        val configInput = input["config"] as? Map<String, Any> ?: emptyMap()

        val url = configInput["url"] as? String ?: existingConfig?.url ?: "http://localhost:3002/i3x/v1"
        val authType = configInput["authType"] as? String ?: existingConfig?.authType ?: I3xConnectionConfig.AUTH_TYPE_NONE

        val username = when {
            configInput.containsKey("username") -> configInput["username"] as? String
            else -> existingConfig?.username
        }

        val inputPassword = configInput["password"] as? String
        val password = when {
            inputPassword != null && inputPassword.isNotBlank() -> inputPassword
            else -> existingConfig?.password
        }

        val inputToken = configInput["token"] as? String
        val token = when {
            inputToken != null && inputToken.isNotBlank() -> inputToken
            else -> existingConfig?.token
        }

        val clientId = configInput["clientId"] as? String ?: existingConfig?.clientId ?: I3xConnectionConfig.DEFAULT_CLIENT_ID
        val reconnectDelay = (configInput["reconnectDelay"] as? Number)?.toLong() ?: existingConfig?.reconnectDelay ?: 5000L
        val connectionTimeout = (configInput["connectionTimeout"] as? Number)?.toLong() ?: existingConfig?.connectionTimeout ?: 10000L

        @Suppress("UNCHECKED_CAST")
        val headersList = (configInput["headers"] as? List<Map<String, Any>>)?.map { hMap ->
            I3xHeader(hMap["key"] as? String ?: "", hMap["value"] as? String ?: "")
        } ?: existingConfig?.headers ?: emptyList()

        @Suppress("UNCHECKED_CAST")
        val addressesList = (configInput["addresses"] as? List<Map<String, Any>>)?.map { addrMap ->
            parseAddress(addrMap)
        } ?: existingConfig?.addresses ?: emptyList()

        val connectionConfig = I3xConnectionConfig(
            url = url,
            authType = authType,
            username = username,
            password = password,
            token = token,
            headers = headersList,
            clientId = clientId,
            reconnectDelay = reconnectDelay,
            connectionTimeout = connectionTimeout,
            addresses = addressesList
        )

        return DeviceConfigRequest(
            name = name,
            namespace = namespace,
            nodeId = nodeId,
            config = connectionConfig.toJsonObject(),
            enabled = enabled,
            type = DeviceConfig.DEVICE_TYPE_I3X_CLIENT
        )
    }

    private fun notifyDeviceConfigChange(
        operation: String,
        device: DeviceConfig,
        enabled: Boolean? = null,
        nodeId: String? = null
    ) {
        val changeData = JsonObject()
            .put("operation", operation)
            .put("deviceName", device.name)
            .put("device", device.toJsonObject())

        if (enabled != null) changeData.put("enabled", enabled)
        if (nodeId != null) changeData.put("nodeId", nodeId)

        vertx.eventBus().publish(I3xClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
    }
}
