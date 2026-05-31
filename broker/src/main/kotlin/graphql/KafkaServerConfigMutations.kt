package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Features
import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigRequest
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.devices.kafkaserver.KafkaServerConfig
import at.rocworks.devices.kafkaserver.KafkaServerExtension
import at.rocworks.devices.kafkaserver.KafkaStreamMapping
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

class KafkaServerConfigMutations(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    companion object {
        private val logger: Logger = Utils.getLogger(KafkaServerConfigMutations::class.java)
    }

    private val currentNodeId = Monster.getClusterNodeId(vertx)

    fun createKafkaServer(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            if (!Monster.isFeatureEnabled(Features.KafkaServer)) {
                return@DataFetcher future.apply { complete(mapOf("success" to false, "errors" to listOf("KafkaServer feature is not enabled on this node"))) }
            }

            try {
                val input = env.getArgument<Map<String, Any>>("input")
                    ?: return@DataFetcher future.apply { complete(mapOf("success" to false, "errors" to listOf("Input is required"))) }

                val request = parseDeviceConfigRequest(input)
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
                            future.complete(mapOf(
                                "success" to true,
                                "server" to deviceToMap(savedDevice),
                                "errors" to emptyList<String>()
                            ))
                        } else {
                            future.complete(mapOf("success" to false, "errors" to listOf("Failed to save device: ${saveResult.cause()?.message}")))
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error creating Kafka server: ${e.message}")
                future.complete(mapOf("success" to false, "errors" to listOf("Failed to create server: ${e.message}")))
            }
            future
        }
    }

    fun updateKafkaServer(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            if (!Monster.isFeatureEnabled(Features.KafkaServer)) {
                return@DataFetcher future.apply { complete(mapOf("success" to false, "errors" to listOf("KafkaServer feature is not enabled on this node"))) }
            }

            try {
                val name = env.getArgument<String>("name")
                val input = env.getArgument<Map<String, Any>>("input")
                if (name == null || input == null) {
                    future.complete(mapOf("success" to false, "errors" to listOf("Name and input are required")))
                    return@DataFetcher future
                }

                deviceStore.getDevice(name).onComplete { existingResult ->
                    if (existingResult.failed()) {
                        future.complete(mapOf("success" to false, "errors" to listOf("Database error: ${existingResult.cause()?.message}")))
                        return@onComplete
                    }
                    val existingDevice = existingResult.result()
                    if (existingDevice == null) {
                        future.complete(mapOf("success" to false, "errors" to listOf("Device '$name' not found")))
                        return@onComplete
                    }

                    val request = parseDeviceConfigRequest(input)
                    val validationErrors = request.validate()
                    if (validationErrors.isNotEmpty()) {
                        future.complete(mapOf("success" to false, "errors" to validationErrors))
                        return@onComplete
                    }

                    val updatedDevice = request.toDeviceConfig().copy(
                        createdAt = existingDevice.createdAt,
                        updatedAt = Instant.now()
                    )
                    deviceStore.saveDevice(updatedDevice).onComplete { saveResult ->
                        if (saveResult.succeeded()) {
                            val savedDevice = saveResult.result()
                            notifyDeviceConfigChange("update", savedDevice)
                            future.complete(mapOf(
                                "success" to true,
                                "server" to deviceToMap(savedDevice),
                                "errors" to emptyList<String>()
                            ))
                        } else {
                            future.complete(mapOf("success" to false, "errors" to listOf("Failed to update device: ${saveResult.cause()?.message}")))
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error updating Kafka server: ${e.message}")
                future.complete(mapOf("success" to false, "errors" to listOf("Failed to update server: ${e.message}")))
            }
            future
        }
    }

    fun deleteKafkaServer(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Boolean>()
            if (!Monster.isFeatureEnabled(Features.KafkaServer)) {
                return@DataFetcher future.apply { complete(false) }
            }

            try {
                val name = env.getArgument<String>("name")
                if (name == null) {
                    future.complete(false)
                    return@DataFetcher future
                }

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

                    deviceStore.deleteDevice(name).onComplete { delResult ->
                        if (delResult.succeeded() && delResult.result()) {
                            val changeData = JsonObject().put("operation", "delete").put("deviceName", name)
                            vertx.eventBus().publish(KafkaServerExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                            logger.info("Deleted Kafka server device: $name")
                            future.complete(true)
                        } else {
                            future.complete(false)
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error deleting Kafka server: ${e.message}")
                future.complete(false)
            }
            future
        }
    }

    fun toggleKafkaServer(): DataFetcher<CompletableFuture<Map<String, Any>>> = DataFetcher { env ->
        val name = env.getArgument<String>("name")
        val enabled = env.getArgument<Boolean>("enabled")
        val future = CompletableFuture<Map<String, Any>>()
        
        if (!Monster.isFeatureEnabled(Features.KafkaServer)) {
            return@DataFetcher future.apply { complete(mapOf("success" to false, "errors" to listOf("KafkaServer feature is not enabled on this node"))) }
        }

        try {
            if (name == null || enabled == null) {
                future.complete(mapOf("success" to false, "errors" to listOf("Name and enabled are required")))
                return@DataFetcher future
            }

            deviceStore.toggleDevice(name, enabled).onComplete { result ->
                if (result.succeeded()) {
                    val updatedDevice = result.result()
                    if (updatedDevice != null) {
                        val changeData = JsonObject().put("operation", "toggle").put("deviceName", name).put("enabled", enabled)
                        vertx.eventBus().publish(KafkaServerExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                        logger.info("Toggled Kafka server $name to enabled=$enabled")
                        future.complete(mapOf(
                            "success" to true, 
                            "server" to deviceToMap(updatedDevice), 
                            "errors" to emptyList<String>()
                        ))
                    } else {
                        future.complete(mapOf("success" to false, "errors" to listOf("Device '$name' not found")))
                    }
                } else {
                    future.complete(mapOf("success" to false, "errors" to listOf("Failed to toggle device: ${result.cause()?.message}")))
                }
            }
        } catch (e: Exception) {
            logger.severe("Error toggling Kafka server: ${e.message}")
            future.complete(mapOf("success" to false, "errors" to listOf("Failed to toggle server: ${e.message}")))
        }
        future
    }

    fun reassignKafkaServer(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            if (!Monster.isFeatureEnabled(Features.KafkaServer)) {
                return@DataFetcher future.apply { complete(mapOf("success" to false, "errors" to listOf("KafkaServer feature is not enabled on this node"))) }
            }

            try {
                val name = env.getArgument<String>("name")
                val nodeId = env.getArgument<String>("nodeId")
                if (name == null || nodeId == null) {
                    future.complete(mapOf("success" to false, "errors" to listOf("Name and nodeId are required")))
                    return@DataFetcher future
                }

                val clusterNodes = Monster.getClusterNodeIds(vertx)
                if (!clusterNodes.contains(nodeId)) {
                    future.complete(mapOf("success" to false, "errors" to listOf("Cluster node '$nodeId' not found. Available nodes: ${clusterNodes.joinToString(", ")}")))
                    return@DataFetcher future
                }

                deviceStore.reassignDevice(name, nodeId).onComplete { result ->
                    if (result.succeeded()) {
                        val updatedDevice = result.result()
                        if (updatedDevice != null) {
                            val changeData = JsonObject().put("operation", "reassign").put("deviceName", name).put("nodeId", nodeId)
                            vertx.eventBus().publish(KafkaServerExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                            logger.info("Reassigned Kafka server $name to node $nodeId")
                            future.complete(mapOf(
                                "success" to true, 
                                "server" to deviceToMap(updatedDevice), 
                                "errors" to emptyList<String>()
                            ))
                        } else {
                            future.complete(mapOf("success" to false, "errors" to listOf("Device '$name' not found")))
                        }
                    } else {
                        future.complete(mapOf("success" to false, "errors" to listOf("Failed to reassign device: ${result.cause()?.message}")))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error reassigning Kafka server: ${e.message}")
                future.complete(mapOf("success" to false, "errors" to listOf("Failed to reassign server: ${e.message}")))
            }
            future
        }
    }

    private fun notifyDeviceConfigChange(operation: String, device: DeviceConfig) {
        val changeData = JsonObject()
            .put("operation", operation)
            .put("deviceName", device.name)
            .put("device", deviceToJson(device))
        vertx.eventBus().publish(KafkaServerExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
        logger.info("Notified Kafka Server device config change: $operation for server ${device.name}")
    }

    private fun parseDeviceConfigRequest(input: Map<String, Any>): DeviceConfigRequest {
        val name = input["name"] as String
        val namespace = input["namespace"] as String
        val nodeId = input["nodeId"] as String
        val enabled = input["enabled"] as? Boolean ?: true
        val host = input["host"] as? String ?: "0.0.0.0"
        val port = input["port"] as? Int ?: 9092
        val storeType = input["storeType"] as? String

        @Suppress("UNCHECKED_CAST")
        val streamsInput = input["streams"] as? List<Map<String, Any>> ?: emptyList()
        val streamsList = streamsInput.map { item ->
            KafkaStreamMapping(
                streamName = item["streamName"] as? String ?: "",
                topicFilter = item["topicFilter"] as String,
                retentionHours = item["retentionHours"] as? Int ?: 168,
                storeType = item["storeType"] as? String
            )
        }

        val config = KafkaServerConfig(
            name = name,
            namespace = namespace,
            nodeId = nodeId,
            enabled = enabled,
            host = host,
            port = port,
            storeType = storeType,
            streams = streamsList
        )

        val configJson = config.toJsonObject().apply {
            remove("name")
            remove("namespace")
            remove("nodeId")
            remove("enabled")
        }

        return DeviceConfigRequest(
            name = name,
            namespace = namespace,
            nodeId = nodeId,
            config = configJson,
            enabled = enabled,
            type = DeviceConfig.DEVICE_TYPE_KAFKA_SERVER
        )
    }

    private fun deviceToJson(device: DeviceConfig): JsonObject = JsonObject()
        .put("name", device.name)
        .put("namespace", device.namespace)
        .put("nodeId", device.nodeId)
        .put("config", device.config)
        .put("enabled", device.enabled)
        .put("type", device.type)
        .put("createdAt", device.createdAt.toString())
        .put("updatedAt", device.updatedAt.toString())

    private fun deviceToMap(device: DeviceConfig): Map<String, Any?> {
        val config = try {
            KafkaServerConfig.fromJsonObject(device.config)
        } catch (e: Exception) {
            logger.severe("Failed to parse KafkaServerConfig for ${device.name}: ${e.message}")
            KafkaServerConfig()
        }

        val streamsList = config.streams.map { mapping ->
            mapOf(
                "streamName" to mapping.streamName,
                "topicFilter" to mapping.topicFilter,
                "retentionHours" to mapping.retentionHours,
                "storeType" to mapping.storeType
            )
        }

        val statusVal = if (device.enabled) "RUNNING" else "STOPPED"

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "enabled" to device.enabled,
            "host" to config.host,
            "port" to config.port,
            "storeType" to config.storeType,
            "streams" to streamsList,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString(),
            "isOnCurrentNode" to (device.nodeId == "*" || device.nodeId == currentNodeId),
            "status" to statusVal
        )
    }
}
