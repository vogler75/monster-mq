package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.devices.kafkaclient.KafkaClientExtension
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigRequest
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.KafkaClientConfig
import at.rocworks.stores.devices.PayloadFormat
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL mutations for Kafka client bridge configuration management
 * Mirrors structure of MqttClientConfigMutations but adapted to simplified GraphQL schema.
 */
class KafkaClientConfigMutations(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(KafkaClientConfigMutations::class.java)

    fun createKafkaClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            try {
                val input = env.getArgument<Map<String, Any>>("input")
                    ?: return@DataFetcher future.apply { complete(mapOf("success" to false, "errors" to listOf("Input is required"))) }

                val request = parseDeviceConfigRequest(input)
                val validationErrors = request.validate() + validateKafkaConfig(request.config)
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
                                "client" to deviceToMap(savedDevice),
                                "errors" to emptyList<String>()
                            ))
                        } else {
                            future.complete(mapOf("success" to false, "errors" to listOf("Failed to save device: ${saveResult.cause()?.message}")))
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error creating Kafka client: ${e.message}")
                future.complete(mapOf("success" to false, "errors" to listOf("Failed to create client: ${e.message}")))
            }
            future
        }
    }

    fun updateKafkaClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
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
                    val validationErrors = request.validate() + validateKafkaConfig(request.config)
                    if (validationErrors.isNotEmpty()) {
                        future.complete(mapOf("success" to false, "errors" to validationErrors))
                        return@onComplete
                    }

                    val existingConfig = KafkaClientConfig.fromJson(existingDevice.config)
                    val requestConfig = KafkaClientConfig.fromJson(request.config)
                    val newConfig = requestConfig
                    val updatedDevice = request.toDeviceConfig().copy(
                        createdAt = existingDevice.createdAt,
                        config = JsonObject.mapFrom(newConfig),
                        updatedAt = Instant.now()
                    )
                    deviceStore.saveDevice(updatedDevice).onComplete { saveResult ->
                        if (saveResult.succeeded()) {
                            val savedDevice = saveResult.result()
                            notifyDeviceConfigChange("update", savedDevice)
                            future.complete(mapOf(
                                "success" to true,
                                "client" to deviceToMap(savedDevice),
                                "errors" to emptyList<String>()
                            ))
                        } else {
                            future.complete(mapOf("success" to false, "errors" to listOf("Failed to update device: ${saveResult.cause()?.message}")))
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error updating Kafka client: ${e.message}")
                future.complete(mapOf("success" to false, "errors" to listOf("Failed to update client: ${e.message}")))
            }
            future
        }
    }

    fun deleteKafkaClient(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Boolean>()
            try {
                val name = env.getArgument<String>("name")
                if (name == null) { future.complete(false); return@DataFetcher future }
                deviceStore.getDevice(name).onComplete { existingResult ->
                    if (existingResult.failed()) { future.complete(false); return@onComplete }
                    val existingDevice = existingResult.result()
                    if (existingDevice == null) { future.complete(false); return@onComplete }
                    deviceStore.deleteDevice(name).onComplete { delResult ->
                        if (delResult.succeeded() && delResult.result()) {
                            val changeData = JsonObject().put("operation", "delete").put("deviceName", name)
                            vertx.eventBus().publish(KafkaClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                            logger.info("Deleted Kafka client: $name")
                            future.complete(true)
                        } else future.complete(false)
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error deleting Kafka client: ${e.message}")
                future.complete(false)
            }
            future
        }
    }

    fun startKafkaClient(): DataFetcher<CompletableFuture<Map<String, Any>>> = DataFetcher { env ->
        val name = env.getArgument<String>("name")
        toggleKafkaClient(name, true)
    }
    fun stopKafkaClient(): DataFetcher<CompletableFuture<Map<String, Any>>> = DataFetcher { env ->
        val name = env.getArgument<String>("name")
        toggleKafkaClient(name, false)
    }
    fun toggleKafkaClient(): DataFetcher<CompletableFuture<Map<String, Any>>> = DataFetcher { env ->
        val name = env.getArgument<String>("name")
        val enabled = env.getArgument<Boolean>("enabled")
        toggleKafkaClient(name, enabled)
    }

    private fun toggleKafkaClient(name: String?, enabled: Boolean?): CompletableFuture<Map<String, Any>> {
        val future = CompletableFuture<Map<String, Any>>()
        try {
            if (name == null || enabled == null) {
                future.complete(mapOf("success" to false, "errors" to listOf("Name and enabled are required")))
                return future
            }
            deviceStore.toggleDevice(name, enabled).onComplete { result ->
                if (result.succeeded()) {
                    val updatedDevice = result.result()
                    if (updatedDevice != null) {
                        val changeData = JsonObject().put("operation", "toggle").put("deviceName", name).put("enabled", enabled)
                        vertx.eventBus().publish(KafkaClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                        logger.info("Toggled Kafka client $name to enabled=$enabled")
                        future.complete(mapOf("success" to true, "client" to deviceToMap(updatedDevice), "errors" to emptyList<String>()))
                    } else {
                        future.complete(mapOf("success" to false, "errors" to listOf("Device '$name' not found")))
                    }
                } else {
                    future.complete(mapOf("success" to false, "errors" to listOf("Failed to toggle device: ${result.cause()?.message}")))
                }
            }
        } catch (e: Exception) {
            logger.severe("Error toggling Kafka client: ${e.message}")
            future.complete(mapOf("success" to false, "errors" to listOf("Failed to toggle client: ${e.message}")))
        }
        return future
    }

    fun reassignKafkaClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
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
                            vertx.eventBus().publish(KafkaClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                            logger.info("Reassigned Kafka client $name to node $nodeId")
                            future.complete(mapOf("success" to true, "client" to deviceToMap(updatedDevice), "errors" to emptyList<String>()))
                        } else {
                            future.complete(mapOf("success" to false, "errors" to listOf("Device '$name' not found")))
                        }
                    } else {
                        future.complete(mapOf("success" to false, "errors" to listOf("Failed to reassign device: ${result.cause()?.message}")))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error reassigning Kafka client: ${e.message}")
                future.complete(mapOf("success" to false, "errors" to listOf("Failed to reassign client: ${e.message}")))
            }
            future
        }
    }

    private fun notifyDeviceConfigChange(operation: String, device: DeviceConfig) {
        val changeData = JsonObject()
            .put("operation", operation)
            .put("deviceName", device.name)
            .put("device", deviceToJson(device))
        vertx.eventBus().publish(KafkaClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
        logger.info("Notified Kafka device config change: $operation for device ${device.name}")
    }

    private fun parseDeviceConfigRequest(input: Map<String, Any>): DeviceConfigRequest {
        @Suppress("UNCHECKED_CAST")
        val configMap = input["config"] as? Map<String, Any>
            ?: throw IllegalArgumentException("Invalid or missing 'config' field")

        val kafkaConfig = KafkaClientConfig(
            bootstrapServers = configMap["bootstrapServers"] as? String ?: "localhost:9092",
            groupId = configMap["groupId"] as? String ?: "monstermq-subscriber",
            payloadFormat = configMap["payloadFormat"] as? String ?: PayloadFormat.DEFAULT,
            extraConsumerConfig = (configMap["extraConsumerConfig"] as? Map<*, *>)?.entries?.associate { it.key.toString() to it.value.toString() } ?: emptyMap(),
            pollIntervalMs = (configMap["pollIntervalMs"] as? Number)?.toLong() ?: 500L,
            maxPollRecords = (configMap["maxPollRecords"] as? Number)?.toInt() ?: 100,
            reconnectDelayMs = (configMap["reconnectDelayMs"] as? Number)?.toLong() ?: 5000L,
            destinationTopicPrefix = (configMap["destinationTopicPrefix"] as? String)?.takeIf { it.isNotBlank() }
        )

        val configJson = JsonObject.mapFrom(kafkaConfig)
            .also { it.remove("namespace") }
        return DeviceConfigRequest(
            name = input["name"] as String,
            namespace = input["namespace"] as String,
            nodeId = input["nodeId"] as String,
            config = configJson,
            enabled = input["enabled"] as? Boolean ?: true,
            type = DeviceConfig.DEVICE_TYPE_KAFKA_CLIENT
        )
    }

    private fun validateKafkaConfig(config: JsonObject): List<String> {
        return try {
            val parsed = KafkaClientConfig.fromJson(config)
            parsed.validate()
        } catch (e: Exception) {
            listOf("Invalid Kafka config: ${e.message}")
        }
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
        val currentNodeId = Monster.getClusterNodeId(vertx) ?: "local"
        val config = try { KafkaClientConfig.fromJson(device.config) } catch (e: Exception) {
            logger.severe("Failed to parse KafkaClientConfig for ${device.name}: ${e.message}")
            KafkaClientConfig()
        }
        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "config" to mapOf(
                "bootstrapServers" to config.bootstrapServers,
                "groupId" to config.groupId,
                "payloadFormat" to config.payloadFormat,
                "extraConsumerConfig" to config.extraConsumerConfig,
                "pollIntervalMs" to config.pollIntervalMs,
                "maxPollRecords" to config.maxPollRecords,
                "reconnectDelayMs" to config.reconnectDelayMs,
                "destinationTopicPrefix" to config.destinationTopicPrefix
            ),
            "enabled" to device.enabled,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString(),
            "isOnCurrentNode" to device.isAssignedToNode(currentNodeId)
        )
    }
}
