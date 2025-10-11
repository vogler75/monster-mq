package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.devices.neo4j.Neo4jExtension
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigRequest
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.Neo4jClientConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL mutations for Neo4j client bridge configuration management
 * Provides mutation resolvers for creating, updating, and managing Neo4j clients
 */
class Neo4jClientConfigMutations(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(Neo4jClientConfigMutations::class.java)

    fun createNeo4jClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            try {
                val input = env.getArgument<Map<String, Any>>("input")
                    ?: return@DataFetcher future.apply { complete(mapOf("success" to false, "errors" to listOf("Input is required"))) }

                val request = parseDeviceConfigRequest(input)
                val validationErrors = request.validate() + validateNeo4jConfig(request.config)
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

                    deviceStore.isNamespaceInUse(request.namespace).onComplete { nsResult ->
                        if (nsResult.failed()) {
                            future.complete(mapOf("success" to false, "errors" to listOf("Database error: ${nsResult.cause()?.message}")))
                            return@onComplete
                        }
                        if (nsResult.result()) {
                            future.complete(mapOf("success" to false, "errors" to listOf("Namespace '${request.namespace}' is already in use")))
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
                }
            } catch (e: Exception) {
                logger.severe("Error creating Neo4j client: ${e.message}")
                future.complete(mapOf("success" to false, "errors" to listOf("Failed to create client: ${e.message}")))
            }
            future
        }
    }

    fun updateNeo4jClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
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

                    // Preserve existing password if the new one is blank
                    val existingConfig = Neo4jClientConfig.fromJson(existingDevice.config)
                    val requestConfig = Neo4jClientConfig.fromJson(request.config)
                    val mergedConfig = if (requestConfig.password.isBlank()) {
                        requestConfig.copy(password = existingConfig.password)
                    } else {
                        requestConfig
                    }
                    val mergedConfigJson = mergedConfig.toJson()

                    val validationErrors = request.validate() + validateNeo4jConfig(mergedConfigJson)
                    if (validationErrors.isNotEmpty()) {
                        future.complete(mapOf("success" to false, "errors" to validationErrors))
                        return@onComplete
                    }

                    deviceStore.isNamespaceInUse(request.namespace, name).onComplete { nsResult ->
                        if (nsResult.failed()) {
                            future.complete(mapOf("success" to false, "errors" to listOf("Database error: ${nsResult.cause()?.message}")))
                            return@onComplete
                        }
                        if (nsResult.result()) {
                            future.complete(mapOf("success" to false, "errors" to listOf("Namespace '${request.namespace}' is already in use by another device")))
                            return@onComplete
                        }

                        val updatedDevice = DeviceConfig(
                            name = request.name,
                            namespace = request.namespace,
                            nodeId = request.nodeId,
                            enabled = request.enabled,
                            type = request.type,
                            config = mergedConfig.toJson(),
                            createdAt = existingDevice.createdAt,
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
                }
            } catch (e: Exception) {
                logger.severe("Error updating Neo4j client: ${e.message}")
                future.complete(mapOf("success" to false, "errors" to listOf("Failed to update client: ${e.message}")))
            }
            future
        }
    }

    fun deleteNeo4jClient(): DataFetcher<CompletableFuture<Boolean>> {
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
                            vertx.eventBus().publish(Neo4jExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                            logger.info("Deleted Neo4j client: $name")
                            future.complete(true)
                        } else future.complete(false)
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error deleting Neo4j client: ${e.message}")
                future.complete(false)
            }
            future
        }
    }

    fun startNeo4jClient(): DataFetcher<CompletableFuture<Map<String, Any>>> = DataFetcher { env ->
        val name = env.getArgument<String>("name")
        toggleNeo4jClient(name, true)
    }
    fun stopNeo4jClient(): DataFetcher<CompletableFuture<Map<String, Any>>> = DataFetcher { env ->
        val name = env.getArgument<String>("name")
        toggleNeo4jClient(name, false)
    }
    fun toggleNeo4jClient(): DataFetcher<CompletableFuture<Map<String, Any>>> = DataFetcher { env ->
        val name = env.getArgument<String>("name")
        val enabled = env.getArgument<Boolean>("enabled")
        toggleNeo4jClient(name, enabled)
    }

    private fun toggleNeo4jClient(name: String?, enabled: Boolean?): CompletableFuture<Map<String, Any>> {
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
                        vertx.eventBus().publish(Neo4jExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                        logger.info("Toggled Neo4j client $name to enabled=$enabled")
                        future.complete(mapOf("success" to true, "client" to deviceToMap(updatedDevice), "errors" to emptyList<String>()))
                    } else {
                        future.complete(mapOf("success" to false, "errors" to listOf("Device '$name' not found")))
                    }
                } else {
                    future.complete(mapOf("success" to false, "errors" to listOf("Failed to toggle device: ${result.cause()?.message}")))
                }
            }
        } catch (e: Exception) {
            logger.severe("Error toggling Neo4j client: ${e.message}")
            future.complete(mapOf("success" to false, "errors" to listOf("Failed to toggle client: ${e.message}")))
        }
        return future
    }

    fun reassignNeo4jClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
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
                            vertx.eventBus().publish(Neo4jExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
                            logger.info("Reassigned Neo4j client $name to node $nodeId")
                            future.complete(mapOf("success" to true, "client" to deviceToMap(updatedDevice), "errors" to emptyList<String>()))
                        } else {
                            future.complete(mapOf("success" to false, "errors" to listOf("Device '$name' not found")))
                        }
                    } else {
                        future.complete(mapOf("success" to false, "errors" to listOf("Failed to reassign device: ${result.cause()?.message}")))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error reassigning Neo4j client: ${e.message}")
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
        vertx.eventBus().publish(Neo4jExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
        logger.info("Notified Neo4j device config change: $operation for device ${device.name}")
    }

    private fun parseDeviceConfigRequest(input: Map<String, Any>): DeviceConfigRequest {
        @Suppress("UNCHECKED_CAST")
        val configMap = input["config"] as? Map<String, Any>
            ?: throw IllegalArgumentException("Invalid or missing 'config' field")

        val neo4jConfig = Neo4jClientConfig(
            url = configMap["url"] as? String ?: "bolt://localhost:7687",
            username = configMap["username"] as? String ?: "neo4j",
            password = configMap["password"] as? String ?: "password",
            topicFilters = (configMap["topicFilters"] as? List<*>)?.mapNotNull { it as? String } ?: emptyList(),
            queueSize = (configMap["queueSize"] as? Number)?.toInt() ?: 10000,
            batchSize = (configMap["batchSize"] as? Number)?.toInt() ?: 100,
            reconnectDelayMs = (configMap["reconnectDelayMs"] as? Number)?.toLong() ?: 5000L,
            maxChangeRateSeconds = (configMap["maxChangeRateSeconds"] as? Number)?.toInt() ?: 0
        )

        val configJson = neo4jConfig.toJson()
        return DeviceConfigRequest(
            name = input["name"] as String,
            namespace = input["namespace"] as String,
            nodeId = input["nodeId"] as String,
            config = configJson,
            enabled = input["enabled"] as? Boolean ?: true,
            type = DeviceConfig.DEVICE_TYPE_NEO4J_CLIENT
        )
    }

    private fun validateNeo4jConfig(config: JsonObject): List<String> {
        return try {
            val parsed = Neo4jClientConfig.fromJson(config)
            parsed.validate()
        } catch (e: Exception) {
            listOf("Invalid Neo4j config: ${e.message}")
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
        val config = try { Neo4jClientConfig.fromJson(device.config) } catch (e: Exception) {
            logger.severe("Failed to parse Neo4jClientConfig for ${device.name}: ${e.message}")
            Neo4jClientConfig()
        }
        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "config" to mapOf(
                "url" to config.url,
                "username" to config.username,
                "password" to null, // Don't expose password for security
                "topicFilters" to config.topicFilters,
                "queueSize" to config.queueSize,
                "batchSize" to config.batchSize,
                "reconnectDelayMs" to config.reconnectDelayMs,
                "maxChangeRateSeconds" to config.maxChangeRateSeconds
            ),
            "enabled" to device.enabled,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString(),
            "isOnCurrentNode" to device.isAssignedToNode(currentNodeId)
        )
    }
}
