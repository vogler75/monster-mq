package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.devices.redisclient.RedisClientExtension
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigRequest
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.RedisClientAddress
import at.rocworks.stores.devices.RedisClientConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL mutations for Redis client bridge configuration management.
 */
class RedisClientConfigMutations(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(RedisClientConfigMutations::class.java)
    private val queries = RedisClientConfigQueries(vertx, deviceStore)

    // -- Create --

    fun createRedisClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            try {
                val input = env.getArgument<Map<String, Any>>("input")
                    ?: return@DataFetcher future.apply { complete(errorResult("Input is required")) }

                val request = parseDeviceConfigRequest(input)
                if (!Monster.getEnabledFeaturesForNode(request.nodeId).contains("Redis"))
                    return@DataFetcher future.apply { complete(errorResult("Redis feature is not enabled on node ${request.nodeId}")) }
                val errors = request.validate() + RedisClientConfig.fromJson(request.config).validate()
                if (errors.isNotEmpty()) { future.complete(errorResult(errors)); return@DataFetcher future }

                deviceStore.getDevice(request.name).onComplete { existingRes ->
                    if (existingRes.failed()) { future.complete(dbError(existingRes.cause())); return@onComplete }
                    if (existingRes.result() != null) {
                        future.complete(errorResult("Device '${request.name}' already exists")); return@onComplete
                    }
                    deviceStore.saveDevice(request.toDeviceConfig()).onComplete { saveRes ->
                        if (saveRes.succeeded()) {
                            val saved = saveRes.result()
                            notifyChange("add", saved)
                            future.complete(successResult(saved))
                        } else {
                            future.complete(errorResult("Failed to save: ${saveRes.cause()?.message}"))
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error creating Redis client: ${e.message}")
                future.complete(errorResult("Failed to create: ${e.message}"))
            }
            future
        }
    }

    // -- Update --

    fun updateRedisClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            try {
                val name = env.getArgument<String>("name")
                val input = env.getArgument<Map<String, Any>>("input")
                if (name == null || input == null) {
                    future.complete(errorResult("Name and input are required")); return@DataFetcher future
                }

                deviceStore.getDevice(name).onComplete { existingRes ->
                    if (existingRes.failed()) { future.complete(dbError(existingRes.cause())); return@onComplete }
                    val existing = existingRes.result()
                        ?: run { future.complete(errorResult("Device '$name' not found")); return@onComplete }

                    val request = parseDeviceConfigRequest(input)
                    val errors = request.validate() + RedisClientConfig.fromJson(request.config).validate()
                    if (errors.isNotEmpty()) { future.complete(errorResult(errors)); return@onComplete }

                    val updated = request.toDeviceConfig().copy(
                        createdAt = existing.createdAt,
                        updatedAt = Instant.now()
                    )
                    deviceStore.saveDevice(updated).onComplete { saveRes ->
                        if (saveRes.succeeded()) {
                            val saved = saveRes.result()
                            notifyChange("update", saved)
                            future.complete(successResult(saved))
                        } else {
                            future.complete(errorResult("Failed to update: ${saveRes.cause()?.message}"))
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error updating Redis client: ${e.message}")
                future.complete(errorResult("Failed to update: ${e.message}"))
            }
            future
        }
    }

    // -- Delete --

    fun deleteRedisClient(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Boolean>()
            try {
                val name = env.getArgument<String>("name") ?: run { future.complete(false); return@DataFetcher future }
                deviceStore.getDevice(name).onComplete { existingRes ->
                    if (existingRes.failed() || existingRes.result() == null) { future.complete(false); return@onComplete }
                    deviceStore.deleteDevice(name).onComplete { delRes ->
                        if (delRes.succeeded() && delRes.result()) {
                            vertx.eventBus().publish(
                                RedisClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED,
                                JsonObject().put("operation", "delete").put("deviceName", name)
                            )
                            logger.info("Deleted Redis client: $name")
                            future.complete(true)
                        } else {
                            future.complete(false)
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error deleting Redis client: ${e.message}")
                future.complete(false)
            }
            future
        }
    }

    // -- Start / Stop / Toggle --

    fun startRedisClient(): DataFetcher<CompletableFuture<Map<String, Any>>> = DataFetcher { env ->
        toggleRedisClient(env.getArgument("name"), true)
    }

    fun stopRedisClient(): DataFetcher<CompletableFuture<Map<String, Any>>> = DataFetcher { env ->
        toggleRedisClient(env.getArgument("name"), false)
    }

    fun toggleRedisClient(): DataFetcher<CompletableFuture<Map<String, Any>>> = DataFetcher { env ->
        toggleRedisClient(env.getArgument("name"), env.getArgument("enabled"))
    }

    private fun toggleRedisClient(name: String?, enabled: Boolean?): CompletableFuture<Map<String, Any>> {
        val future = CompletableFuture<Map<String, Any>>()
        if (name == null || enabled == null) {
            future.complete(errorResult("Name and enabled are required")); return future
        }
        deviceStore.toggleDevice(name, enabled).onComplete { result ->
            if (result.succeeded()) {
                val device = result.result()
                if (device != null) {
                    vertx.eventBus().publish(
                        RedisClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED,
                        JsonObject().put("operation", "toggle").put("deviceName", name).put("enabled", enabled)
                    )
                    future.complete(successResult(device))
                } else {
                    future.complete(errorResult("Device '$name' not found"))
                }
            } else {
                future.complete(errorResult("Toggle failed: ${result.cause()?.message}"))
            }
        }
        return future
    }

    // -- Reassign --

    fun reassignRedisClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            try {
                val name = env.getArgument<String>("name")
                val nodeId = env.getArgument<String>("nodeId")
                if (name == null || nodeId == null) {
                    future.complete(errorResult("Name and nodeId are required")); return@DataFetcher future
                }
                val clusterNodes = Monster.getClusterNodeIds(vertx)
                if (!clusterNodes.contains(nodeId)) {
                    future.complete(errorResult("Node '$nodeId' not found. Available: ${clusterNodes.joinToString()}")); return@DataFetcher future
                }
                if (!Monster.getEnabledFeaturesForNode(nodeId).contains("Redis"))
                    return@DataFetcher future.apply { complete(errorResult("Redis feature is not enabled on node $nodeId")) }
                deviceStore.reassignDevice(name, nodeId).onComplete { result ->
                    if (result.succeeded()) {
                        val device = result.result()
                        if (device != null) {
                            vertx.eventBus().publish(
                                RedisClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED,
                                JsonObject().put("operation", "reassign").put("deviceName", name).put("nodeId", nodeId)
                            )
                            future.complete(successResult(device))
                        } else {
                            future.complete(errorResult("Device '$name' not found"))
                        }
                    } else {
                        future.complete(errorResult("Reassign failed: ${result.cause()?.message}"))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error reassigning Redis client: ${e.message}")
                future.complete(errorResult("Reassign failed: ${e.message}"))
            }
            future
        }
    }

    // -- Address management --

    fun addRedisClientAddress(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            try {
                val deviceName = env.getArgument<String>("deviceName")
                    ?: run { future.complete(errorResult("deviceName is required")); return@DataFetcher future }
                @Suppress("UNCHECKED_CAST")
                val inputMap = env.getArgument<Map<String, Any>>("input")
                    ?: run { future.complete(errorResult("input is required")); return@DataFetcher future }

                deviceStore.getDevice(deviceName).onComplete { res ->
                    if (res.failed() || res.result() == null) {
                        future.complete(errorResult("Device '$deviceName' not found")); return@onComplete
                    }
                    val device = res.result()!!
                    val cfg = RedisClientConfig.fromJson(device.config)
                    val newAddr = parseAddress(inputMap)
                    val addrErrors = newAddr.validate()
                    if (addrErrors.isNotEmpty()) { future.complete(errorResult(addrErrors)); return@onComplete }

                    val updatedCfg = cfg.copy(addresses = cfg.addresses + newAddr)
                    val updatedDevice = device.copy(config = updatedCfg.toJsonObject(), updatedAt = Instant.now())

                    deviceStore.saveDevice(updatedDevice).onComplete { saveRes ->
                        if (saveRes.succeeded()) {
                            val saved = saveRes.result()
                            notifyChange("addAddress", saved)
                            future.complete(successResult(saved))
                        } else {
                            future.complete(errorResult("Save failed: ${saveRes.cause()?.message}"))
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error adding address: ${e.message}")
                future.complete(errorResult("Failed: ${e.message}"))
            }
            future
        }
    }

    fun updateRedisClientAddress(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            try {
                val deviceName = env.getArgument<String>("deviceName")
                    ?: run { future.complete(errorResult("deviceName is required")); return@DataFetcher future }
                val redisChannel = env.getArgument<String>("redisChannel")
                    ?: run { future.complete(errorResult("redisChannel is required")); return@DataFetcher future }
                @Suppress("UNCHECKED_CAST")
                val inputMap = env.getArgument<Map<String, Any>>("input")
                    ?: run { future.complete(errorResult("input is required")); return@DataFetcher future }

                deviceStore.getDevice(deviceName).onComplete { res ->
                    if (res.failed() || res.result() == null) {
                        future.complete(errorResult("Device '$deviceName' not found")); return@onComplete
                    }
                    val device = res.result()!!
                    val cfg = RedisClientConfig.fromJson(device.config)
                    val newAddr = parseAddress(inputMap)
                    val addrErrors = newAddr.validate()
                    if (addrErrors.isNotEmpty()) { future.complete(errorResult(addrErrors)); return@onComplete }

                    val addresses = cfg.addresses.map { if (it.redisChannel == redisChannel) newAddr else it }
                    val updatedCfg = cfg.copy(addresses = addresses)
                    val updatedDevice = device.copy(config = updatedCfg.toJsonObject(), updatedAt = Instant.now())

                    deviceStore.saveDevice(updatedDevice).onComplete { saveRes ->
                        if (saveRes.succeeded()) {
                            val saved = saveRes.result()
                            notifyChange("updateAddress", saved)
                            future.complete(successResult(saved))
                        } else {
                            future.complete(errorResult("Save failed: ${saveRes.cause()?.message}"))
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error updating address: ${e.message}")
                future.complete(errorResult("Failed: ${e.message}"))
            }
            future
        }
    }

    fun deleteRedisClientAddress(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            try {
                val deviceName = env.getArgument<String>("deviceName")
                    ?: run { future.complete(errorResult("deviceName is required")); return@DataFetcher future }
                val redisChannel = env.getArgument<String>("redisChannel")
                    ?: run { future.complete(errorResult("redisChannel is required")); return@DataFetcher future }

                deviceStore.getDevice(deviceName).onComplete { res ->
                    if (res.failed() || res.result() == null) {
                        future.complete(errorResult("Device '$deviceName' not found")); return@onComplete
                    }
                    val device = res.result()!!
                    val cfg = RedisClientConfig.fromJson(device.config)
                    val updatedCfg = cfg.copy(addresses = cfg.addresses.filter { it.redisChannel != redisChannel })
                    val updatedDevice = device.copy(config = updatedCfg.toJsonObject(), updatedAt = Instant.now())

                    deviceStore.saveDevice(updatedDevice).onComplete { saveRes ->
                        if (saveRes.succeeded()) {
                            val saved = saveRes.result()
                            notifyChange("deleteAddress", saved)
                            future.complete(successResult(saved))
                        } else {
                            future.complete(errorResult("Save failed: ${saveRes.cause()?.message}"))
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error deleting address: ${e.message}")
                future.complete(errorResult("Failed: ${e.message}"))
            }
            future
        }
    }

    // -- Internal helpers --

    @Suppress("UNCHECKED_CAST")
    private fun parseDeviceConfigRequest(input: Map<String, Any>): DeviceConfigRequest {
        val configMap = input["config"] as? Map<String, Any>
            ?: throw IllegalArgumentException("'config' field is required")

        val addressesList: List<RedisClientAddress> = (configMap["addresses"] as? List<*>)?.mapNotNull { item ->
            (item as? Map<*, *>)?.let { parseAddress(it.entries.associate { e -> e.key.toString() to e.value!! }) }
        } ?: emptyList()

        val redisConfig = RedisClientConfig(
            host = configMap["host"] as? String ?: "localhost",
            port = (configMap["port"] as? Number)?.toInt() ?: 6379,
            password = configMap["password"] as? String,
            database = (configMap["database"] as? Number)?.toInt() ?: 0,
            useSsl = configMap["useSsl"] as? Boolean ?: false,
            sslTrustAll = configMap["sslTrustAll"] as? Boolean ?: false,
            connectionString = configMap["connectionString"] as? String,
            maxPoolSize = (configMap["maxPoolSize"] as? Number)?.toInt() ?: 6,
            reconnectDelayMs = (configMap["reconnectDelayMs"] as? Number)?.toLong() ?: 5000L,
            maxReconnectAttempts = (configMap["maxReconnectAttempts"] as? Number)?.toInt() ?: -1,
            loopPrevention = configMap["loopPrevention"] as? Boolean ?: true,
            addresses = addressesList
        )

        val configJson = redisConfig.toJsonObject()

        return DeviceConfigRequest(
            name = input["name"] as String,
            namespace = input["namespace"] as String,
            nodeId = input["nodeId"] as String,
            config = configJson,
            enabled = input["enabled"] as? Boolean ?: true,
            type = DeviceConfig.DEVICE_TYPE_REDIS_CLIENT
        )
    }

    private fun parseAddress(map: Map<String, Any>): RedisClientAddress = RedisClientAddress(
        mode = map["mode"] as? String ?: RedisClientAddress.MODE_SUBSCRIBE,
        redisChannel = map["redisChannel"] as? String ?: "",
        mqttTopic = map["mqttTopic"] as? String ?: "",
        qos = (map["qos"] as? Number)?.toInt() ?: 0,
        usePatternSubscribe = map["usePatternSubscribe"] as? Boolean ?: false,
        usePatternMatch = map["usePatternMatch"] as? Boolean ?: false,
        kvPollIntervalMs = (map["kvPollIntervalMs"] as? Number)?.toLong() ?: 0L,
        removePath = map["removePath"] as? Boolean ?: true
    )

    private fun notifyChange(operation: String, device: DeviceConfig) {
        val changeData = JsonObject()
            .put("operation", operation)
            .put("deviceName", device.name)
            .put("device", deviceToJson(device))
        vertx.eventBus().publish(RedisClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
        logger.info("Notified Redis device config change: $operation for '${device.name}'")
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

    private fun successResult(device: DeviceConfig): Map<String, Any> = mapOf(
        "success" to true,
        "client" to queries.deviceToMap(device),
        "errors" to emptyList<String>()
    )

    private fun errorResult(message: String): Map<String, Any> =
        mapOf("success" to false, "errors" to listOf(message))

    private fun errorResult(messages: List<String>): Map<String, Any> =
        mapOf("success" to false, "errors" to messages)

    private fun dbError(cause: Throwable?): Map<String, Any> =
        errorResult("Database error: ${cause?.message}")
}
