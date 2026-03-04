package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.devices.natsclient.NatsClientExtension
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigRequest
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.NatsAuthType
import at.rocworks.stores.devices.NatsClientAddress
import at.rocworks.stores.devices.NatsClientConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL mutations for NATS client bridge configuration management.
 */
class NatsClientConfigMutations(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(NatsClientConfigMutations::class.java)
    private val queries = NatsClientConfigQueries(vertx, deviceStore)

    // ── Create ────────────────────────────────────────────────────────────

    fun createNatsClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            try {
                val input = env.getArgument<Map<String, Any>>("input")
                    ?: return@DataFetcher future.apply { complete(errorResult("Input is required")) }

                val request = parseDeviceConfigRequest(input)
                val errors = request.validate() + NatsClientConfig.fromJson(request.config).validate()
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
                logger.severe("Error creating NATS client: ${e.message}")
                future.complete(errorResult("Failed to create: ${e.message}"))
            }
            future
        }
    }

    // ── Update ────────────────────────────────────────────────────────────

    fun updateNatsClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
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
                    val errors = request.validate() + NatsClientConfig.fromJson(request.config).validate()
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
                logger.severe("Error updating NATS client: ${e.message}")
                future.complete(errorResult("Failed to update: ${e.message}"))
            }
            future
        }
    }

    // ── Delete ────────────────────────────────────────────────────────────

    fun deleteNatsClient(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Boolean>()
            try {
                val name = env.getArgument<String>("name") ?: run { future.complete(false); return@DataFetcher future }
                deviceStore.getDevice(name).onComplete { existingRes ->
                    if (existingRes.failed() || existingRes.result() == null) { future.complete(false); return@onComplete }
                    deviceStore.deleteDevice(name).onComplete { delRes ->
                        if (delRes.succeeded() && delRes.result()) {
                            vertx.eventBus().publish(
                                NatsClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED,
                                JsonObject().put("operation", "delete").put("deviceName", name)
                            )
                            logger.info("Deleted NATS client: $name")
                            future.complete(true)
                        } else {
                            future.complete(false)
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error deleting NATS client: ${e.message}")
                future.complete(false)
            }
            future
        }
    }

    // ── Start / Stop / Toggle ─────────────────────────────────────────────

    fun startNatsClient(): DataFetcher<CompletableFuture<Map<String, Any>>> = DataFetcher { env ->
        toggleNatsClient(env.getArgument("name"), true)
    }

    fun stopNatsClient(): DataFetcher<CompletableFuture<Map<String, Any>>> = DataFetcher { env ->
        toggleNatsClient(env.getArgument("name"), false)
    }

    fun toggleNatsClient(): DataFetcher<CompletableFuture<Map<String, Any>>> = DataFetcher { env ->
        toggleNatsClient(env.getArgument("name"), env.getArgument("enabled"))
    }

    private fun toggleNatsClient(name: String?, enabled: Boolean?): CompletableFuture<Map<String, Any>> {
        val future = CompletableFuture<Map<String, Any>>()
        if (name == null || enabled == null) {
            future.complete(errorResult("Name and enabled are required")); return future
        }
        deviceStore.toggleDevice(name, enabled).onComplete { result ->
            if (result.succeeded()) {
                val device = result.result()
                if (device != null) {
                    vertx.eventBus().publish(
                        NatsClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED,
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

    // ── Reassign ──────────────────────────────────────────────────────────

    fun reassignNatsClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
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
                deviceStore.reassignDevice(name, nodeId).onComplete { result ->
                    if (result.succeeded()) {
                        val device = result.result()
                        if (device != null) {
                            vertx.eventBus().publish(
                                NatsClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED,
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
                logger.severe("Error reassigning NATS client: ${e.message}")
                future.complete(errorResult("Reassign failed: ${e.message}"))
            }
            future
        }
    }

    // ── Address management ────────────────────────────────────────────────

    fun addNatsClientAddress(): DataFetcher<CompletableFuture<Map<String, Any>>> {
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
                    val cfg = NatsClientConfig.fromJson(device.config)
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

    fun updateNatsClientAddress(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            try {
                val deviceName = env.getArgument<String>("deviceName")
                    ?: run { future.complete(errorResult("deviceName is required")); return@DataFetcher future }
                val natsSubject = env.getArgument<String>("natsSubject")
                    ?: run { future.complete(errorResult("natsSubject is required")); return@DataFetcher future }
                @Suppress("UNCHECKED_CAST")
                val inputMap = env.getArgument<Map<String, Any>>("input")
                    ?: run { future.complete(errorResult("input is required")); return@DataFetcher future }

                deviceStore.getDevice(deviceName).onComplete { res ->
                    if (res.failed() || res.result() == null) {
                        future.complete(errorResult("Device '$deviceName' not found")); return@onComplete
                    }
                    val device = res.result()!!
                    val cfg = NatsClientConfig.fromJson(device.config)
                    val newAddr = parseAddress(inputMap)
                    val addrErrors = newAddr.validate()
                    if (addrErrors.isNotEmpty()) { future.complete(errorResult(addrErrors)); return@onComplete }

                    val addresses = cfg.addresses.map { if (it.natsSubject == natsSubject) newAddr else it }
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

    fun deleteNatsClientAddress(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            try {
                val deviceName = env.getArgument<String>("deviceName")
                    ?: run { future.complete(errorResult("deviceName is required")); return@DataFetcher future }
                val natsSubject = env.getArgument<String>("natsSubject")
                    ?: run { future.complete(errorResult("natsSubject is required")); return@DataFetcher future }

                deviceStore.getDevice(deviceName).onComplete { res ->
                    if (res.failed() || res.result() == null) {
                        future.complete(errorResult("Device '$deviceName' not found")); return@onComplete
                    }
                    val device = res.result()!!
                    val cfg = NatsClientConfig.fromJson(device.config)
                    val updatedCfg = cfg.copy(addresses = cfg.addresses.filter { it.natsSubject != natsSubject })
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

    // ── Internal helpers ──────────────────────────────────────────────────

    @Suppress("UNCHECKED_CAST")
    private fun parseDeviceConfigRequest(input: Map<String, Any>): DeviceConfigRequest {
        val configMap = input["config"] as? Map<String, Any>
            ?: throw IllegalArgumentException("'config' field is required")

        val serversRaw = configMap["servers"]
        val serversList: List<String> = when (serversRaw) {
            is List<*> -> serversRaw.map { it.toString() }
            is String -> listOf(serversRaw)
            else -> listOf("nats://localhost:4222")
        }

        // Parse addresses list from input if present
        val addressesList: List<NatsClientAddress> = (configMap["addresses"] as? List<*>)?.mapNotNull { item ->
            (item as? Map<*, *>)?.let { parseAddress(it.entries.associate { e -> e.key.toString() to e.value!! }) }
        } ?: emptyList()

        val natsConfig = NatsClientConfig(
            servers = serversList,
            authType = configMap["authType"] as? String ?: NatsAuthType.ANONYMOUS,
            username = configMap["username"] as? String,
            password = configMap["password"] as? String,
            token = configMap["token"] as? String,
            tlsCaCertPath = configMap["tlsCaCertPath"] as? String,
            tlsVerify = configMap["tlsVerify"] as? Boolean ?: true,
            connectTimeoutMs = (configMap["connectTimeoutMs"] as? Number)?.toLong() ?: 5000L,
            reconnectDelayMs = (configMap["reconnectDelayMs"] as? Number)?.toLong() ?: 5000L,
            maxReconnectAttempts = (configMap["maxReconnectAttempts"] as? Number)?.toInt() ?: -1,
            useJetStream = configMap["useJetStream"] as? Boolean ?: false,
            streamName = configMap["streamName"] as? String,
            consumerDurableName = configMap["consumerDurableName"] as? String,
            addresses = addressesList
        )

        val configJson = natsConfig.toJsonObject()

        return DeviceConfigRequest(
            name = input["name"] as String,
            namespace = input["namespace"] as String,
            nodeId = input["nodeId"] as String,
            config = configJson,
            enabled = input["enabled"] as? Boolean ?: true,
            type = DeviceConfig.DEVICE_TYPE_NATS_CLIENT
        )
    }

    private fun parseAddress(map: Map<String, Any>): NatsClientAddress = NatsClientAddress(
        mode = map["mode"] as? String ?: NatsClientAddress.MODE_SUBSCRIBE,
        natsSubject = map["natsSubject"] as? String ?: "",
        mqttTopic = map["mqttTopic"] as? String ?: "",
        qos = (map["qos"] as? Number)?.toInt() ?: 0,
        autoConvert = map["autoConvert"] as? Boolean ?: true,
        removePath = map["removePath"] as? Boolean ?: true
    )

    private fun notifyChange(operation: String, device: DeviceConfig) {
        val changeData = JsonObject()
            .put("operation", operation)
            .put("deviceName", device.name)
            .put("device", deviceToJson(device))
        vertx.eventBus().publish(NatsClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
        logger.info("Notified NATS device config change: $operation for '${device.name}'")
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
