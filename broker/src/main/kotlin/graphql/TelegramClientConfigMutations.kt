package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.devices.telegramclient.TelegramClientExtension
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigRequest
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.TelegramClientConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL mutations for Telegram bot client configuration management.
 */
class TelegramClientConfigMutations(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(TelegramClientConfigMutations::class.java)
    private val queries = TelegramClientConfigQueries(vertx, deviceStore)

    // ── Create ────────────────────────────────────────────────────────────

    fun createTelegramClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            try {
                val input = env.getArgument<Map<String, Any>>("input")
                    ?: return@DataFetcher future.apply { complete(errorResult("Input is required")) }

                val request = parseDeviceConfigRequest(input)
                val errors = request.validate() + TelegramClientConfig.fromJson(request.config).validate()
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
                logger.severe("Error creating Telegram client: ${e.message}")
                future.complete(errorResult("Failed to create: ${e.message}"))
            }
            future
        }
    }

    // ── Update ────────────────────────────────────────────────────────────

    fun updateTelegramClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
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
                    val errors = request.validate() + TelegramClientConfig.fromJson(request.config).validate()
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
                logger.severe("Error updating Telegram client: ${e.message}")
                future.complete(errorResult("Failed to update: ${e.message}"))
            }
            future
        }
    }

    // ── Delete ────────────────────────────────────────────────────────────

    fun deleteTelegramClient(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Boolean>()
            try {
                val name = env.getArgument<String>("name") ?: run { future.complete(false); return@DataFetcher future }
                deviceStore.getDevice(name).onComplete { existingRes ->
                    if (existingRes.failed() || existingRes.result() == null) { future.complete(false); return@onComplete }
                    deviceStore.deleteDevice(name).onComplete { delRes ->
                        if (delRes.succeeded() && delRes.result()) {
                            vertx.eventBus().publish(
                                TelegramClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED,
                                JsonObject().put("operation", "delete").put("deviceName", name)
                            )
                            logger.info("Deleted Telegram client: $name")
                            future.complete(true)
                        } else {
                            future.complete(false)
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error deleting Telegram client: ${e.message}")
                future.complete(false)
            }
            future
        }
    }

    // ── Start / Stop / Toggle ─────────────────────────────────────────────

    fun startTelegramClient(): DataFetcher<CompletableFuture<Map<String, Any>>> = DataFetcher { env ->
        toggleTelegramClient(env.getArgument("name"), true)
    }

    fun stopTelegramClient(): DataFetcher<CompletableFuture<Map<String, Any>>> = DataFetcher { env ->
        toggleTelegramClient(env.getArgument("name"), false)
    }

    fun toggleTelegramClient(): DataFetcher<CompletableFuture<Map<String, Any>>> = DataFetcher { env ->
        toggleTelegramClient(env.getArgument("name"), env.getArgument("enabled"))
    }

    private fun toggleTelegramClient(name: String?, enabled: Boolean?): CompletableFuture<Map<String, Any>> {
        val future = CompletableFuture<Map<String, Any>>()
        if (name == null || enabled == null) {
            future.complete(errorResult("Name and enabled are required")); return future
        }
        deviceStore.toggleDevice(name, enabled).onComplete { result ->
            if (result.succeeded()) {
                val device = result.result()
                if (device != null) {
                    vertx.eventBus().publish(
                        TelegramClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED,
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

    fun reassignTelegramClient(): DataFetcher<CompletableFuture<Map<String, Any>>> {
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
                                TelegramClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED,
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
                logger.severe("Error reassigning Telegram client: ${e.message}")
                future.complete(errorResult("Reassign failed: ${e.message}"))
            }
            future
        }
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    @Suppress("UNCHECKED_CAST")
    private fun parseDeviceConfigRequest(input: Map<String, Any>): DeviceConfigRequest {
        val configMap = input["config"] as? Map<String, Any>
            ?: throw IllegalArgumentException("'config' field is required")

        val allowedUsers: List<String> = (configMap["allowedUsers"] as? List<*>)?.map { it.toString() } ?: emptyList()

        val telegramConfig = TelegramClientConfig(
            botToken = configMap["botToken"] as? String ?: "",
            pollingTimeoutSeconds = (configMap["pollingTimeoutSeconds"] as? Number)?.toInt() ?: 30,
            reconnectDelayMs = (configMap["reconnectDelayMs"] as? Number)?.toLong() ?: 5000L,
            proxyHost = configMap["proxyHost"] as? String,
            proxyPort = (configMap["proxyPort"] as? Number)?.toInt(),
            parseMode = configMap["parseMode"] as? String ?: "Text",
            allowedUsers = allowedUsers
        )

        val configJson = telegramConfig.toJsonObject()

        return DeviceConfigRequest(
            name = input["name"] as String,
            namespace = input["namespace"] as String,
            nodeId = input["nodeId"] as String,
            config = configJson,
            enabled = input["enabled"] as? Boolean ?: true,
            type = DeviceConfig.DEVICE_TYPE_TELEGRAM_CLIENT
        )
    }

    private fun notifyChange(operation: String, device: DeviceConfig) {
        val changeData = JsonObject()
            .put("operation", operation)
            .put("deviceName", device.name)
            .put("device", deviceToJson(device))
        vertx.eventBus().publish(TelegramClientExtension.ADDRESS_DEVICE_CONFIG_CHANGED, changeData)
        logger.info("Notified Telegram device config change: $operation for '${device.name}'")
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
