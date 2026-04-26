package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Features
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.TelegramClientConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL queries for Telegram bot client configuration management.
 */
class TelegramClientConfigQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(TelegramClientConfigQueries::class.java)

    fun telegramClients(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any?>>>()
            if (!Monster.isFeatureEnabled(Features.Telegram))
                return@DataFetcher future.apply { complete(emptyList()) }
            try {
                val name = env.getArgument<String?>("name")
                val nodeId = env.getArgument<String?>("node")

                when {
                    name != null && nodeId != null -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                future.complete(result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_TELEGRAM_CLIENT && it.name == name }
                                    .map { deviceToMap(it) })
                            } else {
                                logger.severe("Error fetching Telegram clients: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    name != null -> {
                        deviceStore.getDevice(name).onComplete { result ->
                            if (result.succeeded()) {
                                val device = result.result()
                                if (device != null && device.type == DeviceConfig.DEVICE_TYPE_TELEGRAM_CLIENT) {
                                    future.complete(listOf(deviceToMap(device)))
                                } else {
                                    future.complete(emptyList())
                                }
                            } else {
                                logger.severe("Error fetching Telegram client: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    nodeId != null -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                future.complete(result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_TELEGRAM_CLIENT }
                                    .map { deviceToMap(it) })
                            } else {
                                logger.severe("Error fetching Telegram clients by node: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    else -> {
                        deviceStore.getAllDevices().onComplete { result ->
                            if (result.succeeded()) {
                                future.complete(result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_TELEGRAM_CLIENT }
                                    .map { deviceToMap(it) })
                            } else {
                                logger.severe("Error fetching Telegram clients: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching Telegram clients: ${e.message}")
                future.complete(emptyList())
            }
            future
        }
    }

    fun telegramClientMetrics(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any>>>()
            if (!Monster.isFeatureEnabled(Features.Telegram))
                return@DataFetcher future.apply { complete(emptyList()) }
            val telegramClient = env.getSource<Map<String, Any>>()
            val deviceName = telegramClient?.get("name") as? String

            if (deviceName == null) {
                future.complete(listOf(emptyMetrics()))
                return@DataFetcher future
            }

            val addr = EventBusAddresses.TelegramBridge.connectorMetrics(deviceName)
            vertx.eventBus().request<JsonObject>(addr, JsonObject()).onComplete { result ->
                if (result.succeeded()) {
                    val body = result.result().body()
                    future.complete(listOf(mapOf(
                        "messagesIn" to body.getDouble("messagesInRate", 0.0),
                        "messagesOut" to body.getDouble("messagesOutRate", 0.0),
                        "errors" to body.getDouble("errors", 0.0),
                        "registeredChats" to body.getInteger("registeredChats", 0),
                        "timestamp" to Instant.now().toString()
                    )))
                } else {
                    future.complete(listOf(emptyMetrics()))
                }
            }

            future
        }
    }

    private fun emptyMetrics(): Map<String, Any> = mapOf(
        "messagesIn" to 0.0,
        "messagesOut" to 0.0,
        "errors" to 0.0,
        "registeredChats" to 0,
        "timestamp" to Instant.now().toString()
    )

    internal fun deviceToMap(device: DeviceConfig): Map<String, Any?> {
        val currentNodeId = Monster.getClusterNodeId(vertx)
        val config = try {
            TelegramClientConfig.fromJson(device.config)
        } catch (e: Exception) {
            logger.severe("Failed to parse TelegramClientConfig for ${device.name}: ${e.message}")
            TelegramClientConfig()
        }

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "config" to mapOf(
                "botToken" to config.maskedBotToken(),
                "pollingTimeoutSeconds" to config.pollingTimeoutSeconds,
                "reconnectDelayMs" to config.reconnectDelayMs,
                "proxyHost" to config.proxyHost,
                "proxyPort" to config.proxyPort,
                "parseMode" to config.parseMode,
                "allowedUsers" to config.allowedUsers
            ),
            "enabled" to device.enabled,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString(),
            "isOnCurrentNode" to device.isAssignedToNode(currentNodeId),
            "metrics" to emptyList<Map<String, Any?>>()
        )
    }
}
