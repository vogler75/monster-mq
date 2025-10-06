package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.WinCCOaConnectionConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL queries for WinCC OA client configuration management
 */
class WinCCOaClientConfigQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(WinCCOaClientConfigQueries::class.java)

    fun winCCOaClients(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { _ ->
            val future = CompletableFuture<List<Map<String, Any>>>()

            try {
                deviceStore.getAllDevices().onComplete { result ->
                    if (result.succeeded()) {
                        // Filter to only return WinCCOA-Client type devices
                        val deviceMaps = result.result()
                            .filter { device -> device.type == DeviceConfig.DEVICE_TYPE_WINCCOA_CLIENT }
                            .map { device -> deviceToMap(device) }
                        future.complete(deviceMaps)
                    } else {
                        logger.severe("Error fetching WinCC OA clients: ${result.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching WinCC OA clients: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    fun winCCOaClient(): DataFetcher<CompletableFuture<Map<String, Any>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>?>()

            try {
                val name = env.getArgument<String>("name")
                if (name == null) {
                    future.complete(null)
                    return@DataFetcher future
                }

                deviceStore.getDevice(name).onComplete { result ->
                    if (result.succeeded()) {
                        val device = result.result()
                        // Only return device if it's WinCCOA-Client type
                        if (device != null && device.type == DeviceConfig.DEVICE_TYPE_WINCCOA_CLIENT) {
                            future.complete(deviceToMap(device))
                        } else {
                            future.complete(null)
                        }
                    } else {
                        logger.severe("Error fetching WinCC OA client: ${result.cause()?.message}")
                        future.complete(null)
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching WinCC OA client: ${e.message}")
                future.complete(null)
            }

            future
        }
    }

    fun winCCOaClientsByNode(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any>>>()

            try {
                val nodeId = env.getArgument<String>("nodeId")
                if (nodeId == null) {
                    future.complete(emptyList())
                    return@DataFetcher future
                }

                deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                    if (result.succeeded()) {
                        // Filter to only return WinCCOA-Client type devices
                        val deviceMaps = result.result()
                            .filter { device -> device.type == DeviceConfig.DEVICE_TYPE_WINCCOA_CLIENT }
                            .map { device -> deviceToMap(device) }
                        future.complete(deviceMaps)
                    } else {
                        logger.severe("Error fetching WinCC OA clients by node: ${result.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching WinCC OA clients by node: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    private fun deviceToMap(device: DeviceConfig): Map<String, Any> {
        val currentNodeId = Monster.getClusterNodeId(vertx) ?: "local"

        // Parse config from JsonObject for WinCC OA Client devices
        val config = WinCCOaConnectionConfig.fromJsonObject(device.config)

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "config" to mapOf(
                "graphqlEndpoint" to config.graphqlEndpoint,
                "websocketEndpoint" to (config.websocketEndpoint ?: ""),
                "username" to (config.username ?: ""),
                "password" to (config.password ?: ""),
                "token" to (config.token ?: ""),
                "reconnectDelay" to config.reconnectDelay,
                "connectionTimeout" to config.connectionTimeout,
                "messageFormat" to config.messageFormat,
                "transformConfig" to mapOf(
                    "removeSystemName" to config.transformConfig.removeSystemName,
                    "convertDotToSlash" to config.transformConfig.convertDotToSlash,
                    "convertUnderscoreToSlash" to config.transformConfig.convertUnderscoreToSlash,
                    "regexPattern" to (config.transformConfig.regexPattern ?: ""),
                    "regexReplacement" to (config.transformConfig.regexReplacement ?: "")
                ),
                "addresses" to config.addresses.map { address ->
                    mapOf(
                        "query" to address.query,
                        "topic" to address.topic,
                        "description" to address.description,
                        "answer" to address.answer,
                        "retained" to address.retained
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
