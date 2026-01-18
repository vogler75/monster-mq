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
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any>>>()

            try {
                val name = env.getArgument<String?>("name")
                val nodeId = env.getArgument<String?>("node")

                when {
                    // Filter by both name and node
                    name != null && nodeId != null -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_WINCCOA_CLIENT && it.name == name }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching WinCC OA clients: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    // Filter by name only
                    name != null -> {
                        deviceStore.getDevice(name).onComplete { result ->
                            if (result.succeeded()) {
                                val device = result.result()
                                if (device != null && device.type == DeviceConfig.DEVICE_TYPE_WINCCOA_CLIENT) {
                                    future.complete(listOf(deviceToMap(device)))
                                } else {
                                    future.complete(emptyList())
                                }
                            } else {
                                logger.severe("Error fetching WinCC OA client: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    // Filter by node only
                    nodeId != null -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_WINCCOA_CLIENT }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching WinCC OA clients by node: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    // No filters - return all
                    else -> {
                        deviceStore.getAllDevices().onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_WINCCOA_CLIENT }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching WinCC OA clients: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching WinCC OA clients: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    private fun deviceToMap(device: DeviceConfig): Map<String, Any> {
        val currentNodeId = Monster.getClusterNodeId(vertx)

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
