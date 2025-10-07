package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.WinCCUaConnectionConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL queries for WinCC Unified client configuration management
 */
class WinCCUaClientConfigQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(WinCCUaClientConfigQueries::class.java)

    fun winCCUaClients(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { _ ->
            val future = CompletableFuture<List<Map<String, Any>>>()

            try {
                deviceStore.getAllDevices().onComplete { result ->
                    if (result.succeeded()) {
                        // Filter to only return WinCCUA-Client type devices
                        val deviceMaps = result.result()
                            .filter { device -> device.type == DeviceConfig.DEVICE_TYPE_WINCCUA_CLIENT }
                            .map { device -> deviceToMap(device) }
                        future.complete(deviceMaps)
                    } else {
                        logger.severe("Error fetching WinCC Unified clients: ${result.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching WinCC Unified clients: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    fun winCCUaClient(): DataFetcher<CompletableFuture<Map<String, Any>?>> {
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
                        // Only return device if it's WinCCUA-Client type
                        if (device != null && device.type == DeviceConfig.DEVICE_TYPE_WINCCUA_CLIENT) {
                            future.complete(deviceToMap(device))
                        } else {
                            future.complete(null)
                        }
                    } else {
                        logger.severe("Error fetching WinCC Unified client: ${result.cause()?.message}")
                        future.complete(null)
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching WinCC Unified client: ${e.message}")
                future.complete(null)
            }

            future
        }
    }

    fun winCCUaClientsByNode(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
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
                        // Filter to only return WinCCUA-Client type devices
                        val deviceMaps = result.result()
                            .filter { device -> device.type == DeviceConfig.DEVICE_TYPE_WINCCUA_CLIENT }
                            .map { device -> deviceToMap(device) }
                        future.complete(deviceMaps)
                    } else {
                        logger.severe("Error fetching WinCC Unified clients by node: ${result.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching WinCC Unified clients by node: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    private fun deviceToMap(device: DeviceConfig): Map<String, Any> {
        val currentNodeId = Monster.getClusterNodeId(vertx) ?: "local"

        // Parse config from JsonObject for WinCC Unified Client devices
        val config = WinCCUaConnectionConfig.fromJsonObject(device.config)

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "config" to mapOf(
                "graphqlEndpoint" to config.graphqlEndpoint,
                "websocketEndpoint" to (config.websocketEndpoint ?: ""),
                "username" to config.username,
                "password" to config.password,
                "reconnectDelay" to config.reconnectDelay,
                "connectionTimeout" to config.connectionTimeout,
                "messageFormat" to config.messageFormat,
                "transformConfig" to mapOf(
                    "convertDotToSlash" to config.transformConfig.convertDotToSlash,
                    "convertUnderscoreToSlash" to config.transformConfig.convertUnderscoreToSlash,
                    "regexPattern" to (config.transformConfig.regexPattern ?: ""),
                    "regexReplacement" to (config.transformConfig.regexReplacement ?: "")
                ),
                "addresses" to config.addresses.map { address ->
                    val addressMap = mutableMapOf<String, Any>(
                        "type" to address.type.name,
                        "topic" to address.topic,
                        "description" to address.description,
                        "retained" to address.retained
                    )
                    if (address.browseArguments != null) {
                        addressMap["browseArguments"] = address.browseArguments.map
                    }
                    if (address.systemNames != null) {
                        addressMap["systemNames"] = address.systemNames
                    }
                    if (address.filterString != null) {
                        addressMap["filterString"] = address.filterString
                    }
                    addressMap
                }
            ),
            "enabled" to device.enabled,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString(),
            "isOnCurrentNode" to device.isAssignedToNode(currentNodeId)
        )
    }
}
