package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.WinCCUaAddressType
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
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_WINCCUA_CLIENT && it.name == name }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching WinCC Unified clients: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    // Filter by name only
                    name != null -> {
                        deviceStore.getDevice(name).onComplete { result ->
                            if (result.succeeded()) {
                                val device = result.result()
                                if (device != null && device.type == DeviceConfig.DEVICE_TYPE_WINCCUA_CLIENT) {
                                    future.complete(listOf(deviceToMap(device)))
                                } else {
                                    future.complete(emptyList())
                                }
                            } else {
                                logger.severe("Error fetching WinCC Unified client: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    // Filter by node only
                    nodeId != null -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_WINCCUA_CLIENT }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching WinCC Unified clients by node: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    // No filters - return all
                    else -> {
                        deviceStore.getAllDevices().onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_WINCCUA_CLIENT }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching WinCC Unified clients: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching WinCC Unified clients: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    private fun deviceToMap(device: DeviceConfig): Map<String, Any> {
        val currentNodeId = Monster.getClusterNodeId(vertx)

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
                    if (address.nameFilters != null) {
                        addressMap["nameFilters"] = address.nameFilters
                    }
                    if (address.type == WinCCUaAddressType.TAG_VALUES) {
                        addressMap["includeQuality"] = address.includeQuality
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
