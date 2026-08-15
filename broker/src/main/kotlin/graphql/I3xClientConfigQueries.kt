package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Features
import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.I3xConnectionConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL queries for i3X Client configuration management
 */
class I3xClientConfigQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(I3xClientConfigQueries::class.java)

    companion object {
        fun deviceToMap(device: DeviceConfig, currentNodeId: String): Map<String, Any> {
            val config = I3xConnectionConfig.fromJsonObject(device.config)

            return mapOf(
                "name" to device.name,
                "namespace" to device.namespace,
                "nodeId" to device.nodeId,
                "config" to mapOf(
                    "url" to config.url,
                    "authType" to config.authType,
                    "username" to (config.username ?: ""),
                    "password" to (config.password ?: ""),
                    "token" to (config.token ?: ""),
                    "clientId" to config.clientId,
                    "reconnectDelay" to config.reconnectDelay,
                    "connectionTimeout" to config.connectionTimeout,
                    "headers" to config.headers.map { header ->
                        mapOf(
                            "key" to header.key,
                            "value" to header.value
                        )
                    },
                    "addresses" to config.addresses.map { address ->
                        mapOf(
                            "elementId" to address.elementId,
                            "topic" to address.topic,
                            "maxDepth" to address.maxDepth,
                            "retained" to address.retained,
                            "qos" to address.qos,
                            "messageFormat" to address.messageFormat,
                            "removePath" to address.removePath,
                            "description" to address.description
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

    fun i3xClients(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any>>>()
            if (!Monster.isFeatureEnabled(Features.I3xClient))
                return@DataFetcher future.apply { complete(emptyList()) }

            try {
                val name = env.getArgument<String>("name")
                val nodeId = env.getArgument<String>("node")
                val currentNodeId = Monster.getClusterNodeId(vertx)

                when {
                    name != null && nodeId != null -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_I3X_CLIENT && it.name == name }
                                    .map { deviceToMap(it, currentNodeId) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching i3X clients: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    name != null -> {
                        deviceStore.getDevice(name).onComplete { result ->
                            if (result.succeeded()) {
                                val device = result.result()
                                if (device != null && device.type == DeviceConfig.DEVICE_TYPE_I3X_CLIENT) {
                                    future.complete(listOf(deviceToMap(device, currentNodeId)))
                                } else {
                                    future.complete(emptyList())
                                }
                            } else {
                                logger.severe("Error fetching i3X client: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    nodeId != null -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_I3X_CLIENT }
                                    .map { deviceToMap(it, currentNodeId) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching i3X clients by node: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    else -> {
                        deviceStore.getAllDevices().onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_I3X_CLIENT }
                                    .map { deviceToMap(it, currentNodeId) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching i3X clients: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching i3X clients: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }
}
