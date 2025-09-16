package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.stores.DeviceConfig
import at.rocworks.devices.opcua.IDeviceConfigStore
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL queries for OPC UA device configuration management
 */
class DeviceConfigQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Logger.getLogger(DeviceConfigQueries::class.java.name)

    fun opcUaDevices(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { _ ->
            val future = CompletableFuture<List<Map<String, Any>>>()

            try {
                deviceStore.getAllDevices().onComplete { result ->
                    if (result.succeeded()) {
                        val deviceMaps = result.result().map { device -> deviceToMap(device) }
                        future.complete(deviceMaps)
                    } else {
                        logger.severe("Error fetching OPC UA devices: ${result.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching OPC UA devices: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    fun opcUaDevice(): DataFetcher<CompletableFuture<Map<String, Any>?>> {
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
                        if (device != null) {
                            future.complete(deviceToMap(device))
                        } else {
                            future.complete(null)
                        }
                    } else {
                        logger.severe("Error fetching OPC UA device: ${result.cause()?.message}")
                        future.complete(null)
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching OPC UA device: ${e.message}")
                future.complete(null)
            }

            future
        }
    }

    fun opcUaDevicesByNode(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
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
                        val deviceMaps = result.result().map { device -> deviceToMap(device) }
                        future.complete(deviceMaps)
                    } else {
                        logger.severe("Error fetching OPC UA devices by node: ${result.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching OPC UA devices by node: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    fun clusterNodes(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { _ ->
            val future = CompletableFuture<List<Map<String, Any>>>()

            try {
                val nodes = Monster.Companion.getClusterNodeIds(vertx)
                val currentNodeId = Monster.Companion.getClusterNodeId(vertx) ?: "local"

                val nodeMaps = nodes.map { nodeId ->
                    mapOf(
                        "nodeId" to nodeId,
                        "isCurrent" to (nodeId == currentNodeId)
                    )
                }

                future.complete(nodeMaps)
            } catch (e: Exception) {
                logger.severe("Error fetching cluster nodes: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    private fun deviceToMap(device: DeviceConfig): Map<String, Any> {
        val currentNodeId = Monster.Companion.getClusterNodeId(vertx) ?: "local"

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "backupNodeId" to (device.backupNodeId ?: ""),
            "config" to mapOf(
                "endpointUrl" to device.config.endpointUrl,
                "securityPolicy" to device.config.securityPolicy,
                "username" to (device.config.username ?: ""),
                "subscriptionSamplingInterval" to device.config.subscriptionSamplingInterval,
                "keepAliveFailuresAllowed" to device.config.keepAliveFailuresAllowed,
                "reconnectDelay" to device.config.reconnectDelay,
                "connectionTimeout" to device.config.connectionTimeout,
                "requestTimeout" to device.config.requestTimeout,
                "monitoringParameters" to mapOf(
                    "bufferSize" to device.config.monitoringParameters.bufferSize,
                    "samplingInterval" to device.config.monitoringParameters.samplingInterval,
                    "discardOldest" to device.config.monitoringParameters.discardOldest
                ),
                "addresses" to device.config.addresses.map { address ->
                    mapOf(
                        "address" to address.address,
                        "topic" to address.topic,
                        "publishMode" to address.publishMode,
                        "removePath" to address.removePath
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