package at.rocworks.graphql

import at.rocworks.Utils
import at.rocworks.Monster
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.Plc4xConnectionConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL queries for PLC4X client configuration management
 */
class Plc4xClientConfigQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(Plc4xClientConfigQueries::class.java)

    fun plc4xClients(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
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
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_PLC4X_CLIENT && it.name == name }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching PLC4X devices: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    // Filter by name only
                    name != null -> {
                        deviceStore.getDevice(name).onComplete { result ->
                            if (result.succeeded()) {
                                val device = result.result()
                                if (device != null && device.type == DeviceConfig.DEVICE_TYPE_PLC4X_CLIENT) {
                                    future.complete(listOf(deviceToMap(device)))
                                } else {
                                    future.complete(emptyList())
                                }
                            } else {
                                logger.severe("Error fetching PLC4X device: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    // Filter by node only
                    nodeId != null -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_PLC4X_CLIENT }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching PLC4X devices by node: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    // No filters - return all
                    else -> {
                        deviceStore.getAllDevices().onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_PLC4X_CLIENT }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching PLC4X devices: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching PLC4X devices: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    private fun deviceToMap(device: DeviceConfig): Map<String, Any> {
        val currentNodeId = Monster.Companion.getClusterNodeId(vertx) ?: "local"

        // Parse config from JsonObject for PLC4X Client devices
        val config = Plc4xConnectionConfig.fromJsonObject(device.config)

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "config" to mapOf(
                "protocol" to config.protocol.uppercase(),
                "connectionString" to config.connectionString,
                "pollingInterval" to config.pollingInterval,
                "reconnectDelay" to config.reconnectDelay,
                "enabled" to config.enabled,
                "addresses" to config.addresses.map { address ->
                    mapOf(
                        "name" to address.name,
                        "address" to address.address,
                        "topic" to address.topic,
                        "qos" to address.qos,
                        "retained" to address.retained,
                        "scalingFactor" to address.scalingFactor,
                        "offset" to address.offset,
                        "deadband" to address.deadband,
                        "publishOnChange" to address.publishOnChange,
                        "mode" to address.mode.name,
                        "enabled" to address.enabled
                    )
                }
            ),
            "enabled" to device.enabled,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString(),
            "isOnCurrentNode" to device.isAssignedToNode(currentNodeId)
            // metrics and metricsHistory are handled by separate field resolvers in MetricsResolver
        )
    }
}
