package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.MqttClientConnectionConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL queries for MQTT client configuration management
 */
class MqttClientConfigQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(MqttClientConfigQueries::class.java)

    fun mqttClients(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
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
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_MQTT_CLIENT && it.name == name }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching MQTT clients: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    // Filter by name only
                    name != null -> {
                        deviceStore.getDevice(name).onComplete { result ->
                            if (result.succeeded()) {
                                val device = result.result()
                                if (device != null && device.type == DeviceConfig.DEVICE_TYPE_MQTT_CLIENT) {
                                    future.complete(listOf(deviceToMap(device)))
                                } else {
                                    future.complete(emptyList())
                                }
                            } else {
                                logger.severe("Error fetching MQTT client: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    // Filter by node only
                    nodeId != null -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_MQTT_CLIENT }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching MQTT clients by node: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    // No filters - return all
                    else -> {
                        deviceStore.getAllDevices().onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_MQTT_CLIENT }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching MQTT clients: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching MQTT clients: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    private fun deviceToMap(device: DeviceConfig): Map<String, Any> {
        val currentNodeId = Monster.getClusterNodeId(vertx) ?: "local"

        // Parse config from JsonObject for MQTT Client devices
        val config = MqttClientConnectionConfig.fromJsonObject(device.config)

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "config" to mapOf(
                "protocol" to config.protocol,
                "hostname" to config.hostname,
                "port" to config.port,
                "username" to (config.username ?: ""),
                "password" to (config.password ?: ""),
                "clientId" to config.clientId,
                "cleanSession" to config.cleanSession,
                "keepAlive" to config.keepAlive,
                "reconnectDelay" to config.reconnectDelay,
                "connectionTimeout" to config.connectionTimeout,
                "addresses" to config.addresses.map { address ->
                    mapOf(
                        "mode" to address.mode,
                        "remoteTopic" to address.remoteTopic,
                        "localTopic" to address.localTopic,
                        "removePath" to address.removePath,
                        "qos" to address.qos
                    )
                },
                "bufferEnabled" to config.bufferEnabled,
                "bufferSize" to config.bufferSize,
                "persistBuffer" to config.persistBuffer,
                "deleteOldestMessages" to config.deleteOldestMessages
            ),
            "enabled" to device.enabled,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString(),
            "isOnCurrentNode" to device.isAssignedToNode(currentNodeId)
        )
    }
}