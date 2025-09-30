package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.MqttClientConnectionConfig
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
        return DataFetcher { _ ->
            val future = CompletableFuture<List<Map<String, Any>>>()

            try {
                deviceStore.getAllDevices().onComplete { result ->
                    if (result.succeeded()) {
                        // Filter to only return MQTT-Client type devices
                        val deviceMaps = result.result()
                            .filter { device -> device.type == DeviceConfig.DEVICE_TYPE_MQTT_CLIENT }
                            .map { device -> deviceToMap(device) }
                        future.complete(deviceMaps)
                    } else {
                        logger.severe("Error fetching MQTT clients: ${result.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching MQTT clients: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    fun mqttClient(): DataFetcher<CompletableFuture<Map<String, Any>?>> {
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
                        // Only return device if it's MQTT-Client type
                        if (device != null && device.type == DeviceConfig.DEVICE_TYPE_MQTT_CLIENT) {
                            future.complete(deviceToMap(device))
                        } else {
                            future.complete(null)
                        }
                    } else {
                        logger.severe("Error fetching MQTT client: ${result.cause()?.message}")
                        future.complete(null)
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching MQTT client: ${e.message}")
                future.complete(null)
            }

            future
        }
    }

    fun mqttClientsByNode(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
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
                        // Filter to only return MQTT-Client type devices
                        val deviceMaps = result.result()
                            .filter { device -> device.type == DeviceConfig.DEVICE_TYPE_MQTT_CLIENT }
                            .map { device -> deviceToMap(device) }
                        future.complete(deviceMaps)
                    } else {
                        logger.severe("Error fetching MQTT clients by node: ${result.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching MQTT clients by node: ${e.message}")
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
                }
            ),
            "enabled" to device.enabled,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString(),
            "isOnCurrentNode" to device.isAssignedToNode(currentNodeId)
        )
    }
}