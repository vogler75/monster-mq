package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.NatsClientConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL queries for NATS client bridge configuration management.
 */
class NatsClientConfigQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(NatsClientConfigQueries::class.java)

    fun natsClients(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any?>>>()
            try {
                val name = env.getArgument<String?>("name")
                val nodeId = env.getArgument<String?>("node")

                when {
                    name != null && nodeId != null -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                future.complete(result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_NATS_CLIENT && it.name == name }
                                    .map { deviceToMap(it) })
                            } else {
                                logger.severe("Error fetching NATS clients: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    name != null -> {
                        deviceStore.getDevice(name).onComplete { result ->
                            if (result.succeeded()) {
                                val device = result.result()
                                if (device != null && device.type == DeviceConfig.DEVICE_TYPE_NATS_CLIENT) {
                                    future.complete(listOf(deviceToMap(device)))
                                } else {
                                    future.complete(emptyList())
                                }
                            } else {
                                logger.severe("Error fetching NATS client: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    nodeId != null -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                future.complete(result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_NATS_CLIENT }
                                    .map { deviceToMap(it) })
                            } else {
                                logger.severe("Error fetching NATS clients by node: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    else -> {
                        deviceStore.getAllDevices().onComplete { result ->
                            if (result.succeeded()) {
                                future.complete(result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_NATS_CLIENT }
                                    .map { deviceToMap(it) })
                            } else {
                                logger.severe("Error fetching NATS clients: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching NATS clients: ${e.message}")
                future.complete(emptyList())
            }
            future
        }
    }

    internal fun deviceToMap(device: DeviceConfig): Map<String, Any?> {
        val currentNodeId = Monster.getClusterNodeId(vertx)
        val config = try {
            NatsClientConfig.fromJson(device.config)
        } catch (e: Exception) {
            logger.severe("Failed to parse NatsClientConfig for ${device.name}: ${e.message}")
            NatsClientConfig()
        }

        val addressesList = config.addresses.map { addr ->
            mapOf(
                "mode" to addr.mode,
                "natsSubject" to addr.natsSubject,
                "mqttTopic" to addr.mqttTopic,
                "qos" to addr.qos,
                "autoConvert" to addr.autoConvert,
                "removePath" to addr.removePath
            )
        }

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "config" to mapOf(
                "servers" to config.servers,
                "authType" to config.authType,
                "username" to config.username,
                "tlsCaCertPath" to config.tlsCaCertPath,
                "tlsVerify" to config.tlsVerify,
                "connectTimeoutMs" to config.connectTimeoutMs,
                "reconnectDelayMs" to config.reconnectDelayMs,
                "maxReconnectAttempts" to config.maxReconnectAttempts,
                "useJetStream" to config.useJetStream,
                "streamName" to config.streamName,
                "consumerDurableName" to config.consumerDurableName,
                "addresses" to addressesList
            ),
            "enabled" to device.enabled,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString(),
            "isOnCurrentNode" to device.isAssignedToNode(currentNodeId),
            "metrics" to emptyList<Map<String, Any?>>()
        )
    }
}
