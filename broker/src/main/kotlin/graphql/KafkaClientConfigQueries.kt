package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.KafkaClientConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL queries for Kafka client bridge configuration management
 * Mirrors structure of MqttClientConfigQueries but adapted to simplified GraphQL schema.
 */
class KafkaClientConfigQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(KafkaClientConfigQueries::class.java)

    fun kafkaClients(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any?>>>()
            try {
                val name = env.getArgument<String?>("name")
                val nodeId = env.getArgument<String?>("node")

                when {
                    // Filter by both name and node
                    name != null && nodeId != null -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_KAFKA_CLIENT && it.name == name }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching Kafka clients: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    // Filter by name only
                    name != null -> {
                        deviceStore.getDevice(name).onComplete { result ->
                            if (result.succeeded()) {
                                val device = result.result()
                                if (device != null && device.type == DeviceConfig.DEVICE_TYPE_KAFKA_CLIENT) {
                                    future.complete(listOf(deviceToMap(device)))
                                } else {
                                    future.complete(emptyList())
                                }
                            } else {
                                logger.severe("Error fetching Kafka client: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    // Filter by node only
                    nodeId != null -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_KAFKA_CLIENT }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching Kafka clients by node: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    // No filters - return all
                    else -> {
                        deviceStore.getAllDevices().onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_KAFKA_CLIENT }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching Kafka clients: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching Kafka clients: ${e.message}")
                future.complete(emptyList())
            }
            future
        }
    }

    private fun deviceToMap(device: DeviceConfig): Map<String, Any?> {
        val currentNodeId = Monster.getClusterNodeId(vertx) ?: "local"
        val config = try { KafkaClientConfig.fromJson(device.config) } catch (e: Exception) {
            logger.severe("Failed to parse KafkaClientConfig for ${device.name}: ${e.message}")
            // Provide minimal fallback so the UI can still show something
            KafkaClientConfig()
        }

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "config" to mapOf(
                "bootstrapServers" to config.bootstrapServers,
                "groupId" to config.groupId,
                "payloadFormat" to config.payloadFormat,
                "extraConsumerConfig" to config.extraConsumerConfig,
                "pollIntervalMs" to config.pollIntervalMs,
                "maxPollRecords" to config.maxPollRecords,
                "reconnectDelayMs" to config.reconnectDelayMs,
                "destinationTopicPrefix" to config.destinationTopicPrefix
            ),
            "enabled" to device.enabled,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString(),
            "isOnCurrentNode" to device.isAssignedToNode(currentNodeId),
            // Empty metrics list placeholders; actual resolvers in MetricsResolver
            "metrics" to emptyList<Map<String, Any?>>(),
            "metricsHistory" to emptyList<Map<String, Any?>>()
        )
    }
}
