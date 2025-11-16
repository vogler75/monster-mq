package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.SparkplugBDecoderConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL queries for SparkplugB Decoder device management
 */
class SparkplugBDecoderQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(SparkplugBDecoderQueries::class.java)

    fun sparkplugBDecoders(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any>>>()

            try {
                val name = env.getArgument<String?>("name")
                val nodeId = env.getArgument<String?>("node") ?: Monster.getClusterNodeId(vertx)

                when {
                    // Filter by both name and node
                    name != null -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_SPARKPLUGB_DECODER && it.name == name }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching SparkplugB Decoders: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    // Filter by node only (or current node if not specified)
                    else -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_SPARKPLUGB_DECODER }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching SparkplugB Decoders by node: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Exception in sparkplugBDecoders query: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }


    /**
     * Convert DeviceConfig to GraphQL-compatible map
     */
    private fun deviceToMap(device: DeviceConfig): Map<String, Any> {
        val config = SparkplugBDecoderConfig.fromJsonObject(device.config)
        val isOnCurrentNode = device.nodeId == Monster.getClusterNodeId(vertx)

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "config" to configToMap(config),
            "enabled" to device.enabled,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString(),
            "isOnCurrentNode" to isOnCurrentNode,
            "metrics" to DataFetcher { _ ->
                fetchMetrics(device.name)
            }
        )
    }

    /**
     * Fetch metrics for a SparkplugB decoder
     */
    private fun fetchMetrics(deviceName: String): CompletableFuture<Map<String, Any>> {
        val future = CompletableFuture<Map<String, Any>>()
        val addr = EventBusAddresses.SparkplugBDecoder.connectorMetrics(deviceName)

        vertx.eventBus().request<JsonObject>(addr, JsonObject()).onComplete { result ->
            if (result.succeeded()) {
                val metricsJson = result.result().body()
                val metrics = mapOf(
                    "messagesIn" to metricsJson.getDouble("messagesInRate", 0.0),
                    "messagesOut" to metricsJson.getDouble("messagesOutRate", 0.0),
                    "messagesSkipped" to metricsJson.getDouble("messagesSkippedRate", 0.0),
                    "timestamp" to Instant.now().toString()
                )
                future.complete(metrics)
            } else {
                logger.fine { "Failed to fetch metrics for SparkplugB Decoder '$deviceName': ${result.cause()?.message}" }
                // Return zeros instead of null to avoid GraphQL errors
                future.complete(
                    mapOf(
                        "messagesIn" to 0.0,
                        "messagesOut" to 0.0,
                        "messagesSkipped" to 0.0,
                        "timestamp" to Instant.now().toString()
                    )
                )
            }
        }

        return future
    }

    /**
     * Convert SparkplugBDecoderConfig to GraphQL-compatible map
     */
    private fun configToMap(config: SparkplugBDecoderConfig): Map<String, Any> {
        return mapOf(
            "sourceNamespace" to config.sourceNamespace,
            "rules" to config.rules.map { rule ->
                mapOf(
                    "name" to rule.name,
                    "nodeIdRegex" to rule.nodeIdRegex,
                    "deviceIdRegex" to rule.deviceIdRegex,
                    "destinationTopic" to rule.destinationTopic,
                    "transformations" to rule.transformations
                )
            }
        )
    }
}
