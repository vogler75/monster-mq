package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.RedisClientConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL queries for Redis client bridge configuration management.
 */
class RedisClientConfigQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(RedisClientConfigQueries::class.java)

    fun redisClients(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
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
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_REDIS_CLIENT && it.name == name }
                                    .map { deviceToMap(it) })
                            } else {
                                logger.severe("Error fetching Redis clients: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    name != null -> {
                        deviceStore.getDevice(name).onComplete { result ->
                            if (result.succeeded()) {
                                val device = result.result()
                                if (device != null && device.type == DeviceConfig.DEVICE_TYPE_REDIS_CLIENT) {
                                    future.complete(listOf(deviceToMap(device)))
                                } else {
                                    future.complete(emptyList())
                                }
                            } else {
                                logger.severe("Error fetching Redis client: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    nodeId != null -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                future.complete(result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_REDIS_CLIENT }
                                    .map { deviceToMap(it) })
                            } else {
                                logger.severe("Error fetching Redis clients by node: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    else -> {
                        deviceStore.getAllDevices().onComplete { result ->
                            if (result.succeeded()) {
                                future.complete(result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_REDIS_CLIENT }
                                    .map { deviceToMap(it) })
                            } else {
                                logger.severe("Error fetching Redis clients: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching Redis clients: ${e.message}")
                future.complete(emptyList())
            }
            future
        }
    }

    fun redisClientMetrics(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any>>>()
            val redisClient = env.getSource<Map<String, Any>>()
            val deviceName = redisClient?.get("name") as? String

            if (deviceName == null) {
                future.complete(listOf(emptyMetrics()))
                return@DataFetcher future
            }

            val addr = EventBusAddresses.RedisBridge.connectorMetrics(deviceName)
            vertx.eventBus().request<JsonObject>(addr, JsonObject()).onComplete { result ->
                if (result.succeeded()) {
                    val body = result.result().body()
                    future.complete(listOf(mapOf(
                        "messagesIn" to body.getDouble("messagesInRate", 0.0),
                        "messagesOut" to body.getDouble("messagesOutRate", 0.0),
                        "errors" to body.getDouble("errors", 0.0),
                        "timestamp" to Instant.now().toString()
                    )))
                } else {
                    future.complete(listOf(emptyMetrics()))
                }
            }

            future
        }
    }

    private fun emptyMetrics(): Map<String, Any> = mapOf(
        "messagesIn" to 0.0,
        "messagesOut" to 0.0,
        "errors" to 0.0,
        "timestamp" to Instant.now().toString()
    )

    internal fun deviceToMap(device: DeviceConfig): Map<String, Any?> {
        val currentNodeId = Monster.getClusterNodeId(vertx)
        val config = try {
            RedisClientConfig.fromJson(device.config)
        } catch (e: Exception) {
            logger.severe("Failed to parse RedisClientConfig for ${device.name}: ${e.message}")
            RedisClientConfig()
        }

        val addressesList = config.addresses.map { addr ->
            mapOf(
                "mode" to addr.mode,
                "redisChannel" to addr.redisChannel,
                "mqttTopic" to addr.mqttTopic,
                "qos" to addr.qos,
                "usePatternSubscribe" to addr.usePatternSubscribe,
                "usePatternMatch" to addr.usePatternMatch,
                "kvPollIntervalMs" to addr.kvPollIntervalMs,
                "publishOnChangeOnly" to addr.publishOnChangeOnly,
                "removePath" to addr.removePath
            )
        }

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "config" to mapOf(
                "host" to config.host,
                "port" to config.port,
                "database" to config.database,
                "useSsl" to config.useSsl,
                "sslTrustAll" to config.sslTrustAll,
                "connectionString" to config.connectionString,
                "maxPoolSize" to config.maxPoolSize,
                "reconnectDelayMs" to config.reconnectDelayMs,
                "maxReconnectAttempts" to config.maxReconnectAttempts,
                "loopPrevention" to config.loopPrevention,
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
