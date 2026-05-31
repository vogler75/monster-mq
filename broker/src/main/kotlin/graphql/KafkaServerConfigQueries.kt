package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Features
import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.devices.kafkaserver.KafkaServerConfig
import at.rocworks.devices.kafkaserver.KafkaStreamMapping
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

class KafkaServerConfigQueries(
    private val vertx: Vertx,
    private val deviceConfigStore: IDeviceConfigStore
) {
    companion object {
        private val logger: Logger = Utils.getLogger(KafkaServerConfigQueries::class.java)
    }

    private val currentNodeId = Monster.getClusterNodeId(vertx)

    fun kafkaServers(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any?>>>()
            if (!Monster.isFeatureEnabled(Features.KafkaServer)) {
                return@DataFetcher future.apply { complete(emptyList()) }
            }

            deviceConfigStore.getAllDevices().onComplete { ar ->
                if (ar.succeeded()) {
                    try {
                        val devices = ar.result().filter { it.type == DeviceConfig.DEVICE_TYPE_KAFKA_SERVER }
                        if (devices.isEmpty()) {
                            future.complete(emptyList())
                            return@onComplete
                        }

                        val futures = devices.map { deviceToMap(it) }
                        CompletableFuture.allOf(*futures.toTypedArray()).whenComplete { _, err ->
                            if (err != null) {
                                future.completeExceptionally(err)
                            } else {
                                future.complete(futures.map { it.join() })
                            }
                        }
                    } catch (e: Exception) {
                        logger.severe("Failed to process Kafka servers: ${e.message}")
                        future.completeExceptionally(e)
                    }
                } else {
                    logger.severe("Failed to load Kafka servers: ${ar.cause()?.message}")
                    future.complete(emptyList())
                }
            }
            future
        }
    }

    fun kafkaServer(): DataFetcher<CompletableFuture<Map<String, Any?>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>?>()
            if (!Monster.isFeatureEnabled(Features.KafkaServer)) {
                return@DataFetcher future.apply { complete(null) }
            }

            val name = env.getArgument<String>("name")
            if (name == null) {
                future.complete(null)
                return@DataFetcher future
            }

            deviceConfigStore.getDevice(name).onComplete { ar ->
                if (ar.succeeded()) {
                    val device = ar.result()
                    if (device != null && device.type == DeviceConfig.DEVICE_TYPE_KAFKA_SERVER) {
                        deviceToMap(device).whenComplete { result, err ->
                            if (err != null) {
                                future.completeExceptionally(err)
                            } else {
                                future.complete(result)
                            }
                        }
                    } else {
                        future.complete(null)
                    }
                } else {
                    logger.severe("Failed to load Kafka server '$name': ${ar.cause()?.message}")
                    future.complete(null)
                }
            }
            future
        }
    }

    private fun deviceToMap(device: DeviceConfig): CompletableFuture<Map<String, Any?>> {
        val future = CompletableFuture<Map<String, Any?>>()
        val config = try {
            KafkaServerConfig.fromJsonObject(device.config)
        } catch (e: Exception) {
            logger.severe("Failed to parse KafkaServerConfig for ${device.name}: ${e.message}")
            KafkaServerConfig()
        }

        val streamsList = config.streams.map { mapping ->
            mapOf(
                "streamName" to mapping.streamName,
                "topicFilter" to mapping.topicFilter,
                "retentionHours" to mapping.retentionHours,
                "storeType" to mapping.storeType,
                "allowWrite" to mapping.allowWrite
            )
        }

        // Check if server is running on the node
        val statusAddress = "kafkaserver.connectors.list.$currentNodeId"

        vertx.eventBus().request<JsonObject>(statusAddress, JsonObject()).onComplete { replyAr ->
            var active = false
            var starting = false
            if (replyAr.succeeded()) {
                val body = replyAr.result().body()
                val list = body.getJsonArray("servers")
                if (list != null && list.contains(device.name)) {
                    active = true
                }
                val startingList = body.getJsonArray("starting")
                if (startingList != null && startingList.contains(device.name)) {
                    starting = true
                }
            }
            
            val statusVal = if (device.enabled) {
                if (active) "RUNNING"
                else if (starting) "STARTING"
                else "ERROR"
            } else "STOPPED"
            
            future.complete(mapOf(
                "name" to device.name,
                "namespace" to device.namespace,
                "nodeId" to device.nodeId,
                "enabled" to device.enabled,
                "host" to config.host,
                "port" to config.port,
                "storeType" to config.storeType,
                "streams" to streamsList,
                "createdAt" to device.createdAt.toString(),
                "updatedAt" to device.updatedAt.toString(),
                "isOnCurrentNode" to (device.nodeId == "*" || device.nodeId == currentNodeId),
                "status" to statusVal
            ))
        }

        return future
    }
}
