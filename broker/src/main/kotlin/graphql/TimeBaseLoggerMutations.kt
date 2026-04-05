package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.logger.TimeBaseLoggerExtension
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.TimeBaseLoggerConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

class TimeBaseLoggerMutations(private val vertx: Vertx, private val deviceStore: IDeviceConfigStore) {
    private val logger: Logger = Utils.getLogger(TimeBaseLoggerMutations::class.java)

    fun create(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            val input = env.getArgument<Map<String, Any>>("input")
            if (input == null) { future.complete(mapOf("success" to false, "errors" to listOf("Input is required"))); return@DataFetcher future }
            val name = input["name"] as String
            val namespace = input["namespace"] as String
            val nodeId = input["nodeId"] as String
            val enabled = input["enabled"] as Boolean
            @Suppress("UNCHECKED_CAST")
            val configInput = input["config"] as Map<String, Any>
            val config = TimeBaseLoggerConfig.fromJson(JsonObject(configInput))
            val device = DeviceConfig(name, namespace, nodeId, config.toJson(), enabled, DeviceConfig.DEVICE_TYPE_TIMEBASE_LOGGER)
            deviceStore.saveDevice(device).onComplete { res ->
                if (res.succeeded()) {
                    notifyChange("add", device)
                    future.complete(mapOf("success" to true, "logger" to deviceToMap(device), "errors" to emptyList<String>()))
                } else future.complete(mapOf("success" to false, "errors" to listOf(res.cause()?.message ?: "Unknown error")))
            }
            future
        }
    }

    fun update(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            val name = env.getArgument<String>("name") ?: ""
            val input = env.getArgument<Map<String, Any>>("input")
            if (input == null) { future.complete(mapOf("success" to false, "errors" to listOf("Input is required"))); return@DataFetcher future }
            val namespace = input["namespace"] as String
            val nodeId = input["nodeId"] as String
            val enabled = input["enabled"] as Boolean
            @Suppress("UNCHECKED_CAST")
            val configInput = input["config"] as Map<String, Any>
            deviceStore.getDevice(name).onComplete { dr ->
                if (dr.result() != null) {
                    val config = TimeBaseLoggerConfig.fromJson(JsonObject(configInput))
                    val updated = DeviceConfig(name, namespace, nodeId, config.toJson(), enabled, DeviceConfig.DEVICE_TYPE_TIMEBASE_LOGGER)
                    deviceStore.saveDevice(updated).onComplete { res ->
                        if (res.succeeded()) {
                            notifyChange("update", updated)
                            future.complete(mapOf("success" to true, "logger" to deviceToMap(updated), "errors" to emptyList<String>()))
                        } else future.complete(mapOf("success" to false, "errors" to listOf(res.cause()?.message ?: "Unknown error")))
                    }
                } else future.complete(mapOf("success" to false, "errors" to listOf("Device not found")))
            }
            future
        }
    }

    fun delete(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val name = env.getArgument<String>("name") ?: ""
            val future = CompletableFuture<Boolean>()
            deviceStore.deleteDevice(name).onComplete { res ->
                if (res.succeeded()) { notifyChange("delete", name); future.complete(true) }
                else future.complete(false)
            }
            future
        }
    }

    fun toggle(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val name = env.getArgument<String>("name") ?: ""
            val enabled = env.getArgument<Boolean>("enabled") ?: false
            val future = CompletableFuture<Boolean>()
            deviceStore.getDevice(name).onComplete { dr ->
                val device = dr.result()
                if (device != null) {
                    val updated = device.copy(enabled = enabled)
                    deviceStore.saveDevice(updated).onComplete { res ->
                        if (res.succeeded()) {
                            vertx.eventBus().publish(TimeBaseLoggerExtension.ADDRESS_DEVICE_CONFIG_CHANGED,
                                JsonObject().put("operation", "toggle").put("deviceName", name).put("enabled", enabled))
                            future.complete(true)
                        } else future.complete(false)
                    }
                } else future.complete(false)
            }
            future
        }
    }

    fun reassign(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val name = env.getArgument<String>("name") ?: ""
            val nodeId = env.getArgument<String>("nodeId") ?: ""
            val future = CompletableFuture<Boolean>()
            deviceStore.getDevice(name).onComplete { dr ->
                val device = dr.result()
                if (device != null) {
                    val updated = device.copy(nodeId = nodeId)
                    deviceStore.saveDevice(updated).onComplete { res ->
                        if (res.succeeded()) {
                            vertx.eventBus().publish(TimeBaseLoggerExtension.ADDRESS_DEVICE_CONFIG_CHANGED,
                                JsonObject().put("operation", "reassign").put("deviceName", name).put("nodeId", nodeId))
                            future.complete(true)
                        } else future.complete(false)
                    }
                } else future.complete(false)
            }
            future
        }
    }

    private fun notifyChange(op: String, device: DeviceConfig) {
        vertx.eventBus().publish(TimeBaseLoggerExtension.ADDRESS_DEVICE_CONFIG_CHANGED,
            JsonObject().put("operation", op).put("deviceName", device.name).put("device", device.toJsonObject()))
    }

    private fun notifyChange(op: String, deviceName: String) {
        vertx.eventBus().publish(TimeBaseLoggerExtension.ADDRESS_DEVICE_CONFIG_CHANGED,
            JsonObject().put("operation", op).put("deviceName", deviceName))
    }

    private fun deviceToMap(device: DeviceConfig): Map<String, Any?> {
        val config = TimeBaseLoggerConfig.fromJson(device.config)
        return mapOf(
            "name" to device.name, "namespace" to device.namespace, "nodeId" to device.nodeId,
            "enabled" to device.enabled, "config" to config.toJson().map,
            "isLocal" to (device.nodeId == Monster.getClusterNodeId(vertx))
        )
    }
}
