package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.TimeBaseLoggerConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

class TimeBaseLoggerQueries(private val vertx: Vertx, private val deviceStore: IDeviceConfigStore) {
    private val logger: Logger = Utils.getLogger(TimeBaseLoggerQueries::class.java)

    fun timebaseLoggers(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any?>>>()
            val name = env.getArgument<String?>("name")
            val nodeId = env.getArgument<String?>("node")
            val filter: (DeviceConfig) -> Boolean = { dev ->
                dev.type == DeviceConfig.DEVICE_TYPE_TIMEBASE_LOGGER &&
                (name == null || dev.name == name) &&
                (nodeId == null || dev.nodeId == nodeId)
            }
            deviceStore.getAllDevices().onComplete { res ->
                if (res.succeeded()) future.complete(res.result().filter(filter).map { deviceToMap(it) })
                else future.complete(emptyList())
            }
            future
        }
    }

    private fun deviceToMap(device: DeviceConfig): Map<String, Any?> {
        val currentNodeId = Monster.getClusterNodeId(vertx)
        val config = TimeBaseLoggerConfig.fromJson(device.config)
        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "enabled" to device.enabled,
            "config" to config.toJson().map,
            "isLocal" to (device.nodeId == currentNodeId)
        )
    }

    fun timebaseLoggerMetrics(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val source = env.getSource<Map<String, Any?>>() ?: return@DataFetcher CompletableFuture.completedFuture(null)
            val deviceName = source["name"] as String
            val future = CompletableFuture<Map<String, Any?>>()
            val address = EventBusAddresses.TimeBaseLoggerBridge.connectorMetrics(deviceName)
            vertx.eventBus().request<JsonObject>(address, JsonObject()).onComplete { res ->
                if (res.succeeded()) future.complete(res.result().body().map)
                else future.complete(null)
            }
            future
        }
    }
}
