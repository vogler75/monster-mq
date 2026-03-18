package at.rocworks.graphql

import at.rocworks.Utils
import at.rocworks.agents.McpServerConfig
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

class McpServerQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(McpServerQueries::class.java)

    fun mcpServers(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher {
            val future = CompletableFuture<List<Map<String, Any?>>>()
            deviceStore.getAllDevices().onComplete { result ->
                if (result.succeeded()) {
                    val servers = result.result()
                        .filter { it.type == DeviceConfig.DEVICE_TYPE_MCP_SERVER }
                        .map { mcpServerToMap(it) }
                    future.complete(servers)
                } else {
                    logger.severe("Error fetching MCP servers: ${result.cause()?.message}")
                    future.complete(emptyList())
                }
            }
            future
        }
    }

    fun mcpServer(): DataFetcher<CompletableFuture<Map<String, Any?>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>?>()
            val name = env.getArgument<String>("name")!!
            deviceStore.getDevice(name).onComplete { result ->
                if (result.succeeded()) {
                    val device = result.result()
                    if (device != null && device.type == DeviceConfig.DEVICE_TYPE_MCP_SERVER) {
                        future.complete(mcpServerToMap(device))
                    } else {
                        future.complete(null)
                    }
                } else {
                    future.complete(null)
                }
            }
            future
        }
    }

    companion object {
        fun mcpServerToMap(device: DeviceConfig): Map<String, Any?> {
            val config = McpServerConfig.fromJsonObject(device.config)
            return mapOf(
                "name" to device.name,
                "namespace" to device.namespace,
                "nodeId" to device.nodeId,
                "enabled" to device.enabled,
                "url" to config.url,
                "transport" to config.transport,
                "createdAt" to device.createdAt.toString(),
                "updatedAt" to device.updatedAt.toString()
            )
        }
    }
}
