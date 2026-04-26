package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Features
import at.rocworks.Utils
import at.rocworks.agents.McpServerConfig
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

class McpServerMutations(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(McpServerMutations::class.java)

    fun createMcpServer(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            if (!Monster.isFeatureEnabled(Features.Mcp))
                return@DataFetcher future.apply { complete(mapOf("success" to false, "message" to "Mcp feature is not enabled on this node")) }
            try {
                val input = env.getArgument<Map<String, Any>>("input")!!
                val deviceConfig = inputToDeviceConfig(input)
                deviceStore.saveDevice(deviceConfig).onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(McpServerQueries.mcpServerToMap(result.result()))
                    } else {
                        future.completeExceptionally(result.cause())
                    }
                }
            } catch (e: Exception) {
                future.completeExceptionally(e)
            }
            future
        }
    }

    fun updateMcpServer(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            if (!Monster.isFeatureEnabled(Features.Mcp))
                return@DataFetcher future.apply { complete(mapOf("success" to false, "message" to "Mcp feature is not enabled on this node")) }
            try {
                val name = env.getArgument<String>("name")!!
                val input = env.getArgument<Map<String, Any>>("input")!!

                deviceStore.getDevice(name).onComplete { getResult ->
                    if (getResult.succeeded() && getResult.result() != null) {
                        val existing = getResult.result()!!
                        val existingConfig = McpServerConfig.fromJsonObject(existing.config)

                        val updatedConfig = McpServerConfig(
                            url = input["url"] as? String ?: existingConfig.url,
                            transport = input["transport"] as? String ?: existingConfig.transport
                        )

                        val updated = existing.copy(
                            namespace = input["namespace"] as? String ?: existing.namespace,
                            nodeId = input["nodeId"] as? String ?: existing.nodeId,
                            enabled = input["enabled"] as? Boolean ?: existing.enabled,
                            config = updatedConfig.toJsonObject(),
                            updatedAt = Instant.now()
                        )

                        deviceStore.saveDevice(updated).onComplete { saveResult ->
                            if (saveResult.succeeded()) {
                                future.complete(McpServerQueries.mcpServerToMap(saveResult.result()))
                            } else {
                                future.completeExceptionally(saveResult.cause())
                            }
                        }
                    } else {
                        future.completeExceptionally(RuntimeException("MCP Server not found: $name"))
                    }
                }
            } catch (e: Exception) {
                future.completeExceptionally(e)
            }
            future
        }
    }

    fun deleteMcpServer(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Boolean>()
            if (!Monster.isFeatureEnabled(Features.Mcp))
                return@DataFetcher future.apply { complete(false) }
            try {
                val name = env.getArgument<String>("name")!!
                deviceStore.deleteDevice(name).onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                    } else {
                        future.completeExceptionally(result.cause())
                    }
                }
            } catch (e: Exception) {
                future.completeExceptionally(e)
            }
            future
        }
    }

    private fun inputToDeviceConfig(input: Map<String, Any>): DeviceConfig {
        val config = McpServerConfig(
            url = input["url"] as String,
            transport = input["transport"] as? String ?: "http"
        )
        return DeviceConfig(
            name = input["name"] as String,
            namespace = input["namespace"] as? String ?: "mcp",
            nodeId = input["nodeId"] as? String ?: "*",
            config = config.toJsonObject(),
            enabled = input["enabled"] as? Boolean ?: true,
            type = DeviceConfig.DEVICE_TYPE_MCP_SERVER,
            createdAt = Instant.now(),
            updatedAt = Instant.now()
        )
    }
}
