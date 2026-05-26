package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.Features
import at.rocworks.bus.EventBusAddresses
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.OpcUaConnectionConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL resolver for OPC UA Server address space browsing (client navigation)
 */
class OPCUABrowserResolver(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(OPCUABrowserResolver::class.java)

    fun opcuaServers(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any>>>()
            if (!Monster.isFeatureEnabled(Features.OpcUa)) {
                future.complete(emptyList())
                return@DataFetcher future
            }

            deviceStore.getAllDevices().onComplete { result ->
                if (result.succeeded()) {
                    val devices = result.result().filter { it.type == DeviceConfig.DEVICE_TYPE_OPCUA_CLIENT }
                    if (devices.isEmpty()) {
                        future.complete(emptyList())
                        return@onComplete
                    }

                    val futures = devices.map { device ->
                        val devConfig = OpcUaConnectionConfig.fromJsonObject(device.config)
                        val statusFuture = CompletableFuture<Map<String, Any>>()
                        
                        if (!device.enabled) {
                            statusFuture.complete(mapOf(
                                "id" to device.name,
                                "name" to device.name,
                                "endpoint" to devConfig.endpointUrl,
                                "enabled" to false,
                                "connected" to false
                            ))
                        } else {
                            val addr = EventBusAddresses.OpcUaBridge.connectorAction(device.name)
                            val options = DeliveryOptions().setSendTimeout(1000)
                            vertx.eventBus().request<JsonObject>(
                                addr, 
                                JsonObject().put("action", "status"),
                                options
                            ).onComplete { reply ->
                                val connected = if (reply.succeeded()) {
                                    reply.result().body().getBoolean("connected", false)
                                } else {
                                    false
                                }
                                statusFuture.complete(mapOf(
                                    "id" to device.name,
                                    "name" to device.name,
                                    "endpoint" to devConfig.endpointUrl,
                                    "enabled" to true,
                                    "connected" to connected
                                ))
                            }
                        }
                        statusFuture
                    }

                    CompletableFuture.allOf(*futures.toTypedArray()).thenAccept {
                        val serverList = futures.map { it.join() }
                        future.complete(serverList)
                    }
                } else {
                    logger.severe("Error fetching OPC UA devices for browser: ${result.cause()?.message}")
                    future.complete(emptyList())
                }
            }

            future
        }
    }

    fun opcuaNodeBrowse(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any>>>()
            if (!Monster.isFeatureEnabled(Features.OpcUa)) {
                future.complete(emptyList())
                return@DataFetcher future
            }

            val serverId = env.getArgument<String>("serverId") ?: throw IllegalArgumentException("serverId is required")
            val nodeId = env.getArgument<String>("nodeId") ?: "ns=0;i=84"

            val addr = EventBusAddresses.OpcUaBridge.connectorAction(serverId)
            val request = JsonObject()
                .put("action", "browse")
                .put("nodeId", nodeId)

            val options = DeliveryOptions().setSendTimeout(5000)
            vertx.eventBus().request<JsonObject>(addr, request, options).onComplete { reply ->
                if (reply.succeeded()) {
                    val body = reply.result().body()
                    val nodesJson = body.getJsonArray("nodes") ?: JsonArray()
                    val nodesList = nodesJson.map { it as JsonObject }.map { node ->
                        mapOf(
                            "nodeId" to node.getString("nodeId"),
                            "browseName" to node.getString("browseName"),
                            "displayName" to node.getString("displayName"),
                            "nodeClass" to node.getString("nodeClass"),
                            "description" to node.getString("description", ""),
                            "dataType" to node.getString("dataType", ""),
                            "value" to node.getString("value", ""),
                            "hasChildren" to node.getBoolean("hasChildren", false),
                            "writable" to node.getBoolean("writable", false),
                            "timestamp" to node.getString("timestamp", "")
                        )
                    }
                    future.complete(nodesList)
                } else {
                    val cause = reply.cause()?.message ?: "Unknown error"
                    logger.warning("Failed to browse OPC UA node $nodeId on server $serverId: $cause")
                    future.completeExceptionally(Exception("Failed to browse: $cause"))
                }
            }

            future
        }
    }

    fun opcuaNodeRead(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            if (!Monster.isFeatureEnabled(Features.OpcUa)) {
                future.complete(emptyMap())
                return@DataFetcher future
            }

            val serverId = env.getArgument<String>("serverId") ?: throw IllegalArgumentException("serverId is required")
            val nodeId = env.getArgument<String>("nodeId") ?: ""

            val addr = EventBusAddresses.OpcUaBridge.connectorAction(serverId)
            val request = JsonObject()
                .put("action", "read")
                .put("nodeId", nodeId)

            val options = DeliveryOptions().setSendTimeout(5000)
            vertx.eventBus().request<JsonObject>(addr, request, options).onComplete { reply ->
                if (reply.succeeded()) {
                    val body = reply.result().body()
                    val nodeMap = mapOf(
                        "nodeId" to body.getString("nodeId"),
                        "browseName" to body.getString("browseName"),
                        "displayName" to body.getString("displayName"),
                        "nodeClass" to body.getString("nodeClass"),
                        "description" to body.getString("description", ""),
                        "dataType" to body.getString("dataType", ""),
                        "value" to body.getString("value", ""),
                        "hasChildren" to body.getBoolean("hasChildren", false),
                        "writable" to body.getBoolean("writable", false),
                        "timestamp" to body.getString("timestamp", "")
                    )
                    future.complete(nodeMap)
                } else {
                    val cause = reply.cause()?.message ?: "Unknown error"
                    logger.warning("Failed to read OPC UA node $nodeId on server $serverId: $cause")
                    future.completeExceptionally(Exception("Failed to read: $cause"))
                }
            }

            future
        }
    }
}
