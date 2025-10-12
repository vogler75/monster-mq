package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.*
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL queries for Flow Engine
 */
class FlowQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(FlowQueries::class.java)

    /**
     * Query flow classes
     */
    fun flowClasses(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any>>>()

            try {
                val name = env.getArgument<String?>("name")

                if (name != null) {
                    // Filter by specific name
                    deviceStore.getDevice(name).onComplete { result ->
                        if (result.succeeded()) {
                            val device = result.result()
                            if (device != null && device.type == DeviceConfig.DEVICE_TYPE_FLOW_CLASS) {
                                future.complete(listOf(flowClassToMap(device)))
                            } else {
                                future.complete(emptyList())
                            }
                        } else {
                            logger.severe("Error fetching flow class: ${result.cause()?.message}")
                            future.complete(emptyList())
                        }
                    }
                } else {
                    // Return all flow classes
                    deviceStore.getAllDevices().onComplete { result ->
                        if (result.succeeded()) {
                            val flowMaps = result.result()
                                .filter { it.type == DeviceConfig.DEVICE_TYPE_FLOW_CLASS }
                                .map { flowClassToMap(it) }
                            future.complete(flowMaps)
                        } else {
                            logger.severe("Error fetching flow classes: ${result.cause()?.message}")
                            future.complete(emptyList())
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching flow classes: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    /**
     * Query a specific flow class
     */
    fun flowClass(): DataFetcher<CompletableFuture<Map<String, Any>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>?>()

            try {
                val name = env.getArgument<String>("name")!!

                deviceStore.getDevice(name).onComplete { result ->
                    if (result.succeeded()) {
                        val device = result.result()
                        if (device != null && device.type == DeviceConfig.DEVICE_TYPE_FLOW_CLASS) {
                            future.complete(flowClassToMap(device))
                        } else {
                            future.complete(null)
                        }
                    } else {
                        logger.severe("Error fetching flow class: ${result.cause()?.message}")
                        future.complete(null)
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching flow class: ${e.message}")
                future.complete(null)
            }

            future
        }
    }

    /**
     * Query flow instances
     */
    fun flowInstances(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any>>>()

            try {
                val flowClassId = env.getArgument<String?>("flowClassId")
                val nodeId = env.getArgument<String?>("nodeId")
                val enabled = env.getArgument<Boolean?>("enabled")

                deviceStore.getAllDevices().onComplete { result ->
                    if (result.succeeded()) {
                        var flows = result.result()
                            .filter { it.type == DeviceConfig.DEVICE_TYPE_FLOW_OBJECT }

                        // Apply filters
                        if (flowClassId != null) {
                            flows = flows.filter {
                                it.config.getString("flowClassId") == flowClassId
                            }
                        }
                        if (nodeId != null) {
                            flows = flows.filter { it.nodeId == nodeId }
                        }
                        if (enabled != null) {
                            flows = flows.filter { it.enabled == enabled }
                        }

                        @Suppress("UNCHECKED_CAST")
                        val flowMaps = flows.map { flowInstanceToMap(it) as Map<String, Any> }
                        future.complete(flowMaps)
                    } else {
                        logger.severe("Error fetching flow instances: ${result.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching flow instances: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    /**
     * Query a specific flow instance
     */
    fun flowInstance(): DataFetcher<CompletableFuture<Map<String, Any>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>?>()

            try {
                val name = env.getArgument<String>("name")!!

                deviceStore.getDevice(name).onComplete { result ->
                    if (result.succeeded()) {
                        val device = result.result()
                        if (device != null && device.type == DeviceConfig.DEVICE_TYPE_FLOW_OBJECT) {
                            @Suppress("UNCHECKED_CAST")
                            future.complete(flowInstanceToMap(device) as Map<String, Any>?)
                        } else {
                            future.complete(null)
                        }
                    } else {
                        logger.severe("Error fetching flow instance: ${result.cause()?.message}")
                        future.complete(null)
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching flow instance: ${e.message}")
                future.complete(null)
            }

            future
        }
    }

    /**
     * Query available flow node types
     */
    fun flowNodeTypes(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { _ ->
            val future = CompletableFuture<List<Map<String, Any>>>()

            try {
                val nodeTypes = listOf(
                    mapOf(
                        "type" to "function",
                        "category" to "function",
                        "description" to "Execute custom JavaScript code",
                        "defaultInputs" to listOf<String>(),
                        "defaultOutputs" to listOf<String>(),
                        "configSchema" to mapOf(
                            "script" to mapOf(
                                "type" to "string",
                                "description" to "JavaScript code to execute",
                                "required" to true
                            )
                        ),
                        "icon" to "code"
                    ),
                    mapOf(
                        "type" to "switch",
                        "category" to "function",
                        "description" to "Route messages based on conditions",
                        "defaultInputs" to listOf("in"),
                        "defaultOutputs" to listOf<String>(),
                        "configSchema" to mapOf(
                            "property" to mapOf(
                                "type" to "string",
                                "description" to "Property to evaluate"
                            ),
                            "rules" to mapOf(
                                "type" to "array",
                                "description" to "Routing rules"
                            )
                        ),
                        "icon" to "fork"
                    ),
                    mapOf(
                        "type" to "change",
                        "category" to "function",
                        "description" to "Modify message properties",
                        "defaultInputs" to listOf("in"),
                        "defaultOutputs" to listOf("out"),
                        "configSchema" to mapOf(
                            "rules" to mapOf(
                                "type" to "array",
                                "description" to "Transformation rules"
                            )
                        ),
                        "icon" to "edit"
                    ),
                    mapOf(
                        "type" to "template",
                        "category" to "function",
                        "description" to "Format strings using templates",
                        "defaultInputs" to listOf<String>(),
                        "defaultOutputs" to listOf("result"),
                        "configSchema" to mapOf(
                            "template" to mapOf(
                                "type" to "string",
                                "description" to "Template string with {{variables}}"
                            ),
                            "format" to mapOf(
                                "type" to "string",
                                "enum" to listOf("plain", "json", "yaml")
                            )
                        ),
                        "icon" to "file-text"
                    ),
                    mapOf(
                        "type" to "delay",
                        "category" to "function",
                        "description" to "Delay, throttle, or debounce messages",
                        "defaultInputs" to listOf("in"),
                        "defaultOutputs" to listOf("out"),
                        "configSchema" to mapOf(
                            "type" to mapOf(
                                "type" to "string",
                                "enum" to listOf("delay", "throttle", "debounce")
                            ),
                            "timeout" to mapOf(
                                "type" to "number",
                                "description" to "Delay in milliseconds"
                            )
                        ),
                        "icon" to "clock"
                    ),
                    mapOf(
                        "type" to "filter",
                        "category" to "function",
                        "description" to "Filter messages based on conditions",
                        "defaultInputs" to listOf("in"),
                        "defaultOutputs" to listOf("pass", "block"),
                        "configSchema" to mapOf(
                            "condition" to mapOf(
                                "type" to "string",
                                "description" to "Filter condition"
                            )
                        ),
                        "icon" to "filter"
                    )
                )

                future.complete(nodeTypes)
            } catch (e: Exception) {
                logger.severe("Error fetching node types: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    // Helper functions

    private fun flowClassToMap(device: DeviceConfig): Map<String, Any> {
        val flowClass = FlowClass.fromJsonObject(device.config)

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "version" to flowClass.version,
            "description" to (flowClass.description ?: ""),
            "nodes" to flowClass.nodes.map { nodeToMap(it) },
            "connections" to flowClass.connections.map { connectionToMap(it) },
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString()
        )
    }

    private fun nodeToMap(node: FlowNode): Map<String, Any> {
        return mapOf(
            "id" to node.id,
            "type" to node.type,
            "name" to node.name,
            "config" to node.config.map,
            "inputs" to node.inputs,
            "outputs" to node.outputs,
            "language" to node.language,
            "position" to (node.position?.let {
                mapOf("x" to it.x, "y" to it.y)
            } ?: mapOf("x" to 0.0, "y" to 0.0))
        )
    }

    private fun connectionToMap(connection: FlowConnection): Map<String, Any> {
        return mapOf(
            "fromNode" to connection.fromNode,
            "fromOutput" to connection.fromOutput,
            "toNode" to connection.toNode,
            "toInput" to connection.toInput
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun flowInstanceToMap(device: DeviceConfig): Map<String, Any> {
        val currentNodeId = Monster.getClusterNodeId(vertx) ?: "local"
        val flowInstance = FlowInstance.fromJsonObject(device.config)

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "flowClassId" to flowInstance.flowClassId,
            "inputMappings" to flowInstance.inputMappings.map { inputMappingToMap(it) },
            "outputMappings" to flowInstance.outputMappings.map { outputMappingToMap(it) },
            "variables" to (flowInstance.variables?.map ?: emptyMap<String, Any>()),
            "enabled" to device.enabled,
            "status" to (null as Any?),
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString(),
            "isOnCurrentNode" to device.isAssignedToNode(currentNodeId)
        ) as Map<String, Any>
    }

    private fun inputMappingToMap(mapping: FlowInputMapping): Map<String, Any> {
        return mapOf(
            "nodeInput" to mapping.nodeInput,
            "type" to mapping.type.name,
            "value" to mapping.value
        )
    }

    private fun outputMappingToMap(mapping: FlowOutputMapping): Map<String, Any> {
        return mapOf(
            "nodeOutput" to mapping.nodeOutput,
            "topic" to mapping.topic
        )
    }
}
