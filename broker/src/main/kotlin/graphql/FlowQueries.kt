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
                        "type" to "database",
                        "category" to "integration",
                        "description" to "Execute SQL queries against JDBC databases",
                        "defaultInputs" to listOf("trigger", "sql", "arguments"),
                        "defaultOutputs" to listOf("result", "error"),
                        "configSchema" to mapOf(
                            "jdbcUrl" to mapOf(
                                "type" to "string",
                                "description" to "JDBC connection URL (e.g., jdbc:postgresql://localhost:5432/db, jdbc:mysql://localhost/db, jdbc:neo4j:bolt://localhost:7687)"
                            ),
                            "username" to mapOf(
                                "type" to "string",
                                "description" to "Database username"
                            ),
                            "password" to mapOf(
                                "type" to "string",
                                "description" to "Database password"
                            ),
                            "sqlStatement" to mapOf(
                                "type" to "string",
                                "description" to "SQL query or DML statement (optional, can be provided via input)"
                            ),
                            "connectionMode" to mapOf(
                                "type" to "string",
                                "enum" to listOf("PER_TRIGGER", "FLOW_INSTANCE"),
                                "description" to "Connection lifecycle mode"
                            ),
                            "enableDynamicSql" to mapOf(
                                "type" to "boolean",
                                "description" to "Allow SQL statement to be provided via input"
                            ),
                            "healthCheckInterval" to mapOf(
                                "type" to "number",
                                "description" to "Connection health check interval in milliseconds (0 to disable)"
                            )
                        ),
                        "icon" to "database"
                    ),
                    mapOf(
                        "type" to "timer",
                        "category" to "source",
                        "description" to "Periodically publish a value based on a frequency",
                        "defaultInputs" to listOf<String>(),
                        "defaultOutputs" to listOf("tick"),
                        "configSchema" to mapOf(
                            "frequency" to mapOf(
                                "type" to "number",
                                "description" to "Frequency in milliseconds between publications (e.g., 1000 for every second)"
                            ),
                            "value" to mapOf(
                                "type" to "string",
                                "description" to "Optional fixed value to publish on each tick (JSON or plain text)"
                            ),
                            "autoStart" to mapOf(
                                "type" to "boolean",
                                "description" to "Start timer automatically when flow instance starts"
                            )
                        ),
                        "icon" to "clock"
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
