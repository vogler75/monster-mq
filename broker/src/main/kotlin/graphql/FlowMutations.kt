package at.rocworks.graphql

import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.*
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL mutations for Flow Engine
 */
class FlowMutations(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(FlowMutations::class.java)

    /**
     * Create a new flow class
     */
    fun createClass(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val input = env.getArgument<Map<String, Any>>("input")!!
                val name = input["name"] as String
                val namespace = input["namespace"] as String
                val version = input["version"] as? String ?: "1.0"
                val description = input["description"] as? String
                @Suppress("UNCHECKED_CAST")
                val nodesInput = input["nodes"] as List<Map<String, Any>>
                @Suppress("UNCHECKED_CAST")
                val connectionsInput = input["connections"] as List<Map<String, Any>>

                // Convert to FlowClass
                val nodes = nodesInput.map { parseFlowNode(it) }
                val connections = connectionsInput.map { parseFlowConnection(it) }

                val flowClass = FlowClass(
                    version = version,
                    description = description,
                    nodes = nodes,
                    connections = connections
                )

                // Create DeviceConfig
                val deviceConfig = DeviceConfig(
                    name = name,
                    namespace = namespace,
                    nodeId = "*",  // Flow classes are not node-specific
                    config = flowClass.toJsonObject(),
                    enabled = true,
                    type = DeviceConfig.DEVICE_TYPE_FLOW_CLASS,
                    createdAt = Instant.now(),
                    updatedAt = Instant.now()
                )

                // Save to store
                deviceStore.saveDevice(deviceConfig).onComplete { result ->
                    if (result.succeeded()) {
                        val saved = result.result()
                        future.complete(flowClassToMap(saved))

                        // Notify FlowEngine about the new class
                        notifyFlowConfigChange("add", saved)
                    } else {
                        logger.severe("Error creating flow class: ${result.cause()?.message}")
                        future.completeExceptionally(result.cause())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error creating flow class: ${e.message}")
                future.completeExceptionally(e)
            }

            future
        }
    }

    /**
     * Update an existing flow class
     */
    fun updateClass(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val name = env.getArgument<String>("name")!!
                val input = env.getArgument<Map<String, Any>>("input")!!

                deviceStore.getDevice(name).onComplete { getResult ->
                    if (getResult.succeeded() && getResult.result() != null) {
                        val existing = getResult.result()!!

                        // Parse updated flow class data
                        val namespace = input["namespace"] as? String ?: existing.namespace
                        val version = input["version"] as? String ?: "1.0"
                        val description = input["description"] as? String
                        @Suppress("UNCHECKED_CAST")
                        val nodesInput = input["nodes"] as List<Map<String, Any>>
                        @Suppress("UNCHECKED_CAST")
                        val connectionsInput = input["connections"] as List<Map<String, Any>>

                        val nodes = nodesInput.map { parseFlowNode(it) }
                        val connections = connectionsInput.map { parseFlowConnection(it) }

                        val flowClass = FlowClass(
                            version = version,
                            description = description,
                            nodes = nodes,
                            connections = connections
                        )

                        val updated = existing.copy(
                            namespace = namespace,
                            config = flowClass.toJsonObject(),
                            updatedAt = Instant.now()
                        )

                        deviceStore.saveDevice(updated).onComplete { saveResult ->
                            if (saveResult.succeeded()) {
                                future.complete(flowClassToMap(saveResult.result()))

                                // Notify FlowEngine about the update
                                notifyFlowConfigChange("update", saveResult.result())
                            } else {
                                future.completeExceptionally(saveResult.cause())
                            }
                        }
                    } else {
                        future.completeExceptionally(Exception("Flow class not found: $name"))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error updating flow class: ${e.message}")
                future.completeExceptionally(e)
            }

            future
        }
    }

    /**
     * Delete a flow class and all its instances (cascading delete)
     */
    fun deleteClass(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Boolean>()

            try {
                val name = env.getArgument<String>("name")!!

                // First, find and delete all flow instances that reference this class
                deviceStore.getAllDevices().onComplete { devicesResult ->
                    if (devicesResult.succeeded()) {
                        val instances = devicesResult.result()
                            .filter { it.type == DeviceConfig.DEVICE_TYPE_FLOW_OBJECT }
                            .filter { it.config.getString("flowClassId") == name }

                        // Delete all instances that reference this class
                        val deleteInstanceFutures = instances.map { instance ->
                            deviceStore.deleteDevice(instance.name).onComplete { deleteResult ->
                                if (deleteResult.succeeded()) {
                                    // Notify FlowEngine about instance deletion
                                    val notification = JsonObject()
                                        .put("operation", "delete")
                                        .put("deviceName", instance.name)
                                        .put("deviceType", DeviceConfig.DEVICE_TYPE_FLOW_OBJECT)
                                    vertx.eventBus().publish("flowengine.flow.config.changed", notification)
                                    logger.info("Deleted orphaned flow instance: ${instance.name}")
                                }
                            }
                        }

                        // After deleting instances, delete the class itself
                        deviceStore.deleteDevice(name).onComplete { result ->
                            if (result.succeeded()) {
                                future.complete(result.result())

                                // Notify FlowEngine
                                val notification = JsonObject()
                                    .put("operation", "delete")
                                    .put("deviceName", name)
                                    .put("deviceType", DeviceConfig.DEVICE_TYPE_FLOW_CLASS)

                                vertx.eventBus().publish("flowengine.flow.config.changed", notification)

                                if (instances.isNotEmpty()) {
                                    logger.info("Deleted flow class '$name' and ${instances.size} associated instance(s)")
                                }
                            } else {
                                future.completeExceptionally(result.cause())
                            }
                        }
                    } else {
                        // If we can't get devices, still try to delete the class
                        logger.warning("Could not fetch devices to check for orphaned instances: ${devicesResult.cause()?.message}")
                        deviceStore.deleteDevice(name).onComplete { result ->
                            if (result.succeeded()) {
                                future.complete(result.result())

                                val notification = JsonObject()
                                    .put("operation", "delete")
                                    .put("deviceName", name)
                                    .put("deviceType", DeviceConfig.DEVICE_TYPE_FLOW_CLASS)

                                vertx.eventBus().publish("flowengine.flow.config.changed", notification)
                            } else {
                                future.completeExceptionally(result.cause())
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                future.completeExceptionally(e)
            }

            future
        }
    }

    /**
     * Create a new flow instance
     */
    fun createInstance(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val input = env.getArgument<Map<String, Any>>("input")!!
                val name = input["name"] as String
                val namespace = input["namespace"] as String
                val nodeId = input["nodeId"] as String
                val flowClassId = input["flowClassId"] as String
                val enabled = input["enabled"] as? Boolean ?: true

                // Parse input/output mappings
                @Suppress("UNCHECKED_CAST")
                val inputMappingsData = input["inputMappings"] as List<Map<String, Any>>
                @Suppress("UNCHECKED_CAST")
                val outputMappingsData = input["outputMappings"] as List<Map<String, Any>>

                val inputMappings = inputMappingsData.map { parseInputMapping(it) }
                val outputMappings = outputMappingsData.map { parseOutputMapping(it) }

                // Parse variables
                @Suppress("UNCHECKED_CAST")
                val variablesData = input["variables"] as? Map<String, Any>
                val variables = variablesData?.let { JsonObject(it) }

                val flowInstance = FlowInstance(
                    flowClassId = flowClassId,
                    inputMappings = inputMappings,
                    outputMappings = outputMappings,
                    variables = variables
                )

                // Create DeviceConfig
                val deviceConfig = DeviceConfig(
                    name = name,
                    namespace = namespace,
                    nodeId = nodeId,
                    config = flowInstance.toJsonObject(),
                    enabled = enabled,
                    type = DeviceConfig.DEVICE_TYPE_FLOW_OBJECT,
                    createdAt = Instant.now(),
                    updatedAt = Instant.now()
                )

                // Save to store
                deviceStore.saveDevice(deviceConfig).onComplete { result ->
                    if (result.succeeded()) {
                        val saved = result.result()
                        future.complete(flowInstanceToMap(saved))

                        // Notify FlowEngine
                        notifyFlowConfigChange("add", saved)
                    } else {
                        future.completeExceptionally(result.cause())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error creating flow instance: ${e.message}")
                future.completeExceptionally(e)
            }

            future
        }
    }

    /**
     * Update an existing flow instance
     */
    fun updateInstance(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val name = env.getArgument<String>("name")!!
                val input = env.getArgument<Map<String, Any>>("input")!!

                deviceStore.getDevice(name).onComplete { getResult ->
                    if (getResult.succeeded() && getResult.result() != null) {
                        val existing = getResult.result()!!

                        // Parse updated data
                        val namespace = input["namespace"] as? String ?: existing.namespace
                        val nodeId = input["nodeId"] as? String ?: existing.nodeId
                        val flowClassId = input["flowClassId"] as String
                        val enabled = input["enabled"] as? Boolean ?: existing.enabled

                        @Suppress("UNCHECKED_CAST")
                        val inputMappingsData = input["inputMappings"] as List<Map<String, Any>>
                        @Suppress("UNCHECKED_CAST")
                        val outputMappingsData = input["outputMappings"] as List<Map<String, Any>>

                        val inputMappings = inputMappingsData.map { parseInputMapping(it) }
                        val outputMappings = outputMappingsData.map { parseOutputMapping(it) }

                        @Suppress("UNCHECKED_CAST")
                        val variablesData = input["variables"] as? Map<String, Any>
                        val variables = variablesData?.let { JsonObject(it) }

                        val flowInstance = FlowInstance(
                            flowClassId = flowClassId,
                            inputMappings = inputMappings,
                            outputMappings = outputMappings,
                            variables = variables
                        )

                        val updated = existing.copy(
                            namespace = namespace,
                            nodeId = nodeId,
                            config = flowInstance.toJsonObject(),
                            enabled = enabled,
                            updatedAt = Instant.now()
                        )

                        deviceStore.saveDevice(updated).onComplete { saveResult ->
                            if (saveResult.succeeded()) {
                                future.complete(flowInstanceToMap(saveResult.result()))
                                notifyFlowConfigChange("update", saveResult.result())
                            } else {
                                future.completeExceptionally(saveResult.cause())
                            }
                        }
                    } else {
                        future.completeExceptionally(Exception("Flow instance not found: $name"))
                    }
                }
            } catch (e: Exception) {
                future.completeExceptionally(e)
            }

            future
        }
    }

    /**
     * Delete a flow instance
     */
    fun deleteInstance(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Boolean>()

            try {
                val name = env.getArgument<String>("name")!!

                deviceStore.deleteDevice(name).onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())

                        val notification = JsonObject()
                            .put("operation", "delete")
                            .put("deviceName", name)
                            .put("deviceType", DeviceConfig.DEVICE_TYPE_FLOW_OBJECT)

                        vertx.eventBus().publish("flowengine.flow.config.changed", notification)
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

    /**
     * Enable a flow instance
     */
    fun enableInstance(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            toggleInstanceEnabled(env.getArgument<String>("name")!!, true)
        }
    }

    /**
     * Disable a flow instance
     */
    fun disableInstance(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            toggleInstanceEnabled(env.getArgument<String>("name")!!, false)
        }
    }

    /**
     * Reassign a flow instance to a different node
     */
    fun reassignInstance(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val name = env.getArgument<String>("name")!!
                val newNodeId = env.getArgument<String>("nodeId")!!

                deviceStore.reassignDevice(name, newNodeId).onComplete { result ->
                    if (result.succeeded() && result.result() != null) {
                        future.complete(flowInstanceToMap(result.result()!!))

                        val notification = JsonObject()
                            .put("operation", "reassign")
                            .put("deviceName", name)
                            .put("deviceType", DeviceConfig.DEVICE_TYPE_FLOW_OBJECT)
                            .put("nodeId", newNodeId)

                        vertx.eventBus().publish("flowengine.flow.config.changed", notification)
                    } else {
                        future.completeExceptionally(Exception("Failed to reassign flow instance"))
                    }
                }
            } catch (e: Exception) {
                future.completeExceptionally(e)
            }

            future
        }
    }

    /**
     * Test a node with sample inputs
     */
    fun testNode(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                // TODO: Implement node testing
                future.complete(mapOf(
                    "success" to true,
                    "outputs" to emptyMap<String, Any>(),
                    "logs" to emptyList<String>(),
                    "errors" to emptyList<String>(),
                    "executionTime" to 0
                ))
            } catch (e: Exception) {
                future.completeExceptionally(e)
            }

            future
        }
    }

    // Helper functions

    private fun toggleInstanceEnabled(name: String, enabled: Boolean): CompletableFuture<Map<String, Any>> {
        val future = CompletableFuture<Map<String, Any>>()

        deviceStore.toggleDevice(name, enabled).onComplete { result ->
            if (result.succeeded() && result.result() != null) {
                future.complete(flowInstanceToMap(result.result()!!))

                val notification = JsonObject()
                    .put("operation", "toggle")
                    .put("deviceName", name)
                    .put("deviceType", DeviceConfig.DEVICE_TYPE_FLOW_OBJECT)
                    .put("enabled", enabled)

                vertx.eventBus().publish("flowengine.flow.config.changed", notification)
            } else {
                future.completeExceptionally(Exception("Failed to toggle flow instance"))
            }
        }

        return future
    }

    private fun notifyFlowConfigChange(operation: String, device: DeviceConfig) {
        val notification = JsonObject()
            .put("operation", operation)
            .put("deviceName", device.name)
            .put("deviceType", device.type)
            .put("device", device.toJsonObject())

        vertx.eventBus().publish("flowengine.flow.config.changed", notification)
    }

    private fun parseFlowNode(data: Map<String, Any>): FlowNode {
        @Suppress("UNCHECKED_CAST")
        val position = (data["position"] as? Map<String, Any>)?.let {
            FlowPosition(
                x = (it["x"] as Number).toDouble(),
                y = (it["y"] as Number).toDouble()
            )
        }

        @Suppress("UNCHECKED_CAST")
        return FlowNode(
            id = data["id"] as String,
            type = data["type"] as String,
            name = data["name"] as String,
            config = JsonObject(data["config"] as? Map<String, Any> ?: emptyMap()),
            inputs = (data["inputs"] as? List<String>) ?: emptyList(),
            outputs = (data["outputs"] as? List<String>) ?: emptyList(),
            language = data["language"] as? String ?: "javascript",
            position = position
        )
    }

    private fun parseFlowConnection(data: Map<String, Any>): FlowConnection {
        return FlowConnection(
            fromNode = data["fromNode"] as String,
            fromOutput = data["fromOutput"] as String,
            toNode = data["toNode"] as String,
            toInput = data["toInput"] as String
        )
    }

    private fun parseInputMapping(data: Map<String, Any>): FlowInputMapping {
        return FlowInputMapping(
            nodeInput = data["nodeInput"] as String,
            type = FlowInputType.valueOf(data["type"] as String),
            value = data["value"] as String
        )
    }

    private fun parseOutputMapping(data: Map<String, Any>): FlowOutputMapping {
        return FlowOutputMapping(
            nodeOutput = data["nodeOutput"] as String,
            topic = data["topic"] as String
        )
    }

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
            "isOnCurrentNode" to false // TODO: Get from Monster
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
