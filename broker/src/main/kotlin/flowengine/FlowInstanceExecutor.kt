package at.rocworks.flowengine

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.devices.*
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

/**
 * Executes a single flow instance as a Vert.x verticle
 * Each flow instance is deployed as its own verticle with direct MQTT subscriptions
 */
class FlowInstanceExecutor(
    private val instanceConfig: DeviceConfig,
    private val flowClassConfig: DeviceConfig
) : AbstractVerticle() {

    companion object {
        private val logger: Logger = Utils.getLogger(FlowInstanceExecutor::class.java)
    }

    private val flowInstance: FlowInstance = FlowInstance.fromJsonObject(instanceConfig.config)
    private val flowClass: FlowClass = FlowClass.fromJsonObject(flowClassConfig.config)

    // Runtime state
    private val topicValues = ConcurrentHashMap<String, TopicValue>()
    private val nodeStates = ConcurrentHashMap<String, MutableMap<String, Any>>()
    private val scriptEngines = ConcurrentHashMap<String, FlowScriptEngine>()

    // Status tracking
    private var executionCount: Long = 0
    private var errorCount: Long = 0
    private var lastExecution: Instant? = null
    private var lastError: String? = null

    // Input mappings organized by type
    private val topicInputMappings: Map<String, String> // "nodeId.inputName" -> "mqtt/topic"
    private val textInputMappings: Map<String, String>  // "nodeId.inputName" -> "constant value"
    private val topicToNodeInputs: Map<String, List<String>> // "mqtt/topic" -> ["nodeId.inputName", ...]

    init {
        // Organize input mappings by type
        val topicMap = mutableMapOf<String, String>()
        val textMap = mutableMapOf<String, String>()
        val topicToInputs = mutableMapOf<String, MutableList<String>>()

        flowInstance.inputMappings.forEach { mapping ->
            when (mapping.type) {
                FlowInputType.TOPIC -> {
                    topicMap[mapping.nodeInput] = mapping.value
                    topicToInputs.getOrPut(mapping.value) { mutableListOf() }.add(mapping.nodeInput)
                }
                FlowInputType.TEXT -> {
                    textMap[mapping.nodeInput] = mapping.value
                }
            }
        }

        topicInputMappings = topicMap
        textInputMappings = textMap
        topicToNodeInputs = topicToInputs
    }

    override fun start(startPromise: Promise<Void>) {
        try {
            logger.info("Starting flow instance verticle: ${instanceConfig.name}")
            logger.info("  - Topic inputs: ${topicInputMappings.size}")
            logger.info("  - Text inputs: ${textInputMappings.size}")
            logger.info("  - Subscribed topics: ${topicToNodeInputs.keys}")

            // Subscribe to MQTT topics
            subscribeToTopics()

            logger.info("Flow instance verticle ${instanceConfig.name} started successfully")
            startPromise.complete()

        } catch (e: Exception) {
            logger.severe("Failed to start flow instance ${instanceConfig.name}: ${e.message}")
            e.printStackTrace()
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        try {
            logger.info("Stopping flow instance verticle: ${instanceConfig.name}")

            // Unsubscribe from MQTT topics
            unsubscribeFromTopics()

            // Close script engines
            scriptEngines.values.forEach { it.close() }
            scriptEngines.clear()

            logger.info("Flow instance verticle ${instanceConfig.name} stopped successfully")
            stopPromise.complete()

        } catch (e: Exception) {
            logger.severe("Error stopping flow instance ${instanceConfig.name}: ${e.message}")
            e.printStackTrace()
            stopPromise.fail(e)
        }
    }

    /**
     * Subscribe to MQTT topics via SessionHandler
     */
    private fun subscribeToTopics() {
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler == null) {
            logger.warning("SessionHandler not available, cannot subscribe to topics")
            return
        }

        val topics = topicToNodeInputs.keys
        topics.forEach { topic ->
            logger.info("  Subscribing to MQTT topic: $topic")
            sessionHandler.subscribeInternal(
                clientId = "flow-${instanceConfig.name}",
                topicFilter = topic,
                qos = 0
            ) { message ->
                handleMessage(message)
            }
        }
    }

    /**
     * Unsubscribe from MQTT topics
     */
    private fun unsubscribeFromTopics() {
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler == null) {
            logger.warning("SessionHandler not available, cannot unsubscribe from topics")
            return
        }

        val topics = topicToNodeInputs.keys
        topics.forEach { topic ->
            logger.info("  Unsubscribing from MQTT topic: $topic")
            sessionHandler.unsubscribeInternal(
                clientId = "flow-${instanceConfig.name}",
                topicFilter = topic
            )
        }
    }

    /**
     * Get list of topics this flow should subscribe to
     */
    fun getSubscribedTopics(): List<String> {
        return topicToNodeInputs.keys.toList()
    }

    /**
     * Handle incoming MQTT message
     */
    private fun handleMessage(message: BrokerMessage) {
        try {
            val topic = message.topicName

            // Store the topic value
            val topicValue = TopicValue(
                value = parsePayload(message.payload),
                timestamp = message.time.toEpochMilli(),
                qos = message.qosLevel
            )
            topicValues[topic] = topicValue

            logger.info("Flow ${instanceConfig.name}: Received message on topic $topic, payload: ${String(message.payload)}")
            logger.info("  Parsed value: ${topicValue.value}")

            // Find all node inputs mapped to this topic
            val nodeInputs = topicToNodeInputs[topic] ?: emptyList()
            logger.info("  Node inputs to trigger: $nodeInputs (found ${nodeInputs.size} mappings)")

            // Trigger execution for each affected node
            nodeInputs.forEach { nodeInput ->
                val (nodeId, _) = parseNodeInput(nodeInput)
                logger.info("  Executing node: $nodeId for input $nodeInput")
                executeNode(nodeId, topic)
            }

        } catch (e: Exception) {
            logger.severe("Error handling message in flow ${instanceConfig.name}: ${e.message}")
            e.printStackTrace()
            errorCount++
            lastError = e.message
        }
    }

    /**
     * Execute a specific node in the flow
     */
    private fun executeNode(nodeId: String, triggeringTopic: String? = null) {
        try {
            // Find the node definition
            val node = flowClass.nodes.find { it.id == nodeId }
            if (node == null) {
                logger.warning("Node $nodeId not found in flow class ${flowClassConfig.name}")
                return
            }

            logger.info("Executing node: $nodeId (${node.type}) in flow ${instanceConfig.name}")

            // Prepare inputs for this node
            val inputs = prepareNodeInputs(node, triggeringTopic)
            logger.info("  Prepared inputs: ${inputs.keys} (${inputs.size} inputs)")
            inputs.forEach { (name, value) ->
                logger.info("    $name: ${value.value} (type: ${value.type})")
            }

            // Get or create node state
            val nodeState = nodeStates.getOrPut(nodeId) { ConcurrentHashMap() }

            // Get flow variables
            val flowVars = flowInstance.variables?.map ?: emptyMap()

            // Execute node based on type
            when (node.type) {
                "function" -> {
                    logger.info("  Executing function node...")
                    executeFunctionNode(node, inputs, nodeState, flowVars)
                }
                else -> {
                    logger.warning("Unsupported node type: ${node.type}")
                }
            }

            // Update execution stats
            executionCount++
            lastExecution = Instant.now()

        } catch (e: Exception) {
            logger.severe("Error executing node $nodeId in flow ${instanceConfig.name}: ${e.message}")
            e.printStackTrace()
            errorCount++
            lastError = e.message
        }
    }

    /**
     * Prepare inputs for a node execution
     */
    private fun prepareNodeInputs(node: FlowNode, triggeringTopic: String?): Map<String, FlowScriptEngine.InputValue> {
        val inputs = mutableMapOf<String, FlowScriptEngine.InputValue>()

        // Add all node inputs
        node.inputs.forEach { inputName ->
            val nodeInput = "${node.id}.$inputName"

            // Check if this is a topic input
            val topicMapping = topicInputMappings[nodeInput]
            if (topicMapping != null) {
                val topicValue = topicValues[topicMapping]
                if (topicValue != null) {
                    inputs[inputName] = FlowScriptEngine.InputValue(
                        value = topicValue.value,
                        type = FlowScriptEngine.InputType.TOPIC,
                        timestamp = topicValue.timestamp,
                        topic = topicMapping
                    )
                }
            }

            // Check if this is a text input
            val textMapping = textInputMappings[nodeInput]
            if (textMapping != null) {
                inputs[inputName] = FlowScriptEngine.InputValue(
                    value = textMapping,
                    type = FlowScriptEngine.InputType.TEXT
                )
            }
        }

        return inputs
    }

    /**
     * Execute a function node (JavaScript or Python)
     */
    private fun executeFunctionNode(
        node: FlowNode,
        inputs: Map<String, FlowScriptEngine.InputValue>,
        nodeState: MutableMap<String, Any>,
        flowVars: Map<String, Any>
    ) {
        val script = node.config.getString("script", "")
        if (script.isBlank()) {
            logger.warning("Function node ${node.id} has no script")
            return
        }

        logger.info("  Script to execute:\n$script")
        logger.info("  Language: ${node.language}")

        // Execute everything in a blocking context to avoid GraalVM issues
        vertx.executeBlocking<FlowScriptEngine.ExecutionResult> {
            // Get or create script engine for this node
            // GraalVM Context initialization must happen on a worker thread, not event loop
            val scriptEngine = scriptEngines[node.id] ?: run {
                logger.info("  Creating new FlowScriptEngine for node ${node.id} on worker thread...")
                val engine = FlowScriptEngine()
                scriptEngines[node.id] = engine
                engine
            }

            logger.info("  Calling scriptEngine.execute()...")

            // Execute the script
            scriptEngine.execute(
                script = script,
                language = node.language,
                inputs = inputs,
                state = nodeState,
                flowVariables = flowVars,
                onOutput = { portName, value ->
                    logger.info("  Script called outputs.send($portName, $value)")
                    handleNodeOutput(node.id, portName, value)
                }
            )
        }.onComplete { asyncResult ->
            if (asyncResult.succeeded()) {
                val result = asyncResult.result()
                logger.info("  Script execution completed: success=${result.success}, logs=${result.logs.size}, errors=${result.errors.size}")

                if (!result.success) {
                    logger.severe("Script execution failed for node ${node.id}: ${result.errors}")
                    errorCount++
                    lastError = result.errors.joinToString(", ")
                }

                // Log console output
                result.logs.forEach { log ->
                    logger.info("Node ${node.id}: $log")
                }
            } else {
                logger.severe("Async script execution failed: ${asyncResult.cause()?.message}")
                asyncResult.cause()?.printStackTrace()
                errorCount++
                lastError = asyncResult.cause()?.message
            }
        }
    }

    /**
     * Handle output from a node
     */
    private fun handleNodeOutput(nodeId: String, portName: String, value: Any?) {
        try {
            val nodeOutput = "$nodeId.$portName"

            logger.fine("Node $nodeId output on port $portName: $value")

            // Check if this output is mapped to an MQTT topic
            val outputMapping = flowInstance.outputMappings.find { it.nodeOutput == nodeOutput }
            if (outputMapping != null) {
                publishToMqtt(outputMapping.topic, value)
            }

            // Find connections from this output
            val connections = flowClass.connections.filter {
                it.fromNode == nodeId && it.fromOutput == portName
            }

            // Trigger connected nodes
            connections.forEach { conn ->
                // Store the value so the target node can access it
                val targetNodeInput = "${conn.toNode}.${conn.toInput}"
                val topicValue = TopicValue(
                    value = value,
                    timestamp = System.currentTimeMillis(),
                    qos = 0
                )
                // Use a synthetic internal topic name for node-to-node communication
                val internalTopic = "flow:${instanceConfig.name}:$targetNodeInput"
                topicValues[internalTopic] = topicValue

                // Execute the target node
                executeNode(conn.toNode, internalTopic)
            }

        } catch (e: Exception) {
            logger.severe("Error handling node output: ${e.message}")
            e.printStackTrace()
        }
    }

    /**
     * Publish value to MQTT topic
     */
    private fun publishToMqtt(topic: String, value: Any?) {
        try {
            val payload = when (value) {
                is String -> value.toByteArray()
                is JsonObject -> value.encode().toByteArray()
                is Map<*, *> -> JsonObject(value as Map<String, Any>).encode().toByteArray()
                else -> value.toString().toByteArray()
            }

            val message = BrokerMessage(
                messageId = 0,
                topicName = topic,
                payload = payload,
                qosLevel = 0,
                isRetain = false,
                isDup = false,
                isQueued = false,
                clientId = instanceConfig.name,
                time = Instant.now()
            )

            // Publish via SessionHandler
            val sessionHandler = Monster.getSessionHandler()
            if (sessionHandler != null) {
                sessionHandler.publishMessage(message)
                logger.info("Published to MQTT topic $topic: $value")
            } else {
                logger.severe("SessionHandler not available for publishing")
            }

        } catch (e: Exception) {
            logger.severe("Error publishing to MQTT: ${e.message}")
            e.printStackTrace()
        }
    }

    /**
     * Get current status of this flow instance
     */
    fun getStatus(): FlowInstanceStatus {
        return FlowInstanceStatus(
            running = true,
            lastExecution = lastExecution,
            executionCount = executionCount,
            errorCount = errorCount,
            lastError = lastError,
            subscribedTopics = getSubscribedTopics()
        )
    }

    // Helper functions

    private fun parsePayload(payload: ByteArray): Any? {
        return try {
            val str = String(payload)
            // Try to parse as JSON
            try {
                JsonObject(str).map
            } catch (e: Exception) {
                // If not JSON, try as number
                str.toDoubleOrNull() ?: str
            }
        } catch (e: Exception) {
            null
        }
    }

    private fun parseNodeInput(nodeInput: String): Pair<String, String> {
        val parts = nodeInput.split(".", limit = 2)
        return if (parts.size == 2) {
            Pair(parts[0], parts[1])
        } else {
            Pair(nodeInput, "")
        }
    }
}
