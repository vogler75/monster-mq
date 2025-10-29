package at.rocworks.flowengine

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.data.TopicTree
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

    // Input mappings organized by topic
    private val topicInputMappings: Map<String, String> // "nodeId.inputName" -> "mqtt/topic"
    private val topicToNodeInputs: Map<String, List<String>> // "mqtt/topic" -> ["nodeId.inputName", ...]
    private val textInputMappings: Map<String, String> // "nodeId.inputName" -> "text value"

    init {
        // Organize input mappings by topic and text
        val topicMap = mutableMapOf<String, String>()
        val topicToInputs = mutableMapOf<String, MutableList<String>>()
        val textMap = mutableMapOf<String, String>()

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
        topicToNodeInputs = topicToInputs
        textInputMappings = textMap
    }

    override fun start(startPromise: Promise<Void>) {
        try {
            logger.info("[${instanceConfig.name}] Starting flow instance")
            logger.fine { "[${instanceConfig.name}]   - Topic inputs: ${topicInputMappings.size}" }
            logger.fine { "[${instanceConfig.name}]   - Subscribed topics: ${topicToNodeInputs.keys}" }

            // Initialize timer nodes with autoStart enabled
            initializeTimerNodes()

            // Subscribe to MQTT topics
            subscribeToTopics()

            logger.info("[${instanceConfig.name}] Flow instance started successfully")
            startPromise.complete()

        } catch (e: Exception) {
            logger.severe("[${instanceConfig.name}] Failed to start flow instance: ${e.message}")
            e.printStackTrace()
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        try {
            logger.info("[${instanceConfig.name}] Stopping flow instance")

            // Cancel all running timers
            nodeStates.values.forEach { nodeState ->
                val timerId = nodeState["timerId"] as? Long
                if (timerId != null) {
                    vertx.cancelTimer(timerId)
                }
            }

            // Unsubscribe from MQTT topics
            unsubscribeFromTopics()

            // Close script engines
            scriptEngines.values.forEach { it.close() }
            scriptEngines.clear()

            logger.info("[${instanceConfig.name}] Flow instance stopped successfully")
            stopPromise.complete()

        } catch (e: Exception) {
            logger.severe("[${instanceConfig.name}] Error stopping flow instance: ${e.message}")
            e.printStackTrace()
            stopPromise.fail(e)
        }
    }

    /**
     * Initialize all timer nodes - they always start with the flow
     */
    private fun initializeTimerNodes() {
        try {
            flowClass.nodes.filter { it.type == "timer" }.forEach { node ->
                val nodeState = nodeStates.getOrPut(node.id) { ConcurrentHashMap() }
                logger.fine { "[${instanceConfig.name}] Initializing timer node: ${node.id}" }
                setupTimerNode(node, nodeState)
            }
        } catch (e: Exception) {
            logger.warning("[${instanceConfig.name}] Error initializing timer nodes: ${e.message}")
        }
    }

    /**
     * Subscribe to MQTT topics via SessionHandler
     */
    private fun subscribeToTopics() {
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler == null) {
            logger.warning("[${instanceConfig.name}] SessionHandler not available, cannot subscribe to topics")
            return
        }

        val clientId = "flow-${instanceConfig.name}"

        // Register eventBus consumer for this flow instance to receive MQTT messages (individual or bulk)
        vertx.eventBus().consumer<Any>(EventBusAddresses.Client.messages(clientId)) { busMessage ->
            try {
                when (val body = busMessage.body()) {
                    is BrokerMessage -> handleMessage(body)
                    is at.rocworks.data.BulkClientMessage -> body.messages.forEach { handleMessage(it) }
                    else -> logger.warning("[${instanceConfig.name}] Unknown message type: ${body?.javaClass?.simpleName}")
                }
            } catch (e: Exception) {
                logger.warning("[${instanceConfig.name}] Error processing message: ${e.message}")
            }
        }

        // Subscribe to all topics via SessionHandler
        val topics = topicToNodeInputs.keys
        topics.forEach { topic ->
            logger.fine { "[${instanceConfig.name}]   Subscribing to MQTT topic: $topic" }
            sessionHandler.subscribeInternalClient(
                clientId = clientId,
                topicFilter = topic,
                qos = 0
            )
        }
    }

    /**
     * Unsubscribe from MQTT topics
     */
    private fun unsubscribeFromTopics() {
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler == null) {
            logger.warning("[${instanceConfig.name}] SessionHandler not available, cannot unsubscribe from topics")
            return
        }

        val clientId = "flow-${instanceConfig.name}"
        val topics = topicToNodeInputs.keys
        topics.forEach { topic ->
            logger.fine { "[${instanceConfig.name}]   Unsubscribing from MQTT topic: $topic" }
            sessionHandler.unsubscribeInternalClient(
                clientId = clientId,
                topicFilter = topic
            )
        }

        // Unregister the client when fully done
        sessionHandler.unregisterInternalClient(clientId)
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

            logger.fine { "[${instanceConfig.name}] Received message on topic $topic, payload: ${String(message.payload)}" }
            logger.fine { "[${instanceConfig.name}]   Parsed value: ${topicValue.value}" }

            // Find all node inputs mapped to this topic (supports wildcard subscriptions!)
            val nodeInputs = topicToNodeInputs.filter { (topicFilter, _) ->
                TopicTree.matches(topicFilter, topic)
            }.values.flatten()
            logger.fine { "[${instanceConfig.name}]   Node inputs to trigger: $nodeInputs (found ${nodeInputs.size} mappings)" }

            // Trigger execution for each affected node
            nodeInputs.forEach { nodeInput ->
                val (nodeId, _) = parseNodeInput(nodeInput)
                logger.fine { "[${instanceConfig.name}]   Executing node: $nodeId for input $nodeInput" }
                executeNode(nodeId, topic)
            }

        } catch (e: Exception) {
            logger.severe("[${instanceConfig.name}] Error handling message: ${e.message}")
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
                logger.warning("[${instanceConfig.name}] Node $nodeId not found in flow class ${flowClassConfig.name}")
                return
            }

            logger.fine { "[${instanceConfig.name}] Executing node: $nodeId (${node.type})" }

            // Prepare inputs for this node
            val inputs = prepareNodeInputs(node, triggeringTopic)
            logger.fine { "[${instanceConfig.name}]   Prepared inputs: ${inputs.keys} (${inputs.size} inputs)" }
            inputs.forEach { (name, value) ->
                logger.fine { "[${instanceConfig.name}]     $name: ${value.value} (type: ${value.type})" }
            }

            // Get or create node state
            val nodeState = nodeStates.getOrPut(nodeId) { ConcurrentHashMap() }

            // Get flow variables
            val flowVars = flowInstance.variables?.map ?: emptyMap()

            // Execute node based on type
            when (node.type) {
                "function" -> {
                    logger.fine { "[${instanceConfig.name}]   Executing function node..." }
                    executeFunctionNode(node, inputs, nodeState, flowVars)
                }
                "database" -> {
                    logger.fine { "[${instanceConfig.name}]   Executing database node..." }
                    executeDatabaseNode(node, inputs, nodeState)
                }
                "timer" -> {
                    logger.fine { "[${instanceConfig.name}]   Setting up timer node..." }
                    setupTimerNode(node, nodeState)
                }
                else -> {
                    logger.warning("[${instanceConfig.name}] Unsupported node type: ${node.type}")
                }
            }

            // Update execution stats
            executionCount++
            lastExecution = Instant.now()

        } catch (e: Exception) {
            logger.severe("[${instanceConfig.name}] Error executing node $nodeId: ${e.message}")
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

            // Check if this is a topic input (from MQTT)
            val topicMapping = topicInputMappings[nodeInput]
            if (topicMapping != null) {
                // For wildcard subscriptions, use the actual triggering topic
                // For exact subscriptions, use the mapped topic directly
                val topicToLookup = if ((topicMapping.contains('+') || topicMapping.contains('#')) && triggeringTopic != null) {
                    // Wildcard subscription: use the actual topic that triggered this execution
                    triggeringTopic
                } else {
                    // Exact subscription: use the mapped topic directly
                    topicMapping
                }

                val topicValue = topicValues[topicToLookup]
                if (topicValue != null) {
                    inputs[inputName] = FlowScriptEngine.InputValue(
                        value = topicValue.value,
                        type = FlowScriptEngine.InputType.TOPIC,
                        timestamp = topicValue.timestamp,
                        topic = topicToLookup
                    )
                }
            }

            // Check if this is an internal connection (from another node)
            val internalTopic = "flow:${instanceConfig.name}:$nodeInput"
            val internalValue = topicValues[internalTopic]
            if (internalValue != null) {
                // Find the source connection to get the output port name
                val sourceConnection = flowClass.connections.find {
                    it.toNode == node.id && it.toInput == inputName
                }
                val topicName = if (sourceConnection != null) {
                    "${sourceConnection.fromNode}.${sourceConnection.fromOutput}"
                } else {
                    null
                }

                inputs[inputName] = FlowScriptEngine.InputValue(
                    value = internalValue.value,
                    type = FlowScriptEngine.InputType.TOPIC,
                    timestamp = internalValue.timestamp,
                    topic = topicName
                )
            }

            // Check if this is a static text input (constant value)
            val textValue = textInputMappings[nodeInput]
            if (textValue != null && inputs[inputName] == null) {
                // Try to parse as JSON array or object first, otherwise use as string
                val parsedValue = try {
                    val trimmed = textValue.trim()
                    when {
                        trimmed.startsWith("[") -> io.vertx.core.json.JsonArray(textValue)
                        trimmed.startsWith("{") -> io.vertx.core.json.JsonObject(textValue)
                        else -> textValue
                    }
                } catch (e: Exception) {
                    // If JSON parsing fails, use as plain string
                    textValue
                }

                inputs[inputName] = FlowScriptEngine.InputValue(
                    value = parsedValue,
                    type = FlowScriptEngine.InputType.TOPIC,
                    timestamp = System.currentTimeMillis(),
                    topic = null
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
            logger.warning("[${instanceConfig.name}] Function node ${node.id} has no script")
            return
        }

        logger.fine { "[${instanceConfig.name}]   Script to execute:\n$script" }
        logger.fine { "[${instanceConfig.name}]   Language: ${node.language}" }

        // Execute everything in a blocking context to avoid GraalVM issues
        vertx.executeBlocking<FlowScriptEngine.ExecutionResult> {
            // Get or create script engine for this node
            // GraalVM Context initialization must happen on a worker thread, not event loop
            val scriptEngine = scriptEngines[node.id] ?: run {
                logger.fine { "[${instanceConfig.name}]   Creating new FlowScriptEngine for node ${node.id} on worker thread..." }
                val engine = FlowScriptEngine()
                scriptEngines[node.id] = engine
                engine
            }

            logger.fine { "[${instanceConfig.name}]   Calling scriptEngine.execute()..." }

            // Execute the script
            scriptEngine.execute(
                script = script,
                language = node.language,
                inputs = inputs,
                state = nodeState,
                flowVariables = flowVars,
                onOutput = { portName, value ->
                    logger.fine { "[${instanceConfig.name}]   Script called outputs.send($portName, $value)" }
                    handleNodeOutput(node.id, portName, value)
                },
                instanceName = instanceConfig.name,
                flowClass = flowClass
            )
        }.onComplete { asyncResult ->
            if (asyncResult.succeeded()) {
                val result = asyncResult.result()
                logger.fine { "[${instanceConfig.name}]   Script execution completed: success=${result.success}, logs=${result.logs.size}, errors=${result.errors.size}" }

                if (!result.success) {
                    logger.severe("[${instanceConfig.name}] Script execution failed for node ${node.id}: ${result.errors}")
                    errorCount++
                    lastError = result.errors.joinToString(", ")
                }

                // Log console output
                result.logs.forEach { log ->
                    logger.fine { "[${instanceConfig.name}] Node ${node.id}: $log" }
                }
            } else {
                logger.severe("[${instanceConfig.name}] Async script execution failed: ${asyncResult.cause()?.message}")
                asyncResult.cause()?.printStackTrace()
                errorCount++
                lastError = asyncResult.cause()?.message
            }
        }
    }

    /**
     * Setup a timer node that periodically publishes to output
     */
    private fun setupTimerNode(
        node: FlowNode,
        nodeState: MutableMap<String, Any>
    ) {
        try {
            val frequency = node.config.getLong("frequency", 1000)
            val value = node.config.getString("value", "")

            if (frequency <= 0) {
                logger.warning("[${instanceConfig.name}] Timer node ${node.id} has invalid frequency: $frequency")
                handleNodeOutput(node.id, "error", "Invalid frequency: must be > 0")
                return
            }

            // Check if timer is already running
            val timerId = nodeState["timerId"] as? Long
            if (timerId != null) {
                logger.fine { "[${instanceConfig.name}] Timer ${node.id} is already running" }
                return
            }

            // Start periodic timer
            val newTimerId = vertx.setPeriodic(frequency) {
                logger.fine { "[${instanceConfig.name}] Timer ${node.id} tick" }
                try {
                    // Generate output value
                    val outputValue = if (value.isBlank()) {
                        // If value is empty, generate timestamp JSON
                        val now = java.time.Instant.now()
                        io.vertx.core.json.JsonObject()
                            .put("ts", now.toString())
                            .put("ms", now.toEpochMilli())
                    } else {
                        // Use configured value - try as JSON first, fallback to string
                        try {
                            io.vertx.core.json.JsonObject(value)
                        } catch (e: Exception) {
                            value
                        }
                    }
                    handleNodeOutput(node.id, "tick", outputValue)
                } catch (e: Exception) {
                    logger.severe("[${instanceConfig.name}] Timer node ${node.id} error: ${e.message}")
                    handleNodeOutput(node.id, "error", e.message ?: "Unknown error")
                }
            }

            // Store timer ID for cleanup
            nodeState["timerId"] = newTimerId
            logger.info("[${instanceConfig.name}] Timer ${node.id} started with frequency ${frequency}ms")

        } catch (e: Exception) {
            logger.severe("[${instanceConfig.name}] Error setting up timer node ${node.id}: ${e.message}")
            e.printStackTrace()
            errorCount++
            lastError = e.message
            handleNodeOutput(node.id, "error", e.message ?: "Unknown error")
        }
    }

    /**
     * Execute a database node
     */
    private fun executeDatabaseNode(
        node: FlowNode,
        inputs: Map<String, FlowScriptEngine.InputValue>,
        nodeState: MutableMap<String, Any>
    ) {
        try {
            logger.fine { "[${instanceConfig.name}]   Reading database node config..." }

            val dbNodeConfig = DatabaseNodeConfig.fromJsonObject(node.config)
            logger.fine { "[${instanceConfig.name}]   DB Node Config: $dbNodeConfig" }

            // Get the JDBC manager (singleton)
            val jdbcManager = JdbcManagerHolder.getInstance()

            // For now, we'll use a simple database connection config from the node config
            // In a real implementation, this would be loaded from a database config store
            val jdbcUrl = node.config.getString("jdbcUrl")
            val username = node.config.getString("username", "")
            val password = node.config.getString("password", "")

            logger.fine { "[${instanceConfig.name}]   JDBC URL: $jdbcUrl" }
            logger.fine { "[${instanceConfig.name}]   Username: $username" }

            if (jdbcUrl.isBlank()) {
                logger.warning("[${instanceConfig.name}] Database node ${node.id} has no jdbcUrl configured")
                handleNodeOutput(node.id, "error", "No database URL configured")
                return
            }

            // Driver is automatically inferred from the JDBC URL
            val connectionConfig = DatabaseConnectionConfig(
                name = node.id,
                jdbcUrl = jdbcUrl,
                username = username,
                password = password
            )

            logger.fine { "[${instanceConfig.name}]   Connection config: $connectionConfig" }

            // Get SQL statement (either from config or from input)
            var sqlStatement = dbNodeConfig.sqlStatement

            if (sqlStatement.isNullOrBlank() && dbNodeConfig.enableDynamicSql) {
                // Look for SQL in inputs
                sqlStatement = inputs["sql"]?.value?.toString()
            }

            if (sqlStatement.isNullOrBlank()) {
                logger.warning("[${instanceConfig.name}] Database node ${node.id} has no SQL statement")
                handleNodeOutput(node.id, "error", "No SQL statement provided")
                return
            }

            logger.fine { "[${instanceConfig.name}]   Executing SQL: $sqlStatement" }

            // Get arguments if provided
            val argumentsInput = inputs["arguments"]?.value
            val arguments = when (argumentsInput) {
                is List<*> -> argumentsInput
                is io.vertx.core.json.JsonArray -> argumentsInput.list
                is JsonObject -> argumentsInput.getMap().values.toList()
                else -> emptyList<Any?>()
            }

            logger.fine { "[${instanceConfig.name}]   Arguments: $arguments" }

            // Execute the query
            vertx.executeBlocking<Any> {
                try {
                    // Determine execution type based on user-selected operation type
                    val isQuery = dbNodeConfig.operationType == DatabaseOperationType.QUERY

                    if (isQuery) {
                        // Execute QUERY (returns result rows)
                        val queryResult = jdbcManager.executeQuery(connectionConfig, sqlStatement, arguments)
                        if (queryResult.isSuccess) {
                            val resultData = queryResult.getOrNull()
                            logger.fine { "[${instanceConfig.name}]   Query result: ${resultData?.size()} rows" }
                            resultData
                        } else {
                            val exception = queryResult.exceptionOrNull()
                            logger.severe("[${instanceConfig.name}]   Query error: ${exception?.message}")
                            throw exception ?: Exception("Query execution failed")
                        }
                    } else {
                        // Execute DML statement
                        val dmlResult = jdbcManager.executeDml(connectionConfig, sqlStatement, arguments)
                        if (dmlResult.isSuccess) {
                            val affectedRows = dmlResult.getOrNull() ?: 0
                            logger.fine { "[${instanceConfig.name}]   DML result: $affectedRows rows affected" }
                            JsonObject()
                                .put("affectedRows", affectedRows)
                                .put("success", true)
                        } else {
                            val exception = dmlResult.exceptionOrNull()
                            logger.severe("[${instanceConfig.name}]   DML error: ${exception?.message}")
                            throw exception ?: Exception("DML execution failed")
                        }
                    }
                } catch (e: Exception) {
                    logger.severe("[${instanceConfig.name}]   Database execution error: ${e.message}")
                    throw e
                }
            }.onComplete { asyncResult ->
                if (asyncResult.succeeded()) {
                    val result = asyncResult.result()
                    logger.fine { "[${instanceConfig.name}]   Database execution completed" }
                    handleNodeOutput(node.id, "result", result)
                } else {
                    val errorMsg = buildString {
                        append("Database execution failed")
                        append("\nSQL: $sqlStatement")
                        if (arguments.isNotEmpty()) {
                            append("\nArguments: $arguments")
                        }
                        append("\nError: ${asyncResult.cause()?.message}")
                    }
                    logger.severe("[${instanceConfig.name}]   Database execution async error: ${asyncResult.cause()?.message}")
                    asyncResult.cause()?.printStackTrace()
                    errorCount++
                    lastError = asyncResult.cause()?.message
                    handleNodeOutput(node.id, "error", errorMsg)
                }
            }

        } catch (e: Exception) {
            logger.severe("[${instanceConfig.name}] Error executing database node ${node.id}: ${e.message}")
            e.printStackTrace()
            errorCount++
            lastError = e.message
            handleNodeOutput(node.id, "error", e.message ?: "Unknown error")
        }
    }

    /**
     * Handle output from a node
     */
    private fun handleNodeOutput(nodeId: String, portName: String, value: Any?) {
        try {
            val nodeOutput = "$nodeId.$portName"

            logger.fine("[${instanceConfig.name}] Node $nodeId output on port $portName: $value")

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
            logger.severe("[${instanceConfig.name}] Error handling node output: ${e.message}")
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
                is Map<*, *> -> {
                    @Suppress("UNCHECKED_CAST")
                    JsonObject(value as Map<String, Any>).encode().toByteArray()
                }
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
                logger.fine { "[${instanceConfig.name}] Published to MQTT topic $topic: $value" }
            } else {
                logger.severe("[${instanceConfig.name}] SessionHandler not available for publishing")
            }

        } catch (e: Exception) {
            logger.severe("[${instanceConfig.name}] Error publishing to MQTT: ${e.message}")
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

    private fun parsePayload(payload: ByteArray): String {
        return String(payload)
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
