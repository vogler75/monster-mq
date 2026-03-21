package at.rocworks.agents

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.data.BulkClientMessage
import at.rocworks.stores.DeviceConfig
import at.rocworks.extensions.graphql.JwtService
import at.rocworks.stores.DeviceConfigStoreFactory
import dev.langchain4j.mcp.client.DefaultMcpClient
import dev.langchain4j.mcp.client.McpClient
import dev.langchain4j.mcp.client.transport.http.StreamableHttpMcpTransport
import dev.langchain4j.mcp.McpToolProvider
import dev.langchain4j.data.message.AiMessage
import dev.langchain4j.memory.chat.MessageWindowChatMemory
import dev.langchain4j.model.chat.ChatModel
import dev.langchain4j.model.chat.listener.*
import dev.langchain4j.model.chat.request.ChatRequest
import dev.langchain4j.data.message.ToolExecutionResultMessage
import dev.langchain4j.service.AiServices
import dev.langchain4j.service.Result
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import java.time.Instant
import java.time.ZonedDateTime
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger

/**
 * Per-agent verticle that handles MQTT subscriptions, LLM invocations,
 * and the ReAct tool-calling loop via LangChain4j AiServices.
 */
class AgentExecutor(
    private val deviceConfig: DeviceConfig
) : AbstractVerticle() {

    private val logger: Logger = Utils.getLogger(this::class.java)

    private lateinit var agentConfig: AgentConfig
    private lateinit var chatModel: ChatModel
    private lateinit var agentTools: AgentTools
    private var aiService: AgentAiService? = null

    private val clientId = "agent-${deviceConfig.name}"
    private val agentName get() = deviceConfig.name

    // A2A topic helpers
    private fun a2aPrefix() = "a2a/v1/${agentConfig.org}/${agentConfig.site}"
    private fun a2aAgentPrefix() = "${a2aPrefix()}/agents/$agentName"
    private fun a2aDiscoveryTopic() = "${a2aPrefix()}/discovery/$agentName"
    private fun a2aInboxTopic() = "${a2aAgentPrefix()}/inbox"
    private fun a2aStatusTopic(taskId: String) = "${a2aAgentPrefix()}/status/$taskId"
    private fun a2aCancelTopic() = "${a2aAgentPrefix()}/cancel/+"
    private fun a2aAgentTopic(subtopic: String) = "${a2aAgentPrefix()}/$subtopic"

    private var cronTimerId: Long? = null
    private val mcpClients = mutableListOf<McpClient>()
    private val pendingTaskResponses = ConcurrentHashMap<String, CompletableFuture<String>>()

    // Metrics
    private val messagesProcessed = AtomicLong(0)
    private val llmCalls = AtomicLong(0)
    private val errors = AtomicLong(0)

    /**
     * LangChain4j AI Service interface.
     * AiServices generates a proxy that handles the ReAct loop automatically.
     */
    interface AgentAiService {
        fun chat(userMessage: String): Result<String>
    }

    override fun start(startPromise: Promise<Void>) {
        try {
            agentConfig = AgentConfig.fromJsonObject(deviceConfig.config)
            logger.fine("Starting agent ${deviceConfig.name} (provider: ${agentConfig.provider}, trigger: ${agentConfig.triggerType})")

            val globalConfig = vertx.orCreateContext.config()

            // Create LLM model with logging listener
            val llmListener = createLlmListener()
            chatModel = LangChain4jFactory.createChatModel(agentConfig, globalConfig, listOf(llmListener))

            // Create tools
            val archiveHandler = Monster.getArchiveHandler()
            val sessionHandler = Monster.getSessionHandler()
            agentTools = AgentTools(
                archiveHandler = archiveHandler,
                retainedStore = null,
                agentClientId = clientId,
                agentName = deviceConfig.name,
                a2aOrg = agentConfig.org,
                a2aSite = agentConfig.site,
                defaultArchiveGroup = agentConfig.defaultArchiveGroup,
                toolLogger = { name, args, result -> publishToolLog(name, args, result) },
                vertx = vertx,
                taskTimeoutMs = agentConfig.taskTimeoutMs,
                registerTaskResponse = { replyTo, future -> pendingTaskResponses[replyTo] = future },
                unregisterTaskResponse = { replyTo -> pendingTaskResponses.remove(replyTo) },
                subAgents = agentConfig.subAgents
            )

            // Build AI Service with ReAct loop
            val memorySize = agentConfig.memoryWindowSize
            val builder = AiServices.builder(AgentAiService::class.java)
                .chatModel(chatModel)
                .chatMemory(MessageWindowChatMemory.withMaxMessages(memorySize))
                .tools(agentTools)

            // Add MCP tool providers if configured
            if (agentConfig.mcpServers.isNotEmpty() || agentConfig.useMonsterMqMcp) {
                val mcpToolProvider = createMcpToolProvider(agentConfig.mcpServers, agentConfig.useMonsterMqMcp, globalConfig)
                if (mcpToolProvider != null) {
                    builder.toolProvider(mcpToolProvider)
                }
            }

            // Handle hallucinated tool names gracefully instead of throwing
            builder.hallucinatedToolNameStrategy { request ->
                logger.fine("Agent ${deviceConfig.name} hallucinated tool: ${request.name()}")
                ToolExecutionResultMessage.from(request, "Error: tool '${request.name()}' does not exist. Use only the tools listed in your available tools.")
            }

            if (agentConfig.systemPrompt.isNotBlank()) {
                builder.systemMessageProvider { agentConfig.systemPrompt }
            }

            aiService = builder.build()

            // Register EventBus consumer to receive MQTT messages
            if (sessionHandler != null) {
                setupEventBusConsumer()
            }

            // Subscribe to input MQTT topics
            if (sessionHandler != null && agentConfig.inputTopics.isNotEmpty()) {
                setupMqttSubscriptions(sessionHandler)
            }

            // Subscribe to task topic for A2A orchestration
            if (sessionHandler != null) {
                setupTaskSubscription(sessionHandler)
            }

            // Setup CRON trigger if applicable
            if (agentConfig.triggerType == TriggerType.CRON) {
                setupCronTrigger()
            }

            // Publish Agent Card and health status
            publishAgentCard()
            publishHealthStatus("ready")

            logger.info("Agent ${deviceConfig.name} started successfully")
            startPromise.complete()

        } catch (e: Exception) {
            logger.severe("Failed to start agent ${deviceConfig.name}: ${e.message}")
            e.printStackTrace()
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.fine("Stopping agent ${deviceConfig.name}...")

        try {
            // Cancel cron timer
            cronTimerId?.let { vertx.cancelTimer(it) }

            // Complete any pending task futures with error
            pendingTaskResponses.forEach { (_, future) ->
                future.completeExceptionally(RuntimeException("Agent stopping"))
            }
            pendingTaskResponses.clear()

            // Unsubscribe MQTT topics
            val sessionHandler = Monster.getSessionHandler()
            if (sessionHandler != null) {
                // Unsubscribe task topic
                sessionHandler.unsubscribeInternalClient(clientId, a2aInboxTopic())
                agentConfig.inputTopics.forEach { topic ->
                    sessionHandler.unsubscribeInternalClient(clientId, topic)
                }
                sessionHandler.unregisterInternalClient(clientId)
            }

            // Close MCP clients
            mcpClients.forEach { client ->
                try { client.close() } catch (e: Exception) {
                    logger.warning("Error closing MCP client: ${e.message}")
                }
            }
            mcpClients.clear()

            // Publish offline status
            publishHealthStatus("stopped")

            logger.info("Agent ${deviceConfig.name} stopped")
            stopPromise.complete()

        } catch (e: Exception) {
            logger.warning("Error stopping agent ${deviceConfig.name}: ${e.message}")
            stopPromise.complete()
        }
    }

    private fun setupEventBusConsumer() {
        vertx.eventBus().consumer<Any>(EventBusAddresses.Client.messages(clientId)) { busMessage ->
            try {
                when (val body = busMessage.body()) {
                    is BrokerMessage -> handleMqttMessage(body)
                    is BulkClientMessage -> body.messages.forEach { handleMqttMessage(it) }
                    else -> logger.warning("Unknown message type: ${body?.javaClass?.simpleName}")
                }
            } catch (e: Exception) {
                logger.warning("Error processing MQTT message in agent ${deviceConfig.name}: ${e.message}")
            }
        }
    }

    private fun setupMqttSubscriptions(sessionHandler: at.rocworks.handlers.SessionHandler) {
        // Subscribe to each input topic
        agentConfig.inputTopics.forEach { topicFilter ->
            logger.fine("Agent ${deviceConfig.name} subscribing to: $topicFilter")
            sessionHandler.subscribeInternalClient(clientId, topicFilter, 0)
        }
    }

    private fun setupCronTrigger() {
        val expression = agentConfig.cronExpression
        if (!expression.isNullOrBlank()) {
            val cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ)
            val parser = CronParser(cronDefinition)
            val cron = parser.parse(expression).validate()
            val executionTime = ExecutionTime.forCron(cron)
            scheduleNextCronExecution(executionTime)
        } else {
            val intervalMs = agentConfig.cronIntervalMs
            if (intervalMs != null && intervalMs > 0) {
                logger.fine("Agent ${deviceConfig.name} setting up periodic trigger: ${intervalMs}ms")
                cronTimerId = vertx.setPeriodic(intervalMs) {
                    executeAgent(agentConfig.cronPrompt?.takeIf { it.isNotBlank() } ?: "It is ${Instant.now()}. Execute your scheduled task.", "cron")
                }
            } else {
                logger.warning("Agent ${deviceConfig.name} has CRON trigger but no cronExpression or cronIntervalMs")
            }
        }
    }

    private fun scheduleNextCronExecution(executionTime: ExecutionTime) {
        val now = ZonedDateTime.now()
        val nextExecution = executionTime.nextExecution(now)
        if (nextExecution.isPresent) {
            val delayMs = java.time.Duration.between(now, nextExecution.get()).toMillis()
            logger.fine("Agent ${deviceConfig.name} next cron execution at ${nextExecution.get()} (in ${delayMs}ms)")
            cronTimerId = vertx.setTimer(delayMs) {
                executeAgent(agentConfig.cronPrompt?.takeIf { it.isNotBlank() } ?: "It is ${Instant.now()}. Execute your scheduled task.", "cron")
                scheduleNextCronExecution(executionTime)
            }
        } else {
            logger.warning("Agent ${deviceConfig.name}: no next cron execution found")
        }
    }

    private fun createMcpToolProvider(serverNames: List<String>, useMonsterMqMcp: Boolean, globalConfig: JsonObject): McpToolProvider? {
        val clients = mutableListOf<McpClient>()

        // Add MonsterMQ's own MCP server if enabled
        if (useMonsterMqMcp) {
            try {
                val mcpConfig = globalConfig.getJsonObject("MCP", JsonObject())
                val mcpPort = mcpConfig.getInteger("Port", 3000)
                val mcpUrl = "http://localhost:$mcpPort/mcp"

                // Generate an internal JWT token for the agent
                val token = JwtService.generateToken("agent-${deviceConfig.name}", true)

                logger.fine("Creating MonsterMQ MCP client at $mcpUrl")

                val transport = StreamableHttpMcpTransport.builder()
                    .url(mcpUrl)
                    .customHeaders(mapOf("Authorization" to "Bearer $token"))
                    .build()

                val client = DefaultMcpClient.builder()
                    .key("monstermq")
                    .transport(transport)
                    .build()

                clients.add(client)
                mcpClients.add(client)
                logger.fine("MonsterMQ MCP client created")
            } catch (e: Exception) {
                logger.warning("Failed to create MonsterMQ MCP client: ${e.message}")
            }
        }

        // Add external MCP servers
        val deviceStore = DeviceConfigStoreFactory.getSharedInstance()
        if (deviceStore != null) {
            for (serverName in serverNames) {
                try {
                    val deviceFuture = deviceStore.getDevice(serverName)
                    val countDownLatch = java.util.concurrent.CountDownLatch(1)
                    var device: DeviceConfig? = null
                    deviceFuture.onComplete { result ->
                        if (result.succeeded()) device = result.result()
                        countDownLatch.countDown()
                    }
                    countDownLatch.await(5, java.util.concurrent.TimeUnit.SECONDS)

                    if (device == null || device!!.type != DeviceConfig.DEVICE_TYPE_MCP_SERVER) {
                        logger.warning("MCP server config not found: $serverName")
                        continue
                    }

                    val mcpConfig = McpServerConfig.fromJsonObject(device!!.config)
                    logger.fine("Creating MCP client for ${serverName}: ${mcpConfig.url}")

                    val transport = StreamableHttpMcpTransport.builder()
                        .url(mcpConfig.url)
                        .build()

                    val client = DefaultMcpClient.builder()
                        .key(serverName)
                        .transport(transport)
                        .build()

                    clients.add(client)
                    mcpClients.add(client)
                    logger.fine("MCP client created for $serverName")

                } catch (e: Exception) {
                    logger.warning("Failed to create MCP client for $serverName: ${e.message}")
                }
            }
        }

        if (clients.isEmpty()) return null

        return McpToolProvider.builder()
            .mcpClients(clients)
            .build()
    }

    private fun buildContextData(): String {
        val lines = mutableListOf<String>()

        // Fetch from archive last-value stores
        if (agentConfig.contextLastvalTopics.isNotEmpty()) {
            val archiveGroups = Monster.getArchiveHandler()?.getDeployedArchiveGroups() ?: emptyMap()
            for ((groupName, topicFilters) in agentConfig.contextLastvalTopics) {
                val store = archiveGroups[groupName]?.lastValStore ?: continue
                for (filter in topicFilters) {
                    store.findMatchingMessages(filter) { msg ->
                        val value = msg.getPayloadAsJson() ?: msg.getPayloadAsBase64()
                        lines.add("[Archive:$groupName] ${msg.topicName} = $value (${msg.time})")
                        lines.size < 500 // safety limit
                    }
                }
            }
        }

        // Fetch retained messages
        if (agentConfig.contextRetainedTopics.isNotEmpty()) {
            val retainedStore = Monster.getRetainedStore()
            if (retainedStore != null) {
                for (filter in agentConfig.contextRetainedTopics) {
                    retainedStore.findMatchingMessages(filter) { msg ->
                        val value = msg.getPayloadAsJson() ?: msg.getPayloadAsBase64()
                        lines.add("[Retained] ${msg.topicName} = $value")
                        lines.size < 500
                    }
                }
            }
        }

        // Fetch history data
        if (agentConfig.contextHistoryQueries.isNotEmpty()) {
            val archiveGroups = Monster.getArchiveHandler()?.getDeployedArchiveGroups() ?: emptyMap()
            for (query in agentConfig.contextHistoryQueries) {
                if (query.topics.isEmpty()) continue
                val archiveGroup = archiveGroups[query.archiveGroup] ?: continue
                val archiveStore = archiveGroup.archiveStore
                if (archiveStore !is at.rocworks.stores.IMessageArchiveExtended) continue

                val endTime = java.time.Instant.now()
                val startTime = endTime.minusSeconds(query.lastSeconds.toLong())

                if (query.isRaw()) {
                    // Raw history: fetch individual messages per topic
                    for (topic in query.topics) {
                        try {
                            val history = archiveStore.getHistory(topic, startTime, endTime, 500)
                            if (history.size() > 0) {
                                lines.add("[History:${query.archiveGroup}:RAW] $topic (last ${query.lastSeconds}s, ${history.size()} records):")
                                for (i in 0 until history.size()) {
                                    val row = history.getJsonObject(i) ?: continue
                                    val time = row.getString("time") ?: row.getValue("time")?.toString() ?: ""
                                    val value = row.getValue("payload_json") ?: row.getValue("payload_b64") ?: ""
                                    lines.add("  $time = $value")
                                    if (lines.size >= 500) break
                                }
                            }
                        } catch (e: Exception) {
                            logger.warning("Failed to fetch raw history for $topic in ${query.archiveGroup}: ${e.message}")
                        }
                        if (lines.size >= 500) break
                    }
                } else {
                    // Aggregated history
                    try {
                        val result = archiveStore.getAggregatedHistory(
                            topics = query.topics,
                            startTime = startTime,
                            endTime = endTime,
                            intervalMinutes = query.intervalMinutes(),
                            functions = listOf(query.function.uppercase()),
                            fields = query.fields
                        )
                        val columns = result.getJsonArray("columns")
                        val rows = result.getJsonArray("rows")
                        if (columns != null && rows != null && rows.size() > 0) {
                            lines.add("[History:${query.archiveGroup}:${query.interval}:${query.function}] ${query.topics.joinToString(", ")} (last ${query.lastSeconds}s, ${rows.size()} rows):")
                            lines.add("  Columns: ${columns.encode()}")
                            for (i in 0 until rows.size()) {
                                lines.add("  ${rows.getValue(i)}")
                                if (lines.size >= 500) break
                            }
                        }
                    } catch (e: Exception) {
                        logger.warning("Failed to fetch aggregated history for ${query.topics} in ${query.archiveGroup}: ${e.message}")
                    }
                }
                if (lines.size >= 500) break
            }
        }

        if (lines.isEmpty()) return ""

        return "--- Context Data (current values for your reference) ---\n" +
            lines.joinToString("\n") +
            "\n--- End Context Data ---"
    }

    private fun handleMqttMessage(msg: BrokerMessage) {
        // 1. Check pending task responses (orchestrator receiving sub-agent results)
        val pendingFuture = pendingTaskResponses.remove(msg.topicName)
        if (pendingFuture != null) {
            val payload = String(msg.payload, Charsets.UTF_8)
            pendingFuture.complete(payload)
            return
        }

        // 2. Check incoming tasks (this agent being invoked by another agent)
        if (msg.topicName == a2aInboxTopic()) {
            handleTaskMessage(msg)
            return
        }

        // 3. Normal MQTT message handling
        val payloadStr = msg.getPayloadAsJson() ?: msg.getPayloadAsBase64()
        val userMessage = "[Topic: ${msg.topicName}] $payloadStr"
        executeAgent(userMessage, msg.topicName)
    }

    private fun setupTaskSubscription(sessionHandler: at.rocworks.handlers.SessionHandler) {
        val inboxTopic = a2aInboxTopic()
        logger.fine("Agent $agentName subscribing to inbox: $inboxTopic")
        sessionHandler.subscribeInternalClient(clientId, inboxTopic, 1)
    }

    private fun handleTaskMessage(msg: BrokerMessage) {
        try {
            val payload = String(msg.payload, Charsets.UTF_8)
            logger.fine { "Agent ${deviceConfig.name} received task message: $payload" }
            val taskJson = try { JsonObject(payload) } catch (_: Exception) { null }

            // Plain-text payload: treat the whole payload as input, no reply
            if (taskJson == null) {
                val taskId = Utils.getUuid()
                logger.info("Agent ${deviceConfig.name} received plain-text task $taskId")
                publishTaskStatus(taskId, "working")
                val taskMessage = "[Task from external, taskId=$taskId]\n$payload"
                executeAgentWithCallback(taskMessage, "task:$taskId") { response, _ ->
                    publishTaskStatus(taskId, if (response != null) "completed" else "failed")
                    if (response != null) publishResponse(response)
                }
                return
            }

            val taskId = taskJson.getString("taskId") ?: Utils.getUuid()
            val input = taskJson.getString("input")
            if (input == null) {
                logger.warning("Agent ${deviceConfig.name} received task $taskId with missing 'input' field, ignoring")
                return
            }
            val replyTo = taskJson.getString("replyTo") ?: a2aStatusTopic(taskId)
            val skill = taskJson.getString("skill")
            val callerAgent = taskJson.getString("callerAgent", "unknown")

            logger.info("Agent ${deviceConfig.name} received task $taskId from $callerAgent (replyTo=$replyTo)")

            // Publish working status
            publishTaskStatus(taskId, "working")

            // Build the user message for the LLM
            val taskMessage = if (skill != null) {
                "[Task from agent '$callerAgent', taskId=$taskId, skill=$skill]\n$input"
            } else {
                "[Task from agent '$callerAgent', taskId=$taskId]\n$input"
            }

            // Execute the agent with a callback to publish the result
            executeAgentWithCallback(taskMessage, "task:$taskId") { response, error ->
                val sessionHandler = Monster.getSessionHandler() ?: return@executeAgentWithCallback
                if (error != null) {
                    // Publish error response
                    val errorJson = JsonObject()
                        .put("taskId", taskId)
                        .put("status", "failed")
                        .put("error", error)
                    val responseMsg = BrokerMessage(clientId, replyTo, errorJson.encode())
                    sessionHandler.publishMessage(responseMsg)
                    publishTaskStatus(taskId, "failed")
                } else {
                    // Publish success response
                    val resultJson = JsonObject()
                        .put("taskId", taskId)
                        .put("status", "completed")
                        .put("result", response)
                    val responseMsg = BrokerMessage(clientId, replyTo, resultJson.encode())
                    sessionHandler.publishMessage(responseMsg)
                    publishTaskStatus(taskId, "completed")
                    // Also publish to configured output topics
                    if (response != null) publishResponse(response)
                }
            }
        } catch (e: Exception) {
            logger.warning("Agent ${deviceConfig.name} failed to handle task: ${e.message}")
        }
    }

    private fun publishTaskStatus(taskId: String, status: String) {
        val sessionHandler = Monster.getSessionHandler() ?: return
        val statusJson = JsonObject()
            .put("taskId", taskId)
            .put("status", status)
            .put("agent", deviceConfig.name)
            .put("timestamp", Instant.now().toString())
        val msg = BrokerMessage(clientId, a2aStatusTopic(taskId), statusJson.encode())
        sessionHandler.publishMessage(msg)
    }

    private fun executeAgentWithCallback(userMessage: String, source: String, callback: (String?, String?) -> Unit) {
        val service = aiService ?: run {
            callback(null, "Agent service not available")
            return
        }

        messagesProcessed.incrementAndGet()
        publishHealthStatus("running")
        logger.fine("Agent ${deviceConfig.name} processing task from $source")

        vertx.executeBlocking {
            llmCalls.incrementAndGet()
            val contextData = buildContextData()
            val fullMessage = if (contextData.isNotBlank()) {
                "$contextData\n\n$userMessage"
            } else {
                userMessage
            }
            service.chat(fullMessage)
        }.onComplete { result ->
            if (result.succeeded()) {
                val chatResult = result.result()
                chatResult?.toolExecutions()?.forEach { toolExecution ->
                    val req = toolExecution.request()
                    if (agentTools.isNativeTool(req.name())) return@forEach
                    val log = JsonObject()
                        .put("type", "mcp-tool-call")
                        .put("timestamp", Instant.now().toString())
                        .put("tool", req.name())
                        .put("arguments", req.arguments()?.take(500))
                        .put("result", toolExecution.result()?.take(1000))
                    publishToAgentTopic("logs/mcp", log)
                }
                val response = chatResult?.content()
                if (response != null) {
                    callback(response, null)
                } else {
                    callback(null, "LLM returned null response")
                }
            } else {
                errors.incrementAndGet()
                val cause = result.cause()
                logger.warning("Agent ${deviceConfig.name} task execution failed: ${cause?.message}")
                callback(null, cause?.message ?: "Unknown error")
            }
            publishHealthStatus("ready")
        }
    }

    private fun executeAgent(userMessage: String, source: String) {
        val service = aiService ?: return

        messagesProcessed.incrementAndGet()
        publishHealthStatus("running")
        logger.fine("Agent ${deviceConfig.name} processing message from $source")

        vertx.executeBlocking {
            llmCalls.incrementAndGet()
            val contextData = buildContextData()
            val fullMessage = if (contextData.isNotBlank()) {
                "$contextData\n\n$userMessage"
            } else {
                userMessage
            }

            logger.fine { "Agent ${deviceConfig.name} LLM request [source=$source, length=${fullMessage.length}]" }

            service.chat(fullMessage)
        }.onComplete { result ->
            if (result.succeeded()) {
                val chatResult = result.result()
                // Log MCP/tool executions that went through LangChain4j's tool provider
                chatResult?.toolExecutions()?.forEach { toolExecution ->
                    val req = toolExecution.request()
                    // Skip native @Tool calls — those are already logged via publishToolLog
                    if (agentTools.isNativeTool(req.name())) return@forEach
                    val log = JsonObject()
                        .put("type", "mcp-tool-call")
                        .put("timestamp", Instant.now().toString())
                        .put("tool", req.name())
                        .put("arguments", req.arguments()?.take(500))
                        .put("result", toolExecution.result()?.take(1000))
                    publishToAgentTopic("logs/mcp", log)
                }
                val response = chatResult?.content()
                if (response != null) {
                    publishResponse(response)
                } else {
                    publishError("LLM returned null response")
                }
            } else {
                errors.incrementAndGet()
                val cause = result.cause()
                logger.warning("Agent ${deviceConfig.name} executeBlocking failed: ${cause?.message}")
                if (cause != null) logger.fine { cause.stackTraceToString() }
                publishError(cause?.message ?: "Unknown error")
            }
            publishHealthStatus("ready")
        }
    }

    private fun publishResponse(response: String) {
        val sessionHandler = Monster.getSessionHandler() ?: return

        val topics = agentConfig.outputTopics.ifEmpty {
            listOf(a2aAgentTopic("response"))
        }

        topics.forEach { topic ->
            try {
                val msg = BrokerMessage(clientId, topic, response)
                sessionHandler.publishMessage(msg)
                logger.finer("Agent ${deviceConfig.name} published response to $topic")
            } catch (e: Exception) {
                logger.warning("Agent ${deviceConfig.name} failed to publish to $topic: ${e.message}")
            }
        }
    }

    private fun publishError(message: String) {
        logger.warning("Agent ${deviceConfig.name} error: $message")
        val log = JsonObject()
            .put("type", "error")
            .put("timestamp", Instant.now().toString())
            .put("message", message)
        publishToAgentTopic("logs/errors", log)
    }

    private fun publishToAgentTopic(subtopic: String, payload: JsonObject) {
        val sessionHandler = Monster.getSessionHandler() ?: return
        val msg = BrokerMessage(clientId, a2aAgentTopic(subtopic), payload.encode())
        sessionHandler.publishMessage(msg)
    }

    private fun createLlmListener(): ChatModelListener {
        return object : ChatModelListener {
            override fun onRequest(requestContext: ChatModelRequestContext) {
                val request = requestContext.chatRequest()
                val messages = request.messages()
                val lastMessage = messages.lastOrNull()
                val log = JsonObject()
                    .put("type", "llm-request")
                    .put("timestamp", Instant.now().toString())
                    .put("model", request.parameters()?.modelName())
                    .put("messageCount", messages.size)
                    .put("lastMessage", lastMessage?.toString()?.take(500))
                    .put("toolCount", request.parameters()?.toolSpecifications()?.size ?: 0)
                publishToAgentTopic("logs/llm", log)
            }

            override fun onResponse(responseContext: ChatModelResponseContext) {
                val response = responseContext.chatResponse()
                val aiMessage = response.aiMessage()
                val metadata = response.metadata()
                val tokenUsage = metadata?.tokenUsage()
                val log = JsonObject()
                    .put("type", "llm-response")
                    .put("timestamp", Instant.now().toString())
                    .put("model", metadata?.modelName())
                    .put("finishReason", metadata?.finishReason()?.name)
                    .put("inputTokens", tokenUsage?.inputTokenCount())
                    .put("outputTokens", tokenUsage?.outputTokenCount())
                    .put("totalTokens", tokenUsage?.totalTokenCount())
                    .put("hasToolCalls", aiMessage.hasToolExecutionRequests())
                    .put("toolCalls", if (aiMessage.hasToolExecutionRequests()) {
                        JsonArray(aiMessage.toolExecutionRequests().map { tc ->
                            JsonObject().put("name", tc.name()).put("arguments", tc.arguments())
                        })
                    } else null)
                    .put("text", aiMessage.text()?.take(500))
                publishToAgentTopic("logs/llm", log)
            }

            override fun onError(errorContext: ChatModelErrorContext) {
                val log = JsonObject()
                    .put("type", "llm-error")
                    .put("timestamp", Instant.now().toString())
                    .put("error", errorContext.error().message)
                publishToAgentTopic("logs/llm", log)
            }
        }
    }

    fun publishToolLog(toolName: String, arguments: String, result: String) {
        val log = JsonObject()
            .put("type", "tool-call")
            .put("timestamp", Instant.now().toString())
            .put("tool", toolName)
            .put("arguments", arguments.take(500))
            .put("result", result.take(1000))
        publishToAgentTopic("logs/tools", log)
    }

    private fun publishAgentCard() {
        val sessionHandler = Monster.getSessionHandler() ?: return
        val agentName = deviceConfig.name

        val card = JsonObject()
            // A2A-compatible fields
            .put("protocolVersion", "1.0")
            .put("name", agentName)
            .put("description", agentConfig.description)
            .put("url", a2aInboxTopic())
            .put("preferredTransport", "MQTT")
            .put("version", agentConfig.version)
            .put("defaultInputModes", listOf("application/json", "text/plain"))
            .put("defaultOutputModes", listOf("application/json", "text/plain"))
            // Agent-specific fields
            .put("provider", agentConfig.provider)
            .put("model", agentConfig.model)
            .put("triggerType", agentConfig.triggerType.name)
            .put("inputTopics", agentConfig.inputTopics)
            .put("outputTopics", agentConfig.outputTopics)
            .put("skills", agentConfig.skills.map { skill ->
                JsonObject()
                    .put("id", skill.name)
                    .put("name", skill.name)
                    .put("description", skill.description)
                    .put("inputSchema", skill.inputSchema)
            })
            .put("status", "running")
            .put("nodeId", deviceConfig.nodeId)
            .put("timestamp", Instant.now().toString())

        val payload = card.encode().toByteArray()
        val msg = BrokerMessage(
            messageId = 0,
            topicName = a2aDiscoveryTopic(),
            payload = payload,
            qosLevel = 1,
            isRetain = true,
            isDup = false,
            isQueued = false,
            clientId = clientId
        )
        sessionHandler.publishMessage(msg)
    }

    private fun publishHealthStatus(status: String) {
        val sessionHandler = Monster.getSessionHandler() ?: return

        val health = JsonObject()
            .put("name", deviceConfig.name)
            .put("status", status)
            .put("timestamp", Instant.now().toString())
            .put("messagesProcessed", messagesProcessed.get())
            .put("llmCalls", llmCalls.get())
            .put("errors", errors.get())

        val payload = health.encode().toByteArray()
        val msg = BrokerMessage(
            messageId = 0,
            topicName = a2aAgentTopic("health"),
            payload = payload,
            qosLevel = 0,
            isRetain = true,
            isDup = false,
            isQueued = false,
            clientId = clientId
        )
        sessionHandler.publishMessage(msg)
    }
}
