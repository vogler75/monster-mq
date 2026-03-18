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
import java.time.Instant
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
    private var cronTimerId: Long? = null
    private val mcpClients = mutableListOf<McpClient>()

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
            agentTools = AgentTools(archiveHandler, null, clientId) { name, args, result ->
                publishToolLog(name, args, result)
            }

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

            // Subscribe to input MQTT topics
            if (sessionHandler != null && agentConfig.inputTopics.isNotEmpty()) {
                setupMqttSubscriptions(sessionHandler)
            }

            // Setup CRON trigger if applicable
            if (agentConfig.triggerType == TriggerType.CRON) {
                setupCronTrigger()
            }

            // Publish Agent Card
            publishAgentCard()

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

            // Unsubscribe MQTT topics
            val sessionHandler = Monster.getSessionHandler()
            if (sessionHandler != null) {
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

    private fun setupMqttSubscriptions(sessionHandler: at.rocworks.handlers.SessionHandler) {
        // Register EventBus consumer to receive MQTT messages
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

        // Subscribe to each input topic
        agentConfig.inputTopics.forEach { topicFilter ->
            logger.fine("Agent ${deviceConfig.name} subscribing to: $topicFilter")
            sessionHandler.subscribeInternalClient(clientId, topicFilter, 0)
        }
    }

    private fun setupCronTrigger() {
        val intervalMs = agentConfig.cronIntervalMs
        if (intervalMs != null && intervalMs > 0) {
            logger.fine("Agent ${deviceConfig.name} setting up periodic trigger: ${intervalMs}ms")
            cronTimerId = vertx.setPeriodic(intervalMs) {
                val message = "It is ${Instant.now()}. Execute your scheduled task."
                executeAgent(message, "cron")
            }
        } else {
            logger.warning("Agent ${deviceConfig.name} has CRON trigger but no cronIntervalMs configured")
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

    private fun handleMqttMessage(msg: BrokerMessage) {
        val payloadStr = msg.getPayloadAsJson() ?: msg.getPayloadAsBase64()
        val userMessage = "[Topic: ${msg.topicName}] $payloadStr"
        executeAgent(userMessage, msg.topicName)
    }

    private fun executeAgent(userMessage: String, source: String) {
        val service = aiService ?: return

        messagesProcessed.incrementAndGet()
        logger.fine("Agent ${deviceConfig.name} processing message from $source")

        vertx.executeBlocking {
            llmCalls.incrementAndGet()
            service.chat(userMessage)
        }.onComplete { result ->
            if (result.succeeded()) {
                val chatResult = result.result()
                // Log MCP/tool executions that went through LangChain4j's tool provider
                chatResult?.toolExecutions()?.forEach { toolExecution ->
                    val req = toolExecution.request()
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
                publishError(result.cause()?.message ?: "Unknown error")
            }
        }
    }

    private fun publishResponse(response: String) {
        val sessionHandler = Monster.getSessionHandler() ?: return

        agentConfig.outputTopics.forEach { topic ->
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
        val msg = BrokerMessage(clientId, "agents/${deviceConfig.name}/$subtopic", payload.encode())
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

        val card = JsonObject()
            .put("name", deviceConfig.name)
            .put("description", agentConfig.description)
            .put("provider", agentConfig.provider)
            .put("model", agentConfig.model)
            .put("triggerType", agentConfig.triggerType.name)
            .put("inputTopics", agentConfig.inputTopics)
            .put("outputTopics", agentConfig.outputTopics)
            .put("skills", agentConfig.skills.map { it.toJsonObject() })
            .put("status", "running")
            .put("nodeId", deviceConfig.nodeId)

        val msg = BrokerMessage(clientId, "agents/${deviceConfig.name}/card", card.encode())
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

        val msg = BrokerMessage(clientId, "agents/${deviceConfig.name}/health", health.encode())
        sessionHandler.publishMessage(msg)
    }
}
