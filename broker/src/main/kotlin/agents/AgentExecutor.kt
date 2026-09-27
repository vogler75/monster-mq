package at.rocworks.agents

import at.rocworks.Const
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
import at.rocworks.genai.decision.IDecisionProvider
import at.rocworks.genai.decision.OpenRouterDecisionProvider
import dev.langchain4j.data.message.AiMessage
import dev.langchain4j.data.message.TextContent
import dev.langchain4j.model.chat.ChatModel
import dev.langchain4j.model.chat.StreamingChatModel
import dev.langchain4j.model.chat.response.ChatResponse
import dev.langchain4j.service.MemoryId
import dev.langchain4j.service.TokenStream
import dev.langchain4j.service.tool.ToolExecution
import dev.langchain4j.model.chat.listener.*
import dev.langchain4j.model.chat.request.ChatRequest
import dev.langchain4j.data.message.ToolExecutionResultMessage
import dev.langchain4j.service.AiServices
import dev.langchain4j.service.Result
import dev.langchain4j.data.message.UserMessage as ChatUserMessage
import dev.langchain4j.service.UserMessage as UserText
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.WorkerExecutor
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
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

    var agentConfig: AgentConfig = AgentConfig.fromJsonObject(deviceConfig.config)
    private var chatModel: ChatModel? = null
    private var streamingChatModel: StreamingChatModel? = null
    private var modelConfig: ChatModelConfig? = null
    private var decisionProvider: IDecisionProvider? = null
    private var agentTools: AgentTools? = null
    private var aiService: AgentAiService? = null
    private var streamingAiService: AgentStreamingAiService? = null
    private var chatMemoryStore: MonsterChatMemoryStore? = null
    private var ragIndex: AgentRagIndex? = null
    private var ragTimerId: Long? = null

    // Chat memories per session (memoryId = session id, "default" when the caller sends none).
    // Bounded LRU: evicted sessions only lose the in-memory copy, persisted history is reloaded on demand.
    private val memories = object : LinkedHashMap<String, TurnAwareChatMemory>(16, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, TurnAwareChatMemory>?): Boolean {
            val evict = size > MAX_SESSIONS
            if (evict && eldest != null) chatMemoryStore?.evictFromCache(eldest.value.id())
            return evict
        }
    }

    // Context data for the running LLM call. It is injected into the outgoing request only
    // (see buildAiService) so that the large context block is never stored in chat memory.
    @Volatile
    private var currentContextData: String? = null
    private var globalConfig: JsonObject? = null
    private var mcpToolProvider: dev.langchain4j.mcp.McpToolProvider? = null
    private var conversationLog: Logger? = null
    private var conversationLogHandler: java.util.logging.FileHandler? = null
    private var llmWorkerExecutor: WorkerExecutor? = null

    private val clientId = "agent-${deviceConfig.name}"
    private val forwardingClientId = "agent-${deviceConfig.name}-fwd"
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
    private var taskTimeoutTimerId: Long? = null
    private val mcpClients = mutableListOf<McpClient>()

    // Pending tasks: taskId -> PendingTask (for timeout monitoring and correlation)
    data class PendingTask(val targetAgent: String, val input: String, val parentTaskId: String? = null, val submittedAt: Long = System.currentTimeMillis())
    data class CollectedResult(val targetAgent: String, val taskId: String, val parentTaskId: String?, val input: String, val status: String, val result: String)
    private val pendingTasks = ConcurrentHashMap<String, PendingTask>()
    private val collectedResults = java.util.concurrent.ConcurrentLinkedQueue<CollectedResult>()
    private val awaitingFutures = ConcurrentHashMap<String, CompletableFuture<CollectedResult>>()
    // Sub-agent tasks that timed out or were cancelled; late replies to them must not be handled as new tasks
    private val expiredTaskIds: MutableSet<String> = java.util.Collections.newSetFromMap(
        java.util.Collections.synchronizedMap(object : LinkedHashMap<String, Boolean>() {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, Boolean>?) = size > 1000
        })
    )

    // The task ID currently being processed by this agent. Set and reset on the LLM worker thread
    // (pool size 1, so executions never overlap) and read by tools, possibly from tool executor threads.
    @Volatile
    private var currentTaskId: String? = null

    @Volatile
    private var currentCallStack: List<String> = emptyList()

    @Volatile
    private var currentTransactionId: String? = null

    // Metrics
    private val messagesProcessed = AtomicLong(0)
    private val llmCalls = AtomicLong(0)
    private val errors = AtomicLong(0)
    private val totalInputTokens = AtomicLong(0)
    private val totalOutputTokens = AtomicLong(0)
    private val totalTokens = AtomicLong(0)

    /**
     * LangChain4j AI Service interface.
     * AiServices generates a proxy that handles the ReAct loop automatically.
     */
    interface AgentAiService {
        fun chat(@MemoryId sessionId: String, @UserText userMessage: String): Result<String>
    }

    interface AgentStreamingAiService {
        fun chat(@MemoryId sessionId: String, @UserText userMessage: String): TokenStream
    }

    /** A single agent invocation, carried from the event loop into the LLM worker thread. */
    private data class AgentRequest(
        val userMessage: String,
        val source: String,
        val triggerContext: TriggerContext? = null,
        val taskId: String? = null,
        val callStack: List<String> = emptyList(),
        val sessionId: String? = null,
        // Start with an empty conversation (new task on a stateless agent)
        val freshMemory: Boolean = false
    )

    /** Outcome of an LLM invocation (blocking or streaming). */
    private data class LlmOutcome(val text: String?, val toolExecutions: List<ToolExecution>)

    companion object {
        private const val DEFAULT_SESSION = "default"
        private const val MAX_SESSIONS = 100
    }

    override fun start(startPromise: Promise<Void>) {
        try {
            agentConfig = AgentConfig.fromJsonObject(deviceConfig.config)
            logger.fine("Starting agent ${deviceConfig.name} (provider: ${agentConfig.providerName ?: agentConfig.provider}, trigger: ${agentConfig.triggerType})")

            globalConfig = vertx.orCreateContext.config()
            llmWorkerExecutor = createLlmWorkerExecutor()

            // Initialize conversation log file if enabled
            if (agentConfig.conversationLogEnabled) {
                try {
                    val logDir = Paths.get("log", "agents")
                    Files.createDirectories(logDir)
                    val pattern = logDir.resolve("${deviceConfig.name}.log").toString()
                    val handler = java.util.logging.FileHandler(pattern, 10 * 1024 * 1024, 10, true)
                    handler.formatter = object : java.util.logging.Formatter() {
                        override fun format(record: java.util.logging.LogRecord) = record.message
                    }
                    val log = Logger.getLogger("agent.conversation.${deviceConfig.name}")
                    log.useParentHandlers = false
                    log.addHandler(handler)
                    conversationLog = log
                    conversationLogHandler = handler
                    logger.info("Conversation logging enabled for agent ${deviceConfig.name}: $pattern")
                } catch (e: Exception) {
                    logger.warning("Failed to open conversation log for agent ${deviceConfig.name}: ${e.message}")
                }
            }

            // Create LLM model with logging listener
            val llmListener = createLlmListener()

            if (!agentConfig.providerName.isNullOrBlank()) {
                // Look up the named GenAI provider from the device store
                val store = DeviceConfigStoreFactory.getSharedInstance()
                if (store != null) {
                    store.getDevice(agentConfig.providerName!!)
                        .onComplete { result ->
                            try {
                                val providerConfig = if (result.succeeded() && result.result() != null) {
                                    GenAiProviderConfig.fromJsonObject(result.result()!!.config)
                                } else {
                                    // Try config.yaml providers before falling back to direct config
                                    resolveConfigYamlProvider(agentConfig.providerName!!, globalConfig!!)
                                }
                                if (isDecisionAgent(providerConfig)) {
                                    initDecisionProvider(providerConfig)
                                } else if (providerConfig != null) {
                                    initChatModels(providerConfig, llmListener)
                                } else {
                                    logger.warning("Provider '${agentConfig.providerName}' not found, falling back to direct config")
                                    if (isDecisionAgent()) {
                                        initDecisionProvider()
                                    } else {
                                        initChatModels(null, llmListener)
                                    }
                                }
                                doStart(startPromise)
                            } catch (e: Exception) {
                                logger.severe("Failed to start agent ${deviceConfig.name}: ${e.message}")
                                startPromise.fail(e)
                            }
                        }
                } else {
                    // No device store — try config.yaml providers
                    val configProvider = resolveConfigYamlProvider(agentConfig.providerName!!, globalConfig!!)
                    if (isDecisionAgent(configProvider)) {
                        initDecisionProvider(configProvider)
                    } else if (configProvider != null) {
                        logger.fine("Using config.yaml provider '${agentConfig.providerName}'")
                        initChatModels(configProvider, llmListener)
                    } else {
                        logger.warning("Device store not available, falling back to direct config for provider '${agentConfig.providerName}'")
                        if (isDecisionAgent()) {
                            initDecisionProvider()
                        } else {
                            initChatModels(null, llmListener)
                        }
                    }
                    doStart(startPromise)
                }
            } else {
                if (isDecisionAgent()) {
                    initDecisionProvider()
                } else {
                    initChatModels(null, llmListener)
                }
                doStart(startPromise)
            }

        } catch (e: Exception) {
            logger.severe("Failed to start agent ${deviceConfig.name}: ${e.message}")
            e.printStackTrace()
            startPromise.fail(e)
        }
    }

    private fun initChatModels(providerConfig: GenAiProviderConfig?, llmListener: ChatModelListener) {
        val config = LangChain4jFactory.toChatModelConfig(agentConfig, providerConfig)
        modelConfig = config
        chatModel = LangChain4jFactory.createChatModel(config, globalConfig!!, listOf(llmListener))
        if (agentConfig.streamingEnabled) {
            streamingChatModel = LangChain4jFactory.createStreamingChatModel(config, globalConfig!!, listOf(llmListener))
            logger.info("Agent ${deviceConfig.name} token streaming enabled (${config.provider})")
        }
    }

    internal fun isDecisionAgent(providerConfig: GenAiProviderConfig? = null): Boolean {
        val providerType = (providerConfig?.type ?: agentConfig.provider).lowercase()
        return isDecisionProviderType(providerType)
    }

    internal fun isDecisionProviderType(providerType: String): Boolean {
        return providerType == "openrouter-decision" ||
               providerType == "decision" ||
               providerType.endsWith("-decision")
    }

    private fun initDecisionProvider(providerConfig: GenAiProviderConfig? = null) {
        val effectiveProvider = providerConfig?.type ?: agentConfig.provider
        val effectiveApiKey = agentConfig.apiKey ?: providerConfig?.apiKey ?: providerConfig?.baseUrl
        val apiKey = LangChain4jFactory.resolveApiKey(effectiveApiKey, effectiveProvider, globalConfig!!)
        val endpoint = agentConfig.endpoint ?: providerConfig?.endpoint
        decisionProvider = OpenRouterDecisionProvider(vertx, apiKey, endpoint)
        logger.info("Agent ${deviceConfig.name} initialized OpenRouterDecisionProvider (model: ${agentConfig.model ?: providerConfig?.model ?: "typesafe/jev-1.13"})")
    }

    /**
     * Resolves a GenAiProviderConfig from config.yaml's GenAI.Providers section by name.
     */
    private fun resolveConfigYamlProvider(name: String, globalConfig: JsonObject): GenAiProviderConfig? {
        val providers = globalConfig.getJsonObject("GenAI", JsonObject())
            .getJsonObject("Providers", JsonObject())
        val section = providers.getJsonObject(name) ?: return null
        val keyToType = mapOf(
            "AzureOpenAI" to "azure-openai",
            "Gemini"      to "gemini",
            "Claude"      to "claude",
            "OpenAI"      to "openai",
            "Ollama"      to "ollama",
            "LlamaCpp"    to "llamacpp",
            "OpenRouter"  to "openrouter"
        )
        val type = keyToType[name] ?: name.lowercase()
        return GenAiProviderConfig(
            type = type,
            model = section.getString("Model"),
            apiKey = section.getString("ApiKey"),
            endpoint = section.getString("Endpoint"),
            serviceVersion = section.getString("ServiceVersion"),
            baseUrl = section.getString("BaseUrl"),
            temperature = section.getDouble("Temperature", 0.7),
            maxTokens = section.getInteger("MaxTokens")
        )
    }

    private fun doStart(startPromise: Promise<Void>) {
        try {
            val sessionHandler = Monster.getSessionHandler()
            if (decisionProvider != null) {
                logger.info("Agent ${deviceConfig.name} running in DECISION mode (provider: ${decisionProvider?.providerName})")
            } else {
                // Create tools
                val archiveHandler = Monster.getArchiveHandler()
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
                    taskTimeoutSeconds = agentConfig.taskTimeoutSeconds,
                    getCurrentTaskId = { currentTaskId },
                    // Called on the worker thread before the task is published, so the wait handle
                    // exists before any reply can arrive on the event loop.
                    registerPendingTask = { taskId, targetAgent, input ->
                        awaitingFutures[taskId] = CompletableFuture()
                        pendingTasks[taskId] = PendingTask(targetAgent, input, parentTaskId = currentTaskId)
                    },
                    cancelPendingTask = { taskId -> discardPendingTask(taskId) },
                    subAgentsAllowAll = agentConfig.subAgentsAllowAll,
                    subAgents = agentConfig.subAgents,
                    visibleAgentTags = agentConfig.visibleAgentTags,
                    isolatedAgent = agentConfig.isolatedAgent,
                    allowedPublishTopics = agentConfig.allowedPublishTopics,
                    awaitSubAgentResult = { taskId, targetAgent, timeoutSec -> awaitSubAgentResult(taskId, targetAgent, timeoutSec) },
                    getCurrentCallStack = { currentCallStack },
                    maxCallDepth = agentConfig.maxCallDepth
                )

                // Build AI Service with ReAct loop
                buildAiService()
            }

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

            // Setup periodic pending task timeout checker
            setupTaskTimeoutChecker()

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
            // Cancel timers
            cronTimerId?.let { vertx.cancelTimer(it) }
            taskTimeoutTimerId?.let { vertx.cancelTimer(it) }
            pendingTasks.clear()
            collectedResults.clear()
            awaitingFutures.forEach { (_, future) -> future.cancel(true) }
            awaitingFutures.clear()
            ragTimerId?.let { vertx.cancelTimer(it) }
            ragTimerId = null
            ragIndex = null

            // Unsubscribe MQTT topics
            val sessionHandler = Monster.getSessionHandler()
            if (sessionHandler != null) {
                // Unsubscribe task topics from main client
                sessionHandler.unsubscribeInternalClient(clientId, a2aInboxTopic())
                sessionHandler.unsubscribeInternalClient(clientId, "${a2aInboxTopic()}/+")
                sessionHandler.unregisterInternalClient(clientId)

                // Unsubscribe input topics from forwarding client
                agentConfig.inputTopics.forEach { topic ->
                    sessionHandler.unsubscribeInternalClient(forwardingClientId, topic)
                }
                sessionHandler.unregisterInternalClient(forwardingClientId)
            }

            // Close MCP clients
            mcpClients.forEach { client ->
                try { client.close() } catch (e: Exception) {
                    logger.warning("Error closing MCP client: ${e.message}")
                }
            }
            mcpClients.clear()

            // Release chat memory (the persistent store is shared by all agents and ref-counted)
            synchronized(memories) { memories.clear() }
            chatMemoryStore?.let {
                it.evictAgentFromCache(agentName)
                MonsterChatMemoryStore.release(it)
            }
            chatMemoryStore = null

            // Close conversation log
            try { conversationLogHandler?.close() } catch (_: Exception) {}
            conversationLogHandler = null
            conversationLog = null

            // Close dedicated LLM worker executor
            try { llmWorkerExecutor?.close() } catch (e: Exception) {
                logger.warning("Error closing LLM worker executor for agent ${deviceConfig.name}: ${e.message}")
            }
            // Close decision provider
            try {
                (decisionProvider as? OpenRouterDecisionProvider)?.close()
            } catch (e: Exception) {
                logger.warning("Error closing decision provider: ${e.message}")
            }
            decisionProvider = null

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
        val address = EventBusAddresses.Client.messages(clientId)
        logger.info("Agent $agentName registering EventBus consumer at address: $address")
        vertx.eventBus().consumer<Any>(address) { busMessage ->
            try {
                when (val body = busMessage.body()) {
                    is BrokerMessage -> {
                        logger.info("Agent $agentName received message on topic: ${body.topicName}")
                        handleMqttMessage(body)
                    }
                    is BulkClientMessage -> {
                        logger.info("Agent $agentName received bulk message with ${body.messages.size} messages")
                        body.messages.forEach { handleMqttMessage(it) }
                    }
                    else -> logger.warning("Unknown message type: ${body?.javaClass?.simpleName}")
                }
            } catch (e: Exception) {
                logger.warning("Error processing MQTT message in agent ${deviceConfig.name}: ${e.message}")
            }
        }
    }

    private fun buildAiService() {
        if (agentConfig.persistMemory && chatMemoryStore == null) {
            val storeType = Monster.getConfigStoreType(globalConfig ?: JsonObject())
            chatMemoryStore = MonsterChatMemoryStore.acquire(storeType, globalConfig ?: JsonObject(), vertx)
        }
        synchronized(memories) { memories.clear() }

        if (agentConfig.ragEnabled && ragIndex == null) {
            setupRagIndex()
        }

        aiService = configureAiService(AiServices.builder(AgentAiService::class.java))
            .chatModel(chatModel)
            // Streaming invocations report tool calls through the TokenStream callbacks instead
            .beforeToolExecution { logToolRequest(it.request()) }
            .afterToolExecution { logToolResult(it) }
            .build()

        streamingAiService = streamingChatModel?.let { model ->
            configureAiService(AiServices.builder(AgentStreamingAiService::class.java))
                .streamingChatModel(model)
                .build()
        }
    }

    /** Applies the configuration shared by the blocking and the streaming AI service. */
    private fun <T> configureAiService(builder: AiServices<T>): AiServices<T> {
        val tools = mutableListOf<Any>(agentTools!!)
        ragIndex?.let { tools.add(AgentRagTools(it, agentConfig.ragMaxResults, ::publishToolLog)) }

        builder
            .chatMemoryProvider { memoryId -> memoryFor(memoryId.toString()) }
            .tools(tools)
            .maxSequentialToolsInvocations(agentConfig.maxToolIterations)
            // Lets independent tool calls of one LLM response (e.g. several invokeAgent calls) run in parallel
            .executeToolsConcurrently()
            .chatRequestTransformer { request -> injectContextData(request) }

        // Add MCP tool providers if configured
        if (agentConfig.mcpServers.isNotEmpty() || agentConfig.useMonsterMqMcp) {
            if (mcpToolProvider == null) {
                mcpToolProvider = createMcpToolProvider(agentConfig.mcpServers, agentConfig.useMonsterMqMcp, globalConfig!!)
            }
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
        return builder
    }

    private fun memoryFor(sessionId: String): TurnAwareChatMemory {
        synchronized(memories) {
            return memories.getOrPut(sessionId) {
                TurnAwareChatMemory(
                    id = "$agentName:$sessionId",
                    maxMessages = agentConfig.memoryWindowSize,
                    chatMemoryStore = chatMemoryStore
                )
            }
        }
    }

    /**
     * Prepends the current context data to the last user message of an outgoing LLM request.
     * Done at request level (not in the user message itself) so that context snapshots are
     * sent on every ReAct step but never accumulate in the chat memory.
     */
    private fun injectContextData(request: ChatRequest): ChatRequest {
        val context = currentContextData
        if (context.isNullOrBlank()) return request
        val messages = request.messages()
        val index = messages.indexOfLast { it is ChatUserMessage }
        if (index < 0) return request
        val original = messages[index] as ChatUserMessage
        val augmented = if (original.hasSingleText()) {
            ChatUserMessage.from("$context\n\n${original.singleText()}")
        } else {
            ChatUserMessage.from(listOf(TextContent.from(context)) + original.contents())
        }
        val newMessages = messages.toMutableList().also { it[index] = augmented }
        return request.toBuilder().messages(newMessages).build()
    }

    private fun logToolRequest(request: dev.langchain4j.agent.tool.ToolExecutionRequest) {
        val log = JsonObject()
            .put("type", "llm-tool-request")
            .put("timestamp", Instant.now().toString())
            .put("tool", request.name())
            .put("arguments", request.arguments())
        publishToAgentTopic("logs/llm", log)
        writeToConversationLog { sb ->
            sb.append("  TOOL_REQUEST:\n")
            sb.append("    tool: \"${request.name()}\"\n")
            sb.append("    timestamp: \"${Instant.now()}\"\n")
            sb.append("    arguments:\n")
            sb.append(formatJsonAsYaml(request.arguments() ?: "", 6))
            sb.append("\n\n")
        }
    }

    private fun logToolResult(execution: ToolExecution) {
        val request = execution.request()
        val log = JsonObject()
            .put("type", "llm-tool-result")
            .put("timestamp", Instant.now().toString())
            .put("tool", request.name())
            .put("failed", execution.hasFailed())
            .put("result", execution.result())
        publishToAgentTopic("logs/llm", log)
        writeToConversationLog { sb ->
            sb.append("  TOOL_RESULT:\n")
            sb.append("    tool: \"${request.name()}\"\n")
            sb.append("    timestamp: \"${Instant.now()}\"\n")
            sb.append("    failed: ${execution.hasFailed()}\n")
            val resultText = execution.result() ?: ""
            sb.append("    result: |\n")
            sb.append(indent(resultText, 6))
            sb.append("\n\n")
        }
    }

    private fun setupRagIndex() {
        try {
            val base = modelConfig
            val provider = agentConfig.embeddingProvider?.takeIf { it.isNotBlank() } ?: base?.provider ?: agentConfig.provider
            // Reuse credentials of the chat model only when the embedding uses the same provider
            val sameBase = base?.takeIf { provider.equals(it.provider, ignoreCase = true) }
            val embeddingModel = LangChain4jFactory.createEmbeddingModel(
                provider = provider,
                model = agentConfig.embeddingModel?.takeIf { it.isNotBlank() },
                apiKey = sameBase?.apiKey,
                endpoint = sameBase?.endpoint,
                serviceVersion = sameBase?.serviceVersion,
                globalConfig = globalConfig ?: JsonObject()
            )
            val index = AgentRagIndex(agentName, embeddingModel, agentConfig)
            ragIndex = index
            val refresh = {
                vertx.executeBlocking(Callable { index.refresh() }, false).onFailure { e ->
                    logger.warning("Agent $agentName RAG index refresh failed: ${e.message}")
                }
            }
            refresh()
            ragTimerId = vertx.setPeriodic(maxOf(agentConfig.ragRefreshSeconds, 10) * 1000) { refresh() }
            logger.info("Agent $agentName semantic search enabled (embedding provider: $provider)")
        } catch (e: Exception) {
            logger.warning("Agent $agentName could not enable semantic search: ${e.message}")
            ragIndex = null
        }
    }

    private fun setupMqttSubscriptions(sessionHandler: at.rocworks.handlers.SessionHandler) {
        // Subscribe to each input topic using a separate forwarding client so that
        // incoming messages are re-published to the inbox as normal MQTT messages
        // without causing a duplicate delivery on the main agent consumer.
        agentConfig.inputTopics.forEach { topicFilter ->
            logger.fine("Agent ${deviceConfig.name} subscribing forwarding client to: $topicFilter")
            sessionHandler.subscribeInternalClient(forwardingClientId, topicFilter, 0)
        }

        // Register a dedicated EventBus consumer for the forwarding client
        val fwdAddress = EventBusAddresses.Client.messages(forwardingClientId)
        vertx.eventBus().consumer<Any>(fwdAddress) { busMessage ->
            try {
                when (val body = busMessage.body()) {
                    is BrokerMessage -> forwardInputToInbox(body)
                    is BulkClientMessage -> body.messages.forEach { forwardInputToInbox(it) }
                    else -> logger.warning("Unknown message type in forwarding consumer: ${body?.javaClass?.simpleName}")
                }
            } catch (e: Exception) {
                logger.warning("Error in forwarding consumer for agent ${deviceConfig.name}: ${e.message}")
            }
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
                    executeAgent(AgentRequest(agentConfig.cronPrompt?.takeIf { it.isNotBlank() } ?: "It is ${toLocalTime(Instant.now())}. Execute your scheduled task.", "cron", TriggerContext(TriggerType.CRON)))
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
                executeAgent(AgentRequest(agentConfig.cronPrompt?.takeIf { it.isNotBlank() } ?: "It is ${toLocalTime(Instant.now())}. Execute your scheduled task.", "cron", TriggerContext(TriggerType.CRON)))
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

    private fun buildTriggerContextBlock(triggerContext: TriggerContext?): String {
        val triggerLines = mutableListOf<String>()
        if (triggerContext != null) {
            triggerLines.add("Triggered by: ${triggerContext.type}")
            if (triggerContext.topicName != null) {
                triggerLines.add("Triggering Topic: ${triggerContext.topicName}")
            }
            if (triggerContext.value != null) {
                triggerLines.add("Triggering Value: ${triggerContext.value}")
            }
        } else {
            triggerLines.add("Triggered by: MANUAL")
        }

        return if (triggerLines.isNotEmpty()) {
            "--- Trigger Context ---\n" + triggerLines.joinToString("\n") + "\n--- End Trigger Context ---\n\n"
        } else {
            ""
        }
    }

    private fun buildContextData(triggerContext: TriggerContext? = null): String {
        val triggerBlock = buildTriggerContextBlock(triggerContext)
        val sections = mutableListOf<ContextBudget.Section>()
        val contextLogLines = mutableListOf<String>()  // Summary for conversation log
        // Without a token budget a line limit protects the LLM from huge contexts;
        // with a budget more data may be fetched because it is truncated afterwards.
        val maxLines = if (agentConfig.contextMaxTokens > 0) 5000 else 500
        var lineCount = 0

        // Fetch from archive last-value stores
        if (agentConfig.contextLastvalTopics.isNotEmpty()) {
            val archiveGroups = Monster.getArchiveHandler()?.getDeployedArchiveGroups() ?: emptyMap()
            for ((groupName, topicFilters) in agentConfig.contextLastvalTopics) {
                val store = archiveGroups[groupName]?.lastValStore ?: continue
                for (filter in topicFilters) {
                    val lines = mutableListOf<String>()
                    store.findMatchingMessages(filter) { msg ->
                        val value = msg.getPayloadAsString()
                        lines.add("[Archive:$groupName] ${msg.topicName} = $value (${toLocalTime(msg.time)})")
                        lineCount++ < maxLines // safety limit
                    }
                    if (lines.isNotEmpty()) {
                        sections.add(ContextBudget.Section(null, lines))
                        contextLogLines.add("  LastValue archive=$groupName filter=$filter -> ${lines.size} values")
                    }
                }
            }
        }

        // Fetch retained messages
        if (agentConfig.contextRetainedTopics.isNotEmpty()) {
            val retainedStore = Monster.getRetainedStore()
            if (retainedStore != null) {
                for (filter in agentConfig.contextRetainedTopics) {
                    val lines = mutableListOf<String>()
                    retainedStore.findMatchingMessages(filter) { msg ->
                        val value = msg.getPayloadAsString()
                        lines.add("[Retained] ${msg.topicName} = $value")
                        lineCount++ < maxLines
                    }
                    if (lines.isNotEmpty()) {
                        sections.add(ContextBudget.Section(null, lines))
                        contextLogLines.add("  Retained filter=$filter -> ${lines.size} values")
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
                    // Raw history: pass result directly to the LLM
                    for (topic in query.topics) {
                        try {
                            val history = archiveStore.getHistory(topic, startTime, endTime, 500)
                            if (history.size() > 0) {
                                val title = "[History:${query.archiveGroup}:RAW] $topic (last ${query.lastSeconds}s, ${history.size()} records):"
                                contextLogLines.add("  History archive=${query.archiveGroup} mode=RAW topic=$topic range=${startTime}..${endTime} -> ${history.size()} rows")
                                val csv = jsonArrayToCsv(history, query.decimals).lines()
                                sections.add(ContextBudget.Section(title, csv, headLines = 1, keepTail = true))
                                lineCount += csv.size
                            }
                        } catch (e: Exception) {
                            logger.warning("Failed to fetch raw history for $topic in ${query.archiveGroup}: ${e.message}")
                        }
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
                        val rows = result.getJsonArray("rows")
                        val rowCount = rows?.size() ?: 0
                        if (rowCount > 0) {
                            val title = "[History:${query.archiveGroup}:${query.interval}:${query.function}] ${query.topics.joinToString(", ")} (last ${query.lastSeconds}s, $rowCount rows):"
                            contextLogLines.add("  History archive=${query.archiveGroup} mode=${query.interval}:${query.function} topics=${query.topics.joinToString(",")} range=${startTime}..${endTime} -> $rowCount rows")
                            val csv = columnarJsonToCsv(result, query.decimals).lines()
                            sections.add(ContextBudget.Section(title, csv, headLines = 1, keepTail = true))
                            lineCount += csv.size
                        }
                    } catch (e: Exception) {
                        logger.warning("Failed to fetch aggregated history for ${query.topics} in ${query.archiveGroup}: ${e.message}")
                    }
                }
                if (lineCount >= maxLines) break
            }
        }

        val now = Instant.now().atZone(localZone)
        val header = "--- Context Data (current time: ${now.format(localTimeFormatter)}, timezone: $localZone) ---"
        val footer = "--- End Context Data ---"

        // Apply the token budget; the trigger block, header and footer are never truncated
        val fitted = if (agentConfig.contextMaxTokens > 0) {
            val reserved = ContextBudget.estimateTokens(triggerBlock) + ContextBudget.estimateTokens(header) +
                ContextBudget.estimateTokens(footer) + 2
            val before = sections.sumOf { ContextBudget.sectionTokens(it) }
            val result = ContextBudget.fit(sections, maxOf(0, agentConfig.contextMaxTokens - reserved))
            val after = result.sumOf { ContextBudget.sectionTokens(it) }
            if (after < before) contextLogLines.add("  Budget: ~$before tokens truncated to ~$after (contextMaxTokens=${agentConfig.contextMaxTokens})")
            result
        } else sections
        val lines = ContextBudget.render(fitted)

        // Write context fetch summary to conversation log
        writeToConversationLog { sb ->
            sb.append("  CONTEXT:\n")
            sb.append("    timestamp: \"${Instant.now()}\"\n")
            if (contextLogLines.isEmpty()) {
                sb.append("    data: \"(no context data configured or returned)\"\n")
            } else {
                sb.append("    data: |\n")
                val dataStr = contextLogLines.joinToString("\n")
                sb.append(indent(dataStr, 6))
                sb.append("\n")
            }
            sb.append("\n")
        }

        if (lines.isEmpty()) return triggerBlock.trimEnd()

        val mainContext = "$header\n" +
            lines.joinToString("\n") +
            "\n$footer"

        return if (triggerBlock.isNotBlank()) {
            triggerBlock + mainContext
        } else {
            mainContext
        }
    }

    /**
     * Builds an immutable structured context snapshot as a JsonObject.
     * Used by Jev and structured decision models as the 'state' input.
     */
    fun buildContextSnapshot(triggerContext: TriggerContext? = null, input: String? = null): JsonObject {
        val state = JsonObject()

        // 1. Trigger context
        val triggerObj = JsonObject()
        if (triggerContext?.topicName != null) {
            triggerObj.put("topic", triggerContext.topicName)
            val raw = triggerContext.value ?: input
            if (raw != null) {
                triggerObj.put("payload", parseJsonOrString(raw))
            }
        } else if (input != null) {
            triggerObj.put("payload", parseJsonOrString(input))
        }
        if (!triggerObj.isEmpty) {
            state.put("trigger", triggerObj)
        }

        // 2. Current context (LastVal)
        if (agentConfig.contextLastvalTopics.isNotEmpty()) {
            val currentObj = JsonObject()
            val archiveGroups = Monster.getArchiveHandler()?.getDeployedArchiveGroups() ?: emptyMap()
            for ((groupName, topicFilters) in agentConfig.contextLastvalTopics) {
                val store = archiveGroups[groupName]?.lastValStore ?: continue
                for (filter in topicFilters) {
                    store.findMatchingMessages(filter) { msg ->
                        currentObj.put(msg.topicName, parseJsonOrString(msg.getPayloadAsString()))
                        currentObj.size() < 500
                    }
                }
            }
            state.put("current", currentObj)
        }

        // 3. Retained messages
        if (agentConfig.contextRetainedTopics.isNotEmpty()) {
            val retainedObj = JsonObject()
            val retainedStore = Monster.getRetainedStore()
            if (retainedStore != null) {
                for (filter in agentConfig.contextRetainedTopics) {
                    retainedStore.findMatchingMessages(filter) { msg ->
                        retainedObj.put(msg.topicName, parseJsonOrString(msg.getPayloadAsString()))
                        retainedObj.size() < 500
                    }
                }
            }
            state.put("retained", retainedObj)
        }

        // 4. Historical context
        if (agentConfig.contextHistoryQueries.isNotEmpty()) {
            val historyObj = JsonObject()
            val archiveGroups = Monster.getArchiveHandler()?.getDeployedArchiveGroups() ?: emptyMap()
            for (query in agentConfig.contextHistoryQueries) {
                if (query.topics.isEmpty()) continue
                val archiveGroup = archiveGroups[query.archiveGroup] ?: continue
                val archiveStore = archiveGroup.archiveStore
                if (archiveStore !is at.rocworks.stores.IMessageArchiveExtended) continue

                val endTime = Instant.now()
                val startTime = endTime.minusSeconds(query.lastSeconds.toLong())

                if (query.isRaw()) {
                    val rawObj = JsonObject()
                    for (topic in query.topics) {
                        try {
                            val history = archiveStore.getHistory(topic, startTime, endTime, 500)
                            rawObj.put(topic, history)
                        } catch (e: Exception) {
                            logger.warning("Failed to fetch raw history for $topic: ${e.message}")
                        }
                    }
                    historyObj.put("${query.archiveGroup}:RAW", rawObj)
                } else {
                    try {
                        val result = archiveStore.getAggregatedHistory(
                            topics = query.topics,
                            startTime = startTime,
                            endTime = endTime,
                            intervalMinutes = query.intervalMinutes(),
                            functions = listOf(query.function.uppercase()),
                            fields = query.fields
                        )
                        historyObj.put("${query.archiveGroup}:${query.interval}:${query.function}", result)
                    } catch (e: Exception) {
                        logger.warning("Failed to fetch aggregated history for ${query.topics}: ${e.message}")
                    }
                }
            }
            state.put("history", historyObj)
        }

        return state
    }

    private fun parseJsonOrString(raw: String): Any {
        val trimmed = raw.trim()
        if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
            try { return JsonObject(trimmed) } catch (_: Exception) {}
        } else if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
            try { return JsonArray(trimmed) } catch (_: Exception) {}
        }
        return trimmed.toDoubleOrNull() ?: trimmed.toLongOrNull() ?: trimmed.toBooleanStrictOrNull() ?: raw
    }

    fun extractDecisionQuestions(): JsonObject {
        val prompt = agentConfig.systemPrompt.trim()
        if (prompt.isNotBlank()) {
            try {
                if (prompt.startsWith("[") && prompt.endsWith("]")) {
                    val array = JsonArray(prompt)
                    val questionsObj = JsonObject()
                    for (i in 0 until array.size()) {
                        val item = array.getValue(i)
                        if (item is JsonObject) {
                            val id = item.getString("id")
                                ?: item.getString("name")
                                ?: item.getString("key")
                                ?: "q${i + 1}"
                            val qObj = JsonObject()
                            val rawType = item.getString("type", "noul").lowercase()
                            val type = when (rawType) {
                                "boolean", "bool", "noul" -> "noul"
                                "categorical", "choice", "select" -> "choice"
                                "scale", "score", "rating" -> "score"
                                else -> rawType
                            }
                            qObj.put("type", type)
                            val instructions = item.getString("instructions")
                                ?: item.getString("question")
                                ?: item.getString("desc")
                                ?: id
                            qObj.put("instructions", instructions)
                            if (item.containsKey("criteria")) {
                                qObj.put("criteria", item.getValue("criteria"))
                            } else if (item.containsKey("options")) {
                                val opts = item.getValue("options")
                                if (opts is JsonArray) {
                                    if (type == "score") {
                                        qObj.put("criteria", opts)
                                    } else {
                                        val optObj = JsonObject()
                                        opts.forEach { o -> optObj.put(o.toString(), o.toString()) }
                                        qObj.put("criteria", optObj)
                                    }
                                } else {
                                    qObj.put("criteria", opts)
                                }
                            } else {
                                if (type == "noul") {
                                    qObj.put("criteria", JsonObject().put("true", "True").put("false", "False"))
                                }
                            }
                            questionsObj.put(id, qObj)
                        }
                    }
                    if (!questionsObj.isEmpty) return questionsObj
                } else {
                    val jsonPrompt = JsonObject(prompt)
                    if (jsonPrompt.containsKey("questions") && jsonPrompt.getValue("questions") is JsonObject) {
                        return jsonPrompt.getJsonObject("questions")
                    } else if (!jsonPrompt.isEmpty && jsonPrompt.fieldNames().any { key ->
                        val v = jsonPrompt.getValue(key)
                        v is JsonObject && (v.containsKey("type") || v.containsKey("instructions") || v.containsKey("question"))
                    }) {
                        return jsonPrompt
                    }
                }
            } catch (_: Exception) {}
        }

        if (agentConfig.skills.isNotEmpty()) {
            val questionsObj = JsonObject()
            for (skill in agentConfig.skills) {
                val qObj = JsonObject()
                val schema = skill.inputSchema ?: JsonObject()
                val qType = schema.getString("type", "choice")
                qObj.put("type", qType)
                qObj.put("instructions", skill.description.ifBlank { skill.name })
                if (schema.containsKey("criteria")) {
                    qObj.put("criteria", schema.getValue("criteria"))
                } else {
                    if (qType == "noul") {
                        qObj.put("criteria", JsonObject().put("true", "Yes / True").put("false", "No / False"))
                    } else if (qType == "choice") {
                        qObj.put("criteria", JsonObject().put("option_a", "Option A").put("option_b", "Option B"))
                    }
                }
                questionsObj.put(skill.name, qObj)
            }
            if (!questionsObj.isEmpty) return questionsObj
        }

        val instructions = if (prompt.isNotBlank()) prompt else "Evaluate telemetry context and classify operating status."
        return JsonObject().put("decision", JsonObject()
            .put("type", "noul")
            .put("instructions", instructions)
            .put("criteria", JsonObject()
                .put("true", "Normal / affirmative")
                .put("false", "Anomaly / negative")
            )
        )
    }

    private val localZone: ZoneId by lazy {
        agentConfig.timezone?.let {
            try { ZoneId.of(it) } catch (_: Exception) { ZoneId.systemDefault() }
        } ?: ZoneId.systemDefault()
    }
    private val localTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX")

    private fun toLocalTime(utcString: String): String {
        return try {
            val instant = Instant.parse(utcString)
            instant.atZone(localZone).format(localTimeFormatter)
        } catch (_: Exception) {
            utcString
        }
    }

    private fun toLocalTime(instant: Instant): String {
        return instant.atZone(localZone).format(localTimeFormatter)
    }

    private fun formatValue(value: Any?, decimals: Int?): String {
        if (value == null) return ""
        if (decimals != null && value is Number) {
            return "%.${decimals}f".format(value.toDouble())
        }
        val str = when (value) {
            is io.vertx.core.json.JsonObject -> value.encode()
            is io.vertx.core.json.JsonArray -> value.encode()
            else -> value.toString()
        }
        // Convert UTC timestamps to local time
        if (str.length > 18 && str[10] == 'T' && str.endsWith("Z")) {
            return toLocalTime(str)
        }
        // Escape CSV if the value contains characters that would break column separation.
        if (str.contains(',') || str.contains('"') || str.contains('\n') || str.contains('\r')) {
            return "\"" + str.replace("\"", "\"\"") + "\""
        }
        return str
    }

    /**
     * Convert a columnar JSON result ({"columns":[...], "rows":[[...],...]}) to CSV.
     */
    private fun columnarJsonToCsv(result: io.vertx.core.json.JsonObject, decimals: Int? = null): String {
        val columns = result.getJsonArray("columns") ?: return ""
        val rows = result.getJsonArray("rows") ?: return ""
        val sb = StringBuilder()
        sb.appendLine(columns.joinToString(","))
        for (i in 0 until rows.size()) {
            val row = rows.getJsonArray(i) ?: continue
            sb.appendLine((0 until row.size()).joinToString(",") { formatValue(row.getValue(it), decimals) })
        }
        return sb.toString().trimEnd()
    }

    /**
     * Convert a JsonArray of JsonObjects to CSV (using keys from the first object as headers).
     */
    private fun jsonArrayToCsv(array: io.vertx.core.json.JsonArray, decimals: Int? = null): String {
        if (array.size() == 0) return ""
        val first = array.getJsonObject(0) ?: return ""
        val keys = first.fieldNames().toList()
        val sb = StringBuilder()
        sb.appendLine(keys.joinToString(","))
        for (i in 0 until array.size()) {
            val obj = array.getJsonObject(i) ?: continue
            sb.appendLine(keys.joinToString(",") { formatValue(obj.getValue(it), decimals) })
        }
        return sb.toString().trimEnd()
    }

    private fun handleMqttMessage(msg: BrokerMessage) {
        val inboxPrefix = a2aInboxTopic()

        // Log all inbox messages
        if (msg.topicName.startsWith(inboxPrefix)) {
            logger.info("Agent $agentName inbox message [${msg.topicName}]: ${String(msg.payload, Charsets.UTF_8).take(500)}")
        }

        // 1. Messages on inbox sub-topics (inbox/{taskId}) — data-driven routing
        if (msg.topicName.startsWith("$inboxPrefix/")) {
            val taskId = msg.topicName.substringAfterLast("/")
            when {
                // Reply to a sub-agent task we submitted
                pendingTasks.containsKey(taskId) -> handleSubAgentReply(msg)
                // Late reply to a task that already timed out or was cancelled. Handling it as a new
                // task would make the agents answer each other forever.
                taskId in expiredTaskIds || isReplyPayload(msg) ->
                    logger.info("Agent $agentName ignoring late reply for task $taskId")
                // New incoming task (from another agent or forwarded input)
                else -> handleTaskMessage(msg)
            }
            return
        }

        // 2. Backward compat: base inbox topic (external agents may still use it)
        if (msg.topicName == inboxPrefix) {
            handleTaskMessage(msg)
            return
        }

        // 3. Unexpected topic — input topics are handled by the forwarding client
        logger.warning("Agent $agentName received unexpected message on topic: ${msg.topicName}")
    }

    /** A task reply carries a status but no input; a task request always has an input. */
    private fun isReplyPayload(msg: BrokerMessage): Boolean {
        val json = try { JsonObject(String(msg.payload, Charsets.UTF_8)) } catch (_: Exception) { return false }
        return json.containsKey("status") && !json.containsKey("input")
    }

    private fun forwardInputToInbox(msg: BrokerMessage) {
        val sessionHandler = Monster.getSessionHandler() ?: return
        val payloadStr = String(msg.payload, Charsets.UTF_8)

        val taskId = Utils.getUuid()

        // Try to parse as JSON; embed as JSON if valid, otherwise as text
        val inputValue: Any = try {
            JsonObject(payloadStr)
        } catch (_: Exception) {
            try {
                JsonArray(payloadStr)
            } catch (_: Exception) {
                payloadStr
            }
        }

        val taskJson = JsonObject()
            .put("taskId", taskId)
            .put("input", inputValue)
            .put("sourceTopic", msg.topicName)

        val inboxMsg = BrokerMessage(clientId, "${a2aInboxTopic()}/$taskId", taskJson.encode())
        logger.fine("Agent $agentName forwarding input topic [${msg.topicName}] to inbox as task $taskId")
        sessionHandler.publishMessage(inboxMsg)
    }

    private fun handleSubAgentReply(msg: BrokerMessage) {
        val payload = String(msg.payload, Charsets.UTF_8)
        val replyJson = try { JsonObject(payload) } catch (_: Exception) { null }

        val taskId = replyJson?.getString("taskId") ?: msg.topicName.substringAfterLast("/")
        val status = replyJson?.getString("status") ?: "unknown"
        val result = replyJson?.getValue("result")?.let { value ->
            when (value) {
                is String -> value
                is JsonObject -> value.encode()
                is JsonArray -> {
                    // Extract text from A2A result array: [{"type":"text","text":"..."},...]
                    val texts = (0 until value.size()).mapNotNull { i ->
                        value.getJsonObject(i)?.getString("text")
                    }
                    if (texts.isNotEmpty()) texts.joinToString("\n") else value.encode()
                }
                else -> value.toString()
            }
        } ?: replyJson?.getString("error") ?: payload

        // Remove from pending tasks and collect the result
        val pending = pendingTasks.remove(taskId) ?: return
        val collected = CollectedResult(pending.targetAgent, taskId, pending.parentTaskId, pending.input, status, result)

        // The waiting worker thread removes the future itself (the reply may arrive before it waits)
        val future = awaitingFutures[taskId]
        if (future != null) {
            future.complete(collected)
            logger.info("Agent $agentName completed synchronous wait for task $taskId from ${pending.targetAgent} (status=$status)")
        } else {
            collectedResults.add(collected)
            logger.info("Agent $agentName collected reply for task $taskId from ${pending.targetAgent} (status=$status, remaining=${pendingTasks.size})")

            // When all pending tasks are resolved, resume with compiled results
            if (pendingTasks.isEmpty()) {
                resumeWithCollectedResults()
            }
        }
    }

    /**
     * Called when all pending sub-agent tasks have completed (or timed out).
     * Compiles all collected results into a single message and feeds it to the LLM once.
     */
    private fun resumeWithCollectedResults() {
        val results = mutableListOf<CollectedResult>()
        while (collectedResults.isNotEmpty()) {
            collectedResults.poll()?.let { results.add(it) }
        }
        if (results.isEmpty()) return

        val resultText = results.joinToString("\n\n") { r ->
            val parentInfo = if (r.parentTaskId != null) ", parentTaskId=${r.parentTaskId}" else ""
            "Agent '${r.targetAgent}' (taskId=${r.taskId}$parentInfo, status=${r.status}, request='${r.input.take(100)}'):\n${r.result}"
        }

        val userMessage = "[Sub-agent results received]\n$resultText\n\n[All tasks complete. Summarize the results and respond to the user. Do NOT invoke more agents unless the user explicitly asks.]"
        executeAgent(AgentRequest(userMessage, "task-results", TriggerContext(TriggerType.MANUAL)))
    }

    /**
     * Blocks the calling LLM worker (or tool) thread until the sub-agent task registered with
     * [registerPendingTask][AgentTools] completes, fails or times out.
     */
    private fun awaitSubAgentResult(taskId: String, targetAgent: String, timeoutSec: Long): String {
        val future = awaitingFutures[taskId]
            ?: return "Error: task $taskId to agent '$targetAgent' is not pending (cancelled or already expired)."
        return try {
            // The periodic timeout checker usually completes the future first; this is the hard limit
            val collected = future.get(timeoutSec + 30, TimeUnit.SECONDS)
            when (collected.status) {
                "completed" -> collected.result
                "timeout" -> "Agent '$targetAgent' did not respond within $timeoutSec seconds (taskId=$taskId)."
                else -> "Agent '$targetAgent' task $taskId ended with status '${collected.status}': ${collected.result}"
            }
        } catch (e: java.util.concurrent.TimeoutException) {
            discardPendingTask(taskId)
            "Agent '$targetAgent' did not respond within $timeoutSec seconds (taskId=$taskId)."
        } catch (e: Exception) {
            discardPendingTask(taskId)
            "Error waiting for agent '$targetAgent' (taskId=$taskId): ${e.message}"
        } finally {
            awaitingFutures.remove(taskId)
        }
    }

    /** Forgets a sub-agent task; a reply arriving later is ignored instead of being handled as a new task. */
    private fun discardPendingTask(taskId: String) {
        expiredTaskIds.add(taskId)
        pendingTasks.remove(taskId)
        awaitingFutures.remove(taskId)?.cancel(true)
    }

    private fun setupTaskTimeoutChecker() {
        val checkIntervalMs = 15_000L // check every 15 seconds
        val timeoutMs = agentConfig.taskTimeoutSeconds * 1000

        taskTimeoutTimerId = vertx.setPeriodic(checkIntervalMs) {
            val now = System.currentTimeMillis()
            val timedOut = pendingTasks.entries.filter { now - it.value.submittedAt > timeoutMs }
            timedOut.forEach { (taskId, pending) ->
                pendingTasks.remove(taskId)
                expiredTaskIds.add(taskId)
                logger.warning("Agent $agentName task $taskId to '${pending.targetAgent}' timed out after ${agentConfig.taskTimeoutSeconds}s")
                val timeoutResult = CollectedResult(pending.targetAgent, taskId, pending.parentTaskId, pending.input, "timeout",
                    "Agent '${pending.targetAgent}' did not respond within ${agentConfig.taskTimeoutSeconds} seconds")
                val future = awaitingFutures[taskId]
                if (future != null) {
                    future.complete(timeoutResult)
                } else {
                    collectedResults.add(timeoutResult)
                }
            }
            // If timeouts cleared all pending tasks, resume with whatever we have
            if (pendingTasks.isEmpty() && collectedResults.isNotEmpty()) {
                resumeWithCollectedResults()
            }
        }
    }

    private fun setupTaskSubscription(sessionHandler: at.rocworks.handlers.SessionHandler) {
        val inboxTopic = a2aInboxTopic()
        // Subscribe with QoS 0: internal clients have no clientStatus, so QoS 1/2 messages
        // would be treated as "offline without persistent session" and silently dropped.
        logger.info("Agent $agentName subscribing to inbox: $inboxTopic")
        sessionHandler.subscribeInternalClient(clientId, inboxTopic, 0)
        // Also subscribe to inbox/+ so we receive sub-agent replies (replyTo = inbox/{taskId})
        val inboxReplyTopic = "${inboxTopic}/+"
        logger.info("Agent $agentName subscribing to inbox replies: $inboxReplyTopic")
        sessionHandler.subscribeInternalClient(clientId, inboxReplyTopic, 0)
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
                val request = AgentRequest(taskMessage, "task:$taskId", TriggerContext(TriggerType.MANUAL),
                    taskId = taskId, freshMemory = !agentConfig.stateEnabled)
                executeAgentRequest(request) { response, _ ->
                    publishTaskStatus(taskId, if (response != null) "completed" else "failed")
                    if (response != null) publishResponse(response)
                }
                return
            }

            val taskId = taskJson.getString("taskId") ?: Utils.getUuid()
            val parentTaskId = taskJson.getString("parentTaskId")
            // Use "input" field if present (MonsterMQ format), otherwise pass the full JSON to the LLM.
            // Input can be a string, JSON object, or JSON array — serialize structured types.
            val input = when (val raw = taskJson.getValue("input")) {
                is String -> raw
                is JsonObject -> raw.encode()
                is JsonArray -> raw.encode()
                null -> payload
                else -> raw.toString()
            }
            val replyTo = taskJson.getString("replyTo") ?: a2aStatusTopic(taskId)
            val skill = taskJson.getString("skill")
            val callerAgent = taskJson.getString("callerAgent") ?: taskJson.getString("from") ?: "unknown"
            val sourceTopic = taskJson.getString("sourceTopic")

            logger.info("Agent ${deviceConfig.name} received task $taskId from $callerAgent (replyTo=$replyTo)")

            val callStack = taskJson.getJsonArray("callStack")?.mapNotNull { it?.toString() } ?: emptyList()
            // Optional conversation id: tasks with the same sessionId share one chat memory
            val sessionId = taskJson.getString("sessionId")?.takeIf { it.isNotBlank() }

            // Publish working status
            publishTaskStatus(taskId, "working", parentTaskId)

            // Build the user message for the LLM
            val header = buildString {
                append("[Task from agent '$callerAgent', taskId=$taskId")
                if (skill != null) append(", skill=$skill")
                if (sourceTopic != null) append(", topic=$sourceTopic")
                append("]")
            }
            val taskMessage = "$header\n$input"

            val triggerContext = if (sourceTopic != null) {
                TriggerContext(TriggerType.MQTT, topicName = sourceTopic, value = input)
            } else {
                TriggerContext(TriggerType.MANUAL)
            }

            // The task ID and call stack are set on the worker thread while this task runs, so
            // sub-agent calls can reference it as parentTaskId and detect call cycles.
            val request = AgentRequest(
                userMessage = taskMessage,
                source = "task:$taskId",
                triggerContext = triggerContext,
                taskId = taskId,
                callStack = callStack,
                sessionId = sessionId,
                freshMemory = !agentConfig.stateEnabled && sessionId == null
            )
            executeAgentRequest(request) { response, error ->
                val sessionHandler = Monster.getSessionHandler() ?: return@executeAgentRequest
                if (error != null) {
                    // Publish error response
                    val errorJson = JsonObject()
                        .put("taskId", taskId)
                        .put("status", "failed")
                        .put("agent", agentName)
                        .put("error", error)
                    val responseMsg = BrokerMessage(clientId, replyTo, errorJson.encode())
                    sessionHandler.publishMessage(responseMsg)
                    publishTaskStatus(taskId, "failed", parentTaskId)
                } else {
                    // Publish success response
                    val resultJson = JsonObject()
                        .put("taskId", taskId)
                        .put("status", "completed")
                        .put("agent", agentName)
                        .put("result", response)
                    val responseMsg = BrokerMessage(clientId, replyTo, resultJson.encode())
                    sessionHandler.publishMessage(responseMsg)
                    publishTaskStatus(taskId, "completed", parentTaskId)
                    // Also publish to configured output topics
                    if (response != null) publishResponse(response)
                }
            }
        } catch (e: Exception) {
            logger.warning("Agent ${deviceConfig.name} failed to handle task: ${e.message}")
        }
    }

    private fun publishTaskStatus(taskId: String, status: String, parentTaskId: String? = null) {
        val sessionHandler = Monster.getSessionHandler() ?: return
        val statusJson = JsonObject()
            .put("taskId", taskId)
            .put("status", status)
            .put("agent", deviceConfig.name)
            .put("timestamp", Instant.now().toString())
        if (parentTaskId != null) statusJson.put("parentTaskId", parentTaskId)
        val msg = BrokerMessage(clientId, a2aStatusTopic(taskId), statusJson.encode())
        sessionHandler.publishMessage(msg)
    }

    private fun createLlmWorkerExecutor(): WorkerExecutor {
        val maxExecuteSeconds = maxOf(agentConfig.taskTimeoutSeconds + 60, 60 * 60)
        logger.fine("Agent ${deviceConfig.name} LLM worker max execute time: ${maxExecuteSeconds}s")
        return vertx.createSharedWorkerExecutor(
            "agent-llm-${deviceConfig.name}",
            1,
            maxExecuteSeconds,
            TimeUnit.SECONDS
        )
    }

    private fun executeLlmBlocking(block: () -> LlmOutcome): Future<LlmOutcome> {
        val callable = Callable { block() }
        return llmWorkerExecutor?.executeBlocking(callable) ?: vertx.executeBlocking(callable)
    }

    private fun executeDecisionBlocking(block: () -> String): Future<String> {
        val callable = Callable { block() }
        return llmWorkerExecutor?.executeBlocking(callable) ?: vertx.executeBlocking(callable)
    }

    private fun executeDecisionAgentWithCallback(
        userMessage: String,
        source: String,
        triggerContext: TriggerContext? = null,
        taskId: String? = null,
        callback: (String?, String?) -> Unit
    ) {
        val provider = decisionProvider ?: run {
            callback(null, "Decision provider not available")
            return
        }

        val txId = Utils.getUuid()
        currentTransactionId = txId
        writeToConversationLog { sb ->
            sb.append("================================================================================\n")
            sb.append("TRANSACTION START (DECISION) | ID: $txId | Time: ${Instant.now()} | Source: $source")
            if (taskId != null) {
                sb.append(" | Task ID: $taskId")
            }
            sb.append("\n--------------------------------------------------------------------------------\n\n")
        }

        messagesProcessed.incrementAndGet()
        publishHealthStatus("running")
        logger.fine("Agent ${deviceConfig.name} processing decision task from $source")

        executeDecisionBlocking {
            llmCalls.incrementAndGet()
            val state = buildContextSnapshot(triggerContext, userMessage)
            val questions = extractDecisionQuestions()
            val model = agentConfig.model?.takeIf { it.isNotBlank() } ?: "typesafe/jev-1.13"

            writeToConversationLog { sb ->
                sb.append("  DECISION_REQUEST:\n")
                sb.append("    model: \"$model\"\n")
                sb.append("    timestamp: \"${Instant.now()}\"\n")
                sb.append("    questions:\n")
                sb.append(formatJsonAsYaml(questions.encode(), 6))
                sb.append("\n    state:\n")
                sb.append(formatJsonAsYaml(state.encode(), 6))
                sb.append("\n\n")
            }

            val startTime = System.currentTimeMillis()
            val debugInput = JsonObject()
                .put("model", model)
                .put("timestamp", Instant.now().toString())
                .put("questions", questions)
                .put("state", state)
            publishDebug("input", debugInput)

            try {
                val future = provider.decide(model, questions, state, agentConfig.taskTimeoutSeconds)
                val responseJson = future.get(agentConfig.taskTimeoutSeconds + 5, TimeUnit.SECONDS)
                val durationMs = System.currentTimeMillis() - startTime

                publishDebug("output", responseJson)

                writeToConversationLog { sb ->
                    sb.append("  DECISION_RESPONSE:\n")
                    sb.append("    timestamp: \"${Instant.now()}\"\n")
                    sb.append("    durationMs: $durationMs\n")
                    sb.append("    response:\n")
                    sb.append(formatJsonAsYaml(responseJson.encode(), 6))
                    sb.append("\n\n")
                }

                responseJson.encode()
            } catch (e: Exception) {
                val durationMs = System.currentTimeMillis() - startTime
                val cause = e.cause ?: e
                val debugError = JsonObject()
                    .put("timestamp", Instant.now().toString())
                    .put("durationMs", durationMs)
                    .put("error", cause.message)
                publishDebug("error", debugError)

                writeToConversationLog { sb ->
                    sb.append("  DECISION_ERROR:\n")
                    sb.append("    timestamp: \"${Instant.now()}\"\n")
                    sb.append("    durationMs: $durationMs\n")
                    sb.append("    error: \"${cause.message}\"\n\n")
                }
                throw cause
            }
        }.onComplete { result ->
            writeToConversationLog { sb ->
                sb.append("--------------------------------------------------------------------------------\n")
                sb.append("TRANSACTION END (DECISION) | ID: $txId | Status: ${if (result.succeeded()) "SUCCESS" else "FAILED"}\n")
                sb.append("================================================================================\n\n")
            }
            if (txId == currentTransactionId) {
                currentTransactionId = null
            }
            if (result.succeeded()) {
                val response = result.result()
                callback(response, null)
            } else {
                errors.incrementAndGet()
                val cause = result.cause()
                logger.warning("Decision Agent ${deviceConfig.name} failed: ${cause?.message}")
                publishError(cause?.message ?: "Unknown error")
                callback(null, cause?.message ?: "Unknown error")
            }
            publishHealthStatus("ready")
        }
    }

    /** Runs a request and publishes the response (or error) to the configured output topics. */
    private fun executeAgent(request: AgentRequest) {
        executeAgentRequest(request) { response, error ->
            if (response != null) publishResponse(response)
            else if (decisionProvider == null) publishError(error ?: "Unknown error")
        }
    }

    private fun executeAgentRequest(request: AgentRequest, callback: (String?, String?) -> Unit) {
        if (decisionProvider != null) {
            executeDecisionAgentWithCallback(request.userMessage, request.source, request.triggerContext, request.taskId, callback)
            return
        }

        if (aiService == null) {
            callback(null, "Agent service not available")
            return
        }

        messagesProcessed.incrementAndGet()
        publishHealthStatus("running")
        logger.fine("Agent ${deviceConfig.name} processing message from ${request.source}")

        // Everything below runs on the agent's single LLM worker thread, so requests are processed
        // one after another and the per-request state (task, call stack, context) never overlaps.
        executeLlmBlocking {
            val txId = Utils.getUuid()
            currentTransactionId = txId
            currentTaskId = request.taskId
            currentCallStack = request.callStack
            writeToConversationLog { sb ->
                sb.append("================================================================================\n")
                sb.append("TRANSACTION START | ID: $txId | Time: ${Instant.now()} | Source: ${request.source}")
                if (request.taskId != null) sb.append(" | Task ID: ${request.taskId}")
                if (request.sessionId != null) sb.append(" | Session: ${request.sessionId}")
                sb.append("\n--------------------------------------------------------------------------------\n\n")
            }
            var success = false
            try {
                llmCalls.incrementAndGet()
                val sessionId = request.sessionId ?: DEFAULT_SESSION
                if (request.freshMemory) memoryFor(sessionId).clear()
                currentContextData = buildContextData(request.triggerContext).takeIf { it.isNotBlank() }
                val outcome = try {
                    invokeLlm(sessionId, request)
                } catch (e: Exception) {
                    // Providers such as Gemini reject invalid message ordering in the chat history.
                    // Clear this session's memory (including the persisted copy) and retry once.
                    val fullErrorMessage = generateSequence(e as Throwable?) { it.cause }.joinToString(" | ") { it.message ?: "" }
                    if (fullErrorMessage.contains("function call turn") ||
                        fullErrorMessage.contains("function response turn") ||
                        fullErrorMessage.contains("INVALID_ARGUMENT")) {
                        logger.warning("Agent ${deviceConfig.name} chat history invalid, clearing session '$sessionId' and retrying: ${e.message?.take(200)}")
                        memoryFor(sessionId).clear()
                        invokeLlm(sessionId, request)
                    } else {
                        throw e
                    }
                }
                success = true
                outcome
            } finally {
                writeToConversationLog { sb ->
                    sb.append("--------------------------------------------------------------------------------\n")
                    sb.append("TRANSACTION END | ID: $txId | Status: ${if (success) "SUCCESS" else "FAILED"}\n")
                    sb.append("================================================================================\n\n")
                }
                currentContextData = null
                currentTaskId = null
                currentCallStack = emptyList()
                currentTransactionId = null
            }
        }.onComplete { result ->
            if (result.succeeded()) {
                val outcome = result.result()
                // Log MCP/tool executions that went through LangChain4j's tool provider
                outcome.toolExecutions.forEach { toolExecution ->
                    val req = toolExecution.request()
                    // Skip native @Tool calls — those are already logged via publishToolLog
                    if (agentTools?.isNativeTool(req.name()) == true) return@forEach
                    val log = JsonObject()
                        .put("type", "mcp-tool-call")
                        .put("timestamp", Instant.now().toString())
                        .put("tool", req.name())
                        .put("arguments", req.arguments()?.take(500))
                        .put("result", toolExecution.result()?.take(1000))
                    publishToAgentTopic("logs/mcp", log)
                }
                if (outcome.text != null) {
                    callback(outcome.text, null)
                } else {
                    callback(null, "LLM returned null response")
                }
            } else {
                errors.incrementAndGet()
                val cause = result.cause()
                logger.warning("Agent ${deviceConfig.name} LLM call failed: ${cause?.message}")
                if (cause != null) logger.fine { cause.stackTraceToString() }
                callback(null, cause?.message ?: "Unknown error")
            }
            publishHealthStatus("ready")
        }
    }

    private fun invokeLlm(sessionId: String, request: AgentRequest): LlmOutcome {
        streamingAiService?.let { return invokeStreaming(it, sessionId, request) }
        val service = aiService ?: throw IllegalStateException("Agent service not available")
        val result = service.chat(sessionId, request.userMessage)
        return LlmOutcome(result.content(), result.toolExecutions() ?: emptyList())
    }

    /**
     * Streams the answer token by token to a2a/v1/{org}/{site}/agents/{name}/stream/{taskId}
     * (the transaction ID is used for requests without a task) and blocks until it is complete.
     * The final chunk has done=true.
     */
    private fun invokeStreaming(service: AgentStreamingAiService, sessionId: String, request: AgentRequest): LlmOutcome {
        val streamId = request.taskId ?: currentTransactionId ?: Utils.getUuid()
        val topic = a2aAgentTopic("stream/$streamId")
        val seq = AtomicLong(0)
        val executions = java.util.Collections.synchronizedList(mutableListOf<ToolExecution>())
        val completion = CompletableFuture<ChatResponse>()
        var error: String? = null
        try {
            service.chat(sessionId, request.userMessage)
                .onPartialResponse { token -> publishStreamChunk(topic, streamId, seq.getAndIncrement(), token, false) }
                .beforeToolExecution { logToolRequest(it.request()) }
                .onToolExecuted { execution ->
                    executions.add(execution)
                    logToolResult(execution)
                }
                .onCompleteResponse { completion.complete(it) }
                .onError { completion.completeExceptionally(it) }
                .start()
            val response = completion.get(maxOf(agentConfig.taskTimeoutSeconds + 60, 60 * 60), TimeUnit.SECONDS)
            return LlmOutcome(response.aiMessage()?.text(), executions.toList())
        } catch (e: java.util.concurrent.ExecutionException) {
            val cause = e.cause ?: e
            error = cause.message
            throw (cause as? Exception) ?: e
        } catch (e: Exception) {
            error = e.message
            throw e
        } finally {
            publishStreamChunk(topic, streamId, seq.getAndIncrement(), "", true, error)
        }
    }

    private fun publishStreamChunk(topic: String, streamId: String, seq: Long, token: String, done: Boolean, error: String? = null) {
        val sessionHandler = Monster.getSessionHandler() ?: return
        val chunk = JsonObject()
            .put("taskId", streamId)
            .put("agent", agentName)
            .put("seq", seq)
            .put("token", token)
            .put("done", done)
        if (error != null) chunk.put("error", error)
        sessionHandler.publishMessage(BrokerMessage(clientId, topic, chunk.encode()))
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

    private fun publishDebug(type: String, payload: String) {
        val sessionHandler = Monster.getSessionHandler() ?: return
        // 1. Direct agent debug topic e.g. agents/Agent0/debug/input
        sessionHandler.publishMessage(BrokerMessage(clientId, "agents/$agentName/debug/$type", payload))
        // 2. A2A hierarchical debug topic e.g. a2a/v1/default/default/agents/Agent0/debug/input
        sessionHandler.publishMessage(BrokerMessage(clientId, a2aAgentTopic("debug/$type"), payload))
    }

    private fun publishDebug(type: String, payload: JsonObject) {
        publishDebug(type, payload.encode())
    }

    private fun chatRequestMessagesToJson(request: ChatRequest, onlyLast: Boolean = false): JsonArray {
        val messages = request.messages()
        val list = if (onlyLast && messages.isNotEmpty()) listOf(messages.last()) else messages
        return JsonArray(list.map { message ->
            JsonObject()
                .put("type", message.type()?.name ?: "UNKNOWN")
                .put("content", message.toString())
        })
    }

    private fun createLlmListener(): ChatModelListener {
        return object : ChatModelListener {
            override fun onRequest(requestContext: ChatModelRequestContext) {
                val request = requestContext.chatRequest()
                val messages = request.messages()
                val lastMessage = messages.lastOrNull()

                // Only log all messages for the initial request (typically system + user message)
                val onlyLast = messages.size > 2

                logger.info(
                    "Agent ${deviceConfig.name} LLM request: model=${request.parameters()?.modelName() ?: "default"}, " +
                        "messages=${messages.size}, tools=${request.parameters()?.toolSpecifications()?.size ?: 0}"
                )
                val log = JsonObject()
                    .put("type", "llm-request")
                    .put("timestamp", Instant.now().toString())
                    .put("model", request.parameters()?.modelName())
                    .put("messageCount", messages.size)
                    .put("lastMessage", lastMessage?.toString())
                    .put("messages", chatRequestMessagesToJson(request, onlyLast))
                    .put("toolCount", request.parameters()?.toolSpecifications()?.size ?: 0)
                publishToAgentTopic("logs/llm", log)

                // Publish to debug topic: agents/<agentName>/debug/input
                val debugInput = JsonObject()
                    .put("timestamp", Instant.now().toString())
                    .put("model", request.parameters()?.modelName())
                    .put("messages", chatRequestMessagesToJson(request, false))
                if (request.parameters()?.toolSpecifications()?.isNotEmpty() == true) {
                    debugInput.put("tools", JsonArray(request.parameters()!!.toolSpecifications().map { it.name() }))
                }
                publishDebug("input", debugInput)

                // Write to conversation log file
                writeToConversationLog { sb ->
                    sb.append("  REQUEST:\n")
                    sb.append("    timestamp: \"${Instant.now()}\"\n")
                    sb.append("    model: \"${request.parameters()?.modelName() ?: "unknown"}\"\n")
                    sb.append("    messages:\n")
                    val listToLog = if (onlyLast && lastMessage != null) listOf(lastMessage) else messages
                    listToLog.forEach { msg ->
                        val type = msg.type()?.name ?: "UNKNOWN"
                        val text = msg.toString()
                        sb.append("      - role: \"$type\"\n")
                        sb.append("        content: |\n")
                        sb.append(indent(text, 10))
                        sb.append("\n")
                    }
                    sb.append("\n")
                }
            }

            override fun onResponse(responseContext: ChatModelResponseContext) {
                val response = responseContext.chatResponse()
                val aiMessage = response.aiMessage()
                val metadata = response.metadata()
                val tokenUsage = metadata?.tokenUsage()

                // Accumulate token counters
                tokenUsage?.let {
                    it.inputTokenCount()?.let { n -> totalInputTokens.addAndGet(n.toLong()) }
                    it.outputTokenCount()?.let { n -> totalOutputTokens.addAndGet(n.toLong()) }
                    it.totalTokenCount()?.let { n -> totalTokens.addAndGet(n.toLong()) }
                }

                logger.info(
                    "Agent ${deviceConfig.name} LLM response: model=${metadata?.modelName() ?: "default"}, " +
                        "finish=${metadata?.finishReason()?.name ?: "unknown"}, " +
                        "textLength=${aiMessage.text()?.length ?: 0}, toolCalls=${aiMessage.toolExecutionRequests()?.size ?: 0}"
                )
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
                    .put("text", aiMessage.text())
                publishToAgentTopic("logs/llm", log)

                // Publish to debug topic: agents/<agentName>/debug/output
                val debugOutput = JsonObject()
                    .put("timestamp", Instant.now().toString())
                    .put("model", metadata?.modelName())
                    .put("text", aiMessage.text())
                if (aiMessage.hasToolExecutionRequests()) {
                    debugOutput.put("toolCalls", JsonArray(aiMessage.toolExecutionRequests().map { tc ->
                        JsonObject().put("name", tc.name()).put("arguments", tc.arguments())
                    }))
                }
                if (tokenUsage != null) {
                    debugOutput.put("tokens", JsonObject()
                        .put("input", tokenUsage.inputTokenCount())
                        .put("output", tokenUsage.outputTokenCount())
                        .put("total", tokenUsage.totalTokenCount()))
                }
                publishDebug("output", debugOutput)

                // Write full response to log file
                writeToConversationLog { sb ->
                    sb.append("  RESPONSE:\n")
                    sb.append("    timestamp: \"${Instant.now()}\"\n")
                    sb.append("    model: \"${metadata?.modelName() ?: "unknown"}\"\n")
                    if (tokenUsage != null) {
                        sb.append("    tokens:\n")
                        sb.append("      input: ${tokenUsage.inputTokenCount() ?: 0}\n")
                        sb.append("      output: ${tokenUsage.outputTokenCount() ?: 0}\n")
                        sb.append("      total: ${tokenUsage.totalTokenCount() ?: 0}\n")
                    }
                    sb.append("    finishReason: \"${metadata?.finishReason()?.name ?: "unknown"}\"\n")
                    if (aiMessage.hasToolExecutionRequests()) {
                        sb.append("    toolCalls:\n")
                        aiMessage.toolExecutionRequests().forEach { tc ->
                            sb.append("      - name: \"${tc.name()}\"\n")
                            sb.append("        arguments:\n")
                            sb.append(formatJsonAsYaml(tc.arguments() ?: "", 10))
                            sb.append("\n")
                        }
                    }
                    if (aiMessage.text() != null) {
                        sb.append("    text: |\n")
                        sb.append(indent(aiMessage.text(), 6))
                        sb.append("\n")
                    }
                    sb.append("\n")
                }
            }

            override fun onError(errorContext: ChatModelErrorContext) {
                val log = JsonObject()
                    .put("type", "llm-error")
                    .put("timestamp", Instant.now().toString())
                    .put("error", errorContext.error().message)
                publishToAgentTopic("logs/llm", log)

                // Publish to debug topic: agents/<agentName>/debug/error
                val debugError = JsonObject()
                    .put("timestamp", Instant.now().toString())
                    .put("error", errorContext.error().message)
                publishDebug("error", debugError)

                // Write error to log file
                writeToConversationLog { sb ->
                    sb.append("  ERROR:\n")
                    sb.append("    timestamp: \"${Instant.now()}\"\n")
                    sb.append("    message: |\n")
                    val errMsg = errorContext.error().message ?: "Unknown error"
                    sb.append(indent(errMsg, 6))
                    sb.append("\n\n")
                }
            }
        }
    }

    private fun writeToConversationLog(block: (StringBuilder) -> Unit) {
        val log = conversationLog ?: return
        try {
            val sb = StringBuilder()
            block(sb)
            log.info(sb.toString())
        } catch (e: Exception) {
            logger.warning("Failed to write conversation log for agent ${deviceConfig.name}: ${e.message}")
        }
    }

    private fun indent(text: String, spaces: Int): String {
        val indentStr = " ".repeat(spaces)
        return text.lines().joinToString("\n") { line ->
            if (line.isEmpty()) "" else "$indentStr$line"
        }
    }

    private fun formatJsonAsYaml(jsonStr: String, spaces: Int): String {
        return try {
            val json = io.vertx.core.json.JsonObject(jsonStr)
            val indentStr = " ".repeat(spaces)
            json.map.entries.joinToString("\n") { (key, value) ->
                val valStr = when (value) {
                    is io.vertx.core.json.JsonObject -> value.encode()
                    is io.vertx.core.json.JsonArray -> value.encode()
                    else -> value?.toString() ?: "null"
                }
                if (valStr.contains("\n")) {
                    "$indentStr$key: |\n" + indent(valStr, spaces + 2)
                } else {
                    "$indentStr$key: $valStr"
                }
            }
        } catch (_: Exception) {
            indent(jsonStr, spaces)
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
            .put("tags", agentConfig.tags)
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
            .put("inputTokens", totalInputTokens.get())
            .put("outputTokens", totalOutputTokens.get())
            .put("totalTokens", totalTokens.get())

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

data class TriggerContext(
    val type: TriggerType,
    val topicName: String? = null,
    val value: String? = null
)
