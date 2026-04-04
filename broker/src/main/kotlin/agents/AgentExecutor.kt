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
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
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
    private var chatMemory: MessageWindowChatMemory? = null
    private var globalConfig: JsonObject? = null
    private var mcpToolProvider: dev.langchain4j.mcp.McpToolProvider? = null
    private var conversationLog: Logger? = null
    private var conversationLogHandler: java.util.logging.FileHandler? = null

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

    // The task ID currently being processed by this agent (set on event loop, read from worker thread)
    @Volatile
    private var currentTaskId: String? = null

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
        fun chat(userMessage: String): Result<String>
    }

    override fun start(startPromise: Promise<Void>) {
        try {
            agentConfig = AgentConfig.fromJsonObject(deviceConfig.config)
            logger.fine("Starting agent ${deviceConfig.name} (provider: ${agentConfig.providerName ?: agentConfig.provider}, trigger: ${agentConfig.triggerType})")

            globalConfig = vertx.orCreateContext.config()

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
                            chatModel = if (result.succeeded() && result.result() != null) {
                                val providerConfig = GenAiProviderConfig.fromJsonObject(result.result()!!.config)
                                LangChain4jFactory.createChatModel(providerConfig, agentConfig, globalConfig!!, listOf(llmListener))
                            } else {
                                // Try config.yaml providers before falling back to direct config
                                val configProvider = resolveConfigYamlProvider(agentConfig.providerName!!, globalConfig!!)
                                if (configProvider != null) {
                                    logger.fine("Using config.yaml provider '${agentConfig.providerName}'")
                                    LangChain4jFactory.createChatModel(configProvider, agentConfig, globalConfig!!, listOf(llmListener))
                                } else {
                                    logger.warning("Provider '${agentConfig.providerName}' not found, falling back to direct config")
                                    LangChain4jFactory.createChatModel(agentConfig, globalConfig!!, listOf(llmListener))
                                }
                            }
                            doStart(startPromise)
                        }
                } else {
                    // No device store — try config.yaml providers
                    val configProvider = resolveConfigYamlProvider(agentConfig.providerName!!, globalConfig!!)
                    if (configProvider != null) {
                        logger.fine("Using config.yaml provider '${agentConfig.providerName}'")
                        chatModel = LangChain4jFactory.createChatModel(configProvider, agentConfig, globalConfig!!, listOf(llmListener))
                    } else {
                        logger.warning("Device store not available, falling back to direct config for provider '${agentConfig.providerName}'")
                        chatModel = LangChain4jFactory.createChatModel(agentConfig, globalConfig!!, listOf(llmListener))
                    }
                    doStart(startPromise)
                }
            } else {
                chatModel = LangChain4jFactory.createChatModel(agentConfig, globalConfig!!, listOf(llmListener))
                doStart(startPromise)
            }

        } catch (e: Exception) {
            logger.severe("Failed to start agent ${deviceConfig.name}: ${e.message}")
            e.printStackTrace()
            startPromise.fail(e)
        }
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
            "Ollama"      to "ollama"
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
                getCurrentTaskId = { currentTaskId },
                registerPendingTask = { taskId, targetAgent, input -> pendingTasks[taskId] = PendingTask(targetAgent, input, parentTaskId = currentTaskId) },
                subAgentsAllowAll = agentConfig.subAgentsAllowAll,
                subAgents = agentConfig.subAgents
            )

            // Build AI Service with ReAct loop
            buildAiService()

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

            // Close conversation log
            try { conversationLogHandler?.close() } catch (_: Exception) {}
            conversationLogHandler = null
            conversationLog = null

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
        val memorySize = agentConfig.memoryWindowSize
        chatMemory = MessageWindowChatMemory.withMaxMessages(memorySize)
        val builder = AiServices.builder(AgentAiService::class.java)
            .chatModel(chatModel)
            .chatMemory(chatMemory)
            .tools(agentTools)

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

        aiService = builder.build()
    }

    /**
     * Rebuild the AI service with a fresh chat memory. Used to recover from
     * invalid message ordering errors (e.g. Gemini rejecting orphaned tool results).
     */
    private fun rebuildAiService() {
        logger.warning("Agent ${deviceConfig.name} rebuilding AI service to recover from chat history error")
        buildAiService()
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
                    executeAgent(agentConfig.cronPrompt?.takeIf { it.isNotBlank() } ?: "It is ${toLocalTime(Instant.now())}. Execute your scheduled task.", "cron")
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
                executeAgent(agentConfig.cronPrompt?.takeIf { it.isNotBlank() } ?: "It is ${toLocalTime(Instant.now())}. Execute your scheduled task.", "cron")
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
        val contextLogLines = mutableListOf<String>()  // Summary for conversation log

        // Fetch from archive last-value stores
        if (agentConfig.contextLastvalTopics.isNotEmpty()) {
            val archiveGroups = Monster.getArchiveHandler()?.getDeployedArchiveGroups() ?: emptyMap()
            for ((groupName, topicFilters) in agentConfig.contextLastvalTopics) {
                val store = archiveGroups[groupName]?.lastValStore ?: continue
                for (filter in topicFilters) {
                    var count = 0
                    store.findMatchingMessages(filter) { msg ->
                        val value = msg.getPayloadAsString()
                        lines.add("[Archive:$groupName] ${msg.topicName} = $value (${toLocalTime(msg.time)})")
                        count++
                        lines.size < 500 // safety limit
                    }
                    if (count > 0) contextLogLines.add("  LastValue archive=$groupName filter=$filter -> $count values")
                }
            }
        }

        // Fetch retained messages
        if (agentConfig.contextRetainedTopics.isNotEmpty()) {
            val retainedStore = Monster.getRetainedStore()
            if (retainedStore != null) {
                for (filter in agentConfig.contextRetainedTopics) {
                    var count = 0
                    retainedStore.findMatchingMessages(filter) { msg ->
                        val value = msg.getPayloadAsString()
                        lines.add("[Retained] ${msg.topicName} = $value")
                        count++
                        lines.size < 500
                    }
                    if (count > 0) contextLogLines.add("  Retained filter=$filter -> $count values")
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
                                lines.add("[History:${query.archiveGroup}:RAW] $topic (last ${query.lastSeconds}s, ${history.size()} records):")
                                contextLogLines.add("  History archive=${query.archiveGroup} mode=RAW topic=$topic range=${startTime}..${endTime} -> ${history.size()} rows")
                                lines.add(jsonArrayToCsv(history, query.decimals))
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
                            lines.add("[History:${query.archiveGroup}:${query.interval}:${query.function}] ${query.topics.joinToString(", ")} (last ${query.lastSeconds}s, $rowCount rows):")
                            contextLogLines.add("  History archive=${query.archiveGroup} mode=${query.interval}:${query.function} topics=${query.topics.joinToString(",")} range=${startTime}..${endTime} -> $rowCount rows")
                            lines.add(columnarJsonToCsv(result, query.decimals))
                        }
                    } catch (e: Exception) {
                        logger.warning("Failed to fetch aggregated history for ${query.topics} in ${query.archiveGroup}: ${e.message}")
                    }
                }
                if (lines.size >= 500) break
            }
        }

        // Write context fetch summary to conversation log
        writeToConversationLog { sb ->
            sb.append("===== CONTEXT [${Instant.now()}] =====\n")
            if (contextLogLines.isEmpty()) {
                sb.append("  (no context data configured or returned)\n")
            } else {
                contextLogLines.forEach { sb.append("$it\n") }
            }
            sb.append("=====\n\n")
        }

        if (lines.isEmpty()) return ""

        val now = Instant.now().atZone(localZone)
        val header = "--- Context Data (current time: ${now.format(localTimeFormatter)}, timezone: $localZone) ---"
        return "$header\n" +
            lines.joinToString("\n") +
            "\n--- End Context Data ---"
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
        val str = value.toString()
        // Convert UTC timestamps to local time
        if (str.length > 18 && str[10] == 'T' && str.endsWith("Z")) {
            return toLocalTime(str)
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
            if (pendingTasks.containsKey(taskId)) {
                // Reply to a sub-agent task we submitted
                handleSubAgentReply(msg)
            } else {
                // New incoming task (from another agent or forwarded input)
                chatMemory?.clear()
                handleTaskMessage(msg)
            }
            return
        }

        // 2. Backward compat: base inbox topic (external agents may still use it)
        if (msg.topicName == inboxPrefix) {
            chatMemory?.clear()
            handleTaskMessage(msg)
            return
        }

        // 3. Unexpected topic — input topics are handled by the forwarding client
        logger.warning("Agent $agentName received unexpected message on topic: ${msg.topicName}")
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
        collectedResults.add(CollectedResult(pending.targetAgent, taskId, pending.parentTaskId, pending.input, status, result))

        logger.info("Agent $agentName collected reply for task $taskId from ${pending.targetAgent} (status=$status, remaining=${pendingTasks.size})")

        // When all pending tasks are resolved, resume with compiled results
        if (pendingTasks.isEmpty()) {
            resumeWithCollectedResults()
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
        executeAgent(userMessage, "task-results")
    }

    private fun setupTaskTimeoutChecker() {
        val checkIntervalMs = 15_000L // check every 15 seconds
        val timeoutMs = agentConfig.taskTimeoutMs

        taskTimeoutTimerId = vertx.setPeriodic(checkIntervalMs) {
            val now = System.currentTimeMillis()
            val timedOut = pendingTasks.entries.filter { now - it.value.submittedAt > timeoutMs }
            timedOut.forEach { (taskId, pending) ->
                pendingTasks.remove(taskId)
                logger.warning("Agent $agentName task $taskId to '${pending.targetAgent}' timed out after ${timeoutMs / 1000}s")
                collectedResults.add(CollectedResult(pending.targetAgent, taskId, pending.parentTaskId, pending.input, "timeout",
                    "Agent '${pending.targetAgent}' did not respond within ${timeoutMs / 1000} seconds"))
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
                executeAgentWithCallback(taskMessage, "task:$taskId") { response, _ ->
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

            // Track the current task ID so sub-agent calls can reference it as parentTaskId
            currentTaskId = taskId

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

            // Execute the agent with a callback to publish the result
            executeAgentWithCallback(taskMessage, "task:$taskId") { response, error ->
                currentTaskId = null
                val sessionHandler = Monster.getSessionHandler() ?: return@executeAgentWithCallback
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
            try {
                service.chat(fullMessage)
            } catch (e: Exception) {
                val fullErrorMessage = generateSequence(e as Throwable?) { it.cause }.joinToString(" | ") { it.message ?: "" }
                if (fullErrorMessage.contains("function call turn") ||
                    fullErrorMessage.contains("function response turn") ||
                    fullErrorMessage.contains("INVALID_ARGUMENT")) {
                    logger.warning("Agent ${deviceConfig.name} chat history invalid, rebuilding service and retrying: ${e.message?.take(200)}")
                    rebuildAiService()
                    aiService?.chat(fullMessage) ?: throw e
                } else {
                    throw e
                }
            }
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

            try {
                service.chat(fullMessage)
            } catch (e: Exception) {
                // Gemini rejects invalid message ordering in chat history (e.g. orphaned tool results
                // after memory window eviction). Clear memory and retry with just this message.
                if (e.message?.contains("function call turn") == true ||
                    e.message?.contains("function response turn") == true) {
                    logger.warning("Agent ${deviceConfig.name} chat history invalid, clearing memory and retrying")
                    chatMemory?.clear()
                    service.chat(fullMessage)
                } else {
                    throw e
                }
            }
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

                // Write full conversation to log file
                writeToConversationLog { sb ->
                    sb.append("===== REQUEST [${Instant.now()}] model=${request.parameters()?.modelName()} =====\n")
                    messages.forEach { msg ->
                        val type = msg.type()?.name ?: "UNKNOWN"
                        val text = msg.toString()
                        sb.append("[$type] $text\n")
                    }
                    sb.append("=====\n\n")
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

                // Write full response to log file
                writeToConversationLog { sb ->
                    val tokens = if (tokenUsage != null) "in=${tokenUsage.inputTokenCount()} out=${tokenUsage.outputTokenCount()} total=${tokenUsage.totalTokenCount()}" else ""
                    sb.append("===== RESPONSE [${Instant.now()}] model=${metadata?.modelName()} tokens=$tokens finish=${metadata?.finishReason()?.name} =====\n")
                    if (aiMessage.hasToolExecutionRequests()) {
                        aiMessage.toolExecutionRequests().forEach { tc ->
                            sb.append("[TOOL_CALL] ${tc.name()}(${tc.arguments()})\n")
                        }
                    }
                    if (aiMessage.text() != null) {
                        sb.append(aiMessage.text())
                        sb.append("\n")
                    }
                    sb.append("=====\n\n")
                }
            }

            override fun onError(errorContext: ChatModelErrorContext) {
                val log = JsonObject()
                    .put("type", "llm-error")
                    .put("timestamp", Instant.now().toString())
                    .put("error", errorContext.error().message)
                publishToAgentTopic("logs/llm", log)

                // Write error to log file
                writeToConversationLog { sb ->
                    sb.append("===== ERROR [${Instant.now()}] =====\n")
                    sb.append("${errorContext.error().message}\n")
                    sb.append("=====\n\n")
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
