package at.rocworks.agents

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.data.BulkClientMessage
import at.rocworks.stores.DeviceConfig
import dev.langchain4j.memory.chat.MessageWindowChatMemory
import dev.langchain4j.model.chat.ChatModel
import dev.langchain4j.service.AiServices
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
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

    // Metrics
    private val messagesProcessed = AtomicLong(0)
    private val llmCalls = AtomicLong(0)
    private val errors = AtomicLong(0)

    /**
     * LangChain4j AI Service interface.
     * AiServices generates a proxy that handles the ReAct loop automatically.
     */
    interface AgentAiService {
        fun chat(userMessage: String): String
    }

    override fun start(startPromise: Promise<Void>) {
        try {
            agentConfig = AgentConfig.fromJsonObject(deviceConfig.config)
            logger.fine("Starting agent ${deviceConfig.name} (provider: ${agentConfig.provider}, trigger: ${agentConfig.triggerType})")

            val globalConfig = vertx.orCreateContext.config()

            // Create LLM model
            chatModel = LangChain4jFactory.createChatModel(agentConfig, globalConfig)

            // Create tools
            val archiveHandler = Monster.getArchiveHandler()
            val sessionHandler = Monster.getSessionHandler()
            agentTools = AgentTools(archiveHandler, null, clientId)

            // Build AI Service with ReAct loop
            val memorySize = agentConfig.memoryWindowSize
            val builder = AiServices.builder(AgentAiService::class.java)
                .chatModel(chatModel)
                .chatMemory(MessageWindowChatMemory.withMaxMessages(memorySize))
                .tools(agentTools)

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
            try {
                llmCalls.incrementAndGet()
                service.chat(userMessage)
            } catch (e: Exception) {
                errors.incrementAndGet()
                logger.warning("Agent ${deviceConfig.name} LLM error: ${e.message}")
                throw e
            }
        }.onComplete { result ->
            if (result.succeeded()) {
                val response = result.result()
                publishResponse(response)
            } else {
                logger.warning("Agent ${deviceConfig.name} execution failed: ${result.cause()?.message}")
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
