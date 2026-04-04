package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.agents.AgentConfig
import at.rocworks.agents.AgentExtension
import at.rocworks.agents.AgentSkill
import at.rocworks.agents.ContextHistoryQuery
import at.rocworks.agents.TriggerType
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL mutation resolvers for AI Agents
 */
class AgentMutations(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(AgentMutations::class.java)

    fun createAgent(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            try {
                val input = env.getArgument<Map<String, Any>>("input")!!
                val name = input["name"] as? String ?: ""
                if (!name.matches(Regex("^[a-zA-Z0-9_-]+$"))) {
                    future.completeExceptionally(RuntimeException("Invalid agent name '${name}': only letters, digits, underscores, and hyphens are allowed (no spaces)"))
                    return@DataFetcher future
                }
                val deviceConfig = inputToDeviceConfig(input)

                deviceStore.saveDevice(deviceConfig).onComplete { result ->
                    if (result.succeeded()) {
                        val saved = result.result()
                        future.complete(AgentQueries.agentToMap(saved))
                        notifyConfigChange("add", saved)
                    } else {
                        future.completeExceptionally(result.cause())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error creating agent: ${e.message}")
                future.completeExceptionally(e)
            }

            future
        }
    }

    fun updateAgent(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()

            try {
                val name = env.getArgument<String>("name")!!
                val input = env.getArgument<Map<String, Any>>("input")!!

                deviceStore.getDevice(name).onComplete { getResult ->
                    if (getResult.succeeded() && getResult.result() != null) {
                        val existing = getResult.result()!!
                        val updated = mergeAgentConfig(existing, input)

                        deviceStore.saveDevice(updated).onComplete { saveResult ->
                            if (saveResult.succeeded()) {
                                val saved = saveResult.result()
                                future.complete(AgentQueries.agentToMap(saved))
                                notifyConfigChange("update", saved)
                            } else {
                                future.completeExceptionally(saveResult.cause())
                            }
                        }
                    } else {
                        future.completeExceptionally(RuntimeException("Agent not found: $name"))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error updating agent: ${e.message}")
                future.completeExceptionally(e)
            }

            future
        }
    }

    fun deleteAgent(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Boolean>()

            try {
                val name = env.getArgument<String>("name")!!

                deviceStore.deleteDevice(name).onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                        notifyConfigChange("delete", name)
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

    fun startAgent(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()

            try {
                val name = env.getArgument<String>("name")!!

                deviceStore.toggleDevice(name, true).onComplete { result ->
                    if (result.succeeded() && result.result() != null) {
                        val device = result.result()!!
                        future.complete(AgentQueries.agentToMap(device))
                        notifyConfigChange("toggle", device)
                    } else {
                        future.completeExceptionally(result.cause() ?: RuntimeException("Agent not found"))
                    }
                }
            } catch (e: Exception) {
                future.completeExceptionally(e)
            }

            future
        }
    }

    fun stopAgent(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()

            try {
                val name = env.getArgument<String>("name")!!

                deviceStore.toggleDevice(name, false).onComplete { result ->
                    if (result.succeeded() && result.result() != null) {
                        val device = result.result()!!
                        future.complete(AgentQueries.agentToMap(device))
                        notifyConfigChange("toggle", device)
                    } else {
                        future.completeExceptionally(result.cause() ?: RuntimeException("Agent not found"))
                    }
                }
            } catch (e: Exception) {
                future.completeExceptionally(e)
            }

            future
        }
    }

    private fun inputToDeviceConfig(input: Map<String, Any>): DeviceConfig {
        val name = input["name"] as String
        val namespace = input["namespace"] as String
        val nodeId = input["nodeId"] as String
        val enabled = input["enabled"] as? Boolean ?: false

        val agentConfig = AgentConfig(
            org = input["org"] as? String ?: "default",
            site = input["site"] as? String ?: "default",
            description = input["description"] as? String ?: "",
            version = input["version"] as? String ?: "1.0.0",
            skills = parseSkills(input["skills"]),
            inputTopics = parseStringList(input["inputTopics"]),
            outputTopics = parseStringList(input["outputTopics"]),
            triggerType = (input["triggerType"] as? String)?.let { TriggerType.fromString(it) } ?: TriggerType.MQTT,
            cronExpression = input["cronExpression"] as? String,
            cronIntervalMs = (input["cronIntervalMs"] as? Number)?.toLong(),
            cronPrompt = input["cronPrompt"] as? String,
            provider = input["provider"] as? String ?: "gemini",
            providerName = input["providerName"] as? String,
            model = input["model"] as? String,
            apiKey = input["apiKey"] as? String,
            endpoint = input["endpoint"] as? String,
            serviceVersion = input["serviceVersion"] as? String,
            systemPrompt = input["systemPrompt"] as? String ?: "",
            maxTokens = (input["maxTokens"] as? Number)?.toInt(),
            temperature = (input["temperature"] as? Number)?.toDouble() ?: 0.7,
            maxToolIterations = (input["maxToolIterations"] as? Number)?.toInt() ?: 10,
            memoryWindowSize = (input["memoryWindowSize"] as? Number)?.toInt() ?: 40,
            stateEnabled = input["stateEnabled"] as? Boolean ?: true,
            enableThinking = input["enableThinking"] as? Boolean ?: false,
            conversationLogEnabled = input["conversationLogEnabled"] as? Boolean ?: false,
            mcpServers = parseStringList(input["mcpServers"]),
            useMonsterMqMcp = input["useMonsterMqMcp"] as? Boolean ?: false,
            defaultArchiveGroup = input["defaultArchiveGroup"] as? String ?: "Default",
            contextLastvalTopics = parseContextLastvalTopics(input["contextLastvalTopics"]),
            contextRetainedTopics = parseStringList(input["contextRetainedTopics"]),
            contextHistoryQueries = parseContextHistoryQueries(input["contextHistoryQueries"]),
            timezone = input["timezone"] as? String,
            taskTimeoutMs = (input["taskTimeoutMs"] as? Number)?.toLong() ?: 60000,
            subAgentsAllowAll = input["subAgentsAllowAll"] as? Boolean ?: false,
            subAgents = parseStringList(input["subAgents"])
        )

        return DeviceConfig(
            name = name,
            namespace = namespace,
            nodeId = nodeId,
            config = agentConfig.toJsonObject(),
            enabled = enabled,
            type = DeviceConfig.DEVICE_TYPE_AGENT,
            createdAt = Instant.now(),
            updatedAt = Instant.now()
        )
    }

    private fun mergeAgentConfig(existing: DeviceConfig, input: Map<String, Any>): DeviceConfig {
        val existingConfig = AgentConfig.fromJsonObject(existing.config)

        val merged = AgentConfig(
            org = input["org"] as? String ?: existingConfig.org,
            site = input["site"] as? String ?: existingConfig.site,
            description = input["description"] as? String ?: existingConfig.description,
            version = input["version"] as? String ?: existingConfig.version,
            skills = if (input.containsKey("skills")) parseSkills(input["skills"]) else existingConfig.skills,
            inputTopics = if (input.containsKey("inputTopics")) parseStringList(input["inputTopics"]) else existingConfig.inputTopics,
            outputTopics = if (input.containsKey("outputTopics")) parseStringList(input["outputTopics"]) else existingConfig.outputTopics,
            triggerType = (input["triggerType"] as? String)?.let { TriggerType.fromString(it) } ?: existingConfig.triggerType,
            cronExpression = if (input.containsKey("cronExpression")) input["cronExpression"] as? String else existingConfig.cronExpression,
            cronIntervalMs = if (input.containsKey("cronIntervalMs")) (input["cronIntervalMs"] as? Number)?.toLong() else existingConfig.cronIntervalMs,
            cronPrompt = if (input.containsKey("cronPrompt")) input["cronPrompt"] as? String else existingConfig.cronPrompt,
            provider = input["provider"] as? String ?: existingConfig.provider,
            providerName = if (input.containsKey("providerName")) input["providerName"] as? String else existingConfig.providerName,
            model = if (input.containsKey("model")) input["model"] as? String else existingConfig.model,
            apiKey = if (input.containsKey("apiKey")) input["apiKey"] as? String else existingConfig.apiKey,
            endpoint = if (input.containsKey("endpoint")) input["endpoint"] as? String else existingConfig.endpoint,
            serviceVersion = if (input.containsKey("serviceVersion")) input["serviceVersion"] as? String else existingConfig.serviceVersion,
            systemPrompt = input["systemPrompt"] as? String ?: existingConfig.systemPrompt,
            maxTokens = if (input.containsKey("maxTokens")) (input["maxTokens"] as? Number)?.toInt() else existingConfig.maxTokens,
            temperature = (input["temperature"] as? Number)?.toDouble() ?: existingConfig.temperature,
            maxToolIterations = (input["maxToolIterations"] as? Number)?.toInt() ?: existingConfig.maxToolIterations,
            memoryWindowSize = (input["memoryWindowSize"] as? Number)?.toInt() ?: existingConfig.memoryWindowSize,
            stateEnabled = input["stateEnabled"] as? Boolean ?: existingConfig.stateEnabled,
            enableThinking = input["enableThinking"] as? Boolean ?: existingConfig.enableThinking,
            conversationLogEnabled = input["conversationLogEnabled"] as? Boolean ?: existingConfig.conversationLogEnabled,
            mcpServers = if (input.containsKey("mcpServers")) parseStringList(input["mcpServers"]) else existingConfig.mcpServers,
            useMonsterMqMcp = input["useMonsterMqMcp"] as? Boolean ?: existingConfig.useMonsterMqMcp,
            defaultArchiveGroup = input["defaultArchiveGroup"] as? String ?: existingConfig.defaultArchiveGroup,
            contextLastvalTopics = if (input.containsKey("contextLastvalTopics")) parseContextLastvalTopics(input["contextLastvalTopics"]) else existingConfig.contextLastvalTopics,
            contextRetainedTopics = if (input.containsKey("contextRetainedTopics")) parseStringList(input["contextRetainedTopics"]) else existingConfig.contextRetainedTopics,
            contextHistoryQueries = if (input.containsKey("contextHistoryQueries")) parseContextHistoryQueries(input["contextHistoryQueries"]) else existingConfig.contextHistoryQueries,
            timezone = if (input.containsKey("timezone")) input["timezone"] as? String else existingConfig.timezone,
            taskTimeoutMs = (input["taskTimeoutMs"] as? Number)?.toLong() ?: existingConfig.taskTimeoutMs,
            subAgentsAllowAll = input["subAgentsAllowAll"] as? Boolean ?: existingConfig.subAgentsAllowAll,
            subAgents = if (input.containsKey("subAgents")) parseStringList(input["subAgents"]) else existingConfig.subAgents
        )

        return existing.copy(
            namespace = input["namespace"] as? String ?: existing.namespace,
            nodeId = input["nodeId"] as? String ?: existing.nodeId,
            config = merged.toJsonObject(),
            enabled = input["enabled"] as? Boolean ?: existing.enabled,
            updatedAt = Instant.now()
        )
    }

    @Suppress("UNCHECKED_CAST")
    private fun parseSkills(raw: Any?): List<AgentSkill> {
        if (raw == null) return emptyList()
        val list = raw as? List<Map<String, Any>> ?: return emptyList()
        return list.map { map ->
            AgentSkill(
                name = map["name"] as? String ?: "",
                description = map["description"] as? String ?: "",
                inputSchema = (map["inputSchema"] as? Map<String, Any>)?.let { JsonObject(it) }
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun parseStringList(raw: Any?): List<String> {
        if (raw == null) return emptyList()
        return (raw as? List<String>) ?: emptyList()
    }

    @Suppress("UNCHECKED_CAST")
    private fun parseContextLastvalTopics(raw: Any?): Map<String, List<String>> {
        if (raw == null) return emptyMap()
        val map = raw as? Map<String, Any> ?: return emptyMap()
        return map.mapValues { (_, v) ->
            when (v) {
                is List<*> -> v.filterIsInstance<String>()
                else -> emptyList()
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun parseContextHistoryQueries(raw: Any?): List<ContextHistoryQuery> {
        if (raw == null) return emptyList()
        val list = raw as? List<Map<String, Any>> ?: return emptyList()
        return list.map { map ->
            ContextHistoryQuery(
                archiveGroup = map["archiveGroup"] as? String ?: "Default",
                topics = (map["topics"] as? List<String>) ?: emptyList(),
                lastSeconds = (map["lastSeconds"] as? Number)?.toInt() ?: 3600,
                interval = map["interval"] as? String ?: "RAW",
                function = map["function"] as? String ?: "AVG",
                fields = (map["fields"] as? List<String>) ?: emptyList(),
                decimals = (map["decimals"] as? Number)?.toInt()
            )
        }
    }

    private fun notifyConfigChange(operation: String, device: DeviceConfig) {
        val changeData = JsonObject()
            .put("operation", operation)
            .put("deviceName", device.name)
            .put("deviceType", DeviceConfig.DEVICE_TYPE_AGENT)
            .put("device", device.toJsonObject())
        vertx.eventBus().request<JsonObject>(AgentExtension.ADDRESS_AGENT_CONFIG_CHANGED, changeData).onComplete {}
    }

    private fun notifyConfigChange(operation: String, deviceName: String) {
        val changeData = JsonObject()
            .put("operation", operation)
            .put("deviceName", deviceName)
            .put("deviceType", DeviceConfig.DEVICE_TYPE_AGENT)
        vertx.eventBus().request<JsonObject>(AgentExtension.ADDRESS_AGENT_CONFIG_CHANGED, changeData).onComplete {}
    }
}
