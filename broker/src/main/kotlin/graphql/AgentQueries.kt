package at.rocworks.graphql

import at.rocworks.Utils
import at.rocworks.agents.AgentConfig
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL query resolvers for AI Agents
 */
class AgentQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(AgentQueries::class.java)

    fun agents(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any?>>>()

            try {
                val nodeIdFilter = env.getArgument<String?>("nodeId")
                val enabledFilter = env.getArgument<Boolean?>("enabled")

                deviceStore.getAllDevices().onComplete { result ->
                    if (result.succeeded()) {
                        val agents = result.result()
                            .filter { it.type == DeviceConfig.DEVICE_TYPE_AGENT }
                            .filter { nodeIdFilter == null || it.nodeId == nodeIdFilter }
                            .filter { enabledFilter == null || it.enabled == enabledFilter }
                            .map { agentToMap(it) }
                        future.complete(agents)
                    } else {
                        logger.severe("Error fetching agents: ${result.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching agents: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    fun agent(): DataFetcher<CompletableFuture<Map<String, Any?>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>?>()

            try {
                val name = env.getArgument<String>("name")!!

                deviceStore.getDevice(name).onComplete { result ->
                    if (result.succeeded()) {
                        val device = result.result()
                        if (device != null && device.type == DeviceConfig.DEVICE_TYPE_AGENT) {
                            future.complete(agentToMap(device))
                        } else {
                            future.complete(null)
                        }
                    } else {
                        logger.severe("Error fetching agent $name: ${result.cause()?.message}")
                        future.complete(null)
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching agent: ${e.message}")
                future.complete(null)
            }

            future
        }
    }

    companion object {
        fun agentToMap(device: DeviceConfig): Map<String, Any?> {
            val agentConfig = AgentConfig.fromJsonObject(device.config)
            return mapOf(
                "name" to device.name,
                "description" to agentConfig.description,
                "version" to agentConfig.version,
                "namespace" to device.namespace,
                "nodeId" to device.nodeId,
                "enabled" to device.enabled,
                "skills" to agentConfig.skills.map { skill ->
                    mapOf(
                        "name" to skill.name,
                        "description" to skill.description,
                        "inputSchema" to skill.inputSchema?.map
                    )
                },
                "inputTopics" to agentConfig.inputTopics,
                "outputTopics" to agentConfig.outputTopics,
                "triggerType" to agentConfig.triggerType.name,
                "cronExpression" to agentConfig.cronExpression,
                "cronIntervalMs" to agentConfig.cronIntervalMs,
                "provider" to agentConfig.provider,
                "model" to agentConfig.model,
                "systemPrompt" to agentConfig.systemPrompt,
                "maxTokens" to agentConfig.maxTokens,
                "temperature" to agentConfig.temperature,
                "maxToolIterations" to agentConfig.maxToolIterations,
                "memoryWindowSize" to agentConfig.memoryWindowSize,
                "stateEnabled" to agentConfig.stateEnabled,
                "mcpServers" to agentConfig.mcpServers,
                "useMonsterMqMcp" to agentConfig.useMonsterMqMcp,
                "defaultArchiveGroup" to agentConfig.defaultArchiveGroup,
                "contextLastvalTopics" to agentConfig.contextLastvalTopics.mapValues { it.value },
                "contextRetainedTopics" to agentConfig.contextRetainedTopics,
                "contextHistoryQueries" to agentConfig.contextHistoryQueries.map { q ->
                    mapOf(
                        "archiveGroup" to q.archiveGroup,
                        "topics" to q.topics,
                        "lastSeconds" to q.lastSeconds,
                        "interval" to q.interval,
                        "function" to q.function,
                        "fields" to q.fields
                    )
                },
                "createdAt" to device.createdAt.toString(),
                "updatedAt" to device.updatedAt.toString()
            )
        }
    }
}
