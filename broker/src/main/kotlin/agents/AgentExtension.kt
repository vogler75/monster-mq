package at.rocworks.agents

import at.rocworks.Const
import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigStoreFactory
import at.rocworks.stores.IDeviceConfigStore
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

/**
 * Agent Extension - Cluster-aware coordinator for AI agents.
 *
 * Follows the same Extension + Connector pattern used by FlowEngineExtension
 * and all device bridges. Manages agent lifecycle: deploy, undeploy, toggle.
 */
class AgentExtension : AbstractVerticle() {

    private val logger: Logger = Utils.getLogger(this::class.java)

    private lateinit var deviceStore: IDeviceConfigStore
    private val agentVerticles = ConcurrentHashMap<String, String>() // agentName -> deploymentId
    private lateinit var currentNodeId: String

    companion object {
        const val ADDRESS_AGENT_CONFIG_CHANGED = "agent.config.changed"
    }

    override fun start(startPromise: Promise<Void>) {
        logger.fine("Starting AgentExtension...")

        try {
            currentNodeId = Monster.getClusterNodeId(vertx)
            logger.fine("AgentExtension running on node: $currentNodeId")

            initializeDeviceStore()
                .compose { loadAndDeployAgents() }
                .compose { setupEventBusHandlers() }
                .onComplete { result ->
                    if (result.succeeded()) {
                        logger.fine("AgentExtension started successfully")
                        startPromise.complete()
                    } else {
                        logger.severe("Failed to start AgentExtension: ${result.cause()?.message}")
                        startPromise.fail(result.cause())
                    }
                }

        } catch (e: Exception) {
            logger.severe("Exception during AgentExtension startup: ${e.message}")
            e.printStackTrace()
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.fine("Stopping AgentExtension...")

        try {
            val agentNames = agentVerticles.keys.toList()

            if (agentNames.isEmpty()) {
                deviceStore.close().onComplete {
                    logger.info("AgentExtension stopped successfully")
                    stopPromise.complete()
                }
                return
            }

            var completed = 0
            agentNames.forEach { agentName ->
                undeployAgent(agentName).onComplete {
                    completed++
                    if (completed == agentNames.size) {
                        deviceStore.close().onComplete {
                            logger.info("AgentExtension stopped successfully")
                            stopPromise.complete()
                        }
                    }
                }
            }

        } catch (e: Exception) {
            logger.severe("Exception during AgentExtension stop: ${e.message}")
            stopPromise.fail(e)
        }
    }

    private fun initializeDeviceStore(): Future<Void> {
        val promise = Promise.promise<Void>()

        try {
            val config = vertx.orCreateContext.config()
            val configStoreType = Monster.getConfigStoreType(config)

            val store = if (configStoreType != "NONE") {
                DeviceConfigStoreFactory.create(configStoreType, config, vertx)
            } else {
                null
            }

            if (store != null) {
                deviceStore = store
                deviceStore.initialize().onComplete { result ->
                    if (result.succeeded()) {
                        logger.fine("Agent device store initialized successfully")
                        promise.complete()
                    } else {
                        logger.severe("Failed to initialize DeviceConfigStore: ${result.cause()?.message}")
                        promise.fail(RuntimeException("Failed to initialize database"))
                    }
                }
            } else {
                val message = "No DeviceConfigStore available (ConfigStoreType: $configStoreType), AgentExtension disabled"
                logger.warning(message)
                promise.fail(RuntimeException(message))
            }

        } catch (e: Exception) {
            logger.severe("Failed to create DeviceConfigStore: ${e.message}")
            promise.fail(RuntimeException("Failed to initialize database"))
        }

        return promise.future()
    }

    private fun isAgentForThisNode(device: DeviceConfig): Boolean {
        return device.nodeId == "*" || device.nodeId == currentNodeId
    }

    private fun loadAndDeployAgents(): Future<Void> {
        val promise = Promise.promise<Void>()

        // Load all devices and filter: type=Agent, enabled, assigned to this node or wildcard "*"
        deviceStore.getAllDevices().onComplete { result ->
            if (result.succeeded()) {
                val agents = result.result().filter {
                    it.type == DeviceConfig.DEVICE_TYPE_AGENT && it.enabled && isAgentForThisNode(it)
                }
                logger.fine("Found ${agents.size} enabled agents for node $currentNodeId")

                if (agents.isEmpty()) {
                    promise.complete()
                    return@onComplete
                }

                var completedCount = 0
                var successCount = 0

                agents.forEach { agent ->
                    deployAgent(agent).onComplete { deployResult ->
                        completedCount++
                        if (deployResult.succeeded()) {
                            successCount++
                        } else {
                            logger.warning("Failed to deploy agent ${agent.name}: ${deployResult.cause()?.message}")
                        }
                        if (completedCount == agents.size) {
                            logger.info("Agent deployment completed: $successCount/$completedCount agents deployed")
                            promise.complete()
                        }
                    }
                }
            } else {
                logger.severe("Failed to load agents: ${result.cause()?.message}")
                promise.fail(result.cause())
            }
        }

        return promise.future()
    }

    private fun deployAgent(deviceConfig: DeviceConfig): Future<String> {
        val promise = Promise.promise<String>()

        try {
            val executor = AgentExecutor(deviceConfig)
            val options = io.vertx.core.DeploymentOptions()
                .setConfig(vertx.orCreateContext.config())
                .setMaxWorkerExecuteTime(2 * 60 * 1_000_000_000L) // 2 minutes — LLM calls with tool loops can be long
                .setMaxWorkerExecuteTimeUnit(java.util.concurrent.TimeUnit.NANOSECONDS)
            vertx.deployVerticle(executor, options).onComplete { deployResult ->
                if (deployResult.succeeded()) {
                    val deploymentId = deployResult.result()
                    agentVerticles[deviceConfig.name] = deploymentId
                    logger.info("Deployed agent ${deviceConfig.name} (deploymentId: $deploymentId)")
                    promise.complete(deviceConfig.name)
                } else {
                    logger.severe("Failed to deploy agent ${deviceConfig.name}: ${deployResult.cause()?.message}")
                    deployResult.cause()?.printStackTrace()
                    promise.fail(deployResult.cause())
                }
            }
        } catch (e: Exception) {
            logger.severe("Error deploying agent ${deviceConfig.name}: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    private fun undeployAgent(agentName: String): Future<Void> {
        val promise = Promise.promise<Void>()

        try {
            val deploymentId = agentVerticles.remove(agentName)
            if (deploymentId != null) {
                vertx.undeploy(deploymentId).onComplete { result ->
                    if (result.succeeded()) {
                        logger.info("Undeployed agent $agentName")
                    } else {
                        logger.warning("Error undeploying agent $agentName: ${result.cause()?.message}")
                    }
                    promise.complete()
                }
            } else {
                promise.complete()
            }
        } catch (e: Exception) {
            logger.severe("Error undeploying agent $agentName: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    private fun setupEventBusHandlers(): Future<Void> {
        val promise = Promise.promise<Void>()

        try {
            vertx.eventBus().consumer<JsonObject>(ADDRESS_AGENT_CONFIG_CHANGED) { message ->
                handleAgentConfigChange(message)
            }
            promise.complete()
        } catch (e: Exception) {
            promise.fail(e)
        }

        return promise.future()
    }

    private fun handleAgentConfigChange(message: Message<JsonObject>) {
        try {
            val changeData = message.body()
            val operation = changeData.getString("operation")
            val deviceName = changeData.getString("deviceName")

            logger.finer("Handling agent config change: $operation for $deviceName")

            when (operation) {
                "add", "update" -> {
                    val deviceJson = changeData.getJsonObject("device")
                    val device = DeviceConfig.fromJsonObject(deviceJson)

                    if (isAgentForThisNode(device) && device.enabled) {
                        undeployAgent(deviceName)
                            .compose { deployAgent(device) }
                            .onComplete { result ->
                                if (result.succeeded()) {
                                    message.reply(JsonObject().put("success", true))
                                } else {
                                    message.fail(500, result.cause()?.message ?: "Deployment failed")
                                }
                            }
                    } else {
                        undeployAgent(deviceName)
                            .onComplete { message.reply(JsonObject().put("success", true)) }
                    }
                }

                "toggle" -> {
                    val deviceJson = changeData.getJsonObject("device")
                    if (deviceJson != null) {
                        val device = DeviceConfig.fromJsonObject(deviceJson)
                        if (isAgentForThisNode(device) && device.enabled) {
                            undeployAgent(deviceName)
                                .compose { deployAgent(device) }
                                .onComplete { deployResult ->
                                    if (deployResult.succeeded()) {
                                        logger.info("Enabled and deployed agent: $deviceName")
                                        message.reply(JsonObject().put("success", true))
                                    } else {
                                        message.fail(500, deployResult.cause()?.message ?: "Deployment failed")
                                    }
                                }
                        } else {
                            undeployAgent(deviceName).onComplete {
                                logger.info("Disabled and undeployed agent: $deviceName")
                                message.reply(JsonObject().put("success", true))
                            }
                        }
                    } else {
                        message.fail(500, "No device data in toggle message")
                    }
                }

                "delete" -> {
                    undeployAgent(deviceName)
                        .onComplete { message.reply(JsonObject().put("success", true)) }
                }

                else -> {
                    message.fail(400, "Unknown operation: $operation")
                }
            }

        } catch (e: Exception) {
            logger.severe("Error handling agent config change: ${e.message}")
            e.printStackTrace()
            message.fail(500, e.message)
        }
    }
}
