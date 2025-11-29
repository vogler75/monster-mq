package at.rocworks.flowengine

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
 * Flow Engine Extension - Main coordination verticle for flow management
 *
 * Responsibilities:
 * - Cluster-aware flow management (only manages flows assigned to current node)
 * - Deploys FlowInstanceExecutor verticles for each flow instance
 * - Each flow verticle subscribes directly to its MQTT topics
 * - Handles configuration changes via EventBus
 */
class FlowEngineExtension : AbstractVerticle() {

    private val logger: Logger = Utils.getLogger(this::class.java)

    // Device configuration store
    private lateinit var deviceStore: IDeviceConfigStore

    // Track deployed flow verticles: instanceName -> deploymentId
    private val flowVerticles = ConcurrentHashMap<String, String>()

    // Track loaded flow classes
    private val flowClasses = ConcurrentHashMap<String, DeviceConfig>()

    // Current cluster node ID
    private lateinit var currentNodeId: String

    companion object {
        // EventBus addresses
        const val ADDRESS_FLOW_CONFIG_CHANGED = "flowengine.flow.config.changed"
    }

    override fun start(startPromise: Promise<Void>) {
        logger.fine("Starting FlowEngineExtension...")

        try {
            // Get current node ID
            currentNodeId = Monster.getClusterNodeId(vertx)
            logger.fine("FlowEngineExtension running on node: $currentNodeId")

            // Initialize device store
            initializeDeviceStore()
                .compose { loadFlowClasses() }
                .compose { loadAndDeployFlows() }
                .compose { setupEventBusHandlers() }
                .onComplete { result ->
                    if (result.succeeded()) {
                        logger.fine("FlowEngineExtension started successfully")
                        startPromise.complete()
                    } else {
                        logger.severe("Failed to start FlowEngineExtension: ${result.cause()?.message}")
                        startPromise.fail(result.cause())
                    }
                }

        } catch (e: Exception) {
            logger.severe("Exception during FlowEngineExtension startup: ${e.message}")
            e.printStackTrace()
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.fine("Stopping FlowEngineExtension...")

        try {
            // Undeploy all flow verticles
            val instanceNames = flowVerticles.keys.toList()

            if (instanceNames.isEmpty()) {
                flowClasses.clear()
                deviceStore.close()
                    .onComplete { result ->
                        if (result.succeeded()) {
                            logger.info("FlowEngineExtension stopped successfully")
                            stopPromise.complete()
                        } else {
                            logger.warning("Error during FlowEngineExtension shutdown: ${result.cause()?.message}")
                            stopPromise.complete() // Complete anyway
                        }
                    }
                return
            }

            var completed = 0
            instanceNames.forEach { instanceName ->
                undeployFlow(instanceName).onComplete {
                    completed++
                    if (completed == instanceNames.size) {
                        flowClasses.clear()
                        deviceStore.close()
                            .onComplete { result ->
                                if (result.succeeded()) {
                                    logger.info("FlowEngineExtension stopped successfully")
                                    stopPromise.complete()
                                } else {
                                    logger.warning("Error during FlowEngineExtension shutdown: ${result.cause()?.message}")
                                    stopPromise.complete() // Complete anyway
                                }
                            }
                    }
                }
            }

        } catch (e: Exception) {
            logger.severe("Exception during FlowEngineExtension stop: ${e.message}")
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
                deviceStore.initialize()
                    .onComplete { result ->
                        if (result.succeeded()) {
                            logger.fine("Flow Engine device store initialized successfully")
                            promise.complete()
                        } else {
                            logger.severe("Failed to initialize DeviceConfigStore: ${result.cause()?.message}")
                            promise.fail(RuntimeException("Failed to initialize database"))
                        }
                    }
            } else {
                val message = "No DeviceConfigStore implementation available for ConfigStoreType: $configStoreType"
                logger.severe(message)
                promise.fail(RuntimeException(message))
            }

        } catch (e: NotImplementedError) {
            logger.warning("DeviceConfigStore not implemented for this store type, Flow Engine will be disabled")
            promise.fail(RuntimeException("Store type not implemented"))
        } catch (e: Exception) {
            logger.severe("Failed to create DeviceConfigStore: ${e.message}")
            promise.fail(RuntimeException("Failed to initialize database"))
        }

        return promise.future()
    }

    private fun loadFlowClasses(): Future<Void> {
        val promise = Promise.promise<Void>()

        // Load all Flow-Class devices
        deviceStore.getAllDevices()
            .onComplete { result ->
                if (result.succeeded()) {
                    val classes = result.result().filter { it.type == DeviceConfig.DEVICE_TYPE_FLOW_CLASS }
                    classes.forEach { flowClass ->
                        flowClasses[flowClass.name] = flowClass
                    }
                    if (flowClasses.size>0) logger.info("Loaded ${flowClasses.size} flow classes")
                    promise.complete()
                } else {
                    logger.severe("Failed to load flow classes: ${result.cause()?.message}")
                    promise.fail(result.cause())
                }
            }

        return promise.future()
    }

    private fun loadAndDeployFlows(): Future<Void> {
        val promise = Promise.promise<Void>()

        // Load Flow-Object instances assigned to this node
        deviceStore.getEnabledDevicesByNode(currentNodeId)
            .onComplete { result ->
                if (result.succeeded()) {
                    // Filter to only include Flow-Object devices
                    val flows = result.result().filter { device ->
                        device.type == DeviceConfig.DEVICE_TYPE_FLOW_OBJECT
                    }
                    logger.fine("Found ${flows.size} enabled flow instances assigned to node $currentNodeId")

                    if (flows.isEmpty()) {
                        promise.complete()
                        return@onComplete
                    }

                    // Deploy each flow
                    var completedCount = 0
                    var successCount = 0

                    flows.forEach { flow ->
                        deployFlow(flow)
                            .onComplete { deployResult ->
                                completedCount++
                                if (deployResult.succeeded()) {
                                    successCount++
                                    logger.fine("Successfully deployed flow ${flow.name}")
                                } else {
                                    logger.warning("Failed to deploy flow ${flow.name}: ${deployResult.cause()?.message}")
                                }

                                // Complete when all flows have been processed
                                if (completedCount == flows.size) {
                                    logger.info("Flow deployment completed: $successCount/$completedCount flows deployed successfully")
                                    promise.complete()
                                }
                            }
                    }
                } else {
                    logger.severe("Failed to load flows: ${result.cause()?.message}")
                    promise.fail(result.cause())
                }
            }

        return promise.future()
    }

    private fun deployFlow(instanceConfig: DeviceConfig): Future<String> {
        val promise = Promise.promise<String>()

        try {
            if (!instanceConfig.isAssignedToNode(currentNodeId)) {
                promise.fail(Exception("Flow ${instanceConfig.name} is not assigned to node $currentNodeId"))
                return promise.future()
            }

            // Get the flow class ID from instance config
            val flowClassId = instanceConfig.config.getString("flowClassId")
            if (flowClassId.isNullOrBlank()) {
                promise.fail(Exception("Flow instance ${instanceConfig.name} has no flowClassId"))
                return promise.future()
            }

            // Get the flow class
            val flowClassConfig = flowClasses[flowClassId]
            if (flowClassConfig == null) {
                promise.fail(Exception("Flow class $flowClassId not found for instance ${instanceConfig.name}"))
                return promise.future()
            }

            // Deploy as verticle
            val executor = FlowInstanceExecutor(instanceConfig, flowClassConfig)
            vertx.deployVerticle(executor).onComplete { deployResult ->
                if (deployResult.succeeded()) {
                    val deploymentId = deployResult.result()
                    flowVerticles[instanceConfig.name] = deploymentId
                    logger.info("Deployed flow instance verticle ${instanceConfig.name} (class: $flowClassId, deploymentId: $deploymentId)")
                    promise.complete(instanceConfig.name)
                } else {
                    logger.severe("Failed to deploy flow verticle ${instanceConfig.name}: ${deployResult.cause()?.message}")
                    deployResult.cause()?.printStackTrace()
                    promise.fail(deployResult.cause())
                }
            }

        } catch (e: Exception) {
            logger.severe("Error deploying flow ${instanceConfig.name}: ${e.message}")
            e.printStackTrace()
            promise.fail(e)
        }

        return promise.future()
    }

    private fun undeployFlow(instanceName: String): Future<Void> {
        val promise = Promise.promise<Void>()

        try {
            val deploymentId = flowVerticles.remove(instanceName)
            if (deploymentId != null) {
                vertx.undeploy(deploymentId).onComplete { result ->
                    if (result.succeeded()) {
                        logger.info("Undeployed flow instance verticle $instanceName")
                        promise.complete()
                    } else {
                        logger.warning("Error undeploying flow $instanceName: ${result.cause()?.message}")
                        promise.complete() // Complete anyway to avoid blocking
                    }
                }
            } else {
                promise.complete()
            }
        } catch (e: Exception) {
            logger.severe("Error undeploying flow $instanceName: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    private fun setupEventBusHandlers(): Future<Void> {
        val promise = Promise.promise<Void>()

        try {
            // Handle flow configuration changes
            vertx.eventBus().consumer<JsonObject>(ADDRESS_FLOW_CONFIG_CHANGED) { message ->
                handleFlowConfigChange(message)
            }

            promise.complete()

        } catch (e: Exception) {
            promise.fail(e)
        }

        return promise.future()
    }

    private fun handleFlowConfigChange(message: Message<JsonObject>) {
        try {
            val changeData = message.body()
            val operation = changeData.getString("operation")
            val deviceName = changeData.getString("deviceName")
            val deviceType = changeData.getString("deviceType")

            logger.finer("Handling flow config change: $operation for $deviceType $deviceName")

            when (deviceType) {
                DeviceConfig.DEVICE_TYPE_FLOW_CLASS -> {
                    // Flow class changed - reload it
                    deviceStore.getDevice(deviceName).onComplete { result ->
                        if (result.succeeded() && result.result() != null) {
                            flowClasses[deviceName] = result.result()!!
                            logger.finer("Reloaded flow class: $deviceName")
                            message.reply(JsonObject().put("success", true))
                        } else {
                            message.fail(500, "Failed to reload flow class")
                        }
                    }
                }

                DeviceConfig.DEVICE_TYPE_FLOW_OBJECT -> {
                    // Flow instance changed
                    when (operation) {
                        "add", "update" -> {
                            val deviceJson = changeData.getJsonObject("device")
                            val device = DeviceConfig.fromJsonObject(deviceJson)

                            if (device.isAssignedToNode(currentNodeId) && device.enabled) {
                                // Redeploy flow verticle
                                undeployFlow(deviceName)
                                    .compose { deployFlow(device) }
                                    .onComplete { result ->
                                        if (result.succeeded()) {
                                            message.reply(JsonObject().put("success", true))
                                        } else {
                                            message.fail(500, result.cause()?.message ?: "Deployment failed")
                                        }
                                    }
                            } else {
                                // Not for this node or disabled - just undeploy
                                undeployFlow(deviceName)
                                    .onComplete { message.reply(JsonObject().put("success", true)) }
                            }
                        }

                        "toggle" -> {
                            // Reload device from store to get current enabled state
                            deviceStore.getDevice(deviceName).onComplete { result ->
                                if (result.succeeded() && result.result() != null) {
                                    val device = result.result()!!
                                    if (device.isAssignedToNode(currentNodeId) && device.enabled) {
                                        // Enable: Deploy the flow
                                        undeployFlow(deviceName)
                                            .compose { deployFlow(device) }
                                            .onComplete { deployResult ->
                                                if (deployResult.succeeded()) {
                                                    logger.info("Enabled and deployed flow: $deviceName")
                                                    message.reply(JsonObject().put("success", true))
                                                } else {
                                                    message.fail(500, deployResult.cause()?.message ?: "Deployment failed")
                                                }
                                            }
                                    } else {
                                        // Disable or not for this node: Undeploy the flow
                                        undeployFlow(deviceName).onComplete {
                                            logger.info("Disabled and undeployed flow: $deviceName")
                                            message.reply(JsonObject().put("success", true))
                                        }
                                    }
                                } else {
                                    message.fail(500, "Failed to load device")
                                }
                            }
                        }

                        "delete" -> {
                            undeployFlow(deviceName)
                                .onComplete { message.reply(JsonObject().put("success", true)) }
                        }

                        else -> {
                            message.fail(400, "Unknown operation: $operation")
                        }
                    }
                }
            }

        } catch (e: Exception) {
            logger.severe("Error handling flow config change: ${e.message}")
            e.printStackTrace()
            message.fail(500, e.message)
        }
    }

    /**
     * Get list of currently active flows on this node
     */
    fun getActiveFlows(): List<String> {
        return flowVerticles.keys.toList()
    }
}
