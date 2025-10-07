package at.rocworks.devices.winccoa

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.DeviceConfigStoreFactory
import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.core.shareddata.LocalMap
import at.rocworks.stores.DeviceConfig
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

/**
 * WinCC OA Extension - Main coordination verticle for WinCC OA device management
 *
 * Responsibilities:
 * - Cluster-aware device management (only manages devices assigned to current node)
 * - Deploys/undeploys WinCCOaConnector verticles per device
 * - Handles configuration changes via EventBus
 * - Routes MQTT subscriptions to appropriate connectors
 */
class WinCCOaExtension : AbstractVerticle() {

    private val logger: Logger = Utils.getLogger(this::class.java)

    // Device configuration store
    private lateinit var deviceStore: IDeviceConfigStore

    // Track deployed connector verticles
    private val deployedConnectors = ConcurrentHashMap<String, String>() // deviceName -> deploymentId

    // Track active device configurations
    private val activeDevices = ConcurrentHashMap<String, DeviceConfig>() // deviceName -> config

    // Current cluster node ID
    private lateinit var currentNodeId: String

    // Shared data for cross-verticle communication
    private lateinit var deviceRegistry: LocalMap<String, String> // namespace -> deviceName

    companion object {
        // EventBus addresses
        const val ADDRESS_DEVICE_CONFIG_CHANGED = "winccoa.device.config.changed"
        const val ADDRESS_WINCCOA_VALUE_PUBLISH = "winccoa.value.publish"
    }

    override fun start(startPromise: Promise<Void>) {
        logger.info("Starting WinCCOaExtension...")

        try {
            // Get current node ID
            currentNodeId = Monster.getClusterNodeId(vertx)
            logger.info("WinCCOaExtension running on node: $currentNodeId")

            // Initialize shared data
            deviceRegistry = vertx.sharedData().getLocalMap("winccoa.device.registry")

            // Initialize device store
            initializeDeviceStore()
                .compose { loadAndDeployDevices() }
                .compose { setupEventBusHandlers() }
                .onComplete { result ->
                    if (result.succeeded()) {
                        logger.info("WinCCOaExtension started successfully")
                        startPromise.complete()
                    } else {
                        logger.severe("Failed to start WinCCOaExtension: ${result.cause()?.message}")
                        startPromise.fail(result.cause())
                    }
                }

        } catch (e: Exception) {
            logger.severe("Exception during WinCCOaExtension startup: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Stopping WinCCOaExtension...")

        // Undeploy all connectors
        val undeployFutures = deployedConnectors.values.map { deploymentId ->
            vertx.undeploy(deploymentId)
        }

        Future.all<Void>(undeployFutures as List<Future<Void>>)
            .compose { deviceStore.close() }
            .onComplete { result ->
                if (result.succeeded()) {
                    logger.info("WinCCOaExtension stopped successfully")
                    stopPromise.complete()
                } else {
                    logger.warning("Error during WinCCOaExtension shutdown: ${result.cause()?.message}")
                    stopPromise.complete() // Complete anyway
                }
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
                            logger.info("WinCC OA device store initialized successfully")
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
            logger.warning("DeviceConfigStore not implemented for this store type, WinCC OA features will be disabled")
            promise.fail(RuntimeException("Store type not implemented"))
        } catch (e: Exception) {
            logger.severe("Failed to create DeviceConfigStore: ${e.message}")
            promise.fail(RuntimeException("Failed to initialize database"))
        }

        return promise.future()
    }

    private fun loadAndDeployDevices(): Future<Void> {
        val promise = Promise.promise<Void>()

        // Load devices assigned to this node
        deviceStore.getEnabledDevicesByNode(currentNodeId)
            .onComplete { result ->
                if (result.succeeded()) {
                    // Filter to only include WinCC OA Client devices
                    val devices = result.result().filter { device ->
                        device.type == DeviceConfig.DEVICE_TYPE_WINCCOA_CLIENT
                    }
                    logger.info("Found ${devices.size} enabled WinCC OA Client devices assigned to node $currentNodeId")

                    if (devices.isEmpty()) {
                        promise.complete()
                        return@onComplete
                    }

                    // Deploy connectors for each device individually, allowing failures
                    var completedCount = 0
                    var successCount = 0

                    devices.forEach { device ->
                        deployConnectorForDevice(device)
                            .onComplete { deployResult ->
                                completedCount++
                                if (deployResult.succeeded()) {
                                    successCount++
                                    logger.info("Successfully deployed connector for device ${device.name}")
                                } else {
                                    logger.warning("Failed to deploy connector for device ${device.name}: ${deployResult.cause()?.message}")
                                }

                                // Complete when all devices have been processed (regardless of success/failure)
                                if (completedCount == devices.size) {
                                    logger.info("WinCC OA device deployment completed: $successCount/$completedCount devices deployed successfully")
                                    promise.complete()
                                }
                            }
                    }
                } else {
                    logger.severe("Failed to load devices: ${result.cause()?.message}")
                    promise.fail(result.cause())
                }
            }

        return promise.future()
    }

    private fun deployConnectorForDevice(device: DeviceConfig): Future<String> {
        val promise = Promise.promise<String>()

        if (!device.isAssignedToNode(currentNodeId)) {
            promise.fail(Exception("Device ${device.name} is not assigned to node $currentNodeId"))
            return promise.future()
        }

        try {
            // Create connector configuration
            val connectorConfig = JsonObject()
                .put("device", device.toJsonObject())

            // Deploy connector verticle
            val options = DeploymentOptions().setConfig(connectorConfig)
            vertx.deployVerticle(WinCCOaConnector(), options)
                .onComplete { result ->
                    if (result.succeeded()) {
                        val deploymentId = result.result()
                        deployedConnectors[device.name] = deploymentId
                        activeDevices[device.name] = device
                        deviceRegistry[device.namespace] = device.name

                        logger.info("Deployed WinCCOaConnector for device ${device.name} (${deploymentId})")
                        promise.complete(deploymentId)
                    } else {
                        logger.severe("Failed to deploy connector for device ${device.name}: ${result.cause()?.message}")
                        promise.fail(result.cause())
                    }
                }

        } catch (e: Exception) {
            promise.fail(e)
        }

        return promise.future()
    }

    private fun undeployConnectorForDevice(deviceName: String): Future<Void> {
        val promise = Promise.promise<Void>()

        val deploymentId = deployedConnectors[deviceName]
        if (deploymentId != null) {
            vertx.undeploy(deploymentId)
                .onComplete { result ->
                    deployedConnectors.remove(deviceName)
                    val device = activeDevices.remove(deviceName)
                    if (device != null) {
                        deviceRegistry.remove(device.namespace)
                    }

                    if (result.succeeded()) {
                        logger.info("Undeployed WinCCOaConnector for device $deviceName")
                        promise.complete()
                    } else {
                        logger.warning("Failed to undeploy connector for device $deviceName: ${result.cause()?.message}")
                        promise.complete() // Continue anyway
                    }
                }
        } else {
            logger.warning("No deployed connector found for device $deviceName")
            promise.complete()
        }

        return promise.future()
    }

    private fun setupEventBusHandlers(): Future<Void> {
        val promise = Promise.promise<Void>()

        try {
            // Handle device configuration changes
            vertx.eventBus().consumer<JsonObject>(ADDRESS_DEVICE_CONFIG_CHANGED) { message ->
                handleDeviceConfigChange(message)
            }

            // Handle WinCC OA value publishing to MQTT bus
            vertx.eventBus().consumer<BrokerMessage>(ADDRESS_WINCCOA_VALUE_PUBLISH) { message ->
                handleWinCCOaValuePublish(message)
            }

            // Provide list of active connector device names
            vertx.eventBus().consumer<JsonObject>(EventBusAddresses.WinCCOaBridge.CONNECTORS_LIST) { msg ->
                try {
                    val list = activeDevices.keys.toList()
                    msg.reply(JsonObject().put("devices", list))
                } catch (e: Exception) {
                    msg.fail(500, e.message)
                }
            }

            promise.complete()

        } catch (e: Exception) {
            promise.fail(e)
        }

        return promise.future()
    }

    private fun handleWinCCOaValuePublish(message: Message<BrokerMessage>) {
        try {
            val mqttMessage = message.body()
            logger.fine("Forwarding WinCC OA value to MQTT bus: ${mqttMessage.topicName} = ${String(mqttMessage.payload)}")

            // Use the shared SessionHandler to ensure proper archiving and distribution
            val sessionHandler = Monster.getSessionHandler()
            if (sessionHandler != null) {
                sessionHandler.publishMessage(mqttMessage)
            } else {
                logger.severe("SessionHandler not available for WinCC OA message publishing")
            }

        } catch (e: Exception) {
            logger.severe("Error forwarding WinCC OA value to MQTT bus: ${e.message}")
        }
    }

    private fun handleDeviceConfigChange(message: Message<JsonObject>) {
        try {
            val changeData = message.body()
            val operation = changeData.getString("operation") // "add", "update", "delete", "toggle", "reassign"
            val deviceName = changeData.getString("deviceName")

            logger.info("Handling device config change: $operation for device $deviceName")

            when (operation) {
                "add", "update", "addAddress", "deleteAddress" -> {
                    val deviceJson = changeData.getJsonObject("device")
                    val device = DeviceConfig.fromJsonObject(deviceJson)

                    logger.info("Device $deviceName: nodeId=${device.nodeId}, currentNodeId=$currentNodeId, enabled=${device.enabled}, isAssigned=${device.isAssignedToNode(currentNodeId)}")

                    if (device.isAssignedToNode(currentNodeId) && device.enabled) {
                        // Redeploy connector for this device
                        logger.info("Deploying WinCC OA connector for device $deviceName on node $currentNodeId")
                        undeployConnectorForDevice(deviceName)
                            .compose { deployConnectorForDevice(device) }
                            .onComplete { result ->
                                if (result.succeeded()) {
                                    logger.info("Successfully redeployed connector for device $deviceName after $operation")
                                    message.reply(JsonObject().put("success", true))
                                } else {
                                    logger.warning("Failed to redeploy connector for device $deviceName after $operation: ${result.cause()?.message}")
                                    message.fail(500, result.cause()?.message ?: "Deployment failed")
                                }
                            }
                    } else {
                        // Device not for this node or disabled - just undeploy if exists
                        logger.info("Skipping deployment for device $deviceName: not assigned to this node or disabled")
                        undeployConnectorForDevice(deviceName)
                            .onComplete { message.reply(JsonObject().put("success", true)) }
                    }
                }

                "delete" -> {
                    undeployConnectorForDevice(deviceName)
                        .onComplete { message.reply(JsonObject().put("success", true)) }
                }

                "toggle" -> {
                    val enabled = changeData.getBoolean("enabled", false)

                    // Get device from database
                    deviceStore.getDevice(deviceName)
                        .onComplete { deviceResult ->
                            if (deviceResult.succeeded()) {
                                val device = deviceResult.result()
                                if (device != null) {
                                    if (enabled && device.isAssignedToNode(currentNodeId)) {
                                        // Enable: undeploy if exists, then deploy
                                        undeployConnectorForDevice(deviceName)
                                            .compose { deployConnectorForDevice(device.copy(enabled = true)) }
                                            .onComplete { result ->
                                                if (result.succeeded()) {
                                                    message.reply(JsonObject().put("success", true))
                                                } else {
                                                    message.fail(500, result.cause()?.message ?: "Deploy failed")
                                                }
                                            }
                                    } else {
                                        // Disable: undeploy
                                        undeployConnectorForDevice(deviceName)
                                            .onComplete { message.reply(JsonObject().put("success", true)) }
                                    }
                                } else {
                                    message.fail(404, "Device not found: $deviceName")
                                }
                            } else {
                                message.fail(500, "Failed to load device: ${deviceResult.cause()?.message}")
                            }
                        }
                }

                "reassign" -> {
                    val newNodeId = changeData.getString("nodeId")
                    val device = activeDevices[deviceName]

                    if (device != null) {
                        if (newNodeId == currentNodeId && device.enabled) {
                            // Device reassigned TO this node - deploy
                            deployConnectorForDevice(device.copy(nodeId = newNodeId))
                                .onComplete { result ->
                                    if (result.succeeded()) {
                                        message.reply(JsonObject().put("success", true))
                                    } else {
                                        message.fail(500, result.cause()?.message ?: "Deploy failed")
                                    }
                                }
                        } else {
                            // Device reassigned FROM this node - undeploy
                            undeployConnectorForDevice(deviceName)
                                .onComplete { message.reply(JsonObject().put("success", true)) }
                        }
                    } else {
                        message.reply(JsonObject().put("success", true))
                    }
                }

                else -> {
                    message.fail(400, "Unknown operation: $operation")
                }
            }

        } catch (e: Exception) {
            logger.severe("Error handling device config change: ${e.message}")
            message.fail(500, e.message)
        }
    }

    /**
     * Get list of currently active devices on this node
     */
    fun getActiveDevices(): List<DeviceConfig> {
        return activeDevices.values.toList()
    }

    /**
     * Get device store for external access (e.g., GraphQL)
     */
    fun getDeviceStore(): IDeviceConfigStore {
        return deviceStore
    }
}
