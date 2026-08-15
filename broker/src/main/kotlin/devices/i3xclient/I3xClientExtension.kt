package at.rocworks.devices.i3xclient

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigStoreFactory
import at.rocworks.stores.IDeviceConfigStore
import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.core.shareddata.LocalMap
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

/**
 * i3X Client Extension - Main coordination verticle for i3X Client device management.
 *
 * Responsibilities:
 * - Cluster-aware device management (only manages devices assigned to current node)
 * - Deploys/undeploys I3xClientConnector verticles per device
 * - Handles configuration changes via EventBus
 * - Routes incoming i3X updates to the broker's message bus
 */
class I3xClientExtension : AbstractVerticle() {

    private val logger: Logger = Utils.getLogger(this::class.java)

    // Device configuration store
    private lateinit var deviceStore: IDeviceConfigStore

    // Track deployed connector verticles (deviceName -> deploymentId)
    private val deployedConnectors = ConcurrentHashMap<String, String>()

    // Track active device configurations (deviceName -> config)
    private val activeDevices = ConcurrentHashMap<String, DeviceConfig>()

    // Current cluster node ID
    private lateinit var currentNodeId: String

    // Shared data for cross-verticle communication (namespace -> deviceName)
    private lateinit var deviceRegistry: LocalMap<String, String>

    companion object {
        const val ADDRESS_DEVICE_CONFIG_CHANGED = "i3x.device.config.changed"
        const val ADDRESS_I3X_VALUE_PUBLISH = "i3x.value.publish"
    }

    override fun start(startPromise: Promise<Void>) {
        logger.fine("Starting I3xClientExtension...")

        try {
            currentNodeId = Monster.getClusterNodeId(vertx)
            logger.fine("I3xClientExtension running on node: $currentNodeId")

            deviceRegistry = vertx.sharedData().getLocalMap("i3x.device.registry")

            initializeDeviceStore()
                .compose { loadAndDeployDevices() }
                .compose { setupEventBusHandlers() }
                .onComplete { result ->
                    if (result.succeeded()) {
                        logger.fine("I3xClientExtension started successfully")
                        startPromise.complete()
                    } else {
                        logger.severe("Failed to start I3xClientExtension: ${result.cause()?.message}")
                        startPromise.fail(result.cause())
                    }
                }

        } catch (e: Exception) {
            logger.severe("Exception during I3xClientExtension startup: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.fine("Stopping I3xClientExtension...")

        val undeployFutures = deployedConnectors.values.map { deploymentId ->
            vertx.undeploy(deploymentId)
        }

        Future.all(undeployFutures)
            .compose { deviceStore.close() }
            .onComplete { result ->
                if (result.succeeded()) {
                    logger.fine("I3xClientExtension stopped successfully")
                    stopPromise.complete()
                } else {
                    logger.warning("Error during I3xClientExtension shutdown: ${result.cause()?.message}")
                    stopPromise.complete()
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
                            logger.fine("i3X device store initialized successfully")
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
            logger.warning("DeviceConfigStore not implemented for this store type, i3X features will be disabled")
            promise.fail(RuntimeException("Store type not implemented"))
        } catch (e: Exception) {
            logger.severe("Failed to create DeviceConfigStore: ${e.message}")
            promise.fail(RuntimeException("Failed to initialize database"))
        }

        return promise.future()
    }

    private fun loadAndDeployDevices(): Future<Void> {
        val promise = Promise.promise<Void>()

        deviceStore.getEnabledDevicesByNode(currentNodeId)
            .onComplete { result ->
                if (result.succeeded()) {
                    val devices = result.result().filter { device ->
                        device.type == DeviceConfig.DEVICE_TYPE_I3X_CLIENT
                    }
                    logger.fine("Found ${devices.size} enabled i3X Client devices assigned to node $currentNodeId")

                    if (devices.isEmpty()) {
                        promise.complete()
                        return@onComplete
                    }

                    var completedCount = 0
                    var successCount = 0

                    devices.forEach { device ->
                        deployConnectorForDevice(device)
                            .onComplete { deployResult ->
                                completedCount++
                                if (deployResult.succeeded()) {
                                    successCount++
                                    logger.fine("Successfully deployed connector for device ${device.name}")
                                } else {
                                    logger.warning("Failed to deploy connector for device ${device.name}: ${deployResult.cause()?.message}")
                                }

                                if (completedCount == devices.size) {
                                    logger.info("i3X device deployment completed: $successCount/$completedCount devices deployed successfully")
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
            val connectorConfig = JsonObject().put("device", device.toJsonObject())
            val connectorVerticle = I3xClientConnector()
            val options = DeploymentOptions().setConfig(connectorConfig)

            vertx.deployVerticle(connectorVerticle, options)
                .onComplete { result ->
                    if (result.succeeded()) {
                        val deploymentId = result.result()
                        deployedConnectors[device.name] = deploymentId
                        activeDevices[device.name] = device
                        deviceRegistry[device.namespace] = device.name

                        logger.info("Deployed I3xClientConnector for device ${device.name} ($deploymentId)")
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
                        logger.info("Undeployed I3xClientConnector for device $deviceName")
                        promise.complete()
                    } else {
                        logger.warning("Failed to undeploy connector for device $deviceName: ${result.cause()?.message}")
                        promise.complete()
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
            vertx.eventBus().consumer<JsonObject>(ADDRESS_DEVICE_CONFIG_CHANGED) { message ->
                handleDeviceConfigChange(message)
            }

            vertx.eventBus().consumer<BrokerMessage>(ADDRESS_I3X_VALUE_PUBLISH) { message ->
                handleI3xValuePublish(message)
            }

            vertx.eventBus().consumer<JsonObject>(EventBusAddresses.I3xBridge.connectorsList(currentNodeId)) { msg ->
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

    private fun handleI3xValuePublish(message: Message<BrokerMessage>) {
        try {
            val mqttMessage = message.body()
            logger.fine("Forwarding i3X value to MQTT bus: ${mqttMessage.topicName} = ${String(mqttMessage.payload)}")

            val sessionHandler = Monster.getSessionHandler()
            if (sessionHandler != null) {
                sessionHandler.publishMessage(mqttMessage)
            } else {
                logger.severe("SessionHandler not available for i3X message publishing")
            }

        } catch (e: Exception) {
            logger.severe("Error forwarding i3X value to MQTT bus: ${e.message}")
        }
    }

    private fun handleDeviceConfigChange(message: Message<JsonObject>) {
        try {
            val changeData = message.body()
            val operation = changeData.getString("operation")
            val deviceName = changeData.getString("deviceName")

            logger.info("Handling i3X device config change: $operation for device $deviceName")

            when (operation) {
                "add", "update", "addAddress", "deleteAddress" -> {
                    val deviceJson = changeData.getJsonObject("device")
                    val device = DeviceConfig.fromJsonObject(deviceJson)

                    if (device.isAssignedToNode(currentNodeId) && device.enabled) {
                        logger.info("Deploying i3X connector for device $deviceName on node $currentNodeId")
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
                    deviceStore.getDevice(deviceName)
                        .onComplete { deviceResult ->
                            if (deviceResult.succeeded()) {
                                val device = deviceResult.result()
                                if (device != null) {
                                    if (enabled && device.isAssignedToNode(currentNodeId)) {
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
                            deployConnectorForDevice(device.copy(nodeId = newNodeId))
                                .onComplete { result ->
                                    if (result.succeeded()) {
                                        message.reply(JsonObject().put("success", true))
                                    } else {
                                        message.fail(500, result.cause()?.message ?: "Deploy failed")
                                    }
                                }
                        } else {
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

    fun getActiveDevices(): List<DeviceConfig> = activeDevices.values.toList()
    fun getDeviceStore(): IDeviceConfigStore = deviceStore
}
