package at.rocworks.devices.script

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigStoreFactory
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.ScriptConfig
import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

/**
 * Cluster-wide coordinator verticle for Script devices.
 * Manages deployment, undeployment, and reconfiguration of ScriptConnector verticles.
 */
class ScriptExtension(
    private var deviceStore: IDeviceConfigStore? = null
) : AbstractVerticle() {

    companion object {
        const val ADDRESS_DEVICE_CONFIG_CHANGED = "script.device.config.changed"
        private val logger: Logger = Utils.getLogger(ScriptExtension::class.java)

        @Volatile
        private var instance: ScriptExtension? = null

        fun getInstance(): ScriptExtension? = instance
    }

    val globalStore = ScriptGlobalStore()
    val activeConnectors = ConcurrentHashMap<String, ScriptConnector>()
    private val activeDeployments = ConcurrentHashMap<String, String>() // deviceName -> deploymentId
    private val activeDevices = ConcurrentHashMap<String, DeviceConfig>()

    private lateinit var currentNodeId: String

    override fun start(startPromise: Promise<Void>) {
        instance = this
        currentNodeId = Monster.getClusterNodeId(vertx)
        logger.info("Starting ScriptExtension on cluster node: $currentNodeId")

        initializeStore()
            .compose { loadAndDeployDevices() }
            .compose { setupEventBusHandlers() }
            .onComplete { res ->
                if (res.succeeded()) {
                    logger.info("ScriptExtension started successfully")
                    startPromise.complete()
                } else {
                    logger.severe("Failed to start ScriptExtension: ${res.cause()?.message}")
                    startPromise.fail(res.cause())
                }
            }
    }

    override fun stop(stopPromise: Promise<Void>) {
        val undeployFutures = activeDeployments.values.map { vertx.undeploy(it) }
        Future.all(undeployFutures).onComplete {
            activeConnectors.clear()
            activeDeployments.clear()
            activeDevices.clear()
            instance = null
            logger.info("ScriptExtension stopped")
            stopPromise.complete()
        }
    }

    private fun initializeStore(): Future<Void> {
        val promise = Promise.promise<Void>()
        if (deviceStore != null) {
            promise.complete()
            return promise.future()
        }

        val shared = DeviceConfigStoreFactory.getSharedInstance()
        if (shared != null) {
            deviceStore = shared
            promise.complete()
            return promise.future()
        }

        val config = vertx.orCreateContext.config()
        val configStoreType = Monster.getConfigStoreType(config)
        if (configStoreType != "NONE") {
            try {
                val store = DeviceConfigStoreFactory.create(configStoreType, config, vertx)
                if (store != null) {
                    deviceStore = store
                    store.initialize().onComplete { res ->
                        if (res.succeeded()) promise.complete()
                        else promise.fail(res.cause())
                    }
                } else {
                    promise.complete()
                }
            } catch (e: Exception) {
                logger.warning("Error creating DeviceConfigStore for ScriptExtension: ${e.message}")
                promise.complete()
            }
        } else {
            promise.complete()
        }
        return promise.future()
    }

    private fun loadAndDeployDevices(): Future<Void> {
        val promise = Promise.promise<Void>()
        val store = deviceStore
        if (store == null) {
            promise.complete()
            return promise.future()
        }

        store.getEnabledDevicesByNode(currentNodeId).onComplete { res ->
            if (res.succeeded()) {
                val scriptDevices = res.result().filter { it.type == DeviceConfig.DEVICE_TYPE_SCRIPT }
                logger.info("Found ${scriptDevices.size} enabled Script devices assigned to node $currentNodeId")

                if (scriptDevices.isEmpty()) {
                    promise.complete()
                    return@onComplete
                }

                val futures = scriptDevices.map { deployConnector(it) }
                Future.all(futures).onComplete {
                    promise.complete()
                }
            } else {
                logger.severe("Failed to load script devices: ${res.cause()?.message}")
                promise.fail(res.cause())
            }
        }

        return promise.future()
    }

    fun deployConnector(device: DeviceConfig): Future<String> {
        val promise = Promise.promise<String>()
        if (!device.isAssignedToNode(currentNodeId)) {
            promise.complete("")
            return promise.future()
        }

        val connector = ScriptConnector(
            deviceConfig = device,
            deviceStore = deviceStore,
            globalStore = globalStore,
            scriptInvoker = { name, args -> executeScript(name, args) }
        )

        val options = DeploymentOptions()
        vertx.deployVerticle(connector, options).onComplete { res ->
            if (res.succeeded()) {
                val deploymentId = res.result()
                activeDeployments[device.name] = deploymentId
                activeConnectors[device.name] = connector
                activeDevices[device.name] = device
                logger.info("Deployed ScriptConnector for '${device.name}' ($deploymentId)")
                promise.complete(deploymentId)
            } else {
                logger.severe("Failed to deploy ScriptConnector for '${device.name}': ${res.cause()?.message}")
                promise.fail(res.cause())
            }
        }

        return promise.future()
    }

    fun undeployConnector(deviceName: String): Future<Void> {
        val promise = Promise.promise<Void>()
        val deploymentId = activeDeployments.remove(deviceName)
        activeConnectors.remove(deviceName)
        activeDevices.remove(deviceName)

        if (deploymentId != null) {
            vertx.undeploy(deploymentId).onComplete { res ->
                if (res.succeeded()) {
                    logger.info("Undeployed ScriptConnector for '$deviceName'")
                    promise.complete()
                } else {
                    logger.warning("Error undeploying ScriptConnector for '$deviceName': ${res.cause()?.message}")
                    promise.complete()
                }
            }
        } else {
            promise.complete()
        }
        return promise.future()
    }

    private fun setupEventBusHandlers(): Future<Void> {
        val promise = Promise.promise<Void>()

        vertx.eventBus().consumer<JsonObject>(ADDRESS_DEVICE_CONFIG_CHANGED) { msg ->
            try {
                val body = msg.body()
                val operation = body.getString("operation")
                val deviceName = body.getString("name")

                when (operation) {
                    "create", "update" -> {
                        deviceStore?.getDevice(deviceName)?.onComplete { res ->
                            if (res.succeeded() && res.result() != null) {
                                val dev = res.result()!!
                                if (dev.type == DeviceConfig.DEVICE_TYPE_SCRIPT) {
                                    undeployConnector(deviceName).compose {
                                        if (dev.enabled && dev.isAssignedToNode(currentNodeId)) {
                                            deployConnector(dev)
                                        } else {
                                            Future.succeededFuture("")
                                        }
                                    }.onComplete { r ->
                                        if (r.succeeded()) msg.reply(JsonObject().put("success", true))
                                        else msg.fail(500, r.cause()?.message)
                                    }
                                } else {
                                    msg.reply(JsonObject().put("success", true))
                                }
                            } else {
                                msg.fail(404, "Device not found")
                            }
                        }
                    }
                    "delete" -> {
                        undeployConnector(deviceName).onComplete {
                            msg.reply(JsonObject().put("success", true))
                        }
                    }
                    "toggle" -> {
                        val enabled = body.getBoolean("enabled", false)
                        deviceStore?.getDevice(deviceName)?.onComplete { res ->
                            if (res.succeeded() && res.result() != null) {
                                val dev = res.result()!!
                                if (dev.type == DeviceConfig.DEVICE_TYPE_SCRIPT) {
                                    undeployConnector(deviceName).compose {
                                        if (enabled && dev.isAssignedToNode(currentNodeId)) {
                                            deployConnector(dev.copy(enabled = true))
                                        } else {
                                            Future.succeededFuture("")
                                        }
                                    }.onComplete { r ->
                                        if (r.succeeded()) msg.reply(JsonObject().put("success", true))
                                        else msg.fail(500, r.cause()?.message)
                                    }
                                } else {
                                    msg.reply(JsonObject().put("success", true))
                                }
                            } else {
                                msg.fail(404, "Device not found")
                            }
                        }
                    }
                    else -> msg.reply(JsonObject().put("success", true))
                }
            } catch (e: Exception) {
                logger.severe("Error handling script config change: ${e.message}")
                msg.fail(500, e.message)
            }
        }

        promise.complete()
        return promise.future()
    }

    fun getConnector(name: String): ScriptConnector? = activeConnectors[name]

    fun executeScript(name: String, args: Map<String, Any?>): Any? {
        val connector = activeConnectors[name] ?: return null
        return connector.executeCallable(args)
    }

    /**
     * Executes a dry-run test sandbox for a script configuration.
     */
    fun testScript(
        name: String,
        config: ScriptConfig,
        testTopic: String?,
        testPayload: String?,
        testArgs: Map<String, Any?>?
    ): ScriptExecutionResult {
        val engine = ScriptEngine(
            scriptName = name,
            config = config,
            globalStore = globalStore,
            scriptStorage = ScriptStorage(name, null, currentNodeId),
            recentLogs = null,
            mqttPublisher = null,
            scriptInvoker = { n, a -> executeScript(n, a) }
        )

        val mockMsg = if (testTopic != null || testPayload != null) {
            BrokerMessage(
                messageId = 1,
                topicName = testTopic ?: "test/topic",
                payload = (testPayload ?: "").toByteArray(),
                qosLevel = 0,
                isRetain = false,
                isDup = false,
                isQueued = false,
                clientId = "sandbox-tester"
            )
        } else null

        return engine.execute(mockMsg, testArgs, dryRun = true)
    }
}
