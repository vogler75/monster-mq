package at.rocworks.logger

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
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
 * TimeBaseLoggerExtension
 * Coordinates deployment of TimeBase Logger instances.
 */
class TimeBaseLoggerExtension : AbstractVerticle() {
    private val logger: Logger = Utils.getLogger(this::class.java)

    private lateinit var deviceStore: IDeviceConfigStore
    private val deployedLoggers = ConcurrentHashMap<String, String>()
    private val activeDevices = ConcurrentHashMap<String, DeviceConfig>()
    private lateinit var currentNodeId: String
    private lateinit var deviceRegistry: LocalMap<String, String>

    companion object {
        const val ADDRESS_DEVICE_CONFIG_CHANGED = "timebase.logger.device.config.changed"
    }

    override fun start(startPromise: Promise<Void>) {
        try {
            currentNodeId = Monster.getClusterNodeId(vertx)
            deviceRegistry = vertx.sharedData().getLocalMap("timebase.logger.device.registry")

            initializeDeviceStore()
                .compose { loadAndDeployDevices() }
                .compose { setupEventBusHandlers() }
                .onComplete { res ->
                    if (res.succeeded()) {
                        logger.fine("TimeBaseLoggerExtension started successfully")
                        startPromise.complete()
                    } else {
                        logger.severe("Failed to start TimeBaseLoggerExtension: ${res.cause()?.message}")
                        startPromise.fail(res.cause())
                    }
                }
        } catch (e: Exception) {
            logger.severe("Error starting TimeBaseLoggerExtension: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        val undeployFutures = deployedLoggers.values.map { vertx.undeploy(it) }
        Future.all<Void>(undeployFutures)
            .compose { deviceStore.close() }
            .onComplete { stopPromise.complete() }
    }

    private fun initializeDeviceStore(): Future<Void> {
        val p = Promise.promise<Void>()
        try {
            val config = vertx.orCreateContext.config()
            val storeType = Monster.getConfigStoreType(config)
            val store = if (storeType != "NONE") DeviceConfigStoreFactory.create(storeType, config, vertx) else null
            if (store != null) {
                deviceStore = store
                deviceStore.initialize().onComplete { if (it.succeeded()) p.complete() else p.fail("Failed to init device store") }
            } else p.fail("No DeviceConfigStore for type $storeType")
        } catch (e: Exception) { p.fail(e) }
        return p.future()
    }

    private fun loadAndDeployDevices(): Future<Void> {
        val p = Promise.promise<Void>()
        deviceStore.getEnabledDevicesByNode(currentNodeId).onComplete { res ->
            if (res.succeeded()) {
                val devices = res.result().filter { it.type == DeviceConfig.DEVICE_TYPE_TIMEBASE_LOGGER }
                if (devices.isEmpty()) { p.complete(); return@onComplete }
                var processed = 0
                devices.forEach { dev ->
                    deployLogger(dev).onComplete { processed++; if (processed == devices.size) p.complete() }
                }
            } else p.fail(res.cause())
        }
        return p.future()
    }

    private fun deployLogger(device: DeviceConfig): Future<String> {
        val p = Promise.promise<String>()
        if (!device.isAssignedToNode(currentNodeId)) return Future.failedFuture("Not assigned to this node")
        try {
            val timeBaseLogger = TimeBaseLogger()
            val cfg = JsonObject().put("device", device.toJsonObject())
            val options = DeploymentOptions().setConfig(cfg)
            vertx.deployVerticle(timeBaseLogger, options).onComplete { res ->
                if (res.succeeded()) {
                    deployedLoggers[device.name] = res.result()
                    activeDevices[device.name] = device
                    deviceRegistry[device.namespace] = device.name
                    p.complete(res.result())
                } else p.fail(res.cause())
            }
        } catch (e: Exception) { p.fail(e) }
        return p.future()
    }

    private fun undeployLogger(deviceName: String): Future<Void> {
        val deploymentId = deployedLoggers.remove(deviceName)
        activeDevices.remove(deviceName)?.let { deviceRegistry.remove(it.namespace) }
        return if (deploymentId == null) Future.succeededFuture() else vertx.undeploy(deploymentId)
    }

    private fun setupEventBusHandlers(): Future<Void> {
        vertx.eventBus().consumer<JsonObject>(ADDRESS_DEVICE_CONFIG_CHANGED) { msg -> handleConfigChange(msg) }
        vertx.eventBus().consumer<JsonObject>(EventBusAddresses.TimeBaseLoggerBridge.CONNECTORS_LIST) { msg ->
            msg.reply(JsonObject().put("devices", activeDevices.keys.toList()))
        }
        return Future.succeededFuture()
    }

    private fun handleConfigChange(message: Message<JsonObject>) {
        try {
            val body = message.body()
            val op = body.getString("operation")
            val name = body.getString("deviceName")
            when (op) {
                "add", "update" -> {
                    val device = DeviceConfig.fromJsonObject(body.getJsonObject("device"))
                    if (device.type != DeviceConfig.DEVICE_TYPE_TIMEBASE_LOGGER) {
                        message.reply(JsonObject().put("success", true)); return
                    }
                    if (device.isAssignedToNode(currentNodeId) && device.enabled) {
                        undeployLogger(name).compose { deployLogger(device) }.onComplete { r ->
                            if (r.succeeded()) message.reply(JsonObject().put("success", true))
                            else message.fail(500, r.cause()?.message)
                        }
                    } else undeployLogger(name).onComplete { message.reply(JsonObject().put("success", true)) }
                }
                "delete" -> undeployLogger(name).onComplete { message.reply(JsonObject().put("success", true)) }
                "toggle" -> {
                    val enabled = body.getBoolean("enabled", false)
                    deviceStore.getDevice(name).onComplete { dr ->
                        val device = dr.result()
                        if (device != null) {
                            if (enabled && device.isAssignedToNode(currentNodeId)) {
                                undeployLogger(name).compose { deployLogger(device.copy(enabled = true)) }
                                    .onComplete { r -> if (r.succeeded()) message.reply(JsonObject().put("success", true)) else message.fail(500, r.cause()?.message) }
                            } else undeployLogger(name).onComplete { message.reply(JsonObject().put("success", true)) }
                        } else message.fail(404, "Device not found")
                    }
                }
                else -> message.reply(JsonObject().put("success", true))
            }
        } catch (e: Exception) { message.fail(500, e.message) }
    }
}
