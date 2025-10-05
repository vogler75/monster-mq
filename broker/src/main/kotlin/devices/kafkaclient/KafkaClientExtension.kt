package at.rocworks.devices.kafkaclient

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
 * KafkaClientExtension
 * Coordinates deployment of KafkaClientConnector instances for devices of type DEVICE_TYPE_KAFKA_CLIENT.
 */
class KafkaClientExtension : AbstractVerticle() {
    private val logger: Logger = Utils.getLogger(this::class.java)

    private lateinit var deviceStore: IDeviceConfigStore
    private val deployedConnectors = ConcurrentHashMap<String, String>()
    private val activeDevices = ConcurrentHashMap<String, DeviceConfig>()
    private lateinit var currentNodeId: String
    private lateinit var deviceRegistry: LocalMap<String, String>

    companion object {
        const val ADDRESS_DEVICE_CONFIG_CHANGED = "kafkaclient.device.config.changed"
    }

    override fun start(startPromise: Promise<Void>) {
        try {
            currentNodeId = Monster.getClusterNodeId(vertx)
            deviceRegistry = vertx.sharedData().getLocalMap("kafkaclient.device.registry")
            initializeDeviceStore()
                .compose { loadAndDeployDevices() }
                .compose { setupEventBusHandlers() }
                .onComplete { res ->
                    if (res.succeeded()) startPromise.complete() else startPromise.fail(res.cause())
                }
        } catch (e: Exception) {
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        val undeployFutures = deployedConnectors.values.map { vertx.undeploy(it) }
        @Suppress("UNCHECKED_CAST")
        Future.all<Void>(undeployFutures as List<Future<Void>>)
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
                deviceStore.initialize().onComplete { initRes ->
                    if (initRes.succeeded()) p.complete() else p.fail(RuntimeException("Failed to init device store"))
                }
            } else {
                p.fail(RuntimeException("No DeviceConfigStore for type $storeType"))
            }
        } catch (e: Exception) {
            p.fail(e)
        }
        return p.future()
    }

    private fun loadAndDeployDevices(): Future<Void> {
        val p = Promise.promise<Void>()
        deviceStore.getEnabledDevicesByNode(currentNodeId).onComplete { res ->
            if (res.succeeded()) {
                val devices = res.result().filter { it.type == DeviceConfig.DEVICE_TYPE_KAFKA_CLIENT }
                if (devices.isEmpty()) { p.complete(); return@onComplete }
                var processed = 0; devices.forEach { dev ->
                    deployConnector(dev).onComplete { dRes ->
                        processed++
                        if (!dRes.succeeded()) {
                            logger.warning("Failed to deploy KafkaClientConnector for ${dev.name}: ${dRes.cause()?.message}")
                        }
                        if (processed == devices.size) p.complete()
                    }
                }
            } else p.fail(res.cause())
        }
        return p.future()
    }

    private fun deployConnector(device: DeviceConfig): Future<String> {
        val p = Promise.promise<String>()
        if (!device.isAssignedToNode(currentNodeId)) { p.fail(Exception("Device not assigned to this node")); return p.future() }
        val cfg = JsonObject().put("device", device.toJsonObject())
        val options = DeploymentOptions().setConfig(cfg)
        vertx.deployVerticle(KafkaClientConnector(), options).onComplete { res ->
            if (res.succeeded()) {
                deployedConnectors[device.name] = res.result()
                activeDevices[device.name] = device
                deviceRegistry[device.namespace] = device.name
                p.complete(res.result())
            } else p.fail(res.cause())
        }
        return p.future()
    }

    private fun undeployConnector(deviceName: String): Future<Void> {
        val p = Promise.promise<Void>()
        val deploymentId = deployedConnectors.remove(deviceName)
        activeDevices.remove(deviceName)?.let { deviceRegistry.remove(it.namespace) }
        if (deploymentId == null) { p.complete(); return p.future() }
        vertx.undeploy(deploymentId).onComplete { p.complete() }
        return p.future()
    }

    private fun setupEventBusHandlers(): Future<Void> {
        val p = Promise.promise<Void>()
        try {
            // Config change operations
            vertx.eventBus().consumer<JsonObject>(ADDRESS_DEVICE_CONFIG_CHANGED) { msg -> handleConfigChange(msg) }

            // List connectors
            vertx.eventBus().consumer<JsonObject>(EventBusAddresses.KafkaBridge.CONNECTORS_LIST) { msg ->
                try { msg.reply(JsonObject().put("devices", activeDevices.keys.toList())) } catch (e: Exception) { msg.fail(500, e.message) }
            }
            p.complete()
        } catch (e: Exception) { p.fail(e) }
        return p.future()
    }

    private fun handleConfigChange(message: Message<JsonObject>) {
        try {
            val body = message.body()
            val op = body.getString("operation")
            val name = body.getString("deviceName")
            when (op) {
                "add", "update", "addAddress", "deleteAddress" -> {
                    val deviceJson = body.getJsonObject("device")
                    val device = DeviceConfig.fromJsonObject(deviceJson)
                    if (device.type != DeviceConfig.DEVICE_TYPE_KAFKA_CLIENT) { message.reply(JsonObject().put("success", true)); return }
                    if (device.isAssignedToNode(currentNodeId) && device.enabled) {
                        undeployConnector(name).compose { deployConnector(device) }.onComplete { r ->
                            if (r.succeeded()) message.reply(JsonObject().put("success", true)) else message.fail(500, r.cause()?.message)
                        }
                    } else {
                        undeployConnector(name).onComplete { message.reply(JsonObject().put("success", true)) }
                    }
                }
                "delete" -> { undeployConnector(name).onComplete { message.reply(JsonObject().put("success", true)) } }
                "toggle" -> {
                    val enabled = body.getBoolean("enabled", false)
                    deviceStore.getDevice(name).onComplete { dr ->
                        if (dr.succeeded()) {
                            val device = dr.result()
                            if (device != null) {
                                if (enabled && device.isAssignedToNode(currentNodeId)) {
                                    undeployConnector(name).compose { deployConnector(device.copy(enabled = true)) }.onComplete { r ->
                                        if (r.succeeded()) message.reply(JsonObject().put("success", true)) else message.fail(500, r.cause()?.message)
                                    }
                                } else {
                                    undeployConnector(name).onComplete { message.reply(JsonObject().put("success", true)) }
                                }
                            } else message.fail(404, "Device not found")
                        } else message.fail(500, dr.cause()?.message)
                    }
                }
                "reassign" -> {
                    val newNodeId = body.getString("nodeId")
                    val device = activeDevices[name]
                    if (device != null) {
                        if (newNodeId == currentNodeId && device.enabled) {
                            deployConnector(device.copy(nodeId = newNodeId)).onComplete { r ->
                                if (r.succeeded()) message.reply(JsonObject().put("success", true)) else message.fail(500, r.cause()?.message)
                            }
                        } else {
                            undeployConnector(name).onComplete { message.reply(JsonObject().put("success", true)) }
                        }
                    } else message.reply(JsonObject().put("success", true))
                }
                else -> message.fail(400, "Unknown operation: $op")
            }
        } catch (e: Exception) { message.fail(500, e.message) }
    }
}
