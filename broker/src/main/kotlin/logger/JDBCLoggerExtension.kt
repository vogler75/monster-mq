package at.rocworks.logger

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigStoreFactory
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.JDBCLoggerConfig
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
 * JDBCLoggerExtension
 * Coordinates deployment of JDBC Logger instances for devices of type DEVICE_TYPE_JDBC_LOGGER.
 * Manages lifecycle based on device configuration changes from IDeviceConfigStore.
 */
class JDBCLoggerExtension : AbstractVerticle() {
    private val logger: Logger = Utils.getLogger(this::class.java)

    private lateinit var deviceStore: IDeviceConfigStore
    private val deployedLoggers = ConcurrentHashMap<String, String>() // deviceName -> deploymentId
    private val activeDevices = ConcurrentHashMap<String, DeviceConfig>()
    private lateinit var currentNodeId: String
    private lateinit var deviceRegistry: LocalMap<String, String>

    companion object {
        const val ADDRESS_DEVICE_CONFIG_CHANGED = "jdbc.logger.device.config.changed"
    }

    override fun start(startPromise: Promise<Void>) {
        try {
            currentNodeId = Monster.getClusterNodeId(vertx)
            deviceRegistry = vertx.sharedData().getLocalMap("jdbc.logger.device.registry")

            initializeDeviceStore()
                .compose { loadAndDeployDevices() }
                .compose { setupEventBusHandlers() }
                .onComplete { res ->
                    if (res.succeeded()) {
                        logger.info("JDBCLoggerExtension started successfully")
                        startPromise.complete()
                    } else {
                        logger.severe("Failed to start JDBCLoggerExtension: ${res.cause()?.message}")
                        startPromise.fail(res.cause())
                    }
                }
        } catch (e: Exception) {
            logger.severe("Error starting JDBCLoggerExtension: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Stopping JDBCLoggerExtension...")
        val undeployFutures = deployedLoggers.values.map { vertx.undeploy(it) }
        @Suppress("UNCHECKED_CAST")
        Future.all<Void>(undeployFutures as List<Future<Void>>)
            .compose { deviceStore.close() }
            .onComplete {
                logger.info("JDBCLoggerExtension stopped")
                stopPromise.complete()
            }
    }

    private fun initializeDeviceStore(): Future<Void> {
        val p = Promise.promise<Void>()
        try {
            val config = vertx.orCreateContext.config()
            val storeType = Monster.getConfigStoreType(config)
            val store = if (storeType != "NONE") {
                DeviceConfigStoreFactory.create(storeType, config, vertx)
            } else null

            if (store != null) {
                deviceStore = store
                deviceStore.initialize().onComplete { initRes ->
                    if (initRes.succeeded()) {
                        logger.info("Device store initialized: $storeType")
                        p.complete()
                    } else {
                        logger.severe("Failed to initialize device store")
                        p.fail(RuntimeException("Failed to init device store"))
                    }
                }
            } else {
                p.fail(RuntimeException("No DeviceConfigStore for type $storeType"))
            }
        } catch (e: Exception) {
            logger.severe("Error initializing device store: ${e.message}")
            p.fail(e)
        }
        return p.future()
    }

    private fun loadAndDeployDevices(): Future<Void> {
        val p = Promise.promise<Void>()
        deviceStore.getEnabledDevicesByNode(currentNodeId).onComplete { res ->
            if (res.succeeded()) {
                val devices = res.result().filter { it.type == DeviceConfig.DEVICE_TYPE_JDBC_LOGGER }
                logger.info("Found ${devices.size} JDBC Logger devices for node $currentNodeId")

                if (devices.isEmpty()) {
                    p.complete()
                    return@onComplete
                }

                var processed = 0
                devices.forEach { dev ->
                    deployLogger(dev).onComplete { dRes ->
                        processed++
                        if (!dRes.succeeded()) {
                            logger.warning("Failed to deploy JDBC Logger ${dev.name}: ${dRes.cause()?.message}")
                        }
                        if (processed == devices.size) {
                            p.complete()
                        }
                    }
                }
            } else {
                logger.severe("Failed to load devices: ${res.cause()?.message}")
                p.fail(res.cause())
            }
        }
        return p.future()
    }

    private fun deployLogger(device: DeviceConfig): Future<String> {
        val p = Promise.promise<String>()

        if (!device.isAssignedToNode(currentNodeId)) {
            p.fail(Exception("Device ${device.name} not assigned to this node"))
            return p.future()
        }

        try {
            // Parse config to determine database type
            val config = JDBCLoggerConfig.fromJson(device.config)

            // Create appropriate logger instance
            val jdbcLogger = when (config.databaseType.uppercase()) {
                "QUESTDB" -> QuestDBLogger()
                "POSTGRESQL" -> {
                    logger.info("Using PostgreSQL logger for database type: ${config.databaseType}")
                    PostgreSQLLogger()
                }
                "TIMESCALEDB" -> {
                    logger.info("Using PostgreSQL logger for TimescaleDB (PostgreSQL compatible)")
                    PostgreSQLLogger()
                }
                else -> {
                    logger.warning("Unknown database type: ${config.databaseType}, using PostgreSQL logger as fallback")
                    PostgreSQLLogger()
                }
            }

            val cfg = JsonObject().put("device", device.toJsonObject())
            val options = DeploymentOptions().setConfig(cfg)

            vertx.deployVerticle(jdbcLogger, options).onComplete { res ->
                if (res.succeeded()) {
                    deployedLoggers[device.name] = res.result()
                    activeDevices[device.name] = device
                    deviceRegistry[device.namespace] = device.name
                    logger.info("Deployed JDBC Logger ${device.name} (${config.databaseType}): ${res.result()}")
                    p.complete(res.result())
                } else {
                    logger.severe("Failed to deploy JDBC Logger ${device.name}: ${res.cause()?.message}")
                    p.fail(res.cause())
                }
            }
        } catch (e: Exception) {
            logger.severe("Error deploying logger ${device.name}: ${e.message}")
            p.fail(e)
        }

        return p.future()
    }

    private fun undeployLogger(deviceName: String): Future<Void> {
        val p = Promise.promise<Void>()
        val deploymentId = deployedLoggers.remove(deviceName)
        activeDevices.remove(deviceName)?.let { deviceRegistry.remove(it.namespace) }

        if (deploymentId == null) {
            p.complete()
            return p.future()
        }

        logger.info("Undeploying JDBC Logger $deviceName")
        vertx.undeploy(deploymentId).onComplete {
            logger.info("Undeployed JDBC Logger $deviceName")
            p.complete()
        }
        return p.future()
    }

    private fun setupEventBusHandlers(): Future<Void> {
        val p = Promise.promise<Void>()
        try {
            // Config change operations
            vertx.eventBus().consumer<JsonObject>(ADDRESS_DEVICE_CONFIG_CHANGED) { msg ->
                handleConfigChange(msg)
            }

            // List loggers
            vertx.eventBus().consumer<JsonObject>(EventBusAddresses.JDBCLoggerBridge.CONNECTORS_LIST) { msg ->
                try {
                    msg.reply(JsonObject().put("devices", activeDevices.keys.toList()))
                } catch (e: Exception) {
                    msg.fail(500, e.message)
                }
            }

            logger.info("Event bus handlers registered")
            p.complete()
        } catch (e: Exception) {
            logger.severe("Error setting up event bus handlers: ${e.message}")
            p.fail(e)
        }
        return p.future()
    }

    private fun handleConfigChange(message: Message<JsonObject>) {
        try {
            val body = message.body()
            val op = body.getString("operation")
            val name = body.getString("deviceName")

            logger.info("Handling config change: $op for device $name")

            when (op) {
                "add", "update" -> {
                    val deviceJson = body.getJsonObject("device")
                    val device = DeviceConfig.fromJsonObject(deviceJson)

                    if (device.type != DeviceConfig.DEVICE_TYPE_JDBC_LOGGER) {
                        message.reply(JsonObject().put("success", true))
                        return
                    }

                    if (device.isAssignedToNode(currentNodeId) && device.enabled) {
                        undeployLogger(name).compose { deployLogger(device) }.onComplete { r ->
                            if (r.succeeded()) {
                                message.reply(JsonObject().put("success", true))
                            } else {
                                message.fail(500, r.cause()?.message)
                            }
                        }
                    } else {
                        undeployLogger(name).onComplete {
                            message.reply(JsonObject().put("success", true))
                        }
                    }
                }

                "delete" -> {
                    undeployLogger(name).onComplete {
                        message.reply(JsonObject().put("success", true))
                    }
                }

                "toggle" -> {
                    val enabled = body.getBoolean("enabled", false)
                    deviceStore.getDevice(name).onComplete { dr ->
                        if (dr.succeeded()) {
                            val device = dr.result()
                            if (device != null) {
                                if (enabled && device.isAssignedToNode(currentNodeId)) {
                                    undeployLogger(name).compose {
                                        deployLogger(device.copy(enabled = true))
                                    }.onComplete { r ->
                                        if (r.succeeded()) {
                                            message.reply(JsonObject().put("success", true))
                                        } else {
                                            message.fail(500, r.cause()?.message)
                                        }
                                    }
                                } else {
                                    undeployLogger(name).onComplete {
                                        message.reply(JsonObject().put("success", true))
                                    }
                                }
                            } else {
                                message.fail(404, "Device not found")
                            }
                        } else {
                            message.fail(500, dr.cause()?.message)
                        }
                    }
                }

                "reassign" -> {
                    val newNodeId = body.getString("nodeId")
                    val device = activeDevices[name]
                    if (device != null) {
                        if (newNodeId == currentNodeId && device.enabled) {
                            deployLogger(device.copy(nodeId = newNodeId)).onComplete { r ->
                                if (r.succeeded()) {
                                    message.reply(JsonObject().put("success", true))
                                } else {
                                    message.fail(500, r.cause()?.message)
                                }
                            }
                        } else {
                            undeployLogger(name).onComplete {
                                message.reply(JsonObject().put("success", true))
                            }
                        }
                    } else {
                        message.reply(JsonObject().put("success", true))
                    }
                }

                else -> {
                    message.fail(400, "Unknown operation: $op")
                }
            }
        } catch (e: Exception) {
            logger.severe("Error handling config change: ${e.message}")
            message.fail(500, e.message)
        }
    }
}
