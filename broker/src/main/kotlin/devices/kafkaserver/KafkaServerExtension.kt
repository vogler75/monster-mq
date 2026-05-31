package at.rocworks.devices.kafkaserver

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.auth.UserManager
import at.rocworks.handlers.SessionHandler
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.KafkaQueueStoreFactory
import at.rocworks.handlers.KafkaStreamOrchestrator
import at.rocworks.extensions.KafkaProtocolServer
import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

class KafkaServerExtension(
    private val sessionHandler: SessionHandler,
    private val deviceConfigStore: IDeviceConfigStore?,
    private val userManager: UserManager
) : AbstractVerticle() {

    companion object {
        private val logger: Logger = Utils.getLogger(KafkaServerExtension::class.java)
        const val ADDRESS_DEVICE_CONFIG_CHANGED = "kafkaserver.device.config.changed"
    }

    private val runningServers = ConcurrentHashMap<String, RunningKafkaServer>()
    private val startingServers = ConcurrentHashMap.newKeySet<String>()
    private lateinit var currentNodeId: String

    override fun start(startPromise: Promise<Void>) {
        currentNodeId = Monster.getClusterNodeId(vertx)
        logger.fine("Starting Kafka Server Extension on node: $currentNodeId")

        // Register event bus handlers
        setupEventBusHandlers()

        // Load and start configured servers from database store
        loadAndDeployServers().onComplete { ar ->
            if (ar.succeeded()) {
                logger.info("Kafka Server Extension initialized successfully on node: $currentNodeId")
                startPromise.complete()
            } else {
                logger.severe("Failed to initialize Kafka servers: ${ar.cause()?.message}")
                startPromise.fail(ar.cause())
            }
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.fine("Stopping Kafka Server Extension")
        val stopFutures = runningServers.keys.map { stopServer(it) }
        Future.all<Void>(stopFutures).onComplete {
            runningServers.clear()
            stopPromise.complete()
        }
    }

    private fun setupEventBusHandlers() {
        // Dynamic config changes targeted cluster-wide or node-specific
        vertx.eventBus().consumer<JsonObject>(ADDRESS_DEVICE_CONFIG_CHANGED) { msg ->
            handleConfigChange(msg)
        }

        // List active connectors / servers on this node
        vertx.eventBus().consumer<JsonObject>("kafkaserver.connectors.list.$currentNodeId") { msg ->
            msg.reply(JsonObject()
                .put("servers", runningServers.keys.toList())
                .put("starting", startingServers.toList()))
        }
    }

    private fun loadAndDeployServers(): Future<Void> {
        if (deviceConfigStore == null) {
            logger.warning("No device configuration store available for Kafka Server Extension")
            return Future.succeededFuture()
        }

        val promise = Promise.promise<Void>()
        deviceConfigStore.getEnabledDevicesByNode(currentNodeId).onComplete { res ->
            if (res.succeeded()) {
                val devices = res.result().filter { it.type == DeviceConfig.DEVICE_TYPE_KAFKA_SERVER }
                if (devices.isEmpty()) {
                    promise.complete()
                    return@onComplete
                }

                var deployedCount = 0
                val totalCount = devices.size
                devices.forEach { dev ->
                    val config = KafkaServerConfig.fromJsonObject(dev.config.copy().put("name", dev.name).put("namespace", dev.namespace))
                    deployServer(config).onComplete { deployRes ->
                        deployedCount++
                        if (deployRes.failed()) {
                            logger.severe("Failed to deploy Kafka server '${dev.name}': ${deployRes.cause()?.message}")
                        }
                        if (deployedCount == totalCount) {
                            promise.complete()
                        }
                    }
                }
            } else {
                promise.fail(res.cause())
            }
        }
        return promise.future()
    }

    private fun deployServer(config: KafkaServerConfig): Future<Void> {
        val promise = Promise.promise<Void>()
        
        if (runningServers.containsKey(config.name)) {
            promise.complete()
            return promise.future()
        }

        startingServers.add(config.name)
        logger.info("Deploying dynamic Kafka server '${config.name}' on port ${config.port}...")

        // Create a custom merged config JsonObject context for the server instance
        val baseConfig = config().copy()
        val serverConfigJson = config.toJsonObject()
        val customConfig = baseConfig.put("KafkaServer", serverConfigJson)

        val multiplexer = at.rocworks.stores.KafkaMultiplexingQueueStore(vertx, customConfig)
        multiplexer.initialize().onComplete { multiRes ->
            if (multiRes.failed()) {
                logger.warning("Failed to initialize dynamic Kafka stream storage: ${multiRes.cause()?.message}")
            }
            
            val orchestrator = KafkaStreamOrchestrator(customConfig, multiplexer, sessionHandler, deviceConfigStore)
            val server = KafkaProtocolServer(customConfig, multiplexer, sessionHandler, userManager, deviceConfigStore)

            // Deploy Orchestrator first, then deploy Protocol Server
            vertx.deployVerticle(orchestrator).compose { orchId ->
                vertx.deployVerticle(server).map { servId ->
                    RunningKafkaServer(config, orchId, servId)
                }
            }.onComplete { deployRes ->
                startingServers.remove(config.name)
                if (deployRes.succeeded()) {
                    val running = deployRes.result()
                    runningServers[config.name] = running
                    logger.info("Dynamic Kafka server '${config.name}' is fully operational on host ${config.host}:${config.port}")
                    promise.complete()
                } else {
                    promise.fail(deployRes.cause())
                }
            }
        }

        return promise.future()
    }

    private fun stopServer(name: String): Future<Void> {
        val promise = Promise.promise<Void>()
        val running = runningServers.remove(name)
        
        if (running == null) {
            promise.complete()
            return promise.future()
        }

        logger.info("Stopping dynamic Kafka server '${name}'...")
        // Undeploy child verticles
        vertx.undeploy(running.serverDeploymentId).compose {
            vertx.undeploy(running.orchestratorDeploymentId)
        }.onComplete { res ->
            if (res.succeeded()) {
                logger.info("Dynamic Kafka server '${name}' stopped successfully.")
                promise.complete()
            } else {
                logger.severe("Failed to gracefully stop dynamic Kafka server '${name}': ${res.cause()?.message}")
                promise.fail(res.cause())
            }
        }

        return promise.future()
    }

    private fun handleConfigChange(message: Message<JsonObject>) {
        try {
            val body = message.body()
            val op = body.getString("operation")
            val name = body.getString("deviceName")
            
            when (op) {
                "add", "update" -> {
                    val deviceJson = body.getJsonObject("device")
                    val device = DeviceConfig.fromJsonObject(deviceJson)
                    if (device.type != DeviceConfig.DEVICE_TYPE_KAFKA_SERVER) {
                        message.reply(JsonObject().put("success", true))
                        return
                    }

                    if (device.isAssignedToNode(currentNodeId) && device.enabled) {
                        val config = KafkaServerConfig.fromJsonObject(
                            device.config.copy()
                                .put("name", device.name)
                                .put("namespace", device.namespace)
                                .put("nodeId", device.nodeId)
                                .put("enabled", device.enabled)
                        )
                        stopServer(name).compose { deployServer(config) }.onComplete { res ->
                            if (res.succeeded()) {
                                message.reply(JsonObject().put("success", true))
                            } else {
                                message.fail(500, res.cause()?.message)
                            }
                        }
                    } else {
                        stopServer(name).onComplete {
                            message.reply(JsonObject().put("success", true))
                        }
                    }
                }
                "delete" -> {
                    stopServer(name).onComplete {
                        message.reply(JsonObject().put("success", true))
                    }
                }
                "toggle" -> {
                    val enabled = body.getBoolean("enabled", false)
                    deviceConfigStore?.getDevice(name)?.onComplete { dr ->
                        if (dr.succeeded()) {
                            val device = dr.result()
                            if (device != null && device.type == DeviceConfig.DEVICE_TYPE_KAFKA_SERVER) {
                                if (enabled && device.isAssignedToNode(currentNodeId)) {
                                    val config = KafkaServerConfig.fromJsonObject(
                                        device.config.copy()
                                            .put("name", device.name)
                                            .put("namespace", device.namespace)
                                            .put("nodeId", device.nodeId)
                                            .put("enabled", true)
                                    )
                                    stopServer(name).compose { deployServer(config) }.onComplete { res ->
                                        if (res.succeeded()) message.reply(JsonObject().put("success", true))
                                        else message.fail(500, res.cause()?.message)
                                    }
                                } else {
                                    stopServer(name).onComplete {
                                        message.reply(JsonObject().put("success", true))
                                    }
                                }
                            } else {
                                message.fail(404, "Kafka Server device config not found")
                            }
                        } else {
                            message.fail(500, dr.cause()?.message)
                        }
                    }
                }
                "reassign" -> {
                    val newNodeId = body.getString("nodeId")
                    deviceConfigStore?.getDevice(name)?.onComplete { dr ->
                        if (dr.succeeded()) {
                            val device = dr.result()
                            if (device != null && device.type == DeviceConfig.DEVICE_TYPE_KAFKA_SERVER) {
                                if (newNodeId == currentNodeId && device.enabled) {
                                    val config = KafkaServerConfig.fromJsonObject(
                                        device.config.copy()
                                            .put("name", device.name)
                                            .put("namespace", device.namespace)
                                            .put("nodeId", newNodeId)
                                            .put("enabled", device.enabled)
                                    )
                                    deployServer(config).onComplete { res ->
                                        if (res.succeeded()) message.reply(JsonObject().put("success", true))
                                        else message.fail(500, res.cause()?.message)
                                    }
                                } else {
                                    stopServer(name).onComplete {
                                        message.reply(JsonObject().put("success", true))
                                    }
                                }
                            } else {
                                message.reply(JsonObject().put("success", true))
                            }
                        } else {
                            message.fail(500, dr.cause()?.message)
                        }
                    }
                }
                else -> message.fail(400, "Unsupported Kafka Server dynamic operation: $op")
            }
        } catch (e: Exception) {
            logger.severe("Failed handling EventBus configuration update: ${e.message}")
            message.fail(500, e.message)
        }
    }
}

private class RunningKafkaServer(
    val config: KafkaServerConfig,
    val orchestratorDeploymentId: String,
    val serverDeploymentId: String
)
