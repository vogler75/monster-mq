package at.rocworks.devices.opcuaserver

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.auth.UserManager
// Removed - we'll define our own EventBusAddresses
import at.rocworks.handlers.SessionHandler
import at.rocworks.stores.IDeviceConfigStore
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

/**
 * Extension for managing OPC UA Server instances in the MonsterMQ broker
 */
class OpcUaServerExtension(
    private val sessionHandler: SessionHandler,
    private val deviceConfigStore: IDeviceConfigStore?,
    private val userManager: UserManager?
) : AbstractVerticle() {

    companion object {
        private val logger: Logger = Utils.getLogger(OpcUaServerExtension::class.java)
        const val DEVICE_TYPE = "OPCUA-Server"
    }

    private val runningServers = ConcurrentHashMap<String, OpcUaServerInstance>()
    private val serverStatuses = ConcurrentHashMap<String, OpcUaServerStatus>()
    private lateinit var currentNodeId: String

    override fun start(startPromise: Promise<Void>) {
        currentNodeId = Monster.getClusterNodeId(vertx)
        logger.info("Starting OPC UA Server Extension on node: $currentNodeId")

        // Register message bus handlers
        registerMessageHandlers()

        // Load and start configured servers from device store
        loadConfiguredServers()

        startPromise.complete()
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Stopping OPC UA Server Extension")

        // Stop all running servers
        runningServers.forEach { (name, instance) ->
            try {
                instance.stop()
                logger.info("Stopped OPC UA Server: $name")
            } catch (e: Exception) {
                logger.severe("Failed to stop OPC UA Server $name: ${e.message}")
            }
        }
        runningServers.clear()
        serverStatuses.clear()

        stopPromise.complete()
    }

    /**
     * Register message bus handlers for control commands
     */
    private fun registerMessageHandlers() {
        // Handler for control commands targeted to this node or all nodes
        val controlAddress = "opcua.server.control.$currentNodeId"
        val allNodesAddress = "opcua.server.control.*"

        vertx.eventBus().consumer<JsonObject>(controlAddress) { message ->
            handleControlCommand(message)
        }

        vertx.eventBus().consumer<JsonObject>(allNodesAddress) { message ->
            handleControlCommand(message)
        }

        // Handler for status requests
        val statusAddress = "opcua.server.status.$currentNodeId"
        vertx.eventBus().consumer<JsonObject>(statusAddress) { message ->
            handleStatusRequest(message)
        }

        logger.info("Registered message bus handlers for OPC UA Server control")
    }

    /**
     * Handle control commands from message bus
     */
    private fun handleControlCommand(message: Message<JsonObject>) {
        try {
            val command = OpcUaServerCommand.fromJsonObject(message.body())
            logger.info("Received control command: ${command.action} for server: ${command.serverName}")

            // Check if this command is for this node
            if (command.nodeId != "*" && command.nodeId != currentNodeId) {
                // Not for this node
                return
            }

            val result = when (command.action) {
                OpcUaServerCommand.Action.START -> {
                    command.config?.let { startServer(it) }
                        ?: JsonObject().put("error", "No configuration provided")
                }

                OpcUaServerCommand.Action.STOP -> {
                    stopServer(command.serverName)
                }

                OpcUaServerCommand.Action.UPDATE_CONFIG -> {
                    command.config?.let { updateServerConfig(command.serverName, it) }
                        ?: JsonObject().put("error", "No configuration provided")
                }

                OpcUaServerCommand.Action.GET_STATUS -> {
                    getServerStatus(command.serverName)
                }
            }

            message.reply(result)

        } catch (e: Exception) {
            logger.severe("Error handling control command: ${e.message}")
            message.fail(500, e.message ?: "Internal error")
        }
    }

    /**
     * Handle status request
     */
    private fun handleStatusRequest(message: Message<JsonObject>) {
        try {
            val serverName = message.body().getString("serverName")

            val status = if (serverName != null) {
                getServerStatus(serverName)
            } else {
                // Return all server statuses
                JsonObject().apply {
                    serverStatuses.forEach { (name, status) ->
                        put(name, status.toJsonObject())
                    }
                }
            }

            message.reply(status)

        } catch (e: Exception) {
            logger.severe("Error handling status request: ${e.message}")
            message.fail(500, e.message ?: "Internal error")
        }
    }

    /**
     * Load configured servers from device store
     */
    private fun loadConfiguredServers() {
        if (deviceConfigStore == null) {
            logger.warning("No device config store available, skipping server configuration loading")
            return
        }

        deviceConfigStore.getAllDevices().onComplete { result ->
            if (result.succeeded()) {
                try {
                    // Get all OPC UA Server configurations for this node
                    val configs = result.result()
                        .filter { device ->
                            device.type == DEVICE_TYPE && (device.nodeId == "*" || device.nodeId == currentNodeId)
                        }

                    configs.forEach { device ->
                        try {
                            // Parse the configuration
                            val configJson = JsonObject(device.config.toJsonObject().toString())

                            // Check if we have the full OPC UA server config stored
                            val serverConfig = if (configJson.containsKey("opcUaServerConfig")) {
                                // Use the stored OPC UA server configuration
                                OpcUaServerConfig.fromJsonObject(configJson.getJsonObject("opcUaServerConfig"))
                            } else {
                                // Legacy fallback - reconstruct from device config
                                configJson.apply {
                                    put("name", device.name)
                                    put("namespace", device.namespace)
                                    put("nodeId", device.nodeId)
                                    put("enabled", device.enabled)
                                }
                                OpcUaServerConfig.fromJsonObject(configJson)
                            }

                            if (serverConfig.enabled) {
                                startServerInternal(serverConfig)
                            }

                        } catch (e: Exception) {
                            logger.severe("Failed to load OPC UA Server configuration ${device.name}: ${e.message}")
                        }
                    }

                    logger.info("Loaded ${runningServers.size} OPC UA Server configurations")

                } catch (e: Exception) {
                    logger.severe("Failed to load OPC UA Server configurations: ${e.message}")
                }
            } else {
                logger.severe("Failed to load device configurations: ${result.cause()?.message}")
            }
        }
    }

    /**
     * Start an OPC UA Server
     */
    private fun startServer(config: OpcUaServerConfig, skipSave: Boolean = false): JsonObject {
        return try {
            // Check if server is already running
            if (runningServers.containsKey(config.name)) {
                JsonObject()
                    .put("success", false)
                    .put("error", "Server '${config.name}' is already running")
            } else {
                val status = startServerInternal(config, skipSave)
                JsonObject()
                    .put("success", status.status == OpcUaServerStatus.Status.RUNNING)
                    .put("status", status.toJsonObject())
            }
        } catch (e: Exception) {
            logger.severe("Failed to start OPC UA Server '${config.name}': ${e.message}")
            JsonObject()
                .put("success", false)
                .put("error", e.message)
        }
    }

    /**
     * Internal method to start a server
     */
    private fun startServerInternal(config: OpcUaServerConfig, skipSave: Boolean = false): OpcUaServerStatus {
        val instance = OpcUaServerInstance(
            config,
            vertx,
            sessionHandler,
            userManager
        )

        val status = instance.start()

        if (status.status == OpcUaServerStatus.Status.RUNNING) {
            runningServers[config.name] = instance
            serverStatuses[config.name] = status

            // Publish status update
            publishStatusUpdate(status)

            // Save to device store if available (unless explicitly skipped)
            if (!skipSave) {
                saveServerConfig(config)
            }
        }

        return status
    }

    /**
     * Stop an OPC UA Server
     */
    private fun stopServer(serverName: String): JsonObject {
        return try {
            val instance = runningServers.remove(serverName)
            if (instance != null) {
                val status = instance.stop()
                serverStatuses[serverName] = status

                // Publish status update
                publishStatusUpdate(status)

                JsonObject()
                    .put("success", true)
                    .put("status", status.toJsonObject())
            } else {
                JsonObject()
                    .put("success", false)
                    .put("error", "Server '$serverName' is not running on this node")
            }
        } catch (e: Exception) {
            logger.severe("Failed to stop OPC UA Server '$serverName': ${e.message}")
            JsonObject()
                .put("success", false)
                .put("error", e.message)
        }
    }

    /**
     * Update server configuration
     */
    private fun updateServerConfig(serverName: String, newConfig: OpcUaServerConfig): JsonObject {
        return try {
            // Stop existing server if running
            val wasRunning = runningServers.containsKey(serverName)
            if (wasRunning) {
                stopServer(serverName)
            }

            // Don't save here - the GraphQL mutation already saved to the database
            // Just restart with the new configuration if it was running and is enabled
            if (wasRunning && newConfig.enabled) {
                startServer(newConfig, skipSave = true)
            } else {
                JsonObject()
                    .put("success", true)
                    .put("message", "Configuration updated")
            }
        } catch (e: Exception) {
            logger.severe("Failed to update OPC UA Server configuration '$serverName': ${e.message}")
            JsonObject()
                .put("success", false)
                .put("error", e.message)
        }
    }

    /**
     * Get server status
     */
    private fun getServerStatus(serverName: String): JsonObject {
        val instance = runningServers[serverName]
        val status = instance?.getStatus() ?: serverStatuses[serverName]

        return if (status != null) {
            JsonObject()
                .put("success", true)
                .put("status", status.toJsonObject())
        } else {
            JsonObject()
                .put("success", false)
                .put("error", "Server '$serverName' not found on this node")
        }
    }

    /**
     * Save server configuration to device store
     */
    private fun saveServerConfig(config: OpcUaServerConfig) {
        deviceConfigStore?.let { store ->
            try {
                // Store the full OPC UA server configuration in the config field as JSON
                val serverConfigJson = config.toJsonObject()

                // Convert server addresses to OpcUaAddress format for storage in the addresses field
                val opcUaAddresses = config.addresses.map { serverAddress ->
                    at.rocworks.stores.OpcUaAddress(
                        address = serverAddress.mqttTopic,
                        topic = serverAddress.mqttTopic,
                        publishMode = "SEPARATE",
                        removePath = false
                    )
                }

                // Create a wrapper OpcUaConnectionConfig that contains the full server config
                val connectionConfig = at.rocworks.stores.OpcUaConnectionConfig(
                    endpointUrl = "opc.tcp://localhost:${config.port}/${config.path}",
                    updateEndpointUrl = false,
                    securityPolicy = "None",
                    username = null,
                    password = null,
                    subscriptionSamplingInterval = 0.0,
                    keepAliveFailuresAllowed = 3,
                    reconnectDelay = 5000L,
                    connectionTimeout = 10000L,
                    requestTimeout = 5000L,
                    monitoringParameters = at.rocworks.stores.MonitoringParameters(),
                    addresses = opcUaAddresses, // Store server addresses here for visibility
                    certificateConfig = at.rocworks.stores.CertificateConfig()
                )

                // Add the full server config to the connection config's JSON representation
                val configJsonObject = connectionConfig.toJsonObject()
                configJsonObject.put("opcUaServerConfig", serverConfigJson)

                val deviceConfig = at.rocworks.stores.DeviceConfig(
                    name = config.name,
                    namespace = config.namespace,
                    nodeId = config.nodeId,
                    config = at.rocworks.stores.OpcUaConnectionConfig.fromJsonObject(configJsonObject),
                    enabled = config.enabled,
                    type = DEVICE_TYPE
                )

                // Save to store
                store.saveDevice(deviceConfig).onComplete { result ->
                    if (result.succeeded()) {
                        logger.fine("Saved OPC UA Server configuration for '${config.name}'")
                    } else {
                        logger.warning("Failed to save OPC UA Server configuration: ${result.cause()?.message}")
                    }
                }

            } catch (e: Exception) {
                logger.warning("Failed to save OPC UA Server configuration: ${e.message}")
            }
        }
    }

    /**
     * Publish status update to message bus
     */
    private fun publishStatusUpdate(status: OpcUaServerStatus) {
        val statusAddress = "opcua.server.status.update.${status.serverName}"
        vertx.eventBus().publish(statusAddress, status.toJsonObject())
    }
}

