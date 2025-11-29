package at.rocworks.devices.opcuaserver

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.auth.UserManager
// Removed - we'll define our own EventBusAddresses
import at.rocworks.handlers.SessionHandler
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonArray
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
    }

    private val runningServers = ConcurrentHashMap<String, OpcUaServerInstance>()
    private val serverStatuses = ConcurrentHashMap<String, OpcUaServerStatus>()
    private lateinit var currentNodeId: String

    override fun start(startPromise: Promise<Void>) {
        currentNodeId = Monster.getClusterNodeId(vertx)
        logger.fine("Starting OPC UA Server Extension on node: $currentNodeId")

        // Register message bus handlers
        registerMessageHandlers()

        // Load and start configured servers from device store
        loadConfiguredServers()

        startPromise.complete()
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.fine("Stopping OPC UA Server Extension")

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
    }

    /**
     * Handle control commands from message bus
     */
    private fun handleControlCommand(message: Message<JsonObject>) {
        try {
            val command = OpcUaServerCommand.fromJsonObject(message.body())
            logger.fine("Received control command: ${command.action} for server: ${command.serverName}")

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
                            device.type == DeviceConfig.DEVICE_TYPE_OPCUA_SERVER && (device.nodeId == "*" || device.nodeId == currentNodeId)
                        }

                    // Start all enabled servers asynchronously
                    configs.forEach { device ->
                        try {
                            // Parse the configuration - the device.config is already a JsonObject
                            val configJson = device.config

                            // Handle both old nested format and new flattened format
                            val serverConfig = if (configJson.containsKey("opcUaServerConfig")) {
                                // Legacy nested format - extract from opcUaServerConfig field
                                val serverConfigJson = configJson.getJsonObject("opcUaServerConfig").copy().apply {
                                    // Add back nodeId and enabled from DeviceConfig
                                    put("nodeId", device.nodeId)
                                    put("enabled", device.enabled)
                                }
                                val config = OpcUaServerConfig.fromJsonObject(serverConfigJson)
                                logger.info("Loaded OPC UA server '${device.name}' (legacy nested format) with ${config.addresses.size} address mappings")
                                config
                            } else {
                                // New flattened format or legacy fallback
                                val serverConfigJson = configJson.copy().apply {
                                    // Add back nodeId and enabled from DeviceConfig
                                    put("name", device.name)
                                    put("namespace", device.namespace)
                                    put("nodeId", device.nodeId)
                                    put("enabled", device.enabled)
                                }

                                val config = OpcUaServerConfig.fromJsonObject(serverConfigJson)
                                logger.info("Loaded OPC UA server '${device.name}' (flattened format) with ${config.addresses.size} address mappings")
                                config
                            }

                            if (serverConfig.enabled) {
                                // Start server asynchronously without blocking
                                startServerAsync(serverConfig, skipSave = true)
                            }

                        } catch (e: Exception) {
                            logger.severe("Failed to load OPC UA Server configuration ${device.name}: ${e.message}")
                        }
                    }
                } catch (e: Exception) {
                    logger.severe("Failed to load OPC UA Server configurations: ${e.message}")
                }
            } else {
                logger.severe("Failed to load device configurations: ${result.cause()?.message}")
            }
        }
    }

    /**
     * Start an OPC UA Server (async handler for message bus commands)
     */
    private fun startServer(config: OpcUaServerConfig, skipSave: Boolean = false): JsonObject {
        // Check if server is already running
        if (runningServers.containsKey(config.name)) {
            return JsonObject()
                .put("success", false)
                .put("error", "Server '${config.name}' is already running")
        }

        // Start server asynchronously
        startServerAsync(config, skipSave)

        // Return immediate response indicating startup is in progress
        return JsonObject()
            .put("success", true)
            .put("message", "Server '${config.name}' startup initiated")
    }

    /**
     * Asynchronously start a server without blocking the event loop
     */
    private fun startServerAsync(config: OpcUaServerConfig, skipSave: Boolean = false) {
        val instance = OpcUaServerInstance(
            config,
            vertx,
            sessionHandler,
            userManager
        )

        // Start server in blocking thread to avoid blocking event loop
        vertx.executeBlocking<OpcUaServerStatus> {
            try {
                instance.start()
            } catch (e: Exception) {
                logger.severe("Failed to start OPC UA Server '${config.name}' in blocking thread: ${e.message}")
                OpcUaServerStatus(
                    serverName = config.name,
                    nodeId = config.nodeId,
                    status = OpcUaServerStatus.Status.ERROR,
                    port = config.port,
                    error = e.message
                )
            }
        }.onComplete { asyncResult ->
            // Handle completion asynchronously without blocking
            if (asyncResult.succeeded()) {
                val status = asyncResult.result()
                logger.info("OPC UA Server '${config.name}' startup completed with status: ${status.status}")

                if (status.status == OpcUaServerStatus.Status.RUNNING) {
                    runningServers[config.name] = instance
                    serverStatuses[config.name] = status

                    // Publish status update
                    publishStatusUpdate(status)

                    // Save to device store if available (unless explicitly skipped)
                    if (!skipSave) {
                        saveServerConfig(config)
                    }
                } else {
                    logger.warning("OPC UA Server '${config.name}' failed to start: ${status.error}")
                    serverStatuses[config.name] = status
                    publishStatusUpdate(status)
                }
            } else {
                logger.severe("OPC UA Server '${config.name}' startup failed: ${asyncResult.cause()?.message}")
                val status = OpcUaServerStatus(
                    serverName = config.name,
                    nodeId = config.nodeId,
                    status = OpcUaServerStatus.Status.ERROR,
                    port = config.port,
                    error = asyncResult.cause()?.message
                )
                serverStatuses[config.name] = status
                publishStatusUpdate(status)
            }
        }
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
     * Update server configuration without restarting
     */
    private fun updateServerConfig(serverName: String, newConfig: OpcUaServerConfig): JsonObject {
        return try {
            val runningServer = runningServers[serverName]

            if (runningServer != null) {
                // Server is running - update configuration dynamically
                logger.fine("Dynamically updating OPC UA server '$serverName' configuration")

                // Update the server's configuration in memory
                runningServer.updateConfiguration(newConfig)

                JsonObject()
                    .put("success", true)
                    .put("message", "Configuration updated dynamically")
            } else if (newConfig.enabled) {
                // Server not running but should be enabled - start it asynchronously
                startServerAsync(newConfig, skipSave = true)
                JsonObject()
                    .put("success", true)
                    .put("message", "Server startup initiated")
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
                // Create a flattened config structure with all OPC UA server fields
                // Remove nodeId and enabled (stored in DeviceConfig table directly)
                val configJson = config.toJsonObject().apply {
                    remove("nodeId")
                    remove("enabled")
                }

                val deviceConfig = at.rocworks.stores.DeviceConfig(
                    name = config.name,
                    namespace = config.namespace,
                    nodeId = config.nodeId,
                    config = configJson,
                    enabled = config.enabled,
                    type = DeviceConfig.DEVICE_TYPE_OPCUA_SERVER
                )

                // Save to store
                store.saveDevice(deviceConfig).onComplete { result ->
                    if (result.succeeded()) {
                        logger.fine { "Saved OPC UA Server configuration for '${config.name}'" }
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

