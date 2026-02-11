package at.rocworks.graphql

import at.rocworks.devices.opcuaserver.*
import at.rocworks.extensions.graphql.GraphQLAuthContext
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.DeviceConfig
import at.rocworks.Monster
import at.rocworks.Utils
import graphql.schema.DataFetcher
import graphql.GraphQLException
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL mutation resolvers for OPC UA Server management
 */
class OpcUaServerMutations(
    private val vertx: Vertx,
    private val deviceConfigStore: IDeviceConfigStore
) {
    companion object {
        private val logger: Logger = Utils.getLogger(OpcUaServerMutations::class.java)
        private const val DEVICE_TYPE = "OPCUA-Server"
    }

    private val currentNodeId = Monster.getClusterNodeId(vertx)
    private val certificateManager = OpcUaServerCertificateManager()

    /**
     * Create a new OPC UA Server configuration
     */
    fun createOpcUaServer(): DataFetcher<CompletableFuture<OpcUaServerResult>> {
        return DataFetcher { env ->
            val future = CompletableFuture<OpcUaServerResult>()

            try {
                val config = env.getArgument<Map<String, Any>>("config")
                    ?: throw IllegalArgumentException("Config is required")

                val serverConfig = parseServerConfig(config)

                // Validate configuration
                validateServerConfig(serverConfig)

                // Convert to DeviceConfig format
                val deviceConfig = convertToDeviceConfig(serverConfig)

                // Save to device store
                deviceConfigStore.saveDevice(deviceConfig).onComplete { result ->
                    if (result.succeeded()) {
                        // Send command to start server if enabled
                        if (serverConfig.enabled) {
                            startServerCommand(serverConfig) { success, message ->
                                future.complete(OpcUaServerResult(
                                    success = success,
                                    message = message,
                                    server = if (success) convertToOpcUaServerInfo(deviceConfig) else null,
                                    errors = if (!success && message != null) listOf(message) else emptyList()
                                ))
                            }
                        } else {
                            future.complete(OpcUaServerResult(
                                success = true,
                                message = "OPC UA Server configuration created successfully",
                                server = convertToOpcUaServerInfo(deviceConfig),
                                errors = emptyList()
                            ))
                        }
                    } else {
                        logger.severe("Failed to save OPC UA Server configuration: ${result.cause()?.message}")
                        future.complete(OpcUaServerResult(
                            success = false,
                            message = "Failed to save server configuration",
                            server = null,
                            errors = listOf(result.cause()?.message ?: "Unknown error")
                        ))
                    }
                }

            } catch (e: Exception) {
                logger.severe("Error creating OPC UA Server: ${e.message}")
                future.complete(OpcUaServerResult(
                    success = false,
                    message = "Failed to create server",
                    server = null,
                    errors = listOf(e.message ?: "Unknown error")
                ))
            }

            future
        }
    }

    /**
     * Update an existing OPC UA Server configuration
     */
    fun updateOpcUaServer(): DataFetcher<CompletableFuture<OpcUaServerResult>> {
        return DataFetcher { env ->
            val future = CompletableFuture<OpcUaServerResult>()

            try {
                val serverName = env.getArgument<String>("name")
                    ?: throw IllegalArgumentException("Server name is required")

                val inputMap = env.getArgument<Map<String, Any>>("input")
                    ?: throw IllegalArgumentException("Input is required")

                // Get existing server configuration
                deviceConfigStore.getDevice(serverName).onComplete { getResult ->
                    if (getResult.succeeded() && getResult.result()?.type == DEVICE_TYPE) {
                        try {
                            val existingDeviceConfig = getResult.result()!!

                            // Parse the input server config
                            val serverConfig = parseServerInputForUpdate(inputMap, existingDeviceConfig)

                            // Validate configuration
                            validateServerConfig(serverConfig)

                            // Convert to DeviceConfig format
                            val updatedDeviceConfig = convertToDeviceConfig(serverConfig).copy(
                                createdAt = existingDeviceConfig.createdAt // Preserve creation time
                            )

                            // Check if enabled status changed
                            val wasEnabled = existingDeviceConfig.enabled
                            val isEnabled = updatedDeviceConfig.enabled

                            // Save updated configuration
                            deviceConfigStore.saveDevice(updatedDeviceConfig).onComplete { saveResult ->
                                if (saveResult.succeeded()) {
                                    // Handle server state transitions
                                    if (wasEnabled && !isEnabled) {
                                        // Was enabled, now disabled - stop the server
                                        stopServerCommand(serverName, serverConfig.nodeId) { success, message ->
                                            future.complete(OpcUaServerResult(
                                                success = true,
                                                message = "OPC UA Server updated and stopped successfully",
                                                server = convertToOpcUaServerInfo(updatedDeviceConfig),
                                                errors = if (!success && message != null) listOf("Warning: " + message) else emptyList()
                                            ))
                                        }
                                    } else if (!wasEnabled && isEnabled) {
                                        // Was disabled, now enabled - start the server
                                        startServerCommand(serverConfig) { success, message ->
                                            future.complete(OpcUaServerResult(
                                                success = success,
                                                message = if (success) "OPC UA Server updated and started successfully" else message,
                                                server = if (success) convertToOpcUaServerInfo(updatedDeviceConfig) else null,
                                                errors = if (!success && message != null) listOf(message) else emptyList()
                                            ))
                                        }
                                    } else if (isEnabled) {
                                        // Stays enabled - send update command
                                        updateServerCommand(serverConfig) { success, message ->
                                            future.complete(OpcUaServerResult(
                                                success = success,
                                                message = if (success) "OPC UA Server updated successfully" else message,
                                                server = if (success) convertToOpcUaServerInfo(updatedDeviceConfig) else null,
                                                errors = if (!success && message != null) listOf(message) else emptyList()
                                            ))
                                        }
                                    } else {
                                        // Stays disabled - just update config
                                        future.complete(OpcUaServerResult(
                                            success = true,
                                            message = "OPC UA Server configuration updated successfully",
                                            server = convertToOpcUaServerInfo(updatedDeviceConfig),
                                            errors = emptyList()
                                        ))
                                    }
                                } else {
                                    logger.severe("Failed to save updated OPC UA Server configuration: ${saveResult.cause()?.message}")
                                    future.complete(OpcUaServerResult(
                                        success = false,
                                        message = "Failed to save server configuration",
                                        server = null,
                                        errors = listOf(saveResult.cause()?.message ?: "Unknown error")
                                    ))
                                }
                            }
                        } catch (e: Exception) {
                            logger.severe("Error updating OPC UA Server $serverName: ${e.message}")
                            future.complete(OpcUaServerResult(
                                success = false,
                                message = "Failed to update server",
                                server = null,
                                errors = listOf(e.message ?: "Unknown error")
                            ))
                        }
                    } else {
                        future.complete(OpcUaServerResult(
                            success = false,
                            message = "Server '$serverName' not found",
                            server = null,
                            errors = listOf("Server not found")
                        ))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error in updateOpcUaServer: ${e.message}")
                future.complete(OpcUaServerResult(
                    success = false,
                    message = "Failed to update server",
                    server = null,
                    errors = listOf(e.message ?: "Unknown error")
                ))
            }

            future
        }
    }

    /**
     * Start an OPC UA Server
     */
    fun startOpcUaServer(): DataFetcher<CompletableFuture<OpcUaServerOperationResult>> {
        return DataFetcher { env ->
            val future = CompletableFuture<OpcUaServerOperationResult>()

            val serverName = env.getArgument<String>("serverName")
                ?: return@DataFetcher future.apply {
                    complete(OpcUaServerOperationResult(false, "Server name is required"))
                }

            val nodeId = env.getArgument<String>("nodeId") ?: "*"

            // Get server configuration
            deviceConfigStore.getDevice(serverName).onComplete { result ->
                if (result.succeeded() && result.result()?.type == DEVICE_TYPE) {
                    try {
                        val deviceConfig = result.result()!!
                        val configJson = deviceConfig.config

                        // Load server config using helper method (handles both old and new formats)
                        val serverConfig = loadServerConfigFromDeviceConfig(deviceConfig).copy(enabled = true) // Enable when starting

                        // Update enabled status in store
                        val updatedDeviceConfig = deviceConfig.copy(enabled = true)
                        deviceConfigStore.saveDevice(updatedDeviceConfig).onComplete { saveResult ->
                            if (saveResult.succeeded()) {
                                // Send start command
                                startServerCommand(serverConfig) { success, message ->
                                    future.complete(OpcUaServerOperationResult(success, message))
                                }
                            } else {
                                future.complete(OpcUaServerOperationResult(
                                    false,
                                    "Failed to update server configuration: ${saveResult.cause()?.message}"
                                ))
                            }
                        }

                    } catch (e: Exception) {
                        logger.severe("Error starting OPC UA Server $serverName: ${e.message}")
                        future.complete(OpcUaServerOperationResult(false, e.message))
                    }
                } else {
                    future.complete(OpcUaServerOperationResult(false, "Server '$serverName' not found"))
                }
            }

            future
        }
    }

    /**
     * Stop an OPC UA Server
     */
    fun stopOpcUaServer(): DataFetcher<CompletableFuture<OpcUaServerOperationResult>> {
        return DataFetcher { env ->
            val future = CompletableFuture<OpcUaServerOperationResult>()

            val serverName = env.getArgument<String>("serverName")
                ?: return@DataFetcher future.apply {
                    complete(OpcUaServerOperationResult(false, "Server name is required"))
                }

            val nodeId = env.getArgument<String>("nodeId") ?: "*"

            // Send stop command
            stopServerCommand(serverName, nodeId) { success, message ->
                if (success) {
                    // Update enabled status in store
                    deviceConfigStore.getDevice(serverName).onComplete { result ->
                        if (result.succeeded() && result.result()?.type == DEVICE_TYPE) {
                            val deviceConfig = result.result()!!.copy(enabled = false)
                            deviceConfigStore.saveDevice(deviceConfig).onComplete { saveResult ->
                                if (saveResult.failed()) {
                                    logger.warning("Failed to update server enabled status: ${saveResult.cause()?.message}")
                                }
                            }
                        }
                    }
                }
                future.complete(OpcUaServerOperationResult(success, message))
            }

            future
        }
    }

    /**
     * Delete an OPC UA Server configuration
     */
    fun deleteOpcUaServer(): DataFetcher<CompletableFuture<OpcUaServerOperationResult>> {
        return DataFetcher { env ->
            val future = CompletableFuture<OpcUaServerOperationResult>()

            val serverName = env.getArgument<String>("serverName")
                ?: return@DataFetcher future.apply {
                    complete(OpcUaServerOperationResult(false, "Server name is required"))
                }

            // First stop the server if it's running
            stopServerCommand(serverName, "*") { _, _ ->
                // Then delete the configuration
                deviceConfigStore.deleteDevice(serverName).onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(OpcUaServerOperationResult(
                            true,
                            "Server '$serverName' deleted successfully"
                        ))
                    } else {
                        logger.severe("Failed to delete OPC UA Server $serverName: ${result.cause()?.message}")
                        future.complete(OpcUaServerOperationResult(
                            false,
                            "Failed to delete server: ${result.cause()?.message}"
                        ))
                    }
                }
            }

            future
        }
    }

    /**
     * Add an address mapping to an existing OPC UA Server
     */
    fun addOpcUaServerAddress(): DataFetcher<CompletableFuture<OpcUaServerOperationResult>> {
        return DataFetcher { env ->
            val future = CompletableFuture<OpcUaServerOperationResult>()

            val serverName = env.getArgument<String>("serverName")
                ?: return@DataFetcher future.apply {
                    complete(OpcUaServerOperationResult(false, "Server name is required"))
                }

            val addressInput = env.getArgument<Map<String, Any>>("address")
                ?: return@DataFetcher future.apply {
                    complete(OpcUaServerOperationResult(false, "Address is required"))
                }

            // Get current server configuration
            deviceConfigStore.getDevice(serverName).onComplete { result ->
                if (result.succeeded() && result.result()?.type == DEVICE_TYPE) {
                    try {
                        val deviceConfig = result.result()!!
                        val configJson = deviceConfig.config

                        // Load server config using helper method (handles both old and new formats)
                        val serverConfig = loadServerConfigFromDeviceConfig(deviceConfig)

                        // Parse new address
                        val newAddress = parseOpcUaServerAddress(addressInput)

                        // Add the new address to the list
                        val updatedAddresses = serverConfig.addresses + newAddress
                        val updatedConfig = serverConfig.copy(addresses = updatedAddresses)

                        // Convert back to DeviceConfig and save
                        val updatedDeviceConfig = convertToDeviceConfig(updatedConfig)
                        deviceConfigStore.saveDevice(updatedDeviceConfig).onComplete { saveResult ->
                            if (saveResult.succeeded()) {
                                // If server is running, send update command
                                if (deviceConfig.enabled) {
                                    updateServerCommand(updatedConfig) { success, message ->
                                        future.complete(OpcUaServerOperationResult(success, message))
                                    }
                                } else {
                                    future.complete(OpcUaServerOperationResult(
                                        true,
                                        "Address added successfully to server '$serverName'"
                                    ))
                                }
                            } else {
                                future.complete(OpcUaServerOperationResult(
                                    false,
                                    "Failed to save configuration: ${saveResult.cause()?.message}"
                                ))
                            }
                        }

                    } catch (e: Exception) {
                        logger.severe("Error adding address to OPC UA Server $serverName: ${e.message}")
                        future.complete(OpcUaServerOperationResult(false, e.message))
                    }
                } else {
                    future.complete(OpcUaServerOperationResult(false, "Server '$serverName' not found"))
                }
            }

            future
        }
    }

    /**
     * Remove an address mapping from an existing OPC UA Server
     */
    fun removeOpcUaServerAddress(): DataFetcher<CompletableFuture<OpcUaServerOperationResult>> {
        return DataFetcher { env ->
            val future = CompletableFuture<OpcUaServerOperationResult>()

            val serverName = env.getArgument<String>("serverName")
                ?: return@DataFetcher future.apply {
                    complete(OpcUaServerOperationResult(false, "Server name is required"))
                }

            val mqttTopic = env.getArgument<String>("mqttTopic")
                ?: return@DataFetcher future.apply {
                    complete(OpcUaServerOperationResult(false, "MQTT topic is required"))
                }

            // Get current server configuration
            deviceConfigStore.getDevice(serverName).onComplete { result ->
                if (result.succeeded() && result.result()?.type == DEVICE_TYPE) {
                    try {
                        val deviceConfig = result.result()!!
                        val configJson = deviceConfig.config

                        // Load server config using helper method (handles both old and new formats)
                        val serverConfig = loadServerConfigFromDeviceConfig(deviceConfig)

                        // Remove the address with matching MQTT topic
                        val updatedAddresses = serverConfig.addresses.filter { it.mqttTopic != mqttTopic }

                        if (updatedAddresses.size == serverConfig.addresses.size) {
                            future.complete(OpcUaServerOperationResult(
                                false,
                                "Address with MQTT topic '$mqttTopic' not found"
                            ))
                            return@onComplete
                        }

                        val updatedConfig = serverConfig.copy(addresses = updatedAddresses)

                        // Convert back to DeviceConfig and save
                        val updatedDeviceConfig = convertToDeviceConfig(updatedConfig)
                        deviceConfigStore.saveDevice(updatedDeviceConfig).onComplete { saveResult ->
                            if (saveResult.succeeded()) {
                                // If server is running, send update command
                                if (deviceConfig.enabled) {
                                    updateServerCommand(updatedConfig) { success, message ->
                                        future.complete(OpcUaServerOperationResult(success, message))
                                    }
                                } else {
                                    future.complete(OpcUaServerOperationResult(
                                        true,
                                        "Address removed successfully from server '$serverName'"
                                    ))
                                }
                            } else {
                                future.complete(OpcUaServerOperationResult(
                                    false,
                                    "Failed to save configuration: ${saveResult.cause()?.message}"
                                ))
                            }
                        }

                    } catch (e: Exception) {
                        logger.severe("Error removing address from OPC UA Server $serverName: ${e.message}")
                        future.complete(OpcUaServerOperationResult(false, e.message))
                    }
                } else {
                    future.complete(OpcUaServerOperationResult(false, "Server '$serverName' not found"))
                }
            }

            future
        }
    }

    /**
     * Parse server configuration from GraphQL input
     */
    private fun parseServerConfig(config: Map<String, Any>): OpcUaServerConfig {
        // Use default addresses and security for simplified configuration
        val addresses = emptyList<OpcUaServerAddress>()
        val security = OpcUaServerSecurity()

        return OpcUaServerConfig(
            name = config["name"] as? String ?: throw IllegalArgumentException("Name is required"),
            namespace = config["namespace"] as? String ?: throw IllegalArgumentException("Namespace is required"),
            nodeId = config["nodeId"] as? String ?: throw IllegalArgumentException("NodeId is required"),
            enabled = config["enabled"] as? Boolean ?: true,
            port = (config["port"] as? Number)?.toInt() ?: 4840,
            path = config["path"] as? String ?: "monstermq",
            hostname = (config["hostname"] as? String)?.takeIf { it.isNotBlank() },
            bindAddress = (config["bindAddress"] as? String)?.takeIf { it.isNotBlank() },
            namespaceIndex = (config["namespaceIndex"] as? Number)?.toInt() ?: 1,
            namespaceUri = config["namespaceUri"] as? String ?: "urn:monstermq:opcua:${config["name"]}",
            addresses = addresses,
            security = security,
            bufferSize = (config["bufferSize"] as? Number)?.toInt() ?: 1000,
            updateInterval = (config["updateInterval"] as? Number)?.toLong() ?: 1000L
        )
    }

    /**
     * Parse server configuration from GraphQL input for update operations
     * This preserves existing addresses from the stored configuration
     */
    private fun parseServerInputForUpdate(input: Map<String, Any>, existingDeviceConfig: DeviceConfig): OpcUaServerConfig {
        // Load existing server config to get current addresses
        val existingConfig = loadServerConfigFromDeviceConfig(existingDeviceConfig)

        // Parse addresses from input if provided, otherwise keep existing addresses
        @Suppress("UNCHECKED_CAST")
        val addresses = (input["addresses"] as? List<Map<String, Any>>)?.map { addrInput ->
            parseOpcUaServerAddress(addrInput)
        } ?: existingConfig.addresses

        // Parse security from input if provided, otherwise keep existing security
        @Suppress("UNCHECKED_CAST")
        val security = (input["security"] as? Map<String, Any>)?.let { secInput ->
            @Suppress("UNCHECKED_CAST")
            val securityPolicies = (secInput["securityPolicies"] as? List<String>) ?: listOf("None")
            OpcUaServerSecurity(
                keystorePath = secInput["keystorePath"] as? String ?: "server-keystore.jks",
                keystorePassword = secInput["keystorePassword"] as? String ?: "password",
                certificateAlias = secInput["certificateAlias"] as? String ?: "server-cert",
                securityPolicies = securityPolicies,
                allowAnonymous = secInput["allowAnonymous"] as? Boolean ?: true,
                requireAuthentication = secInput["requireAuthentication"] as? Boolean ?: false
            )
        } ?: existingConfig.security

        return OpcUaServerConfig(
            name = input["name"] as? String ?: existingConfig.name,
            namespace = input["namespace"] as? String ?: existingConfig.namespace,
            nodeId = input["nodeId"] as? String ?: existingConfig.nodeId,
            enabled = input["enabled"] as? Boolean ?: existingConfig.enabled,
            port = (input["port"] as? Number)?.toInt() ?: existingConfig.port,
            path = input["path"] as? String ?: existingConfig.path,
            hostname = if (input.containsKey("hostname")) (input["hostname"] as? String)?.takeIf { it.isNotBlank() } else existingConfig.hostname,
            bindAddress = if (input.containsKey("bindAddress")) (input["bindAddress"] as? String)?.takeIf { it.isNotBlank() } else existingConfig.bindAddress,
            namespaceIndex = (input["namespaceIndex"] as? Number)?.toInt() ?: existingConfig.namespaceIndex,
            namespaceUri = input["namespaceUri"] as? String ?: existingConfig.namespaceUri,
            addresses = addresses,
            security = security,
            bufferSize = (input["bufferSize"] as? Number)?.toInt() ?: existingConfig.bufferSize,
            updateInterval = (input["updateInterval"] as? Number)?.toLong() ?: existingConfig.updateInterval
        )
    }

    /**
     * Validate server configuration
     */
    private fun validateServerConfig(config: OpcUaServerConfig) {
        if (config.name.isBlank()) {
            throw IllegalArgumentException("Server name cannot be empty")
        }
        if (config.namespace.isBlank()) {
            throw IllegalArgumentException("Namespace cannot be empty")
        }
        if (config.nodeId.isBlank()) {
            throw IllegalArgumentException("Node ID cannot be empty")
        }
        if (config.port < 1 || config.port > 65535) {
            throw IllegalArgumentException("Port must be between 1 and 65535")
        }
        // Allow empty addresses for initial creation - addresses can be added later

        // Validate addresses if present
        config.addresses.forEach { addr ->
            if (addr.mqttTopic.isBlank()) {
                throw IllegalArgumentException("MQTT topic cannot be empty")
            }
        }
    }

    /**
     * Convert OpcUaServerConfig to DeviceConfig
     */
    private fun convertToDeviceConfig(serverConfig: OpcUaServerConfig): DeviceConfig {
        // Create a flattened config structure with all OPC UA server fields
        // Remove nodeId and enabled (stored in DeviceConfig table directly)
        val configJson = serverConfig.toJsonObject().apply {
            remove("nodeId")
            remove("enabled")
        }

        return DeviceConfig(
            name = serverConfig.name,
            namespace = serverConfig.namespace,
            nodeId = serverConfig.nodeId,
            config = configJson,
            enabled = serverConfig.enabled,
            type = DEVICE_TYPE
        )
    }

    /**
     * Convert DeviceConfig to OpcUaServerInfo
     */
    private fun convertToOpcUaServerInfo(deviceConfig: DeviceConfig): OpcUaServerInfo {
        // Get the config JSON directly
        val configJson = deviceConfig.config

        // All OPC UA server fields are now flattened to the top level
        return OpcUaServerInfo(
            name = deviceConfig.name,
            namespace = deviceConfig.namespace,
            nodeId = deviceConfig.nodeId,
            enabled = deviceConfig.enabled,
            port = configJson.getInteger("port", 4840),
            path = configJson.getString("path", "server"),
            hostname = configJson.getString("hostname"),
            bindAddress = configJson.getString("bindAddress"),
            namespaceIndex = configJson.getInteger("namespaceIndex", 1),
            namespaceUri = configJson.getString("namespaceUri", "urn:monstermq:opcua:${deviceConfig.name}"),
            addresses = parseAddresses(configJson),
            security = parseSecurity(configJson),
            bufferSize = configJson.getInteger("bufferSize", 1000),
            updateInterval = configJson.getLong("updateInterval", 1000L),
            createdAt = configJson.getString("createdAt") ?: Instant.now().toString(),
            updatedAt = configJson.getString("updatedAt") ?: Instant.now().toString(),
            isOnCurrentNode = deviceConfig.nodeId == "*" || deviceConfig.nodeId == currentNodeId,
            status = null,
            trustedCertificates = emptyList(), // Certificate loading is handled separately in queries
            untrustedCertificates = emptyList() // Certificate loading is handled separately in queries
        )
    }

    /**
     * Extract port from OPC UA endpoint URL
     */
    private fun extractPortFromEndpointUrl(endpointUrl: String): Int {
        return try {
            // Extract port from URL like "opc.tcp://localhost:4841/path"
            val regex = Regex("opc\\.tcp://[^:]+:(\\d+)")
            val match = regex.find(endpointUrl)
            match?.groupValues?.get(1)?.toIntOrNull() ?: 4840
        } catch (e: Exception) {
            4840
        }
    }

    /**
     * Extract path from OPC UA endpoint URL
     */
    private fun extractPathFromEndpointUrl(endpointUrl: String): String {
        return try {
            // Extract path from URL like "opc.tcp://localhost:4841/monstermq"
            val regex = Regex("opc\\.tcp://[^/]+/(.*)")
            val match = regex.find(endpointUrl)
            match?.groupValues?.get(1) ?: "monstermq"
        } catch (e: Exception) {
            "monstermq"
        }
    }

    /**
     * Parse OPC UA Server Address from stored config
     */
    private fun parseAddresses(serverConfigJson: JsonObject): List<OpcUaServerAddressInfo> {
        return serverConfigJson.getJsonArray("addresses", io.vertx.core.json.JsonArray())
            .filterIsInstance<JsonObject>()
            .map { addrJson ->
                OpcUaServerAddressInfo(
                    mqttTopic = addrJson.getString("mqttTopic", ""),
                    displayName = addrJson.getString("displayName")?.takeIf { it.isNotBlank() },
                    browseName = addrJson.getString("browseName"),
                    description = addrJson.getString("description"),
                    dataType = addrJson.getString("dataType", "TEXT"),
                    accessLevel = addrJson.getString("accessLevel", "READ_ONLY"),
                    unit = addrJson.getString("unit")
                )
            }
    }

    /**
     * Parse OPC UA Server Security from stored config
     */
    private fun parseSecurity(serverConfigJson: JsonObject): OpcUaServerSecurityInfo {
        val securityJson = serverConfigJson.getJsonObject("security", JsonObject())
        return OpcUaServerSecurityInfo(
            keystorePath = securityJson.getString("keystorePath", "server-keystore.jks"),
            certificateAlias = securityJson.getString("certificateAlias", "server-cert"),
            securityPolicies = securityJson.getJsonArray("securityPolicies", io.vertx.core.json.JsonArray())
                .filterIsInstance<String>()
                .ifEmpty { listOf("None") },
            allowAnonymous = securityJson.getBoolean("allowAnonymous", true),
            requireAuthentication = securityJson.getBoolean("requireAuthentication", false)
        )
    }

    /**
     * Send start command to OPC UA Server Extension
     */
    private fun startServerCommand(config: OpcUaServerConfig, callback: (Boolean, String?) -> Unit) {
        val command = OpcUaServerCommand(
            action = OpcUaServerCommand.Action.START,
            serverName = config.name,
            nodeId = config.nodeId,
            config = config
        )

        val targetNodeId = if (config.nodeId == "*") currentNodeId else config.nodeId
        val controlAddress = "opcua.server.control.$targetNodeId"

        vertx.eventBus().request<JsonObject>(controlAddress, command.toJsonObject()).onComplete { asyncResult ->
            if (asyncResult.succeeded()) {
                val response = asyncResult.result().body()
                val success = response.getBoolean("success", false)
                val message = if (success) {
                    "Server '${config.name}' started successfully"
                } else {
                    response.getString("error") ?: "Failed to start server"
                }
                callback(success, message)
            } else {
                callback(false, "Failed to communicate with cluster node: ${asyncResult.cause()?.message}")
            }
        }

        // Timeout after 10 seconds
        vertx.setTimer(10000) {
            callback(false, "Timeout waiting for server start command")
        }
    }

    /**
     * Send stop command to OPC UA Server Extension
     */
    private fun stopServerCommand(serverName: String, nodeId: String, callback: (Boolean, String?) -> Unit) {
        val command = OpcUaServerCommand(
            action = OpcUaServerCommand.Action.STOP,
            serverName = serverName,
            nodeId = nodeId
        )

        val targetNodeId = if (nodeId == "*") currentNodeId else nodeId
        val controlAddress = "opcua.server.control.$targetNodeId"

        vertx.eventBus().request<JsonObject>(controlAddress, command.toJsonObject()).onComplete { asyncResult ->
            if (asyncResult.succeeded()) {
                val response = asyncResult.result().body()
                val success = response.getBoolean("success", false)
                val message = if (success) {
                    "Server '$serverName' stopped successfully"
                } else {
                    response.getString("error") ?: "Failed to stop server"
                }
                callback(success, message)
            } else {
                callback(false, "Failed to communicate with cluster node: ${asyncResult.cause()?.message}")
            }
        }

        // Timeout after 10 seconds
        vertx.setTimer(10000) {
            callback(false, "Timeout waiting for server stop command")
        }
    }

    /**
     * Send update command to OPC UA Server Extension
     */
    private fun updateServerCommand(config: OpcUaServerConfig, callback: (Boolean, String?) -> Unit) {
        val command = OpcUaServerCommand(
            action = OpcUaServerCommand.Action.UPDATE_CONFIG,
            serverName = config.name,
            nodeId = config.nodeId,
            config = config
        )

        val targetNodeId = if (config.nodeId == "*") currentNodeId else config.nodeId
        val controlAddress = "opcua.server.control.$targetNodeId"

        vertx.eventBus().request<JsonObject>(controlAddress, command.toJsonObject()).onComplete { asyncResult ->
            if (asyncResult.succeeded()) {
                val response = asyncResult.result().body()
                val success = response.getBoolean("success", false)
                val message = if (success) {
                    "Server '${config.name}' updated successfully"
                } else {
                    response.getString("error") ?: "Failed to update server"
                }
                callback(success, message)
            } else {
                callback(false, "Failed to communicate with cluster node: ${asyncResult.cause()?.message}")
            }
        }

        // Timeout after 10 seconds
        vertx.setTimer(10000) {
            callback(false, "Timeout waiting for server update command")
        }
    }

    /**
     * Load OpcUaServerConfig from DeviceConfig, handling both legacy nested format and new flattened format
     */
    private fun loadServerConfigFromDeviceConfig(deviceConfig: DeviceConfig): OpcUaServerConfig {
        val configJson = deviceConfig.config

        return if (configJson.containsKey("opcUaServerConfig")) {
            // Legacy nested format - extract from opcUaServerConfig field
            val serverConfigJson = configJson.getJsonObject("opcUaServerConfig").apply {
                // Add back nodeId and enabled from DeviceConfig
                put("nodeId", deviceConfig.nodeId)
                put("enabled", deviceConfig.enabled)
            }
            OpcUaServerConfig.fromJsonObject(serverConfigJson)
        } else {
            // New flattened format - all fields are at top level
            configJson.apply {
                // Add back nodeId and enabled from DeviceConfig
                put("name", deviceConfig.name)
                put("namespace", deviceConfig.namespace)
                put("nodeId", deviceConfig.nodeId)
                put("enabled", deviceConfig.enabled)
            }
            OpcUaServerConfig.fromJsonObject(configJson)
        }
    }

    /**
     * Parse OPC UA Server Address from GraphQL input
     */
    private fun parseOpcUaServerAddress(input: Map<String, Any>): OpcUaServerAddress {
        return OpcUaServerAddress(
            mqttTopic = input["mqttTopic"] as? String ?: throw IllegalArgumentException("MQTT topic is required"),
            displayName = (input["displayName"] as? String)?.takeIf { it.isNotBlank() },
            browseName = input["browseName"] as? String,
            description = input["description"] as? String,
            dataType = input["dataType"]?.let {
                OpcUaServerDataType.valueOf(it as String)
            } ?: OpcUaServerDataType.TEXT,
            accessLevel = input["accessLevel"]?.let {
                OpcUaAccessLevel.valueOf(it as String)
            } ?: OpcUaAccessLevel.READ_ONLY,
            unit = input["unit"] as? String
        )
    }

    /**
     * Trust certificates (move from untrusted to trusted directory)
     */
    fun trustOpcUaServerCertificates(): DataFetcher<CompletableFuture<CertificateManagementResultGraphQL>> {
        return DataFetcher { env ->
            val future = CompletableFuture<CertificateManagementResultGraphQL>()
            val serverName = env.getArgument<String>("serverName")
            val fingerprints = env.getArgument<List<String>>("fingerprints")

            if (serverName == null || fingerprints == null || fingerprints.isEmpty()) {
                future.complete(CertificateManagementResultGraphQL(false, "Server name and fingerprints are required", emptyList()))
                return@DataFetcher future
            }

            try {
                // Get server configuration to find security directory
                deviceConfigStore.getDevice(serverName).onComplete { result ->
                    if (result.succeeded() && result.result()?.type == DEVICE_TYPE) {
                        try {
                            val device = result.result()!!
                            val securityDir = device.config.getJsonObject("security", JsonObject())
                                .getString("certificateDir", "./security")

                            val managementResult = certificateManager.trustCertificates(serverName, securityDir, fingerprints)

                            val certificateInfos = managementResult.affectedCertificates.map { cert ->
                                OpcUaServerCertificateInfo(
                                    serverName = cert.serverName,
                                    fingerprint = cert.fingerprint,
                                    subject = cert.subject,
                                    issuer = cert.issuer,
                                    validFrom = cert.validFrom,
                                    validTo = cert.validTo,
                                    trusted = cert.trusted,
                                    filePath = cert.filePath,
                                    firstSeen = cert.firstSeen
                                )
                            }

                            future.complete(CertificateManagementResultGraphQL(
                                success = managementResult.success,
                                message = managementResult.message,
                                affectedCertificates = certificateInfos
                            ))
                        } catch (e: Exception) {
                            logger.severe("Error trusting certificates for server $serverName: ${e.message}")
                            future.complete(CertificateManagementResultGraphQL(false, "Error: ${e.message}", emptyList()))
                        }
                    } else {
                        future.complete(CertificateManagementResultGraphQL(false, "Server not found", emptyList()))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error in trustOpcUaServerCertificates: ${e.message}")
                future.complete(CertificateManagementResultGraphQL(false, "Error: ${e.message}", emptyList()))
            }

            future
        }
    }


    /**
     * Delete certificates completely (from both trusted and untrusted directories)
     */
    fun deleteOpcUaServerCertificates(): DataFetcher<CompletableFuture<CertificateManagementResultGraphQL>> {
        return DataFetcher { env ->
            val future = CompletableFuture<CertificateManagementResultGraphQL>()
            val serverName = env.getArgument<String>("serverName")
            val fingerprints = env.getArgument<List<String>>("fingerprints")

            if (serverName == null || fingerprints == null || fingerprints.isEmpty()) {
                future.complete(CertificateManagementResultGraphQL(false, "Server name and fingerprints are required", emptyList()))
                return@DataFetcher future
            }

            try {
                // Get server configuration to find security directory
                deviceConfigStore.getDevice(serverName).onComplete { result ->
                    if (result.succeeded() && result.result()?.type == DEVICE_TYPE) {
                        try {
                            val device = result.result()!!
                            val securityDir = device.config.getJsonObject("security", JsonObject())
                                .getString("certificateDir", "./security")

                            val managementResult = certificateManager.deleteCertificates(serverName, securityDir, fingerprints)

                            val certificateInfos = managementResult.affectedCertificates.map { cert ->
                                OpcUaServerCertificateInfo(
                                    serverName = cert.serverName,
                                    fingerprint = cert.fingerprint,
                                    subject = cert.subject,
                                    issuer = cert.issuer,
                                    validFrom = cert.validFrom,
                                    validTo = cert.validTo,
                                    trusted = cert.trusted,
                                    filePath = cert.filePath,
                                    firstSeen = cert.firstSeen
                                )
                            }

                            future.complete(CertificateManagementResultGraphQL(
                                success = managementResult.success,
                                message = managementResult.message,
                                affectedCertificates = certificateInfos
                            ))
                        } catch (e: Exception) {
                            logger.severe("Error deleting certificates for server $serverName: ${e.message}")
                            future.complete(CertificateManagementResultGraphQL(false, "Error: ${e.message}", emptyList()))
                        }
                    } else {
                        future.complete(CertificateManagementResultGraphQL(false, "Server not found", emptyList()))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error in deleteOpcUaServerCertificates: ${e.message}")
                future.complete(CertificateManagementResultGraphQL(false, "Error: ${e.message}", emptyList()))
            }

            future
        }
    }
}

/**
 * GraphQL result types for OPC UA Server operations
 */
data class OpcUaServerResult(
    val success: Boolean,
    val message: String?,
    val server: OpcUaServerInfo?,
    val errors: List<String>
)

data class OpcUaServerOperationResult(
    val success: Boolean,
    val message: String?
)

data class CertificateManagementResultGraphQL(
    val success: Boolean,
    val message: String?,
    val affectedCertificates: List<OpcUaServerCertificateInfo>
)