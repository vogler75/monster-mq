package at.rocworks.graphql

import at.rocworks.devices.opcuaserver.*
import at.rocworks.extensions.graphql.GraphQLAuthContext
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.OpcUaConnectionConfig
import at.rocworks.stores.MonitoringParameters
import at.rocworks.stores.CertificateConfig
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
                        val configJson = JsonObject(deviceConfig.config.toJsonObject().toString())

                        // Get the stored OPC UA server config or create from legacy data
                        val serverConfig = if (configJson.containsKey("opcUaServerConfig")) {
                            OpcUaServerConfig.fromJsonObject(configJson.getJsonObject("opcUaServerConfig"))
                        } else {
                            // Legacy fallback - reconstruct from device config
                            configJson.apply {
                                put("name", deviceConfig.name)
                                put("namespace", deviceConfig.namespace)
                                put("nodeId", deviceConfig.nodeId)
                                put("enabled", true)
                            }
                            OpcUaServerConfig.fromJsonObject(configJson)
                        }.copy(enabled = true) // Enable when starting

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
                        val configJson = JsonObject(deviceConfig.config.toJsonObject().toString())

                        // Get the stored OPC UA server config or create from legacy data
                        val serverConfig = if (configJson.containsKey("opcUaServerConfig")) {
                            OpcUaServerConfig.fromJsonObject(configJson.getJsonObject("opcUaServerConfig"))
                        } else {
                            // Legacy fallback - reconstruct from device config
                            configJson.apply {
                                put("name", deviceConfig.name)
                                put("namespace", deviceConfig.namespace)
                                put("nodeId", deviceConfig.nodeId)
                                put("enabled", deviceConfig.enabled)
                            }
                            OpcUaServerConfig.fromJsonObject(configJson)
                        }

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
                        val configJson = JsonObject(deviceConfig.config.toJsonObject().toString())

                        // Get the stored OPC UA server config or create from legacy data
                        val serverConfig = if (configJson.containsKey("opcUaServerConfig")) {
                            OpcUaServerConfig.fromJsonObject(configJson.getJsonObject("opcUaServerConfig"))
                        } else {
                            // Legacy fallback - reconstruct from device config
                            configJson.apply {
                                put("name", deviceConfig.name)
                                put("namespace", deviceConfig.namespace)
                                put("nodeId", deviceConfig.nodeId)
                                put("enabled", deviceConfig.enabled)
                            }
                            OpcUaServerConfig.fromJsonObject(configJson)
                        }

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
            namespaceIndex = (config["namespaceIndex"] as? Number)?.toInt() ?: 1,
            namespaceUri = config["namespaceUri"] as? String ?: "urn:monstermq:opcua:${config["name"]}",
            addresses = addresses,
            security = security,
            bufferSize = (config["bufferSize"] as? Number)?.toInt() ?: 1000,
            updateInterval = (config["updateInterval"] as? Number)?.toLong() ?: 1000L
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
            if (addr.displayName.isBlank()) {
                throw IllegalArgumentException("Display name cannot be empty")
            }
        }
    }

    /**
     * Convert OpcUaServerConfig to DeviceConfig
     */
    private fun convertToDeviceConfig(serverConfig: OpcUaServerConfig): DeviceConfig {
        // Store the full OPC UA server configuration in the config field as JSON
        val serverConfigJson = serverConfig.toJsonObject()

        // Create a wrapper OpcUaConnectionConfig that contains the full server config
        val connectionConfig = OpcUaConnectionConfig(
            endpointUrl = "opc.tcp://localhost:${serverConfig.port}/${serverConfig.path}",
            updateEndpointUrl = false,
            securityPolicy = "None",
            username = null,
            password = null,
            subscriptionSamplingInterval = 0.0,
            keepAliveFailuresAllowed = 3,
            reconnectDelay = 5000L,
            connectionTimeout = 10000L,
            requestTimeout = 5000L,
            monitoringParameters = MonitoringParameters(),
            addresses = emptyList(),
            certificateConfig = CertificateConfig()
        )

        // Add the full server config to the connection config's JSON representation
        val configJsonObject = connectionConfig.toJsonObject()
        configJsonObject.put("opcUaServerConfig", serverConfigJson)

        return DeviceConfig(
            name = serverConfig.name,
            namespace = serverConfig.namespace,
            nodeId = serverConfig.nodeId,
            config = OpcUaConnectionConfig.fromJsonObject(configJsonObject),
            enabled = serverConfig.enabled,
            type = DEVICE_TYPE
        )
    }

    /**
     * Convert DeviceConfig to OpcUaServerInfo
     */
    private fun convertToOpcUaServerInfo(deviceConfig: DeviceConfig): OpcUaServerInfo {
        // Get the connection config JSON
        val configJson = JsonObject(deviceConfig.config.toJsonObject().toString())

        // Check if we have the full OPC UA server config stored
        val serverConfigJson = configJson.getJsonObject("opcUaServerConfig")

        return if (serverConfigJson != null) {
            // Use the stored OPC UA server configuration
            OpcUaServerInfo(
                name = deviceConfig.name,
                namespace = deviceConfig.namespace,
                nodeId = deviceConfig.nodeId,
                enabled = deviceConfig.enabled,
                port = serverConfigJson.getInteger("port", 4840),
                path = serverConfigJson.getString("path", "monstermq"),
                namespaceIndex = serverConfigJson.getInteger("namespaceIndex", 1),
                namespaceUri = serverConfigJson.getString("namespaceUri", "urn:monstermq:opcua:${deviceConfig.name}"),
                addresses = parseAddresses(serverConfigJson),
                security = parseSecurity(serverConfigJson),
                bufferSize = serverConfigJson.getInteger("bufferSize", 1000),
                updateInterval = serverConfigJson.getLong("updateInterval", 1000L),
                createdAt = serverConfigJson.getString("createdAt") ?: Instant.now().toString(),
                updatedAt = serverConfigJson.getString("updatedAt") ?: Instant.now().toString(),
                isOnCurrentNode = deviceConfig.nodeId == "*" || deviceConfig.nodeId == currentNodeId,
                status = null
            )
        } else {
            // Fallback for legacy data or missing config - extract port from endpointUrl
            val endpointUrl = configJson.getString("endpointUrl", "opc.tcp://localhost:4840/monstermq")
            val port = extractPortFromEndpointUrl(endpointUrl)
            val path = extractPathFromEndpointUrl(endpointUrl)

            OpcUaServerInfo(
                name = deviceConfig.name,
                namespace = deviceConfig.namespace,
                nodeId = deviceConfig.nodeId,
                enabled = deviceConfig.enabled,
                port = port,
                path = path,
                namespaceIndex = configJson.getInteger("namespaceIndex", 1),
                namespaceUri = configJson.getString("namespaceUri", "urn:monstermq:opcua:${deviceConfig.name}"),
                addresses = emptyList(),
                security = OpcUaServerSecurityInfo(
                    keystorePath = "server-keystore.jks",
                    certificateAlias = "server-cert",
                    securityPolicies = listOf("None"),
                    allowAnonymous = true,
                    requireAuthentication = false
                ),
                bufferSize = configJson.getInteger("bufferSize", 1000),
                updateInterval = configJson.getLong("updateInterval", 1000L),
                createdAt = Instant.now().toString(),
                updatedAt = Instant.now().toString(),
                isOnCurrentNode = deviceConfig.nodeId == "*" || deviceConfig.nodeId == currentNodeId,
                status = null
            )
        }
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
                    displayName = addrJson.getString("displayName", ""),
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
     * Parse OPC UA Server Address from GraphQL input
     */
    private fun parseOpcUaServerAddress(input: Map<String, Any>): OpcUaServerAddress {
        return OpcUaServerAddress(
            mqttTopic = input["mqttTopic"] as? String ?: throw IllegalArgumentException("MQTT topic is required"),
            displayName = input["displayName"] as? String ?: throw IllegalArgumentException("Display name is required"),
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