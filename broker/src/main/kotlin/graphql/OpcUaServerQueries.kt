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
import io.vertx.core.json.JsonObject
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL query resolvers for OPC UA Server management
 */
class OpcUaServerQueries(
    private val vertx: Vertx,
    private val deviceConfigStore: IDeviceConfigStore
) {
    companion object {
        private val logger: Logger = Utils.getLogger(OpcUaServerQueries::class.java)
        private const val DEVICE_TYPE = "OPCUA-Server"
    }

    private val currentNodeId = Monster.getClusterNodeId(vertx)

    /**
     * Get all OPC UA server configurations across the cluster
     */
    fun opcUaServers(): DataFetcher<CompletableFuture<List<OpcUaServerInfo>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<OpcUaServerInfo>>()

            deviceConfigStore.getAllDevices().onComplete { result ->
                if (result.succeeded()) {
                    try {
                        val servers = result.result()
                            .filter { it.type == DEVICE_TYPE }
                            .map { device ->
                                convertToOpcUaServerInfo(device)
                            }

                        // Get status for each server
                        getServersWithStatus(servers, future)

                    } catch (e: Exception) {
                        logger.severe("Error processing OPC UA servers: ${e.message}")
                        future.completeExceptionally(e)
                    }
                } else {
                    logger.severe("Failed to load OPC UA servers: ${result.cause()?.message}")
                    future.complete(emptyList()) // Return empty list instead of null to satisfy non-null GraphQL schema
                }
            }

            future
        }
    }

    /**
     * Get a specific OPC UA server by name
     */
    fun opcUaServer(): DataFetcher<CompletableFuture<OpcUaServerInfo?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<OpcUaServerInfo?>()
            val name = env.getArgument<String>("name")

            if (name == null) {
                future.complete(null)
                return@DataFetcher future
            }

            deviceConfigStore.getDevice(name).onComplete { result ->
                if (result.succeeded() && result.result()?.type == DEVICE_TYPE) {
                    try {
                        val serverInfo = convertToOpcUaServerInfo(result.result()!!)
                        getServerWithStatus(serverInfo, future)
                    } catch (e: Exception) {
                        logger.severe("Error processing OPC UA server: ${e.message}")
                        future.completeExceptionally(e)
                    }
                } else {
                    future.complete(null)
                }
            }

            future
        }
    }

    /**
     * Get OPC UA servers assigned to a specific cluster node
     */
    fun opcUaServersByNode(): DataFetcher<CompletableFuture<List<OpcUaServerInfo>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<OpcUaServerInfo>>()
            val nodeId = env.getArgument<String>("nodeId")

            if (nodeId == null) {
                future.complete(emptyList())
                return@DataFetcher future
            }

            deviceConfigStore.getAllDevices().onComplete { result ->
                if (result.succeeded()) {
                    try {
                        val servers = result.result()
                            .filter { it.type == DEVICE_TYPE && (it.nodeId == nodeId || it.nodeId == "*") }
                            .map { device ->
                                convertToOpcUaServerInfo(device)
                            }

                        getServersWithStatus(servers, future)

                    } catch (e: Exception) {
                        logger.severe("Error processing OPC UA servers for node $nodeId: ${e.message}")
                        future.completeExceptionally(e)
                    }
                } else {
                    logger.severe("Failed to load OPC UA servers for node $nodeId: ${result.cause()?.message}")
                    future.complete(emptyList())
                }
            }

            future
        }
    }

    /**
     * Convert DeviceConfig to OpcUaServerInfo
     */
    private fun convertToOpcUaServerInfo(device: DeviceConfig): OpcUaServerInfo {
        // Get the connection config JSON
        val configJson = JsonObject(device.config.toJsonObject().toString())

        // Check if we have the full OPC UA server config stored
        val serverConfigJson = configJson.getJsonObject("opcUaServerConfig")

        return if (serverConfigJson != null) {
            // Use the stored OPC UA server configuration
            OpcUaServerInfo(
                name = device.name,
                namespace = device.namespace,
                nodeId = device.nodeId,
                enabled = device.enabled,
                port = serverConfigJson.getInteger("port", 4840),
                path = serverConfigJson.getString("path", "server"),
                namespaceIndex = serverConfigJson.getInteger("namespaceIndex", 1),
                namespaceUri = serverConfigJson.getString("namespaceUri", "urn:MonsterMQ:OpcUaServer"),
                addresses = parseAddresses(serverConfigJson),
                security = parseSecurity(serverConfigJson),
                bufferSize = serverConfigJson.getInteger("bufferSize", 1000),
                updateInterval = serverConfigJson.getLong("updateInterval", 100L),
                createdAt = serverConfigJson.getString("createdAt") ?: "",
                updatedAt = serverConfigJson.getString("updatedAt") ?: "",
                isOnCurrentNode = device.nodeId == "*" || device.nodeId == currentNodeId,
                status = null // Will be populated later
            )
        } else {
            // Fallback for legacy data or missing config - extract port from endpointUrl
            val endpointUrl = configJson.getString("endpointUrl", "opc.tcp://localhost:4840/server")
            val port = extractPortFromEndpointUrl(endpointUrl)
            val path = extractPathFromEndpointUrl(endpointUrl)

            OpcUaServerInfo(
                name = device.name,
                namespace = device.namespace,
                nodeId = device.nodeId,
                enabled = device.enabled,
                port = port,
                path = path,
                namespaceIndex = configJson.getInteger("namespaceIndex", 1),
                namespaceUri = configJson.getString("namespaceUri", "urn:MonsterMQ:OpcUaServer"),
                addresses = parseAddresses(configJson),
                security = parseSecurity(configJson),
                bufferSize = configJson.getInteger("bufferSize", 1000),
                updateInterval = configJson.getLong("updateInterval", 100L),
                createdAt = device.config.toJsonObject().getString("createdAt") ?: "",
                updatedAt = device.config.toJsonObject().getString("updatedAt") ?: "",
                isOnCurrentNode = device.nodeId == "*" || device.nodeId == currentNodeId,
                status = null // Will be populated later
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
            match?.groupValues?.get(1) ?: "server"
        } catch (e: Exception) {
            "server"
        }
    }

    private fun parseAddresses(configJson: JsonObject): List<OpcUaServerAddressInfo> {
        return configJson.getJsonArray("addresses", io.vertx.core.json.JsonArray())
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

    private fun parseSecurity(configJson: JsonObject): OpcUaServerSecurityInfo {
        val securityJson = configJson.getJsonObject("security", JsonObject())
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
     * Get servers with their runtime status
     */
    private fun getServersWithStatus(servers: List<OpcUaServerInfo>, future: CompletableFuture<List<OpcUaServerInfo>>) {
        if (servers.isEmpty()) {
            future.complete(emptyList())
            return
        }

        val serversWithStatus = mutableListOf<OpcUaServerInfo>()
        var completedCount = 0

        servers.forEach { server ->
            getServerStatus(server.name, server.nodeId) { status ->
                serversWithStatus.add(server.copy(status = status))
                completedCount++

                if (completedCount == servers.size) {
                    future.complete(serversWithStatus.toList())
                }
            }
        }

        // Timeout after 5 seconds
        vertx.setTimer(5000) {
            if (completedCount < servers.size) {
                logger.warning("Timeout waiting for server status, returning partial results")
                future.complete(serversWithStatus.toList())
            }
        }
    }

    /**
     * Get a single server with its runtime status
     */
    private fun getServerWithStatus(server: OpcUaServerInfo, future: CompletableFuture<OpcUaServerInfo?>) {
        getServerStatus(server.name, server.nodeId) { status ->
            future.complete(server.copy(status = status))
        }
    }

    /**
     * Get runtime status for a server from the appropriate cluster node
     */
    private fun getServerStatus(serverName: String, nodeId: String, callback: (OpcUaServerStatusInfo?) -> Unit) {
        val targetNodeId = if (nodeId == "*") currentNodeId else nodeId
        val statusAddress = "opcua.server.status.$targetNodeId"

        val statusRequest = JsonObject().put("serverName", serverName)

        vertx.eventBus().request<JsonObject>(statusAddress, statusRequest).onComplete { asyncResult ->
            if (asyncResult.succeeded()) {
                try {
                    val response = asyncResult.result().body()
                    val statusJson = response.getJsonObject("status")
                    if (statusJson != null) {
                        val status = OpcUaServerStatus.fromJsonObject(statusJson)
                        callback(convertToOpcUaServerStatusInfo(status))
                    } else {
                        callback(null)
                    }
                } catch (e: Exception) {
                    logger.warning("Error parsing server status for $serverName: ${e.message}")
                    callback(null)
                }
            } else {
                // Server not running or node not available
                callback(null)
            }
        }

        // Timeout after 1 second
        vertx.setTimer(1000) {
            callback(null)
        }
    }

    private fun convertToOpcUaServerStatusInfo(status: OpcUaServerStatus): OpcUaServerStatusInfo {
        return OpcUaServerStatusInfo(
            serverName = status.serverName,
            nodeId = status.nodeId,
            status = status.status.name,
            port = status.port,
            boundAddresses = status.boundAddresses,
            endpointUrl = status.endpointUrl,
            activeConnections = status.activeConnections,
            nodeCount = status.nodeCount,
            error = status.error,
            lastUpdated = status.lastUpdated.toString()
        )
    }
}

/**
 * GraphQL model classes for OPC UA Server information
 */
data class OpcUaServerInfo(
    val name: String,
    val namespace: String,
    val nodeId: String,
    val enabled: Boolean,
    val port: Int,
    val path: String,
    val namespaceIndex: Int,
    val namespaceUri: String,
    val addresses: List<OpcUaServerAddressInfo>,
    val security: OpcUaServerSecurityInfo,
    val bufferSize: Int,
    val updateInterval: Long,
    val createdAt: String,
    val updatedAt: String,
    val isOnCurrentNode: Boolean,
    val status: OpcUaServerStatusInfo?
)

data class OpcUaServerAddressInfo(
    val mqttTopic: String,
    val displayName: String,
    val browseName: String?,
    val description: String?,
    val dataType: String,
    val accessLevel: String,
    val unit: String?
)

data class OpcUaServerSecurityInfo(
    val keystorePath: String,
    val certificateAlias: String,
    val securityPolicies: List<String>,
    val allowAnonymous: Boolean,
    val requireAuthentication: Boolean
)

data class OpcUaServerStatusInfo(
    val serverName: String,
    val nodeId: String,
    val status: String,
    val port: Int?,
    val boundAddresses: List<String>,
    val endpointUrl: String?,
    val activeConnections: Int,
    val nodeCount: Int,
    val error: String?,
    val lastUpdated: String
)