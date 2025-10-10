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
     * Get OPC UA server configurations with optional filters
     */
    fun opcUaServers(): DataFetcher<CompletableFuture<List<OpcUaServerInfo>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<OpcUaServerInfo>>()
            val name = env.getArgument<String?>("name")
            val nodeId = env.getArgument<String?>("node")

            when {
                // Filter by both name and node
                name != null && nodeId != null -> {
                    deviceConfigStore.getDevicesByNode(nodeId).onComplete { result ->
                        if (result.succeeded()) {
                            try {
                                val servers = result.result()
                                    .filter { it.type == DEVICE_TYPE && it.name == name }
                                    .map { device -> convertToOpcUaServerInfo(device) }
                                getServersWithStatus(servers, future)
                            } catch (e: Exception) {
                                logger.severe("Error processing OPC UA servers: ${e.message}")
                                future.completeExceptionally(e)
                            }
                        } else {
                            logger.severe("Failed to load OPC UA servers: ${result.cause()?.message}")
                            future.complete(emptyList())
                        }
                    }
                }
                // Filter by name only
                name != null -> {
                    deviceConfigStore.getDevice(name).onComplete { result ->
                        if (result.succeeded() && result.result()?.type == DEVICE_TYPE) {
                            try {
                                val serverInfo = convertToOpcUaServerInfo(result.result()!!)
                                getServersWithStatus(listOf(serverInfo), future)
                            } catch (e: Exception) {
                                logger.severe("Error processing OPC UA server: ${e.message}")
                                future.completeExceptionally(e)
                            }
                        } else {
                            future.complete(emptyList())
                        }
                    }
                }
                // Filter by node only
                nodeId != null -> {
                    deviceConfigStore.getAllDevices().onComplete { result ->
                        if (result.succeeded()) {
                            try {
                                val servers = result.result()
                                    .filter { it.type == DEVICE_TYPE && (it.nodeId == nodeId || it.nodeId == "*") }
                                    .map { device -> convertToOpcUaServerInfo(device) }
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
                }
                // No filters - return all
                else -> {
                    deviceConfigStore.getAllDevices().onComplete { result ->
                        if (result.succeeded()) {
                            try {
                                val servers = result.result()
                                    .filter { it.type == DEVICE_TYPE }
                                    .map { device -> convertToOpcUaServerInfo(device) }
                                getServersWithStatus(servers, future)
                            } catch (e: Exception) {
                                logger.severe("Error processing OPC UA servers: ${e.message}")
                                future.completeExceptionally(e)
                            }
                        } else {
                            logger.severe("Failed to load OPC UA servers: ${result.cause()?.message}")
                            future.complete(emptyList())
                        }
                    }
                }
            }

            future
        }
    }

    /**
     * Get certificates for a specific OPC UA server
     */
    fun opcUaServerCertificates(): DataFetcher<CompletableFuture<List<OpcUaServerCertificateInfo>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<OpcUaServerCertificateInfo>>()
            val serverName = env.getArgument<String>("serverName")
            val trusted = env.getArgument<Boolean?>("trusted")

            if (serverName == null) {
                future.complete(emptyList())
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

                            val certificateManager = OpcUaServerCertificateManager()
                            val certificates = certificateManager.getCertificates(serverName, securityDir, trusted)

                            val certificateInfos = certificates.map { cert ->
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

                            future.complete(certificateInfos)
                        } catch (e: Exception) {
                            logger.severe("Error getting certificates for server $serverName: ${e.message}")
                            future.completeExceptionally(e)
                        }
                    } else {
                        future.complete(emptyList())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error in opcUaServerCertificates: ${e.message}")
                future.completeExceptionally(e)
            }

            future
        }
    }

    /**
     * Convert DeviceConfig to OpcUaServerInfo
     */
    private fun convertToOpcUaServerInfo(device: DeviceConfig): OpcUaServerInfo {
        // Get the config JSON directly
        val configJson = device.config


        // After flattening, all config is directly in the main JSON object
        // Extract port from endpointUrl if available, otherwise use the port field
        val endpointUrl = configJson.getString("endpointUrl", "opc.tcp://localhost:4840/server")
        val port = configJson.getInteger("port") ?: extractPortFromEndpointUrl(endpointUrl)
        val path = configJson.getString("path") ?: extractPathFromEndpointUrl(endpointUrl)

        // Get certificate information
        val securityDir = configJson.getJsonObject("security", JsonObject())
            .getString("certificateDir", "./security")
        val certificateManager = OpcUaServerCertificateManager()
        val trustedCertificates = try {
            certificateManager.getCertificates(device.name, securityDir, trustedFilter = true).map { cert ->
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
        } catch (e: Exception) {
            logger.warning("Error loading trusted certificates for server ${device.name}: ${e.message}")
            emptyList()
        }

        val untrustedCertificates = try {
            certificateManager.getCertificates(device.name, securityDir, trustedFilter = false).map { cert ->
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
        } catch (e: Exception) {
            logger.warning("Error loading untrusted certificates for server ${device.name}: ${e.message}")
            emptyList()
        }

        return OpcUaServerInfo(
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
            createdAt = configJson.getString("createdAt") ?: "",
            updatedAt = configJson.getString("updatedAt") ?: "",
            isOnCurrentNode = device.nodeId == "*" || device.nodeId == currentNodeId,
            status = null, // Will be populated later
            trustedCertificates = trustedCertificates,
            untrustedCertificates = untrustedCertificates
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
            match?.groupValues?.get(1) ?: "server"
        } catch (e: Exception) {
            "server"
        }
    }

    private fun parseAddresses(configJson: JsonObject): List<OpcUaServerAddressInfo> {
        val addressesArray = configJson.getJsonArray("addresses", io.vertx.core.json.JsonArray())

        val result = addressesArray
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

        return result
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
    val status: OpcUaServerStatusInfo?,
    val trustedCertificates: List<OpcUaServerCertificateInfo>,
    val untrustedCertificates: List<OpcUaServerCertificateInfo>
)

data class OpcUaServerAddressInfo(
    val mqttTopic: String,
    val displayName: String?,
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

data class OpcUaServerCertificateInfo(
    val serverName: String,
    val fingerprint: String,
    val subject: String,
    val issuer: String,
    val validFrom: String,
    val validTo: String,
    val trusted: Boolean,
    val filePath: String,
    val firstSeen: String
)