package at.rocworks.devices.opcuaserver

import at.rocworks.stores.DeviceConfig
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant

/**
 * OPC UA Server configuration for exposing MQTT topics as OPC UA nodes
 */
data class OpcUaServerConfig(
    val name: String,                           // Server identifier (e.g., "factory-server-01")
    val namespace: String,                      // MQTT topic namespace prefix for internal communication
    val nodeId: String,                         // Target cluster node ("*" for all nodes, specific ID for single node)
    val enabled: Boolean = true,
    val port: Int = 4840,                       // OPC UA server port
    val path: String = "server",                // OPC UA server endpoint path
    val namespaceIndex: Int = 1,                // OPC UA namespace index (must be >= 1)
    val namespaceUri: String = "urn:MonsterMQ:OpcUaServer", // OPC UA namespace URI
    val addresses: List<OpcUaServerAddress>,    // MQTT to OPC UA topic mappings
    val security: OpcUaServerSecurity,          // Security configuration
    val bufferSize: Int = 1000,                 // Buffer size for value updates
    val updateInterval: Long = 100,             // Minimum update interval in milliseconds
    val type: String = DeviceConfig.DEVICE_TYPE_OPCUA_SERVER,
    val createdAt: Instant = Instant.now(),
    val updatedAt: Instant = Instant.now()
) {
    companion object {
        fun fromJsonObject(json: JsonObject): OpcUaServerConfig {
            val addressesArray = json.getJsonArray("addresses", JsonArray())

            return OpcUaServerConfig(
                name = json.getString("name"),
                namespace = json.getString("namespace"),
                nodeId = json.getString("nodeId"),
                enabled = json.getBoolean("enabled", true),
                port = json.getInteger("port", 4840),
                path = json.getString("path", "server"),
                namespaceIndex = json.getInteger("namespaceIndex", 1),
                namespaceUri = json.getString("namespaceUri", "urn:MonsterMQ:OpcUaServer"),
                addresses = addressesArray
                    .filterIsInstance<JsonObject>()
                    .map { OpcUaServerAddress.fromJsonObject(it) },
                security = OpcUaServerSecurity.fromJsonObject(
                    json.getJsonObject("security", JsonObject())
                ),
                bufferSize = json.getInteger("bufferSize", 1000),
                updateInterval = json.getLong("updateInterval", 100L),
                type = json.getString("type", DeviceConfig.DEVICE_TYPE_OPCUA_SERVER),
                createdAt = json.getString("createdAt")?.let { Instant.parse(it) } ?: Instant.now(),
                updatedAt = json.getString("updatedAt")?.let { Instant.parse(it) } ?: Instant.now()
            )
        }
    }

    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("name", name)
            .put("namespace", namespace)
            .put("nodeId", nodeId)
            .put("enabled", enabled)
            .put("port", port)
            .put("path", path)
            .put("namespaceIndex", namespaceIndex)
            .put("namespaceUri", namespaceUri)
            .put("addresses", JsonArray(addresses.map { it.toJsonObject() }))
            .put("security", security.toJsonObject())
            .put("bufferSize", bufferSize)
            .put("updateInterval", updateInterval)
            .put("type", type)
            .put("createdAt", createdAt.toString())
            .put("updatedAt", updatedAt.toString())
    }
}

/**
 * Mapping configuration for MQTT topic to OPC UA node
 */
data class OpcUaServerAddress(
    val mqttTopic: String,                      // MQTT topic pattern with wildcards (e.g., "factory/+/temperature")
    val displayName: String? = null,            // Optional human-readable display name for OPC UA node
    val browseName: String? = null,             // Optional browse name (defaults to last topic segment)
    val description: String? = null,            // Optional description for the node
    val dataType: OpcUaServerDataType,          // Data type conversion strategy
    val accessLevel: OpcUaAccessLevel = OpcUaAccessLevel.READ_ONLY,
    val unit: String? = null                    // Engineering unit (e.g., "°C", "bar")
) {
    companion object {
        fun fromJsonObject(json: JsonObject): OpcUaServerAddress {
            return OpcUaServerAddress(
                mqttTopic = json.getString("mqttTopic") ?: throw IllegalArgumentException("mqttTopic is required"),
                displayName = json.getString("displayName")?.takeIf { it.isNotBlank() },
                browseName = json.getString("browseName"),
                description = json.getString("description"),
                dataType = try {
                    OpcUaServerDataType.valueOf(json.getString("dataType", OpcUaServerDataType.TEXT.name))
                } catch (e: Exception) {
                    OpcUaServerDataType.TEXT
                },
                accessLevel = try {
                    OpcUaAccessLevel.valueOf(json.getString("accessLevel", OpcUaAccessLevel.READ_ONLY.name))
                } catch (e: Exception) {
                    OpcUaAccessLevel.READ_ONLY
                },
                unit = json.getString("unit")
            )
        }
    }

    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("mqttTopic", mqttTopic)
            .apply {
                displayName?.let { put("displayName", it) }
                browseName?.let { put("browseName", it) }
                description?.let { put("description", it) }
                unit?.let { put("unit", it) }
            }
            .put("dataType", dataType.name)
            .put("accessLevel", accessLevel.name)
    }
}

/**
 * Data type conversion strategy between MQTT and OPC UA
 */
enum class OpcUaServerDataType {
    BINARY,      // ByteString - raw payload bytes
    TEXT,        // String - payload as UTF-8 text
    NUMERIC,     // Double - parse payload as number
    BOOLEAN,     // Boolean - parse payload as true/false
    JSON         // Custom JSON format: {"value": x, "timestamp": "iso", "status": int}
}

/**
 * OPC UA node access level
 */
enum class OpcUaAccessLevel {
    READ_ONLY,   // OPC UA clients can only read
    READ_WRITE   // OPC UA clients can read and write (writes go back to MQTT)
}

/**
 * Security configuration for OPC UA server
 */
data class OpcUaServerSecurity(
    val keystorePath: String = "server-keystore.jks",  // Reuse MQTT server keystore
    val keystorePassword: String = "password",
    val certificateAlias: String = "server-cert",       // Certificate alias in keystore
    val securityPolicies: List<String> = listOf("None", "Basic256Sha256"),
    val allowAnonymous: Boolean = true,
    val requireAuthentication: Boolean = false,         // Use MonsterMQ user management
    val allowUnencrypted: Boolean = true,               // Allow unencrypted connections (set to false to require encryption)
    val certificateDir: String = "./security",          // Directory for certificates
    val createSelfSigned: Boolean = true                // Create self-signed certificate if not exists
) {
    companion object {
        fun fromJsonObject(json: JsonObject): OpcUaServerSecurity {
            return OpcUaServerSecurity(
                keystorePath = json.getString("keystorePath", "server-keystore.jks"),
                keystorePassword = json.getString("keystorePassword", "password"),
                certificateAlias = json.getString("certificateAlias", "server-cert"),
                securityPolicies = json.getJsonArray("securityPolicies", JsonArray())
                    .filterIsInstance<String>()
                    .ifEmpty { listOf("None", "Basic256Sha256") },
                allowAnonymous = json.getBoolean("allowAnonymous", true),
                requireAuthentication = json.getBoolean("requireAuthentication", false),
                allowUnencrypted = json.getBoolean("allowUnencrypted", true),
                certificateDir = json.getString("certificateDir", "./security"),
                createSelfSigned = json.getBoolean("createSelfSigned", true)
            )
        }
    }

    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("keystorePath", keystorePath)
            .put("keystorePassword", keystorePassword)
            .put("certificateAlias", certificateAlias)
            .put("securityPolicies", JsonArray(securityPolicies))
            .put("allowAnonymous", allowAnonymous)
            .put("requireAuthentication", requireAuthentication)
            .put("allowUnencrypted", allowUnencrypted)
            .put("certificateDir", certificateDir)
            .put("createSelfSigned", createSelfSigned)
    }
}

/**
 * Commands for controlling OPC UA Server via message bus
 */
data class OpcUaServerCommand(
    val action: Action,
    val serverName: String,
    val nodeId: String,        // "*" for all nodes, specific ID for target node
    val config: OpcUaServerConfig? = null
) {
    enum class Action {
        START,
        STOP,
        UPDATE_CONFIG,
        GET_STATUS
    }

    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("action", action.name)
            .put("serverName", serverName)
            .put("nodeId", nodeId)
            .apply {
                config?.let { put("config", it.toJsonObject()) }
            }
    }

    companion object {
        fun fromJsonObject(json: JsonObject): OpcUaServerCommand {
            return OpcUaServerCommand(
                action = Action.valueOf(json.getString("action")),
                serverName = json.getString("serverName"),
                nodeId = json.getString("nodeId"),
                config = json.getJsonObject("config")?.let { OpcUaServerConfig.fromJsonObject(it) }
            )
        }
    }
}

/**
 * Status of an OPC UA Server instance
 */
data class OpcUaServerStatus(
    val serverName: String,
    val nodeId: String,
    val status: Status,
    val port: Int? = null,
    val boundAddresses: List<String> = emptyList(),
    val endpointUrl: String? = null,
    val activeConnections: Int = 0,
    val nodeCount: Int = 0,
    val error: String? = null,
    val lastUpdated: Instant = Instant.now()
) {
    enum class Status {
        STOPPED,
        STARTING,
        RUNNING,
        ERROR,
        STOPPING
    }

    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("serverName", serverName)
            .put("nodeId", nodeId)
            .put("status", status.name)
            .apply {
                port?.let { put("port", it) }
                if (boundAddresses.isNotEmpty()) {
                    put("boundAddresses", JsonArray(boundAddresses))
                }
                endpointUrl?.let { put("endpointUrl", it) }
                put("activeConnections", activeConnections)
                put("nodeCount", nodeCount)
                error?.let { put("error", it) }
            }
            .put("lastUpdated", lastUpdated.toString())
    }

    companion object {
        fun fromJsonObject(json: JsonObject): OpcUaServerStatus {
            return OpcUaServerStatus(
                serverName = json.getString("serverName"),
                nodeId = json.getString("nodeId"),
                status = Status.valueOf(json.getString("status")),
                port = json.getInteger("port"),
                boundAddresses = json.getJsonArray("boundAddresses", JsonArray())
                    .filterIsInstance<String>(),
                endpointUrl = json.getString("endpointUrl"),
                activeConnections = json.getInteger("activeConnections", 0),
                nodeCount = json.getInteger("nodeCount", 0),
                error = json.getString("error"),
                lastUpdated = json.getString("lastUpdated")?.let { Instant.parse(it) } ?: Instant.now()
            )
        }
    }
}