package at.rocworks.stores

import at.rocworks.stores.devices.*
import io.vertx.core.json.JsonObject
import java.time.Instant

/**
 * Device configuration with cluster node assignment
 */
data class DeviceConfig(
    val name: String,                       // Device identifier (e.g., "plc01", "server")
    val namespace: String,                  // MQTT topic namespace (e.g., "opcua/plc01", "server")
    val nodeId: String,                     // Cluster node ID that handles this device
    val config: JsonObject,                 // Device-specific configuration as JSON
    val enabled: Boolean = true,
    val type: String = DEVICE_TYPE_OPCUA_CLIENT, // Device type (OPCUA-Client, OPCUA-Server, etc.)
    val createdAt: Instant = Instant.now(),
    val updatedAt: Instant = Instant.now()
) {
    companion object {
        // Global constant for device types
        const val DEVICE_TYPE_OPCUA_CLIENT = "OPCUA-Client"
        const val DEVICE_TYPE_OPCUA_SERVER = "OPCUA-Server"
        const val DEVICE_TYPE_MQTT_CLIENT = "MQTT-Client"
        const val DEVICE_TYPE_KAFKA_CLIENT = "KAFKA-Client"
        const val DEVICE_TYPE_WINCCOA_CLIENT = "WinCCOA-Client"
        const val DEVICE_TYPE_WINCCUA_CLIENT = "WinCCUA-Client"
        const val DEVICE_TYPE_PLC4X_CLIENT = "PLC4X-Client"
        const val DEVICE_TYPE_NEO4J_CLIENT = "Neo4j-Client"
        const val DEVICE_TYPE_JDBC_LOGGER = "JDBC-Logger"
        const val DEVICE_TYPE_SPARKPLUGB_DECODER = "SparkplugB-Decoder"
        const val DEVICE_TYPE_FLOW_CLASS = "Flow-Class"
        const val DEVICE_TYPE_FLOW_OBJECT = "Flow-Object"

        fun fromJsonObject(json: JsonObject): DeviceConfig {
            return DeviceConfig(
                name = json.getString("name"),
                namespace = json.getString("namespace"),
                nodeId = json.getString("nodeId"),
                config = json.getJsonObject("config") ?: JsonObject(),
                enabled = json.getBoolean("enabled", true),
                type = json.getString("type", DEVICE_TYPE_OPCUA_CLIENT),
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
            .put("config", config)
            .put("enabled", enabled)
            .put("type", type)
            .put("createdAt", createdAt.toString())
            .put("updatedAt", updatedAt.toString())
    }

    fun isAssignedToNode(currentNodeId: String): Boolean {
        return nodeId == currentNodeId
    }

    fun validateNamespace(): Boolean {
        return namespace.isNotBlank()
    }

    fun validateName(): Boolean {
        return name.matches(Regex("^[a-zA-Z0-9_-]+$"))
    }
}

/**
 * Device configuration change request for GraphQL
 */
data class DeviceConfigRequest(
    val name: String,
    val namespace: String,
    val nodeId: String,
    val config: JsonObject,
    val enabled: Boolean = true,
    val type: String = DeviceConfig.DEVICE_TYPE_OPCUA_CLIENT
) {
    fun toDeviceConfig(): DeviceConfig {
        return DeviceConfig(
            name = name,
            namespace = namespace,
            nodeId = nodeId,
            config = config,
            enabled = enabled,
            type = type,
            createdAt = Instant.now(),
            updatedAt = Instant.now()
        )
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()

        if (name.isBlank()) {
            errors.add("name cannot be blank")
        }

        if (!name.matches(Regex("^[a-zA-Z0-9_-]+$"))) {
            errors.add("name can only contain letters, numbers, underscores, and hyphens")
        }

        if (namespace.isBlank()) {
            errors.add("namespace cannot be blank")
        }

        if (nodeId.isBlank()) {
            errors.add("nodeId cannot be blank")
        }

        // Basic config validation - ensure it's not empty
        if (config.isEmpty) {
            errors.add("config cannot be empty")
        }

        return errors
    }
}
