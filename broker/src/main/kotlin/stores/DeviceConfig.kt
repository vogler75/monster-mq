package at.rocworks.stores

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant

/**
 * OPC UA device configuration with cluster node assignment
 */
data class DeviceConfig(
    val name: String,                       // Device identifier (e.g., "plc01")
    val namespace: String,                  // MQTT topic namespace (e.g., "opcua/plc01")
    val nodeId: String,                     // Cluster node ID that handles this device
    val backupNodeId: String? = null,       // Future: backup node for failover
    val config: OpcUaConnectionConfig,      // OPC UA connection configuration
    val enabled: Boolean = true,
    val createdAt: Instant = Instant.now(),
    val updatedAt: Instant = Instant.now()
) {
    companion object {
        fun fromJsonObject(json: JsonObject): DeviceConfig {
            return DeviceConfig(
                name = json.getString("name"),
                namespace = json.getString("namespace"),
                nodeId = json.getString("nodeId"),
                backupNodeId = json.getString("backupNodeId"),
                config = OpcUaConnectionConfig.fromJsonObject(json.getJsonObject("config")),
                enabled = json.getBoolean("enabled", true),
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
            .put("backupNodeId", backupNodeId)
            .put("config", config.toJsonObject())
            .put("enabled", enabled)
            .put("createdAt", createdAt.toString())
            .put("updatedAt", updatedAt.toString())
    }

    fun isAssignedToNode(currentNodeId: String): Boolean {
        return nodeId == currentNodeId
    }

    fun validateNamespace(): Boolean {
        return namespace.matches(Regex("^[a-zA-Z0-9_/-]+$"))
    }

    fun validateName(): Boolean {
        return name.matches(Regex("^[a-zA-Z0-9_-]+$"))
    }
}

/**
 * OPC UA address configuration for subscriptions
 */
data class OpcUaAddress(
    val address: String,        // BrowsePath://Objects/Factory/# or NodeId://ns=2;i=16
    val topic: String,          // MQTT topic where to publish the values (for single mode)
    val publishMode: String = "SEPARATE",  // "SINGLE" = all values on one topic, "SEPARATE" = each node gets own topic
    val removePath: Boolean = true       // Remove base path before first wildcard from topic (default: true)
) {
    companion object {
        const val PUBLISH_MODE_SINGLE = "SINGLE"
        const val PUBLISH_MODE_SEPARATE = "SEPARATE"

        fun fromJsonObject(json: JsonObject): OpcUaAddress {
            return OpcUaAddress(
                address = json.getString("address"),
                topic = json.getString("topic"),
                publishMode = json.getString("publishMode", PUBLISH_MODE_SEPARATE),
                removePath = json.getBoolean("removePath", true)
            )
        }
    }

    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("address", address)
            .put("topic", topic)
            .put("publishMode", publishMode)
            .put("removePath", removePath)
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()

        if (address.isBlank()) {
            errors.add("address cannot be blank")
        }

        if (topic.isBlank()) {
            errors.add("topic cannot be blank")
        }

        // Validate address format
        if (!address.startsWith("BrowsePath://") && !address.startsWith("NodeId://")) {
            errors.add("address must start with 'BrowsePath://' or 'NodeId://'")
        }

        // Validate publish mode
        if (publishMode != PUBLISH_MODE_SINGLE && publishMode != PUBLISH_MODE_SEPARATE) {
            errors.add("publishMode must be '$PUBLISH_MODE_SINGLE' or '$PUBLISH_MODE_SEPARATE'")
        }

        return errors
    }

    /**
     * Check if this is a node ID address (NodeId://...)
     */
    fun isNodeIdAddress(): Boolean = address.startsWith("NodeId://")

    /**
     * Check if this is a browse path address (BrowsePath://...)
     */
    fun isBrowsePathAddress(): Boolean = address.startsWith("BrowsePath://")

    /**
     * Get the node ID from a node address
     */
    fun getNodeId(): String? {
        return if (isNodeIdAddress()) {
            address.substringAfter("NodeId://")
        } else null
    }

    /**
     * Get the browse path from a path address
     */
    fun getBrowsePath(): String? {
        return if (isBrowsePathAddress()) {
            address.substringAfter("BrowsePath://")
        } else null
    }

    /**
     * Check if this address uses single topic mode (all values on one topic)
     */
    fun isSingleTopicMode(): Boolean = publishMode == PUBLISH_MODE_SINGLE

    /**
     * Check if this address uses separate topic mode (each node gets own topic)
     */
    fun isSeparateTopicMode(): Boolean = publishMode == PUBLISH_MODE_SEPARATE

    /**
     * Generate the MQTT topic for a specific node value
     * @param namespace The device namespace (e.g., "opcua/factory")
     * @param nodeId The OPC UA node ID (e.g., "ns=2;i=16")
     * @param browsePath Optional browse path for separate mode
     */
    fun generateMqttTopic(namespace: String, nodeId: String, browsePath: String? = null): String {
        return when (publishMode) {
            PUBLISH_MODE_SINGLE -> {
                // All values go to the configured topic with namespace prefix
                if (topic.startsWith(namespace)) topic else "$namespace/$topic"
            }
            PUBLISH_MODE_SEPARATE -> {
                // Each node gets its own topic: namespace + / + nodeId or browsePath
                var topicPath = if (browsePath != null && browsePath.isNotEmpty()) {
                    browsePath
                } else {
                    nodeId
                }

                // Apply removePath logic for browse paths if enabled
                if (removePath && isBrowsePathAddress() && browsePath != null) {
                    topicPath = removeBasePath(browsePath)
                }

                "$namespace/$topicPath"
            }
            else -> topic // fallback
        }
    }

    /**
     * Remove the base path before first wildcard from a browse path
     * Example: "Objects/Factory/Line1/Station1" with base "Objects/Factory/#" -> "Line1/Station1"
     */
    private fun removeBasePath(fullBrowsePath: String): String {
        if (!isBrowsePathAddress()) return fullBrowsePath

        val configuredPath = getBrowsePath() ?: return fullBrowsePath

        // Find the base path (everything before first # or +)
        val wildcardIndex = configuredPath.indexOfFirst { it == '#' || it == '+' }
        if (wildcardIndex <= 0) return fullBrowsePath

        val basePath = configuredPath.substring(0, wildcardIndex).trimEnd('/')

        // Remove the base path from the full browse path
        return if (fullBrowsePath.startsWith(basePath)) {
            fullBrowsePath.substring(basePath.length).trimStart('/')
        } else {
            fullBrowsePath
        }
    }
}

/**
 * OPC UA connection configuration parameters
 */
data class OpcUaConnectionConfig(
    val endpointUrl: String,
    val updateEndpointUrl: Boolean = true,
    val securityPolicy: String = "None",
    val username: String? = null,
    val password: String? = null,
    val subscriptionSamplingInterval: Double = 0.0,
    val keepAliveFailuresAllowed: Int = 3,
    val reconnectDelay: Long = 5000,
    val connectionTimeout: Long = 10000,
    val requestTimeout: Long = 5000,
    val monitoringParameters: MonitoringParameters = MonitoringParameters(),
    val addresses: List<OpcUaAddress> = emptyList()
) {
    companion object {
        fun fromJsonObject(json: JsonObject): OpcUaConnectionConfig {
            val monitoringParams = json.getJsonObject("monitoringParameters")?.let {
                MonitoringParameters.fromJsonObject(it)
            } ?: MonitoringParameters()

            val addresses = json.getJsonArray("addresses")?.map { addressObj ->
                OpcUaAddress.fromJsonObject(addressObj as JsonObject)
            } ?: emptyList()

            return OpcUaConnectionConfig(
                endpointUrl = json.getString("endpointUrl"),
                updateEndpointUrl = json.getBoolean("updateEndpointUrl", true),
                securityPolicy = json.getString("securityPolicy", "None"),
                username = json.getString("username"),
                password = json.getString("password"),
                subscriptionSamplingInterval = json.getDouble("subscriptionSamplingInterval", 0.0),
                keepAliveFailuresAllowed = json.getInteger("keepAliveFailuresAllowed", 3),
                reconnectDelay = json.getLong("reconnectDelay", 5000),
                connectionTimeout = json.getLong("connectionTimeout", 10000),
                requestTimeout = json.getLong("requestTimeout", 5000),
                monitoringParameters = monitoringParams,
                addresses = addresses
            )
        }
    }

    fun toJsonObject(): JsonObject {
        val addressArray = JsonArray()
        addresses.forEach { address ->
            addressArray.add(address.toJsonObject())
        }

        return JsonObject()
            .put("endpointUrl", endpointUrl)
            .put("updateEndpointUrl", updateEndpointUrl)
            .put("securityPolicy", securityPolicy)
            .put("username", username)
            .put("password", password)
            .put("subscriptionSamplingInterval", subscriptionSamplingInterval)
            .put("keepAliveFailuresAllowed", keepAliveFailuresAllowed)
            .put("reconnectDelay", reconnectDelay)
            .put("connectionTimeout", connectionTimeout)
            .put("requestTimeout", requestTimeout)
            .put("monitoringParameters", monitoringParameters.toJsonObject())
            .put("addresses", addressArray)
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()

        if (endpointUrl.isBlank()) {
            errors.add("endpointUrl cannot be blank")
        }

        if (!endpointUrl.startsWith("opc.tcp://")) {
            errors.add("endpointUrl must start with opc.tcp://")
        }

        if (subscriptionSamplingInterval < 0) {
            errors.add("subscriptionSamplingInterval cannot be negative")
        }

        if (keepAliveFailuresAllowed < 0) {
            errors.add("keepAliveFailuresAllowed cannot be negative")
        }

        if (reconnectDelay < 1000) {
            errors.add("reconnectDelay should be at least 1000ms")
        }

        // Validate addresses
        addresses.forEachIndexed { index, address ->
            val addressErrors = address.validate()
            addressErrors.forEach { error ->
                errors.add("Address $index: $error")
            }
        }

        return errors
    }
}

/**
 * OPC UA monitoring parameters for subscriptions
 */
data class MonitoringParameters(
    val bufferSize: Int = 100,
    val samplingInterval: Double = 0.0,
    val discardOldest: Boolean = false
) {
    companion object {
        fun fromJsonObject(json: JsonObject): MonitoringParameters {
            return MonitoringParameters(
                bufferSize = json.getInteger("bufferSize", 100),
                samplingInterval = json.getDouble("samplingInterval", 0.0),
                discardOldest = json.getBoolean("discardOldest", false)
            )
        }
    }

    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("bufferSize", bufferSize)
            .put("samplingInterval", samplingInterval)
            .put("discardOldest", discardOldest)
    }
}

/**
 * Device configuration change request for GraphQL
 */
data class DeviceConfigRequest(
    val name: String,
    val namespace: String,
    val nodeId: String,
    val backupNodeId: String? = null,
    val config: OpcUaConnectionConfig,
    val enabled: Boolean = true
) {
    fun toDeviceConfig(): DeviceConfig {
        return DeviceConfig(
            name = name,
            namespace = namespace,
            nodeId = nodeId,
            backupNodeId = backupNodeId,
            config = config,
            enabled = enabled,
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

        if (!namespace.matches(Regex("^[a-zA-Z0-9_/-]+$"))) {
            errors.add("namespace must contain only letters, numbers, underscores, hyphens, and slashes")
        }

        if (nodeId.isBlank()) {
            errors.add("nodeId cannot be blank")
        }

        errors.addAll(config.validate())

        return errors
    }
}