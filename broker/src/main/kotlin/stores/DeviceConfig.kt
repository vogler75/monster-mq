package at.rocworks.stores

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant

/**
 * OPC UA device configuration with cluster node assignment
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
        // Global constant for OPC UA Client device type
        const val DEVICE_TYPE_OPCUA_CLIENT = "OPCUA-Client"
        const val DEVICE_TYPE_OPCUA_SERVER = "OPCUA-Server"
        const val DEVICE_TYPE_MQTT_CLIENT = "MQTT-Client"
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
                // For nodeId subscriptions, only use the topic without appending nodeId
                if (isNodeIdAddress()) {
                    // NodeId subscriptions: only use namespace + topic
                    if (topic.isNotEmpty()) {
                        "$namespace/$topic"
                    } else {
                        namespace
                    }
                } else {
                    // BrowsePath subscriptions: namespace + topic + browsePath
                    var topicPath = if (browsePath != null && browsePath.isNotEmpty()) {
                        browsePath
                    } else {
                        nodeId
                    }

                    // Apply removePath logic for browse paths if enabled
                    if (removePath && isBrowsePathAddress() && browsePath != null) {
                        topicPath = removeBasePath(browsePath)
                    }

                    // Incorporate the address topic field into the namespace path
                    if (topic.isNotEmpty()) {
                        "$namespace/$topic/$topicPath"
                    } else {
                        "$namespace/$topicPath"
                    }
                }
            }
            else -> topic // fallback
        }
    }

    /**
     * Unescape forward slashes in a browse path
     * Converts escaped slashes (\/) to actual slashes (/)
     */
    private fun unescapeSlashes(path: String): String {
        return path.replace("\\/", "/")
    }

    /**
     * Remove the base path before first wildcard from a browse path
     * Example: "Objects/Factory/Line1/Station1" with base "Objects/Factory/#" -> "Line1/Station1"
     * Handles escaped slashes in configured path: "ns=2;s=85\/Mqtt\/home/Original/#" -> "ns=2;s=85/Mqtt/home/Original/#"
     */
    private fun removeBasePath(fullBrowsePath: String): String {
        if (!isBrowsePathAddress()) return fullBrowsePath

        val configuredPath = getBrowsePath() ?: return fullBrowsePath

        // Unescape slashes in the configured path for comparison
        val unescapedConfiguredPath = unescapeSlashes(configuredPath)

        // Find the base path (everything before first # or +)
        val wildcardIndex = unescapedConfiguredPath.indexOfFirst { it == '#' || it == '+' }
        if (wildcardIndex <= 0) return fullBrowsePath

        val basePath = unescapedConfiguredPath.substring(0, wildcardIndex).trimEnd('/')

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
    val addresses: List<OpcUaAddress> = emptyList(),
    val certificateConfig: CertificateConfig = CertificateConfig()
) {
    companion object {
        fun fromJsonObject(json: JsonObject): OpcUaConnectionConfig {
            try {
                val monitoringParams = try {
                    json.getJsonObject("monitoringParameters")?.let {
                        MonitoringParameters.fromJsonObject(it)
                    } ?: MonitoringParameters()
                } catch (e: Exception) {
                    println("Error parsing monitoringParameters: ${e.message}")
                    MonitoringParameters()
                }

                val addresses = try {
                    json.getJsonArray("addresses")?.map { addressObj ->
                        OpcUaAddress.fromJsonObject(addressObj as JsonObject)
                    } ?: emptyList()
                } catch (e: Exception) {
                    println("Error parsing addresses: ${e.message}")
                    emptyList()
                }

                val certificateConfig = try {
                    json.getJsonObject("certificateConfig")?.let {
                        CertificateConfig.fromJsonObject(it)
                    } ?: CertificateConfig()
                } catch (e: Exception) {
                    println("Error parsing certificateConfig: ${e.message}")
                    CertificateConfig()
                }

                val config = OpcUaConnectionConfig(
                    endpointUrl = try { json.getString("endpointUrl", "opc.tcp://localhost:4840/server") } catch (e: Exception) {
                        println("Error parsing endpointUrl: ${e.message}")
                        "opc.tcp://localhost:4840/server"
                    },
                    updateEndpointUrl = try { json.getBoolean("updateEndpointUrl", true) } catch (e: Exception) {
                        println("Error parsing updateEndpointUrl: ${e.message}")
                        true
                    },
                    securityPolicy = try { json.getString("securityPolicy", "None") } catch (e: Exception) {
                        println("Error parsing securityPolicy: ${e.message}")
                        "None"
                    },
                    username = try { json.getString("username") } catch (e: Exception) {
                        println("Error parsing username: ${e.message}")
                        null
                    },
                    password = try { json.getString("password") } catch (e: Exception) {
                        println("Error parsing password: ${e.message}")
                        null
                    },
                    subscriptionSamplingInterval = try { json.getDouble("subscriptionSamplingInterval", 0.0) } catch (e: Exception) {
                        println("Error parsing subscriptionSamplingInterval: ${e.message}")
                        0.0
                    },
                    keepAliveFailuresAllowed = try { json.getInteger("keepAliveFailuresAllowed", 3) } catch (e: Exception) {
                        println("Error parsing keepAliveFailuresAllowed: ${e.message}")
                        3
                    },
                    reconnectDelay = try { json.getLong("reconnectDelay", 5000) } catch (e: Exception) {
                        println("Error parsing reconnectDelay: ${e.message}")
                        5000
                    },
                    connectionTimeout = try { json.getLong("connectionTimeout", 10000) } catch (e: Exception) {
                        println("Error parsing connectionTimeout: ${e.message}")
                        10000
                    },
                    requestTimeout = try { json.getLong("requestTimeout", 5000) } catch (e: Exception) {
                        println("Error parsing requestTimeout: ${e.message}")
                        5000
                    },
                    monitoringParameters = monitoringParams,
                    addresses = addresses,
                    certificateConfig = certificateConfig
                )

                // DeviceConfig is only for OPC UA Client devices
                // OPC UA Server devices should use a separate configuration system

                return config
            } catch (e: Exception) {
                println("Overall error in fromJsonObject: ${e.message}")
                throw e
            }
        }
    }

    fun toJsonObject(): JsonObject {
        val result = JsonObject()
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
            .put("certificateConfig", certificateConfig.toJsonObject())

        // Add addresses array if we have addresses
        if (addresses.isNotEmpty()) {
            val addressArray = JsonArray()
            addresses.forEach { address ->
                addressArray.add(address.toJsonObject())
            }
            result.put("addresses", addressArray)
        }

        return result
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
 * Certificate configuration for OPC UA connections
 */
data class CertificateConfig(
    val securityDir: String = "security",
    val applicationName: String = "MonsterMQ@localhost",
    val applicationUri: String = "urn:MonsterMQ:Client",
    val organization: String = "MonsterMQ",
    val organizationalUnit: String = "Client",
    val localityName: String = "Unknown",
    val countryCode: String = "XX",
    val createSelfSigned: Boolean = true,
    val keystorePassword: String = "password",
    val validateServerCertificate: Boolean = true,
    val autoAcceptServerCertificates: Boolean = false
) {
    companion object {
        fun fromJsonObject(json: JsonObject): CertificateConfig {
            return CertificateConfig(
                securityDir = json.getString("securityDir", "security"),
                applicationName = json.getString("applicationName", "MonsterMQ@localhost"),
                applicationUri = json.getString("applicationUri", "urn:MonsterMQ:Client"),
                organization = json.getString("organization", "MonsterMQ"),
                organizationalUnit = json.getString("organizationalUnit", "Client"),
                localityName = json.getString("localityName", "Unknown"),
                countryCode = json.getString("countryCode", "XX"),
                createSelfSigned = json.getBoolean("createSelfSigned", true),
                keystorePassword = json.getString("keystorePassword", "password"),
                validateServerCertificate = json.getBoolean("validateServerCertificate", true),
                autoAcceptServerCertificates = json.getBoolean("autoAcceptServerCertificates", false)
            )
        }
    }

    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("securityDir", securityDir)
            .put("applicationName", applicationName)
            .put("applicationUri", applicationUri)
            .put("organization", organization)
            .put("organizationalUnit", organizationalUnit)
            .put("localityName", localityName)
            .put("countryCode", countryCode)
            .put("createSelfSigned", createSelfSigned)
            .put("keystorePassword", keystorePassword)
            .put("validateServerCertificate", validateServerCertificate)
            .put("autoAcceptServerCertificates", autoAcceptServerCertificates)
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

        if (!namespace.matches(Regex("^[a-zA-Z0-9_/-]+$"))) {
            errors.add("namespace must contain only letters, numbers, underscores, hyphens, and slashes")
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

/**
 * MQTT Client address configuration for subscriptions and publications
 */
data class MqttClientAddress(
    val mode: String,              // "SUBSCRIBE" or "PUBLISH"
    val remoteTopic: String,       // Remote MQTT topic (with wildcards)
    val localTopic: String,        // Local MQTT topic destination/source
    val removePath: Boolean = true // Remove base path before wildcard
) {
    companion object {
        const val MODE_SUBSCRIBE = "SUBSCRIBE"
        const val MODE_PUBLISH = "PUBLISH"

        fun fromJsonObject(json: JsonObject): MqttClientAddress {
            return MqttClientAddress(
                mode = json.getString("mode"),
                remoteTopic = json.getString("remoteTopic"),
                localTopic = json.getString("localTopic"),
                removePath = json.getBoolean("removePath", true)
            )
        }
    }

    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("mode", mode)
            .put("remoteTopic", remoteTopic)
            .put("localTopic", localTopic)
            .put("removePath", removePath)
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()

        if (mode.isBlank()) {
            errors.add("mode cannot be blank")
        }

        if (mode != MODE_SUBSCRIBE && mode != MODE_PUBLISH) {
            errors.add("mode must be '$MODE_SUBSCRIBE' or '$MODE_PUBLISH'")
        }

        if (remoteTopic.isBlank()) {
            errors.add("remoteTopic cannot be blank")
        }

        if (localTopic.isBlank()) {
            errors.add("localTopic cannot be blank")
        }

        return errors
    }

    /**
     * Check if this is a subscribe address (bring data IN from remote broker)
     */
    fun isSubscribe(): Boolean = mode == MODE_SUBSCRIBE

    /**
     * Check if this is a publish address (push data OUT to remote broker)
     */
    fun isPublish(): Boolean = mode == MODE_PUBLISH
}

/**
 * MQTT Client connection configuration parameters
 */
data class MqttClientConnectionConfig(
    val protocol: String,          // "tcp", "tcps", "ws", "wss"
    val hostname: String,
    val port: Int,
    val username: String? = null,
    val password: String? = null,
    val clientId: String,
    val cleanSession: Boolean = true,
    val keepAlive: Int = 60,
    val reconnectDelay: Long = 5000,
    val connectionTimeout: Long = 30000,
    val addresses: List<MqttClientAddress> = emptyList()
) {
    companion object {
        const val PROTOCOL_TCP = "tcp"
        const val PROTOCOL_TCPS = "tcps"
        const val PROTOCOL_WS = "ws"
        const val PROTOCOL_WSS = "wss"

        fun fromJsonObject(json: JsonObject): MqttClientConnectionConfig {
            try {
                val addresses = try {
                    json.getJsonArray("addresses")?.map { addressObj ->
                        MqttClientAddress.fromJsonObject(addressObj as JsonObject)
                    } ?: emptyList()
                } catch (e: Exception) {
                    println("Error parsing addresses: ${e.message}")
                    emptyList()
                }

                return MqttClientConnectionConfig(
                    protocol = json.getString("protocol", PROTOCOL_TCP),
                    hostname = json.getString("hostname", "localhost"),
                    port = json.getInteger("port", 1883),
                    username = json.getString("username"),
                    password = json.getString("password"),
                    clientId = json.getString("clientId", "monstermq-client"),
                    cleanSession = json.getBoolean("cleanSession", true),
                    keepAlive = json.getInteger("keepAlive", 60),
                    reconnectDelay = json.getLong("reconnectDelay", 5000L),
                    connectionTimeout = json.getLong("connectionTimeout", 30000L),
                    addresses = addresses
                )
            } catch (e: Exception) {
                println("Overall error in fromJsonObject: ${e.message}")
                throw e
            }
        }
    }

    fun toJsonObject(): JsonObject {
        val result = JsonObject()
            .put("protocol", protocol)
            .put("hostname", hostname)
            .put("port", port)
            .put("username", username)
            .put("password", password)
            .put("clientId", clientId)
            .put("cleanSession", cleanSession)
            .put("keepAlive", keepAlive)
            .put("reconnectDelay", reconnectDelay)
            .put("connectionTimeout", connectionTimeout)

        // Add addresses array if we have addresses
        if (addresses.isNotEmpty()) {
            val addressArray = JsonArray()
            addresses.forEach { address ->
                addressArray.add(address.toJsonObject())
            }
            result.put("addresses", addressArray)
        }

        return result
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()

        if (protocol.isBlank()) {
            errors.add("protocol cannot be blank")
        }

        if (protocol !in listOf(PROTOCOL_TCP, PROTOCOL_TCPS, PROTOCOL_WS, PROTOCOL_WSS)) {
            errors.add("protocol must be one of: $PROTOCOL_TCP, $PROTOCOL_TCPS, $PROTOCOL_WS, $PROTOCOL_WSS")
        }

        if (hostname.isBlank()) {
            errors.add("hostname cannot be blank")
        }

        if (port < 1 || port > 65535) {
            errors.add("port must be between 1 and 65535")
        }

        if (clientId.isBlank()) {
            errors.add("clientId cannot be blank")
        }

        if (keepAlive < 0) {
            errors.add("keepAlive cannot be negative")
        }

        if (reconnectDelay < 1000) {
            errors.add("reconnectDelay should be at least 1000ms")
        }

        if (connectionTimeout < 1000) {
            errors.add("connectionTimeout should be at least 1000ms")
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

    /**
     * Build the MQTT broker URL from protocol, hostname, and port
     */
    fun getBrokerUrl(): String {
        return "$protocol://$hostname:$port"
    }
}