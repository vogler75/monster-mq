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
        const val DEVICE_TYPE_KAFKA_CLIENT = "KAFKA-Client"
        const val DEVICE_TYPE_WINCCOA_CLIENT = "WinCCOA-Client"
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
    val removePath: Boolean = true, // Remove base path before wildcard
    val qos: Int = 0               // QoS level (0, 1, or 2)
) {
    companion object {
        const val MODE_SUBSCRIBE = "SUBSCRIBE"
        const val MODE_PUBLISH = "PUBLISH"

        fun fromJsonObject(json: JsonObject): MqttClientAddress {
            return MqttClientAddress(
                mode = json.getString("mode"),
                remoteTopic = json.getString("remoteTopic"),
                localTopic = json.getString("localTopic"),
                removePath = json.getBoolean("removePath", true),
                qos = json.getInteger("qos", 0)
            )
        }
    }

    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("mode", mode)
            .put("remoteTopic", remoteTopic)
            .put("localTopic", localTopic)
            .put("removePath", removePath)
            .put("qos", qos)
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

        if (qos !in 0..2) {
            errors.add("qos must be 0, 1, or 2")
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
    val addresses: List<MqttClientAddress> = emptyList(),
    // Disconnected buffer configuration (for handling messages when connection is lost)
    val bufferEnabled: Boolean = false,
    val bufferSize: Int = 5000,
    val persistBuffer: Boolean = false,
    val deleteOldestMessages: Boolean = true
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
                    addresses = addresses,
                    bufferEnabled = json.getBoolean("bufferEnabled", false),
                    bufferSize = json.getInteger("bufferSize", 5000),
                    persistBuffer = json.getBoolean("persistBuffer", false),
                    deleteOldestMessages = json.getBoolean("deleteOldestMessages", true)
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
            .put("bufferEnabled", bufferEnabled)
            .put("bufferSize", bufferSize)
            .put("persistBuffer", persistBuffer)
            .put("deleteOldestMessages", deleteOldestMessages)

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

        if (bufferSize < 1 || bufferSize > 100000) {
            errors.add("bufferSize must be between 1 and 100000")
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

/**
 * WinCC OA Bridge address configuration for subscriptions
 */
data class WinCCOaAddress(
    val query: String,              // GraphQL query for dpQueryConnectSingle (e.g., "SELECT '_original.._value', '_original.._stime' FROM 'System1:*'")
    val topic: String,              // MQTT topic prefix (e.g., "winccoa/plant1")
    val description: String = "",   // Optional description for this address
    val answer: Boolean = false,    // Whether to request the answer row in dpQueryConnectSingle (initial value)
    val retained: Boolean = false   // Whether to publish MQTT messages with retained flag
) {
    companion object {
        fun fromJsonObject(json: JsonObject): WinCCOaAddress {
            return WinCCOaAddress(
                query = json.getString("query"),
                topic = json.getString("topic"),
                description = json.getString("description", ""),
                answer = json.getBoolean("answer", false),
                retained = json.getBoolean("retained", false)
            )
        }
    }

    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("query", query)
            .put("topic", topic)
            .put("description", description)
            .put("answer", answer)
            .put("retained", retained)
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()

        if (query.isBlank()) {
            errors.add("query cannot be blank")
        }

        if (topic.isBlank()) {
            errors.add("topic cannot be blank")
        }

        return errors
    }
}

/**
 * WinCC OA topic transformation configuration
 */
data class WinCCOaTransformConfig(
    val removeSystemName: Boolean = true,          // Remove "System1:" prefix from datapoint names
    val convertDotToSlash: Boolean = true,         // Convert "." to "/" in datapoint names
    val convertUnderscoreToSlash: Boolean = false, // Convert "_" to "/" in datapoint names
    val regexPattern: String? = null,              // Additional regex pattern for conversion (e.g., "([0-9]+)" to "$1")
    val regexReplacement: String? = null           // Replacement string for regex pattern
) {
    companion object {
        fun fromJsonObject(json: JsonObject): WinCCOaTransformConfig {
            return WinCCOaTransformConfig(
                removeSystemName = json.getBoolean("removeSystemName", true),
                convertDotToSlash = json.getBoolean("convertDotToSlash", true),
                convertUnderscoreToSlash = json.getBoolean("convertUnderscoreToSlash", false),
                regexPattern = json.getString("regexPattern"),
                regexReplacement = json.getString("regexReplacement")
            )
        }
    }

    fun toJsonObject(): JsonObject {
        val obj = JsonObject()
            .put("removeSystemName", removeSystemName)
            .put("convertDotToSlash", convertDotToSlash)
            .put("convertUnderscoreToSlash", convertUnderscoreToSlash)

        if (regexPattern != null) obj.put("regexPattern", regexPattern)
        if (regexReplacement != null) obj.put("regexReplacement", regexReplacement)

        return obj
    }

    /**
     * Transform a datapoint name to MQTT topic using configured rules
     */
    fun transformDpNameToTopic(dpName: String): String {
        var result = dpName

        // Remove system name if configured
        if (removeSystemName) {
            val colonIndex = result.indexOf(':')
            if (colonIndex > 0) {
                result = result.substring(colonIndex + 1)
            }
        }

        // Remove trailing dots before conversion (e.g., "ExampleDP_Result." -> "ExampleDP_Result")
        // This prevents empty topic levels after dot-to-slash conversion
        result = result.trimEnd('.')

        // Convert dots to slashes
        if (convertDotToSlash) {
            result = result.replace('.', '/')
        }

        // Convert underscores to slashes
        if (convertUnderscoreToSlash) {
            result = result.replace('_', '/')
        }

        // Apply regex transformation if configured
        if (regexPattern != null && regexReplacement != null) {
            try {
                result = result.replace(Regex(regexPattern), regexReplacement)
            } catch (e: Exception) {
                // Log error but continue with untransformed result
            }
        }

        // Final cleanup: remove any leading/trailing slashes to avoid empty topic levels
        // This handles edge cases where transformations might create leading/trailing slashes
        result = result.trim('/')

        return result
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()

        // Validate regex pattern if provided
        if (regexPattern != null) {
            try {
                Regex(regexPattern)
            } catch (e: Exception) {
                errors.add("Invalid regex pattern: ${e.message}")
            }
        }

        // Regex replacement requires regex pattern
        if (regexReplacement != null && regexPattern == null) {
            errors.add("regexReplacement requires regexPattern to be set")
        }

        return errors
    }
}

/**
 * WinCC OA message format configuration
 */
enum class WinCCOaMessageFormat {
    JSON_ISO,    // { "value": <value>, "time": <ISO timestamp> }
    JSON_MS,     // { "value": <value>, "time": <ms since epoch> }
    RAW_VALUE    // Just the plain value (as text or binary for BLOBs)
}

/**
 * WinCC OA connection configuration parameters
 */
data class WinCCOaConnectionConfig(
    val graphqlEndpoint: String,                            // GraphQL endpoint URL (e.g., "http://winccoa:4000/graphql")
    val websocketEndpoint: String? = null,                  // WebSocket endpoint URL (e.g., "ws://winccoa:4000/graphql"), defaults to graphqlEndpoint with ws:// protocol
    val username: String? = null,                           // Username for authentication
    val password: String? = null,                           // Password for authentication
    val token: String? = null,                              // Direct token (if provided, skips login)
    val reconnectDelay: Long = 5000,                        // Reconnection delay in ms
    val connectionTimeout: Long = 10000,                    // Connection timeout in ms
    val addresses: List<WinCCOaAddress> = emptyList(),      // Configured addresses (queries)
    val transformConfig: WinCCOaTransformConfig = WinCCOaTransformConfig(), // Topic transformation rules
    val messageFormat: String = "JSON_ISO"                  // Message format (JSON_ISO, JSON_MS, RAW_VALUE)
) {
    companion object {
        const val FORMAT_JSON_ISO = "JSON_ISO"
        const val FORMAT_JSON_MS = "JSON_MS"
        const val FORMAT_RAW_VALUE = "RAW_VALUE"

        fun fromJsonObject(json: JsonObject): WinCCOaConnectionConfig {
            try {
                val addresses = try {
                    json.getJsonArray("addresses")?.map { addressObj ->
                        WinCCOaAddress.fromJsonObject(addressObj as JsonObject)
                    } ?: emptyList()
                } catch (e: Exception) {
                    println("Error parsing addresses: ${e.message}")
                    emptyList()
                }

                val transformConfig = try {
                    json.getJsonObject("transformConfig")?.let {
                        WinCCOaTransformConfig.fromJsonObject(it)
                    } ?: WinCCOaTransformConfig()
                } catch (e: Exception) {
                    println("Error parsing transformConfig: ${e.message}")
                    WinCCOaTransformConfig()
                }

                return WinCCOaConnectionConfig(
                    graphqlEndpoint = json.getString("graphqlEndpoint", "http://winccoa:4000/graphql"),
                    websocketEndpoint = json.getString("websocketEndpoint"),
                    username = json.getString("username"),
                    password = json.getString("password"),
                    token = json.getString("token"),
                    reconnectDelay = json.getLong("reconnectDelay", 5000),
                    connectionTimeout = json.getLong("connectionTimeout", 10000),
                    addresses = addresses,
                    transformConfig = transformConfig,
                    messageFormat = json.getString("messageFormat", FORMAT_JSON_ISO)
                )
            } catch (e: Exception) {
                println("Overall error in WinCCOaConnectionConfig.fromJsonObject: ${e.message}")
                throw e
            }
        }
    }

    fun toJsonObject(): JsonObject {
        val result = JsonObject()
            .put("graphqlEndpoint", graphqlEndpoint)
            .put("websocketEndpoint", websocketEndpoint)
            .put("username", username)
            .put("password", password)
            .put("token", token)
            .put("reconnectDelay", reconnectDelay)
            .put("connectionTimeout", connectionTimeout)
            .put("transformConfig", transformConfig.toJsonObject())
            .put("messageFormat", messageFormat)

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

        if (graphqlEndpoint.isBlank()) {
            errors.add("graphqlEndpoint cannot be blank")
        }

        if (!graphqlEndpoint.startsWith("http://") && !graphqlEndpoint.startsWith("https://")) {
            errors.add("graphqlEndpoint must start with http:// or https://")
        }

        if (reconnectDelay < 1000) {
            errors.add("reconnectDelay should be at least 1000ms")
        }

        if (connectionTimeout < 1000) {
            errors.add("connectionTimeout should be at least 1000ms")
        }

        // Validate that if username is provided, password must also be provided (and vice versa)
        if ((username != null && password == null) || (username == null && password != null)) {
            errors.add("Both username and password must be provided together, or both omitted for anonymous access")
        }

        // Validate message format
        if (messageFormat !in listOf(FORMAT_JSON_ISO, FORMAT_JSON_MS, FORMAT_RAW_VALUE)) {
            errors.add("messageFormat must be one of: $FORMAT_JSON_ISO, $FORMAT_JSON_MS, $FORMAT_RAW_VALUE")
        }

        // Validate addresses
        addresses.forEachIndexed { index, address ->
            val addressErrors = address.validate()
            addressErrors.forEach { error ->
                errors.add("Address $index: $error")
            }
        }

        // Validate transform config
        val transformErrors = transformConfig.validate()
        transformErrors.forEach { error ->
            errors.add("Transform config: $error")
        }

        return errors
    }

    /**
     * Get the WebSocket endpoint URL (defaults to graphqlEndpoint with ws:// protocol if not specified)
     */
    fun getWebSocketEndpoint(): String {
        if (websocketEndpoint != null) {
            return websocketEndpoint
        }

        // Convert http(s):// to ws(s)://
        return when {
            graphqlEndpoint.startsWith("https://") -> "wss://" + graphqlEndpoint.substring(8)
            graphqlEndpoint.startsWith("http://") -> "ws://" + graphqlEndpoint.substring(7)
            else -> graphqlEndpoint
        }
    }
}