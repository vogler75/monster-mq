package at.rocworks.stores.devices

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/**
 * User property key-value pair for MQTT v5 messages
 */
data class UserProperty(
    val key: String,
    val value: String
)

/**
 * MQTT Client address configuration for subscriptions and publications
 */
data class MqttClientAddress(
    val mode: String,              // "SUBSCRIBE" or "PUBLISH"
    val remoteTopic: String,       // Remote MQTT topic (with wildcards)
    val localTopic: String,        // Local MQTT topic destination/source
    val removePath: Boolean = true, // Remove base path before wildcard
    val qos: Int = 0,              // QoS level (0, 1, or 2)
    // MQTT v5 Subscription Options (only for SUBSCRIBE mode)
    val noLocal: Boolean = false,  // Don't receive messages you published
    val retainHandling: Int = 0,   // 0=Send retained, 1=Send retained only if new sub, 2=Never send retained
    val retainAsPublished: Boolean = false,  // Preserve original retain flag when forwarding
    // MQTT v5 Message Properties (only for PUBLISH mode)
    val messageExpiryInterval: Long? = null,  // Message expiry in seconds (null = no expiry)
    val contentType: String? = null,  // MIME type of the payload
    val responseTopicPattern: String? = null,  // Topic for request-response pattern
    val payloadFormatIndicator: Boolean = false,  // Payload is UTF-8 text
    val userProperties: List<UserProperty> = emptyList()  // Custom key-value pairs
) {
    companion object {
        const val MODE_SUBSCRIBE = "SUBSCRIBE"
        const val MODE_PUBLISH = "PUBLISH"

        fun fromJsonObject(json: JsonObject): MqttClientAddress {
            val userProperties = try {
                json.getJsonArray("userProperties")?.map { propObj ->
                    val prop = propObj as JsonObject
                    UserProperty(prop.getString("key"), prop.getString("value"))
                } ?: emptyList()
            } catch (e: Exception) {
                emptyList()
            }
            
            return MqttClientAddress(
                mode = json.getString("mode"),
                remoteTopic = json.getString("remoteTopic"),
                localTopic = json.getString("localTopic"),
                removePath = json.getBoolean("removePath", true),
                qos = json.getInteger("qos", 0),
                noLocal = json.getBoolean("noLocal", false),
                retainHandling = json.getInteger("retainHandling", 0),
                retainAsPublished = json.getBoolean("retainAsPublished", false),
                messageExpiryInterval = json.getLong("messageExpiryInterval"),
                contentType = json.getString("contentType"),
                responseTopicPattern = json.getString("responseTopicPattern"),
                payloadFormatIndicator = json.getBoolean("payloadFormatIndicator", false),
                userProperties = userProperties
            )
        }
    }

    fun toJsonObject(): JsonObject {
        val result = JsonObject()
            .put("mode", mode)
            .put("remoteTopic", remoteTopic)
            .put("localTopic", localTopic)
            .put("removePath", removePath)
            .put("qos", qos)
            .put("noLocal", noLocal)
            .put("retainHandling", retainHandling)
            .put("retainAsPublished", retainAsPublished)
            .put("payloadFormatIndicator", payloadFormatIndicator)
        
        // Add optional message properties
        messageExpiryInterval?.let { result.put("messageExpiryInterval", it) }
        contentType?.let { result.put("contentType", it) }
        responseTopicPattern?.let { result.put("responseTopicPattern", it) }
        
        // Add user properties if present
        if (userProperties.isNotEmpty()) {
            val userPropsArray = JsonArray()
            userProperties.forEach { prop ->
                userPropsArray.add(JsonObject().put("key", prop.key).put("value", prop.value))
            }
            result.put("userProperties", userPropsArray)
        }
        
        return result
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
    val brokerUrl: String,         // Complete broker URL (e.g., "tcp://host:1883", "ssl://host:8883", "wss://host:443/mqtt")
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
    val deleteOldestMessages: Boolean = true,
    // Loop prevention (prevents bridge from republishing its own messages)
    val loopPrevention: Boolean = true,
    // SSL/TLS configuration
    val sslVerifyCertificate: Boolean = true,  // Verify SSL certificates (disable for self-signed certificates)
    // MQTT v5 properties
    val protocolVersion: Int = 4,  // 4 for MQTT v3.1.1, 5 for MQTT v5.0
    val sessionExpiryInterval: Long? = null,  // Session expiry interval in seconds (MQTT v5 only, 0-4294967295)
    val receiveMaximum: Int? = null,  // Maximum number of QoS 1 and QoS 2 publications to process concurrently (MQTT v5 only, 1-65535)
    val maximumPacketSize: Long? = null,  // Maximum packet size in bytes (MQTT v5 only, 1-268435455)
    val topicAliasMaximum: Int? = null  // Maximum number of topic aliases (MQTT v5 only, 0-65535)
) {
    companion object {
        const val PROTOCOL_TCP = "tcp"
        const val PROTOCOL_SSL = "ssl"  // Changed from PROTOCOL_TCPS for Paho compatibility
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
                    brokerUrl = json.getString("brokerUrl") ?: "tcp://localhost:1883",
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
                    deleteOldestMessages = json.getBoolean("deleteOldestMessages", true),
                    loopPrevention = json.getBoolean("loopPrevention", true),
                    // Default to true (secure by default) - can be disabled for self-signed certificates
                    sslVerifyCertificate = when (val value = json.getValue("sslVerifyCertificate")) {
                        is Boolean -> value
                        else -> true
                    },
                    // MQTT v5 properties
                    protocolVersion = json.getInteger("protocolVersion", 4),
                    sessionExpiryInterval = json.getLong("sessionExpiryInterval"),
                    receiveMaximum = json.getInteger("receiveMaximum"),
                    maximumPacketSize = json.getLong("maximumPacketSize"),
                    topicAliasMaximum = json.getInteger("topicAliasMaximum")
                )
            } catch (e: Exception) {
                println("Overall error in fromJsonObject: ${e.message}")
                throw e
            }
        }
    }

    fun toJsonObject(): JsonObject {
        val result = JsonObject()
            .put("brokerUrl", brokerUrl)
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
            .put("loopPrevention", loopPrevention)
            .put("sslVerifyCertificate", sslVerifyCertificate)
            .put("protocolVersion", protocolVersion)

        // Add MQTT v5 properties only if they are set
        sessionExpiryInterval?.let { result.put("sessionExpiryInterval", it) }
        receiveMaximum?.let { result.put("receiveMaximum", it) }
        maximumPacketSize?.let { result.put("maximumPacketSize", it) }
        topicAliasMaximum?.let { result.put("topicAliasMaximum", it) }

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

    /**
     * Extract protocol from broker URL
     */
    fun getProtocol(): String? {
        return try {
            val uri = java.net.URI(brokerUrl)
            uri.scheme
        } catch (e: Exception) {
            null
        }
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()

        if (brokerUrl.isBlank()) {
            errors.add("brokerUrl cannot be blank")
        }

        // Validate URL format and protocol
        try {
            val uri = java.net.URI(brokerUrl)
            val protocol = uri.scheme

            if (protocol.isNullOrBlank()) {
                errors.add("brokerUrl must include a protocol (tcp://, ssl://, ws://, or wss://)")
            } else if (protocol !in listOf(PROTOCOL_TCP, PROTOCOL_SSL, PROTOCOL_WS, PROTOCOL_WSS)) {
                errors.add("protocol must be one of: $PROTOCOL_TCP, $PROTOCOL_SSL, $PROTOCOL_WS, $PROTOCOL_WSS")
            }

            if (uri.host.isNullOrBlank()) {
                errors.add("brokerUrl must include a hostname")
            }

            // Port is optional - default ports will be used by Paho:
            // tcp:// = 1883, ssl:// = 8883, ws:// = 80, wss:// = 443
        } catch (e: Exception) {
            errors.add("invalid brokerUrl format: ${e.message}")
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
}
