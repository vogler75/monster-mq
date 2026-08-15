package at.rocworks.stores.devices

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.net.URI

/**
 * Custom HTTP header for i3X Client requests
 */
data class I3xHeader(
    val key: String,
    val value: String
) {
    companion object {
        fun fromJsonObject(json: JsonObject): I3xHeader {
            return I3xHeader(
                key = json.getString("key") ?: "",
                value = json.getString("value") ?: ""
            )
        }
    }

    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("key", key)
            .put("value", value)
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()
        if (key.isBlank()) {
            errors.add("header key cannot be blank")
        }
        return errors
    }
}

/**
 * i3X Client address configuration for object subscriptions and topic mapping
 */
data class I3xAddress(
    val elementId: String,                  // i3X object element ID/path (e.g. "factory/line1/motor1" or "sensors")
    val topic: String,                      // Local MQTT destination topic or prefix
    val maxDepth: Int = 1,                  // 1 = single element, 0 = unlimited / recursive subtree, N = depth limit
    val retained: Boolean = false,          // Whether to publish MQTT messages with retained flag
    val qos: Int = 0,                       // QoS level: 0, 1, or 2
    val messageFormat: String = FORMAT_RAW_VALUE, // Message format: RAW_VALUE, JSON_ISO, JSON_MS, VQT
    val removePath: Boolean = false,        // When subscribing recursively, whether to remove base elementId from sub-topic
    val description: String = ""            // Optional description
) {
    companion object {
        const val FORMAT_RAW_VALUE = "RAW_VALUE"
        const val FORMAT_JSON_ISO  = "JSON_ISO"
        const val FORMAT_JSON_MS   = "JSON_MS"
        const val FORMAT_VQT       = "VQT"

        val VALID_FORMATS = listOf(FORMAT_RAW_VALUE, FORMAT_JSON_ISO, FORMAT_JSON_MS, FORMAT_VQT)

        fun fromJsonObject(json: JsonObject): I3xAddress {
            return I3xAddress(
                elementId = json.getString("elementId") ?: "",
                topic = json.getString("topic") ?: "",
                maxDepth = json.getInteger("maxDepth", 1),
                retained = json.getBoolean("retained", false),
                qos = json.getInteger("qos", 0),
                messageFormat = json.getString("messageFormat", FORMAT_RAW_VALUE),
                removePath = json.getBoolean("removePath", false),
                description = json.getString("description", "")
            )
        }
    }

    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("elementId", elementId)
            .put("topic", topic)
            .put("maxDepth", maxDepth)
            .put("retained", retained)
            .put("qos", qos)
            .put("messageFormat", messageFormat)
            .put("removePath", removePath)
            .put("description", description)
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()
        if (elementId.isBlank()) {
            errors.add("elementId cannot be blank")
        }
        if (topic.isBlank()) {
            errors.add("topic cannot be blank")
        }
        if (maxDepth < 0) {
            errors.add("maxDepth must be >= 0 (0 means recursive subtree)")
        }
        if (qos !in 0..2) {
            errors.add("qos must be 0, 1, or 2")
        }
        if (messageFormat !in VALID_FORMATS) {
            errors.add("messageFormat must be one of: ${VALID_FORMATS.joinToString(", ")}")
        }
        return errors
    }
}

/**
 * i3X Client connection configuration parameters
 */
data class I3xConnectionConfig(
    val url: String,                                        // i3X server endpoint URL (e.g., "http://localhost:3002/i3x/v1")
    val authType: String = AUTH_TYPE_NONE,                  // NONE, BASIC, BEARER, CUSTOM_HEADERS
    val username: String? = null,                           // Username for Basic auth
    val password: String? = null,                           // Password for Basic auth
    val token: String? = null,                              // Bearer token
    val headers: List<I3xHeader> = emptyList(),             // Custom HTTP headers
    val clientId: String = DEFAULT_CLIENT_ID,               // Client ID used when registering i3X subscriptions
    val reconnectDelay: Long = 5000L,                       // Reconnection delay in milliseconds
    val connectionTimeout: Long = 10000L,                   // Connection timeout in milliseconds
    val addresses: List<I3xAddress> = emptyList()           // Configured object subscriptions
) {
    companion object {
        const val AUTH_TYPE_NONE           = "NONE"
        const val AUTH_TYPE_BASIC          = "BASIC"
        const val AUTH_TYPE_BEARER         = "BEARER"
        const val AUTH_TYPE_CUSTOM_HEADERS = "CUSTOM_HEADERS"

        val VALID_AUTH_TYPES = listOf(AUTH_TYPE_NONE, AUTH_TYPE_BASIC, AUTH_TYPE_BEARER, AUTH_TYPE_CUSTOM_HEADERS)
        const val DEFAULT_CLIENT_ID = "monstermq-i3x-client"

        fun fromJsonObject(json: JsonObject): I3xConnectionConfig {
            val headersList = try {
                json.getJsonArray("headers")?.map { headerObj ->
                    I3xHeader.fromJsonObject(headerObj as JsonObject)
                } ?: emptyList()
            } catch (e: Exception) {
                emptyList()
            }

            val addressesList = try {
                json.getJsonArray("addresses")?.map { addrObj ->
                    I3xAddress.fromJsonObject(addrObj as JsonObject)
                } ?: emptyList()
            } catch (e: Exception) {
                emptyList()
            }

            return I3xConnectionConfig(
                url = json.getString("url") ?: "http://localhost:3002/i3x/v1",
                authType = json.getString("authType", AUTH_TYPE_NONE),
                username = json.getString("username"),
                password = json.getString("password"),
                token = json.getString("token"),
                headers = headersList,
                clientId = json.getString("clientId", DEFAULT_CLIENT_ID).ifBlank { DEFAULT_CLIENT_ID },
                reconnectDelay = json.getLong("reconnectDelay", 5000L),
                connectionTimeout = json.getLong("connectionTimeout", 10000L),
                addresses = addressesList
            )
        }
    }

    fun toJsonObject(): JsonObject {
        val obj = JsonObject()
            .put("url", url)
            .put("authType", authType)
            .put("clientId", clientId)
            .put("reconnectDelay", reconnectDelay)
            .put("connectionTimeout", connectionTimeout)

        if (username != null) obj.put("username", username)
        if (password != null) obj.put("password", password)
        if (token != null) obj.put("token", token)

        if (headers.isNotEmpty()) {
            val headersArray = JsonArray()
            headers.forEach { headersArray.add(it.toJsonObject()) }
            obj.put("headers", headersArray)
        }

        if (addresses.isNotEmpty()) {
            val addressArray = JsonArray()
            addresses.forEach { addressArray.add(it.toJsonObject()) }
            obj.put("addresses", addressArray)
        }

        return obj
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()

        if (url.isBlank()) {
            errors.add("url cannot be blank")
        } else {
            try {
                val uri = URI(url)
                val scheme = uri.scheme?.lowercase()
                if (scheme != "http" && scheme != "https") {
                    errors.add("url must start with http:// or https://")
                }
                if (uri.host.isNullOrBlank()) {
                    errors.add("url must specify a valid host")
                }
            } catch (e: Exception) {
                errors.add("invalid url format: ${e.message}")
            }
        }

        if (authType !in VALID_AUTH_TYPES) {
            errors.add("authType must be one of: ${VALID_AUTH_TYPES.joinToString(", ")}")
        }

        if (authType == AUTH_TYPE_BASIC) {
            if (username.isNullOrBlank()) {
                errors.add("username is required for BASIC auth")
            }
            if (password.isNullOrBlank()) {
                errors.add("password is required for BASIC auth")
            }
        }

        if (authType == AUTH_TYPE_BEARER) {
            if (token.isNullOrBlank()) {
                errors.add("token is required for BEARER auth")
            }
        }

        if (clientId.isBlank()) {
            errors.add("clientId cannot be blank")
        }

        if (reconnectDelay < 1000) {
            errors.add("reconnectDelay should be at least 1000ms")
        }

        if (connectionTimeout < 1000) {
            errors.add("connectionTimeout should be at least 1000ms")
        }

        headers.forEachIndexed { index, header ->
            header.validate().forEach { err ->
                errors.add("Header $index: $err")
            }
        }

        addresses.forEachIndexed { index, address ->
            address.validate().forEach { err ->
                errors.add("Address $index: $err")
            }
        }

        return errors
    }

    /**
     * Normalized base URL without trailing slashes
     */
    fun normalizedBaseUrl(): String {
        return url.trim().trimEnd('/')
    }
}
