package at.rocworks.stores.devices

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

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
