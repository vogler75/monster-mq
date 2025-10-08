package at.rocworks.stores.devices

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/**
 * WinCC Unified address type
 */
enum class WinCCUaAddressType {
    TAG_VALUES,      // Subscribe to tag value changes
    ACTIVE_ALARMS    // Subscribe to active alarms
}

/**
 * WinCC Unified Client address configuration for subscriptions
 */
data class WinCCUaAddress(
    val type: WinCCUaAddressType,           // Address type: TAG_VALUES or ACTIVE_ALARMS
    val topic: String,                      // MQTT topic prefix (e.g., "tags" or "alarms")
    val description: String = "",           // Optional description for this address
    val retained: Boolean = false,          // Whether to publish MQTT messages with retained flag

    // For TAG_VALUES type
    val nameFilters: List<String>? = null,   // Name filters for tag browsing (e.g., ["HMI_*", "TANK_*"])
    val includeQuality: Boolean = false,      // Whether to include quality information in TAG_VALUES data

    // For ACTIVE_ALARMS type
    val systemNames: List<String>? = null,   // Optional system names filter for alarms
    val filterString: String? = null         // Optional filter string for alarms
) {
    companion object {
        fun fromJsonObject(json: JsonObject): WinCCUaAddress {
            val typeStr = json.getString("type", "TAG_VALUES")
            val type = try {
                WinCCUaAddressType.valueOf(typeStr)
            } catch (e: Exception) {
                WinCCUaAddressType.TAG_VALUES
            }

            val systemNamesList = json.getJsonArray("systemNames")?.map { it.toString() }

            // Handle nameFilters (new format) or convert from browseArguments (legacy format)
            val nameFiltersList = json.getJsonArray("nameFilters")?.map { it.toString() }
                ?: json.getJsonObject("browseArguments")?.getString("filter")?.let { listOf(it) }

            return WinCCUaAddress(
                type = type,
                topic = json.getString("topic"),
                description = json.getString("description", ""),
                retained = json.getBoolean("retained", false),
                nameFilters = nameFiltersList,
                includeQuality = json.getBoolean("includeQuality", false),
                systemNames = systemNamesList,
                filterString = json.getString("filterString")
            )
        }
    }

    fun toJsonObject(): JsonObject {
        val obj = JsonObject()
            .put("type", type.name)
            .put("topic", topic)
            .put("description", description)
            .put("retained", retained)

        if (nameFilters != null) {
            obj.put("nameFilters", JsonArray(nameFilters))
        }

        if (type == WinCCUaAddressType.TAG_VALUES) {
            obj.put("includeQuality", includeQuality)
        }

        if (systemNames != null) {
            obj.put("systemNames", JsonArray(systemNames))
        }

        if (filterString != null) {
            obj.put("filterString", filterString)
        }

        return obj
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()

        if (topic.isBlank()) {
            errors.add("topic cannot be blank")
        }

        when (type) {
            WinCCUaAddressType.TAG_VALUES -> {
                if (nameFilters == null || nameFilters.isEmpty()) {
                    errors.add("nameFilters is required for TAG_VALUES address type")
                }
            }
            WinCCUaAddressType.ACTIVE_ALARMS -> {
                // systemNames and filterString are optional for alarms
            }
        }

        return errors
    }
}

/**
 * WinCC Unified topic transformation configuration
 */
data class WinCCUaTransformConfig(
    val convertDotToSlash: Boolean = true,         // Convert "." to "/" in tag names
    val convertUnderscoreToSlash: Boolean = false, // Convert "_" to "/" in tag names
    val regexPattern: String? = null,              // Additional regex pattern for conversion
    val regexReplacement: String? = null           // Replacement string for regex pattern
) {
    companion object {
        fun fromJsonObject(json: JsonObject): WinCCUaTransformConfig {
            return WinCCUaTransformConfig(
                convertDotToSlash = json.getBoolean("convertDotToSlash", true),
                convertUnderscoreToSlash = json.getBoolean("convertUnderscoreToSlash", false),
                regexPattern = json.getString("regexPattern"),
                regexReplacement = json.getString("regexReplacement")
            )
        }
    }

    fun toJsonObject(): JsonObject {
        val obj = JsonObject()
            .put("convertDotToSlash", convertDotToSlash)
            .put("convertUnderscoreToSlash", convertUnderscoreToSlash)

        if (regexPattern != null) obj.put("regexPattern", regexPattern)
        if (regexReplacement != null) obj.put("regexReplacement", regexReplacement)

        return obj
    }

    /**
     * Transform a tag name to MQTT topic using configured rules
     */
    fun transformTagNameToTopic(tagName: String): String {
        var result = tagName

        // Remove trailing dots before conversion
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

        // Final cleanup: remove any leading/trailing slashes
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
 * WinCC Unified message format configuration
 */
enum class WinCCUaMessageFormat {
    JSON_ISO,    // { "value": <value>, "time": <ISO timestamp> }
    JSON_MS,     // { "value": <value>, "time": <ms since epoch> }
    RAW_VALUE    // Just the plain value
}

/**
 * WinCC Unified connection configuration parameters
 */
data class WinCCUaConnectionConfig(
    val graphqlEndpoint: String,                            // GraphQL endpoint URL (e.g., "http://winccua:4000/graphql")
    val websocketEndpoint: String? = null,                  // WebSocket endpoint URL (e.g., "ws://winccua:4000/graphql"), defaults to graphqlEndpoint with ws:// protocol
    val username: String,                                   // Username for authentication (required)
    val password: String,                                   // Password for authentication (required)
    val reconnectDelay: Long = 5000,                        // Reconnection delay in ms
    val connectionTimeout: Long = 10000,                    // Connection timeout in ms
    val addresses: List<WinCCUaAddress> = emptyList(),      // Configured addresses (subscriptions)
    val transformConfig: WinCCUaTransformConfig = WinCCUaTransformConfig(), // Topic transformation rules
    val messageFormat: String = "JSON_ISO"                  // Message format (JSON_ISO, JSON_MS, RAW_VALUE)
) {
    companion object {
        const val FORMAT_JSON_ISO = "JSON_ISO"
        const val FORMAT_JSON_MS = "JSON_MS"
        const val FORMAT_RAW_VALUE = "RAW_VALUE"

        fun fromJsonObject(json: JsonObject): WinCCUaConnectionConfig {
            try {
                val addresses = try {
                    json.getJsonArray("addresses")?.map { addressObj ->
                        WinCCUaAddress.fromJsonObject(addressObj as JsonObject)
                    } ?: emptyList()
                } catch (e: Exception) {
                    println("Error parsing addresses: ${e.message}")
                    emptyList()
                }

                val transformConfig = try {
                    json.getJsonObject("transformConfig")?.let {
                        WinCCUaTransformConfig.fromJsonObject(it)
                    } ?: WinCCUaTransformConfig()
                } catch (e: Exception) {
                    println("Error parsing transformConfig: ${e.message}")
                    WinCCUaTransformConfig()
                }

                return WinCCUaConnectionConfig(
                    graphqlEndpoint = json.getString("graphqlEndpoint", "http://winccua:4000/graphql"),
                    websocketEndpoint = json.getString("websocketEndpoint"),
                    username = json.getString("username") ?: throw IllegalArgumentException("username is required"),
                    password = json.getString("password") ?: throw IllegalArgumentException("password is required"),
                    reconnectDelay = json.getLong("reconnectDelay", 5000),
                    connectionTimeout = json.getLong("connectionTimeout", 10000),
                    addresses = addresses,
                    transformConfig = transformConfig,
                    messageFormat = json.getString("messageFormat", FORMAT_JSON_ISO)
                )
            } catch (e: Exception) {
                println("Overall error in WinCCUaConnectionConfig.fromJsonObject: ${e.message}")
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

        if (username.isBlank()) {
            errors.add("username cannot be blank")
        }

        if (password.isBlank()) {
            errors.add("password cannot be blank")
        }

        if (reconnectDelay < 1000) {
            errors.add("reconnectDelay should be at least 1000ms")
        }

        if (connectionTimeout < 1000) {
            errors.add("connectionTimeout should be at least 1000ms")
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
