package at.rocworks.stores.devices

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/**
 * SparkplugB Decoder Rule - Defines regex-based routing for SparkplugB messages
 */
data class SparkplugBDecoderRule(
    val name: String,                           // Rule identifier (e.g., "acme-devices")
    val nodeIdRegex: String,                    // Regex pattern to match SparkplugB node ID (e.g., "^acme\\..*")
    val deviceIdRegex: String,                  // Regex pattern to match SparkplugB device ID (e.g., ".*")
    val destinationTopic: String,               // MQTT topic template with variables $nodeId, $deviceId
    val transformations: Map<String, String> = emptyMap() // Optional transformations: nodeId/deviceId -> regex substitution
) {
    companion object {
        fun fromJsonObject(json: JsonObject): SparkplugBDecoderRule {
            val transformationsJson = json.getJsonObject("transformations", JsonObject())
            val transformations = transformationsJson.map.entries.associate {
                it.key to it.value.toString()
            }

            return SparkplugBDecoderRule(
                name = json.getString("name"),
                nodeIdRegex = json.getString("nodeIdRegex"),
                deviceIdRegex = json.getString("deviceIdRegex"),
                destinationTopic = json.getString("destinationTopic"),
                transformations = transformations
            )
        }
    }

    fun toJsonObject(): JsonObject {
        val transformationsJson = JsonObject()
        transformations.forEach { (key, value) ->
            transformationsJson.put(key, value)
        }

        return JsonObject()
            .put("name", name)
            .put("nodeIdRegex", nodeIdRegex)
            .put("deviceIdRegex", deviceIdRegex)
            .put("destinationTopic", destinationTopic)
            .put("transformations", transformationsJson)
    }

    /**
     * Validate the rule configuration
     */
    fun validate(): List<String> {
        val errors = mutableListOf<String>()

        if (name.isBlank()) {
            errors.add("Rule name cannot be blank")
        }

        if (nodeIdRegex.isBlank()) {
            errors.add("nodeIdRegex cannot be blank")
        } else {
            try {
                Regex(nodeIdRegex)
            } catch (e: Exception) {
                errors.add("Invalid nodeIdRegex pattern: ${e.message}")
            }
        }

        if (deviceIdRegex.isBlank()) {
            errors.add("deviceIdRegex cannot be blank")
        } else {
            try {
                Regex(deviceIdRegex)
            } catch (e: Exception) {
                errors.add("Invalid deviceIdRegex pattern: ${e.message}")
            }
        }

        if (destinationTopic.isBlank()) {
            errors.add("destinationTopic cannot be blank")
        }

        // Validate transformation keys
        transformations.keys.forEach { key ->
            if (key !in listOf("nodeId", "deviceId")) {
                errors.add("Invalid transformation key: $key (must be 'nodeId' or 'deviceId')")
            }
        }

        // Validate transformation regex substitution syntax
        transformations.forEach { (key, value) ->
            val parseError = parseRegexSubstitution(value)
            if (parseError != null) {
                errors.add("Invalid transformation for '$key': $parseError")
            }
        }

        return errors
    }

    /**
     * Parse regex substitution string (s/pattern/replacement/flags)
     * Returns error message if invalid, null if valid
     */
    private fun parseRegexSubstitution(substitution: String): String? {
        if (!substitution.startsWith("s/")) {
            return "Transformation must start with 's/' (e.g., 's/pattern/replacement/flags')"
        }

        // Parse s/pattern/replacement/flags format
        val parts = substitution.substring(2).split("/")
        if (parts.size < 2) {
            return "Invalid format. Expected: s/pattern/replacement/flags"
        }

        val pattern = parts[0]
        if (pattern.isEmpty()) {
            return "Pattern cannot be empty"
        }

        // Validate regex pattern
        try {
            Regex(pattern)
        } catch (e: Exception) {
            return "Invalid regex pattern: ${e.message}"
        }

        return null
    }

    /**
     * Apply regex transformation to a value
     * Supports Perl-style s/pattern/replacement/flags syntax
     *
     * Example: s/\./\//g transforms "acme.building1" to "acme/building1"
     */
    fun applyTransformation(key: String, value: String): String {
        val transformation = transformations[key] ?: return value

        if (!transformation.startsWith("s/")) {
            return value // Invalid format, return original
        }

        try {
            // Parse s/pattern/replacement/flags
            val parts = transformation.substring(2).split("/")
            if (parts.size < 2) {
                return value // Invalid format
            }

            val pattern = parts[0]
            val replacement = parts.getOrElse(1) { "" }
            val flags = parts.getOrElse(2) { "" }

            val regex = Regex(pattern)

            // Check for 'g' flag (global replace - default in Kotlin replace())
            // In Kotlin, replace() replaces all occurrences by default
            return regex.replace(value, replacement)

        } catch (e: Exception) {
            // If transformation fails, return original value
            return value
        }
    }

    /**
     * Check if nodeId and deviceId match this rule's patterns
     */
    fun matches(nodeId: String, deviceId: String): Boolean {
        return try {
            val nodeIdMatches = Regex(nodeIdRegex).matches(nodeId)
            val deviceIdMatches = Regex(deviceIdRegex).matches(deviceId)
            nodeIdMatches && deviceIdMatches
        } catch (e: Exception) {
            false
        }
    }

    /**
     * Build the destination topic by applying transformations and substituting template variables
     */
    fun buildDestinationTopic(nodeId: String, deviceId: String): String {
        // Apply transformations to nodeId and deviceId
        val transformedNodeId = applyTransformation("nodeId", nodeId)
        val transformedDeviceId = applyTransformation("deviceId", deviceId)

        // Substitute template variables
        var topic = destinationTopic
            .replace("\$nodeId", transformedNodeId)
            .replace("\$deviceId", transformedDeviceId)

        // Clean up: remove empty deviceId path segments (e.g., "decoded/node//" -> "decoded/node/")
        // This handles node-level messages where deviceId is empty
        topic = topic.replace(Regex("/+"), "/")  // Replace multiple slashes with single slash
        if (topic.endsWith("/")) {
            topic = topic.dropLast(1)  // Remove trailing slash
        }

        return topic
    }
}

/**
 * SparkplugB Decoder Configuration
 */
data class SparkplugBDecoderConfig(
    val sourceNamespace: String = "spBv1.0",    // Source SparkplugB namespace (default: "spBv1.0")
    val rules: List<SparkplugBDecoderRule> = emptyList() // Ordered list of decoding rules (first match wins)
) {
    companion object {
        fun fromJsonObject(json: JsonObject): SparkplugBDecoderConfig {
            val rulesArray = json.getJsonArray("rules", JsonArray())
            val rules = rulesArray.mapNotNull { ruleObj ->
                try {
                    SparkplugBDecoderRule.fromJsonObject(ruleObj as JsonObject)
                } catch (e: Exception) {
                    null // Skip invalid rules
                }
            }

            return SparkplugBDecoderConfig(
                sourceNamespace = json.getString("sourceNamespace", "spBv1.0"),
                rules = rules
            )
        }
    }

    fun toJsonObject(): JsonObject {
        val rulesArray = JsonArray()
        rules.forEach { rule ->
            rulesArray.add(rule.toJsonObject())
        }

        return JsonObject()
            .put("sourceNamespace", sourceNamespace)
            .put("rules", rulesArray)
    }

    /**
     * Validate the decoder configuration
     */
    fun validate(): List<String> {
        val errors = mutableListOf<String>()

        if (sourceNamespace.isBlank()) {
            errors.add("sourceNamespace cannot be blank")
        }

        if (rules.isEmpty()) {
            errors.add("At least one rule must be defined")
        }

        // Check for duplicate rule names
        val ruleNames = rules.map { it.name }
        val duplicates = ruleNames.groupingBy { it }.eachCount().filter { it.value > 1 }.keys
        if (duplicates.isNotEmpty()) {
            errors.add("Duplicate rule names found: ${duplicates.joinToString(", ")}")
        }

        // Validate each rule
        rules.forEachIndexed { index, rule ->
            val ruleErrors = rule.validate()
            ruleErrors.forEach { error ->
                errors.add("Rule ${index + 1} ('${rule.name}'): $error")
            }
        }

        return errors
    }

    /**
     * Find the first rule that matches the given nodeId and deviceId
     * Returns null if no rule matches
     */
    fun findMatchingRule(nodeId: String, deviceId: String): SparkplugBDecoderRule? {
        return rules.firstOrNull { rule ->
            rule.matches(nodeId, deviceId)
        }
    }

    /**
     * Get subscription topic filters for this decoder
     * Returns list of topics for all protobuf message types (NBIRTH, NDEATH, NDATA, NCMD, DBIRTH, DDEATH, DDATA, DCMD)
     * Excludes STATE messages which are not protobuf and not relevant for decoding
     */
    fun getSubscriptionTopics(): List<String> {
        // Subscribe only to protobuf message types, not STATE
        val messageTypes = listOf("NBIRTH", "NDEATH", "NDATA", "NCMD", "DBIRTH", "DDEATH", "DDATA", "DCMD")
        return messageTypes.map { messageType ->
            "$sourceNamespace/+/$messageType/#"
        }
    }
}
