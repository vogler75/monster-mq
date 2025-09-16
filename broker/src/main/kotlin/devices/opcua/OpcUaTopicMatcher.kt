package at.rocworks.devices.opcua

import at.rocworks.data.MqttSubscription

/**
 * Utility for matching and parsing OPC UA topic patterns
 */
object OpcUaTopicMatcher {

    // OPC UA topic patterns
    private val NODE_TOPIC_PATTERN = Regex("^opcua/([a-zA-Z0-9_-]+)/node/(.+)$")
    private val PATH_TOPIC_PATTERN = Regex("^opcua/([a-zA-Z0-9_-]+)/path/(.+)$")
    private val GENERAL_OPCUA_PATTERN = Regex("^opcua/([a-zA-Z0-9_-]+)/(node|path)/(.*)$")

    /**
     * Check if a topic matches OPC UA patterns
     */
    fun isOpcUaTopic(topic: String): Boolean {
        return GENERAL_OPCUA_PATTERN.matches(topic)
    }

    /**
     * Parse an OPC UA topic into components
     */
    fun parseOpcUaTopic(topic: String): OpcUaTopicInfo? {
        val match = GENERAL_OPCUA_PATTERN.find(topic) ?: return null

        val deviceName = match.groupValues[1]
        val topicType = when (match.groupValues[2]) {
            "node" -> OpcUaTopicType.NODE
            "path" -> OpcUaTopicType.PATH
            else -> return null
        }
        val address = match.groupValues[3]

        return OpcUaTopicInfo(
            topic = topic,
            deviceName = deviceName,
            topicType = topicType,
            address = address
        )
    }

    /**
     * Check if a subscription is for OPC UA topics
     */
    fun isOpcUaSubscription(subscription: MqttSubscription): Boolean {
        return isOpcUaTopic(subscription.topicName)
    }

    /**
     * Parse subscription into OPC UA topic info
     */
    fun parseOpcUaSubscription(subscription: MqttSubscription): OpcUaTopicInfo? {
        return parseOpcUaTopic(subscription.topicName)
    }

    /**
     * Check if a topic pattern contains wildcards
     */
    fun hasWildcards(topic: String): Boolean {
        return topic.contains("#") || topic.contains("+")
    }

    /**
     * Validate device name format
     */
    fun isValidDeviceName(deviceName: String): Boolean {
        return deviceName.matches(Regex("^[a-zA-Z0-9_-]+$"))
    }

    /**
     * Validate OPC UA NodeId format
     */
    fun isValidNodeId(nodeIdStr: String): Boolean {
        return try {
            // Basic validation - more sophisticated validation would be done by Milo
            nodeIdStr.isNotBlank() && (
                nodeIdStr.startsWith("ns=") ||
                nodeIdStr.startsWith("i=") ||
                nodeIdStr.startsWith("s=") ||
                nodeIdStr.startsWith("g=") ||
                nodeIdStr.startsWith("b=")
            )
        } catch (e: Exception) {
            false
        }
    }

    /**
     * Generate MQTT topic for OPC UA node value
     */
    fun generateMqttTopic(deviceName: String, browsePath: String): String {
        return "opcua/$deviceName/path/$browsePath"
    }

    /**
     * Extract device name from MQTT topic
     */
    fun extractDeviceName(topic: String): String? {
        val match = GENERAL_OPCUA_PATTERN.find(topic)
        return match?.groupValues?.get(1)
    }
}

/**
 * Parsed OPC UA topic information
 */
data class OpcUaTopicInfo(
    val topic: String,
    val deviceName: String,
    val topicType: OpcUaTopicType,
    val address: String
) {
    /**
     * Check if this is a node-based subscription
     */
    fun isNodeSubscription(): Boolean = topicType == OpcUaTopicType.NODE

    /**
     * Check if this is a path-based subscription
     */
    fun isPathSubscription(): Boolean = topicType == OpcUaTopicType.PATH

    /**
     * Check if the address contains wildcards
     */
    fun hasWildcards(): Boolean = OpcUaTopicMatcher.hasWildcards(address)

    /**
     * Get the NodeId for node-based subscriptions
     */
    fun getNodeId(): String? {
        return if (isNodeSubscription()) address else null
    }

    /**
     * Get the browse path for path-based subscriptions
     */
    fun getBrowsePath(): String? {
        return if (isPathSubscription()) address else null
    }
}

/**
 * Type of OPC UA topic subscription
 */
enum class OpcUaTopicType {
    NODE,   // Direct NodeId subscription: opcua/device/node/ns=2;s=TagName
    PATH    // Browse path subscription: opcua/device/path/Objects/Variables/#
}

/**
 * Result of OPC UA topic matching operation
 */
sealed class OpcUaTopicMatchResult {
    object NotOpcUaTopic : OpcUaTopicMatchResult()
    object InvalidTopic : OpcUaTopicMatchResult()
    data class ValidTopic(val topicInfo: OpcUaTopicInfo) : OpcUaTopicMatchResult()
}

/**
 * Topic matching utility with result wrapper
 */
fun matchOpcUaTopic(topic: String): OpcUaTopicMatchResult {
    if (!OpcUaTopicMatcher.isOpcUaTopic(topic)) {
        return OpcUaTopicMatchResult.NotOpcUaTopic
    }

    val topicInfo = OpcUaTopicMatcher.parseOpcUaTopic(topic)
    return if (topicInfo != null) {
        OpcUaTopicMatchResult.ValidTopic(topicInfo)
    } else {
        OpcUaTopicMatchResult.InvalidTopic
    }
}