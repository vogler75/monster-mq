package at.rocworks.devices.mqttclient

import at.rocworks.stores.MqttClientAddress

/**
 * Utility class for transforming MQTT topics between local and remote brokers
 *
 * Handles topic path transformation with removePath logic:
 * - Subscribe: transforms remote topics to local topics
 * - Publish: transforms local topics to remote topics
 */
object MqttTopicTransformer {

    /**
     * Transform a remote topic to a local topic for SUBSCRIBE mode
     *
     * Example:
     *   remoteTopic: "enterprise/site/sensor/sensor1"
     *   address.remoteTopic: "enterprise/site/sensor/#"
     *   address.localTopic: "plant1/site"
     *   address.removePath: true
     *   Result: "plant1/site/sensor1"
     *
     * @param receivedTopic The actual topic received from remote broker
     * @param address The MQTT client address configuration
     * @return The transformed local topic
     */
    fun remoteToLocal(receivedTopic: String, address: MqttClientAddress): String {
        if (!address.isSubscribe()) {
            throw IllegalArgumentException("Address must be SUBSCRIBE mode for remoteToLocal transformation")
        }

        val remoteTopic = address.remoteTopic
        val localTopic = address.localTopic
        val removePath = address.removePath

        // Find the base path (everything before the wildcard)
        val wildcardIndex = remoteTopic.indexOfFirst { it == '#' || it == '+' }

        return if (wildcardIndex > 0 && removePath) {
            // Remove base path before wildcard
            val basePath = remoteTopic.substring(0, wildcardIndex).trimEnd('/')

            // Extract the suffix after the base path
            val suffix = if (receivedTopic.startsWith(basePath)) {
                receivedTopic.substring(basePath.length).trimStart('/')
            } else {
                // Topic doesn't match base path, use full received topic
                receivedTopic
            }

            // Combine local topic with suffix
            if (suffix.isNotEmpty()) {
                "$localTopic/$suffix"
            } else {
                localTopic
            }
        } else {
            // No wildcard or removePath=false: append full received topic
            "$localTopic/$receivedTopic"
        }
    }

    /**
     * Transform a local topic to a remote topic for PUBLISH mode
     *
     * Example:
     *   publishedTopic: "opcua/factory/line1/temp"
     *   address.localTopic: "opcua/factory/#"
     *   address.remoteTopic: "cloud/factory"
     *   address.removePath: true
     *   Result: "cloud/factory/line1/temp"
     *
     * @param publishedTopic The actual topic published locally
     * @param address The MQTT client address configuration
     * @return The transformed remote topic, or null if topic doesn't match pattern
     */
    fun localToRemote(publishedTopic: String, address: MqttClientAddress): String? {
        if (!address.isPublish()) {
            throw IllegalArgumentException("Address must be PUBLISH mode for localToRemote transformation")
        }

        val localTopic = address.localTopic
        val remoteTopic = address.remoteTopic
        val removePath = address.removePath

        // Check if local topic has wildcard
        val wildcardIndex = localTopic.indexOfFirst { it == '#' || it == '+' }

        return if (wildcardIndex > 0) {
            // Local topic has wildcard - extract base path
            val basePath = localTopic.substring(0, wildcardIndex).trimEnd('/')

            // Check if published topic matches the base path
            if (!publishedTopic.startsWith(basePath)) {
                // Topic doesn't match pattern - skip
                return null
            }

            if (removePath) {
                // Remove base path and combine with remote topic
                val suffix = publishedTopic.substring(basePath.length).trimStart('/')
                if (suffix.isNotEmpty()) {
                    "$remoteTopic/$suffix"
                } else {
                    remoteTopic
                }
            } else {
                // Keep full local topic path
                "$remoteTopic/$publishedTopic"
            }
        } else {
            // Exact match without wildcard
            if (publishedTopic == localTopic) {
                remoteTopic
            } else if (publishedTopic.startsWith("$localTopic/")) {
                // Published topic is more specific than configured
                val suffix = publishedTopic.substring(localTopic.length + 1)
                "$remoteTopic/$suffix"
            } else {
                // Topic doesn't match - skip
                null
            }
        }
    }

    /**
     * Check if a published local topic matches the configured local topic pattern
     *
     * @param publishedTopic The topic to check
     * @param localTopicPattern The pattern from address configuration (may contain wildcards)
     * @return true if the topic matches the pattern
     */
    fun matchesLocalPattern(publishedTopic: String, localTopicPattern: String): Boolean {
        return matchesMqttTopic(publishedTopic, localTopicPattern)
    }

    /**
     * Check if a received remote topic matches the configured remote topic pattern
     *
     * @param receivedTopic The topic to check
     * @param remoteTopicPattern The pattern from address configuration (may contain wildcards)
     * @return true if the topic matches the pattern
     */
    fun matchesRemotePattern(receivedTopic: String, remoteTopicPattern: String): Boolean {
        return matchesMqttTopic(receivedTopic, remoteTopicPattern)
    }

    /**
     * MQTT topic matching algorithm
     * Supports:
     * - '#' wildcard (multi-level, must be last character)
     * - '+' wildcard (single level)
     *
     * Examples:
     * - "sensor/#" matches "sensor/temp", "sensor/temp/1", etc.
     * - "sensor/+/temp" matches "sensor/1/temp", "sensor/2/temp"
     * - "sensor/+" matches "sensor/1" but not "sensor/1/temp"
     */
    private fun matchesMqttTopic(topic: String, pattern: String): Boolean {
        // Exact match
        if (topic == pattern) return true

        val topicLevels = topic.split('/')
        val patternLevels = pattern.split('/')

        // Multi-level wildcard '#' - must be last
        if (patternLevels.lastOrNull() == "#") {
            val patternPrefix = patternLevels.dropLast(1)
            // Check if topic starts with the pattern prefix
            if (topicLevels.size < patternPrefix.size) return false
            return patternPrefix.indices.all { i ->
                patternPrefix[i] == "+" || patternPrefix[i] == topicLevels[i]
            }
        }

        // Single-level wildcard '+' or exact match
        if (topicLevels.size != patternLevels.size) return false
        return patternLevels.indices.all { i ->
            patternLevels[i] == "+" || patternLevels[i] == topicLevels[i]
        }
    }
}