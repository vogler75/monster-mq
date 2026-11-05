package at.rocworks

/**
 * Syntax checks for MQTT topic names and topic filters (MQTT 3.1.1 / 5.0 §4.7).
 *
 * netty's MqttDecoder only rejects wildcards in a PUBLISH topic, everything else
 * (empty names/filters, misplaced wildcards, U+0000) must be checked by the broker.
 */
object MqttValidation {

    /**
     * A Topic Name (PUBLISH, Will Topic, Response Topic): at least one character [MQTT-4.7.3-1],
     * no wildcard characters [MQTT-4.7.1-1], no U+0000 [MQTT-1.5.3-2].
     */
    fun isValidTopicName(topic: String): Boolean =
        topic.isNotEmpty() && !containsWildcard(topic) && topic.indexOf('\u0000') < 0

    fun containsWildcard(topic: String): Boolean = topic.indexOf('+') >= 0 || topic.indexOf('#') >= 0

    /**
     * A Topic Filter (SUBSCRIBE/UNSUBSCRIBE): at least one character [MQTT-4.7.3-1], no U+0000,
     * '#' only as the whole last level [MQTT-4.7.1-2], '+' only as a whole level [MQTT-4.7.1-3].
     */
    fun isValidTopicFilter(filter: String): Boolean {
        if (filter.isEmpty() || filter.indexOf('\u0000') >= 0) return false
        val levels = filter.split('/')
        levels.forEachIndexed { index, level ->
            if (level.contains('#') && (level != "#" || index != levels.lastIndex)) return false
            if (level.contains('+') && level != "+") return false
        }
        return true
    }

    /** Shared subscription filter ($share/{ShareName}/{filter}, MQTT 5.0 §4.8.2). */
    fun isSharedSubscription(filter: String): Boolean = filter.startsWith("\$share/")

    /**
     * Properties a client may send in a PUBLISH (MQTT 5.0 §3.3.2.3): Payload Format Indicator,
     * Message Expiry Interval, Content Type, Response Topic, Correlation Data, Topic Alias and
     * User Property. Subscription Identifier (11) is checked separately (Protocol Error).
     */
    val ALLOWED_CLIENT_PUBLISH_PROPERTIES = setOf(1, 2, 3, 8, 9, 35, 38)
}
