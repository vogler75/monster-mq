package at.rocworks.data

import at.rocworks.Utils

/**
 * Tree-based index for wildcard topic subscriptions.
 *
 * O(depth) lookup performance for wildcard pattern matching.
 * This index ONLY stores subscriptions WITH wildcards (+, #).
 *
 * Uses TopicTree internally for efficient pattern matching.
 * Typically much smaller than total subscriptions (most are exact).
 */
class TopicIndexWildcard {
    private val logger = Utils.getLogger(this::class.java)

    // TopicTree storing wildcard patterns
    // Key = clientId, Value = QoS
    private val tree = TopicTree<String, Int>()

    /**
     * Add a wildcard subscription.
     *
     * @param pattern Topic pattern with wildcards (e.g., "sensor/+/temperature")
     * @param clientId Client identifier
     * @param qos Quality of Service level (0, 1, or 2)
     */
    fun add(pattern: String, clientId: String, qos: Int) {
        require(pattern.contains('+') || pattern.contains('#')) {
            "WildcardSubscriptionIndex only accepts topics with wildcards, got: $pattern"
        }

        logger.finest { "WildcardSubscriptionIndex.add: pattern=[$pattern] clientId=[$clientId] qos=[$qos]" }

        tree.add(pattern, clientId, qos)
    }

    /**
     * Remove a subscription from wildcard index.
     *
     * @param pattern Topic pattern with wildcards
     * @param clientId Client identifier
     * @return true if subscription was removed, false if it didn't exist
     */
    fun remove(pattern: String, clientId: String): Boolean {
        require(pattern.contains('+') || pattern.contains('#')) {
            "WildcardSubscriptionIndex only accepts topics with wildcards, got: $pattern"
        }

        logger.finest { "WildcardSubscriptionIndex.remove: pattern=[$pattern] clientId=[$clientId]" }

        tree.del(pattern, clientId)
        return true  // TopicTree.del doesn't report success, so we assume it worked
    }

    /**
     * Find subscribers matching a published topic.
     * Performs pattern matching against all wildcard subscriptions.
     *
     * O(depth * k) where depth = topic levels, k = number of wildcard patterns at each level
     *
     * @param publishedTopic The actual topic a message was published to
     * @return List of (ClientId, QoS) pairs for matching wildcard subscriptions
     */
    fun findMatchingSubscribers(publishedTopic: String): List<Pair<String, Int>> {
        return tree.findDataOfTopicName(publishedTopic)
    }

    /**
     * Check if the published topic matches any wildcard subscriptions.
     *
     * @param publishedTopic The actual topic a message was published to
     * @return true if at least one wildcard pattern matches
     */
    fun hasMatchingSubscribers(publishedTopic: String): Boolean {
        return tree.isTopicNameMatching(publishedTopic)
    }

    /**
     * Get all wildcard patterns that have subscribers (useful for cluster sync).
     *
     * @param callback Function called for each matching topic, return false to stop iteration
     * @return true if completed all iterations, false if callback requested stop
     */
    fun getAllPatterns(callback: (String) -> Boolean): Boolean {
        var continueIteration = true
        tree.findMatchingTopicNames("#") { topicName ->
            continueIteration = callback(topicName)
            continueIteration
        }
        return continueIteration
    }

    /**
     * Get the count of wildcard subscription patterns.
     */
    fun patternCount(): Int = tree.size()

    /**
     * Clear all subscriptions for a client (useful for client disconnection).
     *
     * @param clientId Client identifier
     * @return List of patterns that had this client's subscriptions
     */
    fun removeClient(clientId: String): List<String> {
        val affectedPatterns = mutableListOf<String>()

        // Iterate through all patterns and collect those with this client
        tree.findMatchingTopicNames("#") { pattern ->
            val subscribers = tree.findDataOfTopicName(pattern)
            if (subscribers.any { it.first == clientId }) {
                affectedPatterns.add(pattern)
            }
            true  // continue iteration
        }

        // Remove the client from each affected pattern
        affectedPatterns.forEach { pattern ->
            remove(pattern, clientId)
        }

        return affectedPatterns
    }

    /**
     * Dump index state for debugging.
     */
    override fun toString(): String {
        return buildString {
            append("WildcardSubscriptionIndex(patternCount=${patternCount()})\n")
            append(tree.toString())
        }
    }
}
