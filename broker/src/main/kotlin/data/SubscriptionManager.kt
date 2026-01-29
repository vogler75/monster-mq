package at.rocworks.data

import at.rocworks.Utils
import java.util.concurrent.ConcurrentHashMap

/**
 * Unified subscription manager combining exact and wildcard indexes.
 *
 * This is the main abstraction for subscription operations:
 * - Routes subscriptions to appropriate index (exact vs wildcard)
 * - Provides unified lookup interface
 * - Handles client disconnections
 * - Cluster-aware (works with cluster replication, but replication is handled by SessionHandler)
 *
 * Performance characteristics:
 * - add/remove: O(1) for exact, O(depth) for wildcard
 * - findAllSubscribers: O(1) for exact + O(depth) for wildcards = O(1) PRACTICAL
 *
 * The dual-index approach provides order of magnitude improvements for realistic deployments:
 * - Most subscriptions (80%+) are exact topics
 * - Wildcard subscriptions are less common but still supported efficiently
 *
 * CLUSTER REPLICATION:
 * This manager handles LOCAL subscription tracking only.
 * Cluster replication of topicNodeMapping is handled by SessionHandler via SetMapReplicator.
 */
class SubscriptionManager {
    private val logger = Utils.getLogger(this::class.java)

    // Exact subscriptions: O(1) lookup
    private val exactIndex = TopicIndexExact()

    // Wildcard subscriptions: O(depth) lookup
    private val wildcardIndex = TopicIndexWildcard()
    
    // Track noLocal subscriptions: clientId → Set<topicPattern>
    private val noLocalSubscriptions = ConcurrentHashMap<String, MutableSet<String>>()

    /**
     * Add a subscription.
     * Routes to appropriate index (exact or wildcard) based on content.
     *
     * @param clientId Client identifier
     * @param topicOrPattern Topic name or pattern (e.g., "sensor/room1/temp" or "sensor/+/temp")
     * @param qos Quality of Service level (0, 1, or 2)
     * @param noLocal MQTT v5: Don't send messages back to the publishing client
     * @return true if subscription was added, false if it already existed
     */
    fun subscribe(clientId: String, topicOrPattern: String, qos: Int, noLocal: Boolean = false) {
        if (topicOrPattern.contains('+') || topicOrPattern.contains('#')) {
            // Wildcard subscription
            logger.finest { "SubscriptionManager.subscribe: wildcard pattern=[$topicOrPattern] clientId=[$clientId] qos=[$qos] noLocal=$noLocal" }
            wildcardIndex.add(topicOrPattern, clientId, qos)
        } else {
            // Exact subscription
            logger.finest { "SubscriptionManager.subscribe: exact topic=[$topicOrPattern] clientId=[$clientId] qos=[$qos] noLocal=$noLocal" }
            exactIndex.add(topicOrPattern, clientId, qos)
        }
        
        // Track noLocal subscriptions separately
        if (noLocal) {
            noLocalSubscriptions.getOrPut(clientId) { ConcurrentHashMap.newKeySet() }.add(topicOrPattern)
        }
    }

    /**
     * Remove a subscription.
     * Removes from appropriate index.
     *
     * @param clientId Client identifier
     * @param topicOrPattern Topic name or pattern
     * @return true if subscription was removed, false if it didn't exist
     */
    fun unsubscribe(clientId: String, topicOrPattern: String): Boolean {
        val removed = if (topicOrPattern.contains('+') || topicOrPattern.contains('#')) {
            // Wildcard subscription
            logger.finest { "SubscriptionManager.unsubscribe: wildcard pattern=[$topicOrPattern] clientId=[$clientId]" }
            wildcardIndex.remove(topicOrPattern, clientId)
        } else {
            // Exact subscription
            logger.finest { "SubscriptionManager.unsubscribe: exact topic=[$topicOrPattern] clientId=[$clientId]" }
            exactIndex.remove(topicOrPattern, clientId)
        }
        
        // Remove from noLocal tracking
        noLocalSubscriptions[clientId]?.remove(topicOrPattern)
        if (noLocalSubscriptions[clientId]?.isEmpty() == true) {
            noLocalSubscriptions.remove(clientId)
        }
        
        return removed
    }

    /**
     * Find all subscribers for a published message topic.
     *
     * This is the CRITICAL PATH - called for every published message.
     *
     * Performance:
     * - Exact matches: O(1) lookup
     * - Wildcard matches: O(depth) where depth is topic levels (typically 3-5)
     * - Combined: O(1) PRACTICAL (dominated by exact lookup in realistic scenarios)
     *
     * @param publishedTopic The topic a message was published to (e.g., "sensor/room1/temperature")
     * @return List of (ClientId, QoS) pairs for all matching subscriptions (exact + wildcard)
     */
    fun findAllSubscribers(publishedTopic: String): List<Pair<String, Int>> {
        logger.finest { "SubscriptionManager.findAllSubscribers: topic=[$publishedTopic]" }

        val result = mutableListOf<Pair<String, Int>>()

        // Fast path: exact subscriptions (O(1))
        val exactMatches = exactIndex.findSubscribers(publishedTopic)
        result.addAll(exactMatches)

        // Slower path: wildcard subscriptions (O(depth), but typically much smaller set)
        val wildcardMatches = wildcardIndex.findMatchingSubscribers(publishedTopic)
        result.addAll(wildcardMatches)

        // In practice, duplicates are rare (would require same client to subscribe to exact AND wildcard)
        // But we deduplicate to be safe, keeping highest QoS if duplicates exist
        val deduped = result
            .groupBy { it.first }  // Group by clientId
            .mapValues { (_, entries) -> entries.maxByOrNull { it.second } ?: entries.first() }
            .values
            .toList()

        logger.finest { "SubscriptionManager.findAllSubscribers: found ${deduped.size} subscribers (exact=${exactMatches.size}, wildcard=${wildcardMatches.size})" }

        return deduped
    }
    
    /**
     * Check if a client has a noLocal subscription matching the given topic.
     * Used to filter out messages that the client itself published.
     *
     * @param clientId Client identifier
     * @param publishedTopic The topic being checked
     * @return true if client has noLocal subscription for this topic
     */
    fun hasNoLocalSubscription(clientId: String, publishedTopic: String): Boolean {
        val patterns = noLocalSubscriptions[clientId]
        if (patterns == null) return false
        
        return patterns.any { pattern ->
            if (pattern.contains('+') || pattern.contains('#')) {
                wildcardIndex.matchesPattern(publishedTopic, pattern)
            } else {
                pattern == publishedTopic
            }
        }
    }

    /**
     * Check if a topic has any subscribers (exact or wildcard).
     * Useful for optimization: don't process message if no subscribers.
     *
     * @param publishedTopic Topic name
     * @return true if at least one subscription matches
     */
    fun hasSubscribers(publishedTopic: String): Boolean {
        return exactIndex.hasSubscribers(publishedTopic) || wildcardIndex.hasMatchingSubscribers(publishedTopic)
    }

    /**
     * Check if a specific client has a subscription to a topic/pattern.
     * Used for MQTT v5 Retain Handling option 1 (send only if new subscription).
     *
     * @param clientId Client identifier
     * @param topicOrPattern Topic name or pattern
     * @return true if client has this subscription
     */
    fun hasSubscription(clientId: String, topicOrPattern: String): Boolean {
        return if (topicOrPattern.contains('+') || topicOrPattern.contains('#')) {
            wildcardIndex.hasSubscriber(topicOrPattern, clientId)
        } else {
            exactIndex.hasSubscriber(topicOrPattern, clientId)
        }
    }

    /**
     * Get all topics/patterns that have subscribers.
     * Used by cluster replication to update topicNodeMapping.
     *
     * @return Set of all exact topics + wildcard patterns with subscribers
     */
    fun getAllTopicsAndPatterns(): Set<String> {
        val result = mutableSetOf<String>()
        result.addAll(exactIndex.getAllTopics())

        // Add all wildcard patterns
        wildcardIndex.getAllPatterns { pattern ->
            result.add(pattern)
            true  // continue iteration
        }

        return result
    }

    /**
     * Remove all subscriptions for a disconnected client.
     * Triggers cluster replication cleanup via the caller (SessionHandler).
     *
     * @param clientId Client identifier
     * @return List of topics/patterns that had this client's subscriptions
     */
    fun disconnectClient(clientId: String): List<String> {
        logger.fine { "SubscriptionManager.disconnectClient: removing all subscriptions for clientId=[$clientId]" }

        val affectedTopics = mutableListOf<String>()

        // Remove from exact index
        affectedTopics.addAll(exactIndex.removeClient(clientId))

        // Remove from wildcard index
        affectedTopics.addAll(wildcardIndex.removeClient(clientId))

        logger.fine { "SubscriptionManager.disconnectClient: removed subscriptions from ${affectedTopics.size} topics/patterns" }

        return affectedTopics
    }

    /**
     * Get statistics about current subscriptions.
     * Useful for monitoring and metrics.
     */
    data class SubscriptionStats(
        val totalExactTopics: Int,
        val totalExactSubscriptions: Int,
        val totalWildcardPatterns: Int,
        val totalWildcardSubscriptions: Int
    )

    fun getStats(): SubscriptionStats {
        return SubscriptionStats(
            totalExactTopics = exactIndex.topicCount(),
            totalExactSubscriptions = exactIndex.subscriptionCount(),
            totalWildcardPatterns = wildcardIndex.patternCount(),
            totalWildcardSubscriptions = wildcardIndex.patternCount()  // TODO: add count method to WildcardSubscriptionIndex
        )
    }

    /**
     * Dump internal state for debugging.
     */
    override fun toString(): String {
        return buildString {
            val stats = getStats()
            append("SubscriptionManager(\n")
            append("  exactTopics=${stats.totalExactTopics}, exactSubs=${stats.totalExactSubscriptions}\n")
            append("  wildcardPatterns=${stats.totalWildcardPatterns}\n")
            append(")\n")
            append(exactIndex.toString())
            append("\n")
            append(wildcardIndex.toString())
        }
    }
}
