package at.rocworks.data

import at.rocworks.Utils
import java.util.concurrent.ConcurrentHashMap

/**
 * Fast hash-based index for exact topic subscriptions (no wildcards).
 *
 * O(1) lookup performance for exact topic matches.
 * This index ONLY stores subscriptions without wildcards (+, #).
 *
 * Thread-safe via ConcurrentHashMap.
 */
class TopicIndexExact {
    private val logger = Utils.getLogger(this::class.java)

    // topic → Set<(ClientId, QoS)>
    private val index = ConcurrentHashMap<String, MutableSet<Pair<String, Int>>>()

    /**
     * Add an exact topic subscription.
     *
     * @param topic Exact topic name (no wildcards)
     * @param clientId Client identifier
     * @param qos Quality of Service level (0, 1, or 2)
     */
    fun add(topic: String, clientId: String, qos: Int) {
        require(!topic.contains('+') && !topic.contains('#')) {
            "ExactTopicIndex only accepts topics without wildcards, got: $topic"
        }

        logger.finest { "ExactTopicIndex.add: topic=[$topic] clientId=[$clientId] qos=[$qos]" }

        val subscribers = index.getOrPut(topic) { ConcurrentHashMap.newKeySet() }
        subscribers.add(Pair(clientId, qos))
    }

    /**
     * Remove a subscription from exact index.
     *
     * @param topic Exact topic name
     * @param clientId Client identifier
     * @return true if subscription was removed, false if it didn't exist
     */
    fun remove(topic: String, clientId: String): Boolean {
        require(!topic.contains('+') && !topic.contains('#')) {
            "ExactTopicIndex only accepts topics without wildcards, got: $topic"
        }

        logger.finest { "ExactTopicIndex.remove: topic=[$topic] clientId=[$clientId]" }

        val subscribers = index[topic] ?: return false
        val removed = subscribers.removeAll { it.first == clientId }

        // Clean up empty entries
        if (subscribers.isEmpty()) {
            index.remove(topic)
        }

        return removed
    }

    /**
     * Find subscribers for an exact topic match.
     * O(1) operation.
     *
     * @param topic Exact topic name
     * @return List of (ClientId, QoS) pairs
     */
    fun findSubscribers(topic: String): List<Pair<String, Int>> {
        return (index[topic] ?: emptySet()).toList()
    }

    /**
     * Get all topics that have subscribers (useful for cluster sync).
     *
     * @return Set of topic names with at least one subscriber
     */
    fun getAllTopics(): Set<String> {
        return index.keys.toSet()
    }

    /**
     * Check if a topic has any subscribers.
     *
     * @param topic Topic name
     * @return true if topic has subscribers
     */
    fun hasSubscribers(topic: String): Boolean {
        return index.containsKey(topic) && index[topic]?.isNotEmpty() == true
    }

    /**
     * Get the count of unique topics with subscribers.
     */
    fun topicCount(): Int = index.size

    /**
     * Get total number of subscriptions across all topics.
     */
    fun subscriptionCount(): Int = index.values.sumOf { it.size }

    /**
     * Clear all subscriptions for a client (useful for client disconnection).
     *
     * @param clientId Client identifier
     * @return List of topics that had this client's subscriptions
     */
    fun removeClient(clientId: String): List<String> {
        val affectedTopics = mutableListOf<String>()

        index.forEach { (topic, subscribers) ->
            if (subscribers.removeAll { it.first == clientId }) {
                affectedTopics.add(topic)

                // Clean up empty topics
                if (subscribers.isEmpty()) {
                    index.remove(topic)
                }
            }
        }

        return affectedTopics
    }

    /**
     * Dump index state for debugging.
     */
    override fun toString(): String {
        return buildString {
            append("ExactTopicIndex(topics=${index.size}, subscriptions=${subscriptionCount()})\n")
            index.forEach { (topic, subscribers) ->
                append("  $topic: ${subscribers.joinToString(", ") { (cid, qos) -> "$cid:$qos" }}\n")
            }
        }
    }
}
