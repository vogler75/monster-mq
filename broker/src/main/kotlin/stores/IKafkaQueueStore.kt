package at.rocworks.stores

import at.rocworks.data.BrokerMessage
import io.vertx.core.Future

interface IKafkaQueueStore {
    /**
     * Enqueue a batch of messages to the Kafka sequential queue.
     * Each message will be assigned a unique, monotonically increasing offset ID.
     */
    fun enqueue(messages: List<BrokerMessage>): Future<Void>

    /**
     * Fetch a batch of messages from the Kafka sequential queue starting from a specific offset.
     * Returns a list of pairs where each pair contains the offset ID and the message.
     */
    fun fetch(topic: String, startOffset: Long, limit: Int): Future<List<Pair<Long, BrokerMessage>>>

    /**
     * Get the committed offset (watermark) for a specific consumer group, topic, and partition.
     * Returns null if no offset has been committed yet.
     */
    fun getOffset(groupId: String, topic: String, partition: Int): Future<Long?>

    /**
     * Commit (save) the offset for a specific consumer group, topic, and partition.
     */
    fun commitOffset(groupId: String, topic: String, partition: Int, offset: Long): Future<Void>

    /**
     * Get the latest offset (watermark) currently available for a topic.
     * Returns -1 or 0 if the topic is empty.
     */
    fun getLatestOffset(topic: String): Future<Long>

    /**
     * Get the earliest offset (watermark) currently available for a topic.
     */
    fun getEarliestOffset(topic: String): Future<Long>

    /**
     * Prune messages older than the given timestamp from the queue.
     * Returns the number of deleted messages.
     */
    fun pruneExpired(olderThanMs: Long): Future<Int>

    /**
     * Get all consumer groups and their associated topics and commit watermarks.
     */
    fun getConsumerGroups(): Future<List<KafkaConsumerGroup>> = Future.succeededFuture(emptyList())

    /**
     * Delete a consumer group (remove all committed offsets).
     */
    fun deleteConsumerGroup(groupId: String): Future<Boolean> = Future.succeededFuture(false)
}

data class KafkaConsumerGroup(
    val groupId: String,
    val topics: List<String>,
    val lastCommitTime: Long
)
