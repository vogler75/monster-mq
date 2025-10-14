package at.rocworks.logger.queue

import at.rocworks.data.BrokerMessage

/**
 * Queue interface for buffering MQTT messages before writing to database.
 * Supports both memory-based and disk-based implementations for reliability.
 */
interface ILoggerQueue {
    /**
     * Check if the queue is full (cannot accept more messages)
     */
    fun isQueueFull(): Boolean

    /**
     * Get current number of messages in queue
     */
    fun getSize(): Int

    /**
     * Get maximum capacity of queue
     */
    fun getCapacity(): Int

    /**
     * Add a message to the queue
     * @param message The MQTT message to buffer
     */
    fun add(message: BrokerMessage)

    /**
     * Poll a block of messages from the queue
     * Blocks until at least one message is available or timeout is reached
     * @param handler Function called for each message in the block
     * @return Number of messages in the block
     */
    fun pollBlock(handler: (BrokerMessage) -> Unit): Int

    /**
     * Commit the last polled block (indicates successful write)
     * If not called, the block will be retried on next poll
     */
    fun pollCommit()

    /**
     * Close the queue and release resources
     */
    fun close()
}
