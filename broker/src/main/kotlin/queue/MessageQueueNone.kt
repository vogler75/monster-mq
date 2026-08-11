package at.rocworks.queue

import at.rocworks.data.BrokerMessage
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.logging.Logger

/**
 * Unbuffered memory queue implementation (Store-and-Forward disabled).
 * Does not retain or retry output blocks if database writes fail.
 */
class MessageQueueNone(
    private val logger: Logger,
    private val queueSize: Int,
    private val blockSize: Int,
    private val pollTimeout: Long  // milliseconds
) : IMessageQueue {

    private val queue = ArrayBlockingQueue<BrokerMessage>(queueSize)
    private var queueFull = false
    private var droppedMessages = 0L

    override fun isQueueFull(): Boolean = queueFull

    override fun getCapacity(): Int = queueSize

    override fun getSize(): Int = queue.size

    override fun add(message: BrokerMessage) {
        try {
            queue.add(message)
            if (queueFull) {
                queueFull = false
                logger.warning("Queue not full anymore. [${getSize()}/${getCapacity()}] - dropped ${droppedMessages} messages total")
            }
        } catch (e: IllegalStateException) {
            droppedMessages++
            if (!queueFull) {
                queueFull = true
                logger.warning("Queue is FULL! [${getSize()}/${getCapacity()}] - dropping messages starting with topic: ${message.topicName}")
            } else if (droppedMessages % 1000 == 0L) {
                logger.warning("Queue still FULL! [${getSize()}/${getCapacity()}] - dropped ${droppedMessages} messages total")
            }
        }
    }

    override fun pollBlock(handler: (BrokerMessage) -> Unit): Int {
        val block = arrayListOf<BrokerMessage>()
        var message: BrokerMessage? = queue.poll(pollTimeout, TimeUnit.MILLISECONDS)
        while (message != null) {
            block.add(message)
            handler(message)
            message = if (block.size < blockSize) queue.poll() else null
        }
        return block.size
    }

    override fun pollCommit() {
        // No-op for unbuffered queue (messages are dequeued immediately during pollBlock)
    }

    override fun close() {
        queue.clear()
    }
}
