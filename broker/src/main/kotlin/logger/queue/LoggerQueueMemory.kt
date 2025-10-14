package at.rocworks.logger.queue

import at.rocworks.data.BrokerMessage
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.logging.Logger

/**
 * Memory-based queue implementation for buffering MQTT messages.
 * Messages are lost on restart but provides best performance.
 */
class LoggerQueueMemory(
    private val logger: Logger,
    private val queueSize: Int,
    private val blockSize: Int,
    private val pollTimeout: Long  // milliseconds
) : ILoggerQueue {

    private val queue = ArrayBlockingQueue<BrokerMessage>(queueSize)
    private var queueFull = false

    // Block for retry on write failure
    private val outputBlock = arrayListOf<BrokerMessage>()

    override fun isQueueFull(): Boolean = queueFull

    override fun getCapacity(): Int = queueSize

    override fun getSize(): Int = queue.size

    override fun add(message: BrokerMessage) {
        try {
            queue.add(message)
            if (queueFull) {
                queueFull = false
                logger.warning("Queue not full anymore. [${getSize()}/${getCapacity()}]")
            }
        } catch (e: IllegalStateException) {
            if (!queueFull) {
                queueFull = true
                logger.warning("Queue is FULL! [${getSize()}/${getCapacity()}] - dropping message on topic: ${message.topicName}")
            }
        }
    }

    override fun pollBlock(handler: (BrokerMessage) -> Unit): Int {
        // If last block failed, retry it
        if (outputBlock.isNotEmpty()) {
            logger.warning("Retrying last block of ${outputBlock.size} messages in 1 second...")
            Thread.sleep(1000)
            logger.warning("Retrying last block now.")
            outputBlock.forEach(handler)
            return outputBlock.size
        }

        // Poll new block
        var message: BrokerMessage? = queue.poll(pollTimeout, TimeUnit.MILLISECONDS)
        while (message != null) {
            outputBlock.add(message)
            handler(message)
            message = if (outputBlock.size < blockSize) queue.poll() else null
        }

        return outputBlock.size
    }

    override fun pollCommit() {
        outputBlock.clear()
    }

    override fun close() {
        // Nothing to close for memory queue
    }
}
