package at.rocworks.logger.queue

import at.rocworks.data.BrokerMessage
import java.io.*
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import java.util.logging.Logger

/**
 * Disk-based queue implementation for buffering MQTT messages.
 * Messages persist across restarts, providing reliability during database outages.
 * Uses memory-mapped file for performance.
 */
class LoggerQueueDisk(
    private val deviceName: String,
    private val logger: Logger,
    private val queueSize: Int,
    private val blockSize: Int,
    private val pollTimeout: Long,
    private val diskPath: String
) : ILoggerQueue {

    private var queueFull = false
    private val outputBlock = arrayListOf<BrokerMessage>()
    private val semaphore = Semaphore(0)

    private val fileSize = queueSize.toLong() * 2048L // ~2KB per message estimate
    private val fileName = "$diskPath/jdbc-logger-${deviceName}.buf"
    private val file: RandomAccessFile
    private val buffer: MappedByteBuffer
    private val lock = ReentrantLock()

    private val startPosition = Int.SIZE_BYTES * 2
    private var writePosition = startPosition
    private var readPosition = startPosition

    init {
        // Ensure directory exists
        File(diskPath).mkdirs()

        logger.info("Disk queue file: $fileName (size: ${fileSize / 1024 / 1024} MB)")

        file = RandomAccessFile(fileName, "rw")

        if (file.length() != fileSize) {
            // New file - initialize
            logger.info("Creating new disk queue file")
            file.setLength(fileSize)
            buffer = file.channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize)
            writeReadPosition()
            writeWritePosition()
        } else {
            // Existing file - recover positions
            buffer = file.channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize)
            buffer.position(0)
            readPosition = buffer.int
            writePosition = buffer.int
            logger.info("Recovered queue positions: read=$readPosition write=$writePosition (${getSize()} messages buffered)")
        }
    }

    private fun enqueue(message: BrokerMessage): Boolean {
        lock.lock()
        try {
            val dataBytes = serialize(message)
            val dataSize = dataBytes.size + Int.SIZE_BYTES

            logger.finest { "Enqueue: ReadPos $readPosition WritePos $writePosition Data size: ${dataBytes.size}" }

            // Check if we need to wrap around
            if (writePosition + dataSize > fileSize) {
                buffer.position(writePosition)
                buffer.putInt(0) // EOF marker
                writePosition = startPosition
            }

            // Check if we would overwrite unread data
            if (writePosition < readPosition && writePosition + dataSize >= readPosition) {
                return false // Queue full
            }

            // Write data
            buffer.position(writePosition)
            buffer.putInt(dataBytes.size)
            buffer.put(dataBytes)
            writePosition += dataSize
            writeWritePosition()

            return true
        } finally {
            lock.unlock()
        }
    }

    private fun dequeue(): BrokerMessage? {
        lock.lock()
        try {
            if (readPosition == writePosition) {
                return null // Empty
            }

            logger.finest { "Dequeue: ReadPos $readPosition WritePos $writePosition" }

            buffer.position(readPosition)
            var dataSize = buffer.int

            // Check for EOF marker
            if (dataSize == 0) {
                readPosition = startPosition
                if (writePosition == startPosition) {
                    return null // Empty after wrap
                }
                buffer.position(startPosition)
                dataSize = buffer.int
            }

            val dataBytes = ByteArray(dataSize)
            buffer.get(dataBytes)
            val message = deserialize(dataBytes)

            readPosition += dataSize + Int.SIZE_BYTES

            return message
        } finally {
            lock.unlock()
        }
    }

    private fun writeReadPosition() {
        buffer.position(0)
        buffer.putInt(readPosition)
    }

    private fun writeWritePosition() {
        buffer.position(Int.SIZE_BYTES)
        buffer.putInt(writePosition)
    }

    override fun isQueueFull(): Boolean = queueFull

    override fun getCapacity(): Int = queueSize

    override fun getSize(): Int {
        val size = writePosition - readPosition
        return if (size >= 0) size / 2048 else (queueSize + size) / 2048 // Approximate
    }

    override fun add(message: BrokerMessage) {
        if (enqueue(message)) {
            if (queueFull) {
                queueFull = false
                logger.warning("Disk queue not full anymore. [${getSize()}/${getCapacity()}]")
            }
        } else {
            if (!queueFull) {
                queueFull = true
                logger.warning("Disk queue is FULL! [${getSize()}/${getCapacity()}] - dropping message on topic: ${message.topicName}")
            }
        }
        semaphore.release()
    }

    override fun pollBlock(handler: (BrokerMessage) -> Unit): Int {
        // Retry last block if it failed
        if (outputBlock.isNotEmpty()) {
            logger.warning("Retrying last block of ${outputBlock.size} messages in 1 second...")
            Thread.sleep(1000)
            logger.warning("Retrying last block now.")
            outputBlock.forEach(handler)
            return outputBlock.size
        }

        // Poll new block
        var message: BrokerMessage? = dequeue()
        if (message == null) {
            // Wait for data
            if (semaphore.tryAcquire(pollTimeout, TimeUnit.MILLISECONDS)) {
                message = dequeue()
            }
        }

        while (message != null) {
            outputBlock.add(message)
            handler(message)
            message = if (outputBlock.size < blockSize) dequeue() else null
        }

        return outputBlock.size
    }

    override fun pollCommit() {
        lock.lock()
        try {
            outputBlock.clear()
            writeReadPosition() // Persist read position
            buffer.force() // Force to disk
        } finally {
            lock.unlock()
        }
    }

    override fun close() {
        logger.info("Closing disk queue, flushing buffer...")
        buffer.force()
        file.close()
    }

    private fun serialize(message: BrokerMessage): ByteArray {
        ByteArrayOutputStream().use { baos ->
            ObjectOutputStream(baos).use { oos ->
                oos.writeObject(message)
                return baos.toByteArray()
            }
        }
    }

    private fun deserialize(bytes: ByteArray): BrokerMessage {
        ByteArrayInputStream(bytes).use { bais ->
            ObjectInputStream(bais).use { ois ->
                return ois.readObject() as BrokerMessage
            }
        }
    }
}
