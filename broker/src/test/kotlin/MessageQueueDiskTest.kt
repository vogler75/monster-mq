package at.rocworks

import at.rocworks.data.BrokerMessage
import at.rocworks.queue.MessageQueueDisk
import org.junit.Assert.*
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.time.Instant
import java.util.logging.Logger

class MessageQueueDiskTest {
    @get:Rule
    val tempFolder = TemporaryFolder()

    private val logger = Logger.getLogger(MessageQueueDiskTest::class.java.name)

    @Test
    fun testQueueWraparoundAndRead() {
        // queueSize = 10 -> fileSize = 10 * 2048 = 20480 bytes
        val queue = MessageQueueDisk(
            queueName = "test-q",
            deviceName = "dev1",
            logger = logger,
            queueSize = 10,
            blockSize = 5,
            pollTimeout = 100,
            diskPath = tempFolder.root.absolutePath
        )

        val largePayload = ByteArray(1500) { 42 }
        val sentMessages = mutableListOf<String>()

        // Add messages until we wrap around
        for (i in 1..25) {
            val uuid = "msg-$i"
            val msg = BrokerMessage(
                messageUuid = uuid,
                messageId = i,
                topicName = "test/wrap",
                payload = largePayload,
                qosLevel = 1,
                isRetain = false,
                isDup = false,
                isQueued = true,
                clientId = "sender",
                time = Instant.now()
            )
            // Drain periodically so buffer doesn't overflow
            if (i % 5 == 0) {
                queue.pollBlock { m ->
                    sentMessages.add(m.messageUuid)
                }
                queue.pollCommit()
            }
            queue.add(msg)
        }

        // Drain remaining
        while (true) {
            val count = queue.pollBlock { m ->
                sentMessages.add(m.messageUuid)
            }
            queue.pollCommit()
            if (count == 0) break
        }

        queue.close()
        // Verify messages were read and order preserved
        assertTrue("Expected messages to have been processed", sentMessages.isNotEmpty())
        for (i in 0 until sentMessages.size - 1) {
            val currNum = sentMessages[i].removePrefix("msg-").toInt()
            val nextNum = sentMessages[i + 1].removePrefix("msg-").toInt()
            assertTrue("Messages should maintain sequence order", nextNum > currNum)
        }
    }

    @Test
    fun testQueueFullDetection() {
        // Small queue: queueSize = 4 -> 8192 bytes
        val queue = MessageQueueDisk(
            queueName = "test-full",
            deviceName = "dev2",
            logger = logger,
            queueSize = 4,
            blockSize = 1,
            pollTimeout = 10,
            diskPath = tempFolder.root.absolutePath
        )

        val largePayload = ByteArray(2500) { 1 }
        // Adding messages without dequeuing should eventually mark queueFull
        var dropped = false
        for (i in 1..10) {
            val msg = BrokerMessage(
                messageUuid = "msg-$i",
                messageId = i,
                topicName = "test/full",
                payload = largePayload,
                qosLevel = 1,
                isRetain = false,
                isDup = false,
                isQueued = true,
                clientId = "sender",
                time = Instant.now()
            )
            queue.add(msg)
            if (queue.isQueueFull()) {
                dropped = true
                break
            }
        }

        assertTrue("Queue should have flagged queueFull when capacity exceeded", dropped)
        queue.close()
    }
}
