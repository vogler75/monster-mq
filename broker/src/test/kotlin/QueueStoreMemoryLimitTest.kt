package at.rocworks

import at.rocworks.data.BrokerMessage
import at.rocworks.stores.memory.QueueStoreMemory
import org.junit.Assert.assertEquals
import org.junit.Test
import java.time.Instant

class QueueStoreMemoryLimitTest {

    @Test
    fun testQueueStoreMemoryCapsCapacityAndEvictsOldest() {
        // Create memory queue store with max 3 messages per client
        val store = QueueStoreMemory(visibilityTimeoutSeconds = 30, maxQueuedMessagesPerClient = 3)

        val clientId = "test-client"
        val messages = (1..5).map { i ->
            BrokerMessage(
                messageUuid = "uuid-$i",
                messageId = i,
                topicName = "test/topic",
                payload = "payload-$i".toByteArray(),
                qosLevel = 1,
                isRetain = false,
                isDup = false,
                isQueued = false,
                clientId = "publisher",
                time = Instant.now()
            ) to listOf(clientId)
        }

        store.enqueueMessages(messages)

        // Capacity cap check
        assertEquals(3L, store.countQueuedMessagesForClient(clientId))

        // FIFO eviction check: the 2 oldest messages ("uuid-1", "uuid-2") should be evicted,
        // leaving "uuid-3", "uuid-4", "uuid-5"
        val pending = store.fetchPendingMessages(clientId, limit = 10)
        assertEquals(3, pending.size)
        assertEquals("uuid-3", pending[0].messageUuid)
        assertEquals("uuid-4", pending[1].messageUuid)
        assertEquals("uuid-5", pending[2].messageUuid)
    }

    @Test
    fun testQueueStoreMemoryUnboundedWhenLimitIsZero() {
        val store = QueueStoreMemory(visibilityTimeoutSeconds = 30, maxQueuedMessagesPerClient = 0)
        val clientId = "unbounded-client"
        val messages = (1..10).map { i ->
            BrokerMessage(
                messageUuid = "uuid-$i",
                messageId = i,
                topicName = "test/topic",
                payload = "payload-$i".toByteArray(),
                qosLevel = 1,
                isRetain = false,
                isDup = false,
                isQueued = false,
                clientId = "publisher",
                time = Instant.now()
            ) to listOf(clientId)
        }

        store.enqueueMessages(messages)
        assertEquals(10L, store.countQueuedMessagesForClient(clientId))
    }
}
