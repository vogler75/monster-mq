package at.rocworks.peerlink

import at.rocworks.bus.IMessageBus
import at.rocworks.data.BrokerMessage
import at.rocworks.handlers.MessageHandler
import at.rocworks.handlers.SessionHandler
import at.rocworks.peerlink.core.IncludeExclude
import at.rocworks.peerlink.wire.*
import org.junit.Assert.*
import org.junit.Test
import org.mockito.Mockito

class InjectorTest {

    @Test
    fun testPacerTokenBucket() {
        val pacer = Pacer(factor = 3.0, maxRate = 0.0)

        // With zero lag, limit should be 0 (no pacing)
        val rateZero = pacer.limit(0, 1000)
        assertEquals(0.0, rateZero, 0.001)

        // With lag exceeding threshold (1500 > 1000), limit is at least floor (1000)
        val rateLag = pacer.limit(1500, 1000)
        assertTrue(rateLag >= 1000.0)

        // Taking tokens when burst is available does not sleep
        val slept = pacer.take(100.0)
        assertFalse(slept)
    }

    @Test
    fun testRecordDropOnSizeLimit() {
        val sessionHandler = Mockito.mock(SessionHandler::class.java)
        val messageBus = TestMessageBus()

        val injector = Injector(
            sourceNodeId = "peer-src",
            sessionHandler = sessionHandler,
            messageBus = messageBus,
            filter = IncludeExclude.create(listOf("#"), emptyList()),
            hook = null,
            maxMessageSize = 50 // small limit
        )

        val rec = Record(
            topic = "test/large",
            payload = ByteArray(100) // exceeds 50
        )
        val buf = ByteArray(recordSize(rec))
        encodeRecord(buf, rec)

        val batch = Batch(
            header = BatchHeader(baseOffset = 1L, count = 1, recordsBytes = buf.size),
            records = buf
        )
        val batchIn = BatchIn(batch, System.currentTimeMillis())
        val ac = ApplyContext(epoch = 1L)

        injector.applyBatch(ac, batchIn)

        // Should have been dropped due to size
        assertEquals(1, injector.dropped[DROP_SIZE].sum())
        assertEquals(0, injector.injected.sum())
    }

    @Test
    fun testRetainedSnapshotFillSkipping() {
        val messageHandler = Mockito.mock(MessageHandler::class.java)
        val sessionHandler = Mockito.mock(SessionHandler::class.java) { invocation ->
            if (invocation.method.name == "getMessageHandler") messageHandler
            else Mockito.RETURNS_DEFAULTS.answer(invocation)
        }
        val messageBus = TestMessageBus()

        val testStore = TestMessageStore()
        testStore.store["retained/topic"] = BrokerMessage(
            topicName = "retained/topic",
            payload = "old".toByteArray(),
            qosLevel = 0,
            isRetain = true,
            clientId = "c1"
        )
        Mockito.`when`(messageHandler.getRetainedStore()).thenReturn(testStore)

        val injector = Injector(
            sourceNodeId = "peer-src",
            sessionHandler = sessionHandler,
            messageBus = messageBus,
            filter = IncludeExclude.create(listOf("#"), emptyList()),
            hook = null,
            maxMessageSize = 1024
        )

        val rec = Record(
            flags = FlagSnapshot or FlagRetain,
            topic = "retained/topic",
            payload = "new".toByteArray()
        )
        val buf = ByteArray(recordSize(rec))
        encodeRecord(buf, rec)

        val batch = Batch(
            header = BatchHeader(flags = BatchFlagSnapshot, baseOffset = 0L, count = 1, recordsBytes = buf.size),
            records = buf
        )
        val batchIn = BatchIn(batch, System.currentTimeMillis())
        val ac = ApplyContext(epoch = 1L, mode = SNAP_FILL)

        injector.applyBatch(ac, batchIn)

        // In SNAP_FILL mode, existing retained topic should be skipped
        assertEquals(1, injector.snapSkipped.sum())
        assertEquals(0, injector.injected.sum())
    }
}
