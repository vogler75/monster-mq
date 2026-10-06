package at.rocworks.peerlink.core

import org.junit.Assert.*
import org.junit.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class PeerLogTest {

    private fun logTestFrame(size: Int, a: Long, b: Long): ByteArray {
        val f = ByteArray(size)
        val bb = ByteBuffer.wrap(f).order(ByteOrder.LITTLE_ENDIAN)
        bb.putInt(size - 4)
        bb.putLong(a)
        bb.putLong(b)
        return f
    }

    private fun logTestMarkers(f: ByteArray): Pair<Long, Long> {
        val bb = ByteBuffer.wrap(f).order(ByteOrder.LITTLE_ENDIAN)
        bb.position(4)
        val a = bb.getLong()
        val b = bb.getLong()
        return Pair(a, b)
    }

    private fun appendSeq(log: PeerLog, n: Int, size: Int = 32) {
        for (i in 0 until n) {
            val next = log.getLEO()
            val (off, ok) = log.append(logTestFrame(size, next, 0), LogKind.Client)
            assertTrue("Append should succeed", ok)
            assertEquals(next, off)
        }
    }

    @Test
    fun testEpochAndDefaults() {
        val a = PeerLog(LogConfig(consumers = listOf("b")))
        val b = PeerLog(LogConfig(consumers = listOf("b")))
        try {
            assertNotEquals(0L, a.epoch)
            assertNotEquals(0L, b.epoch)
            assertNotEquals(a.epoch, b.epoch)

            val (lso, leo) = a.bounds()
            assertEquals(1L, lso)
            assertEquals(1L, leo)

            val s = a.stats()
            assertEquals(2_000_000L, s.maxMessages)
            assertEquals(256L shl 20, s.maxBytes)
            assertEquals((1 shl 20) + (64 shl 10), a.maxRecordBytes)
            assertEquals(1L, a.committed(0))
            assertEquals(0, a.consumerIndex("b"))
            assertEquals(-1, a.consumerIndex("x"))
            assertEquals(1, a.numConsumers())
        } finally {
            a.close()
            b.close()
        }
    }

    @Test
    fun testFrameAccounted() {
        val cases = listOf(
            Pair(4, 8L), Pair(8, 8L), Pair(9, 16L), Pair(200, 208L), Pair(208, 208L),
            Pair(209, 224L), Pair(1000, 1024L), Pair(32768, 32768L), Pair(32769, 40960L), Pair(1 shl 20, (1 shl 20).toLong())
        )
        for ((n, want) in cases) {
            assertEquals("logFrameAccounted($n)", want, logFrameAccounted(n))
        }
    }

    @Test
    fun testAppendAndRead() {
        val log = PeerLog(LogConfig(consumers = listOf("c1")))
        try {
            appendSeq(log, 100)
            assertEquals(101L, log.getLEO())

            val out = ArrayList<ByteArray>()
            val res = log.readFor(0, 1L, 50, 0, out)
            assertEquals(1L, res.base)
            assertEquals(50, res.count)
            assertEquals(0L, res.lost)
            assertFalse(res.truncated)
            assertEquals(50, out.size)

            for (i in 0 until 50) {
                val (a, _) = logTestMarkers(out[i])
                assertEquals(1L + i, a)
            }

            // Read second half
            val res2 = log.readFor(0, 51L, 100, 0, out)
            assertEquals(51L, res2.base)
            assertEquals(50, res2.count)
            assertEquals(0L, res2.lost)
            assertFalse(res2.truncated)
        } finally {
            log.close()
        }
    }

    @Test
    fun testEvictionByCount() {
        val log = PeerLog(LogConfig(maxMessages = 10, maxBytes = 1000000, consumers = listOf("c1")))
        try {
            appendSeq(log, 25)
            val (lso, leo) = log.bounds()
            assertEquals(26L, leo)
            assertEquals(16L, lso) // kept 10 messages: 16..25

            val stats = log.stats()
            assertEquals(15L, stats.evictedByCount)
            assertEquals(15L, stats.evictedUnread)

            val out = ArrayList<ByteArray>()
            val res = log.readFor(0, 5L, 50, 0, out)
            assertEquals(16L, res.base)
            assertEquals(11L, res.lost) // 16 - 5 = 11 lost
            assertEquals(10, res.count)
        } finally {
            log.close()
        }
    }

    @Test
    fun testCommitAndTrim() {
        val log = PeerLog(LogConfig(consumers = listOf("c1", "c2")))
        try {
            appendSeq(log, 50)
            log.commit(0, 20L)
            assertEquals(1L, log.bounds().first) // c2 is still at 1L, so LWM is 1L

            log.commit(1, 15L)
            assertEquals(15L, log.bounds().first) // min(20, 15) = 15L, trimmed to 15L

            val stats = log.stats()
            assertEquals(14L, stats.trimmed) // 1..14 trimmed

            // Commit beyond end should throw
            try {
                log.commit(0, 999L)
                fail("Expected LogCommitBeyondEndException")
            } catch (e: LogCommitBeyondEndException) {
                // Expected
            }
        } finally {
            log.close()
        }
    }

    @Test
    fun testLossAccounting() {
        val log = PeerLog(LogConfig(maxMessages = 10, maxBytes = 1000000, consumers = listOf("c1")))
        try {
            appendSeq(log, 20)
            // LSO is 11, c1 committed is 1. Records 1..10 evicted without being served or committed
            val cs = log.observeConsumer(0)
            assertEquals(10L, cs.lostTotal)
            assertEquals(20L, cs.lag)
        } finally {
            log.close()
        }
    }

    @Test
    fun testResume() {
        val log = PeerLog(LogConfig(consumers = listOf("c1")))
        try {
            appendSeq(log, 50)
            log.commit(0, 30L)

            // Resume same epoch, offset within range
            val r1 = log.resume(0, log.epoch, 35L)
            assertEquals(35L, r1.resumeAt)
            assertTrue(r1.consumerStateUsed)
            assertFalse(r1.sourceReset)
            assertEquals(0L, r1.lostOnResume)

            // Resume same epoch, offset out of range
            try {
                log.resume(0, log.epoch, 100L)
                fail("Expected LogOffsetOutOfRangeException")
            } catch (e: LogOffsetOutOfRangeException) {
                // Expected
            }

            // Resume different epoch (source reset)
            val r2 = log.resume(0, 999999L, 10L)
            assertTrue(r2.sourceReset)
            assertEquals(35L, r2.resumeAt) // uses committed (which advanced to 35)
        } finally {
            log.close()
        }
    }

    @Test
    fun testDrainAndSeal() {
        val log = PeerLog(LogConfig(consumers = listOf("c1")))
        try {
            appendSeq(log, 20)
            log.setConsumerState(0, LogConsumerState.Connected)

            Thread.ofVirtual().start {
                Thread.sleep(50)
                log.commit(0, 21L)
            }

            val drainRes = log.drain(1000L)
            assertTrue(drainRes.complete)
            assertEquals(21L, drainRes.target)
            assertEquals(21L, drainRes.finalLEO)
            assertTrue(log.isSealed())

            // Append after seal must fail and count uncaptured
            val (_, ok) = log.append(logTestFrame(32, 0, 0))
            assertFalse(ok)
            assertEquals(1L, log.stats().uncapturedAtShutdown)
        } finally {
            log.close()
        }
    }

    @Test
    fun testWaiters() {
        val log = PeerLog(LogConfig(consumers = listOf("c1")))
        try {
            val waiter = LogWaiter()
            val woken = AtomicBoolean(false)

            Thread.ofVirtual().start {
                val ok = log.waitFor(waiter, 5L, 2000L)
                woken.set(ok)
            }

            Thread.sleep(30)
            assertFalse(woken.get())

            appendSeq(log, 5)
            // Should be woken now
            Thread.sleep(50)
            assertTrue(woken.get())
        } finally {
            log.close()
        }
    }

    @Test
    fun testConcurrency() {
        val log = PeerLog(LogConfig(maxMessages = 10000, consumers = listOf("c1")))
        val writers = 4
        val msgsPerWriter = 1000
        val latch = CountDownLatch(writers)
        val readErrors = ConcurrentLinkedQueue<Throwable>()

        try {
            // Virtual thread reader
            val readerRunning = AtomicBoolean(true)
            Thread.ofVirtual().start {
                val out = ArrayList<ByteArray>()
                var from = 1L
                while (readerRunning.get()) {
                    try {
                        val res = log.readFor(0, from, 100, 0, out)
                        if (res.count > 0) {
                            from = res.base + res.count
                            log.commit(0, from)
                        } else {
                            Thread.sleep(1)
                        }
                    } catch (t: Throwable) {
                        readErrors.add(t)
                        break
                    }
                }
            }

            for (w in 0 until writers) {
                Thread.ofVirtual().start {
                    try {
                        for (i in 0 until msgsPerWriter) {
                            val next = log.getLEO()
                            log.append(logTestFrame(32, next, 0))
                        }
                    } finally {
                        latch.countDown()
                    }
                }
            }

            assertTrue("Writers should complete within 5s", latch.await(5, TimeUnit.SECONDS))
            Thread.sleep(100)
            readerRunning.set(false)

            assertTrue("No read errors during concurrent write/read", readErrors.isEmpty())
            assertEquals((writers * msgsPerWriter + 1).toLong(), log.getLEO())
        } finally {
            log.close()
        }
    }
}
