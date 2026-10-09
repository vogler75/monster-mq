package at.rocworks.peerlink

import at.rocworks.peerlink.core.*
import at.rocworks.peerlink.wire.*
import org.junit.Assert.*
import org.junit.Test
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.logging.Logger

// Persistent-expiry backlog reclamation (plan-peerlink-interest-routing 6.5 and section 11): expiring one
// of overlapping PER filters abandons only the uncovered non-retained backlog of that peer, over several
// MaxScanPerFetch ticks, without Lost/GAP and without touching the other consumer's bits.
class InterestSweepTest {

    private val logger = Logger.getLogger(InterestSweepTest::class.java.name)

    private fun frame(topic: String, flags: Int = 1): ByteArray {
        val r = Record(flags = flags, topic = topic, clientID = "c", payload = topic.toByteArray())
        return ByteArray(recordSize(r)).also { encodeRecord(it, r) }
    }

    private fun full(gen: Long, vararg es: InterestEntry) =
        InterestSnapshot(gen, InterestFlagFirst or InterestFlagLast, es.toMutableList())

    // Reads everything consumer c is served from its committed offset, committing as it goes.
    private fun drain(log: PeerLog, c: Int): Pair<List<String>, Long> {
        val topics = ArrayList<String>()
        var lost = 0L
        val out = ArrayList<ByteArray>()
        val deltas = IntList()
        val view = RecordView()
        var from = log.committed(c)
        while (from < log.getLEO()) {
            val r = log.readSparse(c, from, 10_000, 1 shl 24, out, deltas)
            lost += r.lost
            for (f in out) { decodeRecord(f, view); topics.add(view.topicString()) }
            val next = r.base + maxOf(r.span, r.count.toLong())
            if (next <= from) break
            log.commit(c, next)
            from = next
        }
        return Pair(topics, lost)
    }

    @Test
    fun testOverlappingExpiryReclaimedOverTicks() {
        val maxScan = 1024
        val log = PeerLog(LogConfig(consumers = listOf("b", "c"), masked = true, maxScan = maxScan))
        val table = InterestTable(listOf(InterestTable.Consumer("b", true), InterestTable.Consumer("c", true)),
            false, 100, 1024, logger)
        try {
            table.connect(0, 1L, true)
            table.applySnapshot(0, full(1,
                InterestEntry(InterestPer, 10, "a/#"),                      // expires
                InterestEntry(InterestPer, InterestExpiryNever, "a/keep/#"), // remains (PER)
                InterestEntry(InterestVol, 0, "a/vol")))                    // remains (VOL)
            table.connect(1, 1L, true)
            table.applySnapshot(1, full(1, InterestEntry(InterestVol, 0, "#")))

            // Four records per round: uncovered after expiry, covered by PER, covered by VOL, retained.
            val rounds = 1000
            var expectDiscard = 0L
            for (i in 0 until rounds) {
                for ((topic, flags) in listOf("a/x/$i" to 1, "a/keep/$i" to 1, "a/vol" to 1, "a/r/$i" to (1 or FlagRetain))) {
                    val mask = table.match(topic)
                    assertEquals(3L, mask)
                    assertTrue(log.appendMask(frame(topic, flags), LogKind.Client, mask).second)
                }
                expectDiscard++
            }
            val total = log.getLEO() - 1
            assertTrue(total > 3L * maxScan)

            table.disconnect(0, nowMs = 0L)
            table.expire(nowMs = 5_000L)
            assertEquals(0L, table.persistentExpired.get())
            table.expire(nowMs = 11_000L)
            assertEquals(1L, table.persistentExpired.get())
            assertEquals(0L, table.match("a/x/new") and 1L)
            assertEquals(1L, table.match("a/keep/new") and 1L)

            // Consumer c fetches while b's backlog is swept.
            val stop = AtomicBoolean(false)
            val failure = AtomicReference<Throwable>()
            val cTopics = ArrayList<String>()
            var cLost = 0L
            val reader = Thread {
                try {
                    while (!stop.get()) {
                        val (t, l) = drain(log, 1)
                        synchronized(cTopics) { cTopics.addAll(t) }
                        cLost += l
                        Thread.sleep(1)
                    }
                } catch (e: Throwable) { failure.set(e) }
            }
            reader.start()

            var ticks = 0
            var prev = -1L
            while (table.backlogDiscarded.get() != prev || ticks == 0) {
                prev = table.backlogDiscarded.get()
                table.sweep(log)
                ticks++
                assertTrue("sweep does not finish", ticks < 100)
            }
            stop.set(true)
            reader.join()
            failure.get()?.let { throw it }

            assertEquals(expectDiscard, table.backlogDiscarded.get())
            assertTrue("reclaim took $ticks ticks", ticks >= (total / maxScan).toInt())

            // b is served only the covered and retained records, without loss.
            val (bTopics, bLost) = drain(log, 0)
            assertEquals(0L, bLost)
            assertEquals(3 * rounds, bTopics.size)
            assertTrue(bTopics.none { it.startsWith("a/x/") })
            assertEquals(rounds, bTopics.count { it.startsWith("a/r/") })
            assertEquals(0L, log.observeConsumer(0).lostTotal)

            // c keeps every record.
            val (rest, restLost) = drain(log, 1)
            cTopics.addAll(rest)
            assertEquals(0L, cLost + restLost)
            assertEquals(total.toInt(), cTopics.size)
            assertEquals(0L, log.observeConsumer(1).lostTotal)
        } finally {
            log.close()
        }
    }

    @Test
    fun testUnsubscribeDoesNotAbandonBacklog() {
        val log = PeerLog(LogConfig(consumers = listOf("b"), masked = true, maxScan = 1024))
        val table = InterestTable(listOf(InterestTable.Consumer("b", true)), false, 100, 1024, logger)
        try {
            table.connect(0, 1L, true)
            table.applySnapshot(0, full(1, InterestEntry(InterestVol, 0, "a/#")))
            for (i in 0 until 10) log.appendMask(frame("a/$i"), LogKind.Client, table.match("a/$i"))
            // An ordinary unsubscribe (delta NONE) keeps what was captured.
            table.applyDelta(0, InterestDelta(2, mutableListOf(InterestEntry(InterestNone, 0, "a/#"))))
            table.sweep(log)
            assertEquals(0L, table.backlogDiscarded.get())
            assertEquals(10, drain(log, 0).first.size)
        } finally {
            log.close()
        }
    }
}
