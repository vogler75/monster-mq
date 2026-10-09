package at.rocworks.peerlink

import at.rocworks.peerlink.wire.*
import org.junit.Assert.*
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger

class InterestTrackerTest {

    private val logger = Logger.getLogger(InterestTrackerTest::class.java.name)

    private fun tracker(
        maxRetainedFrames: Int = 256,
        classifier: ClientClassifier = ClientClassifier { InterestClass.VOL }
    ) = InterestTracker(flushMs = 10, maxFilterBytes = 32768, classifier = classifier,
        maxRetainedFrames = maxRetainedFrames, logger = logger)

    // Subscribes a feed and consumes its initial snapshot.
    private fun live(t: InterestTracker, wakes: AtomicInteger = AtomicInteger()): InterestTracker.Feed {
        val f = t.subscribe { wakes.incrementAndGet() }
        val snap = f.poll()
        assertTrue(snap.all { it is InterestSnapshot })
        return f
    }

    private fun deltaEntries(frames: List<Frame>): List<InterestEntry> =
        frames.flatMap { (it as InterestDelta).entries }

    @Test
    fun testInitialSnapshotEmpty() {
        val t = tracker()
        val f = t.subscribe {}
        val frames = f.poll()
        assertEquals(1, frames.size)
        val s = frames[0] as InterestSnapshot
        assertEquals(1L, s.generation)
        assertEquals(InterestFlagFirst or InterestFlagLast, s.flags)
        assertTrue(s.entries.isEmpty())
        assertTrue(f.poll().isEmpty())
    }

    @Test
    fun testAddRemoveDeltas() {
        val t = tracker()
        val wakes = AtomicInteger()
        val f = live(t, wakes)
        t.added("c1", "a/b")
        t.added("c2", "a/b")
        t.drainEvents()
        t.flush()
        assertEquals(1, wakes.get())
        val d1 = f.poll()
        assertEquals(1, d1.size)
        assertEquals(2L, (d1[0] as InterestDelta).generation)
        assertEquals(listOf(InterestEntry(InterestVol, 0L, "a/b")), deltaEntries(d1))

        // One owner left: no change.
        t.removed("c1", "a/b")
        t.drainEvents()
        t.flush()
        assertTrue(f.poll().isEmpty())

        t.removed("c2", "a/b")
        t.drainEvents()
        t.flush()
        val d2 = f.poll()
        assertEquals(3L, (d2[0] as InterestDelta).generation)
        assertEquals(listOf(InterestEntry(InterestNone, 0L, "a/b")), deltaEntries(d2))
        assertTrue(t.announced().isEmpty())
        assertEquals(2L, t.status().generation)
    }

    @Test
    fun testClassesAggregate() {
        val classes = mapOf("vol" to InterestClass.VOL, "per" to InterestClass(InterestPer, 100L))
        val t = tracker(classifier = ClientClassifier { classes[it] })
        val f = live(t)
        t.added("vol", "x")
        t.added("unknown", "y")
        t.drainEvents()
        t.flush()
        assertEquals(listOf(InterestEntry(InterestVol, 0L, "x")), deltaEntries(f.poll()))

        t.added("per", "x")
        t.drainEvents()
        t.flush()
        assertEquals(listOf(InterestEntry(InterestPer, 100L, "x")), deltaEntries(f.poll()))

        t.removed("per", "x")
        t.drainEvents()
        t.flush()
        assertEquals(listOf(InterestEntry(InterestVol, 0L, "x")), deltaEntries(f.poll()))
    }

    @Test
    fun testNeverAnnounced() {
        val t = tracker()
        val f = live(t)
        t.added("peerlink:node-b", "a/#")
        t.added("c1", "\$SYS/#")
        t.added("c1", "\$share/g/a")
        t.added("c1", "a/#/b")
        t.added("c1", "")
        t.drainEvents()
        t.flush()
        assertTrue(f.poll().isEmpty())
        assertTrue(t.announced().isEmpty())
        assertEquals(2L, t.rejected.get())
    }

    @Test
    fun testExpiryTenPercentRule() {
        val t = tracker()
        val f = live(t)
        fun set(c: InterestClass): List<InterestEntry> {
            t.setSource("s", mapOf("x" to c))
            t.flush()
            return deltaEntries(f.poll())
        }
        assertEquals(1, set(InterestClass(InterestPer, 100L)).size)
        assertTrue(set(InterestClass(InterestPer, 105L)).isEmpty())
        assertTrue(set(InterestClass(InterestPer, 110L)).isEmpty())
        // 120 against the announced 100 is 20 %.
        assertEquals(listOf(InterestEntry(InterestPer, 120L, "x")), set(InterestClass(InterestPer, 120L)))
        assertEquals(1, set(InterestClass.PER_NEVER).size)
        assertEquals(1, set(InterestClass(InterestPer, 120L)).size)
        assertEquals(listOf(InterestEntry(InterestVol, 0L, "x")), set(InterestClass.VOL))
    }

    @Test
    fun testSetSourceReplaces() {
        val t = tracker()
        val f = live(t)
        t.setSource("archive", mapOf("a/#" to InterestClass.PER_NEVER, "b/#" to InterestClass.PER_NEVER,
            "\$SYS/#" to InterestClass.PER_NEVER, "bad/#/x" to InterestClass.PER_NEVER))
        t.flush()
        assertEquals(setOf("a/#", "b/#"), deltaEntries(f.poll()).map { it.filter }.toSet())
        assertEquals(1L, t.rejected.get())

        t.setSource("archive", mapOf("b/#" to InterestClass.PER_NEVER))
        t.flush()
        assertEquals(listOf(InterestEntry(InterestNone, 0L, "a/#")), deltaEntries(f.poll()))
        assertEquals(setOf("b/#"), t.announced().keys)

        // A new feed gets the announced set as snapshot.
        val s = t.subscribe {}.poll()
        assertEquals(listOf(InterestEntry(InterestPer, InterestExpiryNever, "b/#")),
            s.flatMap { (it as InterestSnapshot).entries })
    }

    @Test
    fun testSplitByCount() {
        val t = tracker()
        val f = live(t)
        t.setSource("s", (0 until 1500).associate { "t/$it" to InterestClass.VOL })
        t.flush()
        val frames = f.poll()
        assertEquals(2, frames.size)
        assertEquals(InterestTracker.MaxPendingEntries, (frames[0] as InterestDelta).entries.size)
        assertEquals(476, (frames[1] as InterestDelta).entries.size)
        assertEquals(listOf(2L, 3L), frames.map { (it as InterestDelta).generation })

        val snap = t.subscribe {}.poll().map { it as InterestSnapshot }
        assertEquals(2, snap.size)
        assertEquals(InterestFlagFirst, snap[0].flags)
        assertEquals(InterestFlagLast, snap[1].flags)
        assertTrue(snap.all { it.generation == snap[0].generation })
        assertEquals(1500, snap.sumOf { it.entries.size })
    }

    @Test
    fun testSplitByBytes() {
        val t = tracker()
        val f = live(t)
        val pad = "p".repeat(1000)
        t.setSource("s", (0 until 100).associate { "$pad/$it" to InterestClass.VOL })
        t.flush()
        val frames = f.poll().map { it as InterestDelta }
        assertTrue(frames.size >= 2)
        assertEquals(100, frames.sumOf { it.entries.size })
        for (d in frames) {
            val wb = WireBuffer()
            d.appendFrame(wb)
            assertTrue(wb.length <= MaxConsumerFrame)
        }
    }

    @Test
    fun testOverflowForcesSnapshot() {
        val t = tracker(maxRetainedFrames = 2)
        val f = live(t)
        for (i in 0 until 3) {
            t.setSource("s$i", mapOf("t/$i" to InterestClass.VOL))
            t.flush()
        }
        val frames = f.poll()
        assertEquals(1, frames.size)
        val s = frames[0] as InterestSnapshot
        assertEquals(InterestFlagFirst or InterestFlagLast, s.flags)
        assertEquals(setOf("t/0", "t/1", "t/2"), s.entries.map { it.filter }.toSet())

        // Deltas continue after the snapshot's generation.
        t.setSource("s3", mapOf("t/3" to InterestClass.VOL))
        t.flush()
        assertEquals(s.generation + 1, (f.poll()[0] as InterestDelta).generation)
    }

    @Test
    fun testUnsubscribedFeedGetsNothing() {
        val t = tracker()
        val wakes = AtomicInteger()
        val f = live(t, wakes)
        t.unsubscribe(f)
        t.setSource("s", mapOf("x" to InterestClass.VOL))
        t.flush()
        assertEquals(0, wakes.get())
        assertTrue(f.poll().isEmpty())
    }
    @Test
    fun testGenerationRolloverResetsBySnapshot() {
        val t = tracker()
        val f = live(t)
        f.generation = InterestTracker.GenerationLimit - 2
        t.setSource("a", mapOf("a" to InterestClass.VOL))
        t.flush()
        assertEquals(InterestTracker.GenerationLimit - 1, (f.poll()[0] as InterestDelta).generation)
        // The next delta would reach the limit: a snapshot restarts at generation 1 instead of wrapping.
        t.setSource("b", mapOf("b" to InterestClass.VOL))
        t.flush()
        val frames = f.poll()
        assertEquals(1, frames.size)
        val s = frames[0] as InterestSnapshot
        assertEquals(1L, s.generation)
        assertEquals(setOf("a", "b"), s.entries.map { it.filter }.toSet())
        t.setSource("c", mapOf("c" to InterestClass.VOL))
        t.flush()
        assertEquals(2L, (f.poll()[0] as InterestDelta).generation)
    }

    // Snapshot during concurrent churn, with a small retained history that forces snapshots: a source
    // table fed by the frames ends with exactly the tracker's announced set.
    @Test
    fun testChurnConvergesOnSourceTable() {
        val t = InterestTracker(flushMs = 1, maxFilterBytes = 32768, classifier = ClientClassifier { InterestClass.VOL },
            maxRetainedFrames = 3, logger = logger)
        val table = InterestTable(listOf(InterestTable.Consumer("b", true)), false, 100_000, 1024, logger)
        table.connect(0, 1L, true)
        val f = t.subscribe {}
        t.start()
        val stop = java.util.concurrent.atomic.AtomicBoolean(false)
        val snapshots = AtomicInteger()
        val failure = java.util.concurrent.atomic.AtomicReference<Throwable>()
        val writersDone = java.util.concurrent.atomic.AtomicBoolean(false)
        val puller = Thread {
            var polls = 0
            try {
                while (!stop.get()) {
                    for (fr in f.poll()) when (fr) {
                        is InterestSnapshot -> { table.applySnapshot(0, fr); if (fr.flags and InterestFlagLast != 0) snapshots.incrementAndGet() }
                        is InterestDelta -> table.applyDelta(0, fr)
                        else -> {}
                    }
                    // Every few polls, stop reading until the retained history overflows (or churn ends).
                    if (++polls % 5 == 0) {
                        val end = System.currentTimeMillis() + 500
                        while (!f.needsSnapshot && !writersDone.get() && System.currentTimeMillis() < end) Thread.sleep(1)
                    } else Thread.sleep(1)
                }
            } catch (e: Throwable) { failure.set(e) }
        }
        puller.start()
        val writers = (0 until 4).map { w ->
            Thread {
                val r = java.util.Random(w.toLong())
                repeat(5000) { n ->
                    if (n % 500 == 0) Thread.sleep(5)
                    val topic = "t/${r.nextInt(200)}"
                    if (r.nextBoolean()) t.added("c$w", topic) else t.removed("c$w", topic)
                }
            }
        }
        writers.forEach { it.start() }
        writers.forEach { it.join() }
        writersDone.set(true)
        // Let the worker flush the last changes, then drain.
        Thread.sleep(200)
        t.flush()
        Thread.sleep(50)
        stop.set(true)
        puller.join()
        t.stop()
        failure.get()?.let { throw it }
        for (fr in f.poll()) when (fr) {
            is InterestSnapshot -> table.applySnapshot(0, fr)
            is InterestDelta -> table.applyDelta(0, fr)
            else -> {}
        }
        val announced = t.announced().keys
        assertEquals(announced.size, table.status(0)!!.filters)
        for (i in 0 until 200) assertEquals("t/$i", if ("t/$i" in announced) 1L else 0L, table.match("t/$i"))
        assertTrue("no snapshot was forced during churn", snapshots.get() > 1)
    }
}
