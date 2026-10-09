package at.rocworks.peerlink

import at.rocworks.peerlink.wire.*
import org.junit.Assert.*
import org.junit.Test
import java.util.logging.Logger

class InterestTableTest {

    private val logger = Logger.getLogger(InterestTableTest::class.java.name)

    private fun table(
        unknownAll: Boolean = true,
        maxFilters: Int = 100,
        consumers: List<InterestTable.Consumer> = listOf(InterestTable.Consumer("b", true), InterestTable.Consumer("c", true))
    ) = InterestTable(consumers, unknownAll, maxFilters, 1024, logger)

    private fun vol(f: String) = InterestEntry(InterestVol, 0L, f)
    private fun per(f: String, exp: Long) = InterestEntry(InterestPer, exp, f)
    private fun none(f: String) = InterestEntry(InterestNone, 0L, f)

    private fun snap(gen: Long, flags: Int, vararg es: InterestEntry) =
        InterestSnapshot(gen, flags, es.toMutableList())

    private fun full(gen: Long, vararg es: InterestEntry) = snap(gen, InterestFlagFirst or InterestFlagLast, *es)

    private fun delta(gen: Long, vararg es: InterestEntry) = InterestDelta(gen, es.toMutableList())

    @Test
    fun testUnknownMode() {
        assertEquals(3L, table(unknownAll = true).match("a/b"))
        val t = table(unknownAll = false)
        assertEquals(0L, t.match("a/b"))
        assertEquals("NONE", t.status(0)!!.mode)
        assertEquals("UNKNOWN", t.status(0)!!.state)
    }

    @Test
    fun testSnapshotAndDelta() {
        val t = table(unknownAll = false)
        t.connect(0, 1L, true)
        t.applySnapshot(0, full(5, vol("a/+"), per("p/#", 60)))
        assertEquals(1L, t.match("a/x"))
        assertEquals(1L, t.match("p/q/r"))
        assertEquals(0L, t.match("b"))
        val st = t.status(0)!!
        assertEquals("LIVE", st.state)
        assertEquals("FILTERED", st.mode)
        assertEquals(2, st.filters)
        assertEquals(1, st.filtersPersistent)
        assertEquals(5L, st.snapshotGeneration)

        t.applyDelta(0, delta(6, none("a/+"), vol("b")))
        assertEquals(0L, t.match("a/x"))
        assertEquals(1L, t.match("b"))

        // Generations not above the last are ignored.
        t.applyDelta(0, delta(6, vol("old")))
        t.applyDelta(0, delta(3, vol("old")))
        assertEquals(0L, t.match("old"))
        assertEquals(3L, t.deltasReceived.get())
    }

    @Test
    fun testChunkedSnapshotReplaces() {
        val t = table(unknownAll = false)
        t.connect(0, 1L, true)
        t.applySnapshot(0, full(1, vol("old")))
        t.applySnapshot(0, snap(2, InterestFlagFirst, vol("x")))
        // The old set stays until LAST.
        assertEquals(1L, t.match("old"))
        assertEquals(0L, t.match("x"))
        t.applySnapshot(0, snap(2, InterestFlagLast, vol("y")))
        assertEquals(0L, t.match("old"))
        assertEquals(1L, t.match("x"))
        assertEquals(1L, t.match("y"))
    }

    @Test
    fun testMalformedSnapshot() {
        val t = table()
        t.connect(0, 1L, true)
        assertThrows(InterestProtocolException::class.java) { t.applySnapshot(0, snap(1, InterestFlagLast, vol("x"))) }
        t.applySnapshot(0, snap(1, InterestFlagFirst, vol("x")))
        assertThrows(InterestProtocolException::class.java) { t.applySnapshot(0, snap(2, InterestFlagLast, vol("y"))) }
    }

    @Test
    fun testRejectedEntries() {
        val t = table(unknownAll = false)
        t.connect(0, 1L, true)
        val badUtf8 = InterestEntry(InterestVol, 0L, "x").also { it.filterBytes = byteArrayOf(0xC3.toByte()) }
        t.applySnapshot(0, full(1, vol("ok"), none("n"), vol("a/#/b"), vol(""), InterestEntry(7, 0L, "c"),
            vol("x".repeat(1025)), vol("a\u0000b"), badUtf8))
        assertEquals(7L, t.rejected.get())
        assertEquals(1, t.status(0)!!.filters)
        t.applyDelta(0, delta(2, none("ok"), vol("+/+/#"), vol("a+")))
        assertEquals(8L, t.rejected.get())
        assertEquals(1, t.status(0)!!.filters)
        assertEquals(1L, t.match("q/r/s"))
    }

    @Test
    fun testOverLimitServesAllThenRestores() {
        val t = table(unknownAll = false, maxFilters = 2)
        t.connect(0, 1L, true)
        t.applySnapshot(0, full(1, vol("a"), vol("b"), vol("c")))
        assertEquals("ALL", t.status(0)!!.mode)
        assertEquals(1L, t.match("zzz"))
        assertEquals(1L, t.overLimitCount.get())

        t.applySnapshot(0, full(2, vol("a")))
        assertEquals("FILTERED", t.status(0)!!.mode)
        assertEquals(0L, t.match("zzz"))
        assertEquals(1L, t.match("a"))

        // A delta that crosses the limit switches to ALL as well.
        t.applyDelta(0, delta(3, vol("b"), vol("c")))
        assertEquals("ALL", t.status(0)!!.mode)
        assertEquals(2L, t.overLimitCount.get())
    }

    @Test
    fun testNotCapableIsDense() {
        val t = table(unknownAll = false)
        t.connect(0, 1L, false)
        assertEquals(1L, t.match("anything"))
        assertEquals("ALL", t.status(0)!!.mode)
        t.connect(0, 1L, true)
        assertEquals("UNKNOWN", t.status(0)!!.state)
        assertEquals(0L, t.match("anything"))
    }

    @Test
    fun testDisconnectKeepsFilteringAndRestartDropsVolatile() {
        val t = table(unknownAll = true)
        t.connect(0, 1L, true)
        t.applySnapshot(0, full(1, vol("v"), per("p", InterestExpiryNever)))
        t.disconnect(0)
        assertEquals("DISCONNECTED", t.status(0)!!.state)
        assertEquals("FILTERED", t.status(0)!!.mode)
        assertEquals(1L, t.match("v") and 1L)
        assertEquals(0L, t.match("other") and 1L)

        // Same instance: everything kept.
        t.connect(0, 1L, true)
        assertEquals(2, t.status(0)!!.filters)
        t.disconnect(0)

        // New instance: volatile entries dropped.
        t.connect(0, 2L, true)
        assertEquals(1, t.status(0)!!.filters)
        assertEquals(1L, t.volatileDropped.get())
        assertEquals(0L, t.match("v") and 1L)
        assertEquals(1L, t.match("p") and 1L)
    }

    @Test
    fun testPersistentExpiry() {
        val t = table(unknownAll = false)
        t.connect(0, 1L, true)
        t.applySnapshot(0, full(1, per("short", 10), per("never", InterestExpiryNever)))
        t.disconnect(0, nowMs = 1_000L)
        t.expire(nowMs = 5_000L)
        assertEquals(2, t.status(0)!!.filters)
        t.expire(nowMs = 12_000L)
        assertEquals(1, t.status(0)!!.filters)
        assertEquals(1L, t.persistentExpired.get())
        assertEquals(0L, t.match("short"))
        assertEquals(1L, t.match("never"))
    }

    @Test
    fun testDisabledConsumerAlwaysServed() {
        val t = table(unknownAll = false, consumers = listOf(InterestTable.Consumer("b", false)))
        t.connect(0, 1L, true)
        t.applySnapshot(0, full(1, vol("a")))
        assertEquals(1L, t.match("zzz"))
        assertEquals("OFF", t.status(0)!!.state)
    }
}
