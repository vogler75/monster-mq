package at.rocworks.peerlink

import at.rocworks.data.BrokerMessage
import at.rocworks.peerlink.core.IncludeExclude
import at.rocworks.peerlink.core.IntList
import at.rocworks.peerlink.core.LogConfig
import at.rocworks.peerlink.core.PeerLog
import at.rocworks.peerlink.config.PeerConfig
import at.rocworks.peerlink.wire.*
import org.junit.Assert.*
import org.junit.Assume.assumeTrue
import org.junit.Test
import java.lang.management.ManagementFactory
import java.util.logging.Logger

// Gate G-IR1 (plan-peerlink-interest-routing 9, edge 12.4): 0 %, 10 % and 100 % of the publishes match a
// remote interest of 1k or 10k filters (half wildcards) of two consumers, compared with interest routing
// off. The allocation check always runs. The benchmarks run with -Dpeerlink.bench=true
// (mvn -o test -Dtest=InterestBenchTest -Dpeerlink.bench=true). As on the edge broker (edge plan 12.4),
// the pass criteria apply to the end-to-end throughput, measured by benchmarkGateIR1 for a source serving
// two consumers over loopback TCP. benchmarkSourceSide reports capture ns/op and log bytes of the source
// alone; there the match (about 40-50 ns) roughly doubles the capture of a needed publish, as on edge.
class InterestBenchTest {

    private val logger = Logger.getLogger(InterestBenchTest::class.java.name)
    private val consumers = listOf("broker-b", "broker-c")

    // Filters: even i exact "site/i/temp", odd i wildcard "site/i/+/value".
    private fun filters(n: Int): List<String> =
        (0 until n).map { i -> if (i % 2 == 0) "site/$i/temp" else "site/$i/+/value" }

    private fun interesting(i: Int): String = if (i % 2 == 0) "site/$i/temp" else "site/$i/dev7/value"

    // Shares the first two levels with the filters, so the trie walk does not stop at the root.
    private fun boring(i: Int): String = "site/$i/humidity"

    private fun table(n: Int): InterestTable {
        val t = InterestTable(consumers.map { InterestTable.Consumer(it, true) }, false, n + 1, 1024, logger)
        val entries = filters(n).map { InterestEntry(InterestVol, 0L, it) }
        for (c in consumers.indices) {
            t.connect(c, 1L, true)
            t.applySnapshot(c, InterestSnapshot(1L, InterestFlagFirst or InterestFlagLast, entries.toMutableList()))
        }
        return t
    }

    private fun msg(topic: String) = BrokerMessage(
        topicName = topic,
        payload = ByteArray(64),
        qosLevel = 1,
        isRetain = false,
        clientId = "device-1"
    )

    private fun hook(log: PeerLog, table: InterestTable?): CaptureHook {
        val h = CaptureHook(log = log, filter = IncludeExclude.create(emptyList(), emptyList()))
        h.interest = table
        h.active.set(true)
        return h
    }

    @Test
    fun testSkippedPublishDoesNotAllocate() {
        val bean = ManagementFactory.getThreadMXBean() as? com.sun.management.ThreadMXBean
        assumeTrue(bean != null && bean.isThreadAllocatedMemorySupported)
        bean!!.isThreadAllocatedMemoryEnabled = true
        for (n in listOf(1000, 10000)) {
            val table = table(n)
            PeerLog(LogConfig(consumers = consumers, masked = true)).use { log ->
                val h = hook(log, table)
                val msgs = Array(1024) { msg(boring(it * 7 % n)) }
                // Warm up so that the measured loop runs compiled code; indexed loops allocate no iterator.
                repeat(200) { for (i in msgs.indices) h.capture(msgs[i]) }
                // A single round can pick up unrelated allocations (JIT compilation); one round in five
                // must allocate nothing.
                val tid = Thread.currentThread().id
                val rounds = (0 until 5).map {
                    val before = bean.getThreadAllocatedBytes(tid)
                    repeat(100) { for (i in msgs.indices) h.capture(msgs[i]) }
                    bean.getThreadAllocatedBytes(tid) - before
                }
                assertEquals("bytes allocated by ${100 * msgs.size} skipped publishes ($n filters): $rounds",
                    0L, rounds.minOrNull())
                assertEquals(1L, log.getLEO()) // empty: offsets start at 1
                assertEquals(700L * msgs.size, table.skipped.sum())
            }
        }
    }

    private class Result(val opsPerSec: Double, val nsPerCapture: Double, val logBytes: Long, val delivered: Long)

    // Captures total publishes in batches and serves both consumers after each batch, as the PeerServer
    // read loop does: read (sparse with interest), copy the frames to a socket buffer, commit.
    private fun run(n: Int, percent: Int, interest: Boolean, total: Int): Result {
        val table = if (interest) table(n) else null
        val log = PeerLog(LogConfig(consumers = consumers, masked = interest))
        log.use {
            val h = hook(log, table)
            val msgs = (0 until 1000).map { i ->
                msg(if (i % 100 < percent) interesting(i * 7 % n) else boring(i * 7 % n))
            }
            val frames = ArrayList<ByteArray>(1024)
            val deltas = IntList()
            val socket = ByteArray(4 shl 20)
            val from = LongArray(consumers.size) { 1L } // offsets start at 1
            var delivered = 0L
            var logBytes = 0L
            var captureNs = 0L
            val start = System.nanoTime()
            var done = 0
            while (done < total) {
                val c0 = System.nanoTime()
                for (m in msgs) h.capture(m)
                captureNs += System.nanoTime() - c0
                done += msgs.size
                for (c in consumers.indices) {
                    while (true) {
                        val r = if (interest) log.readSparse(c, from[c], 1024, 1 shl 20, frames, deltas)
                        else log.readFor(c, from[c], 1024, 1 shl 20, frames)
                        if (r.span == 0L) break
                        var pos = 0
                        for (f in frames) {
                            System.arraycopy(f, 0, socket, pos, f.size)
                            pos += f.size
                        }
                        logBytes += pos
                        delivered += frames.size
                        from[c] = r.base + r.span
                        log.commit(c, from[c])
                    }
                }
            }
            val sec = (System.nanoTime() - start) / 1e9
            return Result(total / sec, captureNs.toDouble() / total, logBytes / consumers.size, delivered)
        }
    }

    @Test
    fun benchmarkSourceSide() {
        assumeTrue(System.getProperty("peerlink.bench") == "true")
        val total = 2_000_000
        for (n in listOf(1000, 10000)) {
            // Warm up every code path once.
            for (p in listOf(0, 10, 100)) run(n, p, true, total / 4)
            run(n, 100, false, total / 4)

            fun best(percent: Int, interest: Boolean): Result =
                (0 until 3).map { run(n, percent, interest, total) }.maxByOrNull { it.opsPerSec }!!

            val off = best(100, false)
            println("G-IR1 filters=$n off:        %,12.0f pub/s  capture %6.1f ns/op  log %,d B"
                .format(off.opsPerSec, off.nsPerCapture, off.logBytes))
            val res = listOf(0, 10, 100).associateWith { best(it, true) }
            for ((p, r) in res) {
                println("G-IR1 filters=$n interest %3d%%: %,12.0f pub/s  capture %6.1f ns/op  log %,d B  (x%.2f)"
                    .format(p, r.opsPerSec, r.nsPerCapture, r.logBytes, r.opsPerSec / off.opsPerSec))
            }
            assertEquals(0L, res[0]!!.delivered)
            assertEquals(2L * total, res[100]!!.delivered)
        }
    }

    // End to end: broker-a captures and serves broker-b and broker-c, which pull over loopback TCP and
    // announce the filters. The producer keeps at most window records unacknowledged, so the measured
    // rate is the publish rate the source sustains while keeping both consumers up to date.
    private inner class Cluster(val n: Int, val interest: Boolean) : AutoCloseable {
        val received = consumers.associateWith { java.util.concurrent.atomic.AtomicLong() }
        val a: PeerLinkManager
        val peers: List<PeerLinkManager>

        init {
            val portA = findFreePort()
            a = manager("broker-a", portA, consumers.map { PeerConfig(nodeID = it, address = "", serve = true) }, null)
            peers = consumers.map { id ->
                manager(id, findFreePort(), listOf(PeerConfig(nodeID = "broker-a", address = "127.0.0.1:$portA", serve = false)),
                    received.getValue(id))
            }
            if (interest) {
                val announced = filters(n).associateWith { InterestClass.VOL }
                for (p in peers) p.tracker!!.setSource("bench", announced)
            }
            a.start()
            peers.forEach { it.start() }
            if (interest) {
                waitFor { consumers.indices.all { c -> a.interest!!.status(c)?.let { it.state == "LIVE" && it.filters == n } == true } }
            }
            // Both pullers are connected once a probe reaches each of them.
            val probe = msg(interesting(0))
            waitFor {
                a.capture(probe)
                Thread.sleep(5)
                received.values.all { it.get() > 0 }
            }
            waitFor { consumers.indices.all { c -> a.log.committed(c) >= a.log.getLEO() } }
            received.values.forEach { it.set(0) }
        }

        private fun manager(id: String, port: Int, peers: List<PeerConfig>, counter: java.util.concurrent.atomic.AtomicLong?): PeerLinkManager {
            val messageHandler = org.mockito.Mockito.mock(at.rocworks.handlers.MessageHandler::class.java,
                org.mockito.Mockito.withSettings().stubOnly())
            org.mockito.Mockito.`when`(messageHandler.getRetainedStore()).thenReturn(TestMessageStore())
            // stubOnly: a mock that records its invocations would keep every delivered message.
            val sessionHandler = org.mockito.Mockito.mock(at.rocworks.handlers.SessionHandler::class.java,
                org.mockito.Mockito.withSettings().stubOnly().defaultAnswer { inv ->
                    when (inv.method.name) {
                        "getMessageHandler" -> messageHandler
                        "peerLinkInterestClass" -> InterestClass.VOL
                        "publishMessage" -> { counter?.incrementAndGet(); null }
                        else -> org.mockito.Mockito.RETURNS_DEFAULTS.answer(inv)
                    }
                })
            val cfg = at.rocworks.peerlink.config.PeerLinkConfig(
                enabled = true,
                allowUnauthenticatedPeers = true,
                listener = at.rocworks.peerlink.config.PeerLinkListenerConfig(
                    address = "127.0.0.1", port = port, allowedNetworks = listOf("127.0.0.1/32"), allowPlaintext = true),
                peers = peers,
                interest = at.rocworks.peerlink.config.PeerLinkInterestConfig(enabled = interest, unknown = "NONE")
            )
            val env = at.rocworks.peerlink.config.PeerLinkEnv(nodeID = id,
                nodeIDOrigin = at.rocworks.peerlink.config.NodeIdOrigin.CONFIG, hostname = id)
            return PeerLinkManager(cfg, at.rocworks.peerlink.config.validatePeerLink(cfg, env), sessionHandler,
                messageHandler, TestMessageBus())
        }

        // Publishes total messages and returns publishes per second once both consumers have them all.
        fun run(percent: Int, total: Int, window: Long = 20_000): Double {
            received.values.forEach { it.set(0) }
            val msgs = Array(1000) { i -> msg(if (i % 100 < percent) interesting(i * 7 % n) else boring(i * 7 % n)) }
            val expected = total.toLong() / msgs.size * msgs.count { if (interest) it.topicName.endsWith("temp") || it.topicName.endsWith("value") else true }
            val log = a.log
            val start = System.nanoTime()
            var done = 0
            while (done < total) {
                for (i in msgs.indices) a.capture(msgs[i])
                done += msgs.size
                while (log.getLEO() - minOf(log.committed(0), log.committed(1)) > window) Thread.onSpinWait()
            }
            waitFor { received.values.all { it.get() >= expected } }
            val sec = (System.nanoTime() - start) / 1e9
            received.values.forEach { assertEquals(expected, it.get()) }
            return total / sec
        }

        override fun close() {
            peers.forEach { it.stop() }
            a.stop()
        }
    }

    private fun findFreePort(): Int = java.net.ServerSocket(0).use { it.reuseAddress = true; it.localPort }

    private fun waitFor(timeoutMs: Long = 30_000, cond: () -> Boolean) {
        val end = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < end) {
            if (cond()) return
            Thread.sleep(2)
        }
        fail("timeout")
    }

    @Test
    fun benchmarkGateIR1() {
        assumeTrue(System.getProperty("peerlink.bench") == "true")
        val total = Integer.getInteger("peerlink.bench.total", 1_000_000)
        for (n in listOf(1000, 10000)) {
            // Both clusters run side by side and their rounds alternate, so drift of the machine (JIT,
            // GC, other load) affects interest off and on alike; the best round of each counts.
            val (off, res) = Cluster(n, false).use { offCluster ->
                Cluster(n, true).use { onCluster ->
                    offCluster.run(100, total / 4)
                    listOf(0, 10, 100).forEach { onCluster.run(it, total / 4) }
                    var off = 0.0
                    val res = mutableMapOf(0 to 0.0, 10 to 0.0, 100 to 0.0)
                    repeat(3) {
                        off = maxOf(off, offCluster.run(100, total))
                        for (p in res.keys) res[p] = maxOf(res.getValue(p), onCluster.run(p, total))
                    }
                    off to res
                }
            }
            println("G-IR1 e2e filters=$n off:           %,10.0f pub/s".format(off))
            for ((p, r) in res) {
                println("G-IR1 e2e filters=$n interest %3d%%: %,10.0f pub/s  (x%.2f)".format(p, r, r / off))
            }
            assertTrue("100 %% interest regresses by more than 5 %% ($n filters)", res[100]!! >= off * 0.95)
            assertTrue("10 %% interest is less than 3 times faster ($n filters)", res[10]!! >= off * 3.0)
        }
    }
}
