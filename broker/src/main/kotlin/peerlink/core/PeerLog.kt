package at.rocworks.peerlink.core

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.SecureRandom
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.locks.ReentrantLock

// Ported from edge/internal/peerlink/log.go

const val logChunkShift = 10
const val logChunkSlots = 1 shl logChunkShift // 1024
const val logChunkMask = logChunkSlots - 1
const val logChunkBytes = 16384L // Footprint of attached chunk (slots + cum)
const val logReadMaxChunks = 16
const val logSpareChunks = 4

const val logDefaultMaxMessages = 2_000_000L
const val logDefaultMaxBytes = 256L shl 20 // 256 MiB
const val logDefaultMaxRecordBytes = (1 shl 20) + (64 shl 10) // 1 MiB + 64 KiB

// Accounted footprint of a chunk's consumer mask array (interest routing only).
const val logMasksBytes = logChunkSlots * 8L
const val logDefaultMaxScan = 65536
const val logMinMaxScan = 1024

class LogOffsetOutOfRangeException : Exception("peerlink: log offset out of range")
class LogCommitBeyondEndException : Exception("peerlink: commit beyond log end")
class LogUnknownConsumerException : Exception("peerlink: unknown log consumer")
class LogTooManyConsumersException : Exception("peerlink: interest routing supports at most 64 consumers")

enum class LogKind {
    Client,
    Inline,
    Will;

    override fun toString(): String = when (this) {
        Client -> "client"
        Inline -> "inline"
        Will -> "will"
    }
}

enum class LogConsumerState {
    NeverConnected,
    Connected,
    Disconnected;

    override fun toString(): String = when (this) {
        NeverConnected -> "NEVER_CONNECTED"
        Connected -> "CONNECTED"
        Disconnected -> "DISCONNECTED"
    }
}

data class LogConfig(
    var maxMessages: Long = logDefaultMaxMessages,
    var maxBytes: Long = logDefaultMaxBytes,
    var maxRecordBytes: Int = logDefaultMaxRecordBytes,
    var consumers: List<String> = emptyList(),
    // Per-record consumer masks (interest routing, plan-peerlink-interest-routing 6.3); at most 64 consumers.
    var masked: Boolean = false,
    // Bounds the offsets one sparse read or one lagging advance scans (MaxScanPerFetch).
    var maxScan: Int = logDefaultMaxScan
)

class LogChunk {
    val slots = arrayOfNulls<ByteArray>(logChunkSlots)
    val cum = LongArray(logChunkSlots)
}

class LogConsumer(
    val nodeID: String,
    var committed: Long = 1L,
    var served: Long = 1L,
    var acctNext: Long = 1L,
    var lostTotal: Long = 0L,
    var state: LogConsumerState = LogConsumerState.NeverConnected,
    var reading: Int = 0
)

class LogWaiter {
    var wakeAt: Long = 0L
    var idx: Int = -1
    private val latch = AtomicReference(CountDownLatch(1))

    fun reset() {
        latch.set(CountDownLatch(1))
    }

    fun notifyWake() {
        latch.get().countDown()
    }

    fun await(timeoutMs: Long): Boolean {
        if (timeoutMs <= 0) {
            latch.get().await()
            return true
        }
        return latch.get().await(timeoutMs, TimeUnit.MILLISECONDS)
    }
}

data class LogReadResult(
    var base: Long = 0L,
    var count: Int = 0,
    var bytes: Int = 0,
    var lost: Long = 0L,
    var truncated: Boolean = false,
    // Offsets covered: base..base+span-1; equals count unless records were skipped.
    var span: Long = 0L,
    // Records without the consumer's bit were skipped; the deltas list holds the offsets.
    var sparse: Boolean = false,
    var lso: Long = 0L,
    var leo: Long = 0L
)

data class LogConsumerStats(
    val nodeID: String,
    val state: LogConsumerState,
    val committed: Long,
    val served: Long,
    val lag: Long,
    val lostTotal: Long
)

data class LogResume(
    var resumeAt: Long = 0L,
    var lso: Long = 0L,
    var leo: Long = 0L,
    var committed: Long = 0L,
    var lostOnResume: Long = 0L,
    var consumerStateUsed: Boolean = false,
    var sourceReset: Boolean = false
)

data class LogStats(
    val epoch: Long,
    val lso: Long,
    val leo: Long,
    val lwm: Long,
    val records: Long,
    val bytes: Long,
    val maxBytes: Long,
    val maxMessages: Long,
    val chunks: Int,
    val appendedClient: Long,
    val appendedInline: Long,
    val appendedWill: Long,
    val appendedBytes: Long,
    val trimmed: Long,
    val evictedUnread: Long,
    val evictedByCount: Long,
    val evictedByBytes: Long,
    var captureDroppedSize: Long = 0L,
    var captureDroppedInvalid: Long = 0L,
    val spareMisses: Long,
    var uncapturedAtShutdown: Long = 0L,
    val sealed: Boolean
)

data class LogDrainResult(
    val target: Long,
    var finalLEO: Long = 0L,
    var complete: Boolean = false,
    var waitedMs: Long = 0L,
    var unserved: LongArray = LongArray(0)
)

// The source log (log.go)
class PeerLog(cfg: LogConfig) : AutoCloseable {
    private val lock = ReentrantLock()
    private val commitCondition = lock.newCondition()

    val epoch: Long
    private val startMonoNs: Long = System.nanoTime()

    private val chunks = ArrayList<LogChunk>()
    // Per-record consumer masks parallel to chunks when masked. Written before the slot is published and
    // changed later only by expiry sweeps.
    private val masks = ArrayList<java.util.concurrent.atomic.AtomicLongArray>()
    private var firstBase: Long = 0L
    private val spares = Array(logSpareChunks) { AtomicReference<LogChunk?>(null) }
    private var lso: Long = 1L
    private var leo: Long = 1L
    private var lwm: Long = 1L
    private var bytes: Long = 0L
    private var total: Long = 0L
    private val consumers: Array<LogConsumer>
    private val waiters = ArrayList<LogWaiter>()
    private var minWakeAt: Long = Long.MAX_VALUE
    private var sealed: Boolean = false
    private var closed: Boolean = false

    var maxMessages: Long = cfg.maxMessages
    var maxBytes: Long = cfg.maxBytes
    var maxRecordBytes: Int = cfg.maxRecordBytes
    val masked: Boolean = cfg.masked
    private val maxScan: Int = if (cfg.maxScan <= 0) logDefaultMaxScan else cfg.maxScan
    private val chunkBytes: Long = if (cfg.masked) logChunkBytes + logMasksBytes else logChunkBytes
    // The mask with a bit for every consumer.
    val allMask: Long = if (cfg.consumers.size >= 64) -1L else (1L shl cfg.consumers.size) - 1

    private var appendedClient: Long = 0L
    private var appendedInline: Long = 0L
    private var appendedWill: Long = 0L
    private var trimmed: Long = 0L
    private var evictedUnread: Long = 0L
    private var evictedByCount: Long = 0L
    private var evictedByBytes: Long = 0L
    private var spareMisses: Long = 0L

    private val droppedSize = LongAdder()
    private val droppedInv = LongAdder()
    private val uncaptured = LongAdder()
    private val sealedFlag = AtomicBoolean(false)

    private val refillTrigger = java.util.concurrent.ArrayBlockingQueue<Unit>(1)
    private val refillStopped = CountDownLatch(1)
    private val refillRunning = AtomicBoolean(true)

    init {
        val rnd = SecureRandom()
        var ep = 0L
        while (ep == 0L) {
            ep = rnd.nextLong()
        }
        epoch = ep

        if (maxMessages == 0L) maxMessages = logDefaultMaxMessages
        if (maxBytes == 0L) maxBytes = logDefaultMaxBytes
        if (maxRecordBytes <= 0) maxRecordBytes = logDefaultMaxRecordBytes
        val q = maxBytes / 4
        if (maxRecordBytes.toLong() > q) {
            maxRecordBytes = minOf(q, Int.MAX_VALUE.toLong()).toInt()
        }

        if (cfg.masked && cfg.consumers.size > 64) throw LogTooManyConsumersException()
        consumers = Array(cfg.consumers.size) { i ->
            LogConsumer(cfg.consumers[i], committed = 1L, served = 1L, acctNext = 1L)
        }

        fillSpares()
        Thread.ofVirtual().name("peerlog-refill").start {
            try {
                while (refillRunning.get()) {
                    val unit = refillTrigger.poll(100, TimeUnit.MILLISECONDS)
                    if (unit != null) {
                        fillSpares()
                    }
                }
            } finally {
                refillStopped.countDown()
            }
        }
    }

    private fun fillSpares() {
        for (i in 0 until logSpareChunks) {
            if (spares[i].get() == null) {
                spares[i].compareAndSet(null, LogChunk())
            }
        }
    }

    override fun close() {
        if (refillRunning.compareAndSet(true, false)) {
            refillTrigger.offer(Unit)
            refillStopped.await(2, TimeUnit.SECONDS)
            lock.lock()
            try {
                closed = true
                for (w in waiters) {
                    w.idx = -1
                    w.notifyWake()
                }
                waiters.clear()
                minWakeAt = Long.MAX_VALUE
            } finally {
                lock.unlock()
            }
        }
    }

    fun startMonoNs(): Long = startMonoNs

    fun monoMs(nanoTime: Long = System.nanoTime()): Long {
        val d = nanoTime - startMonoNs
        return if (d < 0) 0L else d / 1_000_000L
    }

    fun checkRecordSize(size: Int): Boolean {
        if (size > maxRecordBytes) {
            droppedSize.increment()
            return false
        }
        return true
    }

    fun countCaptureInvalid() { droppedInv.increment() }
    fun countUncapturedAtShutdown() { uncaptured.increment() }
    fun isSealed(): Boolean = sealedFlag.get()

    fun seal(): Long {
        lock.lock()
        try {
            sealed = true
            sealedFlag.set(true)
            return leo
        } finally {
            lock.unlock()
        }
    }

    fun append(frame: ByteArray, kind: LogKind = LogKind.Client): Pair<Long, Boolean> =
        appendMask(frame, kind, allMask)

    // appendMask stores frame with a bit per consumer index that needs the record; mask is ignored when
    // the log is not masked.
    fun appendMask(frame: ByteArray, kind: LogKind, mask: Long): Pair<Long, Boolean> {
        val n = frame.size
        if (n < 4) {
            droppedInv.increment()
            return Pair(0L, false)
        }
        val recLen = (frame[0].toLong() and 0xFFL) or
                ((frame[1].toLong() and 0xFFL) shl 8) or
                ((frame[2].toLong() and 0xFFL) shl 16) or
                ((frame[3].toLong() and 0xFFL) shl 24)
        if (recLen + 4 != n.toLong()) {
            droppedInv.increment()
            return Pair(0L, false)
        }
        if (n > maxRecordBytes) {
            droppedSize.increment()
            return Pair(0L, false)
        }
        val acc = logFrameAccounted(n)

        var needRefill = false
        val off: Long
        var tck: LogChunk? = null
        var ta = 0L
        var tb = 0L
        lock.lock()
        try {
            if (sealed) {
                uncaptured.increment()
                return Pair(0L, false)
            }
            off = leo
            when (kind) {
                LogKind.Client -> appendedClient++
                LogKind.Inline -> appendedInline++
                LogKind.Will -> appendedWill++
            }
            if (consumers.isEmpty()) {
                leo = off + 1
                lso = leo
                lwm = leo
                trimmed++
                return Pair(off, true)
            }
            if (chunks.isEmpty()) {
                firstBase = off
            }
            val rel = off - firstBase
            val ci = (rel shr logChunkShift).toInt()
            if (ci == chunks.size) {
                attachChunkLocked()
                needRefill = true
            }
            val ck = chunks[ci]
            val slot = (rel and logChunkMask.toLong()).toInt()
            ck.cum[slot] = total
            if (masked) masks[ci].set(slot, mask)
            ck.slots[slot] = frame
            total += acc
            bytes += acc
            leo = off + 1
            if (masked && (mask and allMask) != allMask) {
                val t = skipCaughtUpLocked(off, mask)
                tck = t.first; ta = t.second; tb = t.third
            }
            if (leo - lso > maxMessages || bytes > maxBytes) {
                evictLocked(off)
            }
            if (leo >= minWakeAt) {
                wakeDueLocked()
            }
        } finally {
            lock.unlock()
        }
        clearLogSlots(tck, ta, tb)
        if (needRefill) {
            refillTrigger.offer(Unit)
        }
        return Pair(off, true)
    }

    private fun attachChunkLocked() {
        var ck: LogChunk? = null
        for (i in 0 until logSpareChunks) {
            ck = spares[i].getAndSet(null)
            if (ck != null) break
        }
        if (ck == null) {
            ck = LogChunk()
            spareMisses++
        }
        chunks.add(ck)
        if (masked) masks.add(java.util.concurrent.atomic.AtomicLongArray(logChunkSlots))
        bytes += chunkBytes
    }

    // skipCaughtUpLocked advances C[c] over the record at off for every caught-up consumer whose bit is not
    // in mask (6.5 rule 1), so records a consumer never needs do not pin the log.
    private fun skipCaughtUpLocked(off: Long, mask: Long): Triple<LogChunk?, Long, Long> {
        var moved = false
        for (i in consumers.indices) {
            val con = consumers[i]
            if (con.committed == off && (mask and (1L shl i)) == 0L) {
                con.committed = off + 1
                moved = true
            }
        }
        if (!moved) return Triple(null, 0L, 0L)
        return updateLWMLocked()
    }

    // maskAtLocked returns the consumer mask of the record at x, lso <= x < leo, of a masked log.
    private fun maskAtLocked(x: Long): Long {
        val rel = x - firstBase
        return masks[(rel shr logChunkShift).toInt()].get((rel and logChunkMask.toLong()).toInt())
    }

    // skipLaggingLocked advances C[c] over the leading run of records without bit c, scanning at most
    // maxScan offsets (6.5 rule 2).
    private fun skipLaggingLocked(c: Int): Boolean {
        val con = consumers[c]
        var x = maxOf(con.committed, lso)
        if (x >= leo) return false
        val bit = 1L shl c
        val end = minOf(leo, x + maxScan)
        val start = x
        while (x < end && (maskAtLocked(x) and bit) == 0L) x++
        if (x == start || x <= con.committed) return false
        con.committed = x
        return true
    }

    // advanceSkipped applies the lagging advance (6.5 rule 2) to every consumer. The manager calls it
    // every 100 ms; it is a no-op on an unmasked log.
    fun advanceSkipped() {
        if (!masked) return
        var t: Triple<LogChunk?, Long, Long> = Triple(null, 0L, 0L)
        lock.lock()
        try {
            var moved = false
            for (c in consumers.indices) {
                if (skipLaggingLocked(c)) moved = true
            }
            if (moved) {
                t = updateLWMLocked()
                commitCondition.signalAll()
            }
        } finally {
            lock.unlock()
        }
        clearLogSlots(t.first, t.second, t.third)
    }

    // clearConsumerBits is the persistent-expiry sweep (6.5): starting at from (raised to max(C[c], lso)),
    // it scans at most maxScan offsets and clears bit c of every record for which drop returns true. drop
    // is called under the log lock and must not call into the log. It returns the offset after the last
    // scanned one (leo when done) and the number of bits cleared.
    fun clearConsumerBits(c: Int, from: Long, drop: (ByteArray) -> Boolean): Pair<Long, Long> {
        if (!masked || c < 0 || c >= consumers.size) return Pair(0L, 0L)
        val bit = 1L shl c
        var cleared = 0L
        var t: Triple<LogChunk?, Long, Long> = Triple(null, 0L, 0L)
        var x: Long
        lock.lock()
        try {
            x = maxOf(from, consumers[c].committed, lso)
            val end = minOf(leo, x + maxScan)
            while (x < end) {
                val rel = x - firstBase
                val ci = (rel shr logChunkShift).toInt()
                val slot = (rel and logChunkMask.toLong()).toInt()
                val m = masks[ci].get(slot)
                if ((m and bit) != 0L) {
                    val frame = chunks[ci].slots[slot]
                    if (frame != null && drop(frame)) {
                        masks[ci].set(slot, m and bit.inv())
                        cleared++
                    }
                }
                x++
            }
            if (cleared > 0 && skipLaggingLocked(c)) {
                t = updateLWMLocked()
                commitCondition.signalAll()
            }
        } finally {
            lock.unlock()
        }
        clearLogSlots(t.first, t.second, t.third)
        return Pair(x, cleared)
    }

    private fun detachLocked() {
        var k = 0
        while (k < chunks.size && lso - firstBase >= logChunkSlots.toLong()) {
            firstBase += logChunkSlots
            k++
        }
        if (k > 0) {
            for (i in 0 until k) {
                chunks.removeAt(0)
                if (masked) masks.removeAt(0)
            }
            bytes -= k.toLong() * chunkBytes
        }
    }

    private fun cumAtLocked(x: Long): Long {
        if (x == leo) return total
        val rel = x - firstBase
        val ci = (rel shr logChunkShift).toInt()
        val slot = (rel and logChunkMask.toLong()).toInt()
        return chunks[ci].cum[slot]
    }

    private fun evictLocked(keep: Long) {
        while (lso < keep) {
            val byCount = leo - lso > maxMessages
            if (!byCount && bytes <= maxBytes) return

            val off = lso
            val rel = off - firstBase
            val ci = (rel shr logChunkShift).toInt()
            val slot = (rel and logChunkMask.toLong()).toInt()
            val ck = chunks[ci]
            bytes -= cumAtLocked(off + 1) - ck.cum[slot]
            ck.slots[slot] = null
            if (byCount) evictedByCount++ else evictedByBytes++
            if (off >= lwm) evictedUnread++
            if (masked) accountEvictLocked(off, maskAtLocked(off))
            lso = off + 1
            if (lso - firstBase >= logChunkSlots.toLong()) {
                detachLocked()
            }
        }
    }

    // accountEvictLocked charges the evicted record at off as lost only to the consumers that needed it.
    // On a masked log loss is accounted at eviction, where the record's mask is still known; observeLocked
    // then finds nothing left to charge. A consumer with a read in progress is left to observeLocked.
    private fun accountEvictLocked(off: Long, mask: Long) {
        for (i in consumers.indices) {
            val con = consumers[i]
            if (con.reading > 0 || off < maxOf(con.acctNext, con.committed, con.served)) continue
            if ((mask and (1L shl i)) != 0L) con.lostTotal++
            con.acctNext = off + 1
        }
    }

    private fun wakeDueLocked() {
        var minWake = Long.MAX_VALUE
        var i = 0
        while (i < waiters.size) {
            val w = waiters[i]
            if (w.wakeAt > leo) {
                minWake = minOf(minWake, w.wakeAt)
                i++
                continue
            }
            w.notifyWake()
            removeWaiterLocked(i)
        }
        minWakeAt = minWake
    }

    private fun removeWaiterLocked(i: Int) {
        val w = waiters[i]
        val last = waiters.size - 1
        if (i != last) {
            val lastW = waiters[last]
            waiters[i] = lastW
            lastW.idx = i
        }
        waiters.removeAt(last)
        w.idx = -1
    }

    fun wait(w: LogWaiter, wakeAt: Long): Boolean {
        lock.lock()
        try {
            if (closed || leo >= wakeAt) return false
            w.reset()
            w.wakeAt = wakeAt
            if (w.idx < 0) {
                w.idx = waiters.size
                waiters.add(w)
            }
            minWakeAt = minOf(minWakeAt, wakeAt)
            return true
        } finally {
            lock.unlock()
        }
    }

    fun unwait(w: LogWaiter) {
        lock.lock()
        try {
            if (w.idx >= 0) {
                removeWaiterLocked(w.idx)
                minWakeAt = Long.MAX_VALUE
                for (o in waiters) {
                    minWakeAt = minOf(minWakeAt, o.wakeAt)
                }
            }
        } finally {
            lock.unlock()
        }
    }

    fun waitFor(w: LogWaiter, wakeAt: Long, maxWaitMs: Long): Boolean {
        if (wait(w, wakeAt)) {
            w.await(maxWaitMs)
            unwait(w)
        }
        return getLEO() >= wakeAt
    }

    fun read(from: Long, maxRecords: Int, maxBytes: Int, out: MutableList<ByteArray>): LogReadResult {
        return readRetry(-1, from, maxRecords, maxBytes, out)
    }

    fun readFor(c: Int, from: Long, maxRecords: Int, maxBytes: Int, out: MutableList<ByteArray>): LogReadResult {
        if (c < 0 || c >= consumers.size) {
            out.clear()
            throw LogUnknownConsumerException()
        }
        return readRetry(c, from, maxRecords, maxBytes, out)
    }

    // readSparse is readFor that skips the records without consumer c's bit (6.4). The offsets of the
    // returned frames relative to base go to deltas (cleared first) when the result is sparse. It scans
    // at most maxScan offsets. On an unmasked log it is readFor.
    fun readSparse(c: Int, from: Long, maxRecords: Int, maxBytes: Int, out: MutableList<ByteArray>, deltas: IntList): LogReadResult {
        deltas.clear()
        if (c < 0 || c >= consumers.size) {
            out.clear()
            throw LogUnknownConsumerException()
        }
        return readRetry(c, from, maxRecords, maxBytes, out, if (masked) deltas else null)
    }

    private fun readRetry(c: Int, from: Long, maxRecords: Int, maxBytes: Int, out: MutableList<ByteArray>, deltas: IntList? = null): LogReadResult {
        var attempt = 0
        while (true) {
            val res = if (deltas != null) readSparseInternal(c, from, maxRecords, maxBytes, out, deltas)
            else readInternal(c, from, maxRecords, maxBytes, out)
            if (res.span > 0 || !res.truncated || attempt == 2) {
                return res
            }
            attempt++
        }
    }

    private fun readInternal(c: Int, from: Long, maxRecords: Int, maxBytes: Int, out: MutableList<ByteArray>): LogReadResult {
        val cks = arrayOfNulls<LogChunk>(logReadMaxChunks)
        out.clear()

        val snapLso: Long
        val snapLeo: Long
        val base: Long
        var n: Long
        val fb: Long
        var c0 = 0L

        lock.lock()
        try {
            snapLso = lso
            snapLeo = leo
            if (from == 0L || from > snapLeo) {
                return LogReadResult(base = from, lso = snapLso, leo = snapLeo)
            }
            if (c >= 0) {
                val con = consumers[c]
                observeLocked(con)
                con.reading++
            }
            base = maxOf(from, snapLso)
            n = snapLeo - base
            if (maxRecords <= 0) {
                n = 0
            } else if (n > maxRecords.toLong()) {
                n = maxRecords.toLong()
            }
            fb = firstBase
            if (n > 0) {
                c0 = (base - fb) shr logChunkShift
                var cLast = (base + n - 1 - fb) shr logChunkShift
                if (cLast - c0 >= logReadMaxChunks.toLong()) {
                    cLast = c0 + logReadMaxChunks - 1
                    n = fb + ((cLast + 1) shl logChunkShift) - base
                }
                for (ci in 0 until (cLast - c0 + 1).toInt()) {
                    val idx = (c0 + ci).toInt()
                    if (idx < chunks.size) {
                        cks[ci] = chunks[idx]
                    }
                }
            }
        } finally {
            lock.unlock()
        }

        val res = LogReadResult(base = base, lost = base - from, lso = snapLso, leo = snapLeo)
        var totalBytes = 0
        for (i in 0 until n) {
            val rel = base + i - fb
            val ckIdx = ((rel shr logChunkShift) - c0).toInt()
            val slot = (rel and logChunkMask.toLong()).toInt()
            val chunk = if (ckIdx in 0 until logReadMaxChunks) cks[ckIdx] else null
            val frame = chunk?.slots?.get(slot)
            if (frame == null) {
                res.truncated = true
                break
            }
            val size = frame.size
            if (maxBytes > 0 && out.isNotEmpty() && totalBytes + size > maxBytes) {
                break
            }
            out.add(frame)
            totalBytes += size
        }
        res.count = out.size
        res.span = res.count.toLong()
        res.bytes = totalBytes

        if (c >= 0) {
            lock.lock()
            try {
                val con = consumers[c]
                con.served = maxOf(con.served, base + res.count.toLong())
                con.reading--
            } finally {
                lock.unlock()
            }
        }
        return res
    }

    private fun readSparseInternal(c: Int, from: Long, maxRecords: Int, maxBytes: Int, out: MutableList<ByteArray>, deltas: IntList): LogReadResult {
        val cks = arrayOfNulls<LogChunk>(logReadMaxChunks)
        val mks = arrayOfNulls<java.util.concurrent.atomic.AtomicLongArray>(logReadMaxChunks)
        out.clear()
        deltas.clear()

        val snapLso: Long
        val snapLeo: Long
        val base: Long
        var n: Long
        val fb: Long
        var c0 = 0L
        lock.lock()
        try {
            snapLso = lso
            snapLeo = leo
            if (from == 0L || from > snapLeo) {
                return LogReadResult(base = from, lso = snapLso, leo = snapLeo)
            }
            val con = consumers[c]
            observeLocked(con)
            con.reading++
            base = maxOf(from, snapLso)
            n = minOf(snapLeo - base, maxScan.toLong())
            if (maxRecords <= 0) n = 0
            fb = firstBase
            if (n > 0) {
                c0 = (base - fb) shr logChunkShift
                var cLast = (base + n - 1 - fb) shr logChunkShift
                if (cLast - c0 >= logReadMaxChunks.toLong()) {
                    cLast = c0 + logReadMaxChunks - 1
                    n = fb + ((cLast + 1) shl logChunkShift) - base
                }
                for (k in 0 until (cLast - c0 + 1).toInt()) {
                    val idx = (c0 + k).toInt()
                    if (idx < chunks.size) {
                        cks[k] = chunks[idx]
                        mks[k] = masks[idx]
                    }
                }
            }
        } finally {
            lock.unlock()
        }

        val res = LogReadResult(base = base, lost = base - from, lso = snapLso, leo = snapLeo)
        val bit = 1L shl c
        var totalBytes = 0
        var skipped = false
        var i = 0L
        while (i < n) {
            val rel = base + i - fb
            val k = ((rel shr logChunkShift) - c0).toInt()
            val slot = (rel and logChunkMask.toLong()).toInt()
            val frame = if (k in 0 until logReadMaxChunks) cks[k]?.slots?.get(slot) else null
            if (frame == null) {
                res.truncated = true
                break
            }
            if ((mks[k]!!.get(slot) and bit) == 0L) {
                // The span table (4 bytes per record plus span) counts against maxBytes once the batch is sparse.
                if (!skipped && maxBytes > 0 && out.isNotEmpty() && totalBytes + 4 + 4 * out.size > maxBytes) break
                skipped = true
                i++
                continue
            }
            val size = frame.size
            val cost = if (skipped) size + 4 else size
            val owed = if (skipped) 4 + 4 * out.size else 0
            if (maxBytes > 0 && out.isNotEmpty() && totalBytes + cost + owed > maxBytes) break
            out.add(frame)
            deltas.add(i.toInt())
            totalBytes += size
            i++
            if (out.size >= maxRecords) break
        }
        res.span = i
        res.count = out.size
        res.bytes = totalBytes
        res.sparse = res.count.toLong() < res.span
        if (!res.sparse) deltas.clear()

        lock.lock()
        try {
            val con = consumers[c]
            con.served = maxOf(con.served, base + res.span)
            con.reading--
        } finally {
            lock.unlock()
        }
        return res
    }

    fun commit(c: Int, off: Long) {
        if (c < 0 || c >= consumers.size) throw LogUnknownConsumerException()
        var ck: LogChunk? = null
        var a = 0L
        var b = 0L
        lock.lock()
        try {
            if (off > leo) throw LogCommitBeyondEndException()
            val con = consumers[c]
            if (off <= con.committed) return
            con.committed = off
            if (masked) skipLaggingLocked(c)
            val triple = updateLWMLocked()
            ck = triple.first
            a = triple.second
            b = triple.third
            commitCondition.signalAll()
        } finally {
            lock.unlock()
        }
        clearLogSlots(ck, a, b)
    }

    private fun updateLWMLocked(): Triple<LogChunk?, Long, Long> {
        var minCommitted = Long.MAX_VALUE
        for (con in consumers) {
            minCommitted = minOf(minCommitted, con.committed)
        }
        if (minCommitted == lwm) return Triple(null, 0L, 0L)
        lwm = minCommitted
        if (lwm <= lso) return Triple(null, 0L, 0L)

        val from = lso
        bytes -= cumAtLocked(lwm) - cumAtLocked(from)
        trimmed += lwm - from
        lso = lwm
        detachLocked()
        if (chunks.isEmpty() || firstBase >= lwm) return Triple(null, 0L, 0L)
        var a = 0L
        if (from > firstBase) a = from - firstBase
        return Triple(chunks[0], a, lwm - firstBase)
    }

    private fun clearLogSlots(ck: LogChunk?, a: Long, b: Long) {
        if (ck == null) return
        for (i in a until b) {
            ck.slots[i.toInt()] = null
        }
    }

    fun markServed(c: Int, upTo: Long) {
        if (c < 0 || c >= consumers.size) return
        lock.lock()
        try {
            val con = consumers[c]
            con.served = maxOf(con.served, minOf(upTo, leo))
        } finally {
            lock.unlock()
        }
    }

    fun setConsumerState(c: Int, st: LogConsumerState) {
        if (c < 0 || c >= consumers.size) return
        lock.lock()
        try {
            consumers[c].state = st
        } finally {
            lock.unlock()
        }
    }

    fun consumerIndex(nodeID: String): Int {
        for (i in consumers.indices) {
            if (consumers[i].nodeID == nodeID) return i
        }
        return -1
    }

    fun numConsumers(): Int = consumers.size

    fun committed(c: Int): Long {
        if (c < 0 || c >= consumers.size) return 0L
        lock.lock()
        try {
            return consumers[c].committed
        } finally {
            lock.unlock()
        }
    }

    fun bounds(): Pair<Long, Long> {
        lock.lock()
        try {
            return Pair(lso, leo)
        } finally {
            lock.unlock()
        }
    }

    fun getLEO(): Long {
        lock.lock()
        try {
            return leo
        } finally {
            lock.unlock()
        }
    }

    private fun observeLocked(con: LogConsumer) {
        if (con.reading > 0) return
        var a = maxOf(con.acctNext, con.committed, con.served)
        if (lso > a) {
            con.lostTotal += lso - a
            a = lso
        }
        con.acctNext = a
    }

    fun observeConsumer(c: Int): LogConsumerStats {
        if (c < 0 || c >= consumers.size) return LogConsumerStats("", LogConsumerState.NeverConnected, 0, 0, 0, 0)
        lock.lock()
        try {
            val con = consumers[c]
            observeLocked(con)
            return LogConsumerStats(
                nodeID = con.nodeID,
                state = con.state,
                committed = con.committed,
                served = con.served,
                lag = leo - minOf(con.committed, leo),
                lostTotal = con.lostTotal
            )
        } finally {
            lock.unlock()
        }
    }

    fun consumerStats(): List<LogConsumerStats> {
        lock.lock()
        try {
            return consumers.indices.map { observeConsumer(it) }
        } finally {
            lock.unlock()
        }
    }

    fun consumerStats(c: Int): LogConsumerStats {
        lock.lock()
        try {
            return observeConsumer(c)
        } finally {
            lock.unlock()
        }
    }

    fun resume(c: Int, lastEpoch: Long, resumeOffset: Long): LogResume {
        if (c < 0 || c >= consumers.size) throw LogUnknownConsumerException()
        val effResumeOffset = if (resumeOffset == 0L) 1L else resumeOffset
        var ck: LogChunk? = null
        var a = 0L
        var b = 0L
        lock.lock()
        try {
            val con = consumers[c]
            val r = LogResume(lso = lso, leo = leo)
            when {
                lastEpoch == epoch && effResumeOffset > leo -> throw LogOffsetOutOfRangeException()
                lastEpoch == epoch -> {
                    if (effResumeOffset > con.committed) {
                        con.committed = effResumeOffset
                        val triple = updateLWMLocked()
                        ck = triple.first
                        a = triple.second
                        b = triple.third
                    }
                    if (effResumeOffset >= lso) {
                        r.resumeAt = effResumeOffset
                        r.consumerStateUsed = true
                    } else {
                        r.resumeAt = lso
                        r.lostOnResume = lso - effResumeOffset
                    }
                }
                else -> {
                    r.sourceReset = lastEpoch != 0L
                    r.resumeAt = maxOf(con.committed, lso)
                    if (lso > con.committed) {
                        r.lostOnResume = lso - con.committed
                    }
                }
            }
            observeLocked(con)
            r.lso = lso
            r.committed = con.committed
            return r
        } finally {
            lock.unlock()
            clearLogSlots(ck, a, b)
        }
    }

    fun stats(): LogStats {
        val s: LogStats
        lock.lock()
        try {
            s = LogStats(
                epoch = epoch,
                lso = lso,
                leo = leo,
                lwm = lwm,
                records = leo - lso,
                bytes = bytes,
                maxBytes = maxBytes,
                maxMessages = maxMessages,
                chunks = chunks.size,
                appendedClient = appendedClient,
                appendedInline = appendedInline,
                appendedWill = appendedWill,
                appendedBytes = total,
                trimmed = trimmed,
                evictedUnread = evictedUnread,
                evictedByCount = evictedByCount,
                evictedByBytes = evictedByBytes,
                spareMisses = spareMisses,
                sealed = sealed
            )
        } finally {
            lock.unlock()
        }
        s.captureDroppedSize = droppedSize.sum()
        s.captureDroppedInvalid = droppedInv.sum()
        s.uncapturedAtShutdown = uncaptured.sum()
        return s
    }

    fun drain(timeoutMs: Long, connected: ((Int) -> Boolean)? = null): LogDrainResult {
        val start = System.currentTimeMillis()
        val isConn: (Int) -> Boolean = connected ?: { c ->
            lock.lock()
            try {
                consumers[c].state == LogConsumerState.Connected
            } finally {
                lock.unlock()
            }
        }
        val target = getLEO()
        val res = LogDrainResult(target = target)
        res.complete = drainedTo(target, isConn)
        if (!res.complete && timeoutMs > 0) {
            val deadline = start + timeoutMs
            lock.lock()
            try {
                while (!res.complete) {
                    val remaining = deadline - System.currentTimeMillis()
                    if (remaining <= 0) break
                    commitCondition.await(minOf(remaining, 20L), TimeUnit.MILLISECONDS)
                    res.complete = drainedTo(target, isConn)
                }
            } finally {
                lock.unlock()
            }
        }
        res.finalLEO = seal()
        res.waitedMs = System.currentTimeMillis() - start
        val unservedList = LongArray(consumers.size)
        lock.lock()
        try {
            for (i in consumers.indices) {
                unservedList[i] = res.finalLEO - minOf(consumers[i].committed, res.finalLEO)
            }
        } finally {
            lock.unlock()
        }
        res.unserved = unservedList
        return res
    }

    private fun drainedTo(target: Long, isConn: (Int) -> Boolean): Boolean {
        for (c in consumers.indices) {
            if (isConn(c) && committed(c) < target) return false
        }
        return true
    }
}

// IntList is a growable int array (the sparse delta table) that avoids boxing.
class IntList(capacity: Int = 64) {
    var data = IntArray(capacity)
        private set
    var size = 0
        private set

    fun add(v: Int) {
        if (size == data.size) data = data.copyOf(maxOf(16, data.size * 2))
        data[size++] = v
    }

    operator fun get(i: Int): Int = data[i]
    fun clear() { size = 0 }
    fun toIntArray(): IntArray = data.copyOf(size)
}

// Memory size classes & accounting (log.go: lines 1024-1064)
private val logSizeClasses = intArrayOf(
    8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256, 288, 320, 352,
    384, 416, 448, 480, 512, 576, 640, 704, 768, 896, 1024, 1152, 1280, 1408, 1536, 1792, 2048, 2304,
    2688, 3072, 3200, 3456, 4096, 4864, 5376, 6144, 6528, 6784, 6912, 8192, 9472, 9728, 10240, 10880,
    12288, 13568, 14336, 16384, 18432, 19072, 20480, 21760, 24576, 27264, 28672, 32768
)

private val logSizeBy8 = IntArray(1024 / 8 + 1) { i ->
    findClass(i * 8)
}

private val logSizeBy128 = IntArray((32768 - 1024) / 128 + 1) { i ->
    findClass(1024 + i * 128)
}

private fun findClass(n: Int): Int {
    for (c in logSizeClasses) {
        if (c >= n) return c
    }
    return 0
}


fun logFrameAccounted(n: Int): Long {
    return when {
        n <= 1024 -> logSizeBy8[(n + 7) shr 3].toLong()
        n <= 32768 -> logSizeBy128[(n - 1024 + 127) shr 7].toLong()
        else -> (n.toLong() + 8191L) and 8191L.inv()
    }
}
