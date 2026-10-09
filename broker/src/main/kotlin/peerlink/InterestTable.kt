package at.rocworks.peerlink

import at.rocworks.peerlink.core.MaskNode
import at.rocworks.peerlink.core.PeerLog
import at.rocworks.peerlink.core.TopicFilter
import at.rocworks.peerlink.wire.*
import com.fasterxml.jackson.annotation.JsonInclude
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.StampedLock
import java.util.logging.Logger
import kotlin.concurrent.read
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract
import kotlin.concurrent.write

// Lifecycle state of a peer's interest on this source (plan-peerlink-interest-routing 6, edge 7.1).
enum class InterestState { UNKNOWN, LIVE, DISCONNECTED }

class InterestProtocolException(msg: String) : Exception(msg)

// Interest view of one consumer in the PeerLink status (plan-peerlink-interest-routing 8).
data class InterestStatus(
    val state: String,
    val mode: String,
    val filters: Int,
    val filtersPersistent: Int,
    val snapshotGeneration: Long,
    @get:JsonInclude(JsonInclude.Include.NON_EMPTY) val lastSnapshotAt: String?,
    @get:JsonInclude(JsonInclude.Include.NON_EMPTY) val instanceId: String?
)

// Every change of the trie or the always mask runs under both locks; see InterestTable.
@OptIn(ExperimentalContracts::class)
private inline fun <T> writeLocked(lock: ReentrantReadWriteLock, stamp: StampedLock, action: () -> T): T {
    contract { callsInPlace(action, InvocationKind.EXACTLY_ONCE) }
    return lock.write {
        val w = stamp.writeLock()
        try {
            action()
        } finally {
            stamp.unlockWrite(w)
        }
    }
}

// Remote interest table of a source: per-peer entries and the union trie the capture path matches against
// (plan-peerlink-interest-routing 6). Interest frames, connects, disconnects and expiry take the write
// lock and, inside it, the stamp. Capture matches optimistically against the stamp and takes the read
// lock only when a writer interfered (G-IR1: a read lock costs as much as the trie walk). The table calls the log only from sweep, and the log never calls the
// table, so the lock order is table then log.
class InterestTable(
    consumers: List<Consumer>,
    val unknownAll: Boolean,
    val maxFilters: Int,
    val maxBytes: Int,
    private val logger: Logger
) {
    // One per log consumer, in consumer index order; enabled is false for peers with Interest OFF.
    data class Consumer(val nodeId: String, val enabled: Boolean)

    private class Entry(val cls: Int, val expirySec: Long)

    private class Peer(val idx: Int, val nodeId: String, val enabled: Boolean) {
        val bit: Long = 1L shl idx
        var state = InterestState.UNKNOWN
        var connected = false
        // dense: the last session did not agree CapInterest, so the peer is served everything.
        var dense = false
        var overLimit = false
        var inTrie = false

        var instance = 0L
        var instanceSeen = false
        var disconnectedAtMs = 0L

        var entries = HashMap<String, Entry>()
        var persistent = 0

        var lastGen = -1L
        var snapOpen = false
        var snapGen = 0L
        var snapMarks: HashMap<String, Entry>? = null
        var snapshotGen = 0L
        var lastSnapshotAt: Instant? = null

        var sweeping = false
        var sweepFrom = 0L
        var sweepSeq = 0L
    }

    private val lock = ReentrantReadWriteLock()
    private val stamp = StampedLock()
    private val trie = MaskNode()
    @Volatile private var always = 0L
    private val peers: List<Peer> = consumers.mapIndexed { i, pc -> Peer(i, pc.nodeId, pc.enabled) }

    val skipped = LongAdder()
    val matched = LongAdder()
    val sparseBatches = LongAdder()
    val volatileDropped = AtomicLong()
    val persistentExpired = AtomicLong()
    val backlogDiscarded = AtomicLong()
    val rejected = AtomicLong()
    val overLimitCount = AtomicLong()
    val deltasReceived = AtomicLong()

    init {
        writeLocked(lock, stamp) { for (p in this.peers) applyModeLocked(p) }
    }

    // match returns the consumers that need a non-retained publish on topic.
    fun match(topic: String): Long {
        val s = stamp.tryOptimisticRead()
        if (s != 0L) {
            try {
                val m = matchUnlocked(topic)
                if (stamp.validate(s)) return m
            } catch (_: RuntimeException) {
                // A concurrent writer left the trie inconsistent for this walk; retry under the lock.
            }
        }
        return lock.read { matchUnlocked(topic) }
    }

    private fun matchUnlocked(topic: String): Long {
        var m = always
        if (!trie.isEmpty()) m = m or trie.match(topic)
        return m
    }

    private fun peer(idx: Int): Peer? = peers.getOrNull(idx)

    // Whether p is served by its entries; otherwise its bit is fixed by alwaysLocked.
    private fun filteredLocked(p: Peer): Boolean =
        p.enabled && !p.dense && !p.overLimit && p.state != InterestState.UNKNOWN

    private fun alwaysLocked(p: Peer): Boolean {
        if (!p.enabled || p.dense || p.overLimit) return true
        return p.state == InterestState.UNKNOWN && unknownAll
    }

    // Brings the trie and the always mask in line with p's mode.
    private fun applyModeLocked(p: Peer) {
        val want = filteredLocked(p)
        if (want != p.inTrie) {
            for (f in p.entries.keys) trie.set(f, p.bit, want)
            p.inTrie = want
        }
        always = if (alwaysLocked(p)) always or p.bit else always and p.bit.inv()
    }

    private fun putLocked(p: Peer, f: String, e: Entry) {
        val old = p.entries.put(f, e)
        if (old != null && old.cls == InterestPer) p.persistent--
        if (e.cls == InterestPer) p.persistent++
        if (old == null && p.inTrie) trie.set(f, p.bit, true)
    }

    private fun deleteLocked(p: Peer, f: String): Boolean {
        val old = p.entries.remove(f) ?: return false
        if (old.cls == InterestPer) p.persistent--
        if (p.inTrie) trie.set(f, p.bit, false)
        return true
    }

    private fun clearEntriesLocked(p: Peer) {
        if (p.inTrie) for (f in p.entries.keys) trie.set(f, p.bit, false)
        p.entries.clear()
        p.persistent = 0
    }

    // Checks an entry against section 4; snapshots allow only VOL and PER.
    private fun validEntry(e: InterestEntry, delta: Boolean): Boolean {
        when (e.cls) {
            InterestVol, InterestPer -> {}
            InterestNone -> if (!delta) return false
            else -> return false
        }
        val n = e.filterBytes.size
        if (n == 0 || n > maxBytes || !e.validUtf8() || e.filterBytes.any { it.toInt() == 0 }) return false
        return try {
            TopicFilter.validFilter(e.filter)
            true
        } catch (_: IllegalArgumentException) {
            false
        }
    }

    private fun expiryOf(e: InterestEntry): Long = if (e.cls == InterestPer) e.expirySec else 0L

    // connect is called once a session of consumer idx passed HELLO_OK. capable is the final CapInterest
    // agreement; instance is the consumer's HELLO instanceId.
    fun connect(idx: Int, instance: Long, capable: Boolean) {
        var dropped = 0
        val restarted: Boolean
        val prev: InterestState
        val prevDense: Boolean
        val n: Int
        val state: InterestState
        val p: Peer
        writeLocked(lock, stamp) {
            p = peer(idx)?.takeIf { it.enabled } ?: return
            prev = p.state
            prevDense = p.dense
            p.connected = true
            p.snapOpen = false
            p.snapMarks = null
            p.lastGen = -1L
            p.sweeping = false
            restarted = p.instanceSeen && p.instance != instance
            p.instance = instance
            p.instanceSeen = true
            if (!capable) {
                clearEntriesLocked(p)
                p.dense = true
                p.state = InterestState.LIVE
            } else {
                p.dense = false
                if (prevDense) p.state = InterestState.UNKNOWN
                if (restarted) {
                    val vol = p.entries.filterValues { it.cls == InterestVol }.keys.toList()
                    for (f in vol) if (deleteLocked(p, f)) dropped++
                }
            }
            applyModeLocked(p)
            n = p.entries.size
            state = p.state
        }
        if (dropped > 0) volatileDropped.addAndGet(dropped.toLong())
        if (!capable) {
            if (!prevDense || prev != InterestState.LIVE) {
                logger.info("peerlink: interest not agreed with consumer \"${p.nodeId}\"; serving all records")
            }
            return
        }
        if (restarted) {
            logger.info("peerlink: consumer \"${p.nodeId}\" restarted; volatile interest dropped " +
                "(volatileDropped=$dropped, filters=$n, state=$state)")
        }
    }

    // disconnect is called when the active session of consumer idx ends.
    fun disconnect(idx: Int, nowMs: Long = System.currentTimeMillis()) {
        val changed: Boolean
        val n: Int
        val dense: Boolean
        val p: Peer
        writeLocked(lock, stamp) {
            p = peer(idx)?.takeIf { it.enabled && it.connected } ?: return
            p.connected = false
            p.snapOpen = false
            p.snapMarks = null
            changed = p.state == InterestState.LIVE
            if (changed) {
                p.state = InterestState.DISCONNECTED
                p.disconnectedAtMs = nowMs
            }
            applyModeLocked(p)
            n = p.entries.size
            dense = p.dense
        }
        if (changed) {
            logger.warning("peerlink: peer \"${p.nodeId}\" interest DISCONNECTED (filters=$n, dense=$dense)")
        }
    }

    // applySnapshot applies one INTEREST_SNAPSHOT frame; a protocol error throws InterestProtocolException.
    fun applySnapshot(idx: Int, s: InterestSnapshot, now: Instant = Instant.now()) {
        var rejectedN = 0
        val prev: InterestState
        val wasOver: Boolean
        val over: Boolean
        val n: Int
        val per: Int
        val p: Peer
        writeLocked(lock, stamp) {
            p = peer(idx)?.takeIf { it.enabled } ?: return
            when {
                (s.flags and InterestFlagFirst) != 0 -> {
                    p.snapOpen = true
                    p.snapGen = s.generation
                    p.snapMarks = HashMap(maxOf(16, s.entries.size * 2))
                }
                !p.snapOpen -> throw InterestProtocolException("INTEREST_SNAPSHOT continuation without an open snapshot")
                s.generation != p.snapGen -> throw InterestProtocolException("INTEREST_SNAPSHOT chunk with a different generation")
            }
            val marks = p.snapMarks!!
            for (e in s.entries) {
                if (!validEntry(e, false)) {
                    rejectedN++
                    continue
                }
                marks[e.filter] = Entry(e.cls, expiryOf(e))
            }
            if ((s.flags and InterestFlagLast) == 0) {
                countRejected(p.nodeId, rejectedN)
                return
            }
            // Mark and sweep: the marked set replaces the old one at once.
            prev = p.state
            wasOver = p.overLimit
            clearEntriesLocked(p)
            p.snapMarks = null
            p.snapOpen = false
            p.overLimit = marks.size > maxFilters
            if (!p.overLimit) {
                p.entries = marks
                p.persistent = marks.values.count { it.cls == InterestPer }
                if (p.inTrie) for (f in marks.keys) trie.set(f, p.bit, true)
            }
            p.lastGen = s.generation
            p.snapshotGen = s.generation
            p.lastSnapshotAt = now
            p.state = InterestState.LIVE
            p.sweeping = false
            applyModeLocked(p)
            n = p.entries.size
            per = p.persistent
            over = p.overLimit
        }
        countRejected(p.nodeId, rejectedN)
        overLimitChanged(p.nodeId, wasOver, over, s.entries.size)
        if (prev != InterestState.LIVE) {
            logger.info("peerlink: peer \"${p.nodeId}\" interest LIVE (filters=$n, filtersPersistent=$per, generation=${s.generation})")
        }
    }

    // applyDelta applies one INTEREST_DELTA frame. Deltas with a generation not above the last applied
    // one are ignored; generations are compared as unsigned 32-bit values held in a Long.
    fun applyDelta(idx: Int, d: InterestDelta) {
        deltasReceived.incrementAndGet()
        var rejectedN = 0
        val over: Boolean
        val n: Int
        val p: Peer
        writeLocked(lock, stamp) {
            p = peer(idx)?.takeIf { it.enabled } ?: return
            if (p.lastGen >= 0 && d.generation <= p.lastGen) return
            p.lastGen = d.generation
            if (p.overLimit) return
            for (e in d.entries) {
                if (!validEntry(e, true)) {
                    rejectedN++
                    continue
                }
                if (e.cls == InterestNone) deleteLocked(p, e.filter)
                else putLocked(p, e.filter, Entry(e.cls, expiryOf(e)))
            }
            n = p.entries.size
            over = n > maxFilters
            if (over) {
                clearEntriesLocked(p)
                p.overLimit = true
                applyModeLocked(p)
            }
        }
        countRejected(p.nodeId, rejectedN)
        overLimitChanged(p.nodeId, false, over, n)
    }

    private fun countRejected(peer: String, n: Int) {
        if (n == 0) return
        rejected.addAndGet(n.toLong())
        logger.warning("peerlink: $n invalid interest entries from peer \"$peer\" ignored")
    }

    private fun overLimitChanged(peer: String, was: Boolean, now: Boolean, filters: Int) {
        if (now && !was) {
            overLimitCount.incrementAndGet()
            logger.warning("peerlink: peer \"$peer\" interest exceeds MaxFiltersPerPeer ($filters > $maxFilters); serving all records")
        } else if (was && !now) {
            logger.warning("peerlink: peer \"$peer\" interest within MaxFiltersPerPeer again ($filters); filtering resumed")
        }
    }

    // expire drops the persistent entries of disconnected peers whose announced expiry ran out and starts
    // the backlog sweep for those peers.
    fun expire(nowMs: Long = System.currentTimeMillis()) {
        val evs = ArrayList<Triple<String, Int, Int>>()
        writeLocked(lock, stamp) {
            for (p in peers) {
                if (p.state != InterestState.DISCONNECTED || p.connected || p.persistent == 0) continue
                val down = nowMs - p.disconnectedAtMs
                var n = 0
                val due = p.entries.filter { (_, e) ->
                    e.cls == InterestPer && e.expirySec != InterestExpiryNever && down > e.expirySec * 1000L
                }.keys.toList()
                for (f in due) if (deleteLocked(p, f)) n++
                if (n > 0) {
                    p.sweeping = true
                    p.sweepFrom = 0L
                    p.sweepSeq++
                    evs.add(Triple(p.nodeId, n, p.entries.size))
                }
            }
        }
        for ((peer, expired, left) in evs) {
            persistentExpired.addAndGet(expired.toLong())
            logger.info("peerlink: persistent interest of peer \"$peer\" expired (expired=$expired, filters=$left)")
        }
    }

    // sweep runs one bounded step of the persistent-expiry backlog sweep per peer: bits of non-retained,
    // non-snapshot records the peer no longer matches are cleared (retained publishes, clears, snapshot
    // records and tombstones are kept).
    fun sweep(log: PeerLog?) {
        if (log == null || !log.masked) return
        class Done(val p: Peer, val seq: Long, val next: Long, val end: Boolean)
        val res = ArrayList<Done>()
        lock.read {
            val view = RecordView()
            for (p in peers) {
                if (!p.sweeping) continue
                val bit = p.bit
                val keep = (always and bit) != 0L
                val (next, cleared) = log.clearConsumerBits(p.idx, p.sweepFrom) { frame ->
                    if (keep) return@clearConsumerBits false
                    try {
                        decodeRecord(frame, view)
                    } catch (_: Exception) {
                        return@clearConsumerBits false
                    }
                    if (view.retain() || view.snapshot() || view.skipped() || view.topic.isEmpty()) {
                        false
                    } else {
                        (trie.match(view.topicString()) and bit) == 0L
                    }
                }
                backlogDiscarded.addAndGet(cleared)
                res.add(Done(p, p.sweepSeq, next, next >= log.getLEO()))
            }
        }
        if (res.isEmpty()) return
        writeLocked(lock, stamp) {
            for (r in res) {
                if (r.p.sweepSeq != r.seq || !r.p.sweeping) continue
                if (r.end) r.p.sweeping = false else r.p.sweepFrom = r.next
            }
        }
    }

    fun status(idx: Int): InterestStatus? = lock.read {
        val p = peer(idx) ?: return null
        InterestStatus(
            state = if (!p.enabled) "OFF" else p.state.name,
            mode = when {
                filteredLocked(p) -> "FILTERED"
                alwaysLocked(p) -> "ALL"
                else -> "NONE"
            },
            filters = p.entries.size,
            filtersPersistent = p.persistent,
            snapshotGeneration = p.snapshotGen,
            lastSnapshotAt = p.lastSnapshotAt?.toString(),
            instanceId = if (p.instanceSeen) String.format("%016x", p.instance) else null
        )
    }
}
