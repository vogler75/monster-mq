package at.rocworks.peerlink

import at.rocworks.Utils
import at.rocworks.data.SubscriptionObserver
import at.rocworks.peerlink.core.TopicFilter
import at.rocworks.peerlink.wire.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger

// Class of one owner's interest in a filter: InterestVol, or InterestPer with expirySec
// (InterestExpiryNever for no expiry).
data class InterestClass(val cls: Int, val expirySec: Long = 0L) {
    companion object {
        val VOL = InterestClass(InterestVol, 0L)
        val PER_NEVER = InterestClass(InterestPer, InterestExpiryNever)
    }
}

// Classifies the owner of a runtime subscription; null means the owner is not announced.
fun interface ClientClassifier {
    fun classify(clientId: String): InterestClass?
}

// Redundancy component provider (plan-peerlink-interest-routing 5.2, C6): configured filters of the
// HOT_STANDBY / COLD_STANDBY components, whether they run or not. Main has no redundancy roles yet, so
// the default provider announces nothing; a later provider is passed to InterestTracker.setSource.
interface RedundancyComponentProvider {
    fun filters(): Map<String, InterestClass>
}

object NoRedundancyComponents : RedundancyComponentProvider {
    override fun filters(): Map<String, InterestClass> = emptyMap()
}

// InterestTracker keeps the local interest of this node (plan-peerlink-interest-routing 5.2): per filter
// the owners with their class, the announced class per filter, and the pending changes. It feeds one
// generation cursor per puller (Feed): a snapshot at subscription, then deltas until the retained
// history overflows, which forces a new snapshot.
class InterestTracker(
    private val flushMs: Long,
    private val maxFilterBytes: Int,
    private val classifier: ClientClassifier,
    private val maxRetainedFrames: Int = 256,
    private val logger: Logger = Utils.getLogger(InterestTracker::class.java)
) : SubscriptionObserver {

    companion object {
        const val MaxPendingEntries = 1024
        const val SourcePrefix = "\u0000src:"
        // Generations above this force a snapshot, which resets the source's generation (no u32 wrap).
        const val GenerationLimit = 0xFFFF_FF00L
        private const val SnapshotBudget = MaxConsumerFrame - FrameHeaderLen - InterestSnapshotHeaderLen
        private const val DeltaBudget = MaxConsumerFrame - FrameHeaderLen - InterestDeltaHeaderLen
        private const val MaxWarnedFilters = 1024
    }

    private sealed class Event {
        class Added(val clientId: String, val filter: String) : Event()
        class Removed(val clientId: String, val filter: String) : Event()
    }

    // A puller's cursor. frames holds the frames not yet written; needsSnapshot replaces them by a new
    // snapshot at the next poll.
    inner class Feed internal constructor(private val onWake: () -> Unit) {
        internal val frames = ArrayDeque<Frame>()
        internal var generation = 0L
        internal var needsSnapshot = true

        // Frames to write now, in order; snapshot frames are never interleaved with deltas.
        fun poll(): List<Frame> = synchronized(lock) {
            if (needsSnapshot) {
                frames.clear()
                frames.addAll(snapshotFramesLocked(this))
                needsSnapshot = false
            }
            if (frames.isEmpty()) return emptyList()
            val res = ArrayList(frames)
            frames.clear()
            res
        }

        internal fun wake() = try { onWake() } catch (_: Exception) {}
    }

    private val lock = Any()
    private val owners = HashMap<String, HashMap<String, InterestClass>>()
    private val byOwner = HashMap<String, HashSet<String>>()
    private val announced = HashMap<String, InterestClass>()
    private val pending = LinkedHashSet<String>()
    private var pendingBytes = 0
    private val feeds = ArrayList<Feed>()
    private val warned = HashSet<String>()

    private val events = LinkedBlockingQueue<Event>()
    @Volatile private var worker: Thread? = null

    val rejected = AtomicLong()
    // Delta frames produced since start, the node-wide generation of the status (local.generation).
    private val generation = AtomicLong()

    // --- SubscriptionObserver: only queue work ---

    override fun added(clientId: String, filter: String) {
        events.add(Event.Added(clientId, filter))
    }

    override fun removed(clientId: String, filter: String) {
        events.add(Event.Removed(clientId, filter))
    }

    fun start() {
        if (worker != null) return
        worker = Thread.ofVirtual().name("peerlink-interest-tracker").start { run() }
    }

    fun stop() {
        worker?.interrupt()
        worker = null
    }

    private fun run() {
        var deadline = 0L
        try {
            while (!Thread.currentThread().isInterrupted) {
                val wait = if (deadline == 0L) 1000L else maxOf(0L, deadline - System.currentTimeMillis())
                val ev = events.poll(wait, TimeUnit.MILLISECONDS)
                if (ev != null) {
                    apply(ev)
                    while (true) apply(events.poll() ?: break)
                }
                val now = System.currentTimeMillis()
                val size = synchronized(lock) { pending.size }
                if (size == 0) {
                    deadline = 0L
                    continue
                }
                if (deadline == 0L) deadline = now + flushMs
                if (now >= deadline || overThreshold()) {
                    flush()
                    deadline = 0L
                }
            }
        } catch (_: InterruptedException) {
        }
    }

    // Applies all queued events without flushing (tests and synchronous callers).
    fun drainEvents() {
        while (true) apply(events.poll() ?: break)
    }

    private fun overThreshold(): Boolean = synchronized(lock) {
        pending.size >= MaxPendingEntries || pendingBytes >= DeltaBudget
    }

    private fun apply(ev: Event) {
        when (ev) {
            is Event.Added -> {
                if (!announceable(ev.clientId, ev.filter)) return
                val c = classifier.classify(ev.clientId) ?: return
                synchronized(lock) { putOwnerLocked(ev.clientId, ev.filter, c) }
            }
            is Event.Removed -> synchronized(lock) { removeOwnerLocked(ev.clientId, ev.filter) }
        }
    }

    // Whether a filter of clientId may be announced; invalid filters are counted and logged.
    private fun announceable(clientId: String, filter: String): Boolean {
        if (clientId.startsWith("peerlink:")) return false
        if (filter.startsWith("$")) return false
        val reason = invalidReason(filter) ?: return true
        rejected.incrementAndGet()
        val first = synchronized(lock) { warned.size < MaxWarnedFilters && warned.add(filter) }
        if (first) logger.warning("peerlink: interest filter of client \"$clientId\" not announced: $reason")
        return false
    }

    private fun invalidReason(filter: String): String? {
        if (filter.isEmpty()) return "empty filter"
        if (!Charsets.UTF_8.newEncoder().canEncode(filter)) return "filter is not valid UTF-8"
        val n = filter.toByteArray(Charsets.UTF_8).size
        if (n > maxFilterBytes) return "filter of $n bytes exceeds MaxFilterBytes $maxFilterBytes"
        return try {
            TopicFilter.validFilter(filter)
            null
        } catch (e: IllegalArgumentException) {
            e.message ?: "invalid filter"
        }
    }

    // setSource replaces the filters of a static source (archive groups, redundancy provider).
    fun setSource(key: String, filters: Map<String, InterestClass>) {
        val owner = SourcePrefix + key
        val valid = filters.filterKeys { it.isNotEmpty() && !it.startsWith("$") && invalidReason(it) == null }
        val bad = filters.size - valid.size - filters.keys.count { it.startsWith("$") }
        if (bad > 0) {
            rejected.addAndGet(bad.toLong())
            logger.warning("peerlink: $bad interest filter(s) of source \"$key\" not announced (invalid)")
        }
        synchronized(lock) {
            val old = byOwner[owner]?.toList() ?: emptyList()
            for (f in old) if (f !in valid) removeOwnerLocked(owner, f)
            for ((f, c) in valid) putOwnerLocked(owner, f, c)
        }
    }

    private fun putOwnerLocked(owner: String, filter: String, c: InterestClass) {
        val m = owners.getOrPut(filter) { HashMap(4) }
        if (m.put(owner, c) == c) return
        byOwner.getOrPut(owner) { HashSet() }.add(filter)
        markPendingLocked(filter)
    }

    private fun removeOwnerLocked(owner: String, filter: String) {
        val m = owners[filter] ?: return
        if (m.remove(owner) == null) return
        if (m.isEmpty()) owners.remove(filter)
        byOwner[owner]?.let { s ->
            s.remove(filter)
            if (s.isEmpty()) byOwner.remove(owner)
        }
        markPendingLocked(filter)
    }

    private fun markPendingLocked(filter: String) {
        if (pending.add(filter)) pendingBytes += InterestEntryOverhead + filter.toByteArray(Charsets.UTF_8).size
    }

    // Aggregate class of a filter: PER if any owner is PER (with the largest expiry), else VOL, else NONE.
    private fun aggregateLocked(filter: String): InterestClass? {
        val m = owners[filter] ?: return null
        var vol = false
        var per = false
        var maxExpiry = 0L
        for (c in m.values) {
            if (c.cls == InterestPer) {
                per = true
                if (c.expirySec > maxExpiry) maxExpiry = c.expirySec
            } else {
                vol = true
            }
        }
        return when {
            per -> InterestClass(InterestPer, maxExpiry)
            vol -> InterestClass.VOL
            else -> null
        }
    }

    // flush turns the pending filters into deltas for all feeds; called by the worker every FlushMs or
    // at the size thresholds, and by tests.
    fun flush() {
        val woken: List<Feed>
        synchronized(lock) {
            if (pending.isEmpty()) return
            val entries = ArrayList<InterestEntry>()
            for (f in pending) {
                val now = aggregateLocked(f)
                val was = announced[f]
                if (!changed(was, now)) continue
                if (now == null) {
                    announced.remove(f)
                    entries.add(InterestEntry(InterestNone, 0L, f))
                } else {
                    announced[f] = now
                    entries.add(InterestEntry(now.cls, if (now.cls == InterestPer) now.expirySec else 0L, f))
                }
            }
            pending.clear()
            pendingBytes = 0
            if (entries.isEmpty()) return
            val chunks = split(entries, DeltaBudget)
            generation.addAndGet(chunks.size.toLong())
            for (feed in feeds) {
                if (feed.needsSnapshot) continue
                if (feed.frames.size + chunks.size > maxRetainedFrames || feed.generation + chunks.size >= GenerationLimit) {
                    feed.frames.clear()
                    feed.needsSnapshot = true
                    continue
                }
                for (c in chunks) feed.frames.addLast(InterestDelta(++feed.generation, c))
            }
            woken = ArrayList(feeds)
        }
        woken.forEach { it.wake() }
    }

    // Whether a delta is due: the class changed, or a PER expiry moved by more than 10 % or crossed never.
    private fun changed(was: InterestClass?, now: InterestClass?): Boolean {
        val wc = was?.cls ?: InterestNone
        val nc = now?.cls ?: InterestNone
        if (wc != nc) return true
        if (nc != InterestPer) return false
        val a = was!!.expirySec
        val b = now!!.expirySec
        if (a == b) return false
        if (a == InterestExpiryNever || b == InterestExpiryNever) return true
        return Math.abs(b - a) * 10 > a
    }

    // subscribe registers a puller's cursor; its first poll returns the snapshot of the announced set.
    fun subscribe(onWake: () -> Unit): Feed = synchronized(lock) {
        Feed(onWake).also { feeds.add(it) }
    }

    fun unsubscribe(feed: Feed) {
        synchronized(lock) { feeds.remove(feed) }
    }

    private fun snapshotFramesLocked(feed: Feed): List<Frame> {
        val entries = announced.map { (f, c) ->
            InterestEntry(c.cls, if (c.cls == InterestPer) c.expirySec else 0L, f)
        }
        val chunks = split(entries, SnapshotBudget)
        // The snapshot resets the source's generation, so a cursor near the limit starts over at 1.
        feed.generation = if (feed.generation + 1 >= GenerationLimit) 1L else feed.generation + 1
        return chunks.mapIndexed { i, c ->
            var flags = 0
            if (i == 0) flags = flags or InterestFlagFirst
            if (i == chunks.size - 1) flags = flags or InterestFlagLast
            InterestSnapshot(feed.generation, flags, c)
        }
    }

    // split cuts entries into chunks of at most MaxPendingEntries entries and budget encoded bytes;
    // an empty list gives one empty chunk.
    private fun split(entries: List<InterestEntry>, budget: Int): List<MutableList<InterestEntry>> {
        val res = ArrayList<MutableList<InterestEntry>>()
        var cur = ArrayList<InterestEntry>()
        var bytes = 0
        for (e in entries) {
            val n = e.len()
            if (cur.isNotEmpty() && (cur.size >= MaxPendingEntries || bytes + n > budget)) {
                res.add(cur)
                cur = ArrayList()
                bytes = 0
            }
            cur.add(e)
            bytes += n
        }
        res.add(cur)
        return res
    }

    // Snapshot of the announced set (status and tests).
    fun announced(): Map<String, InterestClass> = synchronized(lock) { HashMap(announced) }

    fun status(): TrackerStatus = synchronized(lock) {
        TrackerStatus(filters = announced.size, generation = generation.get(), rejected = rejected.get())
    }
}
