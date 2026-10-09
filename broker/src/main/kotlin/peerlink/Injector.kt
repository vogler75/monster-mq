package at.rocworks.peerlink

import at.rocworks.Utils
import at.rocworks.bus.IMessageBus
import at.rocworks.data.BrokerMessage
import at.rocworks.data.PeerForward
import at.rocworks.handlers.SessionHandler
import at.rocworks.peerlink.core.IncludeExclude
import at.rocworks.peerlink.core.TopicFilter
import at.rocworks.peerlink.wire.*
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.locks.ReentrantLock
import java.util.logging.Logger

const val DROP_MALFORMED = 0
const val DROP_SIZE_SOURCE = 1
const val DROP_NAMESPACE = 2
const val DROP_FILTERED = 3
const val DROP_SIZE = 4
const val DROP_EXPIRED = 5
const val DROP_STALE = 6
const val DROP_WILL_SUPERSEDED = 7
const val NUM_DROP_REASONS = 8

val DROP_NAMES = arrayOf(
    "malformed", "size_source", "namespace", "filtered",
    "size", "expired", "stale", "will_superseded"
)

const val DIV_SIZE = 0
const val DIV_SIZE_SOURCE = 1
const val DIV_EXPIRED = 2
const val NUM_DIV_REASONS = 3

val DIV_NAMES = arrayOf("size", "size_source", "expired")

const val MARK_REPLICAS_KEY = "mmq-peer-src"

const val SNAP_NONE = 0
const val SNAP_FILL = 1
const val SNAP_NEWER = 2

class RateLimiter(private val maxEntries: Int = 1024) {
    private val lock = ReentrantLock()
    private class RateState(var last: Long, val everyMs: Long, var suppressed: Long)
    private val map = HashMap<String, RateState>()

    fun allow(key: String, everyMs: Long): Pair<Boolean, Long> {
        lock.lock()
        try {
            val now = System.currentTimeMillis()
            val st = map[key]
            if (st == null) {
                if (map.size >= maxEntries) {
                    map.entries.removeIf { now - it.value.last >= it.value.everyMs }
                    if (map.size >= maxEntries) map.clear()
                }
                map[key] = RateState(now, everyMs, 0)
                return Pair(true, 0L)
            }
            if (now - st.last < everyMs) {
                st.suppressed++
                return Pair(false, 0L)
            }
            val n = st.suppressed
            st.last = now
            st.suppressed = 0L
            return Pair(true, n)
        } finally {
            lock.unlock()
        }
    }
}

class Pacer(
    var factor: Double = 0.0,
    var maxRate: Double = 0.0
) {
    private val lock = ReentrantLock()
    private var rate: Double = 0.0
    private var lastLeo: Long = 0L
    private var lastMono: Long = 0L
    private var tokens: Double = 0.0
    private var lastTime: Long = 0L

    companion object {
        const val PACER_FLOOR = 1000.0
    }

    fun observe(leo: Long, monoMs: Long) {
        lock.lock()
        try {
            if (lastMono != 0L && monoMs > lastMono && leo >= lastLeo) {
                val dt = (monoMs - lastMono).toDouble() / 1000.0
                if (dt >= 0.05) {
                    val r = (leo - lastLeo).toDouble() / dt
                    rate = if (rate == 0.0) r else rate * 0.7 + r * 0.3
                    lastLeo = leo
                    lastMono = monoMs
                }
                return
            }
            lastLeo = leo
            lastMono = monoMs
        } finally {
            lock.unlock()
        }
    }

    fun limit(lag: Long, threshold: Int): Double {
        lock.lock()
        try {
            var r = 0.0
            if (factor > 0.0 && lag > threshold.toLong()) {
                r = maxOf(factor * rate, PACER_FLOOR)
            }
            if (maxRate > 0.0 && (r == 0.0 || maxRate < r)) {
                r = maxRate
            }
            return r
        } finally {
            lock.unlock()
        }
    }

    fun take(r: Double): Boolean {
        if (r <= 0.0) return false
        var sleepMs = 0L
        lock.lock()
        try {
            val now = System.currentTimeMillis()
            val burst = maxOf(r / 10.0, 1.0)
            if (lastTime == 0L) {
                tokens = burst
            } else {
                tokens = minOf(burst, tokens + ((now - lastTime).toDouble() / 1000.0) * r)
            }
            lastTime = now
            if (tokens < 1.0) {
                val waitSec = (1.0 - tokens) / r
                sleepMs = maxOf((waitSec * 1000.0).toLong(), 2L)
            }
            tokens -= 1.0
        } finally {
            lock.unlock()
        }
        if (sleepMs > 0) {
            try {
                Thread.sleep(sleepMs)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
            }
            lock.lock()
            try {
                val after = System.currentTimeMillis()
                val burst = maxOf(r / 10.0, 1.0)
                tokens = minOf(burst, tokens + ((after - lastTime).toDouble() / 1000.0) * r)
                lastTime = after
            } finally {
                lock.unlock()
            }
            return true
        }
        return false
    }
}

data class BatchIn(
    val batch: Batch,
    val recvAt: Long,
    var poison: Boolean = false
)

data class ApplyContext(
    val srcRoot: String = "",
    val ownRoot: String = "",
    val epoch: Long = 0L,
    var mode: Int = SNAP_NONE,
    var present: Map<String, Long>? = null,
    var rttHalf: Long = 0L,
    var skewMs: Long = 0L,
    var commitFn: ((Long) -> Unit)? = null
)

class Injector(
    val sourceNodeId: String,
    val sessionHandler: SessionHandler,
    val messageBus: IMessageBus,
    val filter: IncludeExclude,
    val hook: CaptureHook?,
    val maxMessageSize: Int = 0,
    val maxRecordAgeMs: Long = 0L,
    val markReplicas: Boolean = false,
    val fetchMaxRecords: Int = 4096,
    val pace: Pacer = Pacer(),
    val rate: RateLimiter = RateLimiter(),
    val logger: Logger = Utils.getLogger(Injector::class.java)
) {
    val appliedNext = AtomicLong(0)
    val batches = LongAdder()
    val injected = LongAdder()
    val retainOnly = LongAdder()
    val appliedBytes = LongAdder()
    val dupSkipped = LongAdder()
    val zenohDupSkipped = LongAdder()
    val dropped = Array(NUM_DROP_REASONS) { LongAdder() }
    val diverged = Array(NUM_DIV_REASONS) { LongAdder() }
    val rejected = LongAdder()
    val unknownProps = LongAdder()
    val snapFilled = LongAdder()
    val snapSkipped = LongAdder()
    val snapNewer = LongAdder()
    val paced = LongAdder()
    val willResent = LongAdder()
    val hist = LatencyHist()

    fun diverge(reason: Int, topic: String) {
        diverged[reason].increment()
        val (ok, n) = rate.allow("diverged:${DIV_NAMES[reason]}", 10_000L)
        if (ok) {
            logger.warning("peerlink: retained value not applied; retained state diverges from source [peer=$sourceNodeId, reason=${DIV_NAMES[reason]}, topic=$topic, suppressed=$n]")
        }
    }

    private fun hasRetained(ac: ApplyContext, topic: String): Boolean {
        if (ac.present != null) {
            return ac.present!!.containsKey(topic)
        }
        val msg = sessionHandler.messageHandler.getRetainedStore()[topic]
        return (msg != null && msg.payload.isNotEmpty()) || sessionHandler.messageHandler.isRetainedTopicQueued(topic)
    }

    private fun createdRetained(ac: ApplyContext, topic: String): Pair<Long, Boolean> {
        if (ac.present != null) {
            val v = ac.present!![topic]
            return if (v != null) Pair(v, true) else Pair(0L, false)
        }
        val msg = sessionHandler.messageHandler.getRetainedStore()[topic]
        if (msg != null && msg.payload.isNotEmpty()) {
            return Pair(msg.time.epochSecond, true)
        }
        return Pair(0L, false)
    }

    fun applyBatch(ac: ApplyContext, inBatch: BatchIn): Long {
        val h = inBatch.batch.header
        val snapshot = (h.flags and BatchFlagSnapshot) != 0
        var next = h.baseOffset + inBatch.batch.span()
        if (snapshot) next = 0L

        if (inBatch.poison) {
            dropped[DROP_MALFORMED].add(h.count.toLong())
            logger.severe("peerlink: poison batch skipped after repeated CRC failures [peer=$sourceNodeId, baseOffset=${h.baseOffset}, count=${h.count}]")
            return next
        }
        if (h.count == 0) return next

        val it = inBatch.batch.iter()
        val v = RecordView()
        var lastCommitTime = System.currentTimeMillis()

        for (i in 0 until h.count) {
            val (hasRecord, malformed) = try {
                it.next(v)
            } catch (e: Exception) {
                val rest = it.remaining()
                dropped[DROP_MALFORMED].add(rest.toLong())
                logger.severe("peerlink: batch-structural fault; rest of batch dropped [peer=$sourceNodeId, baseOffset=${h.baseOffset}, count=${h.count}, dropped=$rest, error=${e.message}]")
                break
            }
            if (!hasRecord) break

            val off = h.baseOffset + inBatch.batch.delta(i)
            if (malformed != null) {
                dropped[DROP_MALFORMED].increment()
                val (ok, n) = rate.allow("malformed", 10_000L)
                if (ok) {
                    logger.warning("peerlink: malformed record dropped [peer=$sourceNodeId, offset=$off, error=${malformed.message}, suppressed=$n]")
                }
                continue
            }

            if (!snapshot && off < appliedNext.get()) {
                dupSkipped.increment()
                continue
            }

            unknownProps.add(v.unknownProps.toLong())
            applyRecord(ac, inBatch, v, off, h, snapshot)

            if (!snapshot && (i and 63) == 63 && ac.commitFn != null && System.currentTimeMillis() - lastCommitTime >= 100L) {
                ac.commitFn!!.invoke(off + 1L)
                lastCommitTime = System.currentTimeMillis()
            }
        }
        return next
    }

    private fun applyRecord(
        ac: ApplyContext,
        inBatch: BatchIn,
        v: RecordView,
        off: Long,
        h: BatchHeader,
        isSnapshotBatch: Boolean
    ) {
        val retain = v.retain()
        if (v.skipped()) {
            dropped[DROP_SIZE_SOURCE].increment()
            if (retain) diverge(DIV_SIZE_SOURCE, "")
            return
        }

        val topic = v.topicString()
        if (topic.isEmpty() || topic[0] == '$' || TopicFilter.underRoot(topic, ac.ownRoot) || TopicFilter.underRoot(topic, ac.srcRoot)) {
            dropped[DROP_NAMESPACE].increment()
            return
        }

        if (!filter.accept(topic)) {
            dropped[DROP_FILTERED].increment()
            return
        }

        if (maxMessageSize > 0 && v.payload.size > maxMessageSize) {
            dropped[DROP_SIZE].increment()
            if (retain) diverge(DIV_SIZE, topic)
            return
        }

        val isSnapRecord = v.snapshot() && ac.mode != SNAP_NONE
        if (isSnapRecord && ac.mode == SNAP_FILL && hasRetained(ac, topic)) {
            snapSkipped.increment()
            return
        }

        val now = System.currentTimeMillis()
        val ageMs = (h.sourceMonoMs - v.captureMonoMs) + (now - inBatch.recvAt) + ac.rttHalf
        val expired = v.expirySec > 0L && ageMs >= v.expirySec * 1000L
        if (expired && !(retain && v.payload.isEmpty())) {
            dropped[DROP_EXPIRED].increment()
            if (retain) diverge(DIV_EXPIRED, topic)
            return
        }

        val stale = expired || (!isSnapRecord && maxRecordAgeMs > 0L && ageMs > maxRecordAgeMs)
        if (stale && !retain) {
            dropped[DROP_STALE].increment()
            return
        }

        val clientID = v.clientIDString()
        if (v.will()) {
            val connectedLocally = sessionHandler.isConnected(clientID)
            val sessionSince = hook != null && hook.sessions.since(clientID, (System.nanoTime() - ageMs * 1_000_000L))
            if (connectedLocally || sessionSince) {
                dropped[DROP_WILL_SUPERSEDED].increment()
                if (retain) {
                    val localRetained = sessionHandler.messageHandler.getRetainedStore()[topic]
                    if (localRetained != null && localRetained.payload.isNotEmpty() && hook != null) {
                        if (hook.recapture(localRetained, now / 1000L)) {
                            willResent.increment()
                        }
                    }
                }
                return
            }
        }

        if (isSnapRecord && ac.mode == SNAP_NEWER) {
            val (created, ok) = createdRetained(ac, topic)
            if (ok) {
                val srcSec = (v.publishWallNs / 1_000_000L - ac.skewMs) / 1000L
                if (srcSec <= created + 1L) {
                    snapSkipped.increment()
                    return
                }
                snapNewer.increment()
            }
        }

        val fwd = PeerForward(
            sourceNode = sourceNodeId,
            clientId = clientID,
            username = if (v.username.isNotEmpty()) v.usernameString() else null,
            timeNs = v.publishWallNs,
            epoch = ac.epoch,
            offset = if (isSnapshotBatch) 0L else off,
            dup = v.dup(),
            will = v.will(),
            snapshot = v.snapshot()
        )

        // Zenoh dedup check (plan 5a, 6.1)
        val msb = SessionTimes.hashId(sourceNodeId) xor ac.epoch
        val lsb = if (isSnapshotBatch) UUID.randomUUID().leastSignificantBits else off
        val msgUuid = UUID(msb, lsb).toString()
        if (messageBus.isExternalTransport && !messageBus.rememberMessageUuid(msgUuid)) {
            zenohDupSkipped.increment()
            return
        }

        val q = v.qos().toInt()
        var createdSec = maxOf((now - ageMs) / 1000L, 1L)
        var mei: Long? = if (v.expirySec > 0L) v.expirySec else null
        if (v.snapshot()) {
            val wallMs = v.publishWallNs / 1_000_000L - ac.skewMs
            val c = maxOf(wallMs / 1000L, 1L)
            if (wallMs > 0 && c < createdSec) {
                if (mei != null && mei > 0L) {
                    mei = minOf(mei + (createdSec - c), 0xFFFFFFFFL)
                }
                createdSec = c
            }
        }

        val userProps = mutableMapOf<String, String>()
        val it = v.propIter()
        var contentType: String? = null
        var responseTopic: String? = null
        var correlationData: ByteArray? = null
        while (it.hasNext()) {
            val item = it.next() ?: break
            when (item.id) {
                PropContentType -> contentType = String(item.value, StandardCharsets.UTF_8)
                PropResponseTopic -> responseTopic = String(item.value, StandardCharsets.UTF_8)
                PropCorrelationData -> correlationData = item.value.clone()
                PropUserProperty -> {
                    val pair = splitUserProperty(item.value)
                    if (pair != null) {
                        userProps[String(pair.first, StandardCharsets.UTF_8)] = String(pair.second, StandardCharsets.UTF_8)
                    }
                }
            }
        }
        if (markReplicas) {
            userProps[MARK_REPLICAS_KEY] = sourceNodeId
        }

        val msg = BrokerMessage(
            messageUuid = msgUuid,
            topicName = topic,
            payload = v.payload,
            qosLevel = q,
            isRetain = retain,
            isDup = false,
            senderId = clientID,
            clientId = clientID,
            time = Instant.ofEpochSecond(createdSec),
            username = fwd.username,
            peer = fwd,
            peerSource = sourceNodeId,
            isWill = v.will(),
            messageExpiryInterval = mei,
            contentType = contentType,
            responseTopic = responseTopic,
            correlationData = correlationData,
            payloadFormatIndicator = if (v.hasPayloadFormat()) (v.payloadFormat.toInt() and 1) else null,
            userProperties = if (userProps.isNotEmpty()) userProps else null
        )

        if (!isSnapRecord) {
            var lag = 0L
            if (h.leo > fwd.offset + 1L) {
                lag = h.leo - fwd.offset - 1L
            }
            val r = pace.limit(lag, fetchMaxRecords)
            if (r > 0.0 && pace.take(r)) {
                paced.increment()
            }
        }

        try {
            if (stale) {
                sessionHandler.applyRetainedOnly(msg)
                retainOnly.increment()
                hook?.echo?.record(topic, v.payload, true, System.nanoTime())
            } else {
                sessionHandler.publishMessage(msg)
                injected.increment()
                if (isSnapRecord) {
                    snapFilled.increment()
                }
            }
            appliedBytes.add(v.payload.size.toLong())
            hist.observe(ageMs)
        } catch (e: Exception) {
            rejected.increment()
            val (ok, n) = rate.allow("rejected", 10_000L)
            if (ok) {
                logger.warning("peerlink: replica rejected by engine [peer=$sourceNodeId, topic=$topic, error=${e.message}, suppressed=$n]")
            }
        }
    }
}
