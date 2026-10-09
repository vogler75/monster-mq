package at.rocworks.peerlink

import at.rocworks.data.BrokerMessage
import at.rocworks.peerlink.core.IncludeExclude
import at.rocworks.peerlink.core.LogKind
import at.rocworks.peerlink.core.PeerLog
import at.rocworks.peerlink.wire.*
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.locks.ReentrantLock

// Session established time tracking for will supersession (hook.go: lines 240-322)
class SessionTimes {
    private class Shard {
        val lock = ReentrantLock()
        val map = HashMap<Long, Long>()
        var lastPrune: Long = 0L
    }

    private val shards = Array(16) { Shard() }

    companion object {
        private const val TTL_NS = 24L * 3600L * 1_000_000_000L // 24 hours
        private const val SHARD_MAX = 16 shl 10 // 16k entries

        fun hashId(s: String): Long {
            var h = -3750763034362895579L // 14695981039346656037 unsigned as signed long
            for (i in 0 until s.length) {
                h = h xor s[i].code.toLong()
                h *= 1099511628211L
            }
            return h
        }
    }

    fun record(id: String, nowNs: Long) {
        val k = hashId(id)
        val idx = (k and 15L).toInt()
        val sh = shards[idx]
        sh.lock.lock()
        try {
            sh.map[k] = nowNs
            if (sh.map.size > SHARD_MAX || (nowNs - sh.lastPrune > 60_000_000_000L && sh.map.size > 1024)) {
                sh.lastPrune = nowNs
                val it = sh.map.entries.iterator()
                while (it.hasNext()) {
                    val entry = it.next()
                    if (nowNs - entry.value > TTL_NS) {
                        it.remove()
                    }
                }
                if (sh.map.size > SHARD_MAX) {
                    // Drop older half
                    val sorted = sh.map.values.sorted()
                    val cut = sorted[sorted.size / 2]
                    val it2 = sh.map.entries.iterator()
                    while (it2.hasNext()) {
                        if (it2.next().value < cut) {
                            it2.remove()
                        }
                    }
                }
            }
        } finally {
            sh.lock.unlock()
        }
    }

    fun since(id: String, atNs: Long): Boolean {
        val k = hashId(id)
        val idx = (k and 15L).toInt()
        val sh = shards[idx]
        sh.lock.lock()
        try {
            val v = sh.map[k] ?: return false
            return v >= atNs
        } finally {
            sh.lock.unlock()
        }
    }
}

// Echo suppression table (hook.go: lines 324-381)
class EchoTable(windowMs: Int) {
    private val windowNs = windowMs.toLong() * 1_000_000L

    private data class EchoEntry(
        val hash: Long,
        val retain: Boolean,
        val at: Long
    )

    private class Shard {
        val lock = ReentrantLock()
        val map = HashMap<String, EchoEntry>()
        var lastPrune: Long = 0L
    }

    private val shards = Array(16) { Shard() }

    companion object {
        fun payloadHash(b: ByteArray): Long {
            var h = -3750763034362895579L
            for (byte in b) {
                h = h xor (byte.toInt() and 0xFF).toLong()
                h *= 1099511628211L
            }
            return h
        }
    }

    fun record(topic: String, payload: ByteArray, retain: Boolean, nowNs: Long) {
        val hash = SessionTimes.hashId(topic)
        val idx = (hash and 15L).toInt()
        val sh = shards[idx]
        val ent = EchoEntry(payloadHash(payload), retain, nowNs)
        sh.lock.lock()
        try {
            sh.map[topic] = ent
            if (nowNs - sh.lastPrune > windowNs && sh.map.size > 256) {
                sh.lastPrune = nowNs
                val it = sh.map.entries.iterator()
                while (it.hasNext()) {
                    if (nowNs - it.next().value.at > windowNs) {
                        it.remove()
                    }
                }
            }
        } finally {
            sh.lock.unlock()
        }
    }

    fun match(topic: String, payload: ByteArray, retain: Boolean, nowNs: Long): Boolean {
        val hash = SessionTimes.hashId(topic)
        val idx = (hash and 15L).toInt()
        val sh = shards[idx]
        sh.lock.lock()
        try {
            val ent = sh.map[topic] ?: return false
            if (ent.retain != retain || nowNs - ent.at > windowNs) {
                return false
            }
            return ent.hash == payloadHash(payload)
        } finally {
            sh.lock.unlock()
        }
    }
}

// Hook captures local publishes into the log (hook.go: lines 26-238)
class CaptureHook(
    val log: PeerLog?,
    val filter: IncludeExclude,
    val captureWills: Boolean = true,
    val echoSuppressMs: Int = 0,
    val maxExpirySec: Long = 0L,
    val namespacePredicate: ((String) -> Boolean)? = null
) {
    val active = AtomicBoolean(false)
    val draining = AtomicBoolean(false)
    val echo = if (echoSuppressMs > 0) EchoTable(echoSuppressMs) else null
    val sessions = SessionTimes()

    val filtered = LongAdder()
    val echoSuppressed = LongAdder()
    val skipPeer = LongAdder()
    val skipWill = LongAdder()
    val sharedSkipped = LongAdder()
    val refusedIDs = LongAdder()
    val usernameStripped = LongAdder()

    // Remote interest table; null when interest routing is disabled (plan-peerlink-interest-routing 6.2).
    @Volatile var interest: InterestTable? = null

    fun accept(topic: String): Boolean {
        if (topic.isEmpty() || topic[0] == '$' || (namespacePredicate != null && namespacePredicate.invoke(topic))) {
            return false
        }
        return filter.accept(topic)
    }

    private fun isValidUsername(bytes: ByteArray): Boolean {
        if (bytes.size > MaxStringLen) return false
        for (b in bytes) {
            if (b.toInt() == 0) return false
        }
        return try {
            val s = String(bytes, StandardCharsets.UTF_8)
            s.toByteArray(StandardCharsets.UTF_8).contentEquals(bytes)
        } catch (_: Exception) {
            false
        }
    }

    fun capture(message: BrokerMessage, will: Boolean = message.isWill) {
        if (!active.get()) {
            if (log != null && log.isSealed() && accept(message.topicName)) {
                log.countUncapturedAtShutdown()
            }
            return
        }
        if (message.peer != null || message.peerSource != null) {
            skipPeer.increment()
            if (echo != null && message.peer != null && !message.isWill) {
                echo.record(message.topicName, message.payload, message.isRetain, System.nanoTime())
            }
            return
        }
        if (will && (!captureWills || draining.get())) {
            skipWill.increment()
            return
        }
        if (!accept(message.topicName)) {
            filtered.increment()
            return
        }
        if (echo != null && !will && echo.match(message.topicName, message.payload, message.isRetain, System.nanoTime())) {
            echoSuppressed.increment()
            return
        }

        // Retained publishes go to every consumer; others only to consumers whose interest matches. The
        // record is built only after this check, so a skipped publish allocates nothing (G-IR1).
        val table = interest
        var mask = log?.allMask ?: 0L
        if (table != null && !message.isRetain) {
            mask = table.match(message.topicName)
            if (mask == 0L) {
                table.skipped.increment()
                return
            }
            table.matched.increment()
        }

        val inline = message.clientId.isEmpty() || message.clientId == "inline"
        val kind = when {
            will -> LogKind.Will
            inline -> LogKind.Inline
            else -> LogKind.Client
        }
        var flags = (message.qosLevel and FlagQoSMask)
        if (message.isRetain) flags = flags or FlagRetain
        if (message.isDup) flags = flags or FlagDup
        if (will) flags = flags or FlagWill
        if (inline) flags = flags or FlagInline

        var userBytes: ByteArray = message.username?.toByteArray(StandardCharsets.UTF_8) ?: ByteArray(0)
        if (userBytes.isNotEmpty() && !isValidUsername(userBytes)) {
            userBytes = ByteArray(0)
            usernameStripped.increment()
        }

        var expiry = if (will) 0L else (message.messageExpiryInterval ?: 0L)
        if (maxExpirySec > 0L && expiry > maxExpirySec) {
            expiry = maxExpirySec
        }

        val userProps = mutableListOf<UserProp>()
        message.userProperties?.forEach { (k, v) ->
            userProps.add(UserProp(k, v))
        }

        val rec = Record(
            flags = flags,
            publishWallNs = message.time.toEpochMilli() * 1_000_000L,
            captureMonoMs = log?.monoMs(System.nanoTime()) ?: 0L,
            expirySec = expiry,
            payloadFormat = (message.payloadFormatIndicator ?: 0).toByte(),
            topic = message.topicName,
            clientID = message.clientId.ifEmpty { "inline" },
            username = userBytes,
            contentType = message.contentType ?: "",
            responseTopic = message.responseTopic ?: "",
            correlationData = message.correlationData ?: ByteArray(0),
            user = userProps,
            payload = message.payload
        )
        if (message.payloadFormatIndicator != null) {
            rec.flags = rec.flags or FlagPayloadFormat
        }

        if (inline && !rec.validContent()) {
            log?.countCaptureInvalid()
            return
        }

        val size = recordSize(rec)
        if (size == 0) {
            log?.countCaptureInvalid()
            return
        }
        if (log != null && !log.checkRecordSize(size)) {
            return
        }
        val buf = ByteArray(size)
        encodeRecord(buf, rec)
        if (table != null) log?.appendMask(buf, kind, mask) else log?.append(buf, kind)
    }

    fun recapture(message: BrokerMessage, nowSec: Long): Boolean {
        if (log == null || !active.get() || message.payload.isEmpty()) {
            return false
        }
        val before = log.getLEO()
        val copy = message.copy(isRetain = true, isDup = false)
        capture(copy, false)
        return log.getLEO() != before
    }
}
