package at.rocworks.peerlink

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import java.util.concurrent.atomic.LongAdder

val latencyBounds = longArrayOf(
    1, 2, 3, 5, 7, 10, 15, 20, 30, 50, 70, 100, 150, 200, 300, 500, 700,
    1000, 1500, 2000, 3000, 5000, 7000, 10000, 20000, 30000, 60000, 120000, 300000
)

class LatencyHist {
    private val buckets = Array(latencyBounds.size + 1) { LongAdder() }

    fun observe(ms: Long) {
        val v = if (ms < 0) 0L else ms
        var i = 0
        while (i < latencyBounds.size && v > latencyBounds[i]) {
            i++
        }
        buckets[i].increment()
    }

    fun quantile(q: Double): Long {
        val counts = LongArray(buckets.size) { buckets[it].sum() }
        var total = 0L
        for (c in counts) total += c
        if (total == 0L) return -1L
        val rank = (q * (total - 1).toDouble()).toLong() + 1
        var acc = 0L
        for (i in counts.indices) {
            acc += counts[i]
            if (acc >= rank) {
                return if (i < latencyBounds.size) latencyBounds[i] else latencyBounds.last() + 1
            }
        }
        return -1L
    }
}

data class Status(
    val enabled: Boolean,
    val nodeId: String,
    val epoch: Long,
    val listen: String,
    val tls: Boolean,
    val log: LogStatus,
    val admission: AdmissionStatus,
    val consumers: List<ConsumerStatus>,
    val sources: List<SourceStatus>
)

data class KindCounts(
    val client: Long,
    val inline: Long,
    val will: Long
)

data class LogStatus(
    val epoch: Long,
    val lso: Long,
    val leo: Long,
    val lwm: Long,
    val records: Long,
    val bytes: Long,
    val maxBytes: Long,
    val maxMessages: Long,
    val capacitySeconds: Double?,
    val appended: KindCounts,
    val trimmed: Long,
    val evictedUnread: Long,
    val evictedBy: Map<String, Long>,
    val captureDropped: Map<String, Long>,
    val skipPeer: Long,
    val skipWill: Long,
    val filtered: Long,
    val echoSuppressed: Long,
    val sharedSkipped: Long,
    val refusedClientIds: Long,
    val usernameStripped: Long,
    val spareMisses: Long,
    val uncapturedAtShutdown: Long,
    val sealed: Boolean,
    val active: Boolean
)

data class AdmissionStatus(
    val accepted: Long,
    val refusedNetwork: Long,
    val refusedBusy: Long,
    val refusedSniff: Long,
    val refusedPlaintext: Long,
    val refusedHttp: Long,
    val tlsFailures: Long,
    val authFailures: Map<String, Long>,
    val preAuth: Int
)

data class ConsumerStatus(
    val nodeId: String,
    val state: String,
    val remote: String,
    val committed: Long,
    val served: Long,
    val lag: Long,
    val lostTotal: Long,
    val servedRecords: Long,
    val servedBytes: Long,
    val servedSkipped: Map<String, Long>,
    val snapshotServed: Long,
    val sessions: Long,
    val duplicateConsumer: Long,
    val authFailures: Map<String, Long>,
    @JsonInclude(JsonInclude.Include.NON_EMPTY) val lastFetch: String? = null,
    val shutdownUnserved: Long,
    val oaRetained: Boolean,
    val topicRootMismatch: Boolean,
    val retainedClassMismatch: Boolean
)

data class SourceStatus(
    val nodeId: String,
    val address: String,
    val state: String,
    val epoch: Long,
    val appliedNext: Long,
    val sourceLeo: Long,
    val lagRecords: Long,
    val batches: Long,
    val injected: Long,
    val retainOnly: Long,
    val appliedBytes: Long,
    val dupSkipped: Long,
    val zenohDupSkipped: Long = 0L,
    val dropped: Map<String, Long>,
    val retainedDiverged: Map<String, Long>,
    val rejected: Long,
    val unknownProps: Long,
    val gapLostTotal: Long,
    val sourceResets: Long,
    val resetLostLowerBound: Long,
    val reconnects: Long,
    val sessions: Long,
    val crcErrors: Long,
    val snapshotFilled: Long,
    val snapshotSkippedPresent: Long,
    val snapshotNewer: Long,
    val snapshotTruncated: Long,
    val snapshots: Long,
    val snapshotsInterrupted: Long,
    val retainedFlushErrors: Long,
    val supersededWillResent: Long,
    val paced: Long,
    val lastError: String,
    val clockSkewMs: Long,
    val rttMs: Double,
    val topicRootMismatch: Boolean,
    val retainedClassMismatch: Boolean,
    val oaRetained: Boolean,
    val applyDelayMs: ApplyDelay
)

data class ApplyDelay(
    val p50: Long,
    val p99: Long,
    @JsonProperty("p99_9") val p999: Long
)
