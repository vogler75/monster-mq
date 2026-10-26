package at.rocworks.handlers

import at.rocworks.Utils
import io.vertx.core.Vertx
import io.vertx.core.internal.VertxInternal
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicLongArray
import java.util.logging.Logger

/**
 * Broker-wide overload detection, used to push back on publishers.
 *
 * The broker counts as saturated when
 * - an event loop lags: its probe task runs, or is due, more than MaxEventLoopLagMs late, which
 *   means the task queue of that event loop has a backlog (e.g. undelivered event-bus messages), or
 * - a bulk processing worker queue is above its high watermark (see PublishWorkerPool).
 *
 * While saturated, MqttClient stops reading from publishing sockets (TCP flow control then slows
 * the publishers down) for at most MaxPauseMs at a time, so keep-alive handling never stalls.
 */
object Backpressure {
    private val logger: Logger = Utils.getLogger(this::class.java)

    private const val PROBE_INTERVAL_MS = 50L
    private const val EVAL_INTERVAL_NANOS = 1_000_000L // re-evaluate at most every 1 ms
    private const val SIGNAL_HOLD_NANOS = 100_000_000L // an external signal keeps the state for 100 ms

    @Volatile var enabled: Boolean = false
        private set
    @Volatile var maxPauseMs: Long = 1000L
        private set
    private var maxLagNanos: Long = TimeUnit.MILLISECONDS.toNanos(500)

    // Per event loop: the time its next probe is due
    private var probeDue = AtomicLongArray(0)
    private val lastSignal = AtomicLong(0)
    @Volatile private var lastEval = 0L
    @Volatile private var saturated = false

    // Statistics
    val pauseCount = AtomicLong(0)
    private var saturatedSince = 0L

    fun start(vertx: Vertx, enabled: Boolean, maxEventLoopLagMs: Long, maxPauseMs: Long) {
        this.enabled = enabled
        this.maxPauseMs = maxPauseMs.coerceAtLeast(10)
        this.maxLagNanos = TimeUnit.MILLISECONDS.toNanos(maxEventLoopLagMs.coerceAtLeast(10))
        if (!enabled) {
            logger.fine("Backpressure disabled")
            return
        }
        val loops = try {
            (vertx as VertxInternal).nettyEventLoopGroup().toList()
        } catch (e: Exception) {
            logger.warning("Backpressure: event loop lag probe not available: ${e.message}")
            emptyList()
        }
        probeDue = AtomicLongArray(loops.size)
        loops.forEachIndexed { index, loop ->
            fun schedule() {
                probeDue.set(index, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(PROBE_INTERVAL_MS))
                loop.schedule({ schedule() }, PROBE_INTERVAL_MS, TimeUnit.MILLISECONDS)
            }
            loop.execute { schedule() }
        }
        logger.fine("Backpressure enabled: MaxEventLoopLagMs [$maxEventLoopLagMs] MaxPauseMs [$maxPauseMs] event loops [${loops.size}]")
    }

    /**
     * Report saturation from outside the event loops (e.g. a full bulk worker queue).
     */
    fun signal() {
        if (enabled) lastSignal.set(System.nanoTime())
    }

    /**
     * True while the broker is overloaded. Cheap enough to be called per inbound PUBLISH.
     */
    fun isSaturated(): Boolean {
        if (!enabled) return false
        val now = System.nanoTime()
        if (now - lastEval < EVAL_INTERVAL_NANOS) return saturated
        lastEval = now

        var lagging = now - lastSignal.get() < SIGNAL_HOLD_NANOS
        if (!lagging) {
            for (i in 0 until probeDue.length()) {
                val due = probeDue.get(i)
                if (due != 0L && now - due > maxLagNanos) {
                    lagging = true
                    break
                }
            }
        }
        if (lagging != saturated) {
            saturated = lagging
            if (lagging) {
                saturatedSince = now
                logger.info("Backpressure: broker saturated, pausing publishers")
            } else {
                logger.info("Backpressure: broker recovered after ${TimeUnit.NANOSECONDS.toMillis(now - saturatedSince)} ms")
            }
        }
        return saturated
    }
}
