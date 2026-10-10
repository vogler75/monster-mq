package at.rocworks.peerlink

import at.rocworks.Utils
import at.rocworks.peerlink.config.PeerConfig
import at.rocworks.peerlink.config.PeerLinkDefaultPort
import at.rocworks.peerlink.config.PeerLinkSnapshotFill
import at.rocworks.peerlink.core.IncludeExclude
import at.rocworks.peerlink.tls.PeerIdentity
import at.rocworks.peerlink.wire.*
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.Socket
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger
import javax.net.ssl.SSLSocket
import kotlin.random.Random

const val STATE_STOPPED = 0
const val STATE_BACKOFF = 1
const val STATE_DIALING = 2
const val STATE_HANDSHAKE = 3
const val STATE_SNAPSHOT = 4
const val STATE_STREAMING = 5

val STATE_NAMES = arrayOf("STOPPED", "BACKOFF", "DIALING", "HANDSHAKE", "SNAPSHOT", "STREAMING")

const val DIAL_TIMEOUT_MS = 5000
const val BACKOFF_INITIAL_MS = 200L
const val BACKOFF_RESET_LIVE_MS = 30_000L
const val CONFIG_ERROR_EVERY_MS = 300_000L

class GoAwayException(
    val code: GoAwayCode,
    val reasonText: String = "",
    val isLocal: Boolean = false
) : Exception(
    "${if (isLocal) "refused source:" else "source sent"} GOAWAY $code${if (reasonText.isNotEmpty()) ": $reasonText" else ""}"
)

class Puller(
    val manager: PeerLinkManager,
    val peer: PeerConfig,
    val tlsPeer: PeerIdentity,
    val secrets: List<ByteArray>,
    val filter: IncludeExclude,
    val injector: Injector,
    val logger: Logger = Utils.getLogger(Puller::class.java)
) {
    val nodeId: String = peer.nodeId
    val address: String = peer.address

    private val running = AtomicBoolean(false)
    private val graceful = AtomicBoolean(false)
    private val stopRequested = AtomicBoolean(false)
    private val resyncReq = AtomicBoolean(false)
    private val snapPending = AtomicBoolean(false)

    private var workerThread: Thread? = null
    private var activeSocket: Socket? = null

    val epoch = AtomicLong(0)
    val appliedNext = AtomicLong(0)
    val lastSeenLeo = AtomicLong(0)
    val state = AtomicInteger(STATE_STOPPED)
    val oaRetained = AtomicBoolean(false)

    private var crcBase: Long = 0L
    private var crcFails: Int = 0

    val gapLost = AtomicLong(0)
    val sourceResets = AtomicLong(0)
    val resetLost = AtomicLong(0)
    val reconnects = AtomicLong(0)
    val sessions = AtomicLong(0)
    val crcErrors = AtomicLong(0)
    val snapTrunc = AtomicLong(0)
    val snaps = AtomicLong(0)
    val snapInterrupted = AtomicLong(0)
    val flushErrors = AtomicLong(0)
    val clockSkewMs = AtomicLong(0)
    val rttUs = AtomicLong(0)
    val topicRootMismatch = AtomicBoolean(false)
    // What the source announced in the last handshake.
    val remoteBroker = java.util.concurrent.atomic.AtomicReference<PeerBroker?>(null)
    val retainedClassMismatch = AtomicBoolean(false)

    @Volatile var lastError: String = ""
    val rate = RateLimiter()

    fun start() {
        if (running.compareAndSet(false, true)) {
            workerThread = Thread.ofVirtual().name("peerlink-puller-$nodeId").start {
                runLoop()
            }
        }
    }

    fun stop(gracefulShutdown: Boolean) {
        graceful.set(gracefulShutdown)
        stopRequested.set(true)
        if (!gracefulShutdown || state.get() != STATE_STREAMING) {
            try {
                activeSocket?.close()
            } catch (_: Exception) {}
        }
        workerThread?.interrupt()
    }

    fun waitStopped(timeoutMs: Long): Boolean {
        val t = workerThread ?: return true
        t.join(timeoutMs)
        return !t.isAlive
    }

    fun requestResync() {
        resyncReq.set(true)
        try {
            activeSocket?.close()
        } catch (_: Exception) {}
    }

    private fun setState(s: Int) {
        val old = state.getAndSet(s)
        if (old != s && (s == STATE_STREAMING || s == STATE_BACKOFF || s == STATE_STOPPED)) {
            manager.onStateChange?.invoke()
        }
    }

    private data class SessionResult(
        var applied: Boolean = false,
        var livedMs: Long = 0L,
        var configErr: Boolean = false,
        var resync: Boolean = false,
        var code: GoAwayCode? = null,
        var error: Throwable? = null
    )

    private fun runLoop() {
        var backoffMs = BACKOFF_INITIAL_MS
        val reconnectMaxMs = manager.config.fetch.getReconnectMaxMs().toLong()
        var first = true

        try {
            while (!stopRequested.get()) {
                if (!first) {
                    reconnects.incrementAndGet()
                }
                first = false

                val res = runSession()
                if (stopRequested.get()) break

                var waitMs = backoffMs
                when {
                    res.resync -> {
                        backoffMs = BACKOFF_INITIAL_MS
                        waitMs = 0L
                    }
                    res.configErr -> {
                        waitMs = reconnectMaxMs
                    }
                    res.applied || res.livedMs >= BACKOFF_RESET_LIVE_MS -> {
                        backoffMs = BACKOFF_INITIAL_MS
                        waitMs = backoffMs
                    }
                    else -> {
                        waitMs = backoffMs
                        backoffMs = minOf(backoffMs * 2, reconnectMaxMs)
                    }
                }

                logResult(res)

                if (waitMs > 0 && !stopRequested.get()) {
                    val jittered = (waitMs.toDouble() * (0.8 + 0.4 * Random.nextDouble())).toLong()
                    setState(STATE_BACKOFF)
                    try {
                        Thread.sleep(jittered)
                    } catch (_: InterruptedException) {
                        break
                    }
                }
            }
        } finally {
            setState(STATE_STOPPED)
            running.set(false)
        }
    }

    private fun logResult(res: SessionResult) {
        if (res.error == null && res.code == null) return
        val msg = res.error?.message ?: ("GOAWAY " + res.code)
        lastError = msg
        when {
            res.configErr -> {
                val (ok, n) = rate.allow("config:" + res.code, CONFIG_ERROR_EVERY_MS)
                if (ok) {
                    logger.severe("peerlink: link refused; check configuration on both nodes [peer=$nodeId, address=$address, code=${res.code}, error=$msg, suppressed=$n]")
                }
            }
            res.code == GoAwayCode.Shutdown || res.code == GoAwayCode.Superseded -> {
                logger.info("peerlink: source closed link [peer=$nodeId, code=${res.code}]")
            }
            else -> {
                val (ok, n) = rate.allow("link", 10_000L)
                if (ok) {
                    logger.warning("peerlink: link down [peer=$nodeId, address=$address, error=$msg, suppressed=$n]")
                }
            }
        }
    }

    private fun parseHostPort(addr: String): Pair<String, Int> {
        val lastColon = addr.lastIndexOf(':')
        if (lastColon <= 0) return Pair(addr, PeerLinkDefaultPort)
        val host = addr.substring(0, lastColon).trim('[', ']')
        val port = addr.substring(lastColon + 1).toIntOrNull() ?: PeerLinkDefaultPort
        return Pair(host, port)
    }

    private data class HandshakeState(
        val socket: Socket,
        val input: InputStream,
        val output: OutputStream,
        val fr: FrameReader,
        val caps: Long,
        val flags: Int,
        val epoch: Long,
        val srcRoot: String,
        val rttHalfMs: Long,
        val cs: SSLSocket?
    )

    private fun runSession(): SessionResult {
        val res = SessionResult()
        val started = System.currentTimeMillis()
        setState(STATE_DIALING)

        val (host, port) = parseHostPort(address)
        val rawSocket = Socket()
        activeSocket = rawSocket

        try {
            rawSocket.connect(InetSocketAddress(host, port), DIAL_TIMEOUT_MS)
            rawSocket.soTimeout = 10_000
            rawSocket.tcpNoDelay = true

            var currentSocket = rawSocket
            var sslSocket: SSLSocket? = null
            if (manager.config.tls.enabled) {
                setState(STATE_HANDSHAKE)
                val wrapped = manager.peerTls?.wrapClientSocket(
                    rawSocket,
                    host,
                    port,
                    peer.tls.insecureSkipVerify,
                    secrets.isNotEmpty()
                ) ?: throw IllegalStateException("TLS enabled but PeerTls is null")
                sslSocket = wrapped
                currentSocket = wrapped
            }
            activeSocket = currentSocket

            setState(STATE_HANDSHAKE)
            val hs = doHandshake(currentSocket, sslSocket)
            sessions.incrementAndGet()

            // With the agreed CapInterest the full interest snapshot goes out before anything else
            // (plan-peerlink-interest-routing section 5.3); later deltas ride ahead of each FETCH.
            val tracker = manager.tracker
            if (tracker != null && (hs.caps and CapInterest) != 0L) {
                val f = tracker.subscribe { interestWake?.invoke() }
                feed = f
                writeInterest(hs.output, f)
            }

            val ac = ApplyContext(
                srcRoot = hs.srcRoot,
                ownRoot = "",
                epoch = hs.epoch,
                mode = SNAP_NONE,
                rttHalf = hs.rttHalfMs
            )

            var mode = SNAP_NONE
            val fillOK = (hs.caps and CapSnapshotFill) != 0L && manager.config.snapshot.effectiveMode() == PeerLinkSnapshotFill
            if (!fillOK || oaRetained.get()) {
                snapPending.set(false)
            } else if ((hs.flags and HelloOKSnapshotAvailable) != 0) {
                snapPending.set(true)
            }

            if (resyncReq.get()) {
                if ((hs.caps and CapResyncNewer) != 0L) {
                    mode = SNAP_NEWER
                } else {
                    resyncReq.set(false)
                    logger.warning("peerlink: resync requested but source does not support it [peer=$nodeId]")
                }
            } else if (snapPending.get()) {
                mode = SNAP_FILL
            }

            if (mode != SNAP_NONE) {
                setState(STATE_SNAPSHOT)
                ac.mode = mode
                try {
                    doSnapshotPhase(hs, ac)
                    if (mode == SNAP_FILL) snapPending.set(false)
                    if (mode == SNAP_NEWER) {
                        resyncReq.set(false)
                        snapPending.set(false)
                    }
                } catch (e: Exception) {
                    snapInterrupted.incrementAndGet()
                    val (ok, n) = rate.allow("snapshot-interrupted", 10_000L)
                    if (ok) {
                        logger.warning("peerlink: retained snapshot interrupted; pulled again on next session [peer=$nodeId, error=${e.message}, suppressed=$n]")
                    }
                    res.error = e
                    if (e is GoAwayException) {
                        res.code = e.code
                        res.configErr = isConfigError(e.code)
                    }
                    res.livedMs = System.currentTimeMillis() - started
                    return res
                }
                ac.mode = SNAP_NONE
            }

            setState(STATE_STREAMING)
            logger.info("peerlink: streaming from source [peer=$nodeId, address=$address, tls=${sslSocket != null}, epoch=${hs.epoch}, resumeAt=${appliedNext.get()}]")
            val streamRes = doStreaming(hs, ac)
            streamRes.livedMs = System.currentTimeMillis() - started
            return streamRes

        } catch (e: Exception) {
            res.error = e
            if (e is GoAwayException) {
                res.code = e.code
                res.configErr = isConfigError(e.code)
                if (e.code == GoAwayCode.OffsetOutOfRange) {
                    epoch.set(0)
                    appliedNext.set(0)
                }
            }
            res.livedMs = System.currentTimeMillis() - started
            return res
        } finally {
            feed?.let { f -> manager.tracker?.unsubscribe(f) }
            feed = null
            interestWake = null
            try {
                currentSocketSafeClose()
            } catch (_: Exception) {}
        }
    }

    // The interest feed of the current session; null without the agreed CapInterest.
    @Volatile private var feed: InterestTracker.Feed? = null
    @Volatile private var interestWake: (() -> Unit)? = null
    private val interestDeltasSent = AtomicLong()
    private val interestSnapshotsSent = AtomicLong()

    private fun interestWanted(): Boolean =
        manager.tracker != null && manager.config.interest.enabled && !peer.interestOff()

    // writeInterest sends the pending interest frames of the feed; only the thread that owns the
    // output may call it.
    private fun writeInterest(out: OutputStream, f: InterestTracker.Feed) {
        val frames = f.poll()
        if (frames.isEmpty()) return
        for (fr in frames) {
            writeFrame(out, fr)
            when (fr) {
                is InterestDelta -> interestDeltasSent.incrementAndGet()
                is InterestSnapshot -> interestSnapshotsSent.incrementAndGet()
                else -> {}
            }
        }
        out.flush()
    }

    private fun currentSocketSafeClose() {
        try {
            activeSocket?.close()
        } catch (_: Exception) {}
        activeSocket = null
    }

    private fun isConfigError(code: GoAwayCode): Boolean = when (code) {
        GoAwayCode.UnknownPeer, GoAwayCode.WrongNode, GoAwayCode.IdentityMismatch,
        GoAwayCode.AuthFailed, GoAwayCode.NotAllowed, GoAwayCode.SelfConnection,
        GoAwayCode.DuplicateNode, GoAwayCode.Version -> true
        else -> false
    }

    private fun doHandshake(socket: Socket, sslSocket: SSLSocket?): HandshakeState {
        val out = BufferedOutputStream(socket.getOutputStream(), 64 shl 10)
        val input = BufferedInputStream(socket.getInputStream(), 64 shl 10)

        writePreamble(out)
        out.flush()

        val fr = FrameReader(input, MaxPreAuthFrame.toLong())
        val (t1, body1) = fr.readFrame()
        if (t1 != FrameType.ServerHello) {
            if (t1 == FrameType.GoAway) {
                val ga = GoAway()
                ga.decode(body1)
                throw GoAwayException(ga.code, ga.reason)
            }
            throw GoAwayException(GoAwayCode.Protocol, "expected ServerHello, got $t1", isLocal = true)
        }
        val sh = ServerHello()
        sh.decode(body1)
        if (sh.versionMajor != VersionMajor) {
            sendGoAway(socket, GoAwayCode.Version, "unsupported version")
            throw GoAwayException(GoAwayCode.Version, "source speaks major version ${sh.versionMajor}", isLocal = true)
        }

        val exporter = if (sslSocket != null && manager.peerTls != null) {
            manager.peerTls!!.exportKeyingMaterial(sslSocket)
        } else null

        val hello = Hello(
            capabilities = CapsV1 or CapSnapshotFill or CapResyncNewer or CapBatchCRC or
                (if (interestWanted()) CapInterest else 0L),
            instanceID = manager.instanceId,
            maxRecordBytes = injector.maxMessageSize,
            retainedClass = RetainedClass.DB,
            nonceC = newNonce(),
            consumerNodeID = manager.nodeId,
            expectedSourceNodeID = nodeId,
            brokerType = BrokerTypeFull,
            brokerVersion = at.rocworks.Version.getVersion(),
            lastEpoch = epoch.get(),
            resumeOffset = appliedNext.get(),
            lastSeenLeo = lastSeenLeo.get()
        )

        if (sslSocket != null && secrets.isNotEmpty() && exporter != null) {
            hello.flags = hello.flags or HelloFlagMAC
            val macIn = consumerMACInput(sh.nonceS, hello.nonceC, manager.nodeId, nodeId, exporter)
            hello.mac = mac(secrets[0], macIn)
        }

        val sent = System.currentTimeMillis()
        writeFrame(out, hello)
        out.flush()

        val (t2, body2) = fr.readFrame()
        if (t2 != FrameType.HelloOK) {
            if (t2 == FrameType.GoAway) {
                val ga = GoAway()
                ga.decode(body2)
                throw GoAwayException(ga.code, ga.reason)
            }
            throw GoAwayException(GoAwayCode.Protocol, "expected HelloOK, got $t2", isLocal = true)
        }
        val ok = HelloOK()
        ok.decode(body2)
        val rtt = System.currentTimeMillis() - sent

        val canonSrc = ok.sourceNodeID.trim().lowercase()
        if (canonSrc == manager.nodeId) {
            sendGoAway(socket, GoAwayCode.SelfConnection, "self connection")
            throw GoAwayException(GoAwayCode.SelfConnection, "source NodeId equals own NodeId", isLocal = true)
        }
        if (canonSrc != nodeId) {
            sendGoAway(socket, GoAwayCode.WrongNode, "wrong node")
            throw GoAwayException(GoAwayCode.WrongNode, "source announced NodeId ${ok.sourceNodeID}", isLocal = true)
        }

        if (sslSocket != null && secrets.isNotEmpty()) {
            if (exporter == null) {
                sendGoAway(socket, GoAwayCode.AuthFailed, "exporter null")
                throw GoAwayException(GoAwayCode.AuthFailed, "exporter unavailable", isLocal = true)
            }
            val expectedIn = sourceMACInput(hello.nonceC, sh.nonceS, nodeId, manager.nodeId, exporter)
            val matched = matchMAC(secrets, expectedIn, ok.macS)
            if (matched < 0) {
                sendGoAway(socket, GoAwayCode.AuthFailed, "source MAC mismatch")
                throw GoAwayException(GoAwayCode.AuthFailed, "source MAC mismatch", isLocal = true)
            }
        }

        val rttUsVal = rtt * 1000L
        rttUs.set(rttUsVal)
        remoteBroker.set(PeerBroker(ok.brokerType, ok.brokerVersion, protocolVersion(sh.versionMajor, sh.versionMinor)))
        val frameMax = maxOf(manager.config.fetch.getMaxBytes(), ok.maxRecordBytes).toLong() + FrameSlack
        fr.max = frameMax

        if (ok.epoch != epoch.get()) {
            if (epoch.get() != 0L) {
                sourceResets.incrementAndGet()
                var lost = 0L
                val seen = lastSeenLeo.get()
                val applied = appliedNext.get()
                if (seen > applied) lost = seen - applied
                resetLost.addAndGet(lost)
                logger.warning("peerlink: source restarted (new epoch) [peer=$nodeId, resetLost=$lost]")
            }
            epoch.set(ok.epoch)
            crcFails = 0
        }
        appliedNext.set(ok.resumeAt)
        lastSeenLeo.set(ok.leo)
        if (ok.lostOnResume > 0L) {
            gapLost.addAndGet(ok.lostOnResume)
            logger.warning("peerlink: records lost before resume [peer=$nodeId, lostOnResume=${ok.lostOnResume}]")
        }

        socket.soTimeout = 0 // handled by frame reading deadlines
        return HandshakeState(
            socket = socket,
            input = input,
            output = out,
            fr = fr,
            caps = ok.capabilities,
            flags = ok.flags,
            epoch = ok.epoch,
            srcRoot = ok.topicRoot,
            rttHalfMs = rtt / 2,
            cs = sslSocket
        )
    }

    private fun sendGoAway(socket: Socket, code: GoAwayCode, reason: String = "") {
        try {
            socket.soTimeout = 1000
            val out = socket.getOutputStream()
            writeFrame(out, GoAway(code, reason))
            out.flush()
        } catch (_: Exception) {}
    }

    private fun doSnapshotPhase(hs: HandshakeState, ac: ApplyContext) {
        snaps.incrementAndGet()
        val keepAliveMs = manager.config.getKeepAliveSeconds().toLong() * 1000L
        var fetchId = 0

        while (!stopRequested.get()) {
            fetchId++
            val f = Fetch(
                fetchID = fetchId,
                flags = FetchFlagSnapshot,
                offset = 0L,
                commit = 0L,
                maxRecords = manager.config.fetch.getMaxRecords(),
                maxBytes = manager.config.fetch.getMaxBytes(),
                minRecords = 1,
                maxWaitMs = 0
            )
            writeFrame(hs.output, f)
            hs.output.flush()

            val (t, body) = hs.fr.readFrame()
            if (t != FrameType.Batch) {
                if (t == FrameType.GoAway) {
                    val ga = GoAway()
                    ga.decode(body)
                    throw GoAwayException(ga.code, ga.reason)
                }
                throw GoAwayException(GoAwayCode.Protocol, "expected BATCH during snapshot, got $t", isLocal = true)
            }
            val b = Batch()
            b.decode(body)

            if (!b.crcValid()) {
                crcErrors.incrementAndGet()
                throw GoAwayException(GoAwayCode.Protocol, "batch CRC mismatch", isLocal = true)
            }

            val inBatch = BatchIn(b, System.currentTimeMillis())
            ac.skewMs = clockSkewMs.get()
            injector.applyBatch(ac, inBatch)

            if ((b.header.flags and BatchFlagSnapshotEnd) != 0) {
                snapTrunc.addAndGet(b.header.lost)
                logger.info("peerlink: retained snapshot applied [peer=$nodeId, filled=${injector.snapFilled.sum()}, newer=${injector.snapNewer.sum()}, truncated=${b.header.lost}]")
                break
            }
        }
    }

    private sealed class WriteCmd {
        data class FetchCmd(val offset: Long) : WriteCmd()
        data class CommitCmd(val offset: Long) : WriteCmd()
        data class FinalCmd(val goAway: Boolean) : WriteCmd()
        object InterestCmd : WriteCmd()
    }

    private fun doStreaming(hs: HandshakeState, ac: ApplyContext): SessionResult {
        val res = SessionResult()
        val pipeline = minOf(maxOf(manager.config.fetch.getPipeline(), 1), 2)
        val handoff = ArrayBlockingQueue<BatchIn>(pipeline)
        val cmds = LinkedBlockingQueue<WriteCmd>(32)
        val outstanding = AtomicInteger(0)
        val lastFetchSent = AtomicLong(0)
        val closed = AtomicBoolean(false)
        val writerDone = CountDownLatch(1)
        // A wake only needs one queued InterestCmd; offer never blocks the tracker worker.
        interestWake = { cmds.offer(WriteCmd.InterestCmd) }

        val writerThread = Thread.ofVirtual().name("puller-writer-$nodeId").start {
            var fetchId = 0
            var lastCommit = 0L
            try {
                while (!closed.get() && !stopRequested.get()) {
                    val cmd = cmds.poll(maxOf(manager.config.getKeepAliveSeconds().toLong() / 2, 1L), TimeUnit.SECONDS)
                    if (cmd == null) {
                        // Ping if no outstanding fetch
                        if (outstanding.get() == 0) {
                            writeFrame(hs.output, Ping(System.nanoTime()))
                            hs.output.flush()
                        }
                        continue
                    }
                    val applied = appliedNext.get()
                    when (cmd) {
                        is WriteCmd.FetchCmd -> {
                            feed?.let { writeInterest(hs.output, it) }
                            fetchId++
                            val commit = if (applied > lastCommit) {
                                lastCommit = applied
                                applied
                            } else 0L
                            outstanding.incrementAndGet()
                            lastFetchSent.set(System.currentTimeMillis())
                            val f = Fetch(
                                fetchID = fetchId,
                                offset = cmd.offset,
                                commit = commit,
                                maxRecords = manager.config.fetch.getMaxRecords(),
                                maxBytes = manager.config.fetch.getMaxBytes(),
                                minRecords = 1,
                                maxWaitMs = manager.config.fetch.getMaxWaitMs(),
                                lingerMs = manager.config.fetch.lingerMs
                            )
                            writeFrame(hs.output, f)
                            hs.output.flush()
                        }
                        is WriteCmd.InterestCmd -> {
                            feed?.let { writeInterest(hs.output, it) }
                        }
                        is WriteCmd.CommitCmd -> {
                            if (cmd.offset > lastCommit) {
                                lastCommit = cmd.offset
                                writeFrame(hs.output, Commit(cmd.offset))
                                hs.output.flush()
                            }
                        }
                        is WriteCmd.FinalCmd -> {
                            if (applied > lastCommit) {
                                writeFrame(hs.output, Commit(applied))
                            }
                            if (cmd.goAway) {
                                writeFrame(hs.output, GoAway(GoAwayCode.Shutdown, "shutdown"))
                            }
                            hs.output.flush()
                            break
                        }
                    }
                }
            } catch (e: Exception) {
                if (!closed.get()) res.error = e
            } finally {
                writerDone.countDown()
            }
        }

        // Reader loop
        val readerThread = Thread.ofVirtual().name("puller-reader-$nodeId").start {
            var next = appliedNext.get()
            cmds.offer(WriteCmd.FetchCmd(next))

            try {
                while (!closed.get() && !stopRequested.get()) {
                    val (t, body) = hs.fr.readFrame()
                    if (t == FrameType.Batch) {
                        outstanding.decrementAndGet()
                        val b = Batch()
                        b.decode(body)
                        val inBatch = BatchIn(b, System.currentTimeMillis())

                        if (!b.crcValid()) {
                            crcErrors.incrementAndGet()
                            crcFails++
                            if (crcFails >= 3) {
                                inBatch.poison = true
                                crcFails = 0
                            } else {
                                throw GoAwayException(GoAwayCode.Protocol, "batch CRC mismatch", isLocal = true)
                            }
                        } else {
                            crcFails = 0
                        }

                        val h = b.header
                        lastSeenLeo.set(h.leo)
                        injector.batches.increment()
                        if (h.lost > 0L) {
                            gapLost.addAndGet(h.lost)
                            val (ok, n) = rate.allow("gap", 10_000L)
                            if (ok) {
                                logger.warning("peerlink: records lost [peer=$nodeId, lost=${h.lost}, baseOffset=${h.baseOffset}, suppressed=$n]")
                            }
                        }

                        if (b.isSparse() && (hs.caps and CapInterest) == 0L) {
                            throw GoAwayException(GoAwayCode.Protocol, "sparse BATCH without CapInterest", isLocal = true)
                        }
                        // A sparse batch covers span offsets, not count (plan-peerlink-interest-routing 6.4).
                        next = if (h.baseOffset != 0L) h.baseOffset + b.span() else next
                        handoff.put(inBatch)
                        cmds.offer(WriteCmd.FetchCmd(next))
                    } else if (t == FrameType.Pong) {
                        // Handled
                    } else if (t == FrameType.GoAway) {
                        val ga = GoAway()
                        ga.decode(body)
                        throw GoAwayException(ga.code, ga.reason)
                    }
                }
            } catch (e: Exception) {
                if (!closed.get()) {
                    res.error = e
                    if (e is GoAwayException) {
                        res.code = e.code
                        res.configErr = isConfigError(e.code)
                    }
                }
            } finally {
                closed.set(true)
            }
        }

        // Injector loop runs on this thread
        ac.commitFn = { off ->
            if (off > appliedNext.get()) {
                appliedNext.set(off)
            }
            cmds.offer(WriteCmd.CommitCmd(off))
        }

        try {
            while (!closed.get() && !stopRequested.get()) {
                val inBatch = handoff.poll(100, TimeUnit.MILLISECONDS) ?: continue
                ac.rttHalf = rttUs.get() / 2000L
                ac.skewMs = clockSkewMs.get()
                injector.pace.observe(inBatch.batch.header.leo, inBatch.batch.header.sourceMonoMs)
                val next = injector.applyBatch(ac, inBatch)
                if (next > appliedNext.get()) {
                    appliedNext.set(next)
                    res.applied = true
                    cmds.offer(WriteCmd.CommitCmd(next))
                }
            }
        } finally {
            closed.set(true)
            if (graceful.get()) {
                cmds.offer(WriteCmd.FinalCmd(goAway = true))
                writerDone.await(2, TimeUnit.SECONDS)
            } else {
                cmds.offer(WriteCmd.FinalCmd(goAway = false))
            }
            try {
                hs.socket.close()
            } catch (_: Exception) {}
            readerThread.join(1000)
            writerThread.join(1000)
        }

        return res
    }

    fun status(): SourceStatus {
        val sLeo = lastSeenLeo.get()
        val aNext = appliedNext.get()
        val lag = if (sLeo > aNext) sLeo - aNext else 0L

        val droppedMap = mutableMapOf<String, Long>()
        for (i in 0 until NUM_DROP_REASONS) {
            droppedMap[DROP_NAMES[i]] = injector.dropped[i].sum()
        }
        val divMap = mutableMapOf<String, Long>()
        for (i in 0 until NUM_DIV_REASONS) {
            divMap[DIV_NAMES[i]] = injector.diverged[i].sum()
        }

        return SourceStatus(
            nodeId = nodeId,
            address = address,
            state = STATE_NAMES[state.get()],
            epoch = epoch.get(),
            appliedNext = aNext,
            sourceLeo = sLeo,
            lagRecords = lag,
            batches = injector.batches.sum(),
            injected = injector.injected.sum(),
            retainOnly = injector.retainOnly.sum(),
            appliedBytes = injector.appliedBytes.sum(),
            dupSkipped = injector.dupSkipped.sum(),
            zenohDupSkipped = injector.zenohDupSkipped.sum(),
            dropped = droppedMap,
            retainedDiverged = divMap,
            rejected = injector.rejected.sum(),
            unknownProps = injector.unknownProps.sum(),
            gapLostTotal = gapLost.get(),
            sourceResets = sourceResets.get(),
            resetLostLowerBound = resetLost.get(),
            reconnects = reconnects.get(),
            sessions = sessions.get(),
            crcErrors = crcErrors.get(),
            snapshotFilled = injector.snapFilled.sum(),
            snapshotSkippedPresent = injector.snapSkipped.sum(),
            snapshotNewer = injector.snapNewer.sum(),
            snapshotTruncated = snapTrunc.get(),
            snapshots = snaps.get(),
            snapshotsInterrupted = snapInterrupted.get(),
            retainedFlushErrors = flushErrors.get(),
            supersededWillResent = injector.willResent.sum(),
            paced = injector.paced.sum(),
            lastError = lastError,
            clockSkewMs = clockSkewMs.get(),
            rttMs = rttUs.get().toDouble() / 1000.0,
            topicRootMismatch = topicRootMismatch.get(),
            retainedClassMismatch = retainedClassMismatch.get(),
            oaRetained = oaRetained.get(),
            applyDelayMs = ApplyDelay(
                p50 = injector.hist.quantile(0.5),
                p99 = injector.hist.quantile(0.99),
                p999 = injector.hist.quantile(0.999)
            ),
            interest = if (interestWanted()) SourceInterest(active = feed != null,
                deltasSent = interestDeltasSent.get(), snapshotsSent = interestSnapshotsSent.get()) else null,
            peerBrokerType = remoteBroker.get()?.type.orEmpty(),
            peerBrokerVersion = remoteBroker.get()?.version.orEmpty(),
            peerProtocolVersion = remoteBroker.get()?.protocol.orEmpty()
        )
    }
}
