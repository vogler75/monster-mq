package at.rocworks.peerlink

import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.peerlink.core.LogConsumerStats
import at.rocworks.peerlink.core.LogWaiter
import at.rocworks.peerlink.tls.PeerIdentity
import at.rocworks.peerlink.wire.*
import com.fasterxml.jackson.databind.ObjectMapper
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import java.util.logging.Logger
import javax.net.ssl.SSLSocket

class CidrBlock(val prefix: InetAddress, val prefixLength: Int) {
    fun contains(address: InetAddress): Boolean {
        val aBytes = address.address
        val pBytes = prefix.address
        if (aBytes.size != pBytes.size) return false
        var bits = prefixLength
        for (i in aBytes.indices) {
            if (bits >= 8) {
                if (aBytes[i] != pBytes[i]) return false
                bits -= 8
            } else if (bits > 0) {
                val mask = (0xFF shl (8 - bits)) and 0xFF
                if ((aBytes[i].toInt() and mask) != (pBytes[i].toInt() and mask)) return false
                bits = 0
            } else {
                break
            }
        }
        return true
    }

    companion object {
        fun parse(cidr: String): CidrBlock {
            val parts = cidr.split('/')
            val addr = InetAddress.getByName(parts[0])
            val len = if (parts.size > 1) parts[1].toInt() else (if (addr.address.size == 4) 32 else 128)
            return CidrBlock(addr, len)
        }
    }
}

// host:port like the edge broker's net.Conn.RemoteAddr, not InetSocketAddress's "/host:port".
fun remoteString(addr: java.net.SocketAddress?): String {
    val a = addr as? InetSocketAddress ?: return addr?.toString() ?: ""
    val host = a.address?.hostAddress ?: a.hostString
    return if (host.contains(':')) "[$host]:${a.port}" else "$host:${a.port}"
}

class ConsumerSlot(
    val idx: Int,
    val nodeId: String,
    val peer: at.rocworks.peerlink.config.PeerConfig,
    val tlsPeer: PeerIdentity,
    val secrets: List<ByteArray>
) {
    val lock = ReentrantLock()
    var active: ServerSession? = null
    val takeovers = mutableListOf<Takeover>()
    val refused = mutableMapOf<Long, Long>() // instance -> untilMs
    var remote: String = ""
    val authFailures = mutableMapOf<String, Long>()
    var warnedRoot: Boolean = false
    var warnedClass: Boolean = false
    var logConnected: Boolean = false

    val lastFetch = AtomicLong(0)
    val sessions = AtomicLong(0)
    val servedRecords = AtomicLong(0)
    val servedBytes = AtomicLong(0)
    val servedSkipped = AtomicLong(0)
    val snapshotServed = AtomicLong(0)
    val duplicateConsumer = AtomicLong(0)
    val shutdownUnserved = AtomicLong(0)
    val oaRetained = AtomicBoolean(false)
    val topicRootMismatch = AtomicBoolean(false)
    val retainedClassMismatch = AtomicBoolean(false)

    data class Takeover(val atMs: Long, val from: Long, val to: Long)

    fun admit(sess: ServerSession, nowMs: Long): Pair<ServerSession?, Boolean> {
        lock.lock()
        try {
            refused.entries.removeIf { nowMs > it.value }
            if (refused.containsKey(sess.instance)) {
                return Pair(null, true)
            }
            var old: ServerSession? = null
            if (active != null) {
                takeovers.add(Takeover(nowMs, active!!.instance, sess.instance))
                takeovers.removeIf { nowMs - it.atMs > 60_000L }
                if (isAlternating(takeovers)) {
                    refused[sess.instance] = nowMs + 300_000L // 5 min refusal
                    duplicateConsumer.incrementAndGet()
                    takeovers.clear()
                    return Pair(null, true)
                }
                old = active
            }
            active = sess
            remote = sess.remote
            return Pair(old, false)
        } finally {
            lock.unlock()
        }
    }

    private fun isAlternating(ts: List<Takeover>): Boolean {
        var n = 0
        var a = 0L
        var b = 0L
        for (t in ts) {
            if (t.from == t.to) continue
            for (id in longArrayOf(t.from, t.to)) {
                when {
                    a == 0L || a == id -> a = id
                    b == 0L || b == id -> b = id
                    else -> return false
                }
            }
            n++
        }
        return n >= 3 && a != 0L && b != 0L
    }

    fun release(log: at.rocworks.peerlink.core.PeerLog, sess: ServerSession): Boolean {
        lock.lock()
        try {
            if (active != sess) return false
            active = null
            if (logConnected) {
                logConnected = false
                log.setConsumerState(idx, at.rocworks.peerlink.core.LogConsumerState.Disconnected)
            }
            return true
        } finally {
            lock.unlock()
        }
    }

    fun connected(log: at.rocworks.peerlink.core.PeerLog, sess: ServerSession): Boolean {
        lock.lock()
        try {
            if (active != sess) return false
            logConnected = true
            log.setConsumerState(idx, at.rocworks.peerlink.core.LogConsumerState.Connected)
            return true
        } finally {
            lock.unlock()
        }
    }

    fun countAuthFailure(code: GoAwayCode) {
        lock.lock()
        try {
            authFailures[code.name] = (authFailures[code.name] ?: 0L) + 1L
        } finally {
            lock.unlock()
        }
    }

    fun status(ls: LogConsumerStats): ConsumerStatus {
        val failures = mutableMapOf<String, Long>()
        var rem: String
        lock.lock()
        try {
            rem = remote
            failures.putAll(authFailures)
        } finally {
            lock.unlock()
        }
        val lastF = lastFetch.get()
        val lastFStr = if (lastF > 0) java.time.Instant.ofEpochMilli(lastF).toString() else null
        return ConsumerStatus(
            nodeId = nodeId,
            state = ls.state.toString(),
            remote = rem,
            committed = ls.committed,
            served = ls.served,
            lag = ls.lag,
            lostTotal = ls.lostTotal,
            servedRecords = servedRecords.get(),
            servedBytes = servedBytes.get(),
            servedSkipped = mapOf("size" to servedSkipped.get()),
            snapshotServed = snapshotServed.get(),
            sessions = sessions.get(),
            duplicateConsumer = duplicateConsumer.get(),
            authFailures = failures,
            lastFetch = lastFStr,
            shutdownUnserved = shutdownUnserved.get(),
            oaRetained = oaRetained.get(),
            topicRootMismatch = topicRootMismatch.get(),
            retainedClassMismatch = retainedClassMismatch.get()
        )
    }
}

class ServerSession(
    val server: PeerServer,
    val slot: ConsumerSlot,
    val socket: Socket,
    val input: InputStream,
    val output: OutputStream,
    val remote: String,
    val instance: Long,
    val caps: Long,
    val maxRec: Long,
    val keepAliveMs: Long,
    val snapOK: Boolean
) {
    val fetches = LinkedBlockingQueue<Fetch>(16)
    val deltas = at.rocworks.peerlink.core.IntList()
    val waiter = LogWaiter()
    val closed = AtomicBoolean(false)
    val writeLock = ReentrantLock()

    fun goAway(code: GoAwayCode, reason: String = "") {
        if (closed.compareAndSet(false, true)) {
            try {
                writeLock.lock()
                try {
                    socket.soTimeout = 1000
                    writeFrame(output, GoAway(code, reason))
                    output.flush()
                } finally {
                    writeLock.unlock()
                }
            } catch (_: Exception) {}
            try {
                socket.close()
            } catch (_: Exception) {}
        }
    }

    fun close() {
        if (closed.compareAndSet(false, true)) {
            try {
                socket.close()
            } catch (_: Exception) {}
        }
    }
}

class PeerServer(
    val manager: PeerLinkManager,
    val logger: Logger = Utils.getLogger(PeerServer::class.java)
) {
    private var serverSocket: ServerSocket? = null
    private val running = AtomicBoolean(false)
    private val allowedNets = mutableListOf<CidrBlock>()
    private val consumers = mutableListOf<ConsumerSlot>()
    private val consumerById = mutableMapOf<String, ConsumerSlot>()
    private val peerById = mutableMapOf<String, at.rocworks.peerlink.config.PeerConfig>()

    private val admLock = ReentrantLock()
    private val preAuthIp = mutableMapOf<String, Int>()
    private var preAuthAll = 0
    private val maxPerIp = manager.config.listener.getMaxPreAuthPerIp()
    private val peerIps = mutableSetOf<String>()
    private val authedIps = mutableMapOf<String, Long>()

    val accepted = AtomicLong(0)
    val refusedNetwork = AtomicLong(0)
    val refusedBusy = AtomicLong(0)
    val refusedSniff = AtomicLong(0)
    val refusedPlaintext = AtomicLong(0)
    val refusedHttp = AtomicLong(0)
    val tlsFailures = AtomicLong(0)
    val authFailAll = mutableMapOf<String, Long>()
    val authFailLock = ReentrantLock()

    private val rate = RateLimiter()
    private val jsonMapper = ObjectMapper()

    fun initSlots() {
        for (n in manager.config.listener.allowedNetworks) {
            try {
                allowedNets.add(CidrBlock.parse(n))
            } catch (e: Exception) {
                logger.warning("Invalid allowedNetwork CIDR: $n (${e.message})")
            }
        }
        var idx = 0
        for (p in manager.config.peers) {
            val cid = p.nodeId.trim().lowercase()
            peerById[cid] = p
            if (p.getServe()) {
                val tp = PeerIdentity(
                    nodeId = cid,
                    certificateIdentity = p.tls.certificateIdentity,
                    pins = at.rocworks.peerlink.tls.parsePins(p.tls.pinnedSha256)
                )
                val secrets = at.rocworks.peerlink.tls.decodeSecrets(p.sharedSecrets.ifEmpty { manager.config.sharedSecrets })
                val slot = ConsumerSlot(idx++, cid, p, tp, secrets)
                consumers.add(slot)
                consumerById[cid] = slot
            }
        }
    }

    fun start() {
        if (running.compareAndSet(false, true)) {
            val port = manager.config.listener.effectivePort()
            val host = manager.config.listener.listenAddress()
            val ss = ServerSocket()
            ss.reuseAddress = true
            ss.bind(InetSocketAddress(host, port), 256)
            serverSocket = ss
            logger.info("peerlink: listening on $host:$port")

            Thread.ofVirtual().name("peerlink-accept").start {
                acceptLoop(ss)
            }
        }
    }

    fun localPort(): Int = serverSocket?.localPort ?: 0

    fun stop() {
        running.set(false)
        try {
            serverSocket?.close()
        } catch (_: Exception) {}
    }

    private fun acceptLoop(ss: ServerSocket) {
        while (running.get()) {
            val s = try {
                ss.accept()
            } catch (e: Exception) {
                if (!running.get()) break
                continue
            }
            Thread.ofVirtual().name("peerlink-conn").start {
                handleConnection(s)
            }
        }
    }

    private fun handleConnection(socket: Socket) {
        val ip = socket.inetAddress
        if (!isAllowed(ip)) {
            refusedNetwork.incrementAndGet()
            try { socket.close() } catch (_: Exception) {}
            return
        }

        val ipKey = ip.hostAddress
        if (!preAuthAcquire(ipKey)) {
            refusedBusy.incrementAndGet()
            try { socket.close() } catch (_: Exception) {}
            return
        }
        accepted.incrementAndGet()

        try {
            socket.soTimeout = 3000
            val input = BufferedInputStream(socket.getInputStream(), 64 shl 10)
            input.mark(2)
            val first = input.read()
            if (first < 0) {
                socket.close()
                return
            }
            input.reset()

            when (first) {
                0x16 -> {
                    // TLS
                    if (manager.peerTls == null) {
                        refusedSniff.incrementAndGet()
                        socket.close()
                        return
                    }
                    val tlsSocket = try {
                        // The sniff buffered more than the first byte (typically the whole ClientHello);
                        // hand all of it to TLS, otherwise the handshake waits for bytes that never come.
                        manager.peerTls!!.wrapServerSocket(socket, input.readNBytes(input.available()))
                    } catch (e: Exception) {
                        tlsFailures.incrementAndGet()
                        val (ok, n) = rate.allow("tls:$ipKey", 10_000L)
                        if (ok) logger.warning("peerlink: TLS handshake failed [remote=$ipKey, error=$e, suppressed=$n]")
                        socket.close()
                        return
                    }
                    val alpn = tlsSocket.applicationProtocol ?: ALPN
                    if (alpn == "http/1.1") {
                        // Check client certificate
                        val certs = try { tlsSocket.session.peerCertificates } catch (_: Exception) { null }
                        val verified = certs != null && certs.isNotEmpty()
                        if (!verified && !manager.config.allowUnauthenticatedPeers) {
                            refusedHttp.incrementAndGet()
                            tlsSocket.close()
                            return
                        }
                        serveHTTP(tlsSocket, tlsSocket.inputStream, loopback = false)
                        return
                    }
                    servePeer(tlsSocket, isTls = true)
                }
                'M'.code -> {
                    if (manager.config.tls.enabled && !manager.config.listener.allowPlaintext) {
                        refusedPlaintext.incrementAndGet()
                        socket.close()
                        return
                    }
                    servePeer(socket, isTls = false, existingInput = input)
                }
                'G'.code, 'P'.code -> {
                    if (!ip.isLoopbackAddress) {
                        refusedHttp.incrementAndGet()
                        socket.close()
                        return
                    }
                    serveHTTP(socket, input, loopback = true)
                }
                else -> {
                    refusedSniff.incrementAndGet()
                    socket.close()
                }
            }
        } catch (e: Throwable) {
            // Without this log a failed peer session only shows up as EOF on the consumer.
            val (ok, n) = rate.allow("conn:$ipKey", 10_000L)
            if (ok) logger.log(java.util.logging.Level.WARNING, "peerlink: connection from $ipKey failed [error=$e, suppressed=$n]", e)
            try { socket.close() } catch (_: Exception) {}
        } finally {
            preAuthRelease(ipKey)
        }
    }

    private fun isAllowed(ip: InetAddress): Boolean {
        if (allowedNets.isEmpty()) return true
        for (n in allowedNets) {
            if (n.contains(ip)) return true
        }
        return false
    }

    private fun preAuthAcquire(ipKey: String): Boolean {
        admLock.lock()
        try {
            val count = preAuthIp[ipKey] ?: 0
            if (count >= maxPerIp) return false
            if (preAuthAll >= 16 && !peerIps.contains(ipKey) && !authedRecent(ipKey)) return false
            preAuthIp[ipKey] = count + 1
            preAuthAll++
            return true
        } finally {
            admLock.unlock()
        }
    }

    private fun preAuthRelease(ipKey: String) {
        admLock.lock()
        try {
            val count = preAuthIp[ipKey] ?: 0
            if (count <= 1) preAuthIp.remove(ipKey) else preAuthIp[ipKey] = count - 1
            preAuthAll--
        } finally {
            admLock.unlock()
        }
    }

    private fun authedRecent(ipKey: String): Boolean {
        val t = authedIps[ipKey] ?: return false
        return System.currentTimeMillis() - t < 24 * 3600 * 1000L
    }

    private fun rememberAuthed(ipKey: String) {
        admLock.lock()
        try {
            authedIps[ipKey] = System.currentTimeMillis()
        } finally {
            admLock.unlock()
        }
    }

    private fun serveHTTP(socket: Socket, input: InputStream, loopback: Boolean) {
        try {
            socket.soTimeout = 5000
            val line = readAsciiLine(input)
            val parts = line.split(" ")
            if (parts.size < 2) return

            val method = parts[0]
            val pathWithQuery = parts[1]
            val path = if (pathWithQuery.contains('?')) pathWithQuery.substring(0, pathWithQuery.indexOf('?')) else pathWithQuery

            // Headers
            val headers = mutableMapOf<String, String>()
            while (true) {
                val h = readAsciiLine(input)
                if (h.isEmpty()) break
                val colon = h.indexOf(':')
                if (colon > 0) {
                    headers[h.substring(0, colon).trim().lowercase()] = h.substring(colon + 1).trim()
                }
            }

            if (loopback) {
                val origin = headers["origin"]
                val host = headers["host"] ?: ""
                if (!origin.isNullOrEmpty() || !isLoopbackHost(host)) {
                    sendHttpResponse(socket, 403, "{\"error\":\"loopback tools only: Host must be a loopback address and Origin absent\"}")
                    return
                }
            }

            when (path) {
                "/peerlink/v1/status" -> {
                    if (method != "GET") {
                        sendHttpResponse(socket, 405, "{\"error\":\"GET only\"}")
                        return
                    }
                    val st = manager.status()
                    val body = jsonMapper.writeValueAsString(st)
                    sendHttpResponse(socket, 200, body)
                }
                "/peerlink/v1/resync" -> {
                    if (!loopback) {
                        sendHttpResponse(socket, 403, "{\"error\":\"resync is loopback only\"}")
                        return
                    }
                    if (method != "POST") {
                        sendHttpResponse(socket, 405, "{\"error\":\"POST only\"}")
                        return
                    }
                    val query = if (pathWithQuery.contains('?')) pathWithQuery.substring(pathWithQuery.indexOf('?') + 1) else ""
                    val source = query.split("&").mapNotNull {
                        val kv = it.split("=")
                        if (kv.size == 2 && kv[0] == "source") kv[1].lowercase() else null
                    }.firstOrNull() ?: ""

                    if (source.isEmpty()) {
                        sendHttpResponse(socket, 400, "{\"error\":\"missing source query param\"}")
                        return
                    }
                    manager.resync(source)
                    sendHttpResponse(socket, 202, "{\"source\":\"$source\",\"mode\":\"NEWER\",\"status\":\"requested\"}")
                }
                else -> {
                    sendHttpResponse(socket, 404, "{\"error\":\"no handler for $path\"}")
                }
            }
        } catch (_: Exception) {}
        finally {
            try { socket.close() } catch (_: Exception) {}
        }
    }

    private fun isLoopbackHost(host: String): Boolean {
        var h = host
        if (h.contains(':')) h = h.substring(0, h.indexOf(':'))
        h = h.trim('[', ']').lowercase()
        if (h.isEmpty() || h == "localhost") return true
        return try {
            InetAddress.getByName(h).isLoopbackAddress
        } catch (_: Exception) {
            false
        }
    }

    private fun readAsciiLine(input: InputStream): String {
        val sb = StringBuilder()
        while (true) {
            val c = input.read()
            if (c < 0 || c == '\n'.code) break
            if (c != '\r'.code) sb.append(c.toChar())
        }
        return sb.toString()
    }

    private fun sendHttpResponse(socket: Socket, code: Int, body: String) {
        val b = body.toByteArray(StandardCharsets.UTF_8)
        val out = socket.getOutputStream()
        val statusText = when (code) {
            200 -> "OK"
            202 -> "Accepted"
            400 -> "Bad Request"
            403 -> "Forbidden"
            404 -> "Not Found"
            405 -> "Method Not Allowed"
            else -> "Error"
        }
        val header = "HTTP/1.1 $code $statusText\r\n" +
                "Content-Type: application/json\r\n" +
                "Content-Length: ${b.size}\r\n" +
                "Connection: close\r\n\r\n"
        out.write(header.toByteArray(StandardCharsets.ISO_8859_1))
        out.write(b)
        out.flush()
    }

    private fun servePeer(socket: Socket, isTls: Boolean, existingInput: InputStream? = null) {
        socket.soTimeout = 10_000
        val input = existingInput ?: BufferedInputStream(socket.getInputStream(), 64 shl 10)
        val out = BufferedOutputStream(socket.getOutputStream(), 64 shl 10)

        val (major, _) = readPreamble(input)
        if (major != VersionMajor) {
            writeFrame(out, GoAway(GoAwayCode.Version, "unsupported version"))
            out.flush()
            socket.close()
            return
        }

        val shNonce = newNonce()
        // CapInterest is offered before the peer is known and dropped below for a peer with Interest OFF.
        var ownCaps = CapsV1 or CapSnapshotFill or CapResyncNewer or CapBatchCRC
        if (manager.interest != null) ownCaps = ownCaps or CapInterest
        var authModes: Byte = 0
        if (isTls && manager.config.tls.clientAuth != at.rocworks.peerlink.config.ClientAuthType.NONE) {
            authModes = (authModes.toInt() or AuthClientCertRequested.toInt()).toByte()
        }
        for (c in consumers) {
            if (c.secrets.isNotEmpty()) {
                authModes = (authModes.toInt() or AuthSharedSecret.toInt()).toByte()
                break
            }
        }
        val sh = ServerHello(
            versionMajor = VersionMajor,
            versionMinor = VersionMinor,
            capabilities = ownCaps,
            authModes = authModes,
            nonceS = shNonce
        )
        writeFrame(out, sh)
        out.flush()

        val fr = FrameReader(input, MaxPreAuthFrame.toLong())
        val (t, body) = fr.readFrame()
        if (t != FrameType.Hello) {
            writeFrame(out, GoAway(GoAwayCode.Protocol, "expected HELLO"))
            out.flush()
            socket.close()
            return
        }
        val hello = Hello()
        hello.decode(body)

        val cid = hello.consumerNodeID.trim().lowercase()
        if (cid == manager.nodeId) {
            refuse(out, socket, null, GoAwayCode.SelfConnection, "self connection", cid)
            return
        }
        val slot = consumerById[cid]
        if (slot == null) {
            val code = if (peerById.containsKey(cid)) GoAwayCode.NotAllowed else GoAwayCode.UnknownPeer
            refuse(out, socket, null, code, "not configured consumer", cid)
            return
        }

        val exp = hello.expectedSourceNodeID.trim().lowercase()
        if (exp != manager.nodeId) {
            refuse(out, socket, slot, GoAwayCode.WrongNode, "expected source $exp", cid)
            return
        }

        var certAuth = false
        val sslSocket = socket as? SSLSocket
        if (sslSocket != null) {
            val certs = try { sslSocket.session.peerCertificates } catch (_: Exception) { null }
            val x509Certs = certs?.filterIsInstance<java.security.cert.X509Certificate>()?.toTypedArray()
            if (x509Certs != null && x509Certs.isNotEmpty()) {
                val verified = manager.peerTls?.verify(x509Certs, slot.tlsPeer) ?: false
                if (!verified) {
                    refuse(out, socket, slot, GoAwayCode.IdentityMismatch, "cert identity mismatch", cid)
                    return
                }
                certAuth = true
            } else if (slot.peer.tls.requireClientCert) {
                refuse(out, socket, slot, GoAwayCode.IdentityMismatch, "client cert required", cid)
                return
            }
        }

        var macAuth = false
        var matchedSecretIdx = -1
        if (slot.secrets.isNotEmpty() && sslSocket != null) {
            if ((hello.flags and HelloFlagMAC) == 0) {
                refuse(out, socket, slot, GoAwayCode.AuthFailed, "MAC required", cid)
                return
            }
            val exporter = try { manager.peerTls?.exportKeyingMaterial(sslSocket) } catch (_: Exception) { null }
            if (exporter == null) {
                refuse(out, socket, slot, GoAwayCode.AuthFailed, "exporter failed", cid)
                return
            }
            val macIn = consumerMACInput(shNonce, hello.nonceC, cid, manager.nodeId, exporter)
            matchedSecretIdx = matchMAC(slot.secrets, macIn, hello.mac)
            if (matchedSecretIdx < 0) {
                refuse(out, socket, slot, GoAwayCode.AuthFailed, "MAC mismatch", cid)
                return
            }
            macAuth = true
        }

        if (!certAuth && !macAuth && !manager.config.allowUnauthenticatedPeers) {
            refuse(out, socket, slot, GoAwayCode.AuthFailed, "peer not authenticated", cid)
            return
        }

        if (slot.peer.interestOff()) ownCaps = ownCaps and CapInterest.inv()

        val remote = remoteString(socket.remoteSocketAddress)
        val sess = ServerSession(
            server = this,
            slot = slot,
            socket = socket,
            input = input,
            output = out,
            remote = remote,
            instance = hello.instanceID,
            caps = ownCaps and hello.capabilities,
            maxRec = hello.maxRecordBytes.toLong(),
            keepAliveMs = manager.config.getKeepAliveSeconds().toLong() * 1000L,
            snapOK = (ownCaps and hello.capabilities and (CapSnapshotFill or CapResyncNewer)) != 0L
        )

        val (old, dup) = slot.admit(sess, System.currentTimeMillis())
        if (dup) {
            refuse(out, socket, slot, GoAwayCode.DuplicateNode, "duplicate NodeId", cid)
            return
        }
        old?.goAway(GoAwayCode.Superseded, "newer session took over")

        val resume = try {
            manager.log.resume(slot.idx, hello.lastEpoch, hello.resumeOffset)
        } catch (e: Exception) {
            slot.release(manager.log, sess)
            refuse(out, socket, slot, GoAwayCode.OffsetOutOfRange, "resume offset beyond log end", cid)
            return
        }

        val ok = HelloOK(
            capabilities = sess.caps,
            epoch = manager.log.epoch,
            resumeAt = resume.resumeAt,
            logStart = resume.lso,
            leo = resume.leo,
            committed = resume.committed,
            lostOnResume = resume.lostOnResume,
            wallNowMs = System.currentTimeMillis(),
            monoNowMs = manager.log.monoMs(System.nanoTime()),
            maxRecordBytes = manager.log.maxRecordBytes,
            retainedClass = RetainedClass.DB,
            sourceNodeID = manager.nodeId,
            topicRoot = "",
            oaSystem = ""
        )
        if (resume.sourceReset) ok.flags = ok.flags or HelloOKSourceReset
        if (resume.consumerStateUsed) ok.flags = ok.flags or HelloOKConsumerStateUsed
        if ((hello.lastEpoch == 0L || resume.sourceReset) && (sess.caps and CapSnapshotFill) != 0L && sess.snapOK) {
            ok.flags = ok.flags or HelloOKSnapshotAvailable
        }

        if (matchedSecretIdx >= 0 && sslSocket != null) {
            val exporter = manager.peerTls?.exportKeyingMaterial(sslSocket)
            if (exporter != null) {
                val macIn = sourceMACInput(hello.nonceC, shNonce, manager.nodeId, cid, exporter)
                ok.macS = mac(slot.secrets[matchedSecretIdx], macIn)
            }
        }

        writeFrame(out, ok)
        out.flush()
        socket.soTimeout = 0
        rememberAuthed(socket.inetAddress.hostAddress)

        if (!slot.connected(manager.log, sess)) {
            sess.close()
            return
        }

        manager.interest?.connect(slot.idx, hello.instanceID, (sess.caps and CapInterest) != 0L)
        slot.sessions.incrementAndGet()
        logger.info("peerlink: consumer connected [peer=$cid, remote=$remote, tls=$isTls, resumeAt=${resume.resumeAt}]")
        manager.onStateChange?.invoke()

        runSessionLoops(sess)

        if (slot.release(manager.log, sess)) {
            manager.interest?.disconnect(slot.idx)
            logger.info("peerlink: consumer disconnected [peer=$cid, remote=$remote]")
            manager.onStateChange?.invoke()
        }
    }

    private fun refuse(out: OutputStream, socket: Socket, slot: ConsumerSlot?, code: GoAwayCode, reason: String, cid: String) {
        slot?.countAuthFailure(code)
        authFailLock.lock()
        try {
            authFailAll[code.name] = (authFailAll[code.name] ?: 0L) + 1L
        } finally {
            authFailLock.unlock()
        }
        val (ok, n) = rate.allow("refuse:$cid:$code", 10_000L)
        if (ok) {
            logger.severe("peerlink: handshake refused [peer=$cid, code=$code, reason=$reason, suppressed=$n]")
        }
        try {
            writeFrame(out, GoAway(code, reason))
            out.flush()
        } catch (_: Exception) {}
        try {
            socket.close()
        } catch (_: Exception) {}
    }

    private fun runSessionLoops(sess: ServerSession) {
        val readThread = Thread.ofVirtual().name("server-reader-${sess.slot.nodeId}").start {
            val fr = FrameReader(sess.input, MaxConsumerFrame.toLong())
            val keep = sess.keepAliveMs
            try {
                while (!sess.closed.get() && running.get()) {
                    sess.socket.soTimeout = (3 * keep).toInt()
                    val (t, body) = fr.readFrame()
                    when (t) {
                        FrameType.Fetch -> {
                            val f = Fetch()
                            f.decode(body)
                            sess.slot.lastFetch.set(System.currentTimeMillis())
                            manager.log.observeConsumer(sess.slot.idx)
                            if (f.commit != 0L) {
                                manager.log.commit(sess.slot.idx, f.commit)
                            }
                            sess.fetches.put(f)
                        }
                        FrameType.Commit -> {
                            val c = Commit()
                            c.decode(body)
                            manager.log.commit(sess.slot.idx, c.commit)
                        }
                        FrameType.Ping -> {
                            val p = Ping()
                            p.decode(body)
                            sess.writeLock.lock()
                            try {
                                writeFrame(sess.output, Pong(p.token))
                                sess.output.flush()
                            } finally {
                                sess.writeLock.unlock()
                            }
                        }
                        FrameType.GoAway -> {
                            break
                        }
                        FrameType.InterestSnapshot, FrameType.InterestDelta -> {
                            if (!interestFrame(sess, t, body)) break
                        }
                        else -> {}
                    }
                }
            } catch (e: Exception) {
                // Read loop ended
            } finally {
                sess.close()
            }
        }

        // Serve loop on this thread
        try {
            while (!sess.closed.get() && running.get()) {
                val f = sess.fetches.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS) ?: continue
                if ((f.flags and FetchFlagSnapshot) != 0) {
                    serveSnapshot(sess, f)
                } else {
                    serveFetch(sess, f)
                }
            }
        } catch (e: Exception) {
            // Serve loop ended
        } finally {
            sess.close()
            readThread.join(1000)
        }
    }

    // Applies an interest frame; any fault, or the frame without the agreed CapInterest, is a protocol
    // error (plan-peerlink-interest-routing 5.3, 5.4).
    private fun interestFrame(sess: ServerSession, t: FrameType, body: ByteArray): Boolean {
        val table = manager.interest
        if ((sess.caps and CapInterest) == 0L || table == null) {
            sess.goAway(GoAwayCode.Protocol, "interest frame without CapInterest")
            return false
        }
        try {
            when (val f = decodeFrame(t, body)) {
                is InterestSnapshot -> table.applySnapshot(sess.slot.idx, f)
                is InterestDelta -> table.applyDelta(sess.slot.idx, f)
                else -> {}
            }
        } catch (e: Exception) {
            sess.goAway(GoAwayCode.Protocol, e.message ?: "invalid interest frame")
            return false
        }
        return true
    }

    // Reads for this consumer: sparse when CapInterest was agreed, so records the consumer has no
    // interest in are skipped (plan-peerlink-interest-routing 6.4).
    private fun read(sess: ServerSession, from: Long, maxRecords: Int, maxBytes: Int, frames: MutableList<ByteArray>): at.rocworks.peerlink.core.LogReadResult {
        if ((sess.caps and CapInterest) != 0L) {
            sess.deltas.clear()
            return manager.log.readSparse(sess.slot.idx, from, maxRecords, maxBytes, frames, sess.deltas)
        }
        return manager.log.readFor(sess.slot.idx, from, maxRecords, maxBytes, frames)
    }

    private fun serveSnapshot(sess: ServerSession, f: Fetch) {
        val maxRecords = if (f.maxRecords <= 0) 4096 else minOf(f.maxRecords, 65536)
        val maxBytes = if (f.maxBytes <= 0) 1 shl 20 else f.maxBytes
        val limit = manager.config.snapshot.getMaxTopics()

        val pkts = mutableListOf<BrokerMessage>()
        var truncated = 0L

        manager.messageHandler.getRetainedStore().findMatchingMessages("#") { msg ->
            if (msg.payload.isNotEmpty() && manager.hook.accept(msg.topicName)) {
                if (limit > 0 && pkts.size >= limit) {
                    truncated++
                } else {
                    pkts.add(msg)
                }
            }
            true
        }

        val frames = mutableListOf<ByteArray>()
        var total = 0
        val nowSec = System.currentTimeMillis() / 1000L
        val nowMono = manager.log.monoMs(System.nanoTime())

        for (msg in pkts) {
            val (rec, ok) = snapshotRecord(msg, nowSec, nowMono)
            if (!ok || rec == null) continue
            val sz = recordSize(rec)
            if (frames.isNotEmpty() && total + sz > maxBytes) break
            val buf = ByteArray(sz)
            encodeRecord(buf, rec)
            frames.add(buf)
            total += sz
            if (frames.size >= maxRecords) break
        }

        val bounds = manager.log.bounds()
        var flags = BatchFlagSnapshot
        if (frames.isEmpty()) flags = flags or BatchFlagEmpty
        flags = flags or BatchFlagSnapshotEnd // Single batch for in-memory snapshot

        val h = BatchHeader(
            fetchID = f.fetchID,
            flags = flags,
            baseOffset = 0L,
            count = frames.size,
            logStart = bounds.first,
            leo = bounds.second,
            lost = truncated,
            recordsBytes = total,
            sourceMonoMs = nowMono,
            sourceWallMs = System.currentTimeMillis()
        )

        substituteTombstones(frames, sess.maxRec)
        writeBatch(sess, h, frames)
        sess.slot.snapshotServed.addAndGet(frames.size.toLong())
    }

    private fun serveFetch(sess: ServerSession, f: Fetch) {
        val bounds = manager.log.bounds()
        if (f.offset == 0L || f.offset > bounds.second) {
            sess.goAway(GoAwayCode.OffsetOutOfRange, "offset beyond log end")
            return
        }

        val maxRecords = if (f.maxRecords <= 0) 4096 else minOf(f.maxRecords, 65536)
        val maxBytes = if (f.maxBytes <= 0) 1 shl 20 else f.maxBytes
        val minRecords = maxOf(f.minRecords, 1)
        val maxWaitMs = minOf(f.maxWaitMs.toLong(), 60_000L)

        if (f.offset >= bounds.first && f.offset + minRecords > bounds.second && maxWaitMs > 0) {
            manager.log.waitFor(sess.waiter, f.offset + minRecords, maxWaitMs)
        }

        val deadline = System.currentTimeMillis() + maxWaitMs
        val frames = mutableListOf<ByteArray>()
        var res = try {
            read(sess, f.offset, maxRecords, maxBytes, frames)
        } catch (e: Exception) {
            sess.goAway(GoAwayCode.OffsetOutOfRange, "fetch offset beyond log end")
            return
        }
        // Every record up to leo was skipped for this consumer: keep the long poll open.
        while (res.count == 0 && res.span > 0 && res.base + res.span >= res.leo && System.currentTimeMillis() < deadline && !sess.closed.get()) {
            frames.clear()
            manager.log.waitFor(sess.waiter, res.base + res.span + 1, deadline - System.currentTimeMillis())
            res = try {
                read(sess, f.offset, maxRecords, maxBytes, frames)
            } catch (e: Exception) {
                sess.goAway(GoAwayCode.OffsetOutOfRange, "fetch offset beyond log end")
                return
            }
        }

        var flags = 0
        if (res.lost > 0L) flags = flags or BatchFlagGap
        if (res.truncated) flags = flags or BatchFlagTruncated
        var sparse: ByteArray? = null
        if (res.sparse) {
            flags = flags or BatchFlagSparse
            sparse = encodeSparseTable(res.span.toInt(), sess.deltas.data, res.count)
        } else if (res.count == 0) {
            flags = flags or BatchFlagEmpty
        }

        val now = System.currentTimeMillis()
        val h = BatchHeader(
            fetchID = f.fetchID,
            flags = flags,
            baseOffset = res.base,
            count = res.count,
            logStart = res.lso,
            leo = res.leo,
            lost = res.lost,
            recordsBytes = res.bytes,
            sourceMonoMs = manager.log.monoMs(System.nanoTime()),
            sourceWallMs = now
        )

        val skipped = substituteTombstones(frames, sess.maxRec)
        if (skipped > 0) h.recordsBytes = frames.sumOf { it.size }
        writeBatch(sess, h, frames, sparse)
        if (res.sparse) manager.interest?.sparseBatches?.increment()
        sess.slot.servedRecords.addAndGet(res.count.toLong())
        sess.slot.servedBytes.addAndGet(res.bytes.toLong())
        sess.slot.servedSkipped.addAndGet(skipped.toLong())
    }

    private fun substituteTombstones(frames: MutableList<ByteArray>, maxRec: Long): Int {
        if (maxRec <= 0) return 0
        var n = 0
        for (i in frames.indices) {
            if (frames[i].size > maxRec) {
                frames[i] = appendTombstone(ByteArray(0), frames[i])
                n++
            }
        }
        return n
    }

    private fun writeBatch(sess: ServerSession, h: BatchHeader, frames: List<ByteArray>, sparse: ByteArray? = null) {
        if ((sess.caps and CapBatchCRC) != 0L) {
            h.flags = h.flags or BatchFlagCRC
        }
        val prefix = ByteArray(BatchPrefixLen)
        encodeBatchPrefix(prefix, h)
        if ((h.flags and BatchFlagCRC) != 0) {
            setBatchCRC(prefix, frames, sparse = sparse)
        }

        sess.writeLock.lock()
        try {
            sess.output.write(prefix)
            if (sparse != null) sess.output.write(sparse)
            for (f in frames) {
                sess.output.write(f)
            }
            sess.output.flush()
        } finally {
            sess.writeLock.unlock()
        }
    }

    private fun snapshotRecord(msg: BrokerMessage, nowSec: Long, nowMono: Long): Pair<Record?, Boolean> {
        val flags = FlagSnapshot or FlagRetain or (msg.qosLevel and FlagQoSMask)
        var created = msg.time.epochSecond
        if (created <= 0 || created > nowSec) created = nowSec
        var ageMs = (nowSec - created) * 1000L
        if (ageMs > nowMono) ageMs = nowMono
        val captureMono = nowMono - ageMs
        val wallNs = created * 1_000_000_000L
        var expiry = 0L
        val mei = msg.messageExpiryInterval
        if (mei != null && mei > 0) {
            val absExpiry = created + mei
            val remaining = absExpiry - nowSec
            if (remaining <= 0) return Pair(null, false)
            expiry = minOf(remaining + ageMs / 1000L, 0xFFFFFFFFL)
        }
        val userProps = mutableListOf<UserProp>()
        msg.userProperties?.forEach { (k, v) -> userProps.add(UserProp(k, v)) }
        val rec = Record(
            flags = flags,
            publishWallNs = wallNs,
            captureMonoMs = captureMono,
            expirySec = expiry,
            payloadFormat = (msg.payloadFormatIndicator ?: 0).toByte(),
            topic = msg.topicName,
            clientID = msg.clientId.ifEmpty { "inline" },
            username = msg.username?.toByteArray(StandardCharsets.UTF_8) ?: ByteArray(0),
            contentType = msg.contentType ?: "",
            responseTopic = msg.responseTopic ?: "",
            correlationData = msg.correlationData ?: ByteArray(0),
            user = userProps,
            payload = msg.payload
        )
        if (msg.payloadFormatIndicator != null) {
            rec.flags = rec.flags or FlagPayloadFormat
        }
        if (!rec.validContent() || recordSize(rec) == 0) return Pair(null, false)
        return Pair(rec, true)
    }

    fun consumerStatuses(): List<ConsumerStatus> {
        val list = mutableListOf<ConsumerStatus>()
        for (slot in consumers) {
            val ls = manager.log.consumerStats(slot.idx)
            list.add(slot.status(ls).copy(interest = manager.interest?.status(slot.idx)))
        }
        return list
    }

    fun admissionStatus(): AdmissionStatus {
        val failures = mutableMapOf<String, Long>()
        authFailLock.lock()
        try {
            failures.putAll(authFailAll)
        } finally {
            authFailLock.unlock()
        }
        return AdmissionStatus(
            accepted = accepted.get(),
            refusedNetwork = refusedNetwork.get(),
            refusedBusy = refusedBusy.get(),
            refusedSniff = refusedSniff.get(),
            refusedPlaintext = refusedPlaintext.get(),
            refusedHttp = refusedHttp.get(),
            tlsFailures = tlsFailures.get(),
            authFailures = failures,
            preAuth = preAuthAll
        )
    }
}
