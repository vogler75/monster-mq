package at.rocworks

import at.rocworks.bus.EventBusAddresses
import at.rocworks.auth.UserManager
import at.rocworks.data.BrokerMessage
import at.rocworks.data.BulkClientMessage
import at.rocworks.handlers.SessionHandler
import io.vertx.core.AbstractVerticle
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.json.JsonObject
import io.vertx.core.net.NetSocket
import io.vertx.core.parsetools.RecordParser
import java.util.UUID

class NatsClient(
    private val socket: NetSocket,
    private val sessionHandler: SessionHandler,
    private val userManager: UserManager
) : AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    private val clientId = "nats-${UUID.randomUUID()}"
    private var authenticated = false
    private var username: String? = null
    private var verbose = false
    private var headersEnabled = false
    private var closed = false

    // SID -> MQTT topic filter
    private val sidToTopic = mutableMapOf<String, String>()
    // MQTT topic filter -> set of SIDs (multiple SIDs can map to same topic)
    private val topicToSids = mutableMapOf<String, MutableSet<String>>()
    // MQTT topic filter -> number of KV consumers that need it (internal subscription shared with SUB)
    private val kvTopicRefs = mutableMapOf<String, Int>()

    private val jetStreamKv = NatsJetStreamKv(this)

    private val busConsumers = mutableListOf<MessageConsumer<*>>()

    // Pending PUB state for binary payload reading
    private data class PendingPub(val subject: String, val mqttTopic: String, val headerBytes: Int, val numBytes: Int, val replyTo: String?)
    private var pendingPub: PendingPub? = null
    private lateinit var parser: RecordParser

    companion object {
        // Clients gate features (e.g. KV needs >= 2.6.2) on the advertised server version
        const val NATS_SERVER_VERSION = "2.10.0"

        fun deploy(vertx: Vertx, socket: NetSocket, sessionHandler: SessionHandler, userManager: UserManager) {
            val client = NatsClient(socket, sessionHandler, userManager)
            vertx.deployVerticle(client)
        }
    }

    override fun start() {
        logger.fine { "NATS client [$clientId] connected from ${socket.remoteAddress()}" }

        // Send INFO
        val info = JsonObject()
            .put("server_id", "monstermq")
            .put("server_name", "MonsterMQ")
            .put("version", NATS_SERVER_VERSION)
            .put("proto", 1)
            .put("headers", true)
            .put("max_payload", 1048576)
            .put("jetstream", true)
            .put("auth_required", userManager.isUserManagementEnabled())
        writeLine("INFO ${info.encode()}")

        // Set up line-delimited parser
        parser = RecordParser.newDelimited("\r\n") { buffer ->
            handleRecord(buffer)
        }

        socket.handler(parser)

        socket.closeHandler {
            cleanup()
        }

        socket.exceptionHandler { err ->
            logger.fine { "NATS client [$clientId] socket error: ${err.message}" }
            cleanup()
        }

        // Register EventBus consumer for message delivery
        busConsumers.add(
            vertx.eventBus().consumer<Any>(EventBusAddresses.Client.messages(clientId)) { busMessage ->
                handleBusMessage(busMessage.body())
            }
        )

        // Register EventBus consumer for command execution (e.g. disconnect from dashboard)
        busConsumers.add(
            vertx.eventBus().consumer<JsonObject>(EventBusAddresses.Client.commands(clientId)) { message ->
                val command = message.body()
                if (command.getString(Const.COMMAND_KEY) == Const.COMMAND_DISCONNECT) {
                    logger.info { "NATS client [$clientId] disconnect command received via EventBus" }
                    socket.close()
                    message.reply(JsonObject().put("Connected", false))
                }
            }
        )
    }

    override fun stop() {
        logger.fine { "NATS client [$clientId] stop" }
        busConsumers.forEach { it.unregister() }
    }

    private fun handleRecord(buffer: Buffer) {
        // If we're reading a PUB payload
        val pending = pendingPub
        if (pending != null) {
            pendingPub = null
            // Switch back to line-delimited mode
            parser.delimitedMode("\r\n")
            handlePubPayload(pending, buffer)
            return
        }

        val line = buffer.toString()
        if (line.isEmpty()) return

        val spaceIdx = line.indexOf(' ')
        val cmd = if (spaceIdx > 0) line.substring(0, spaceIdx).uppercase() else line.uppercase()
        val args = if (spaceIdx > 0) line.substring(spaceIdx + 1) else ""

        when (cmd) {
            "CONNECT" -> handleConnect(args)
            "PUB" -> handlePub(args, withHeaders = false)
            "HPUB" -> handlePub(args, withHeaders = true)
            "SUB" -> handleSub(args)
            "UNSUB" -> handleUnsub(args)
            "PING" -> writeLine("PONG")
            "PONG" -> {} // ignore
            else -> writeError("Unknown Protocol Operation")
        }
    }

    private fun registerSession() {
        val information = JsonObject()
        information.put("RemoteAddress", socket.remoteAddress().toString())
        information.put("LocalAddress", socket.localAddress().toString())
        information.put("ProtocolVersion", "NATS")
        information.put("SSL", false)
        sessionHandler.setClient(clientId, true, information).onComplete { ar ->
            if (ar.succeeded()) {
                sessionHandler.onlineClient(clientId)
            }
        }
    }

    private fun handleConnect(args: String) {
        val json = try {
            JsonObject(args)
        } catch (e: Exception) {
            writeError("Invalid CONNECT JSON")
            return
        }

        verbose = json.getBoolean("verbose", false)
        headersEnabled = json.getBoolean("headers", false)

        if (!userManager.isUserManagementEnabled()) {
            authenticated = true
            if (verbose) writeLine("+OK")
            registerSession()
            return
        }

        val user = json.getString("user", "")
        val pass = json.getString("pass", "")

        if (user.isEmpty()) {
            // No credentials: allow only if the Anonymous user is enabled
            val anonymousUser = userManager.getUser("Anonymous")
            if (anonymousUser != null && anonymousUser.enabled) {
                authenticated = true
                username = "Anonymous"
                if (verbose) writeLine("+OK")
                registerSession()
            } else {
                writeError("Authorization Violation")
                socket.close()
            }
            return
        }

        userManager.authenticate(user, pass).onComplete { ar ->
            if (ar.succeeded() && ar.result() != null) {
                authenticated = true
                username = user
                if (verbose) writeLine("+OK")
                registerSession()
            } else {
                writeError("Authorization Violation")
                socket.close()
            }
        }
    }

    private fun handleSub(args: String) {
        if (!checkAuth()) return

        // SUB <subject> [queue group] <sid>
        val parts = args.split(" ")
        if (parts.size < 2) {
            writeError("Invalid SUB")
            return
        }
        val subject = parts[0]
        val sid = parts.last() // SID is always last

        val mqttTopic = natsSubjectToMqttTopic(subject)

        // ACL check
        val user = username
        if (user != null && !userManager.canSubscribe(user, mqttTopic)) {
            writeError("Permissions Violation for Subscription to \"$subject\"")
            return
        }

        // Track SID -> topic mapping
        sidToTopic[sid] = mqttTopic
        val sids = topicToSids.getOrPut(mqttTopic) { mutableSetOf() }
        val isNewTopic = sids.isEmpty()
        sids.add(sid)

        // Only subscribe once per unique topic
        if (isNewTopic && !kvTopicRefs.containsKey(mqttTopic)) {
            sessionHandler.subscribeInternalClient(clientId, mqttTopic, 0)
        }
        jetStreamKv.onSubscribe()
    }

    private fun handleUnsub(args: String) {
        if (!checkAuth()) return

        // UNSUB <sid> [max_msgs]
        val parts = args.split(" ")
        if (parts.isEmpty()) {
            writeError("Invalid UNSUB")
            return
        }
        val sid = parts[0]

        val mqttTopic = sidToTopic.remove(sid) ?: return
        val sids = topicToSids[mqttTopic] ?: return
        sids.remove(sid)

        // Only unsubscribe when no more SIDs reference this topic
        if (sids.isEmpty()) {
            topicToSids.remove(mqttTopic)
            if (!kvTopicRefs.containsKey(mqttTopic)) {
                sessionHandler.unsubscribeInternalClient(clientId, mqttTopic)
            }
        }
        jetStreamKv.onUnsubscribe()
    }

    private fun handlePub(args: String, withHeaders: Boolean) {
        if (!checkAuth()) return

        // PUB <subject> [reply-to] <#bytes>
        // HPUB <subject> [reply-to] <#header bytes> <#total bytes>
        val op = if (withHeaders) "HPUB" else "PUB"
        val parts = args.split(" ").filter { it.isNotEmpty() }
        val minParts = if (withHeaders) 3 else 2
        if (parts.size < minParts || parts.size > minParts + 1) {
            writeError("Invalid $op")
            return
        }

        val subject = parts[0]
        val numBytes = parts.last().toIntOrNull()
        val headerBytes = if (withHeaders) parts[parts.size - 2].toIntOrNull() else 0
        if (numBytes == null || headerBytes == null || numBytes < 0 || headerBytes < 0 || headerBytes > numBytes) {
            writeError("Invalid $op byte count")
            return
        }
        val replyTo = if (parts.size == minParts + 1) parts[1] else null

        val mqttTopic = natsSubjectToMqttTopic(subject)

        // ACL check; JetStream API and KV subjects are checked by the KV layer on the bucket topic
        val user = username
        if (user != null && !NatsJetStreamKv.isJetStreamSubject(subject) && !userManager.canPublish(user, mqttTopic)) {
            writeError("Permissions Violation for Publish to \"$subject\"")
            return
        }

        // Switch to fixed-size mode to read exact payload bytes + \r\n
        pendingPub = PendingPub(subject, mqttTopic, headerBytes, numBytes, replyTo)
        parser.fixedSizeMode(numBytes + 2) // +2 for trailing \r\n
    }

    private fun handlePubPayload(pending: PendingPub, buffer: Buffer) {
        // Extract headers and payload (strip trailing \r\n)
        val headers = if (pending.headerBytes > 0) parseHeaders(buffer.getString(0, pending.headerBytes)) else emptyMap()
        val payload = buffer.getBytes(pending.headerBytes, pending.numBytes)

        if (NatsJetStreamKv.isJetStreamSubject(pending.subject)) {
            jetStreamKv.handlePublish(pending.subject, pending.replyTo, headers, payload)
            return
        }

        val message = BrokerMessage(
            messageId = 0,
            topicName = pending.mqttTopic,
            payload = payload,
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = clientId,
            responseTopic = pending.replyTo?.let { natsSubjectToMqttTopic(it) },
            userProperties = headers.ifEmpty { null }
        )
        sessionHandler.publishMessage(message)
    }

    // --- Internals used by the JetStream KV layer ---

    internal val vertxInstance: Vertx get() = vertx
    internal val sessionHandlerInstance: SessionHandler get() = sessionHandler
    internal val clientIdentifier: String get() = clientId

    internal fun canPublish(mqttTopic: String): Boolean {
        val user = username ?: return true
        return userManager.canPublish(user, mqttTopic)
    }

    internal fun canSubscribe(mqttTopic: String): Boolean {
        val user = username ?: return true
        return userManager.canSubscribe(user, mqttTopic)
    }

    /** SIDs of this connection whose subscription matches the given concrete NATS subject. */
    internal fun sidsForSubject(subject: String): List<String> {
        val topic = natsSubjectToMqttTopic(subject)
        val result = mutableListOf<String>()
        for ((filter, sids) in topicToSids) {
            if (mqttTopicMatchesFilter(topic, filter)) result.addAll(sids)
        }
        return result
    }

    /** Write a message to every local subscription matching [subject] (used for API replies and KV push). */
    internal fun deliverLocal(subject: String, replyTo: String?, headerBlock: String?, payload: ByteArray) {
        for (sid in sidsForSubject(subject)) {
            writeMsg(subject, sid, replyTo, headerBlock, payload)
        }
    }

    /** Write a message with [subject] to every local subscription matching [deliverSubject] (JetStream push). */
    internal fun deliverVia(deliverSubject: String, subject: String, replyTo: String?, headerBlock: String?, payload: ByteArray) {
        for (sid in sidsForSubject(deliverSubject)) {
            writeMsg(subject, sid, replyTo, headerBlock, payload)
        }
    }

    internal fun addKvTopic(mqttTopic: String) {
        val refs = kvTopicRefs[mqttTopic] ?: 0
        kvTopicRefs[mqttTopic] = refs + 1
        if (refs == 0 && !topicToSids.containsKey(mqttTopic)) {
            sessionHandler.subscribeInternalClient(clientId, mqttTopic, 0)
        }
    }

    internal fun removeKvTopic(mqttTopic: String) {
        val refs = kvTopicRefs[mqttTopic] ?: return
        if (refs > 1) {
            kvTopicRefs[mqttTopic] = refs - 1
            return
        }
        kvTopicRefs.remove(mqttTopic)
        if (!topicToSids.containsKey(mqttTopic) && !closed) {
            sessionHandler.unsubscribeInternalClient(clientId, mqttTopic)
        }
    }

    private fun writeMsg(subject: String, sid: String, replyTo: String?, headerBlock: String?, payload: ByteArray) {
        val reply = if (replyTo != null) "$replyTo " else ""
        val buf: Buffer
        if (headerBlock != null && headersEnabled) {
            // HMSG <subject> <sid> [reply-to] <#header bytes> <#total bytes>\r\n<headers><payload>\r\n
            val headerBytes = headerBlock.toByteArray(Charsets.UTF_8)
            val line = "HMSG $subject $sid $reply${headerBytes.size} ${headerBytes.size + payload.size}\r\n"
            buf = Buffer.buffer(line.length + headerBytes.size + payload.size + 2)
            buf.appendString(line)
            buf.appendBytes(headerBytes)
        } else {
            // MSG <subject> <sid> [reply-to] <#bytes>\r\n<payload>\r\n
            val line = "MSG $subject $sid $reply${payload.size}\r\n"
            buf = Buffer.buffer(line.length + payload.size + 2)
            buf.appendString(line)
        }
        buf.appendBytes(payload)
        buf.appendString("\r\n")
        socket.write(buf)
    }

    private fun handleBusMessage(body: Any?) {
        when (body) {
            is BrokerMessage -> deliverMessage(body)
            is BulkClientMessage -> body.messages.forEach { deliverMessage(it) }
            else -> logger.warning { "NATS client [$clientId] received unknown message type: ${body?.javaClass?.simpleName}" }
        }
    }

    private fun deliverMessage(message: BrokerMessage) {
        val natsSubject = mqttTopicToNatsSubject(message.topicName)
        val replyTo = message.responseTopic?.let { mqttTopicToNatsSubject(it) }
        val headerBlock = message.userProperties?.takeIf { it.isNotEmpty() }?.let { formatHeaders(null, it) }

        // Find all SIDs that match this topic
        for ((filter, sids) in topicToSids) {
            if (mqttTopicMatchesFilter(message.topicName, filter)) {
                for (sid in sids) {
                    writeMsg(natsSubject, sid, replyTo, headerBlock, message.payload)
                }
            }
        }

        jetStreamKv.onBrokerMessage(message)
    }

    private fun checkAuth(): Boolean {
        if (!userManager.isUserManagementEnabled()) return true
        if (!authenticated) {
            writeError("Authorization Violation")
            return false
        }
        return true
    }

    private fun cleanup() {
        if (closed) return
        closed = true

        jetStreamKv.close()

        // Unsubscribe all topics
        for (mqttTopic in (topicToSids.keys + kvTopicRefs.keys).toSet()) {
            sessionHandler.unsubscribeInternalClient(clientId, mqttTopic)
        }
        sidToTopic.clear()
        topicToSids.clear()
        kvTopicRefs.clear()

        sessionHandler.unregisterInternalClient(clientId)
        sessionHandler.delClient(clientId)

        // Undeploy this verticle
        vertx.undeploy(deploymentID())
    }

    private fun writeLine(line: String) {
        socket.write("$line\r\n")
    }

    private fun writeError(msg: String) {
        writeLine("-ERR '$msg'")
    }

    // --- Header helpers ---

    /** Parse a NATS header block (`NATS/1.0[ status]` line, then `Key: Value` lines). First value wins. */
    private fun parseHeaders(block: String): Map<String, String> {
        val result = linkedMapOf<String, String>()
        block.split("\r\n").drop(1).forEach { line ->
            val idx = line.indexOf(':')
            if (idx > 0) {
                val key = line.substring(0, idx).trim()
                if (!result.containsKey(key)) result[key] = line.substring(idx + 1).trim()
            }
        }
        return result
    }

    // --- Topic conversion helpers ---

    /**
     * Convert NATS subject to MQTT topic:
     * - `.` -> `/`
     * - `*` -> `+`
     * - `>` -> `#`
     */
    internal fun natsSubjectToMqttTopic(subject: String): String {
        return subject.replace('.', '/').replace('*', '+').replace('>', '#')
    }

    /**
     * Convert MQTT topic to NATS subject:
     * - `/` -> `.`
     * - `+` -> `*`
     * - `#` -> `>`
     */
    internal fun mqttTopicToNatsSubject(topic: String): String {
        return topic.replace('/', '.').replace('+', '*').replace('#', '>').replace(' ', '_')
    }

    /**
     * Check if a concrete MQTT topic matches an MQTT topic filter (with wildcards).
     */
    internal fun mqttTopicMatchesFilter(topic: String, filter: String): Boolean {
        if (filter == "#") return true
        val topicParts = topic.split('/')
        val filterParts = filter.split('/')

        var i = 0
        while (i < filterParts.size) {
            val fp = filterParts[i]
            if (fp == "#") return true  // multi-level wildcard matches rest
            if (i >= topicParts.size) return false
            if (fp != "+" && fp != topicParts[i]) return false
            i++
        }
        return i == topicParts.size
    }
}

/** Format a NATS header block; [status] is e.g. `100 Idle Heartbeat` or `404 Message Not Found`. */
internal fun formatHeaders(status: String?, headers: Map<String, String>): String {
    val sb = StringBuilder("NATS/1.0")
    if (status != null) sb.append(' ').append(status)
    sb.append("\r\n")
    // CR/LF would break the header framing, e.g. in MQTT user properties forwarded as headers
    fun clean(text: String) = text.replace('\r', ' ').replace('\n', ' ')
    headers.forEach { (k, v) -> sb.append(clean(k).replace(':', '_')).append(": ").append(clean(v)).append("\r\n") }
    sb.append("\r\n")
    return sb.toString()
}
