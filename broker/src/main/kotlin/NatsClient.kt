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

    // SID -> MQTT topic filter
    private val sidToTopic = mutableMapOf<String, String>()
    // MQTT topic filter -> set of SIDs (multiple SIDs can map to same topic)
    private val topicToSids = mutableMapOf<String, MutableSet<String>>()

    private val busConsumers = mutableListOf<MessageConsumer<*>>()

    // Pending PUB state for binary payload reading
    private data class PendingPub(val mqttTopic: String, val numBytes: Int, val replyTo: String?)
    private var pendingPub: PendingPub? = null
    private lateinit var parser: RecordParser

    companion object {
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
            .put("version", "0.0.1")
            .put("proto", 1)
            .put("max_payload", 1048576)
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
            "PUB" -> handlePub(args)
            "SUB" -> handleSub(args)
            "UNSUB" -> handleUnsub(args)
            "PING" -> writeLine("PONG")
            "PONG" -> {} // ignore
            else -> writeError("Unknown Protocol Operation")
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

        if (!userManager.isUserManagementEnabled()) {
            authenticated = true
            if (verbose) writeLine("+OK")
            return
        }

        val user = json.getString("user", "")
        val pass = json.getString("pass", "")

        if (user.isEmpty()) {
            writeError("Authorization Violation")
            socket.close()
            return
        }

        userManager.authenticate(user, pass).onComplete { ar ->
            if (ar.succeeded() && ar.result() != null) {
                authenticated = true
                username = user
                if (verbose) writeLine("+OK")
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
        if (isNewTopic) {
            sessionHandler.subscribeInternalClient(clientId, mqttTopic, 0)
        }
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
            sessionHandler.unsubscribeInternalClient(clientId, mqttTopic)
        }
    }

    private fun handlePub(args: String) {
        if (!checkAuth()) return

        // PUB <subject> [reply-to] <#bytes>
        val parts = args.split(" ")
        if (parts.size < 2) {
            writeError("Invalid PUB")
            return
        }

        val subject = parts[0]
        val numBytes = try {
            parts.last().toInt()
        } catch (e: NumberFormatException) {
            writeError("Invalid PUB byte count")
            return
        }
        val replyTo = if (parts.size == 3) parts[1] else null

        val mqttTopic = natsSubjectToMqttTopic(subject)

        // ACL check
        val user = username
        if (user != null && !userManager.canPublish(user, mqttTopic)) {
            writeError("Permissions Violation for Publish to \"$subject\"")
            return
        }

        // Switch to fixed-size mode to read exact payload bytes + \r\n
        pendingPub = PendingPub(mqttTopic, numBytes, replyTo)
        parser.fixedSizeMode(numBytes + 2) // +2 for trailing \r\n
    }

    private fun handlePubPayload(pending: PendingPub, buffer: Buffer) {
        // Extract payload (strip trailing \r\n)
        val payload = buffer.getBytes(0, pending.numBytes)

        val message = BrokerMessage(
            messageId = 0,
            topicName = pending.mqttTopic,
            payload = payload,
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = clientId
        )
        sessionHandler.publishMessage(message)
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

        // Find all SIDs that match this topic
        for ((filter, sids) in topicToSids) {
            if (mqttTopicMatchesFilter(message.topicName, filter)) {
                for (sid in sids) {
                    // MSG <subject> <sid> <#bytes>\r\n<payload>\r\n
                    val payload = message.payload
                    val header = "MSG $natsSubject $sid ${payload.size}\r\n"
                    val buf = Buffer.buffer(header.length + payload.size + 2)
                    buf.appendString(header)
                    buf.appendBytes(payload)
                    buf.appendString("\r\n")
                    socket.write(buf)
                }
            }
        }
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
        // Unsubscribe all topics
        for (mqttTopic in topicToSids.keys.toList()) {
            sessionHandler.unsubscribeInternalClient(clientId, mqttTopic)
        }
        sidToTopic.clear()
        topicToSids.clear()

        sessionHandler.unregisterInternalClient(clientId)

        // Undeploy this verticle
        vertx.undeploy(deploymentID())
    }

    private fun writeLine(line: String) {
        socket.write("$line\r\n")
    }

    private fun writeError(msg: String) {
        writeLine("-ERR '$msg'")
    }

    // --- Topic conversion helpers ---

    /**
     * Convert NATS subject to MQTT topic:
     * - `.` -> `/`
     * - `*` -> `+`
     * - `>` -> `#`
     */
    private fun natsSubjectToMqttTopic(subject: String): String {
        return subject.replace('.', '/').replace('*', '+').replace('>', '#')
    }

    /**
     * Convert MQTT topic to NATS subject:
     * - `/` -> `.`
     * - `+` -> `*`
     * - `#` -> `>`
     */
    private fun mqttTopicToNatsSubject(topic: String): String {
        return topic.replace('/', '.').replace('+', '*').replace('#', '>').replace(' ', '_')
    }

    /**
     * Check if a concrete MQTT topic matches an MQTT topic filter (with wildcards).
     */
    private fun mqttTopicMatchesFilter(topic: String, filter: String): Boolean {
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
