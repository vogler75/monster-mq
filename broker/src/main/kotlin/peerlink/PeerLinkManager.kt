package at.rocworks.peerlink

import at.rocworks.Utils
import at.rocworks.bus.IMessageBus
import at.rocworks.data.BrokerMessage
import at.rocworks.handlers.MessageHandler
import at.rocworks.handlers.SessionHandler
import at.rocworks.peerlink.config.*
import at.rocworks.peerlink.core.IncludeExclude
import at.rocworks.peerlink.core.LogConfig
import at.rocworks.peerlink.core.PeerLog
import at.rocworks.peerlink.tls.PeerIdentity
import at.rocworks.peerlink.tls.PeerTls
import at.rocworks.peerlink.tls.parsePins
import at.rocworks.peerlink.tls.decodeSecrets
import java.security.SecureRandom
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger

class PeerLinkManager(
    val config: PeerLinkConfig,
    val setup: PeerLinkSetup,
    val sessionHandler: SessionHandler,
    val messageHandler: MessageHandler,
    val messageBus: IMessageBus,
    val logger: Logger = Utils.getLogger(PeerLinkManager::class.java)
) {
    val nodeId: String = setup.nodeID
    val instanceId: Long = SecureRandom().nextLong() or 1L
    val running = AtomicBoolean(false)
    val draining = AtomicBoolean(false)

    var peerTls: PeerTls? = null
    val log: PeerLog
    val hook: CaptureHook
    var server: PeerServer? = null
    val pullers = mutableMapOf<String, Puller>()
    var onStateChange: (() -> Unit)? = null

    init {
        // Wire into SessionHandler and MessageHandler
        sessionHandler.peerLinkManager = this
        sessionHandler.peerLinkReceiveBus = config.receive.getBus()
        sessionHandler.peerLinkReceiveQueue = config.receive.queue
        messageHandler.peerLinkReceiveArchive = config.receive.getArchive()

        val maxRecordBytes = config.log.getMaxRecordBytes(512 * 1024)
        val consumerList = setup.peers.filter { it.getServe() }.map { it.nodeId.trim().lowercase() }
        log = PeerLog(
            LogConfig(
                maxMessages = config.log.getMaxMessages().toLong(),
                maxBytes = config.log.getMaxBytes(),
                maxRecordBytes = maxRecordBytes,
                consumers = consumerList
            )
        )

        val captureInclude = config.capture.effectiveInclude()
        val captureExclude = config.capture.getExclude("")
        val captureFilter = IncludeExclude.create(captureInclude, captureExclude)

        hook = CaptureHook(
            log = log,
            filter = captureFilter,
            captureWills = config.capture.getWills(),
            echoSuppressMs = config.capture.echoSuppressMs,
            maxExpirySec = 0L
        )

        if (config.tls.enabled) {
            val tls = PeerTls(config.tls, nodeId)
            peerTls = tls
        }

        // Initialize server if we have any consumers to serve
        if (setup.anyServe() || setup.peers.isEmpty()) {
            val srv = PeerServer(this)
            srv.initSlots()
            server = srv
        }

        // Initialize pullers for all configured peers
        for (peerConfig in setup.peers) {
            val pid = peerConfig.nodeId.trim().lowercase()
            val tp = PeerIdentity(
                nodeId = pid,
                certificateIdentity = peerConfig.tls.certificateIdentity,
                pins = parsePins(peerConfig.tls.pinnedSha256)
            )
            val secrets = decodeSecrets(peerConfig.sharedSecrets.ifEmpty { config.sharedSecrets })

            val pInclude = peerConfig.receive.effectiveInclude()
            val pExclude = peerConfig.receive.exclude
            val pFilter = IncludeExclude.create(pInclude, pExclude)

            val injector = Injector(
                sourceNodeId = pid,
                sessionHandler = sessionHandler,
                messageBus = messageBus,
                filter = pFilter,
                hook = hook,
                maxMessageSize = 512 * 1024,
                maxRecordAgeMs = config.receive.maxRecordAgeMs.toLong(),
                markReplicas = config.receive.markReplicas,
                fetchMaxRecords = config.fetch.getMaxRecords(),
                pace = Pacer(
                    factor = config.receive.getCatchUpRateFactor(),
                    maxRate = config.receive.maxApplyRate.toDouble()
                )
            )

            val puller = Puller(
                manager = this,
                peer = peerConfig,
                tlsPeer = tp,
                secrets = secrets,
                filter = pFilter,
                injector = injector
            )
            pullers[pid] = puller
        }
    }

    fun capture(message: BrokerMessage) {
        hook.capture(message)
    }

    fun recordSessionEstablished(clientId: String) {
        hook.sessions.record(clientId, System.nanoTime())
    }

    fun resync(source: String) {
        pullers[source.trim().lowercase()]?.requestResync()
    }

    fun start() {
        if (running.compareAndSet(false, true)) {
            for (info in setup.infos) {
                logger.info("PeerLink: $info")
            }
            for (warn in setup.warnings) {
                logger.warning("PeerLink: $warn")
            }

            server?.start()
            for (puller in pullers.values) {
                puller.start()
            }
            hook.active.set(true)
            logger.info("PeerLink: started with NodeId \"$nodeId\"")
        }
    }

    fun stop(drainMs: Long = config.log.getDrainOnShutdownMs().toLong()) {
        if (running.compareAndSet(true, false)) {
            draining.set(true)
            logger.info("PeerLink: beginning shutdown drain (${drainMs}ms budget)...")

            // Stage 1: Stop pullers (gracefully finish batches and commit)
            for (puller in pullers.values) {
                puller.stop(gracefulShutdown = true)
            }
            for (puller in pullers.values) {
                puller.waitStopped(5000)
            }

            // Stage 2: Begin drain (stop capturing wills)
            hook.draining.set(true)
            hook.active.set(false)

            // Stage 5: Drain log and seal
            log.drain(drainMs)
            log.seal()

            server?.stop()
            logger.info("PeerLink: shutdown complete")
        }
    }

    fun status(): Status {
        val s = log.stats()
        val bounds = log.bounds()

        val appended = KindCounts(
            client = s.appendedClient,
            inline = s.appendedInline,
            will = s.appendedWill
        )

        val logStatus = LogStatus(
            epoch = log.epoch,
            lso = bounds.first,
            leo = bounds.second,
            lwm = s.lwm,
            records = s.records,
            bytes = s.bytes,
            maxBytes = log.maxBytes,
            maxMessages = log.maxMessages,
            capacitySeconds = null,
            appended = appended,
            trimmed = s.trimmed,
            evictedUnread = s.evictedUnread,
            evictedBy = emptyMap(),
            captureDropped = mapOf("size" to s.captureDroppedSize, "invalid" to s.captureDroppedInvalid),
            skipPeer = hook.skipPeer.sum(),
            skipWill = hook.skipWill.sum(),
            filtered = hook.filtered.sum(),
            echoSuppressed = hook.echoSuppressed.sum(),
            sharedSkipped = hook.sharedSkipped.sum(),
            refusedClientIds = hook.refusedIDs.sum(),
            usernameStripped = hook.usernameStripped.sum(),
            spareMisses = s.spareMisses,
            uncapturedAtShutdown = s.uncapturedAtShutdown,
            sealed = log.isSealed(),
            active = hook.active.get()
        )

        val admStatus = server?.admissionStatus() ?: AdmissionStatus(
            accepted = 0, refusedNetwork = 0, refusedBusy = 0, refusedSniff = 0,
            refusedPlaintext = 0, refusedHttp = 0, tlsFailures = 0, authFailures = emptyMap(), preAuth = 0
        )

        val consumerList = server?.consumerStatuses() ?: emptyList()
        val sourceList = pullers.values.map { it.status() }

        val listenAddr = "${config.listener.listenAddress()}:${config.listener.effectivePort()}"
        return Status(
            enabled = config.enabled,
            nodeId = nodeId,
            epoch = log.epoch,
            listen = listenAddr,
            tls = config.tls.enabled,
            log = logStatus,
            admission = admStatus,
            consumers = consumerList,
            sources = sourceList
        )
    }
}
