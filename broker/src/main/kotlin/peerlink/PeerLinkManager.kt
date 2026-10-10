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
    // Remote interest of the consumers served by this node; null when PeerLink.Interest is disabled.
    val interest: InterestTable?
    @Volatile private var interestTicker: Thread? = null
    // Local interest announced to the sources this node pulls from; null when PeerLink.Interest is
    // disabled or no pulled peer has interest on.
    val tracker: InterestTracker?
    @Volatile var redundancyProvider: RedundancyComponentProvider = NoRedundancyComponents

    init {
        // Wire into SessionHandler and MessageHandler
        sessionHandler.peerLinkManager = this
        sessionHandler.peerLinkReceiveBus = config.receive.getBus()
        sessionHandler.peerLinkReceiveQueue = config.receive.queue
        messageHandler.peerLinkReceiveArchive = config.receive.getArchive()
        sessionHandler.peerLinkReceiveBridgeOutbound = config.receive.bridgeOutbound

        val maxRecordBytes = config.log.getMaxRecordBytes(512 * 1024)
        val servedPeers = setup.peers.filter { it.getServe() }
        val consumerList = servedPeers.map { it.nodeId.trim().lowercase() }
        val interestOn = config.interest.enabled && consumerList.size <= peerLinkMaxInterestConsumers
        log = PeerLog(
            LogConfig(
                maxMessages = config.log.getMaxMessages().toLong(),
                maxBytes = config.log.getMaxBytes(),
                maxRecordBytes = maxRecordBytes,
                consumers = consumerList,
                masked = interestOn,
                maxScan = config.interest.getMaxScanPerFetch()
            )
        )
        interest = if (interestOn) InterestTable(
            consumers = servedPeers.map { InterestTable.Consumer(it.nodeId.trim().lowercase(), !it.interestOff()) },
            unknownAll = config.interest.unknownAll(),
            maxFilters = config.interest.getMaxFiltersPerPeer(),
            maxBytes = config.interest.getMaxFilterBytes(),
            logger = logger
        ) else null

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
        hook.interest = interest

        tracker = if (config.interest.enabled && setup.peers.any { !it.interestOff() }) InterestTracker(
            flushMs = config.interest.getFlushMs().toLong(),
            maxFilterBytes = config.interest.getMaxFilterBytes(),
            classifier = ClientClassifier { sessionHandler.peerLinkInterestClass(it) },
            logger = logger
        ) else null

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
            if (!peerConfig.pulls()) continue
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

            tracker?.let { t ->
                refreshStaticInterest()
                t.start()
                sessionHandler.setSubscriptionObserver(t)
                sessionHandler.replaySubscriptions(t)
            }
            server?.start()
            for (puller in pullers.values) {
                puller.start()
            }
            hook.active.set(true)
            startInterestTicker()
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

            interestTicker?.interrupt()
            interestTicker = null
            tracker?.let {
                sessionHandler.setSubscriptionObserver(null)
                it.stop()
            }

            // Stage 5: Drain log and seal
            log.drain(drainMs)
            log.seal()

            server?.stop()
            logger.info("PeerLink: shutdown complete")
        }
    }

    // Every 100 ms: advance consumers past records not meant for them, run the persistent-expiry
    // backlog sweep; once a second: expire persistent interest of disconnected consumers.
    private fun startInterestTicker() {
        if (interest == null && tracker == null) return
        val table = interest
        interestTicker = Thread.ofVirtual().name("peerlink-interest").start {
            var lastExpire = 0L
            while (running.get()) {
                try {
                    Thread.sleep(100)
                } catch (_: InterruptedException) {
                    break
                }
                try {
                    val now = System.currentTimeMillis()
                    if (table != null) {
                        log.advanceSkipped()
                        if (now - lastExpire >= 1000) table.expire(now)
                        table.sweep(log)
                    }
                    if (now - lastExpire >= 1000) {
                        lastExpire = now
                        refreshStaticInterest()
                    }
                } catch (e: Exception) {
                    logger.warning("peerlink: interest maintenance failed: ${e.message}")
                }
            }
        }
    }

    // refreshStaticInterest updates the tracker's static sources: the topic filters of the deployed
    // archive groups (PER, never expiring; only with Receive.Archive) and the redundancy provider.
    fun refreshStaticInterest() {
        val t = tracker ?: return
        val archive = HashMap<String, InterestClass>()
        if (config.receive.getArchive()) {
            at.rocworks.Monster.getArchiveHandler()?.getDeployedArchiveGroups()?.values?.forEach { g ->
                val filters = g.topicFilter.ifEmpty { listOf("#") }
                for (f in filters) archive[f] = InterestClass.PER_NEVER
            }
        }
        t.setSource("archive", archive)
        t.setSource("redundancy", redundancyProvider.filters())
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
            brokerType = at.rocworks.peerlink.wire.BrokerTypeFull,
            brokerVersion = at.rocworks.Version.getVersion(),
            protocolVersion = at.rocworks.peerlink.wire.protocolVersion(
                at.rocworks.peerlink.wire.VersionMajor, at.rocworks.peerlink.wire.VersionMinor),
            epoch = log.epoch,
            listen = listenAddr,
            tls = config.tls.enabled,
            log = logStatus,
            admission = admStatus,
            consumers = consumerList,
            sources = sourceList,
            interest = interestCounters()
        )
    }

    private fun interestCounters(): InterestCounters? {
        val t = interest
        val tr = tracker
        if (t == null && tr == null) return null
        return InterestCounters(
            interestSkipped = t?.skipped?.sum() ?: 0L,
            interestMatched = t?.matched?.sum() ?: 0L,
            sparseBatches = t?.sparseBatches?.sum() ?: 0L,
            volatileDropped = t?.volatileDropped?.get() ?: 0L,
            persistentExpired = t?.persistentExpired?.get() ?: 0L,
            interestBacklogDiscarded = t?.backlogDiscarded?.get() ?: 0L,
            interestRejected = t?.rejected?.get() ?: 0L,
            interestOverLimit = t?.overLimitCount?.get() ?: 0L,
            deltasReceived = t?.deltasReceived?.get() ?: 0L,
            local = tr?.status()
        )
    }
}
