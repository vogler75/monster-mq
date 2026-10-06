package at.rocworks.peerlink.config

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.io.File
import java.net.InetAddress
import java.util.Base64

// Constants and defaults from edge/internal/config/config.go
const val PeerLinkDefaultPort = 1890
const val peerLinkDefaultKeepAlive = 10
const val peerLinkDefaultMaxMessages = 2_000_000
const val peerLinkDefaultMaxBytes = 256L shl 20 // 256 MiB
const val peerLinkRecordAllowance = 64 shl 10 // 64 KiB
const val peerLinkDefaultDrainMs = 2000
const val peerLinkDefaultNeverConnected = 300
const val peerLinkDefaultSnapshotTopics = 1_000_000
const val peerLinkDefaultFetchRecords = 4096
const val peerLinkDefaultFetchBytes = 1 shl 20 // 1 MiB
const val peerLinkDefaultFetchWaitMs = 1000
const val PeerLinkMinFetchWaitMs = 10
const val peerLinkDefaultPipeline = 1
const val peerLinkDefaultReconnectMaxMs = 30000
const val peerLinkDefaultCatchUpFactor = 3.0
const val peerLinkDefaultMaxFrameBytes = (16 shl 20) + peerLinkRecordAllowance
const val peerLinkDefaultInjectWorkers = 1
const val peerLinkDefaultPreAuthPerIP = 2
const val defaultHMISyncBaseTopic = "monstermq/hmi/sync"
const val peerLinkDefaultCertPath = "certs/peer-{NodeId}.pem"
const val peerLinkDefaultKeyPath = "certs/peer-{NodeId}.key"

const val PeerLinkSnapshotFill = "FILL"
const val PeerLinkSnapshotOff = "OFF"
const val PeerLinkSharedSkip = "SKIP"
const val PeerLinkSharedDeliver = "DELIVER"
const val PeerLinkIdentityNone = "NONE"
const val PeerLinkIdentityDNS = "DNS"
const val PeerLinkIdentityCN = "CN"
const val PeerLinkTrustStorePEM = "PEM"
const val PeerLinkTrustStorePKCS12 = "PKCS12"

enum class ClientAuthType {
    NONE, REQUEST, REQUIRED;

    companion object {
        fun fromString(s: String): ClientAuthType? = entries.firstOrNull { it.name.equals(s, ignoreCase = true) }
    }
}

enum class NodeIdOrigin {
    CONFIG, HOSTNAME, FALLBACK
}

// CanonicalNodeID (config.go: lines 1160-1174)
fun canonicalNodeID(id: String): String {
    val c = id.lowercase()
    if (c.isEmpty() || c.length > 64) {
        throw IllegalArgumentException("NodeId \"$id\" must have 1 to 64 characters")
    }
    for (i in c.indices) {
        val b = c[i]
        if (!(b in 'a'..'z' || b in '0'..'9' || b == '.' || b == '_' || b == '-')) {
            throw IllegalArgumentException("NodeId \"$id\" may only contain letters, digits, '.', '_' and '-'")
        }
    }
    return c
}

data class PeerReceiveConfig(
    var include: List<String> = emptyList(),
    var exclude: List<String> = emptyList()
) {
    fun effectiveInclude(): List<String> = if (include.isEmpty()) listOf("#") else include
}

data class PeerTLSConfig(
    var enabled: Boolean? = null,
    var pinnedSha256: List<String> = emptyList(),
    var certificateIdentity: String = "",
    var serverName: String = "",
    var requireClientCert: Boolean = false,
    var insecureSkipVerify: Boolean = false
)

data class PeerConfig(
    var nodeID: String = "",
    var address: String = "",
    var serve: Boolean? = null,
    var sharedSecrets: List<String> = emptyList(),
    var tls: PeerTLSConfig = PeerTLSConfig(),
    var receive: PeerReceiveConfig = PeerReceiveConfig()
) {
    val nodeId: String get() = nodeID
    fun getServe(): Boolean = serve ?: true
    fun pulls(): Boolean = address.isNotEmpty()
}

data class PeerLinkListenerConfig(
    var address: String = "",
    var port: Int = 0,
    var allowedNetworks: List<String> = emptyList(),
    var maxPreAuthPerIp: Int? = null,
    var allowPlaintext: Boolean = false
) {
    fun listenAddress(): String = address.ifEmpty { "0.0.0.0" }
    fun effectivePort(): Int = if (port == 0) PeerLinkDefaultPort else port
    fun getMaxPreAuthPerIp(): Int = maxPreAuthPerIp ?: peerLinkDefaultPreAuthPerIP
}

data class PeerLinkTLSConfig(
    var enabled: Boolean = false,
    var certPath: String = "",
    var keyPath: String = "",
    var trustStorePath: String = "",
    var trustStoreType: String = "",
    var trustStorePassword: String = "",
    var clientAuth: ClientAuthType = ClientAuthType.NONE,
    var identityFallback: String = "",
    var autoGenerate: Boolean = false
) {
    fun effectiveCertPath(): String {
        if (certPath.isEmpty() && autoGenerate) return peerLinkDefaultCertPath
        return certPath
    }

    fun effectiveKeyPath(): String {
        if (keyPath.isEmpty() && autoGenerate) return peerLinkDefaultKeyPath
        return keyPath
    }

    fun effectiveTrustStoreType(): String = trustStoreType.ifEmpty { PeerLinkTrustStorePEM }
    fun effectiveIdentityFallback(): String = identityFallback.ifEmpty { PeerLinkIdentityNone }
}

data class PeerLinkLogConfig(
    var maxMessages: Int? = null,
    var maxBytes: Long? = null,
    var maxRecordBytes: Int = 0,
    var drainOnShutdownMs: Int? = null,
    var neverConnectedWarnSec: Int? = null
) {
    fun getMaxMessages(): Int = maxMessages ?: peerLinkDefaultMaxMessages
    fun getMaxBytes(): Long = maxBytes ?: peerLinkDefaultMaxBytes
    fun getMaxRecordBytes(maxMessageSize: Int): Int {
        if (maxRecordBytes != 0) return maxRecordBytes
        val sz = if (maxMessageSize <= 0) 1 shl 20 else maxMessageSize
        return sz + peerLinkRecordAllowance
    }
    fun getDrainOnShutdownMs(): Int = drainOnShutdownMs ?: peerLinkDefaultDrainMs
    fun getNeverConnectedWarnSec(): Int = neverConnectedWarnSec ?: peerLinkDefaultNeverConnected
}

data class PeerLinkCaptureConfig(
    var wills: Boolean? = null,
    var include: List<String> = emptyList(),
    var exclude: List<String>? = null,
    var echoSuppressMs: Int = 0
) {
    fun getWills(): Boolean = wills ?: true
    fun effectiveInclude(): List<String> = if (include.isEmpty()) listOf("#") else include
    fun getExclude(hmiBase: String): List<String> {
        if (exclude != null) return exclude!!
        val base = (if (hmiBase.isEmpty()) defaultHMISyncBaseTopic else hmiBase).trimEnd('/')
        return listOf("$base/#")
    }
}

data class PeerLinkSnapshotConfig(
    var mode: String = "",
    var maxTopics: Int? = null
) {
    fun effectiveMode(): String = mode.ifEmpty { PeerLinkSnapshotFill }
    fun getMaxTopics(): Int = maxTopics ?: peerLinkDefaultSnapshotTopics
}

data class PeerLinkFetchConfig(
    var maxRecords: Int? = null,
    var maxBytes: Int? = null,
    var maxWaitMs: Int? = null,
    var lingerMs: Int = 0,
    var pipeline: Int? = null,
    var crcOnTls: Boolean = false,
    var reconnectMaxMs: Int? = null
) {
    fun getMaxRecords(): Int = maxRecords ?: peerLinkDefaultFetchRecords
    fun getMaxBytes(): Int = maxBytes ?: peerLinkDefaultFetchBytes
    fun getMaxWaitMs(): Int = maxWaitMs ?: peerLinkDefaultFetchWaitMs
    fun getPipeline(): Int = pipeline ?: peerLinkDefaultPipeline
    fun getReconnectMaxMs(): Int = reconnectMaxMs ?: peerLinkDefaultReconnectMaxMs
}

data class PeerLinkReceiveConfig(
    var bus: Boolean? = null,
    var bridgeOutbound: Boolean = false,
    var archive: Boolean? = null,
    var queue: Boolean = false,
    var sharedSubscriptions: String = "",
    var markReplicas: Boolean = false,
    var catchUpRateFactor: Double? = null,
    var maxApplyRate: Int = 0,
    var maxRecordAgeMs: Int = 0,
    var maxFrameBytes: Int? = null,
    var injectWorkers: Int? = null
) {
    fun getBus(): Boolean = bus ?: true
    fun getArchive(): Boolean = archive ?: true
    fun effectiveSharedSubscriptions(): String = sharedSubscriptions.ifEmpty { PeerLinkSharedSkip }
    fun getCatchUpRateFactor(): Double = catchUpRateFactor ?: peerLinkDefaultCatchUpFactor
    fun getMaxFrameBytes(): Int = maxFrameBytes ?: peerLinkDefaultMaxFrameBytes
    fun getInjectWorkers(): Int = injectWorkers ?: peerLinkDefaultInjectWorkers
}

data class PeerLinkConfig(
    var enabled: Boolean = false,
    var allowUnauthenticatedPeers: Boolean = false,
    var listener: PeerLinkListenerConfig = PeerLinkListenerConfig(),
    var tls: PeerLinkTLSConfig = PeerLinkTLSConfig(),
    var sharedSecrets: List<String> = emptyList(),
    var keepAliveSeconds: Int? = null,
    var log: PeerLinkLogConfig = PeerLinkLogConfig(),
    var capture: PeerLinkCaptureConfig = PeerLinkCaptureConfig(),
    var snapshot: PeerLinkSnapshotConfig = PeerLinkSnapshotConfig(),
    var fetch: PeerLinkFetchConfig = PeerLinkFetchConfig(),
    var receive: PeerLinkReceiveConfig = PeerLinkReceiveConfig(),
    var peers: List<PeerConfig> = emptyList()
) {
    fun getKeepAliveSeconds(): Int = keepAliveSeconds ?: peerLinkDefaultKeepAlive

    fun dialerTLS(peer: PeerConfig): Boolean = peer.tls.enabled ?: tls.enabled

    fun secretsFor(peer: PeerConfig): List<String> =
        if (peer.sharedSecrets.isNotEmpty()) peer.sharedSecrets else sharedSecrets
}

data class PeerLinkSetup(
    var nodeID: String = "",
    var peers: List<PeerConfig> = emptyList(),
    val infos: MutableList<String> = mutableListOf(),
    val warnings: MutableList<String> = mutableListOf()
) {
    fun anyServe(): Boolean = peers.any { it.getServe() }
}

data class PeerLinkEnv(
    val nodeID: String,
    val nodeIDOrigin: NodeIdOrigin,
    val hostname: String,
    val userMgmt: Boolean = false,
    val retainedStore: String = "DB", // MEMORY or DB
    val tcpsTrustStore: String = "",
    val maxMessageSize: Int = 512 * 1024,
    val hmiBase: String = defaultHMISyncBaseTopic,
    val memoryLimitMB: Long = 0L
)

object PeerLinkConfigParser {

    private val allowedPeerLinkKeys = setOf(
        "Enabled", "AllowUnauthenticatedPeers", "Listener", "Tls", "SharedSecrets",
        "KeepAliveSeconds", "Log", "Capture", "Snapshot", "Fetch", "Receive", "Peers"
    )
    private val allowedListenerKeys = setOf("Address", "Port", "AllowedNetworks", "MaxPreAuthPerIp", "AllowPlaintext")
    private val allowedTLSKeys = setOf(
        "Enabled", "CertPath", "KeyPath", "TrustStorePath", "TrustStoreType",
        "TrustStorePassword", "ClientAuth", "IdentityFallback", "AutoGenerate"
    )
    private val allowedLogKeys = setOf("MaxMessages", "MaxBytes", "MaxRecordBytes", "DrainOnShutdownMs", "NeverConnectedWarnSec")
    private val allowedCaptureKeys = setOf("Wills", "Include", "Exclude", "EchoSuppressMs")
    private val allowedSnapshotKeys = setOf("Mode", "MaxTopics")
    private val allowedFetchKeys = setOf("MaxRecords", "MaxBytes", "MaxWaitMs", "LingerMs", "Pipeline", "CrcOnTls", "ReconnectMaxMs")
    private val allowedReceiveKeys = setOf(
        "Bus", "BridgeOutbound", "Archive", "Queue", "SharedSubscriptions", "MarkReplicas",
        "CatchUpRateFactor", "MaxApplyRate", "MaxRecordAgeMs", "MaxFrameBytes", "InjectWorkers"
    )
    private val allowedPeerKeys = setOf("NodeId", "Address", "Serve", "SharedSecrets", "Tls", "Receive")
    private val allowedPeerTLSKeys = setOf(
        "Enabled", "PinnedSha256", "CertificateIdentity", "ServerName", "RequireClientCert", "InsecureSkipVerify"
    )
    private val allowedPeerReceiveKeys = setOf("Include", "Exclude")

    fun checkUnknownKeys(obj: JsonObject, path: String = "PeerLink") {
        for (key in obj.fieldNames()) {
            if (!allowedPeerLinkKeys.contains(key)) {
                throw IllegalArgumentException("Unknown configuration key: $path.$key")
            }
        }
        obj.getJsonObject("Listener")?.let { checkSub(it, allowedListenerKeys, "$path.Listener") }
        obj.getJsonObject("Tls")?.let { checkSub(it, allowedTLSKeys, "$path.Tls") }
        obj.getJsonObject("Log")?.let { checkSub(it, allowedLogKeys, "$path.Log") }
        obj.getJsonObject("Capture")?.let { checkSub(it, allowedCaptureKeys, "$path.Capture") }
        obj.getJsonObject("Snapshot")?.let { checkSub(it, allowedSnapshotKeys, "$path.Snapshot") }
        obj.getJsonObject("Fetch")?.let { checkSub(it, allowedFetchKeys, "$path.Fetch") }
        obj.getJsonObject("Receive")?.let { checkSub(it, allowedReceiveKeys, "$path.Receive") }
        obj.getJsonArray("Peers")?.let { arr ->
            for (i in 0 until arr.size()) {
                val peerObj = arr.getJsonObject(i) ?: continue
                checkSub(peerObj, allowedPeerKeys, "$path.Peers[$i]")
                peerObj.getJsonObject("Tls")?.let { checkSub(it, allowedPeerTLSKeys, "$path.Peers[$i].Tls") }
                peerObj.getJsonObject("Receive")?.let { checkSub(it, allowedPeerReceiveKeys, "$path.Peers[$i].Receive") }
            }
        }
    }

    private fun checkSub(obj: JsonObject, allowed: Set<String>, path: String) {
        for (k in obj.fieldNames()) {
            if (!allowed.contains(k)) {
                throw IllegalArgumentException("Unknown configuration key: $path.$k")
            }
        }
    }

    fun parse(obj: JsonObject): PeerLinkConfig {
        checkUnknownKeys(obj)
        val p = PeerLinkConfig()
        p.enabled = obj.getBoolean("Enabled", false)
        p.allowUnauthenticatedPeers = obj.getBoolean("AllowUnauthenticatedPeers", false)
        if (obj.containsKey("KeepAliveSeconds")) p.keepAliveSeconds = obj.getInteger("KeepAliveSeconds")

        obj.getJsonArray("SharedSecrets")?.let { arr ->
            p.sharedSecrets = arr.map { it.toString() }
        }

        obj.getJsonObject("Listener")?.let { l ->
            p.listener.address = l.getString("Address", "")
            p.listener.port = l.getInteger("Port", 0)
            l.getJsonArray("AllowedNetworks")?.let { arr ->
                p.listener.allowedNetworks = arr.map { it.toString() }
            }
            if (l.containsKey("MaxPreAuthPerIp")) p.listener.maxPreAuthPerIp = l.getInteger("MaxPreAuthPerIp")
            p.listener.allowPlaintext = l.getBoolean("AllowPlaintext", false)
        }

        obj.getJsonObject("Tls")?.let { t ->
            p.tls.enabled = t.getBoolean("Enabled", false)
            p.tls.certPath = t.getString("CertPath", "")
            p.tls.keyPath = t.getString("KeyPath", "")
            p.tls.trustStorePath = t.getString("TrustStorePath", "")
            p.tls.trustStoreType = t.getString("TrustStoreType", "")
            p.tls.trustStorePassword = t.getString("TrustStorePassword", "")
            t.getString("ClientAuth")?.let { ca ->
                p.tls.clientAuth = ClientAuthType.fromString(ca) ?: ClientAuthType.NONE
            }
            p.tls.identityFallback = t.getString("IdentityFallback", "")
            p.tls.autoGenerate = t.getBoolean("AutoGenerate", false)
        }

        obj.getJsonObject("Log")?.let { l ->
            if (l.containsKey("MaxMessages")) p.log.maxMessages = l.getInteger("MaxMessages")
            if (l.containsKey("MaxBytes")) p.log.maxBytes = l.getLong("MaxBytes")
            p.log.maxRecordBytes = l.getInteger("MaxRecordBytes", 0)
            if (l.containsKey("DrainOnShutdownMs")) p.log.drainOnShutdownMs = l.getInteger("DrainOnShutdownMs")
            if (l.containsKey("NeverConnectedWarnSec")) p.log.neverConnectedWarnSec = l.getInteger("NeverConnectedWarnSec")
        }

        obj.getJsonObject("Capture")?.let { c ->
            if (c.containsKey("Wills")) p.capture.wills = c.getBoolean("Wills")
            c.getJsonArray("Include")?.let { arr -> p.capture.include = arr.map { it.toString() } }
            if (c.containsKey("Exclude")) {
                val ex = c.getJsonArray("Exclude")
                p.capture.exclude = ex?.map { it.toString() } ?: emptyList()
            }
            p.capture.echoSuppressMs = c.getInteger("EchoSuppressMs", 0)
        }

        obj.getJsonObject("Snapshot")?.let { s ->
            p.snapshot.mode = s.getString("Mode", "")
            if (s.containsKey("MaxTopics")) p.snapshot.maxTopics = s.getInteger("MaxTopics")
        }

        obj.getJsonObject("Fetch")?.let { f ->
            if (f.containsKey("MaxRecords")) p.fetch.maxRecords = f.getInteger("MaxRecords")
            if (f.containsKey("MaxBytes")) p.fetch.maxBytes = f.getInteger("MaxBytes")
            if (f.containsKey("MaxWaitMs")) p.fetch.maxWaitMs = f.getInteger("MaxWaitMs")
            p.fetch.lingerMs = f.getInteger("LingerMs", 0)
            if (f.containsKey("Pipeline")) p.fetch.pipeline = f.getInteger("Pipeline")
            p.fetch.crcOnTls = f.getBoolean("CrcOnTls", false)
            if (f.containsKey("ReconnectMaxMs")) p.fetch.reconnectMaxMs = f.getInteger("ReconnectMaxMs")
        }

        obj.getJsonObject("Receive")?.let { r ->
            if (r.containsKey("Bus")) p.receive.bus = r.getBoolean("Bus")
            p.receive.bridgeOutbound = r.getBoolean("BridgeOutbound", false)
            if (r.containsKey("Archive")) p.receive.archive = r.getBoolean("Archive")
            p.receive.queue = r.getBoolean("Queue", false)
            p.receive.sharedSubscriptions = r.getString("SharedSubscriptions", "")
            p.receive.markReplicas = r.getBoolean("MarkReplicas", false)
            if (r.containsKey("CatchUpRateFactor")) p.receive.catchUpRateFactor = r.getDouble("CatchUpRateFactor")
            p.receive.maxApplyRate = r.getInteger("MaxApplyRate", 0)
            p.receive.maxRecordAgeMs = r.getInteger("MaxRecordAgeMs", 0)
            if (r.containsKey("MaxFrameBytes")) p.receive.maxFrameBytes = r.getInteger("MaxFrameBytes")
            if (r.containsKey("InjectWorkers")) p.receive.injectWorkers = r.getInteger("InjectWorkers")
        }

        obj.getJsonArray("Peers")?.let { arr ->
            val peerList = mutableListOf<PeerConfig>()
            for (i in 0 until arr.size()) {
                val po = arr.getJsonObject(i) ?: continue
                val pc = PeerConfig()
                pc.nodeID = po.getString("NodeId", "")
                pc.address = po.getString("Address", "")
                if (po.containsKey("Serve")) pc.serve = po.getBoolean("Serve")
                po.getJsonArray("SharedSecrets")?.let { ss -> pc.sharedSecrets = ss.map { it.toString() } }

                po.getJsonObject("Tls")?.let { pt ->
                    if (pt.containsKey("Enabled")) pc.tls.enabled = pt.getBoolean("Enabled")
                    pt.getJsonArray("PinnedSha256")?.let { pa -> pc.tls.pinnedSha256 = pa.map { it.toString() } }
                    pc.tls.certificateIdentity = pt.getString("CertificateIdentity", "")
                    pc.tls.serverName = pt.getString("ServerName", "")
                    pc.tls.requireClientCert = pt.getBoolean("RequireClientCert", false)
                    pc.tls.insecureSkipVerify = pt.getBoolean("InsecureSkipVerify", false)
                }

                po.getJsonObject("Receive")?.let { pr ->
                    pr.getJsonArray("Include")?.let { inc -> pc.receive.include = inc.map { it.toString() } }
                    pr.getJsonArray("Exclude")?.let { exc -> pc.receive.exclude = exc.map { it.toString() } }
                }
                peerList.add(pc)
            }
            p.peers = peerList
        }
        return p
    }
}

// Validation logic (config.go: lines 1230-1572)
fun validatePeerLink(p: PeerLinkConfig, env: PeerLinkEnv): PeerLinkSetup {
    val errs = mutableListOf<String>()
    fun fail(fmt: String, vararg args: Any?) {
        errs.add("PeerLink." + String.format(fmt, *args))
    }

    val s = PeerLinkSetup()

    if (env.nodeIDOrigin == NodeIdOrigin.FALLBACK) {
        errs.add("PeerLink needs a NodeId that differs per host: the hostname is unknown and the fallback \"${env.nodeID}\" is the same everywhere; set NodeId")
    } else {
        try {
            s.nodeID = canonicalNodeID(env.nodeID)
        } catch (e: Exception) {
            errs.add("PeerLink: ${e.message}")
        }
    }

    val tls = p.tls
    val clientAuth = tls.clientAuth
    when (clientAuth) {
        ClientAuthType.NONE, ClientAuthType.REQUEST, ClientAuthType.REQUIRED -> {}
    }
    when (tls.effectiveTrustStoreType()) {
        PeerLinkTrustStorePEM, PeerLinkTrustStorePKCS12 -> {}
        else -> fail("Tls.TrustStoreType \"%s\" must be PEM or PKCS12", tls.trustStoreType)
    }
    when (tls.effectiveIdentityFallback()) {
        PeerLinkIdentityNone, PeerLinkIdentityDNS, PeerLinkIdentityCN -> {}
        else -> fail("Tls.IdentityFallback \"%s\" must be NONE, DNS or CN", tls.identityFallback)
    }
    if (tls.enabled && !tls.autoGenerate && (tls.certPath.isEmpty() || tls.keyPath.isEmpty())) {
        fail("Tls.Enabled needs Tls.CertPath and Tls.KeyPath, or Tls.AutoGenerate")
    }
    if (clientAuth != ClientAuthType.NONE && !tls.enabled) {
        fail("Tls.ClientAuth %s needs Tls.Enabled", clientAuth)
    }
    for ((i, sec) in p.sharedSecrets.withIndex()) {
        val err = checkPeerSecret(sec)
        if (err != null) {
            fail("SharedSecrets[%d]: %s", i, err)
        }
    }
    if (p.sharedSecrets.isNotEmpty() && !tls.enabled) {
        fail("SharedSecrets need Tls.Enabled: without TLS the secret cannot be bound to the connection")
    }

    val l = p.listener
    if (l.port < 0 || l.port > 65535) {
        fail("Listener.Port %d must be 1..65535 (0 = %d)", l.port, PeerLinkDefaultPort)
    }
    for ((i, n) in l.allowedNetworks.withIndex()) {
        if (!validCIDR(n)) {
            fail("Listener.AllowedNetworks[%d] \"%s\" is not a CIDR", i, n)
        }
    }
    if (l.getMaxPreAuthPerIp() < 1) {
        fail("Listener.MaxPreAuthPerIp must be at least 1")
    }

    val waiver = p.allowUnauthenticatedPeers
    if (waiver) {
        if (l.allowedNetworks.isEmpty()) {
            fail("AllowUnauthenticatedPeers needs a non-empty Listener.AllowedNetworks")
        }
        if (env.userMgmt) {
            fail("AllowUnauthenticatedPeers is not allowed with UserManagement.Enabled: replicas are injected without ACL checks")
        }
    }

    var hostLabel = ""
    if (env.nodeIDOrigin == NodeIdOrigin.HOSTNAME) {
        val first = env.hostname.split('.')[0]
        hostLabel = first.lowercase()
    }

    val seen = mutableMapOf<String, Int>()
    var others = 0
    var ownMatched = false
    var exactOwn = false
    var adopted = ""
    var groupSecretUsers = 0

    val filteredPeers = mutableListOf<PeerConfig>()
    for ((i, peer) in p.peers.withIndex()) {
        val id: String
        try {
            id = canonicalNodeID(peer.nodeID)
        } catch (e: Exception) {
            fail("Peers[%d]: %s", i, e.message)
            others++
            continue
        }
        if (seen.containsKey(id)) {
            fail("Peers[%d]: NodeId \"%s\" is already used by Peers[%d] (NodeIds are compared in lower case)", i, peer.nodeID, seen[id])
            continue
        }
        seen[id] = i
        if (s.nodeID.isNotEmpty() && (id == s.nodeID || (hostLabel.isNotEmpty() && id == hostLabel))) {
            ownMatched = true
            if (id == s.nodeID) {
                exactOwn = true
            } else {
                adopted = id
            }
            s.infos.add("PeerLink: Peers[$i] \"${peer.nodeID}\" is this node and is ignored")
            continue
        }
        others++
        val peerCopy = peer.copy(nodeID = id)
        val where = "Peers[$i] ($id)"
        val serve = peerCopy.getServe()
        val pull = peerCopy.pulls()
        if (!serve && !pull) {
            fail("%s needs an Address (this node pulls from it) or Serve: true (it pulls from this node)", where)
        }
        if (pull) {
            val err = checkPeerAddress(peerCopy.address)
            if (err != null) {
                fail("%s.Address: %s", where, err)
            }
        }
        for ((k, sec) in peerCopy.sharedSecrets.withIndex()) {
            val err = checkPeerSecret(sec)
            if (err != null) {
                fail("%s.SharedSecrets[%d]: %s", where, k, err)
            }
        }
        for ((k, pin) in peerCopy.tls.pinnedSha256.withIndex()) {
            if (!validPeerPin(pin)) {
                fail("%s.Tls.PinnedSha256[%d] \"%s\" must be 64 hex digits (SHA-256 of the SPKI or certificate)", where, k, pin)
            }
        }
        for ((k, f) in peerCopy.receive.effectiveInclude().withIndex()) {
            if (!validPeerFilter(f)) {
                fail("%s.Receive.Include[%d] \"%s\" is not a valid topic filter", where, k, f)
            }
        }
        for ((k, f) in peerCopy.receive.exclude.withIndex()) {
            if (!validPeerFilter(f)) {
                fail("%s.Receive.Exclude[%d] \"%s\" is not a valid topic filter", where, k, f)
            }
        }

        val secrets = p.secretsFor(peerCopy)
        if (peerCopy.sharedSecrets.isEmpty() && p.sharedSecrets.isNotEmpty()) {
            groupSecretUsers++
        }
        val dialTLS = pull && p.dialerTLS(peerCopy)
        val listenTLS = serve && tls.enabled
        val pins = peerCopy.tls.pinnedSha256.isNotEmpty()
        val trust = tls.trustStorePath.isNotEmpty() || pins

        if (secrets.isNotEmpty() && ((serve && !tls.enabled) || (pull && !dialTLS))) {
            fail("%s: SharedSecrets need TLS in every direction they are used (Tls.Enabled for Serve, the dialer Tls.Enabled for Address)", where)
        }
        if (pins && !dialTLS && !listenTLS) {
            fail("%s.Tls.PinnedSha256 needs TLS", where)
        }
        if (peerCopy.tls.requireClientCert && clientAuth == ClientAuthType.NONE) {
            fail("%s.Tls.RequireClientCert needs Tls.ClientAuth REQUEST or REQUIRED", where)
        }
        if (serve && clientAuth != ClientAuthType.NONE && !trust) {
            fail("%s: Tls.ClientAuth %s needs Tls.TrustStorePath or PinnedSha256 for this peer", where, clientAuth)
        }
        if (dialTLS && !peerCopy.tls.insecureSkipVerify && !trust && secrets.isEmpty()) {
            fail("%s: the dialer has no truststore, pin or shared secret to verify the peer; set Tls.InsecureSkipVerify: true to connect unauthenticated", where)
        }
        if (!waiver) {
            val certRequired = clientAuth == ClientAuthType.REQUIRED || (clientAuth == ClientAuthType.REQUEST && peerCopy.tls.requireClientCert)
            if (serve && !(listenTLS && (secrets.isNotEmpty() || (certRequired && trust)))) {
                fail("%s is not authenticated when it pulls from this node: use TLS with a client certificate (Tls.ClientAuth REQUIRED, or REQUEST with RequireClientCert) or SharedSecrets, or set AllowUnauthenticatedPeers", where)
            }
            if (pull && !(dialTLS && (secrets.isNotEmpty() || (!peerCopy.tls.insecureSkipVerify && trust)))) {
                fail("%s is not authenticated when this node pulls from it: use TLS with Tls.TrustStorePath or PinnedSha256, or SharedSecrets, or set AllowUnauthenticatedPeers", where)
            }
        }
        filteredPeers.add(peerCopy)
    }

    if (others == 0) {
        fail("Peers: at least one peer other than this node is required")
    }
    if (adopted.isNotEmpty() && !exactOwn) {
        s.infos.add("PeerLink: NodeId \"${s.nodeID}\" (from the hostname) is used as \"$adopted\", its Peers entry")
        s.nodeID = adopted
    }
    if (l.allowPlaintext && tls.enabled && s.anyServe() && !waiver) {
        fail("Listener.AllowPlaintext admits unauthenticated plaintext sessions; it needs AllowUnauthenticatedPeers")
    }
    if (p.peers.size >= 2 && !ownMatched && s.nodeID.isNotEmpty()) {
        s.warnings.add("PeerLink: no Peers entry matches this node (NodeId \"${s.nodeID}\", hostname \"${env.hostname}\"); if this file is shared, no entry matches this host")
    }
    s.peers = filteredPeers

    val maxRecord = p.log.getMaxRecordBytes(env.maxMessageSize)
    val maxBytes = p.log.getMaxBytes()
    val fetchRecords = p.fetch.getMaxRecords()
    if (p.log.maxRecordBytes < 0) {
        fail("Log.MaxRecordBytes must be non-negative (0 = MaxMessageSize + 64 KiB)")
    }
    if (p.log.getMaxMessages() < maxOf(100, fetchRecords)) {
        fail("Log.MaxMessages %d must be at least 100 and at least Fetch.MaxRecords (%d)", p.log.getMaxMessages(), fetchRecords)
    }
    if (maxBytes < (1L shl 20) || maxBytes < 4L * maxRecord.toLong()) {
        fail("Log.MaxBytes %d must be at least 1 MiB and at least 4 x MaxRecordBytes (%d)", maxBytes, maxRecord)
    }
    if (p.log.getDrainOnShutdownMs() < 0) {
        fail("Log.DrainOnShutdownMs must be non-negative")
    }
    if (p.log.getNeverConnectedWarnSec() < 0) {
        fail("Log.NeverConnectedWarnSec must be non-negative")
    }
    val keepAlive = p.getKeepAliveSeconds()
    if (keepAlive < 1) {
        fail("KeepAliveSeconds must be at least 1")
    }

    for ((k, f) in p.capture.effectiveInclude().withIndex()) {
        if (!validPeerFilter(f)) {
            fail("Capture.Include[%d] \"%s\" is not a valid topic filter", k, f)
        }
    }
    for ((k, f) in p.capture.getExclude(env.hmiBase).withIndex()) {
        if (!validPeerFilter(f)) {
            fail("Capture.Exclude[%d] \"%s\" is not a valid topic filter", k, f)
        }
    }
    if (p.capture.echoSuppressMs < 0) {
        fail("Capture.EchoSuppressMs must be non-negative")
    }

    when (p.snapshot.effectiveMode()) {
        PeerLinkSnapshotFill, PeerLinkSnapshotOff -> {}
        else -> fail("Snapshot.Mode \"%s\" must be FILL or OFF", p.snapshot.mode)
    }
    if (p.snapshot.getMaxTopics() < 1) {
        fail("Snapshot.MaxTopics must be at least 1")
    }

    val f = p.fetch
    if (fetchRecords < 1) {
        fail("Fetch.MaxRecords must be at least 1")
    }
    if (f.getMaxBytes() < 1) {
        fail("Fetch.MaxBytes must be at least 1")
    }
    val w = f.getMaxWaitMs()
    if (w < PeerLinkMinFetchWaitMs || w >= keepAlive * 1000) {
        fail("Fetch.MaxWaitMs %d must be at least %d and below KeepAliveSeconds*1000 (%d); a shorter long poll turns an idle link into a busy loop",
            w, PeerLinkMinFetchWaitMs, keepAlive * 1000)
    }
    if (f.lingerMs < 0) {
        fail("Fetch.LingerMs must be non-negative")
    }
    if (f.getPipeline() !in 1..2) {
        fail("Fetch.Pipeline %d must be 1 or 2", f.getPipeline())
    }
    if (f.getReconnectMaxMs() < 1) {
        fail("Fetch.ReconnectMaxMs must be positive")
    }

    val r = p.receive
    when (r.effectiveSharedSubscriptions()) {
        PeerLinkSharedSkip, PeerLinkSharedDeliver -> {}
        else -> fail("Receive.SharedSubscriptions \"%s\" must be SKIP or DELIVER", r.sharedSubscriptions)
    }
    val cf = r.getCatchUpRateFactor()
    if (cf != 0.0 && cf < 1.5) {
        fail("Receive.CatchUpRateFactor %s must be 0 (no pacing) or at least 1.5", cf)
    }
    if (r.maxApplyRate < 0) {
        fail("Receive.MaxApplyRate must be non-negative")
    }
    if (r.maxRecordAgeMs < 0) {
        fail("Receive.MaxRecordAgeMs must be non-negative")
    }
    val mf = r.getMaxFrameBytes()
    val need = f.getMaxBytes() + peerLinkRecordAllowance
    if (mf < need) {
        fail("Receive.MaxFrameBytes %d must be at least Fetch.MaxBytes + 64 KiB (%d)", mf, need)
    }
    if (r.getInjectWorkers() !in 1..16) {
        fail("Receive.InjectWorkers %d must be 1..16", r.getInjectWorkers())
    }

    if (errs.isNotEmpty()) {
        throw IllegalArgumentException(errs.joinToString("; "))
    }

    if (waiver) {
        s.warnings.add("PeerLink: AllowUnauthenticatedPeers is set; peers are admitted by network address only (${l.allowedNetworks.joinToString(", ")})")
    }
    if (groupSecretUsers > 0 && s.peers.size + 1 > 2) {
        s.warnings.add("PeerLink: the group SharedSecrets are used with more than two nodes; any holder can claim any NodeId of the group, prefer per-peer SharedSecrets")
    }
    if (tls.trustStorePath.isNotEmpty() && env.tcpsTrustStore.isNotEmpty()) {
        val expanded = tls.trustStorePath.replace("{NodeId}", s.nodeID)
        if (File(expanded).canonicalPath == File(env.tcpsTrustStore).canonicalPath) {
            s.warnings.add("PeerLink: Tls.TrustStorePath is the TCPS truststore; a certificate issued for an MQTT client could claim a NodeId, use a dedicated peer CA")
        }
    }
    if (env.retainedStore == "MEMORY" && p.snapshot.effectiveMode() == PeerLinkSnapshotOff) {
        s.warnings.add("PeerLink: RetainedStoreType MEMORY with Snapshot.Mode OFF: retained messages of peers are lost when this node restarts")
    }
    if (env.memoryLimitMB > 0) {
        val needMB = 2.2 * maxBytes.toDouble() / (1024 * 1024) + 150
        if (needMB > env.memoryLimitMB) {
            s.warnings.add(String.format("PeerLink: Runtime.MemoryLimitMB %d is below 2.2 x Log.MaxBytes + 150 MiB (%.0f MiB); the GC may run continuously while the log is full", env.memoryLimitMB, needMB))
        }
    }

    return s
}

fun checkPeerAddress(addr: String): String? {
    val colon = addr.lastIndexOf(':')
    if (colon < 0) return "\"$addr\" has no port"
    val host = addr.substring(0, colon).trim('[', ']')
    val portStr = addr.substring(colon + 1)
    if (host.isEmpty()) return "\"$addr\" has no host"
    val port = portStr.toIntOrNull()
    if (port == null || port !in 1..65535) return "\"$addr\": port must be 1..65535"
    return null
}

fun checkPeerSecret(s: String): String? {
    val clean = s.trim()
    val decoded = decodeSecretBytes(clean) ?: return "shared secret is not base64"
    if (decoded.size < 16) return "shared secret has ${decoded.size} bytes, need at least 16"
    return null
}

fun decodeSecretBytes(s: String): ByteArray? {
    val encoders = listOf(
        Base64.getDecoder(),
        Base64.getUrlDecoder()
    )
    for (dec in encoders) {
        try {
            return dec.decode(s)
        } catch (_: Exception) {}
    }
    // Also try padding if unpadded
    val rem = s.length % 4
    if (rem != 0) {
        val padded = s + "=".repeat(4 - rem)
        for (dec in encoders) {
            try {
                return dec.decode(padded)
            } catch (_: Exception) {}
        }
    }
    return null
}

fun validPeerPin(s: String): Boolean {
    val clean = s.replace(":", "").replace(" ", "").trim()
    if (clean.length != 64) return false
    for (c in clean) {
        if (!(c in '0'..'9' || c in 'a'..'f' || c in 'A'..'F')) return false
    }
    return true
}

fun parsePeerPin(s: String): ByteArray {
    val clean = s.replace(":", "").replace(" ", "").trim().lowercase()
    if (clean.length != 64) throw IllegalArgumentException("pin must be 64 hex digits")
    val res = ByteArray(32)
    for (i in 0 until 32) {
        res[i] = clean.substring(i * 2, i * 2 + 2).toInt(16).toByte()
    }
    return res
}

fun validPeerFilter(f: String): Boolean {
    if (f.isEmpty() || f.contains(0.toChar()) || f.length > 65535) return false
    val levels = f.split('/')
    for ((i, lv) in levels.withIndex()) {
        if ((lv.contains('+') || lv.contains('#')) && lv != "+" && lv != "#") return false
        if (lv == "#" && i != levels.size - 1) return false
    }
    return true
}

fun validCIDR(s: String): Boolean {
    val parts = s.split('/')
    if (parts.size != 2) return false
    val prefixLen = parts[1].toIntOrNull() ?: return false
    try {
        val ip = InetAddress.getByName(parts[0])
        val maxPrefix = if (ip.address.size == 4) 32 else 128
        return prefixLen in 0..maxPrefix
    } catch (_: Exception) {
        return false
    }
}
