package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Version
import at.rocworks.peerlink.PeerLinkManager
import at.rocworks.peerlink.Status
import at.rocworks.peerlink.config.PeerConfig
import at.rocworks.peerlink.config.PeerLinkInterestOff
import at.rocworks.peerlink.wire.BrokerTypeFull
import at.rocworks.peerlink.wire.VersionMajor
import at.rocworks.peerlink.wire.VersionMinor
import at.rocworks.peerlink.wire.protocolVersion
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import graphql.schema.DataFetcher
import io.vertx.core.Vertx

data class PeerLinkInfo(
    val enabled: Boolean,
    val nodeId: String,
    val listen: String?,
    val tls: Boolean,
    val brokerType: String,
    val brokerVersion: String,
    val protocolVersion: String,
    val peers: List<PeerLinkPeer>,
    val status: Map<String, Any?>?
)

data class PeerLinkPeer(
    val nodeId: String,
    val address: String?,
    val pull: Boolean,
    val serve: Boolean,
    val interest: String,
    val pullState: String?,
    val serveState: String?,
    val remote: String?,
    val lastError: String?,
    val brokerType: String?,
    val brokerVersion: String?,
    val protocolVersion: String?,
    val source: Map<String, Any?>?,
    val consumer: Map<String, Any?>?
)

/**
 * GraphQL query for the PeerLink configuration and link state of this node (schema-peerlink.graphqls).
 * The edge broker answers the same query.
 */
class PeerLinkQueries(private val vertx: Vertx) {

    fun peerLink(): DataFetcher<PeerLinkInfo> = DataFetcher {
        val manager = Monster.getPeerLinkManager()
        if (manager == null) {
            disabled(Monster.getClusterNodeId(vertx))
        } else {
            build(manager.setup.peers, toDocument(manager.status()), manager.setup.anyServe())
        }
    }

    companion object {
        private val mapper = ObjectMapper()
        private val mapType = object : TypeReference<Map<String, Any?>>() {}

        fun disabled(nodeId: String) = PeerLinkInfo(
            enabled = false, nodeId = nodeId, listen = null, tls = false,
            brokerType = BrokerTypeFull, brokerVersion = Version.getVersion(),
            protocolVersion = protocolVersion(VersionMajor, VersionMinor), peers = emptyList(), status = null
        )

        // Through JSON so that the document matches GET /peerlink/v1/status (unsigned epochs).
        fun toDocument(status: Status): Map<String, Any?> =
            mapper.readValue(mapper.writeValueAsString(status), mapType)

        /** Merges the configured peers with the source and consumer entries of the status document. */
        fun build(peers: List<PeerConfig>, doc: Map<String, Any?>, listening: Boolean): PeerLinkInfo {
            val sources = entriesById(doc["sources"])
            val consumers = entriesById(doc["consumers"])
            return PeerLinkInfo(
                enabled = doc["enabled"] as? Boolean ?: true,
                nodeId = doc["nodeId"] as? String ?: "",
                listen = if (listening) (doc["listen"] as? String)?.ifEmpty { null } else null,
                tls = doc["tls"] as? Boolean ?: false,
                brokerType = doc["brokerType"] as? String ?: BrokerTypeFull,
                brokerVersion = doc["brokerVersion"] as? String ?: Version.getVersion(),
                protocolVersion = doc["protocolVersion"] as? String ?: protocolVersion(VersionMajor, VersionMinor),
                peers = peers.map { peer ->
                    val id = peer.nodeId.trim().lowercase()
                    val source = if (peer.pulls()) sources[id] else null
                    val consumer = if (peer.getServe()) consumers[id] else null
                    // What the peer announced; the pull link's handshake first, else the serve link's.
                    val announced = listOfNotNull(source, consumer)
                        .firstOrNull { !(it["peerProtocolVersion"] as? String).isNullOrEmpty() }
                    PeerLinkPeer(
                        nodeId = id,
                        address = peer.address.ifEmpty { null },
                        pull = peer.pulls(),
                        serve = peer.getServe(),
                        interest = if (peer.interestOff()) PeerLinkInterestOff else "INHERIT",
                        pullState = if (peer.pulls()) (source?.get("state") as? String ?: "STOPPED") else null,
                        serveState = if (peer.getServe()) (consumer?.get("state") as? String ?: "NEVER_CONNECTED") else null,
                        remote = (consumer?.get("remote") as? String)?.ifEmpty { null },
                        lastError = (source?.get("lastError") as? String)?.ifEmpty { null },
                        brokerType = (announced?.get("peerBrokerType") as? String)?.ifEmpty { null },
                        brokerVersion = (announced?.get("peerBrokerVersion") as? String)?.ifEmpty { null },
                        protocolVersion = announced?.get("peerProtocolVersion") as? String,
                        source = source,
                        consumer = consumer
                    )
                },
                status = doc
            )
        }

        @Suppress("UNCHECKED_CAST")
        private fun entriesById(list: Any?): Map<String, Map<String, Any?>> =
            (list as? List<Map<String, Any?>>).orEmpty().associateBy { (it["nodeId"] as? String).orEmpty() }
    }
}
