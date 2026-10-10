package at.rocworks.peerlink

import at.rocworks.peerlink.config.PeerConfig
import at.rocworks.peerlink.core.LogConsumerState
import at.rocworks.peerlink.core.LogConsumerStats
import at.rocworks.peerlink.tls.PeerIdentity
import org.junit.Assert.assertEquals
import org.junit.Test
import java.net.InetAddress
import java.net.InetSocketAddress

// The consumer entries of the status document match the edge broker: upper-case states and host:port remotes.
class ConsumerStatusTest {

    @Test
    fun testStateNamesMatchEdge() {
        val slot = ConsumerSlot(0, "edge-a", PeerConfig(nodeID = "edge-a"), PeerIdentity(nodeId = "edge-a"), emptyList())
        val states = LogConsumerState.values().map { slot.status(LogConsumerStats("edge-a", it, 0, 0, 0, 0)).state }
        assertEquals(listOf("NEVER_CONNECTED", "CONNECTED", "DISCONNECTED"), states)
    }

    @Test
    fun testRemoteIsHostPort() {
        assertEquals("127.0.0.1:42696", remoteString(InetSocketAddress(InetAddress.getByName("127.0.0.1"), 42696)))
        assertEquals("[0:0:0:0:0:0:0:1]:1890", remoteString(InetSocketAddress(InetAddress.getByName("::1"), 1890)))
        assertEquals("", remoteString(null))
    }
}
