package at.rocworks.peerlink.config

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.junit.Assert.*
import org.junit.Test

class PeerLinkConfigTest {

    private val testSecret = "cGVlcmxpbmstdGVzdC1ncm91cC1zZWNyZXQtdjEtMDEyMw=="
    private val testPin = "b6572a5c1146070ae6af4d3cf7a308a2bb0038b20f06081cf726a56c20434f2c"

    private fun validPeerLink(): PeerLinkConfig {
        val p = PeerLinkConfig()
        p.enabled = true
        p.tls.enabled = true
        p.tls.autoGenerate = true
        p.sharedSecrets = listOf(testSecret)
        p.peers = listOf(PeerConfig(nodeID = "node-b", address = "node-b.local:1890"))
        return p
    }

    private fun defaultEnv(nodeId: String = "node-a", hostname: String = "node-a.local"): PeerLinkEnv {
        return PeerLinkEnv(
            nodeID = nodeId,
            nodeIDOrigin = NodeIdOrigin.CONFIG,
            hostname = hostname
        )
    }

    @Test
    fun testPeerLinkDefaults() {
        val p = PeerLinkConfig()
        assertFalse(p.enabled)
        assertFalse(p.allowUnauthenticatedPeers)
        assertEquals(1890, p.listener.effectivePort())
        assertEquals("0.0.0.0", p.listener.listenAddress())
        assertEquals(2, p.listener.getMaxPreAuthPerIp())
        assertEquals(10, p.getKeepAliveSeconds())
        assertEquals(2_000_000, p.log.getMaxMessages())
        assertEquals(256L shl 20, p.log.getMaxBytes())
        assertEquals((1 shl 20) + (64 shl 10), p.log.getMaxRecordBytes(0))
        assertEquals(2000, p.log.getDrainOnShutdownMs())
        assertEquals(300, p.log.getNeverConnectedWarnSec())
        assertEquals("FILL", p.snapshot.effectiveMode())
        assertEquals(1_000_000, p.snapshot.getMaxTopics())
        assertEquals(4096, p.fetch.getMaxRecords())
        assertEquals(1 shl 20, p.fetch.getMaxBytes())
        assertEquals(1000, p.fetch.getMaxWaitMs())
        assertEquals(0, p.fetch.lingerMs)
        assertEquals(1, p.fetch.getPipeline())
        assertEquals(30000, p.fetch.getReconnectMaxMs())
        assertTrue(p.receive.getBus())
        assertFalse(p.receive.bridgeOutbound)
        assertTrue(p.receive.getArchive())
        assertFalse(p.receive.queue)
        assertEquals("SKIP", p.receive.effectiveSharedSubscriptions())
        assertEquals(3.0, p.receive.getCatchUpRateFactor(), 0.001)
        assertEquals((16 shl 20) + (64 shl 10), p.receive.getMaxFrameBytes())
        assertEquals(1, p.receive.getInjectWorkers())
    }

    @Test
    fun testValidPeerLinkPasses() {
        val p = validPeerLink()
        val setup = validatePeerLink(p, defaultEnv())
        assertEquals("node-a", setup.nodeID)
        assertEquals(1, setup.peers.size)
        assertEquals("node-b", setup.peers[0].nodeID)
        assertTrue(setup.anyServe())
    }

    @Test
    fun testUnknownKeysFailStartupEvenWhenDisabled() {
        val json = JsonObject()
            .put("Enabled", false)
            .put("UnknownKey", 123)

        try {
            PeerLinkConfigParser.parse(json)
            fail("Expected IllegalArgumentException for unknown key")
        } catch (e: IllegalArgumentException) {
            assertTrue(e.message!!.contains("Unknown configuration key: PeerLink.UnknownKey"))
        }

        // Sub-object unknown key
        val json2 = JsonObject()
            .put("Enabled", false)
            .put("Tls", JsonObject().put("InvalidKey", true))

        try {
            PeerLinkConfigParser.parse(json2)
            fail("Expected IllegalArgumentException for unknown key in Tls")
        } catch (e: IllegalArgumentException) {
            assertTrue(e.message!!.contains("Unknown configuration key: PeerLink.Tls.InvalidKey"))
        }
    }

    @Test
    fun testCanonicalNodeID() {
        assertEquals("main", canonicalNodeID("MAIN"))
        assertEquals("edge-1.sub_node", canonicalNodeID("Edge-1.Sub_Node"))

        try {
            canonicalNodeID("")
            fail("Empty NodeId should fail")
        } catch (_: IllegalArgumentException) {}

        try {
            canonicalNodeID("invalid@char")
            fail("Invalid characters in NodeId should fail")
        } catch (_: IllegalArgumentException) {}

        try {
            canonicalNodeID("a".repeat(65))
            fail("NodeId longer than 64 characters should fail")
        } catch (_: IllegalArgumentException) {}
    }

    @Test
    fun testOwnPeerIgnored() {
        val p = validPeerLink()
        p.peers = listOf(
            PeerConfig(nodeID = "node-a", address = "node-a.local:1890"), // Own node!
            PeerConfig(nodeID = "node-b", address = "node-b.local:1890")
        )

        val setup = validatePeerLink(p, defaultEnv("node-a"))
        assertEquals(1, setup.peers.size)
        assertEquals("node-b", setup.peers[0].nodeID)
        assertTrue(setup.infos.any { it.contains("Peers[0] \"node-a\" is this node and is ignored") })
    }

    @Test
    fun testFailClosedWithoutAuth() {
        val p = PeerLinkConfig()
        p.enabled = true
        p.tls.enabled = false
        p.peers = listOf(PeerConfig(nodeID = "node-b", address = "node-b.local:1890"))

        try {
            validatePeerLink(p, defaultEnv())
            fail("Unauthenticated peer link should fail validation")
        } catch (e: IllegalArgumentException) {
            assertTrue(e.message!!.contains("is not authenticated"))
        }

        // With waiver: needs AllowedNetworks
        p.allowUnauthenticatedPeers = true
        try {
            validatePeerLink(p, defaultEnv())
            fail("Waiver without AllowedNetworks should fail")
        } catch (e: IllegalArgumentException) {
            assertTrue(e.message!!.contains("AllowUnauthenticatedPeers needs a non-empty Listener.AllowedNetworks"))
        }

        p.listener.allowedNetworks = listOf("10.0.0.0/8")
        val setup = validatePeerLink(p, defaultEnv())
        assertTrue(setup.warnings.any { it.contains("AllowUnauthenticatedPeers is set") })
    }

    @Test
    fun testSharedSecretValidation() {
        assertNull(checkPeerSecret(testSecret))
        assertNotNull(checkPeerSecret("not-base64!"))
        // Base64 of 10 bytes (too short, min 16)
        val shortSec = java.util.Base64.getEncoder().encodeToString(ByteArray(10))
        assertNotNull(checkPeerSecret(shortSec))
    }

    @Test
    fun testPinValidation() {
        assertTrue(validPeerPin(testPin))
        // Colons and spaces allowed
        val formatted = testPin.chunked(2).joinToString(":")
        assertTrue(validPeerPin(formatted))

        assertFalse(validPeerPin("xyz"))
        assertFalse(validPeerPin(testPin + "00")) // 66 hex
    }

    @Test
    fun testCIDRValidation() {
        assertTrue(validCIDR("10.0.0.0/24"))
        assertTrue(validCIDR("192.168.1.1/32"))
        assertTrue(validCIDR("::1/128"))
        assertFalse(validCIDR("10.0.0.0/35"))
        assertFalse(validCIDR("not-a-cidr"))
    }
}
