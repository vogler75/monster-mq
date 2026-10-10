package at.rocworks.graphql

import at.rocworks.peerlink.config.PeerConfig
import graphql.schema.idl.SchemaParser
import org.junit.Assert.*
import org.junit.Test

class PeerLinkQueriesTest {

    private val doc = mapOf(
        "enabled" to true,
        "nodeId" to "main",
        "listen" to "0.0.0.0:1890",
        "tls" to true,
        "consumers" to listOf(
            mapOf("nodeId" to "edge-a", "state" to "CONNECTED", "remote" to "10.0.0.2:51000", "lag" to 3),
            mapOf("nodeId" to "edge-b", "state" to "NEVER_CONNECTED", "remote" to "")
        ),
        "sources" to listOf(
            mapOf("nodeId" to "edge-a", "state" to "STREAMING", "lastError" to "", "lagRecords" to 0),
            mapOf("nodeId" to "edge-c", "state" to "BACKOFF", "lastError" to "connection refused")
        )
    )

    private val peers = listOf(
        PeerConfig(nodeID = "Edge-A", address = "edge-a:1890"),
        PeerConfig(nodeID = "edge-b"),
        PeerConfig(nodeID = "edge-c", address = "edge-c:1890", serve = false, interest = "off")
    )

    @Test
    fun testPeersCarryDirectionAndLinkState() {
        val info = PeerLinkQueries.build(peers, doc, listening = true)
        assertTrue(info.enabled)
        assertEquals("main", info.nodeId)
        assertEquals("0.0.0.0:1890", info.listen)
        assertTrue(info.tls)
        assertEquals(listOf("edge-a", "edge-b", "edge-c"), info.peers.map { it.nodeId })

        val a = info.peers[0]
        assertTrue(a.pull)
        assertTrue(a.serve)
        assertEquals("edge-a:1890", a.address)
        assertEquals("STREAMING", a.pullState)
        assertEquals("CONNECTED", a.serveState)
        assertEquals("10.0.0.2:51000", a.remote)
        assertNull(a.lastError)
        assertEquals("INHERIT", a.interest)
        assertEquals(3, a.consumer!!["lag"])
        assertEquals(0, a.source!!["lagRecords"])

        val b = info.peers[1]
        assertFalse(b.pull)
        assertTrue(b.serve)
        assertNull(b.address)
        assertNull(b.pullState)
        assertNull(b.source)
        assertEquals("NEVER_CONNECTED", b.serveState)
        assertNull(b.remote)

        val c = info.peers[2]
        assertTrue(c.pull)
        assertFalse(c.serve)
        assertEquals("BACKOFF", c.pullState)
        assertNull(c.serveState)
        assertNull(c.consumer)
        assertEquals("connection refused", c.lastError)
        assertEquals("OFF", c.interest)
    }

    @Test
    fun testMissingLinkStatusFallsBackToIdleStates() {
        val info = PeerLinkQueries.build(
            listOf(PeerConfig(nodeID = "edge-d", address = "edge-d:1890")),
            mapOf("enabled" to true, "nodeId" to "main", "listen" to "0.0.0.0:1890", "tls" to false),
            listening = false
        )
        assertNull(info.listen)
        assertEquals("STOPPED", info.peers[0].pullState)
        assertEquals("NEVER_CONNECTED", info.peers[0].serveState)
    }

    @Test
    fun testDisabled() {
        val info = PeerLinkQueries.disabled("node1")
        assertFalse(info.enabled)
        assertEquals("node1", info.nodeId)
        assertTrue(info.peers.isEmpty())
        assertNull(info.status)
    }

    @Test
    fun testSchemaDeclaresPeerLinkQuery() {
        val sdl = listOf("schema-types.graphqls", "schema-queries.graphqls", "schema-peerlink.graphqls").joinToString("\n") {
            javaClass.classLoader.getResourceAsStream(it)!!.bufferedReader().use { r -> r.readText() }
        }
        val registry = SchemaParser().parse(sdl)
        val query = registry.objectTypeExtensions()["Query"]!!.flatMap { it.fieldDefinitions }
        assertTrue(query.any { it.name == "peerLink" })
        val peer = registry.getType("PeerLinkPeer").get() as graphql.language.ObjectTypeDefinition
        assertEquals(
            listOf("nodeId", "address", "pull", "serve", "interest", "pullState", "serveState", "remote",
                "lastError", "source", "consumer"),
            peer.fieldDefinitions.map { it.name }
        )
    }
}
