package at.rocworks.peerlink

import at.rocworks.data.BrokerMessage
import at.rocworks.handlers.MessageHandler
import at.rocworks.handlers.SessionHandler
import at.rocworks.peerlink.config.*
import io.vertx.core.json.JsonObject
import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.junit.After
import org.junit.Assert.*
import org.junit.Assume
import org.junit.Test
import org.mockito.Mockito
import java.io.File
import java.net.HttpURLConnection
import java.net.ServerSocket
import java.net.Socket
import java.net.URI
import java.nio.file.Files
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit

// M4: a Kotlin main broker paired with a Go edge broker (plan-peerlink-interest-routing section 11).
// The edge binary comes from EdgeBinary; without one the tests are skipped.
class InterestMixedEdgeTest {

    private var edge: Process? = null
    private var main: PeerLinkManager? = null
    private var client: MqttClient? = null
    private val mainReceived = CopyOnWriteArrayList<String>()
    private val edgeReceived = CopyOnWriteArrayList<String>()
    private val dir: File = Files.createTempDirectory("peerlink-mixed").toFile()

    private val edgeId = "edge-it"
    private val mainId = "main-it"
    private val edgeMqtt = freePort()
    private val edgePeer = freePort()
    private val mainPeer = freePort()

    private fun freePort(): Int = ServerSocket(0).use { it.reuseAddress = true; it.localPort }

    @After
    fun tearDown() {
        runCatching { client?.disconnect(1000) }
        runCatching { client?.close() }
        main?.stop()
        edge?.let {
            it.destroy()
            if (!it.waitFor(10, TimeUnit.SECONDS)) it.destroyForcibly()
        }
        dir.deleteRecursively()
    }

    private fun waitFor(what: String, timeoutMs: Long = 15000, cond: () -> Boolean) {
        val end = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < end) {
            if (runCatching(cond).getOrDefault(false)) return
            Thread.sleep(50)
        }
        fail("timeout waiting for $what\n--- edge status ---\n" + runCatching { edgeStatus().encodePrettily() }.getOrNull() +
            "\n--- main status ---\n" + runCatching { main?.interest?.status(0) }.getOrNull() +
            "\n--- edge log ---\n" + File(dir, "edge.log").takeIf { it.isFile }?.readText()?.takeLast(4000))
    }

    // edgeInterest: PeerLink.Interest.Enabled on edge; edgePeerOff: Peers[main].Interest OFF on edge.
    private fun startEdge(edgeInterest: Boolean, edgePeerOff: Boolean) {
        val bin = EdgeBinary.get()
        Assume.assumeTrue("edge binary not available", bin != null)
        val yaml = """
            NodeId: $edgeId
            TCP: { Enabled: true, Port: $edgeMqtt, Address: "127.0.0.1" }
            WS: { Enabled: false, Port: ${freePort()}, Address: "127.0.0.1" }
            DefaultStoreType: SQLITE
            SQLite:
              Path: ${File(dir, "edge.db").path}
            GraphQL: { Enabled: false }
            Metrics: { Enabled: false }
            PeerLink:
              Enabled: true
              AllowUnauthenticatedPeers: true
              Listener:
                Address: 127.0.0.1
                Port: $edgePeer
                AllowedNetworks: ["127.0.0.0/8"]
              KeepAliveSeconds: 2
              Fetch: { MaxWaitMs: 200, ReconnectMaxMs: 300 }
              Receive: { Archive: false }
              Interest: { Enabled: $edgeInterest, FlushMs: 5 }
              Peers:
                - NodeId: $mainId
                  Address: "127.0.0.1:$mainPeer"
                  Serve: true
                  Interest: ${if (edgePeerOff) "OFF" else "INHERIT"}
        """.trimIndent()
        val cfg = File(dir, "config.yaml").apply { writeText(yaml) }
        edge = ProcessBuilder(bin!!.path, "-config", cfg.path)
            .directory(dir)
            .redirectErrorStream(true)
            .redirectOutput(File(dir, "edge.log"))
            .start()
        waitFor("edge MQTT port") { Socket("127.0.0.1", edgeMqtt).use { true } }
    }

    private fun startMain(mainPeerOff: Boolean): PeerLinkManager {
        val messageHandler = Mockito.mock(MessageHandler::class.java, Mockito.withSettings().stubOnly())
        Mockito.`when`(messageHandler.getRetainedStore()).thenReturn(TestMessageStore())
        val sessionHandler = Mockito.mock(SessionHandler::class.java, Mockito.withSettings().stubOnly().defaultAnswer { inv ->
            when (inv.method.name) {
                "getMessageHandler" -> messageHandler
                "peerLinkInterestClass" -> InterestClass.VOL
                "publishMessage" -> { mainReceived.add((inv.getArgument(0) as BrokerMessage).topicName); null }
                else -> Mockito.RETURNS_DEFAULTS.answer(inv)
            }
        })
        val cfg = PeerLinkConfig(
            enabled = true,
            allowUnauthenticatedPeers = true,
            keepAliveSeconds = 2,
            listener = PeerLinkListenerConfig(address = "127.0.0.1", port = mainPeer,
                allowedNetworks = listOf("127.0.0.0/8"), allowPlaintext = true),
            fetch = PeerLinkFetchConfig(maxWaitMs = 200, reconnectMaxMs = 300),
            peers = listOf(PeerConfig(nodeID = edgeId, address = "127.0.0.1:$edgePeer", serve = true,
                interest = if (mainPeerOff) "OFF" else "")),
            interest = PeerLinkInterestConfig(enabled = true, unknown = "ALL", flushMs = 5)
        )
        val env = PeerLinkEnv(nodeID = mainId, nodeIDOrigin = NodeIdOrigin.CONFIG, hostname = mainId)
        val m = PeerLinkManager(cfg, validatePeerLink(cfg, env), sessionHandler, messageHandler, TestMessageBus())
        main = m
        m.tracker?.added("client-1", "e2m/#")
        m.start()
        return m
    }

    private fun connectClient(vararg filters: String) {
        val c = MqttClient("tcp://127.0.0.1:$edgeMqtt", "it-client", MemoryPersistence())
        c.connect(MqttConnectOptions().apply { isCleanSession = true })
        for (f in filters) c.subscribe(f, 1) { topic, _ -> edgeReceived.add(topic) }
        client = c
    }

    private fun publishOnEdge(topic: String) = client!!.publish(topic, topic.toByteArray(), 1, false)

    private fun msg(topic: String) = BrokerMessage(topicName = topic, payload = topic.toByteArray(), qosLevel = 1,
        isRetain = false, clientId = "device-1")

    private fun edgeStatus(): JsonObject {
        val conn = URI("http://127.0.0.1:$edgePeer/peerlink/v1/status").toURL().openConnection() as HttpURLConnection
        conn.connectTimeout = 2000
        conn.readTimeout = 2000
        return conn.inputStream.bufferedReader().use { JsonObject(it.readText()) }
    }

    private fun edgeConsumerInterest(): JsonObject? = edgeStatus().getJsonArray("consumers")
        ?.map { it as JsonObject }?.firstOrNull { it.getString("nodeId") == mainId }?.getJsonObject("interest")

    private fun edgeSourceActive(): Boolean = edgeStatus().getJsonArray("sources")
        ?.map { it as JsonObject }?.firstOrNull { it.getString("nodeId") == mainId }
        ?.getJsonObject("interest")?.getBoolean("active") == true

    private fun bothStreaming(m: PeerLinkManager) {
        waitFor("main streaming from edge") { m.status().sources.firstOrNull()?.state == "STREAMING" }
        waitFor("edge streaming from main") {
            edgeStatus().getJsonArray("sources").map { it as JsonObject }
                .any { it.getString("nodeId") == mainId && it.getString("state") == "STREAMING" }
        }
    }

    @Test
    fun testBothDirectionsFiltered() {
        startEdge(edgeInterest = true, edgePeerOff = false)
        val m = startMain(mainPeerOff = false)
        bothStreaming(m)
        connectClient("m2e/#")

        // Edge holds main's filter, main holds edge's.
        waitFor("edge holds main's filter") {
            edgeConsumerInterest()?.let { it.getString("mode") == "FILTERED" && it.getInteger("filters") == 1 } == true
        }
        val table = m.interest!!
        waitFor("main holds edge's filter") {
            table.status(0)?.let { it.state == "LIVE" && it.mode == "FILTERED" } == true && (table.match("m2e/1") and 1L) == 1L
        }
        assertEquals(0L, table.match("other/2") and 1L)
        assertEquals(true, m.status().sources[0].interest?.active)
        assertTrue(edgeSourceActive())

        // edge -> main
        publishOnEdge("other/1")
        publishOnEdge("e2m/1")
        waitFor("e2m/1 on main") { mainReceived.contains("e2m/1") }
        // main -> edge
        m.capture(msg("other/2"))
        m.capture(msg("m2e/1"))
        waitFor("m2e/1 on edge") { edgeReceived.contains("m2e/1") }
        Thread.sleep(300)
        assertEquals(listOf("e2m/1"), mainReceived.toList())
        assertEquals(listOf("m2e/1"), edgeReceived.toList())
        assertTrue(m.status().interest!!.interestSkipped >= 1)
        assertTrue(edgeStatus().getJsonObject("interest").getLong("interestSkipped") >= 1)
    }

    @Test
    fun testEdgeWithoutInterestIsDenseBothWays() {
        startEdge(edgeInterest = false, edgePeerOff = false)
        val m = startMain(mainPeerOff = false)
        bothStreaming(m)
        connectClient("#")

        waitFor("main serves edge dense") { m.interest!!.status(0)?.let { it.mode == "ALL" && it.state == "LIVE" } == true }
        assertEquals(false, m.status().sources[0].interest?.active)
        assertFalse(edgeSourceActive())

        publishOnEdge("other/1")
        waitFor("other/1 on main") { mainReceived.contains("other/1") }
        m.capture(msg("other/2"))
        waitFor("other/2 on edge") { edgeReceived.contains("other/2") }
    }

    @Test
    fun testMainPeerInterestOff() {
        startEdge(edgeInterest = true, edgePeerOff = false)
        val m = startMain(mainPeerOff = true)
        assertNull(m.tracker)
        bothStreaming(m)
        connectClient("m2e/#")

        waitFor("main serves edge dense") { m.interest!!.status(0)?.let { it.mode == "ALL" && it.state == "OFF" } == true }
        waitFor("edge serves main dense") { edgeConsumerInterest()?.getString("mode") == "ALL" }
        assertFalse(edgeSourceActive())

        publishOnEdge("other/1")
        waitFor("other/1 on main") { mainReceived.contains("other/1") }
    }

    @Test
    fun testEdgePeerInterestOff() {
        startEdge(edgeInterest = true, edgePeerOff = true)
        val m = startMain(mainPeerOff = false)
        bothStreaming(m)
        connectClient("#")

        waitFor("main serves edge dense") { m.interest!!.status(0)?.let { it.mode == "ALL" && it.state == "LIVE" } == true }
        // Edge leaves the interest object out for a consumer with Interest OFF.
        waitFor("edge serves main dense") { edgeConsumerInterest().let { it == null || it.getString("mode") == "ALL" } }
        assertEquals(false, m.status().sources[0].interest?.active)

        publishOnEdge("other/1")
        waitFor("other/1 on main") { mainReceived.contains("other/1") }
        m.capture(msg("other/2"))
        waitFor("other/2 on edge") { edgeReceived.contains("other/2") }
    }
}
