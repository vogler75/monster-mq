package at.rocworks.peerlink

import io.vertx.core.json.JsonObject
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.MqttCallback
import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions
import org.junit.After
import org.junit.Assert.*
import org.junit.Assume
import org.junit.Test
import java.io.File
import java.net.HttpURLConnection
import java.net.ServerSocket
import java.net.Socket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.WebSocket
import java.nio.file.Files
import java.util.concurrent.CompletionStage
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit

// IR-M0, M1 and M3 (plan-peerlink-interest-routing section 11) against the real main broker: a child
// JVM running at.rocworks.MonsterKt pulls from a Go edge broker (EdgeBinary). What main announces is
// read from the edge's consumer status; what reaches main from the main clients themselves.
class InterestMainProcessTest {

    private var edge: Process? = null
    private var main: Process? = null
    private val closers = mutableListOf<() -> Unit>()
    private val dir: File = Files.createTempDirectory("peerlink-mainproc").toFile()

    private val edgeId = "edge-pt"
    private val mainId = "main-pt"
    private val edgeMqtt = freePort()
    private val edgePeer = freePort()
    private val mainMqtt = freePort()
    private val mainPeer = freePort()
    private val mainGraphQL = freePort()

    private fun freePort(): Int = ServerSocket(0).use { it.reuseAddress = true; it.localPort }

    @After
    fun tearDown() {
        closers.reversed().forEach { runCatching(it) }
        for (p in listOfNotNull(main, edge)) {
            p.destroy()
            if (!p.waitFor(15, TimeUnit.SECONDS)) p.destroyForcibly()
        }
        dir.deleteRecursively()
    }

    private fun waitFor(what: String, timeoutMs: Long = 20000, cond: () -> Boolean) {
        val end = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < end) {
            if (runCatching(cond).getOrDefault(false)) return
            Thread.sleep(50)
        }
        fail("timeout waiting for $what\n--- edge status ---\n" + runCatching { edgeConsumerInterest()?.encodePrettily() }.getOrNull() +
            "\n--- main source ---\n" + runCatching { mainSource().encodePrettily() }.getOrNull() +
            "\n--- main log ---\n" + File(dir, "main.log").takeIf { it.isFile }?.readText()?.takeLast(6000) +
            "\n--- edge log ---\n" + File(dir, "edge.log").takeIf { it.isFile }?.readText()?.takeLast(3000))
    }

    private fun startEdge() {
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
              Interest: { Enabled: true, FlushMs: 5 }
              Peers:
                - NodeId: $mainId
                  Address: "127.0.0.1:$mainPeer"
                  Serve: true
        """.trimIndent()
        val cfg = File(dir, "edge.yaml").apply { writeText(yaml) }
        edge = ProcessBuilder(bin!!.path, "-config", cfg.path)
            .directory(dir).redirectErrorStream(true).redirectOutput(File(dir, "edge.log")).start()
        waitFor("edge MQTT port") { Socket("127.0.0.1", edgeMqtt).use { true } }
    }

    // bus: PeerLink.Receive.Bus on main.
    private fun startMain(bus: Boolean) {
        val db = File(dir, "main-db").apply { mkdirs() }
        val yaml = """
            NodeId: $mainId
            TCP: $mainMqtt
            WS: 0
            DefaultStoreType: SQLITE
            QueuedMessagesEnabled: true
            SQLite:
              Path: ${db.path}
            GraphQL:
              Enabled: true
              Port: $mainGraphQL
            Dashboard: { Enabled: false }
            Metrics: { Enabled: false }
            Redfish: { Enabled: false }
            PeerLink:
              Enabled: true
              AllowUnauthenticatedPeers: true
              KeepAliveSeconds: 2
              Listener:
                Address: 127.0.0.1
                Port: $mainPeer
                AllowedNetworks: ["127.0.0.0/8"]
              Fetch: { MaxWaitMs: 200, ReconnectMaxMs: 300 }
              Receive: { Archive: false, Bus: $bus }
              Interest: { Enabled: true, FlushMs: 5 }
              Peers:
                - NodeId: $edgeId
                  Address: "127.0.0.1:$edgePeer"
                  Serve: true
        """.trimIndent()
        val cfg = File(dir, "main.yaml").apply { writeText(yaml) }
        val java = File(System.getProperty("java.home"), "bin/java").path
        main = ProcessBuilder(java, "-cp", System.getProperty("java.class.path"), "at.rocworks.MonsterKt", "-config", cfg.path)
            .directory(dir).redirectErrorStream(true).redirectOutput(File(dir, "main.log")).start()
        waitFor("main MQTT port", 90000) { Socket("127.0.0.1", mainMqtt).use { true } }
        waitFor("main streaming from edge") { mainSource().getString("state") == "STREAMING" }
        waitFor("edge streaming from main") {
            status(edgePeer).getJsonArray("sources").map { it as JsonObject }
                .any { it.getString("nodeId") == mainId && it.getString("state") == "STREAMING" }
        }
        waitFor("edge holds main's interest") { edgeConsumerInterest()?.getString("state") == "LIVE" }
    }

    private fun status(port: Int): JsonObject {
        val conn = URI("http://127.0.0.1:$port/peerlink/v1/status").toURL().openConnection() as HttpURLConnection
        conn.connectTimeout = 2000
        conn.readTimeout = 2000
        return conn.inputStream.bufferedReader().use { JsonObject(it.readText()) }
    }

    // Main's interest as the edge holds it: {state, mode, filters, filtersPersistent, ...}.
    private fun edgeConsumerInterest(): JsonObject? = status(edgePeer).getJsonArray("consumers")
        ?.map { it as JsonObject }?.firstOrNull { it.getString("nodeId") == mainId }?.getJsonObject("interest")

    private fun edgeFilters(): Pair<Int, Int>? =
        edgeConsumerInterest()?.let { Pair(it.getInteger("filters"), it.getInteger("filtersPersistent")) }

    private fun edgeSkipped(): Long = status(edgePeer).getJsonObject("interest").getLong("interestSkipped")

    private fun mainSource(): JsonObject = status(mainPeer).getJsonArray("sources").getJsonObject(0)

    private fun mainInjected(): Long = mainSource().getLong("injected")

    private fun mqtt3(port: Int, clientId: String, clean: Boolean, received: MutableList<String>? = null): MqttClient {
        val c = MqttClient("tcp://127.0.0.1:$port", clientId, MemoryPersistence())
        if (received != null) c.setCallback(object : MqttCallback {
            override fun connectionLost(cause: Throwable?) {}
            override fun messageArrived(topic: String, message: MqttMessage) { received.add(topic) }
            override fun deliveryComplete(token: IMqttDeliveryToken?) {}
        })
        c.connect(MqttConnectOptions().apply { isCleanSession = clean })
        closers.add { if (c.isConnected) c.disconnect(1000); c.close() }
        return c
    }

    private fun edgePublisher(): MqttClient = mqtt3(edgeMqtt, "edge-pub", clean = true)

    private fun MqttClient.send(topic: String) = publish(topic, topic.toByteArray(), 1, false)

    // Publishes a marker that main's own subscription wants, so everything published before it on the
    // edge has been decided once the marker reached main.
    private fun fence(pub: MqttClient, seen: List<String>, marker: String) {
        pub.send(marker)
        waitFor("$marker on main") { seen.contains(marker) }
    }

    @Test
    fun testQueuedForOfflinePersistentSession() {
        // IR-M0: a QoS 1 message forwarded for an offline persistent session is queued on main and
        // delivered when the session reconnects.
        startEdge()
        startMain(bus = true)
        val got = CopyOnWriteArrayList<String>()
        val sub = mqtt3(mainMqtt, "pers-1", clean = false, received = got)
        sub.subscribe("q/#", 1)
        waitFor("q/# announced as persistent") { edgeFilters() == Pair(1, 1) }
        sub.disconnect(1000)
        Thread.sleep(500)
        assertEquals("offline persistent session keeps its interest", Pair(1, 1), edgeFilters())

        val before = mainInjected()
        val skipped = edgeSkipped()
        val pub = edgePublisher()
        pub.send("other/1")
        pub.send("q/1")
        waitFor("q/1 injected on main") { mainInjected() > before }
        waitFor("other/1 skipped on edge") { edgeSkipped() > skipped }

        sub.connect(MqttConnectOptions().apply { isCleanSession = false })
        waitFor("queued q/1 delivered on reconnect") { got.contains("q/1") }
        Thread.sleep(300)
        assertEquals(listOf("q/1"), got.toList())
        assertEquals(before + 1, mainInjected())
    }

    @Test
    fun testSessionExpiryWithdrawsInterest() {
        // M3: an MQTT 5 session announced as PER with its expiry; when main expires the session the
        // filter is withdrawn and the edge stops serving it.
        startEdge()
        startMain(bus = true)
        val c = org.eclipse.paho.mqttv5.client.MqttClient("tcp://127.0.0.1:$mainMqtt", "exp-1",
            org.eclipse.paho.mqttv5.client.persist.MemoryPersistence())
        c.connect(MqttConnectionOptions().apply { isCleanStart = false; sessionExpiryInterval = 3L })
        closers.add { if (c.isConnected) c.disconnect(); c.close() }
        c.subscribe("x/#", 1)
        waitFor("x/# announced as persistent") { edgeFilters() == Pair(1, 1) }
        c.disconnect()
        Thread.sleep(1000)
        assertEquals("held during the expiry interval", Pair(1, 1), edgeFilters())

        waitFor("x/# withdrawn after session expiry", 15000) { edgeFilters() == Pair(0, 0) }

        val ctl = CopyOnWriteArrayList<String>()
        mqtt3(mainMqtt, "ctl", clean = true, received = ctl).subscribe("ctl/#", 1)
        waitFor("ctl/# announced") { edgeFilters()?.first == 1 }
        val before = mainInjected()
        val skipped = edgeSkipped()
        val pub = edgePublisher()
        pub.send("x/1")
        fence(pub, ctl, "ctl/1")
        assertTrue(edgeSkipped() > skipped)
        assertEquals("only the marker reached main", before + 1, mainInjected())
    }

    @Test
    fun testGraphQLSubscriptionAnnouncedWithReceiveBus() {
        // M1: a GraphQL subscription is announced when Receive.Bus is on.
        startEdge()
        startMain(bus = true)
        val frames = graphQLSubscribe("g/#")
        waitFor("g/# announced") { edgeFilters() == Pair(1, 0) }
        edgePublisher().send("g/1")
        waitFor("g/1 on the GraphQL subscription") { frames.any { it.contains("\"next\"") && it.contains("g/1") } }
    }

    @Test
    fun testGraphQLSubscriptionNotAnnouncedWithoutReceiveBus() {
        // M1: with Receive.Bus off the bus listener is not announced and g/1 is not forwarded.
        startEdge()
        startMain(bus = false)
        val frames = graphQLSubscribe("g/#")
        Thread.sleep(500)
        val ctl = CopyOnWriteArrayList<String>()
        mqtt3(mainMqtt, "ctl", clean = true, received = ctl).subscribe("ctl/#", 1)
        waitFor("ctl/# announced") { edgeFilters()?.first == 1 }
        Thread.sleep(300)
        assertEquals("only the MQTT subscription is announced", Pair(1, 0), edgeFilters())

        val before = mainInjected()
        val skipped = edgeSkipped()
        val pub = edgePublisher()
        pub.send("g/1")
        fence(pub, ctl, "ctl/1")
        assertTrue(edgeSkipped() > skipped)
        assertEquals(before + 1, mainInjected())
        assertFalse(frames.any { it.contains("g/1") })
    }

    // Opens a graphql-transport-ws subscription to topicUpdates(filter) and returns the received frames.
    private fun graphQLSubscribe(filter: String): List<String> {
        waitFor("main GraphQL port") { Socket("127.0.0.1", mainGraphQL).use { true } }
        val frames = CopyOnWriteArrayList<String>()
        val listener = object : WebSocket.Listener {
            private val buf = StringBuilder()
            override fun onText(ws: WebSocket, data: CharSequence, last: Boolean): CompletionStage<*>? {
                buf.append(data)
                if (last) { frames.add(buf.toString()); buf.setLength(0) }
                ws.request(1)
                return null
            }
        }
        val ws = HttpClient.newHttpClient().newWebSocketBuilder()
            .subprotocols("graphql-transport-ws")
            .buildAsync(URI("ws://127.0.0.1:$mainGraphQL/graphql"), listener)
            .get(10, TimeUnit.SECONDS)
        closers.add { ws.sendClose(WebSocket.NORMAL_CLOSURE, "").get(2, TimeUnit.SECONDS) }
        ws.sendText(JsonObject().put("type", "connection_init").put("payload", JsonObject()).encode(), true).get()
        waitFor("connection_ack") { frames.any { it.contains("connection_ack") } }
        val query = "subscription { topicUpdates(topicFilters: [\"$filter\"]) { topic } }"
        ws.sendText(JsonObject().put("id", "1").put("type", "subscribe")
            .put("payload", JsonObject().put("query", query)).encode(), true).get()
        return frames
    }
}
