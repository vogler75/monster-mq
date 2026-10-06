package at.rocworks.peerlink

import at.rocworks.data.BrokerMessage
import at.rocworks.handlers.MessageHandler
import at.rocworks.handlers.SessionHandler
import at.rocworks.peerlink.config.*
import org.junit.After
import org.junit.Assert.*
import org.junit.Test
import org.mockito.Mockito
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class PeerLinkIntegrationTest {

    private var managerA: PeerLinkManager? = null
    private var managerB: PeerLinkManager? = null

    private fun findFreePort(): Int {
        ServerSocket(0).use { ss ->
            ss.reuseAddress = true
            return ss.localPort
        }
    }

    @After
    fun tearDown() {
        managerB?.stop()
        managerA?.stop()
    }

    @Test
    fun testPlaintextReplicationBetweenTwoBrokers() {
        val portA = findFreePort()
        val portB = findFreePort()

        // --- Mock broker services for Node A ---
        val messageHandlerA = Mockito.mock(MessageHandler::class.java)
        val retainedStoreA = TestMessageStore()
        Mockito.`when`(messageHandlerA.getRetainedStore()).thenReturn(retainedStoreA)
        val sessionHandlerA = Mockito.mock(SessionHandler::class.java) { invocation ->
            if (invocation.method.name == "getMessageHandler") messageHandlerA
            else Mockito.RETURNS_DEFAULTS.answer(invocation)
        }
        val messageBusA = TestMessageBus()

        // --- Mock broker services for Node B ---
        val receivedLatch = CountDownLatch(1)
        val receivedMessages = mutableListOf<BrokerMessage>()
        val messageHandlerB = Mockito.mock(MessageHandler::class.java)
        val retainedStoreB = TestMessageStore()
        Mockito.`when`(messageHandlerB.getRetainedStore()).thenReturn(retainedStoreB)
        val sessionHandlerB = Mockito.mock(SessionHandler::class.java) { invocation ->
            when (invocation.method.name) {
                "getMessageHandler" -> messageHandlerB
                "publishMessage" -> {
                    val msg = invocation.getArgument<BrokerMessage>(0)
                    receivedMessages.add(msg)
                    receivedLatch.countDown()
                    null
                }
                else -> Mockito.RETURNS_DEFAULTS.answer(invocation)
            }
        }
        val messageBusB = TestMessageBus()

        // --- Config Node A ---
        val cfgA = PeerLinkConfig(
            enabled = true,
            allowUnauthenticatedPeers = true,
            listener = PeerLinkListenerConfig(
                address = "127.0.0.1",
                port = portA,
                allowedNetworks = listOf("127.0.0.1/32"),
                allowPlaintext = true
            ),
            peers = listOf(
                PeerConfig(
                    nodeID = "broker-b",
                    address = "",
                    serve = true
                )
            )
        )
        val envA = PeerLinkEnv(nodeID = "broker-a", nodeIDOrigin = NodeIdOrigin.CONFIG, hostname = "broker-a")
        val setupA = validatePeerLink(cfgA, envA)

        // --- Config Node B ---
        val cfgB = PeerLinkConfig(
            enabled = true,
            allowUnauthenticatedPeers = true,
            listener = PeerLinkListenerConfig(
                address = "127.0.0.1",
                port = portB,
                allowedNetworks = listOf("127.0.0.1/32"),
                allowPlaintext = true
            ),
            peers = listOf(
                PeerConfig(
                    nodeID = "broker-a",
                    address = "127.0.0.1:$portA",
                    serve = false
                )
            )
        )
        val envB = PeerLinkEnv(nodeID = "broker-b", nodeIDOrigin = NodeIdOrigin.CONFIG, hostname = "broker-b")
        val setupB = validatePeerLink(cfgB, envB)

        managerA = PeerLinkManager(cfgA, setupA, sessionHandlerA, messageHandlerA, messageBusA)
        managerB = PeerLinkManager(cfgB, setupB, sessionHandlerB, messageHandlerB, messageBusB)

        // Start Node A
        managerA!!.start()

        // Publish a message on Node A before or during pull
        val originalMsg = BrokerMessage(
            topicName = "sensors/temperature",
            payload = "23.5".toByteArray(),
            qosLevel = 1,
            isRetain = false,
            clientId = "sensor-device-1"
        )
        managerA!!.capture(originalMsg)

        // Start Node B to pull from Node A
        managerB!!.start()

        // Wait for replica to be received and applied on Node B
        val ok = receivedLatch.await(5, TimeUnit.SECONDS)
        assertTrue("Node B did not receive replicated message within 5s", ok)

        assertEquals(1, receivedMessages.size)
        val replicated = receivedMessages[0]
        assertEquals("sensors/temperature", replicated.topicName)
        assertEquals("23.5", String(replicated.payload))
        assertEquals("broker-a", replicated.peerSource)
        assertNotNull(replicated.peer)
        assertEquals("broker-a", replicated.peer?.sourceNode)
        assertEquals("sensor-device-1", replicated.clientId)

        // Verify Loop Prevention: If Node B's capture hook receives this replica, it drops it
        managerB!!.capture(replicated)
        assertEquals(1, managerB!!.hook.skipPeer.sum())

        // Verify Status
        val statusA = managerA!!.status()
        assertEquals("broker-a", statusA.nodeId)
        assertTrue(statusA.consumers.isNotEmpty())
        assertEquals("broker-b", statusA.consumers[0].nodeId)

        val statusB = managerB!!.status()
        assertEquals("broker-b", statusB.nodeId)
        assertTrue(statusB.sources.isNotEmpty())
        assertEquals("broker-a", statusB.sources[0].nodeId)
    }

    @Test
    fun testLoopbackHttpStatusEndpoint() {
        val port = findFreePort()
        val messageHandler = Mockito.mock(MessageHandler::class.java)
        val retainedStore = TestMessageStore()
        Mockito.`when`(messageHandler.getRetainedStore()).thenReturn(retainedStore)
        val sessionHandler = Mockito.mock(SessionHandler::class.java) { invocation ->
            if (invocation.method.name == "getMessageHandler") messageHandler
            else Mockito.RETURNS_DEFAULTS.answer(invocation)
        }
        val messageBus = TestMessageBus()

        val cfg = PeerLinkConfig(
            enabled = true,
            allowUnauthenticatedPeers = true,
            listener = PeerLinkListenerConfig(
                address = "127.0.0.1",
                port = port,
                allowedNetworks = listOf("127.0.0.1/32"),
                allowPlaintext = true
            ),
            peers = listOf(
                PeerConfig(
                    nodeID = "other-node",
                    serve = true
                )
            )
        )
        val env = PeerLinkEnv(nodeID = "node-http", nodeIDOrigin = NodeIdOrigin.CONFIG, hostname = "node-http")
        val setup = validatePeerLink(cfg, env)
        managerA = PeerLinkManager(cfg, setup, sessionHandler, messageHandler, messageBus)
        managerA!!.start()

        // Connect via HTTP
        val sock = Socket("127.0.0.1", port)
        try {
            val out = sock.getOutputStream()
            val request = "GET /peerlink/v1/status HTTP/1.1\r\nHost: 127.0.0.1:$port\r\n\r\n"
            out.write(request.toByteArray(Charsets.ISO_8859_1))
            out.flush()

            val reader = BufferedReader(InputStreamReader(sock.getInputStream()))
            val statusLine = reader.readLine()
            assertNotNull(statusLine)
            assertTrue("Expected 200 OK, got: $statusLine", statusLine.contains("200 OK"))

            // Read headers until blank line
            var contentLength = 0
            while (true) {
                val line = reader.readLine() ?: break
                if (line.isEmpty()) break
                if (line.lowercase().startsWith("content-length:")) {
                    contentLength = line.substring(15).trim().toInt()
                }
            }
            assertTrue(contentLength > 0)
            val bodyChars = CharArray(contentLength)
            var read = 0
            while (read < contentLength) {
                val r = reader.read(bodyChars, read, contentLength - read)
                if (r < 0) break
                read += r
            }
            val body = String(bodyChars, 0, read)
            assertTrue("Response body did not contain expected nodeId: $body", body.contains("\"nodeId\":\"node-http\""))
        } finally {
            sock.close()
        }
    }
}
