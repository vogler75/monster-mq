package at.rocworks.peerlink

import at.rocworks.data.BrokerMessage
import at.rocworks.handlers.MessageHandler
import at.rocworks.handlers.SessionHandler
import at.rocworks.peerlink.config.*
import org.junit.After
import org.junit.Assert.*
import org.junit.Test
import org.mockito.Mockito
import java.net.ServerSocket
import java.util.concurrent.CopyOnWriteArrayList

// Interest routing between two main brokers: broker-b pulls from broker-a and announces its subscriptions.
class InterestIntegrationTest {

    private var managerA: PeerLinkManager? = null
    private var managerB: PeerLinkManager? = null
    private val received = CopyOnWriteArrayList<BrokerMessage>()

    private fun findFreePort(): Int = ServerSocket(0).use { it.reuseAddress = true; it.localPort }

    @After
    fun tearDown() {
        managerB?.stop()
        managerA?.stop()
    }

    private fun waitFor(what: String, timeoutMs: Long = 5000, cond: () -> Boolean) {
        val end = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < end) {
            if (cond()) return
            Thread.sleep(20)
        }
        fail("timeout waiting for $what")
    }

    private fun manager(
        nodeId: String,
        port: Int,
        peer: PeerConfig,
        onPublish: ((BrokerMessage) -> Unit)? = null
    ): PeerLinkManager {
        val messageHandler = Mockito.mock(MessageHandler::class.java)
        Mockito.`when`(messageHandler.getRetainedStore()).thenReturn(TestMessageStore())
        val sessionHandler = Mockito.mock(SessionHandler::class.java) { inv ->
            when (inv.method.name) {
                "getMessageHandler" -> messageHandler
                "peerLinkInterestClass" -> InterestClass.VOL
                "publishMessage" -> {
                    onPublish?.invoke(inv.getArgument(0))
                    null
                }
                else -> Mockito.RETURNS_DEFAULTS.answer(inv)
            }
        }
        val cfg = PeerLinkConfig(
            enabled = true,
            allowUnauthenticatedPeers = true,
            listener = PeerLinkListenerConfig(
                address = "127.0.0.1",
                port = port,
                allowedNetworks = listOf("127.0.0.1/32"),
                allowPlaintext = true
            ),
            peers = listOf(peer),
            interest = PeerLinkInterestConfig(enabled = true, unknown = "ALL", flushMs = 10)
        )
        val env = PeerLinkEnv(nodeID = nodeId, nodeIDOrigin = NodeIdOrigin.CONFIG, hostname = nodeId)
        return PeerLinkManager(cfg, validatePeerLink(cfg, env), sessionHandler, messageHandler, TestMessageBus())
    }

    private fun msg(topic: String) = BrokerMessage(
        topicName = topic,
        payload = topic.toByteArray(),
        qosLevel = 1,
        isRetain = false,
        clientId = "device-1"
    )

    @Test
    fun testOnlyInterestingRecordsAreServed() {
        val portA = findFreePort()
        val portB = findFreePort()
        val a = manager("broker-a", portA, PeerConfig(nodeID = "broker-b", address = "", serve = true))
        val b = manager("broker-b", portB, PeerConfig(nodeID = "broker-a", address = "127.0.0.1:$portA", serve = false)) {
            received.add(it)
        }
        managerA = a
        managerB = b
        val table = a.interest!!
        val tracker = b.tracker!!

        tracker.added("client-1", "sensors/#")
        tracker.added("peerlink:broker-c", "other/#")
        a.start()
        b.start()

        waitFor("interest LIVE on broker-a") {
            table.status(0)?.let { it.state == "LIVE" && it.filters == 1 } == true
        }
        assertEquals("FILTERED", table.status(0)!!.mode)

        a.capture(msg("other/x"))
        a.capture(msg("sensors/t1"))
        waitFor("sensors/t1 on broker-b") { received.any { it.topicName == "sensors/t1" } }
        assertEquals(listOf("sensors/t1"), received.map { it.topicName })

        // A new subscription on broker-b reaches broker-a as a delta.
        tracker.added("client-2", "other/#")
        waitFor("delta on broker-a") { table.status(0)!!.filters == 2 }
        a.capture(msg("other/y"))
        waitFor("other/y on broker-b") { received.any { it.topicName == "other/y" } }
        assertEquals(listOf("sensors/t1", "other/y"), received.map { it.topicName })

        val statusA = a.status()
        assertNotNull(statusA.interest)
        assertTrue(statusA.interest!!.interestSkipped >= 1)
        assertTrue(statusA.interest.deltasReceived >= 1)
        assertEquals("LIVE", statusA.consumers[0].interest!!.state)

        val src = b.status().sources[0]
        assertTrue(src.interest!!.active)
        assertTrue(src.interest.deltasSent >= 1)
        assertTrue(src.interest.snapshotsSent >= 1)
        assertEquals(2, b.status().interest!!.local!!.filters)

        // The status JSON has edge's layout: a nested interest object per source, no flat fields.
        val json = com.fasterxml.jackson.databind.ObjectMapper().readTree(
            com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(b.status()))
        val srcJson = json["sources"][0]
        assertNull(srcJson["interestAgreed"])
        assertNull(srcJson["deltasSent"])
        assertTrue(srcJson["interest"]["active"].asBoolean())
        assertNotNull(json["interest"]["local"]["generation"])
        assertNull(json["interest"]["deltasSent"])
        assertEquals(java.lang.Long.toUnsignedString(b.status().epoch), json["epoch"].asText())
    }

    @Test
    fun testPeerWithInterestOffIsServedEverything() {
        val portA = findFreePort()
        val portB = findFreePort()
        val a = manager("broker-a", portA, PeerConfig(nodeID = "broker-b", address = "", serve = true))
        val b = manager("broker-b", portB,
            PeerConfig(nodeID = "broker-a", address = "127.0.0.1:$portA", serve = false, interest = "OFF")) {
            received.add(it)
        }
        managerA = a
        managerB = b
        assertNull(b.tracker)

        a.start()
        b.start()
        waitFor("dense session on broker-a") { table0Connected(a) }

        a.capture(msg("other/x"))
        waitFor("other/x on broker-b") { received.any { it.topicName == "other/x" } }
        assertEquals("ALL", a.interest!!.status(0)!!.mode)
        assertNull(b.status().sources[0].interest)
    }

    private fun table0Connected(a: PeerLinkManager): Boolean =
        a.status().consumers.firstOrNull()?.interest?.mode == "ALL" && a.interest!!.status(0)!!.state == "LIVE"
}
