package at.rocworks.peerlink

import at.rocworks.Monster
import at.rocworks.handlers.ArchiveGroup
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.handlers.MessageHandler
import at.rocworks.handlers.SessionHandler
import at.rocworks.peerlink.config.*
import at.rocworks.stores.IQueueStoreAsync
import at.rocworks.stores.ISessionStoreAsync
import org.junit.After
import org.junit.Assert.*
import org.junit.Test
import org.mockito.Mockito
import java.lang.reflect.Proxy
import java.net.ServerSocket

// M2 / M2b (plan-peerlink-interest-routing section 11): which owners without session details the main
// broker announces. Uses the real SessionHandler classifier wired by PeerLinkManager.
class InterestClassifierTest {

    private var manager: PeerLinkManager? = null

    @After
    fun tearDown() {
        manager?.tracker?.stop()
    }

    private fun <T> unused(type: Class<T>): T = type.cast(Proxy.newProxyInstance(type.classLoader, arrayOf(type)) { _, method, _ ->
        throw UnsupportedOperationException("Unexpected dependency call: ${method.name}")
    })

    private fun manager(bus: Boolean, bridgeOutbound: Boolean, archive: Boolean = false): PeerLinkManager {
        val messageHandler = Mockito.mock(MessageHandler::class.java)
        Mockito.`when`(messageHandler.getRetainedStore()).thenReturn(TestMessageStore())
        val sessions = SessionHandler(unused(ISessionStoreAsync::class.java), unused(IQueueStoreAsync::class.java),
            TestMessageBus(), messageHandler, false)
        val cfg = PeerLinkConfig(
            enabled = true,
            allowUnauthenticatedPeers = true,
            listener = PeerLinkListenerConfig(address = "127.0.0.1", port = ServerSocket(0).use { it.localPort },
                allowedNetworks = listOf("127.0.0.1/32"), allowPlaintext = true),
            receive = PeerLinkReceiveConfig(bus = bus, bridgeOutbound = bridgeOutbound, archive = archive),
            peers = listOf(PeerConfig(nodeID = "broker-a", address = "127.0.0.1:1", serve = false)),
            interest = PeerLinkInterestConfig(enabled = true, unknown = "ALL", flushMs = 5)
        )
        val env = PeerLinkEnv(nodeID = "broker-b", nodeIDOrigin = NodeIdOrigin.CONFIG, hostname = "broker-b")
        val m = PeerLinkManager(cfg, validatePeerLink(cfg, env), sessions, messageHandler, TestMessageBus())
        manager = m
        return m
    }

    // Feeds the owners' subscriptions through the tracker and returns the announced filters.
    private fun announce(m: PeerLinkManager): Map<String, InterestClass> {
        val t = m.tracker!!
        t.start()
        t.added("mqttclient-bridge1", "out/#")
        t.added("graphql-sub-1", "g/#")
        t.added("internal-logger", "i/#")
        t.added("marker", "done")
        val end = System.currentTimeMillis() + 5000
        while (System.currentTimeMillis() < end && !t.announced().containsKey("done")) Thread.sleep(10)
        assertTrue("tracker did not process the subscriptions", t.announced().containsKey("done"))
        return t.announced()
    }

    @Test
    fun testBridgeOutboundAndBusOff() {
        val m = manager(bus = false, bridgeOutbound = false)
        val sh = m.sessionHandler
        assertNull(sh.peerLinkInterestClass("mqttclient-bridge1"))
        assertNull(sh.peerLinkInterestClass("graphql-sub-1"))
        assertEquals(InterestClass.VOL, sh.peerLinkInterestClass("internal-logger"))
        val a = announce(m)
        assertFalse(a.containsKey("out/#"))
        assertFalse(a.containsKey("g/#"))
        assertEquals(InterestClass.VOL, a["i/#"])
    }

    @Test
    fun testBridgeOutboundAndBusOn() {
        val m = manager(bus = true, bridgeOutbound = true)
        val a = announce(m)
        assertEquals(InterestClass.VOL, a["out/#"])
        assertEquals(InterestClass.VOL, a["g/#"])
        assertEquals(InterestClass.VOL, a["i/#"])
    }

    // M2b: standby redundancy components announce their configured filters although they do not
    // subscribe and BridgeOutbound is off. Main has no HOT/COLD_STANDBY roles yet (plan step 2); this
    // covers the provider path that those roles plug into.
    @Test
    fun testRedundancyProviderAnnouncedWithoutBridgeOutbound() {
        val m = manager(bus = false, bridgeOutbound = false)
        m.redundancyProvider = object : RedundancyComponentProvider {
            override fun filters() = mapOf("standby/#" to InterestClass.PER_NEVER)
        }
        m.refreshStaticInterest()
        val a = announce(m)
        assertEquals(InterestClass.PER_NEVER, a["standby/#"])
        assertFalse(a.containsKey("out/#"))

        // Removing the component withdraws the filter.
        m.redundancyProvider = NoRedundancyComponents
        m.refreshStaticInterest()
        m.tracker!!.added("marker", "done2")
        val end = System.currentTimeMillis() + 5000
        while (System.currentTimeMillis() < end && !m.tracker.announced().containsKey("done2")) Thread.sleep(10)
        assertFalse(m.tracker.announced().containsKey("standby/#"))
    }

    @Test
    fun testArchiveReadOnlyBothSkippedFromStaticInterest() {
        val m = manager(bus = false, bridgeOutbound = false, archive = true)
        val monsterMock = Mockito.mock(Monster::class.java)
        val archiveHandlerMock = Mockito.mock(ArchiveHandler::class.java)
        Mockito.`when`(monsterMock.archiveHandler).thenReturn(archiveHandlerMock)

        val singletonField = Monster::class.java.getDeclaredField("singleton")
        singletonField.isAccessible = true
        val prevSingleton = singletonField.get(null)
        singletonField.set(null, monsterMock)

        try {
            val roGroup = Mockito.mock(ArchiveGroup::class.java)
            Mockito.`when`(roGroup.lastValReadOnly).thenReturn(true)
            Mockito.`when`(roGroup.archiveReadOnly).thenReturn(true)
            Mockito.`when`(roGroup.topicFilter).thenReturn(listOf("ro/#"))

            val rwGroup = Mockito.mock(ArchiveGroup::class.java)
            Mockito.`when`(rwGroup.lastValReadOnly).thenReturn(false)
            Mockito.`when`(rwGroup.archiveReadOnly).thenReturn(true)
            Mockito.`when`(rwGroup.topicFilter).thenReturn(listOf("rw/#"))

            Mockito.`when`(archiveHandlerMock.getDeployedArchiveGroups()).thenReturn(mapOf("RO" to roGroup, "RW" to rwGroup))

            m.refreshStaticInterest()
            val a = announce(m)
            assertFalse(a.containsKey("ro/#"))
            assertEquals(InterestClass.PER_NEVER, a["rw/#"])

            // Flipping roGroup lastValReadOnly to false brings the filter back after refresh
            Mockito.`when`(roGroup.lastValReadOnly).thenReturn(false)
            m.refreshStaticInterest()
            m.tracker!!.added("marker", "done3")
            val end = System.currentTimeMillis() + 5000
            while (System.currentTimeMillis() < end && !m.tracker.announced().containsKey("done3")) Thread.sleep(10)
            assertEquals(InterestClass.PER_NEVER, m.tracker.announced()["ro/#"])
        } finally {
            singletonField.set(null, prevSingleton)
        }
    }
}
