package at.rocworks.peerlink

import at.rocworks.data.BrokerMessage
import at.rocworks.peerlink.core.IncludeExclude
import at.rocworks.peerlink.core.LogConfig
import at.rocworks.peerlink.core.PeerLog
import at.rocworks.peerlink.wire.Record
import at.rocworks.peerlink.wire.decodeRecord
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.time.Instant

class CaptureHookTest {

    private lateinit var log: PeerLog
    private lateinit var hook: CaptureHook

    @Before
    fun setUp() {
        log = PeerLog(LogConfig(consumers = listOf("consumer-1")))
        hook = CaptureHook(
            log = log,
            filter = IncludeExclude.create(listOf("sensors/#"), listOf("sensors/internal/#")),
            captureWills = true,
            echoSuppressMs = 1000,
            maxExpirySec = 3600
        )
        hook.active.set(true)
    }

    @After
    fun tearDown() {
        log.close()
    }

    @Test
    fun testSessionTimes() {
        val st = SessionTimes()
        val now = System.nanoTime()

        assertFalse(st.since("client1", now - 1000))
        st.record("client1", now)
        assertTrue(st.since("client1", now - 1000))
        assertFalse(st.since("client1", now + 1000))
    }

    @Test
    fun testEchoTable() {
        val echo = EchoTable(1000)
        val now = System.nanoTime()
        val topic = "sensors/temp"
        val payload = "22.5".toByteArray()

        assertFalse(echo.match(topic, payload, false, now))
        echo.record(topic, payload, false, now)
        assertTrue(echo.match(topic, payload, false, now + 500_000_000L)) // 500ms later
        assertFalse(echo.match(topic, "23.0".toByteArray(), false, now + 500_000_000L)) // different payload
        assertFalse(echo.match("sensors/humidity", payload, false, now + 500_000_000L)) // different topic
        assertFalse(echo.match(topic, payload, true, now + 500_000_000L)) // different retain flag
        assertFalse(echo.match(topic, payload, false, now + 2_000_000_000L)) // 2s later (> 1s window)
    }

    @Test
    fun testCaptureFilters() {
        // Included message
        val msg1 = BrokerMessage(
            topicName = "sensors/temp",
            payload = "21.0".toByteArray(),
            qosLevel = 1,
            isRetain = false,
            clientId = "sensor-1"
        )
        hook.capture(msg1)
        assertEquals(1, log.getLEO() - log.bounds().first)

        // Excluded message
        val msg2 = BrokerMessage(
            topicName = "sensors/internal/health",
            payload = "ok".toByteArray(),
            qosLevel = 0,
            isRetain = false,
            clientId = "sensor-1"
        )
        hook.capture(msg2)
        assertEquals(1, log.getLEO() - log.bounds().first) // Unchanged
        assertEquals(1, hook.filtered.sum())

        // Non-matching message
        val msg3 = BrokerMessage(
            topicName = "actuators/valve",
            payload = "open".toByteArray(),
            qosLevel = 0,
            isRetain = false,
            clientId = "sensor-1"
        )
        hook.capture(msg3)
        assertEquals(1, log.getLEO() - log.bounds().first) // Unchanged
        assertEquals(2, hook.filtered.sum())
    }

    @Test
    fun testEchoSuppression() {
        val topic = "sensors/temp"
        val payload = "25.0".toByteArray()

        // Record into echo table
        hook.echo?.record(topic, payload, false, System.nanoTime())

        val msg = BrokerMessage(
            topicName = topic,
            payload = payload,
            qosLevel = 0,
            isRetain = false,
            clientId = "client-x"
        )
        hook.capture(msg)
        assertEquals(0, log.getLEO() - log.bounds().first)
        assertEquals(1, hook.echoSuppressed.sum())
    }

    @Test
    fun testSkipPeerReplicas() {
        val msg = BrokerMessage(
            topicName = "sensors/temp",
            payload = "25.0".toByteArray(),
            qosLevel = 0,
            isRetain = false,
            clientId = "client-x",
            peerSource = "peer-alpha"
        )
        hook.capture(msg)
        assertEquals(0, log.getLEO() - log.bounds().first)
        assertEquals(1, hook.skipPeer.sum())
    }
}
