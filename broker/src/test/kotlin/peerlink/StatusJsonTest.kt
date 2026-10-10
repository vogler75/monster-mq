package at.rocworks.peerlink

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Test

// Epochs are uint64 like on the edge broker: an epoch with the top bit set is not written negative.
class StatusJsonTest {

    data class Epoch(@get:JsonSerialize(using = UnsignedLongSerializer::class) val epoch: Long)

    @Test
    fun testEpochWrittenUnsigned() {
        val m = ObjectMapper()
        assertEquals("""{"epoch":18446744073709551615}""", m.writeValueAsString(Epoch(-1L)))
        assertEquals("""{"epoch":42}""", m.writeValueAsString(Epoch(42L)))
        assertEquals("18446744073709551615", m.readTree(m.writeValueAsString(Epoch(-1L)))["epoch"].bigIntegerValue().toString())
    }

    // The peer's broker fields are written once a handshake announced them, and omitted before, like on
    // the edge broker.
    @Test
    fun testPeerBrokerOmittedUntilAnnounced() {
        val m = ObjectMapper()
        val cs = ConsumerStatus(nodeId = "b", state = "NEVER_CONNECTED", remote = "", committed = 0, served = 0, lag = 0,
            lostTotal = 0, servedRecords = 0, servedBytes = 0, servedSkipped = emptyMap(), snapshotServed = 0,
            sessions = 0, duplicateConsumer = 0, authFailures = emptyMap(), shutdownUnserved = 0, oaRetained = false,
            topicRootMismatch = false, retainedClassMismatch = false)
        val before = m.readTree(m.writeValueAsString(cs))
        assertFalse(before.has("peerBrokerType") || before.has("peerBrokerVersion") || before.has("peerProtocolVersion"))
        val after = m.readTree(m.writeValueAsString(cs.copy(peerBrokerType = "EDGE", peerBrokerVersion = "1.2",
            peerProtocolVersion = "1.0")))
        assertEquals("EDGE", after["peerBrokerType"].asText())
        assertEquals("1.2", after["peerBrokerVersion"].asText())
        assertEquals("1.0", after["peerProtocolVersion"].asText())
    }
}
