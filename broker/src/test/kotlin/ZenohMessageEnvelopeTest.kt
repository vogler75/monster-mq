package at.rocworks

import at.rocworks.bus.ZenohMessageEnvelope
import at.rocworks.data.BrokerMessage
import org.junit.Assert.*
import org.junit.Test
import java.time.Instant

class ZenohMessageEnvelopeTest {
    @Test
    fun `round trip preserves mqtt metadata and leaves payload outside envelope`() {
        val message = BrokerMessage(
            messageUuid = "message-1",
            messageId = 42,
            topicName = "factory/line1/temperature",
            payload = byteArrayOf(0, 1, 2, -1),
            qosLevel = 2,
            isRetain = true,
            isDup = true,
            isQueued = false,
            clientId = "publisher-a",
            senderId = "publisher-a",
            time = Instant.ofEpochMilli(123456789),
            messageExpiryInterval = 60,
            payloadFormatIndicator = 0,
            contentType = "application/octet-stream",
            responseTopic = "factory/replies",
            correlationData = byteArrayOf(3, 4, 5),
            userProperties = mapOf("site" to "vienna")
        )

        val decoded = ZenohMessageEnvelope.decode(
            message.topicName,
            message.payload,
            ZenohMessageEnvelope.encode("broker-a", message)
        )

        assertEquals("broker-a", decoded.origin)
        assertEquals(message.messageUuid, decoded.message.messageUuid)
        assertEquals(message.messageId, decoded.message.messageId)
        assertEquals(message.topicName, decoded.message.topicName)
        assertArrayEquals(message.payload, decoded.message.payload)
        assertEquals(message.qosLevel, decoded.message.qosLevel)
        assertTrue(decoded.message.isRetain)
        assertTrue(decoded.message.isDup)
        assertEquals(message.senderId, decoded.message.senderId)
        assertEquals(message.time, decoded.message.time)
        assertEquals(message.messageExpiryInterval, decoded.message.messageExpiryInterval)
        assertEquals(message.contentType, decoded.message.contentType)
        assertArrayEquals(message.correlationData, decoded.message.correlationData)
        assertEquals(message.userProperties, decoded.message.userProperties)
    }

    @Test
    fun `native zenoh sample receives mqtt safe defaults`() {
        val decoded = ZenohMessageEnvelope.decode("native/topic", "hello".toByteArray(), null)

        assertNull(decoded.origin)
        assertEquals("native/topic", decoded.message.topicName)
        assertEquals("hello", decoded.message.getPayloadAsString())
        assertEquals(0, decoded.message.qosLevel)
        assertFalse(decoded.message.isRetain)
        assertEquals("zenoh", decoded.message.clientId)
    }

    @Test
    fun `unknown envelope version is treated as native zenoh data`() {
        val decoded = ZenohMessageEnvelope.decode(
            "native/topic",
            byteArrayOf(7),
            "{\"version\":999,\"origin\":\"other\"}".toByteArray()
        )

        assertNull(decoded.origin)
        assertArrayEquals(byteArrayOf(7), decoded.message.payload)
    }
}
