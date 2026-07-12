package at.rocworks

import at.rocworks.bus.MessageBusZenoh
import at.rocworks.data.BrokerMessage
import at.rocworks.data.BrokerMessageCodec
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.time.Instant

class MessageBusZenohTest {

    @Test
    fun testIsTopicAllowed() {
        val config = JsonObject().put("Zenoh", JsonObject()
            .put("Prefix", "monstermq/mqtt")
            .put("Allow", JsonArray().add("sensors/+/temperature").add("actuators/#"))
            .put("Deny", JsonArray().add("sensors/hidden/temperature").add("actuators/debug/#"))
        )

        val bus = MessageBusZenoh(config)

        // Allowed
        assertTrue(bus.isTopicAllowed("sensors/1/temperature"))
        assertTrue(bus.isTopicAllowed("sensors/room2/temperature"))
        assertTrue(bus.isTopicAllowed("actuators/light"))
        assertTrue(bus.isTopicAllowed("actuators/switch/on"))

        // Denied (explicit)
        assertFalse(bus.isTopicAllowed("sensors/hidden/temperature"))
        assertFalse(bus.isTopicAllowed("actuators/debug/light"))

        // Denied (does not match allow)
        assertFalse(bus.isTopicAllowed("sensors/1/humidity"))
        assertFalse(bus.isTopicAllowed("status/online"))
    }

    @Test
    fun testBrokerMessageCodecWithOriginNodeId() {
        val codec = BrokerMessageCodec()

        val msgWithOrigin = BrokerMessage(
            messageUuid = "test-uuid-123",
            messageId = 1,
            topicName = "test/topic",
            payload = "hello".toByteArray(),
            qosLevel = 1,
            isRetain = true,
            isDup = false,
            isQueued = false,
            clientId = "test-client",
            time = Instant.now(),
            originNodeId = "broker-a"
        )

        val bufferWithOrigin = Buffer.buffer()
        codec.encodeToWire(bufferWithOrigin, msgWithOrigin)

        val decodedWithOrigin = codec.decodeFromWire(0, bufferWithOrigin)
        assertEquals(msgWithOrigin.messageUuid, decodedWithOrigin.messageUuid)
        assertEquals(msgWithOrigin.messageId, decodedWithOrigin.messageId)
        assertEquals(msgWithOrigin.topicName, decodedWithOrigin.topicName)
        assertTrue(msgWithOrigin.payload.contentEquals(decodedWithOrigin.payload))
        assertEquals(msgWithOrigin.qosLevel, decodedWithOrigin.qosLevel)
        assertEquals(msgWithOrigin.isRetain, decodedWithOrigin.isRetain)
        assertEquals(msgWithOrigin.isDup, decodedWithOrigin.isDup)
        assertEquals(msgWithOrigin.isQueued, decodedWithOrigin.isQueued)
        assertEquals(msgWithOrigin.clientId, decodedWithOrigin.clientId)
        assertEquals(msgWithOrigin.originNodeId, decodedWithOrigin.originNodeId)
        assertEquals("broker-a", decodedWithOrigin.originNodeId)

        // Without originNodeId
        val msgWithoutOrigin = BrokerMessage(
            messageUuid = "test-uuid-456",
            messageId = 2,
            topicName = "test/another",
            payload = "world".toByteArray(),
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = "test-client-2",
            time = Instant.now(),
            originNodeId = null
        )

        val bufferWithoutOrigin = Buffer.buffer()
        codec.encodeToWire(bufferWithoutOrigin, msgWithoutOrigin)

        val decodedWithoutOrigin = codec.decodeFromWire(0, bufferWithoutOrigin)
        assertEquals(msgWithoutOrigin.messageUuid, decodedWithoutOrigin.messageUuid)
        assertEquals(msgWithoutOrigin.originNodeId, decodedWithoutOrigin.originNodeId)
        assertEquals(null, decodedWithoutOrigin.originNodeId)
    }
}
