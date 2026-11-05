package at.rocworks

import at.rocworks.data.BrokerMessage
import at.rocworks.data.BrokerMessageCodec
import io.vertx.core.buffer.Buffer
import org.junit.Assert.*
import org.junit.Test

/**
 * MQTT 5 User Properties must be forwarded unaltered and in order, duplicate keys included
 * ([MQTT-3.3.2-17], [MQTT-3.3.2-18]).
 */
class BrokerMessageUserPropertiesTest {
    private val props = listOf("tag" to "alpha", "tag" to "beta", "dup" to "first", "dup" to "second")

    private fun message() = BrokerMessage(
        messageId = 0,
        topicName = "a/b",
        payload = "x".toByteArray(),
        qosLevel = 1,
        isRetain = false,
        clientId = "c1",
        userProperties = props
    )

    @Test
    fun `json round trip keeps order and duplicate keys`() {
        val json = BrokerMessage.userPropertiesToJson(props)
        assertEquals(props, BrokerMessage.userPropertiesFromJson(json))
    }

    @Test
    fun `json reader accepts the object format of older versions`() {
        assertEquals(listOf("a" to "1", "b" to "2"), BrokerMessage.userPropertiesFromJson("""{"a":"1","b":"2"}"""))
        assertNull(BrokerMessage.userPropertiesFromJson(null))
        assertNull(BrokerMessage.userPropertiesFromJson(""))
        assertNull(BrokerMessage.userPropertiesFromJson("not json"))
    }

    @Test
    fun `event bus codec keeps order and duplicate keys`() {
        val codec = BrokerMessageCodec()
        val buffer = Buffer.buffer()
        codec.encodeToWire(buffer, message())
        val decoded = codec.decodeFromWire(0, buffer)
        assertEquals(props, decoded.userProperties)
    }

    @Test
    fun `encoded publish size counts every user property`() {
        val withProps = message().encodedPublishSize(true)
        val withoutProps = message().copy(userProperties = null).encodedPublishSize(true)
        // each property: 1 byte id + (2 + key) + (2 + value)
        val expected = props.sumOf { 1 + 2 + it.first.length + 2 + it.second.length }
        assertEquals(expected.toLong(), withProps - withoutProps)
    }
}
