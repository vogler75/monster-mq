package at.rocworks.devices.mqttclient

import org.eclipse.paho.mqttv5.common.MqttException
import org.eclipse.paho.mqttv5.common.packet.MqttReturnCode
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.ByteArrayInputStream

class MqttClientSnifferTest {

    @Test
    fun testIsV3Connack() {
        // Standard MQTT 3.1.1 CONNACK: 0x20 0x02
        assertTrue(isV3Connack(byteArrayOf(0x20.toByte(), 0x02.toByte())))
        assertTrue(isV3Connack(byteArrayOf(0x20.toByte(), 0x02.toByte(), 0x00.toByte(), 0x00.toByte())))

        // MQTT 5 CONNACK (remaining length at least 3 because of property length varint)
        assertFalse(isV3Connack(byteArrayOf(0x20.toByte(), 0x03.toByte())))
        assertFalse(isV3Connack(byteArrayOf(0x20.toByte(), 0x05.toByte())))

        // Other packets
        assertFalse(isV3Connack(byteArrayOf(0x30.toByte(), 0x02.toByte())))
        assertFalse(isV3Connack(byteArrayOf(0x20.toByte())))
        assertFalse(isV3Connack(byteArrayOf()))
    }

    @Test
    fun testIsUnsupportedProtocolVersion() {
        val ex1 = MqttException(MqttReturnCode.RETURN_CODE_UNSUPPORTED_PROTOCOL_VERSION)
        assertTrue(isUnsupportedProtocolVersion(ex1))

        val ex2 = MqttException(MqttException.REASON_CODE_UNSUPPORTED_PROTOCOL_VERSION)
        assertTrue(isUnsupportedProtocolVersion(ex2))

        val ex3 = MqttException(1)
        assertTrue(isUnsupportedProtocolVersion(ex3))

        val exMsg = RuntimeException("Server error: Unsupported protocol version")
        assertTrue(isUnsupportedProtocolVersion(exMsg))

        val exWrapped = RuntimeException("Outer error", ex1)
        assertTrue(isUnsupportedProtocolVersion(exWrapped))

        val regularEx = RuntimeException("Connection timed out")
        assertFalse(isUnsupportedProtocolVersion(regularEx))
    }

    @Test
    fun testSniffingInputStreamReadSingleBytes() {
        val data = byteArrayOf(0x20.toByte(), 0x02.toByte(), 0x00.toByte(), 0x00.toByte())
        var capturedHead: ByteArray? = null

        val sniffingStream = SniffingInputStream(ByteArrayInputStream(data)) { head ->
            capturedHead = head
        }

        val b0 = sniffingStream.read()
        assertEquals(0x20, b0)

        val b1 = sniffingStream.read()
        assertEquals(0x02, b1)
        assertArrayEquals(byteArrayOf(0x20.toByte(), 0x02.toByte()), capturedHead)

        val b2 = sniffingStream.read()
        assertEquals(0x00, b2)

        val b3 = sniffingStream.read()
        assertEquals(0x00, b3)

        assertEquals(-1, sniffingStream.read())
    }

    @Test
    fun testSniffingInputStreamReadBuffer() {
        val data = byteArrayOf(0x20.toByte(), 0x02.toByte(), 0x00.toByte(), 0x00.toByte())
        var capturedHead: ByteArray? = null

        val sniffingStream = SniffingInputStream(ByteArrayInputStream(data)) { head ->
            capturedHead = head
        }

        val buf = ByteArray(4)
        val readCount = sniffingStream.read(buf, 0, 4)
        assertEquals(4, readCount)
        assertArrayEquals(data, buf)
        assertArrayEquals(byteArrayOf(0x20.toByte(), 0x02.toByte()), capturedHead)
    }
}
