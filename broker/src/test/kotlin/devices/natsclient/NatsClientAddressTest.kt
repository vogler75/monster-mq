package at.rocworks.devices.natsclient

import at.rocworks.stores.devices.NatsClientAddress
import io.vertx.core.json.JsonObject
import org.junit.Assert.*
import org.junit.Test

class NatsClientAddressTest {

    // ── isSubscribe / isPublish ───────────────────────────────────────────────

    @Test
    fun `isSubscribe returns true for SUBSCRIBE mode`() {
        val addr = NatsClientAddress(mode = "SUBSCRIBE", natsSubject = "s", mqttTopic = "t")
        assertTrue(addr.isSubscribe())
        assertFalse(addr.isPublish())
    }

    @Test
    fun `isPublish returns true for PUBLISH mode`() {
        val addr = NatsClientAddress(mode = "PUBLISH", natsSubject = "s", mqttTopic = "t")
        assertTrue(addr.isPublish())
        assertFalse(addr.isSubscribe())
    }

    // ── mqttToNatsSubject (autoConvert = true) ───────────────────────────────

    @Test
    fun `mqttToNatsSubject converts forward slashes to dots`() {
        val addr = NatsClientAddress("PUBLISH", "s", "t", autoConvert = true)
        assertEquals("sensors.temp.room1", addr.mqttToNatsSubject("sensors/temp/room1"))
    }

    @Test
    fun `mqttToNatsSubject converts multi-level wildcard`() {
        val addr = NatsClientAddress("PUBLISH", "s", "t", autoConvert = true)
        assertEquals("sensors.>", addr.mqttToNatsSubject("sensors/#"))
    }

    @Test
    fun `mqttToNatsSubject converts single-level wildcard`() {
        val addr = NatsClientAddress("PUBLISH", "s", "t", autoConvert = true)
        assertEquals("sensors.*.temp", addr.mqttToNatsSubject("sensors/+/temp"))
    }

    @Test
    fun `mqttToNatsSubject converts bare hash wildcard`() {
        val addr = NatsClientAddress("PUBLISH", "s", "t", autoConvert = true)
        assertEquals(">", addr.mqttToNatsSubject("#"))
    }

    @Test
    fun `mqttToNatsSubject converts bare plus wildcard`() {
        val addr = NatsClientAddress("PUBLISH", "s", "t", autoConvert = true)
        assertEquals("*", addr.mqttToNatsSubject("+"))
    }

    @Test
    fun `mqttToNatsSubject handles mixed wildcards`() {
        val addr = NatsClientAddress("PUBLISH", "s", "t", autoConvert = true)
        assertEquals("a.*.c.>", addr.mqttToNatsSubject("a/+/c/#"))
    }

    @Test
    fun `mqttToNatsSubject with autoConvert=false is passthrough`() {
        val addr = NatsClientAddress("PUBLISH", "s", "t", autoConvert = false)
        assertEquals("sensors/temp/room1", addr.mqttToNatsSubject("sensors/temp/room1"))
        assertEquals("sensors/#", addr.mqttToNatsSubject("sensors/#"))
    }

    // ── natsToMqttTopic (autoConvert = true) ─────────────────────────────────

    @Test
    fun `natsToMqttTopic converts dots to forward slashes`() {
        val addr = NatsClientAddress("SUBSCRIBE", "s", "t", autoConvert = true)
        assertEquals("sensors/temp/room1", addr.natsToMqttTopic("sensors.temp.room1"))
    }

    @Test
    fun `natsToMqttTopic converts NATS multi-level wildcard`() {
        val addr = NatsClientAddress("SUBSCRIBE", "s", "t", autoConvert = true)
        assertEquals("sensors/#", addr.natsToMqttTopic("sensors.>"))
    }

    @Test
    fun `natsToMqttTopic converts NATS single-level wildcard`() {
        val addr = NatsClientAddress("SUBSCRIBE", "s", "t", autoConvert = true)
        assertEquals("sensors/+/temp", addr.natsToMqttTopic("sensors.*.temp"))
    }

    @Test
    fun `natsToMqttTopic converts bare NATS wildcards`() {
        val addr = NatsClientAddress("SUBSCRIBE", "s", "t", autoConvert = true)
        assertEquals("#", addr.natsToMqttTopic(">"))
        assertEquals("+", addr.natsToMqttTopic("*"))
    }

    @Test
    fun `natsToMqttTopic with autoConvert=false is passthrough`() {
        val addr = NatsClientAddress("SUBSCRIBE", "s", "t", autoConvert = false)
        assertEquals("sensors.temp.room1", addr.natsToMqttTopic("sensors.temp.room1"))
        assertEquals("sensors.>", addr.natsToMqttTopic("sensors.>"))
    }

    // ── Translation round-trips ───────────────────────────────────────────────

    @Test
    fun `mqttToNats then natsToMqtt is identity`() {
        val addr = NatsClientAddress("PUBLISH", "s", "t", autoConvert = true)
        val cases = listOf("a/b/c", "a/#", "a/+/c", "+", "#", "a/b/c/d/e/f")
        cases.forEach { mqtt ->
            assertEquals("round-trip failed for: $mqtt",
                mqtt, addr.natsToMqttTopic(addr.mqttToNatsSubject(mqtt)))
        }
    }

    @Test
    fun `natsToMqtt then mqttToNats is identity`() {
        val addr = NatsClientAddress("SUBSCRIBE", "s", "t", autoConvert = true)
        val cases = listOf("a.b.c", "a.>", "a.*.c", "*", ">", "x.y.z")
        cases.forEach { nats ->
            assertEquals("round-trip failed for: $nats",
                nats, addr.mqttToNatsSubject(addr.natsToMqttTopic(nats)))
        }
    }

    // ── validate ──────────────────────────────────────────────────────────────

    @Test
    fun `validate passes for valid SUBSCRIBE address`() {
        val addr = NatsClientAddress("SUBSCRIBE", "sensors.>", "sensors/#", qos = 1)
        assertTrue(addr.validate().isEmpty())
    }

    @Test
    fun `validate passes for valid PUBLISH address`() {
        val addr = NatsClientAddress("PUBLISH", "commands.*", "commands/+", qos = 0)
        assertTrue(addr.validate().isEmpty())
    }

    @Test
    fun `validate rejects unknown mode`() {
        val addr = NatsClientAddress("FORWARD", "s", "t")
        val errors = addr.validate()
        assertEquals(1, errors.size)
        assertTrue(errors[0].contains("mode"))
    }

    @Test
    fun `validate rejects blank natsSubject`() {
        val addr = NatsClientAddress("SUBSCRIBE", "  ", "t")
        val errors = addr.validate()
        assertEquals(1, errors.size)
        assertTrue(errors[0].contains("natsSubject"))
    }

    @Test
    fun `validate rejects blank mqttTopic`() {
        val addr = NatsClientAddress("SUBSCRIBE", "s", "")
        val errors = addr.validate()
        assertEquals(1, errors.size)
        assertTrue(errors[0].contains("mqttTopic"))
    }

    @Test
    fun `validate rejects qos out of range`() {
        val addr = NatsClientAddress("SUBSCRIBE", "s", "t", qos = 3)
        val errors = addr.validate()
        assertEquals(1, errors.size)
        assertTrue(errors[0].contains("qos"))
    }

    @Test
    fun `validate accumulates multiple errors`() {
        val addr = NatsClientAddress("INVALID", "", "", qos = 5)
        val errors = addr.validate()
        assertEquals(4, errors.size)
    }

    // ── JSON round-trip ───────────────────────────────────────────────────────

    @Test
    fun `toJsonObject produces all expected keys`() {
        val addr = NatsClientAddress("SUBSCRIBE", "sensors.>", "sensors/#", qos = 2, autoConvert = false)
        val json = addr.toJsonObject()
        assertEquals("SUBSCRIBE", json.getString("mode"))
        assertEquals("sensors.>", json.getString("natsSubject"))
        assertEquals("sensors/#", json.getString("mqttTopic"))
        assertEquals(2, json.getInteger("qos"))
        assertFalse(json.getBoolean("autoConvert"))
    }

    @Test
    fun `fromJsonObject produces equal address`() {
        val original = NatsClientAddress("PUBLISH", "cmd.*", "cmd/+", qos = 1, autoConvert = true)
        val restored = NatsClientAddress.fromJsonObject(original.toJsonObject())
        assertEquals(original, restored)
    }

    @Test
    fun `fromJsonObject defaults qos to 0 and autoConvert to true when absent`() {
        val json = JsonObject()
            .put("mode", "SUBSCRIBE")
            .put("natsSubject", "s")
            .put("mqttTopic", "t")
        val addr = NatsClientAddress.fromJsonObject(json)
        assertEquals(0, addr.qos)
        assertTrue(addr.autoConvert)
    }
}
