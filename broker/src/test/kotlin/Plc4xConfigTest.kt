package at.rocworks

import at.rocworks.devices.plc4x.Plc4xPayloadFormatter
import at.rocworks.stores.devices.Plc4xAddress
import at.rocworks.stores.devices.Plc4xPublishFormat
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class Plc4xConfigTest {
    @Test
    fun testPublishFormatDefaultsToJson() {
        val address = Plc4xAddress.fromJsonObject(
            JsonObject()
                .put("name", "holding-register:1")
                .put("address", "holding-register:1")
        )

        assertEquals(Plc4xPublishFormat.JSON, address.publishFormat)
        assertEquals("JSON", address.toJsonObject().getString("publishFormat"))
    }

    @Test
    fun testPublishFormatRoundTrip() {
        val address = Plc4xAddress.fromJsonObject(
            JsonObject()
                .put("name", "holding-register:1")
                .put("address", "holding-register:1")
                .put("publishFormat", "VALUE")
        )

        assertEquals(Plc4xPublishFormat.VALUE, address.publishFormat)
        assertEquals("VALUE", address.toJsonObject().getString("publishFormat"))
    }

    @Test
    fun testJsonPayloadFormatterKeepsEnvelope() {
        val payload = String(
            Plc4xPayloadFormatter.format(
                publishFormat = Plc4xPublishFormat.JSON,
                value = 42,
                deviceName = "plc1",
                addressName = "holding-register:1",
                timestamp = "2026-05-25T12:00:00Z"
            )
        )
        val json = JsonObject(payload)

        assertEquals(42, json.getInteger("value"))
        assertEquals("2026-05-25T12:00:00Z", json.getString("timestamp"))
        assertEquals("plc1", json.getString("device"))
        assertEquals("holding-register:1", json.getString("address"))
    }

    @Test
    fun testValuePayloadFormatterWritesOnlyScalar() {
        assertEquals(
            "42",
            String(
                Plc4xPayloadFormatter.format(
                    publishFormat = Plc4xPublishFormat.VALUE,
                    value = 42,
                    deviceName = "plc1",
                    addressName = "holding-register:1",
                    timestamp = "2026-05-25T12:00:00Z"
                )
            )
        )
        assertEquals(
            "true",
            String(
                Plc4xPayloadFormatter.format(
                    publishFormat = Plc4xPublishFormat.VALUE,
                    value = true,
                    deviceName = "plc1",
                    addressName = "coil:1",
                    timestamp = "2026-05-25T12:00:00Z"
                )
            )
        )
    }

    @Test
    fun testValuePayloadFormatterKeepsArrayJson() {
        val payload = String(
            Plc4xPayloadFormatter.format(
                publishFormat = Plc4xPublishFormat.VALUE,
                value = JsonArray(listOf(1, 2, 3)),
                deviceName = "plc1",
                addressName = "bytes",
                timestamp = "2026-05-25T12:00:00Z"
            )
        )

        assertEquals("[1,2,3]", payload)
        assertTrue(JsonArray(payload).contains(2))
    }
}
