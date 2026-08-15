package at.rocworks

import at.rocworks.devices.i3xclient.I3xPublisher
import at.rocworks.stores.devices.I3xAddress
import at.rocworks.stores.devices.I3xConnectionConfig
import at.rocworks.stores.devices.I3xHeader
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.time.Instant

class I3xConfigTest {

    @Test
    fun testI3xHeaderSerialization() {
        val header = I3xHeader("X-Api-Key", "secret-token")
        val json = header.toJsonObject()
        assertEquals("X-Api-Key", json.getString("key"))
        assertEquals("secret-token", json.getString("value"))

        val restored = I3xHeader.fromJsonObject(json)
        assertEquals(header.key, restored.key)
        assertEquals(header.value, restored.value)
        assertTrue(restored.validate().isEmpty())
    }

    @Test
    fun testI3xAddressSerialization() {
        val address = I3xAddress(
            elementId = "factory/line1/motor1",
            topic = "incoming/motor1",
            maxDepth = 0,
            retained = true,
            qos = 1,
            messageFormat = I3xAddress.FORMAT_JSON_ISO,
            removePath = true,
            description = "Main motor telemetry"
        )
        val json = address.toJsonObject()
        assertEquals("factory/line1/motor1", json.getString("elementId"))
        assertEquals("incoming/motor1", json.getString("topic"))
        assertEquals(0, json.getInteger("maxDepth"))
        assertTrue(json.getBoolean("retained"))
        assertEquals(1, json.getInteger("qos"))
        assertEquals(I3xAddress.FORMAT_JSON_ISO, json.getString("messageFormat"))
        assertTrue(json.getBoolean("removePath"))
        assertEquals("Main motor telemetry", json.getString("description"))

        val restored = I3xAddress.fromJsonObject(json)
        assertEquals(address.elementId, restored.elementId)
        assertEquals(address.topic, restored.topic)
        assertEquals(address.maxDepth, restored.maxDepth)
        assertEquals(address.retained, restored.retained)
        assertEquals(address.qos, restored.qos)
        assertEquals(address.messageFormat, restored.messageFormat)
        assertEquals(address.removePath, restored.removePath)
        assertEquals(address.description, restored.description)
        assertTrue(restored.validate().isEmpty())
    }

    @Test
    fun testI3xConnectionConfigSerialization() {
        val config = I3xConnectionConfig(
            url = "http://remote-broker:3002/i3x/v1",
            authType = I3xConnectionConfig.AUTH_TYPE_BASIC,
            username = "admin",
            password = "password123",
            token = null,
            headers = listOf(I3xHeader("X-Custom", "val")),
            clientId = "my-i3x-client",
            reconnectDelay = 6000L,
            connectionTimeout = 12000L,
            addresses = listOf(
                I3xAddress(elementId = "sensors/temp", topic = "local/temp")
            )
        )

        val json = config.toJsonObject()
        assertEquals("http://remote-broker:3002/i3x/v1", json.getString("url"))
        assertEquals("BASIC", json.getString("authType"))
        assertEquals("admin", json.getString("username"))
        assertEquals("password123", json.getString("password"))
        assertEquals("my-i3x-client", json.getString("clientId"))
        assertEquals(6000L, json.getLong("reconnectDelay"))
        assertEquals(12000L, json.getLong("connectionTimeout"))
        assertEquals(1, json.getJsonArray("headers").size())
        assertEquals(1, json.getJsonArray("addresses").size())

        val restored = I3xConnectionConfig.fromJsonObject(json)
        assertEquals(config.url, restored.url)
        assertEquals(config.authType, restored.authType)
        assertEquals(config.username, restored.username)
        assertEquals(config.password, restored.password)
        assertEquals(config.clientId, restored.clientId)
        assertEquals(1, restored.headers.size)
        assertEquals(1, restored.addresses.size)
        assertTrue(restored.validate().isEmpty())
        assertEquals("http://remote-broker:3002/i3x/v1", restored.normalizedBaseUrl())
    }

    @Test
    fun testTopicResolution() {
        val addrExact = I3xAddress(elementId = "factory/motor", topic = "local/motor", maxDepth = 1)
        assertEquals("local/motor", I3xPublisher.resolveTopic("", addrExact, "factory/motor"))
        assertEquals("plant1/local/motor", I3xPublisher.resolveTopic("plant1", addrExact, "factory/motor"))

        val addrSubtree = I3xAddress(elementId = "factory/line1", topic = "data/line1", maxDepth = 0, removePath = false)
        assertEquals("data/line1/speed", I3xPublisher.resolveTopic("", addrSubtree, "factory/line1/speed"))
        assertEquals("data/line1/sub/sensor", I3xPublisher.resolveTopic("", addrSubtree, "factory/line1/sub/sensor"))

        val addrSubtreeRemovePath = I3xAddress(elementId = "factory/line1", topic = "data/custom", maxDepth = 0, removePath = true)
        assertEquals("data/custom/speed", I3xPublisher.resolveTopic("", addrSubtreeRemovePath, "factory/line1/speed"))
        assertEquals("data/custom/sub/sensor", I3xPublisher.resolveTopic("", addrSubtreeRemovePath, "factory/line1/sub/sensor"))
    }

    @Test
    fun testPayloadFormatting() {
        val fixedTime = "2026-08-15T10:00:00.000Z"

        // RAW_VALUE format
        val rawNumBytes = I3xPublisher.formatPayload(42.5, "Good", fixedTime, I3xAddress.FORMAT_RAW_VALUE)
        assertEquals("42.5", String(rawNumBytes))

        val rawJsonObj = JsonObject().put("rpm", 1500).put("active", true)
        val rawObjBytes = I3xPublisher.formatPayload(rawJsonObj, "Good", fixedTime, I3xAddress.FORMAT_RAW_VALUE)
        val parsedRawJson = JsonObject(String(rawObjBytes))
        assertEquals(1500, parsedRawJson.getInteger("rpm"))
        assertTrue(parsedRawJson.getBoolean("active"))

        // JSON_ISO format
        val isoBytes = I3xPublisher.formatPayload(100, "Good", fixedTime, I3xAddress.FORMAT_JSON_ISO)
        val isoJson = JsonObject(String(isoBytes))
        assertEquals(100, isoJson.getInteger("value"))
        assertEquals("Good", isoJson.getString("quality"))
        assertEquals(fixedTime, isoJson.getString("time"))

        // VQT format
        val vqtBytes = I3xPublisher.formatPayload(true, "Uncertain", fixedTime, I3xAddress.FORMAT_VQT)
        val vqtJson = JsonObject(String(vqtBytes))
        assertTrue(vqtJson.getBoolean("value"))
        assertEquals("Uncertain", vqtJson.getString("quality"))
        assertEquals(fixedTime, vqtJson.getString("timestamp"))
    }

    @Test
    fun testSseArrayAndBatchPayloadStructures() {
        // SSE Array payload: [{"elementId": "pump-101", "value": 45.2, "quality": "Good", "timestamp": "2026-08-15T12:00:00Z"}]
        val arrayPayload = """
            [
              {"elementId": "pump-101", "value": 45.2, "quality": "Good", "timestamp": "2026-08-15T12:00:00Z"},
              {"elementId": "pump-102", "value": {"value": 88.0, "quality": "Good", "timestamp": "2026-08-15T12:00:00Z"}}
            ]
        """.trimIndent()
        val jsonArray = JsonArray(arrayPayload)
        assertEquals(2, jsonArray.size())
        val item1 = jsonArray.getJsonObject(0)
        assertEquals("pump-101", item1.getString("elementId"))
        assertEquals(45.2, item1.getDouble("value"), 0.001)

        // SSE Batch payload: {"sequenceNumber": 1, "updates": [{"elementId": "sensors/temp", "value": 22.5, "quality": "Good"}]}
        val batchPayload = """
            {
              "sequenceNumber": 1,
              "updates": [
                {"elementId": "sensors/temp", "value": 22.5, "quality": "Good", "timestamp": "2026-08-15T12:00:00Z"}
              ]
            }
        """.trimIndent()
        val jsonBatch = JsonObject(batchPayload)
        assertTrue(jsonBatch.containsKey("updates"))
        assertEquals(1, jsonBatch.getJsonArray("updates").size())
    }
}
