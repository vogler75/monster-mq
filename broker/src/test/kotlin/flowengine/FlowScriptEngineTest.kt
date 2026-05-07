package at.rocworks.flowengine

import io.vertx.core.json.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class FlowScriptEngineTest {

    @Test
    fun mqttProxyPublishesWithDefaults() {
        var publishedTopic: String? = null
        var publishedPayload: Any? = null
        var publishedQos: Int? = null
        var publishedRetain: Boolean? = null

        val proxy = FlowScriptEngine.MqttProxy { topic, payload, qos, retain ->
            publishedTopic = topic
            publishedPayload = payload
            publishedQos = qos
            publishedRetain = retain
            true
        }

        assertTrue(proxy.publish("flows/output", "hello"))
        assertEquals("flows/output", publishedTopic)
        assertEquals("hello", publishedPayload)
        assertEquals(0, publishedQos)
        assertEquals(false, publishedRetain)
    }

    @Test
    fun scriptEngineSupportsTwoArgumentMqttPublishCall() {
        val engine = FlowScriptEngine()
        var publishedTopic: String? = null
        var publishedPayload: Any? = null
        var publishedQos: Int? = null
        var publishedRetain: Boolean? = null

        try {
            val result = engine.execute(
                script = """mqtt.publish("flows/output", "hello from script");""",
                inputs = emptyMap(),
                state = mutableMapOf(),
                flowVariables = emptyMap(),
                onOutput = { _, _ -> },
                mqttPublisher = { topic, payload, qos, retain ->
                    publishedTopic = topic
                    publishedPayload = payload
                    publishedQos = qos
                    publishedRetain = retain
                    true
                }
            )

            assertTrue(result.success)
            assertEquals("flows/output", publishedTopic)
            assertEquals("hello from script", publishedPayload)
            assertEquals(0, publishedQos)
            assertEquals(false, publishedRetain)
        } finally {
            engine.close()
        }
    }

    @Test
    fun mqttProxyPassesExplicitFlagsAndStructuredPayload() {
        var publishedPayload: Any? = null
        var publishedQos: Int? = null
        var publishedRetain: Boolean? = null

        val proxy = FlowScriptEngine.MqttProxy { _, payload, qos, retain ->
            publishedPayload = payload
            publishedQos = qos
            publishedRetain = retain
            true
        }

        val payload = JsonObject()
            .put("value", 42)
            .put("status", "ok")

        assertTrue(proxy.publish("flows/state", payload, 1, true))
        assertEquals(payload, publishedPayload)
        assertEquals(1, publishedQos)
        assertEquals(true, publishedRetain)
    }

    @Test
    fun mqttProxyRejectsWildcardTopics() {
        var called = false
        val proxy = FlowScriptEngine.MqttProxy { _, _, _, _ ->
            called = true
            true
        }

        assertFalse(proxy.publish("flows/+/state", "hello"))
        assertFalse(called)
    }
}
