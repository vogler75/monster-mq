package at.rocworks.devices.script

import at.rocworks.data.BrokerMessage
import at.rocworks.stores.devices.ScriptConfig
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test

class ScriptEngineTest {

    @Test
    fun testBasicPythonExecution() {
        val config = ScriptConfig(
            language = "python",
            script = """
x = 10 + 20
log.info("Calculated x:", x)
result = x
"""
        )

        val engine = ScriptEngine("BasicTest", config)
        val res = engine.execute(null)

        assertTrue(res.errors.joinToString("\n"), res.success)
        assertEquals("30", res.returnValue?.toString())
        assertTrue(res.logs.any { it.contains("Calculated x: 30") })
    }

    @Test
    fun testMsgAccessDictAndProperty() {
        val config = ScriptConfig(
            language = "python",
            script = """
topic = msg["topic"]
payload = msg["payload"]
raw = msg["raw_payload"]
qos = msg.qos
retain = msg.retain

temp = payload["temperature"]
log.info(f"Topic: {topic}, Temp: {temp}, QoS: {qos}, Retain: {retain}")

if temp > 40:
    mqtt.publish("alerts/temp", f"High temperature: {temp}", qos=1, retain=True)

result = temp
"""
        )

        val engine = ScriptEngine("MsgTest", config)
        val brokerMsg = BrokerMessage(
            messageId = 1,
            topicName = "sensors/bedroom",
            payload = """{"temperature": 42.5, "unit": "C"}""".toByteArray(),
            qosLevel = 1,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = "test-client"
        )

        val res = engine.execute(brokerMsg)
        assertTrue(res.errors.joinToString("\n"), res.success)
        assertEquals("42.5", res.returnValue?.toString())
        assertEquals(1, res.outputMessages.size)
        assertEquals("alerts/temp", res.outputMessages[0].topic)
        assertEquals("High temperature: 42.5", res.outputMessages[0].payload)
        assertEquals(1, res.outputMessages[0].qos)
        assertTrue(res.outputMessages[0].retain)
    }

    @Test
    fun testStatePersistenceAcrossInvocations() {
        val config = ScriptConfig(
            language = "python",
            script = """
count = state.get("count", 0) + 1
state["count"] = count
result = count
"""
        )

        val engine = ScriptEngine("StateTest", config)

        val res1 = engine.execute(null)
        assertTrue(res1.success)
        assertEquals("1", res1.returnValue?.toString())

        val res2 = engine.execute(null)
        assertTrue(res2.success)
        assertEquals("2", res2.returnValue?.toString())

        val res3 = engine.execute(null)
        assertTrue(res3.success)
        assertEquals("3", res3.returnValue?.toString())
    }

    @Test
    fun testGlobalStoreSharing() {
        val globalStore = ScriptGlobalStore()

        val writerConfig = ScriptConfig(
            language = "python",
            script = """shared.set("broker_mode", "ACTIVE")"""
        )
        val readerConfig = ScriptConfig(
            language = "python",
            script = """result = shared.get("broker_mode")"""
        )

        val writerEngine = ScriptEngine("Writer", writerConfig, globalStore = globalStore)
        val readerEngine = ScriptEngine("Reader", readerConfig, globalStore = globalStore)

        assertTrue(writerEngine.execute(null).success)
        val readerRes = readerEngine.execute(null)
        assertTrue(readerRes.success)
        assertEquals("ACTIVE", readerRes.returnValue?.toString())
    }

    @Test
    fun testInterScriptCall() {
        var calledArgs: Map<String, Any?>? = null
        val invoker: (String, Map<String, Any?>) -> Any? = { name, args ->
            calledArgs = args
            "ResultFrom_$name"
        }

        val config = ScriptConfig(
            language = "python",
            script = """
call_res = scripts.call("TargetScript", {"param1": "foo", "param2": 99})
result = call_res
"""
        )

        val engine = ScriptEngine("Caller", config, scriptInvoker = invoker)
        val res = engine.execute(null)

        assertTrue(res.success)
        assertEquals("ResultFrom_TargetScript", res.returnValue?.toString())
        assertNotNull(calledArgs)
        assertEquals("foo", calledArgs?.get("param1"))
    }

    @Test
    fun testJavaScriptExecution() {
        val config = ScriptConfig(
            language = "javascript",
            script = """
const topic = msg ? msg.topic : "default";
log.info("JS executing on: " + topic);
mqtt.publish("js/output", JSON.stringify({ ok: true, topic: topic }));
result = "done";
"""
        )

        val engine = ScriptEngine("JsTest", config)
        val brokerMsg = BrokerMessage(
            messageId = 1,
            topicName = "test/js",
            payload = "hello".toByteArray(),
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = "test"
        )

        val res = engine.execute(brokerMsg)
        assertTrue(res.errors.joinToString("\n"), res.success)
        assertEquals("done", res.returnValue?.toString())
        assertEquals(1, res.outputMessages.size)
        assertEquals("js/output", res.outputMessages[0].topic)
    }

    @Test
    fun testCompilationErrorHandling() {
        val invalidConfig = ScriptConfig(
            language = "python",
            script = "def invalid_syntax(:"
        )

        try {
            ScriptEngine("InvalidSyntax", invalidConfig)
            assertFalse("Should have thrown compilation exception", true)
        } catch (e: Exception) {
            assertTrue(e.message != null)
        }
    }

    @Test
    fun testJsonModule() {
        val config = ScriptConfig(
            language = "python",
            script = """
temp = msg.payload["temperature"]
encoded = json.encode({"val": temp, "status": "ALARM"})
mqtt.publish("alerts/temp", encoded, retain=True)

decoded = json.decode(encoded)
result = decoded["status"]
"""
        )

        val engine = ScriptEngine("JsonTest", config)
        val brokerMsg = BrokerMessage(
            messageId = 1,
            topicName = "sensors/bedroom",
            payload = """{"temperature": 42.5}""".toByteArray(),
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = "test-client"
        )

        val res = engine.execute(brokerMsg)
        assertTrue(res.errors.joinToString("\n"), res.success)
        assertEquals("ALARM", res.returnValue?.toString())
        assertEquals(1, res.outputMessages.size)
        assertEquals("alerts/temp", res.outputMessages[0].topic)
        assertTrue(res.outputMessages[0].retain)
        assertTrue(res.outputMessages[0].payload.contains("ALARM"))
    }
}
