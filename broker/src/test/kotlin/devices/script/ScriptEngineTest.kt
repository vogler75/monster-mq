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
    fun testStateWithDictAndListAcrossInvocations() {
        val config = ScriptConfig(
            language = "python",
            script = """
if "buffer" not in state:
    state["buffer"] = {"items": []}

if msg is not None:
    state["buffer"]["items"].append(msg["payload"])
    result = len(state["buffer"]["items"])
else:
    result = len(state["buffer"]["items"])
"""
        )

        val engine = ScriptEngine("BufferTest", config)

        val msg1 = BrokerMessage(
            messageId = 1,
            topicName = "test",
            payload = "val1".toByteArray(),
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = "c1"
        )
        val res1 = engine.execute(msg1)
        assertTrue(res1.errors.joinToString("\n"), res1.success)

        val msg2 = BrokerMessage(
            messageId = 2,
            topicName = "test",
            payload = "val2".toByteArray(),
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = "c1"
        )
        val res2 = engine.execute(msg2)
        assertTrue(res2.errors.joinToString("\n"), res2.success)
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

    @Test
    fun testScriptDocumentationAndSkillFiles() {
        val pyDocStream = this::class.java.classLoader.getResourceAsStream("docs/broker-script-python.md")
        assertNotNull("broker-script-python.md should exist in resources", pyDocStream)
        val pyDoc = pyDocStream!!.bufferedReader().use { it.readText() }
        assertTrue(pyDoc.contains("MonsterMQ Main Python Script Reference"))

        val pySkillStream = this::class.java.classLoader.getResourceAsStream("docs/broker-script-python-skill.md")
        assertNotNull("broker-script-python-skill.md should exist in resources", pySkillStream)
        val pySkill = pySkillStream!!.bufferedReader().use { it.readText() }
        assertTrue(pySkill.contains("MonsterMQ Main Python Script Skill"))

        val jsDocStream = this::class.java.classLoader.getResourceAsStream("docs/broker-script-javascript.md")
        assertNotNull("broker-script-javascript.md should exist in resources", jsDocStream)
        val jsDoc = jsDocStream!!.bufferedReader().use { it.readText() }
        assertTrue(jsDoc.contains("MonsterMQ Main JavaScript Script Reference"))

        val jsSkillStream = this::class.java.classLoader.getResourceAsStream("docs/broker-script-javascript-skill.md")
        assertNotNull("broker-script-javascript-skill.md should exist in resources", jsSkillStream)
        val jsSkill = jsSkillStream!!.bufferedReader().use { it.readText() }
        assertTrue(jsSkill.contains("MonsterMQ Main JavaScript Script Skill"))
    }

    @Test
    fun testAverageCalculationTopicAndTimerPattern() {
        val config = ScriptConfig(
            language = "python",
            script = """
import json

TARGET_TOPICS = ["sensor/power"]

if "buffer" not in state:
    state["buffer"] = {topic: [] for topic in TARGET_TOPICS}

if msg is not None:
    topic = msg["topic"]
    if topic in state["buffer"]:
        payload = msg["payload"]
        val = payload.get("Value") if isinstance(payload, dict) else None
        if val is not None:
            state["buffer"][topic].append(float(val))
            log.debug(f"Stored value {val} for {topic}")
else:
    for topic in TARGET_TOPICS:
        values = state["buffer"].get(topic, [])
        if values:
            avg = sum(values) / len(values)
            mqtt.publish(f"{topic}/1MinAvg", json.dumps({"Value": avg, "count": len(values)}))
            state["buffer"][topic] = []
            result = avg
"""
        )

        val engine = ScriptEngine("AveragePatternTest", config)

        // Topic Message 1: 100.0
        val msg1 = BrokerMessage(
            messageId = 1,
            topicName = "sensor/power",
            payload = """{"Value": 100.0}""".toByteArray(),
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = "c1"
        )
        val res1 = engine.execute(msg1)
        assertTrue(res1.errors.joinToString("\n"), res1.success)

        // Topic Message 2: 200.0
        val msg2 = BrokerMessage(
            messageId = 2,
            topicName = "sensor/power",
            payload = """{"Value": 200.0}""".toByteArray(),
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = "c1"
        )
        val res2 = engine.execute(msg2)
        assertTrue(res2.errors.joinToString("\n"), res2.success)

        // Timer Execution (msg = null)
        val resTimer = engine.execute(null)
        assertTrue(resTimer.errors.joinToString("\n"), resTimer.success)
        assertEquals("150", resTimer.returnValue?.toString())
        assertEquals(1, resTimer.outputMessages.size)
        assertEquals("sensor/power/1MinAvg", resTimer.outputMessages[0].topic)
        assertTrue(resTimer.outputMessages[0].payload.contains("150.0"))
    }

    @Test
    fun testScriptConnectorSingletonErrorRecovery() {
        val vertx = io.vertx.core.Vertx.vertx()
        try {
            val deviceConfig = at.rocworks.stores.DeviceConfig(
                name = "ConnectorRecoveryTest",
                namespace = "test",
                nodeId = "local",
                type = at.rocworks.stores.DeviceConfig.DEVICE_TYPE_SCRIPT,
                config = io.vertx.core.json.JsonObject()
                    .put("language", "python")
                    .put("instanceMode", "SINGLETON")
                    .put("script", """
count = state.get("count", 0) + 1
state["count"] = count
if count == 2:
    raise ValueError("Intentional error on 2nd run")
result = count
""")
            )

            val latch = java.util.concurrent.CountDownLatch(1)
            val connector = ScriptConnector(deviceConfig, null, ScriptGlobalStore())
            vertx.deployVerticle(connector).onComplete { latch.countDown() }
            assertTrue(latch.await(5, java.util.concurrent.TimeUnit.SECONDS))

            // 1st run: count = 1 (SUCCESS)
            val res1 = connector.dispatchExecution(null, null, "TEST").toCompletionStage().toCompletableFuture().get()
            assertTrue(res1.success)
            assertEquals("1", res1.returnValue?.toString())
            assertEquals(1L, connector.executionCount.get())
            assertEquals(0L, connector.errorCount.get())

            // 2nd run: raises error (ERROR)
            val res2 = connector.dispatchExecution(null, null, "TEST").toCompletionStage().toCompletableFuture().get()
            assertFalse(res2.success)
            assertEquals(2L, connector.executionCount.get())
            assertEquals(1L, connector.errorCount.get())

            // 3rd run: count = 3 (SUCCESS - verifies queue was NOT poisoned by run 2 error!)
            val res3 = connector.dispatchExecution(null, null, "TEST").toCompletionStage().toCompletableFuture().get()
            assertTrue(res3.success)
            assertEquals("3", res3.returnValue?.toString())
            assertEquals(3L, connector.executionCount.get())
            assertEquals(1L, connector.errorCount.get())
        } finally {
            vertx.close()
        }
    }

    @Test
    fun testTriggerTimePython() {
        val config = ScriptConfig(
            language = "python",
            script = """
iso = trigger_time.iso
time_ms = trigger_time.time_ms
ts = trigger_time.timestamp
dict_time = trigger_time["Time"]
dict_timems = trigger_time["TimeMS"]
str_time = str(trigger_time)
trig_type = trigger.type

result = {
    "iso": iso,
    "time_ms": time_ms,
    "timestamp": ts,
    "dict_time": dict_time,
    "dict_timems": dict_timems,
    "str_time": str_time,
    "type": trig_type
}
"""
        )

        val engine = ScriptEngine("TriggerTimeTest", config)
        val fixedInstant = java.time.Instant.parse("2026-09-21T14:15:00.000Z")
        val ctx = ScriptTriggerContext("TIMER", fixedInstant)
        val res = engine.execute(null, dryRun = false, triggerContext = ctx)

        assertTrue(res.errors.joinToString("\n"), res.success)
        assertNotNull(res.returnValue)
        val ret = res.returnValue as Map<*, *>
        assertEquals("2026-09-21T14:15:00Z", ret["iso"])
        assertEquals("2026-09-21T14:15:00Z", ret["dict_time"])
        assertEquals("2026-09-21T14:15:00Z", ret["str_time"])
        assertEquals(fixedInstant.toEpochMilli(), (ret["time_ms"] as Number).toLong())
        assertEquals(fixedInstant.toEpochMilli(), (ret["dict_timems"] as Number).toLong())
        assertEquals(fixedInstant.epochSecond, (ret["timestamp"] as Number).toLong())
        assertEquals("TIMER", ret["type"])
    }

    @Test
    fun testTriggerTimeJavaScript() {
        val config = ScriptConfig(
            language = "javascript",
            script = """
result = {
    iso: trigger_time.iso,
    time_ms: trigger_time.time_ms,
    timestamp: trigger_time.timestamp,
    type: trigger.type
};
"""
        )

        val engine = ScriptEngine("TriggerTimeJsTest", config)
        val fixedInstant = java.time.Instant.parse("2026-09-21T12:00:00.000Z")
        val ctx = ScriptTriggerContext("TIMER", fixedInstant)
        val res = engine.execute(null, dryRun = false, triggerContext = ctx)

        assertTrue(res.errors.joinToString("\n"), res.success)
        assertNotNull(res.returnValue)
        val ret = res.returnValue as Map<*, *>
        assertEquals("2026-09-21T12:00:00Z", ret["iso"])
        assertEquals(fixedInstant.toEpochMilli(), (ret["time_ms"] as Number).toLong())
        assertEquals("TIMER", ret["type"])
    }
}

