package at.rocworks.extensions.redfish

import io.vertx.core.json.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test

class RedfishMapperTest {

    @Test
    fun testDirectFieldMapping() {
        val payload = """{"reading": 23.5, "sensorId": "cpu-temp", "chassisId": "Rack-01", "readingType": "Temperature", "readingUnits": "Cel"}""".toByteArray()
        val gw = GatewayConfig(
            topicPrefix = "redfish",
            chassisId = "DefaultChassis",
            defaultReadingType = "Temperature",
            defaultReadingUnits = "Cel"
        )

        val records = RedfishMapper.extractSensorRecords(payload, "sensors/rack1/temp", gw)
        assertEquals(1, records.size)
        val r = records[0]
        assertEquals("Rack-01", r.chassisId)
        assertEquals("cpu-temp", r.sensorId)
        assertEquals(23.5, r.reading, 0.001)
        assertEquals("Temperature", r.readingType)
        assertEquals("Cel", r.readingUnits)
        assertEquals("OK", r.health)
        assertEquals("Enabled", r.state)
    }

    @Test
    fun testJsonPathMapping() {
        val payload = """
        {
            "telemetry": {
                "metrics": {
                    "temperature": 75.4,
                    "unit": "Cel"
                }
            },
            "device": {
                "identifier": "motor-temp-1",
                "enclosure": "Line-A"
            }
        }
        """.trimIndent().toByteArray()

        val gw = GatewayConfig(
            topicPrefix = "telemetry/redfish",
            chassisId = "EdgeNode",
            jsonSchema = mapOf(
                "mapping" to mapOf(
                    "reading" to "$.telemetry.metrics.temperature",
                    "sensorId" to "$.device.identifier",
                    "chassisId" to "$.device.enclosure"
                )
            )
        )

        val records = RedfishMapper.extractSensorRecords(payload, "factory/motor1", gw)
        assertEquals(1, records.size)
        val r = records[0]
        assertEquals("Line-A", r.chassisId)
        assertEquals("motor-temp-1", r.sensorId)
        assertEquals(75.4, r.reading, 0.001)
        assertEquals("telemetry/redfish", r.topicPrefix)
    }

    @Test
    fun testArrayPathUnrolling() {
        val payload = """
        {
            "chassis": "Rack-B",
            "readings": [
                {"id": "fan-inlet-1", "val": 2400.0, "type": "AirFlow", "unit": "RPM"},
                {"id": "fan-inlet-2", "val": 2350.0, "type": "AirFlow", "unit": "RPM"},
                {"id": "temp-exhaust", "val": 42.1, "type": "Temperature", "unit": "Cel"}
            ]
        }
        """.trimIndent().toByteArray()

        val gw = GatewayConfig(
            topicPrefix = "redfish",
            chassisId = "FallbackChassis",
            jsonSchema = mapOf(
                "arrayPath" to "$.readings[*]",
                "mapping" to mapOf(
                    "reading" to "$.val",
                    "sensorId" to "$.id",
                    "chassisId" to "$.chassis",
                    "readingType" to "$.type",
                    "readingUnits" to "$.unit"
                )
            )
        )

        val records = RedfishMapper.extractSensorRecords(payload, "sensors/rackB/batch", gw)
        assertEquals(3, records.size)

        assertEquals("fan-inlet-1", records[0].sensorId)
        assertEquals("Rack-B", records[0].chassisId)
        assertEquals(2400.0, records[0].reading, 0.001)
        assertEquals("AirFlow", records[0].readingType)
        assertEquals("RPM", records[0].readingUnits)

        assertEquals("fan-inlet-2", records[1].sensorId)
        assertEquals(2350.0, records[1].reading, 0.001)

        assertEquals("temp-exhaust", records[2].sensorId)
        assertEquals(42.1, records[2].reading, 0.001)
    }

    @Test
    fun testThresholdHealthCalculation() {
        val thresholds = ThresholdsConfig(
            upperCaution = 60.0,
            upperCritical = 80.0,
            lowerCaution = 10.0,
            lowerCritical = 0.0
        )

        assertEquals("OK", RedfishUtils.calculateHealth(25.0, thresholds))
        assertEquals("Warning", RedfishUtils.calculateHealth(65.0, thresholds))
        assertEquals("Critical", RedfishUtils.calculateHealth(85.0, thresholds))
        assertEquals("Warning", RedfishUtils.calculateHealth(5.0, thresholds))
        assertEquals("Critical", RedfishUtils.calculateHealth(-5.0, thresholds))
    }

    @Test
    fun testTopicFallbackForSensorId() {
        val payload = """{"val": 3.3}""".toByteArray()
        val gw = GatewayConfig(
            topicPrefix = "redfish",
            chassisId = "EdgeNode",
            defaultReadingType = "Voltage",
            defaultReadingUnits = "V",
            jsonSchema = mapOf(
                "mapping" to mapOf("reading" to "$.val")
            )
        )

        val records = RedfishMapper.extractSensorRecords(payload, "sensors/rack1/bus_voltage", gw)
        assertEquals(1, records.size)
        val r = records[0]
        assertEquals("bus_voltage", r.sensorId)
        assertEquals("EdgeNode", r.chassisId)
        assertEquals(3.3, r.reading, 0.001)
        assertEquals("Voltage", r.readingType)
        assertEquals("V", r.readingUnits)
    }

    @Test
    fun testEvaluateJsonPath() {
        val json = JsonObject("""
            {
                "a": {
                    "b": [
                        {"c": 10},
                        {"c": 20}
                    ]
                }
            }
        """.trimIndent()).map

        val (val1, ok1) = RedfishMapper.evaluateJsonPath(json, "$.a.b[0].c")
        assertTrue(ok1)
        assertEquals(10, val1)

        val (val2, ok2) = RedfishMapper.evaluateJsonPath(json, "$.a.b[*].c")
        assertTrue(ok2)
        assertTrue(val2 is List<*>)
        assertEquals(listOf(10, 20), val2)
    }
}
