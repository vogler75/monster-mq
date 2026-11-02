package at.rocworks.devices.sparkplugb

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test
import org.eclipse.tahu.message.model.Metric
import org.eclipse.tahu.message.model.MetricDataType

class SparkplugMetricAliasRegistryTest {

    @Test
    fun aliasesAreScopedByGroupNodeAndDevice() {
        val aliases = SparkplugMetricAliasRegistry()
        aliases.replaceNode("group-1", "node-1", mapOf(79L to definition("NodeMetric")))
        aliases.replace("group-1", "node-1", "device-1", mapOf(79L to definition("DeviceMetric")))
        aliases.replaceNode("group-2", "node-1", mapOf(79L to definition("OtherGroupMetric")))

        assertEquals("NodeMetric", aliases.resolve("group-1", "node-1", null, 79L)?.name)
        assertEquals("DeviceMetric", aliases.resolve("group-1", "node-1", "device-1", 79L)?.name)
        assertEquals("OtherGroupMetric", aliases.resolve("group-2", "node-1", null, 79L)?.name)
        assertNull(aliases.resolve("group-1", "node-2", null, 79L))
    }

    @Test
    fun birthReplacementRemovesAliasesNotPresentInTheNewBirth() {
        val aliases = SparkplugMetricAliasRegistry()
        aliases.replace(
            "group-1",
            "node-1",
            "device-1",
            mapOf(79L to definition("Voltage"), 80L to definition("Current"))
        )

        aliases.replace("group-1", "node-1", "device-1", mapOf(79L to definition("LineVoltage")))

        assertEquals("LineVoltage", aliases.resolve("group-1", "node-1", "device-1", 79L)?.name)
        assertNull(aliases.resolve("group-1", "node-1", "device-1", 80L))
    }

    @Test
    fun nodeBirthReplacesNodeAliasesAndClearsPreviousDeviceAliases() {
        val aliases = SparkplugMetricAliasRegistry()
        aliases.replaceNode("group-1", "node-1", mapOf(1L to definition("OldNodeMetric")))
        aliases.replace("group-1", "node-1", "device-1", mapOf(2L to definition("OldDeviceMetric")))

        aliases.replaceNode("group-1", "node-1", mapOf(3L to definition("NewNodeMetric")))

        assertNull(aliases.resolve("group-1", "node-1", null, 1L))
        assertEquals("NewNodeMetric", aliases.resolve("group-1", "node-1", null, 3L)?.name)
        assertNull(aliases.resolve("group-1", "node-1", "device-1", 2L))
    }

    @Test
    fun deviceDeathClearsOnlyThatDeviceAndNodeDeathClearsAllNodeScopes() {
        val aliases = SparkplugMetricAliasRegistry()
        aliases.replaceNode("group-1", "node-1", mapOf(1L to definition("NodeMetric")))
        aliases.replace("group-1", "node-1", "device-1", mapOf(2L to definition("DeviceMetric")))
        aliases.replace("group-1", "node-1", "device-2", mapOf(3L to definition("OtherDeviceMetric")))
        aliases.replace("group-1", "node-2", "device-1", mapOf(4L to definition("OtherNodeMetric")))

        aliases.clearDevice("group-1", "node-1", "device-1")

        assertNull(aliases.resolve("group-1", "node-1", "device-1", 2L))
        assertEquals("NodeMetric", aliases.resolve("group-1", "node-1", null, 1L)?.name)
        assertEquals("OtherDeviceMetric", aliases.resolve("group-1", "node-1", "device-2", 3L)?.name)

        aliases.clearNode("group-1", "node-1")

        assertNull(aliases.resolve("group-1", "node-1", null, 1L))
        assertNull(aliases.resolve("group-1", "node-1", "device-2", 3L))
        assertEquals("OtherNodeMetric", aliases.resolve("group-1", "node-2", "device-1", 4L)?.name)
    }

    @Test
    fun metricNamesPreferExplicitNameThenResolvedAliasThenFallback() {
        assertEquals("Explicit", resolveSparkplugMetricName("Explicit", 79L, "Cached"))
        assertEquals("Cached", resolveSparkplugMetricName(null, 79L, "Cached"))
        assertEquals("alias/79", resolveSparkplugMetricName(null, 79L, null))
        assertEquals("Named", resolveSparkplugMetricName("Named", null, null))
    }

    @Test
    fun birthMetricAliasesIncludeTheNameAndDataType() {
        val named = Metric().apply {
            name = "Temperature"
            alias = 79L
            dataType = MetricDataType.Float
        }
        val noAlias = Metric().apply {
            name = "Unaliased"
            dataType = MetricDataType.Float
        }
        val noName = Metric().apply {
            alias = 80L
            dataType = MetricDataType.Float
        }

        assertEquals(
            mapOf(79L to definition("Temperature")),
            sparkplugMetricAliases(listOf(named, noAlias, noName))
        )
    }

    @Test
    fun dataTypeUsesBirthDefinitionWhenDataMetricOmitsIt() {
        assertEquals("Float", resolveSparkplugMetricDataType("Unknown", "Float"))
        assertEquals("Double", resolveSparkplugMetricDataType("Double", "Float"))
        assertEquals("null", resolveSparkplugMetricDataType(null, null))
    }

    private fun definition(name: String) = SparkplugMetricDefinition(name, "Float")
}
