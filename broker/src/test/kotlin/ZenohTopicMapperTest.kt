package at.rocworks

import at.rocworks.bus.ZenohTopicMapper
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

class ZenohTopicMapperTest {
    @Test
    fun `mqtt wildcards map to zenoh key expressions`() {
        assertEquals("demo/example/**", ZenohTopicMapper.subscriptionKey("demo/example", "#"))
        assertEquals(
            "demo/example/devices/*/status",
            ZenohTopicMapper.subscriptionKey("demo/example/", "devices/+/status")
        )
        assertEquals(
            "demo/example/factory/**",
            ZenohTopicMapper.subscriptionKey("demo/example", "factory/#")
        )
    }

    @Test
    fun `invalid mqtt wildcard placement is rejected`() {
        assertThrows(IllegalArgumentException::class.java) {
            ZenohTopicMapper.subscriptionKey("demo", "factory/#/status")
        }
        assertThrows(IllegalArgumentException::class.java) {
            ZenohTopicMapper.subscriptionKey("demo", "devices/device+/status")
        }
    }

    @Test
    fun `filters covered by a broader filter are removed`() {
        assertEquals(
            listOf("factory/#", "devices/+/status"),
            ZenohTopicMapper.minimalFilters(
                listOf("factory/#", "factory/+/status", "factory/line1/value", "devices/+/status")
            )
        )
        assertEquals(listOf("#"), ZenohTopicMapper.minimalFilters(listOf("factory/#", "#", "devices/+")))
        assertTrue(ZenohTopicMapper.minimalFilters(emptyList()).isEmpty())
    }

    @Test
    fun `mapToZenohKey handles localPrefix translation`() {
        // Without localPrefix
        assertEquals("monstermq/mqtt/sensors/temp", ZenohTopicMapper.mapToZenohKey("sensors/temp", "", "monstermq/mqtt"))
        assertEquals("monstermq/mqtt/sensors/temp", ZenohTopicMapper.mapToZenohKey("/sensors/temp/", "", "monstermq/mqtt/"))

        // With localPrefix
        assertEquals("monstermq/mqtt/sensors/temp", ZenohTopicMapper.mapToZenohKey("data/zenoh/sensors/temp", "data/zenoh", "monstermq/mqtt"))
        assertEquals("monstermq/mqtt/sensors/temp", ZenohTopicMapper.mapToZenohKey("/data/zenoh/sensors/temp/", "data/zenoh/", "monstermq/mqtt/"))
        
        // Mismatch localPrefix should return null
        assertNull(ZenohTopicMapper.mapToZenohKey("sensors/temp", "data/zenoh", "monstermq/mqtt"))
    }

    @Test
    fun `mapToMqttTopic handles localPrefix translation`() {
        // Without localPrefix
        assertEquals("sensors/temp", ZenohTopicMapper.mapToMqttTopic("monstermq/mqtt/sensors/temp", "", "monstermq/mqtt"))

        // With localPrefix
        assertEquals("data/zenoh/sensors/temp", ZenohTopicMapper.mapToMqttTopic("monstermq/mqtt/sensors/temp", "data/zenoh", "monstermq/mqtt"))
        
        // Mismatch remote prefix should return null
        assertNull(ZenohTopicMapper.mapToMqttTopic("other/prefix/sensors/temp", "data/zenoh", "monstermq/mqtt"))

        // Wildcard conversions
        assertEquals("sensors/#", ZenohTopicMapper.mapToMqttTopic("monstermq/mqtt/sensors/**", "", "monstermq/mqtt"))
        assertEquals("data/zenoh/sensors/+/temp", ZenohTopicMapper.mapToMqttTopic("monstermq/mqtt/sensors/*/temp", "data/zenoh", "monstermq/mqtt"))
    }

    @Test
    fun `subscriptionKey handles localPrefix translation`() {
        // Without localPrefix
        assertEquals("monstermq/mqtt/sensors/**", ZenohTopicMapper.subscriptionKey("sensors/#", "", "monstermq/mqtt"))

        // With localPrefix
        assertEquals("monstermq/mqtt/sensors/**", ZenohTopicMapper.subscriptionKey("data/zenoh/sensors/#", "data/zenoh", "monstermq/mqtt"))
        assertEquals("monstermq/mqtt/**", ZenohTopicMapper.subscriptionKey("#", "data/zenoh", "monstermq/mqtt"))
        
        // Mismatch local prefix should return null
        assertNull(ZenohTopicMapper.subscriptionKey("sensors/#", "data/zenoh", "monstermq/mqtt"))
    }

    @Test
    fun `empty remotePrefix mappings`() {
        // Map to Zenoh key with empty remote prefix
        assertEquals("sensors/temp", ZenohTopicMapper.mapToZenohKey("sensors/temp", "", ""))
        assertEquals("sensors/temp", ZenohTopicMapper.mapToZenohKey("data/zenoh/sensors/temp", "data/zenoh", ""))

        // Map back to MQTT topic with empty remote prefix
        assertEquals("sensors/temp", ZenohTopicMapper.mapToMqttTopic("sensors/temp", "", ""))
        assertEquals("data/zenoh/sensors/temp", ZenohTopicMapper.mapToMqttTopic("sensors/temp", "data/zenoh", ""))

        // Subscription key with empty remote prefix
        assertEquals("sensors/**", ZenohTopicMapper.subscriptionKey("sensors/#", "", ""))
        assertEquals("sensors/**", ZenohTopicMapper.subscriptionKey("data/zenoh/sensors/#", "data/zenoh", ""))
        assertEquals("**", ZenohTopicMapper.subscriptionKey("#", "", ""))
    }
}
