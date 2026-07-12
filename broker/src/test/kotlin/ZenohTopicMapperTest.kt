package at.rocworks

import at.rocworks.bus.ZenohTopicMapper
import org.junit.Assert.assertEquals
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
}
