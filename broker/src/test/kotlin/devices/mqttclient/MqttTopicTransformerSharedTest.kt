package at.rocworks.devices.mqttclient

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class MqttTopicTransformerSharedTest {

    @Test
    fun testIsSharedSubscription() {
        assertTrue(MqttTopicTransformer.isSharedSubscription("\$share/group1/sensor/temp"))
        assertTrue(MqttTopicTransformer.isSharedSubscription("\$queue/sensor/temp"))
        assertFalse(MqttTopicTransformer.isSharedSubscription("sensor/temp"))
        assertFalse(MqttTopicTransformer.isSharedSubscription("share/group1/sensor/temp"))
    }

    @Test
    fun testPlainFilter() {
        assertEquals("sensor/temp", MqttTopicTransformer.plainFilter("\$share/group1/sensor/temp"))
        assertEquals("sensor/#", MqttTopicTransformer.plainFilter("\$share/my-group/sensor/#"))
        assertEquals("sensor/temp", MqttTopicTransformer.plainFilter("\$queue/sensor/temp"))
        assertEquals("sensor/temp", MqttTopicTransformer.plainFilter("sensor/temp"))
    }

    @Test
    fun testMatchesRemotePatternWithSharedSubscription() {
        assertTrue(MqttTopicTransformer.matchesRemotePattern("sensor/temp", "\$share/group1/sensor/temp"))
        assertTrue(MqttTopicTransformer.matchesRemotePattern("sensor/123/temp", "\$share/group1/sensor/+/temp"))
        assertTrue(MqttTopicTransformer.matchesRemotePattern("sensor/a/b/c", "\$share/group1/sensor/#"))
        assertTrue(MqttTopicTransformer.matchesRemotePattern("sensor/temp", "\$queue/sensor/temp"))
        assertFalse(MqttTopicTransformer.matchesRemotePattern("other/temp", "\$share/group1/sensor/temp"))
    }
}
