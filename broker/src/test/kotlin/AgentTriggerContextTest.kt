package at.rocworks

import at.rocworks.agents.TriggerContext
import at.rocworks.agents.TriggerType
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class AgentTriggerContextTest {

    @Test
    fun testTriggerContextCreation() {
        val context = TriggerContext(TriggerType.MQTT, "sensors/temperature", "22.5")
        assertEquals(TriggerType.MQTT, context.type)
        assertEquals("sensors/temperature", context.topicName)
        assertEquals("22.5", context.value)
    }

    @Test
    fun testTriggerContextDefaults() {
        val context = TriggerContext(TriggerType.CRON)
        assertEquals(TriggerType.CRON, context.type)
        assertNull(context.topicName)
        assertNull(context.value)
    }
}
