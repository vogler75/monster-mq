package at.rocworks

import at.rocworks.agents.AgentTools
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class AgentToolsTest {

    @Test
    fun testPublishAllowedTopicsEmpty() {
        // By default, empty allowedPublishTopics means ALL topics are allowed
        val tools = AgentTools(
            archiveHandler = null,
            retainedStore = null,
            agentClientId = "test-agent-client",
            agentName = "test-agent",
            allowedPublishTopics = emptyList()
        )

        val result1 = tools.publishMessage("sensors/temp/1", "23.5")
        assertEquals("Published to sensors/temp/1", result1)

        val result2 = tools.publishMessage("commands/heater", "ON")
        assertEquals("Published to commands/heater", result2)
    }

    @Test
    fun testPublishAllowedTopicsRestrictions() {
        val allowedPatterns = listOf(
            "alerts/+/high",
            "commands/heater/#",
            "status/agent"
        )

        val tools = AgentTools(
            archiveHandler = null,
            retainedStore = null,
            agentClientId = "test-agent-client",
            agentName = "test-agent",
            allowedPublishTopics = allowedPatterns
        )

        // Exact match
        assertEquals("Published to status/agent", tools.publishMessage("status/agent", "OK"))

        // Single wildcard match
        assertEquals("Published to alerts/temperature/high", tools.publishMessage("alerts/temperature/high", "critical"))
        assertEquals("Published to alerts/pressure/high", tools.publishMessage("alerts/pressure/high", "critical"))

        // Multi wildcard match
        assertEquals("Published to commands/heater/set", tools.publishMessage("commands/heater/set", "value=20"))
        assertEquals("Published to commands/heater/mode/auto", tools.publishMessage("commands/heater/mode/auto", "true"))

        // Reject mismatch
        val reject1 = tools.publishMessage("alerts/temperature/low", "12.0")
        assertTrue(reject1.contains("Publish rejected"))
        assertTrue(reject1.contains("does not match any allowed publish topics"))

        val reject2 = tools.publishMessage("commands/fan/set", "speed=3")
        assertTrue(reject2.contains("Publish rejected"))

        val reject3 = tools.publishMessage("status/other", "OK")
        assertTrue(reject3.contains("Publish rejected"))
    }
}
