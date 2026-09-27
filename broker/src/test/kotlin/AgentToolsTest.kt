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

    @Test
    fun testInvokeAgentCircularDetection() {
        val tools = AgentTools(
            archiveHandler = null,
            retainedStore = null,
            agentClientId = "agent-A-client",
            agentName = "agent-A",
            subAgentsAllowAll = true,
            getCurrentCallStack = { listOf("user", "agent-B", "agent-C") }
        )

        // Attempting to invoke agent-B when agent-B is in call stack
        val result = tools.invokeAgent("agent-B", "Do task", null)
        assertTrue(result.contains("Circular invocation detected"))
        assertTrue(result.contains("agent-B"))
    }

    @Test
    fun testInvokeAgentMaxCallDepth() {
        val tools = AgentTools(
            archiveHandler = null,
            retainedStore = null,
            agentClientId = "agent-A-client",
            agentName = "agent-A",
            subAgentsAllowAll = true,
            getCurrentCallStack = { listOf("a1", "a2", "a3", "a4", "a5") }
        )

        val result = tools.invokeAgent("agent-Z", "Do task", null)
        assertTrue(result.contains("Maximum agent call depth"))
    }

    @Test
    fun testInvokeAgentSyncAwait() {
        val events = mutableListOf<String>()
        val published = mutableListOf<Pair<String, String>>()
        val tools = AgentTools(
            archiveHandler = null,
            retainedStore = null,
            agentClientId = "agent-A-client",
            agentName = "agent-A",
            subAgentsAllowAll = true,
            getCurrentCallStack = { listOf("agent-root") },
            registerPendingTask = { taskId, _, _ -> events.add("register:$taskId") },
            taskPublisher = { topic, payload -> published.add(topic to payload); events.add("publish"); true },
            awaitSubAgentResult = { _, targetAgent, _ ->
                events.add("await")
                "Calculated result from $targetAgent: 42"
            }
        )

        val result = tools.invokeAgent("agent-calc", "21 * 2", null)
        assertEquals("Calculated result from agent-calc: 42", result)
        // The wait handle must be registered before the task is published
        assertEquals(3, events.size)
        assertTrue(events[0].startsWith("register:"))
        assertEquals("publish", events[1])
        assertEquals("await", events[2])
        val (topic, payload) = published.single()
        assertTrue(topic.startsWith("a2a/v1/default/default/agents/agent-calc/inbox/"))
        val json = io.vertx.core.json.JsonObject(payload)
        assertEquals(listOf("agent-root", "agent-A"), json.getJsonArray("callStack").list)
        assertEquals("a2a/v1/default/default/agents/agent-A/inbox/${json.getString("taskId")}", json.getString("replyTo"))
    }

    @Test
    fun testInvokeAgentPublishFailureCancelsPendingTask() {
        val cancelled = mutableListOf<String>()
        val registered = mutableListOf<String>()
        val tools = AgentTools(
            archiveHandler = null,
            retainedStore = null,
            agentClientId = "agent-A-client",
            agentName = "agent-A",
            subAgentsAllowAll = true,
            registerPendingTask = { taskId, _, _ -> registered.add(taskId) },
            cancelPendingTask = { cancelled.add(it) },
            taskPublisher = { _, _ -> false },
            awaitSubAgentResult = { _, _, _ -> "should not be called" }
        )

        val result = tools.invokeAgent("agent-calc", "21 * 2", null)
        assertTrue(result.contains("could not send task"))
        assertEquals(registered, cancelled)
    }

    @Test
    fun testInvokeAgentAsyncFallback() {
        val tools = AgentTools(
            archiveHandler = null,
            retainedStore = null,
            agentClientId = "agent-A-client",
            agentName = "agent-A",
            subAgentsAllowAll = true,
            taskPublisher = { _, _ -> true },
            awaitSubAgentResult = null
        )

        val result = tools.invokeAgent("agent-worker", "Do background task", null)
        assertTrue(result.contains("Task submitted to agent 'agent-worker'"))
        assertTrue(result.contains("Response will arrive asynchronously"))
    }
}
