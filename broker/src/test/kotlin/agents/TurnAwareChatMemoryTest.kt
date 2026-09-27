package at.rocworks.agents

import dev.langchain4j.agent.tool.ToolExecutionRequest
import dev.langchain4j.data.message.AiMessage
import dev.langchain4j.data.message.SystemMessage
import dev.langchain4j.data.message.ToolExecutionResultMessage
import dev.langchain4j.data.message.UserMessage
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class TurnAwareChatMemoryTest {

    @Test
    fun testEvictionDropsCompleteTurn() {
        val memory = TurnAwareChatMemory("test", maxMessages = 4)

        // Turn 1: simple turn (2 messages)
        memory.add(UserMessage.from("Hello"))
        memory.add(AiMessage.from("Hi there!"))
        assertEquals(2, memory.messages().size)

        // Turn 2: tool-calling turn (4 messages)
        // User -> Ai (calls tool) -> ToolResult -> Ai (final response)
        val toolReq = ToolExecutionRequest.builder().id("call_1").name("calc").arguments("{}").build()
        memory.add(UserMessage.from("Compute 2+2"))
        memory.add(AiMessage.from(listOf(toolReq)))
        memory.add(ToolExecutionResultMessage.from("call_1", "calc", "4"))
        memory.add(AiMessage.from("The result is 4."))

        // Total was 2 + 4 = 6 messages. Max messages is 4.
        // It must have evicted Turn 1 entirely (2 messages), leaving Turn 2 intact (4 messages).
        val msgs = memory.messages()
        assertEquals(4, msgs.size)
        assertTrue(msgs[0] is UserMessage)
        assertEquals("Compute 2+2", (msgs[0] as UserMessage).singleText())
        assertTrue(msgs[1] is AiMessage)
        assertTrue(msgs[2] is ToolExecutionResultMessage)
        assertTrue(msgs[3] is AiMessage)
    }

    @Test
    fun testNeverSplitsToolCallAndResult() {
        // Even if maxMessages is set to 2, Turn 2 has 4 messages and should not be sliced in half
        val memory = TurnAwareChatMemory("test", maxMessages = 2)

        val toolReq = ToolExecutionRequest.builder().id("call_1").name("getVal").arguments("{}").build()
        memory.add(UserMessage.from("What is the temperature?"))
        memory.add(AiMessage.from(listOf(toolReq)))
        memory.add(ToolExecutionResultMessage.from("call_1", "getVal", "23.5 C"))
        memory.add(AiMessage.from("Temperature is 23.5 C."))

        val msgs = memory.messages()
        // It keeps the full turn intact (4 messages) rather than leaving an orphaned tool call
        assertEquals(4, msgs.size)
        assertTrue((msgs[1] as AiMessage).hasToolExecutionRequests())
        assertEquals("call_1", (msgs[2] as ToolExecutionResultMessage).id())
    }

    @Test
    fun testSanitizeRemovesOrphanedToolResults() {
        // Simulate a corrupted or legacy history where a ToolExecutionResultMessage is present without its parent AiMessage
        val orphanResult = ToolExecutionResultMessage.from("orphan_1", "foo", "bar")
        val userMsg = UserMessage.from("Hello")
        val aiMsg = AiMessage.from("Hi")

        val sanitized = TurnAwareChatMemory.sanitize(listOf(orphanResult, userMsg, aiMsg))
        assertEquals(2, sanitized.size)
        assertEquals("Hello", (sanitized[0] as UserMessage).singleText())
        assertEquals("Hi", (sanitized[1] as AiMessage).text())
    }

    @Test
    fun testSanitizeRemovesUnansweredTrailingToolCall() {
        val toolReq = ToolExecutionRequest.builder().id("call_unanswered").name("foo").arguments("{}").build()
        val userMsg1 = UserMessage.from("Do something")
        val aiMsgWithTool = AiMessage.from(listOf(toolReq))
        val userMsg2 = UserMessage.from("Do something else")

        val sanitized = TurnAwareChatMemory.sanitize(listOf(userMsg1, aiMsgWithTool, userMsg2))
        // Abandoned tool call before new UserMessage must be stripped to prevent Gemini turn-ordering errors
        assertEquals(2, sanitized.size)
        assertEquals("Do something", (sanitized[0] as UserMessage).singleText())
        assertEquals("Do something else", (sanitized[1] as UserMessage).singleText())
    }

    @Test
    fun testSerializationRoundTrip() {
        val toolReq = ToolExecutionRequest.builder().id("call_123").name("get_temp").arguments("{\"sensor\":\"A\"}").build()
        val original = listOf(
            UserMessage.from("Check temperature"),
            AiMessage.from(listOf(toolReq)),
            ToolExecutionResultMessage.from("call_123", "get_temp", "24.5"),
            AiMessage.from("Temperature is 24.5")
        )

        val json = dev.langchain4j.data.message.ChatMessageSerializer.messagesToJson(original)
        val deserialized = dev.langchain4j.data.message.ChatMessageDeserializer.messagesFromJson(json)

        assertEquals(4, deserialized.size)
        assertTrue(deserialized[0] is UserMessage)
        assertTrue(deserialized[1] is AiMessage)
        assertEquals(1, (deserialized[1] as AiMessage).toolExecutionRequests().size)
        assertTrue(deserialized[2] is ToolExecutionResultMessage)
        assertEquals("24.5", (deserialized[2] as ToolExecutionResultMessage).text())
        assertTrue(deserialized[3] is AiMessage)
    }

    @Test
    fun testSystemMessageIsKeptOnceAndNeverEvicted() {
        val memory = TurnAwareChatMemory("test", maxMessages = 3)
        memory.add(SystemMessage.from("You are a helper"))
        memory.add(UserMessage.from("Q1"))
        memory.add(AiMessage.from("A1"))
        // AiServices re-adds the system message on every call
        memory.add(SystemMessage.from("You are a helper"))
        memory.add(UserMessage.from("Q2"))
        memory.add(AiMessage.from("A2"))

        val msgs = memory.messages()
        // Budget 3 = system + one 2-message turn: turn 1 evicted, system message retained exactly once
        assertEquals(3, msgs.size)
        assertEquals("You are a helper", (msgs[0] as SystemMessage).text())
        assertEquals("Q2", (msgs[1] as UserMessage).singleText())
        assertEquals(1, msgs.count { it is SystemMessage })
    }

    @Test
    fun testChangedSystemMessageReplacesOldOne() {
        val memory = TurnAwareChatMemory("test", maxMessages = 10)
        memory.add(SystemMessage.from("old prompt"))
        memory.add(UserMessage.from("Q1"))
        memory.add(AiMessage.from("A1"))
        memory.add(SystemMessage.from("new prompt"))

        val msgs = memory.messages()
        assertEquals(3, msgs.size)
        assertEquals("new prompt", (msgs[0] as SystemMessage).text())
        assertTrue(msgs[1] is UserMessage)
    }
}
