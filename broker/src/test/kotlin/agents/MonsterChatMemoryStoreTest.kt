package at.rocworks.agents

import dev.langchain4j.agent.tool.ToolExecutionRequest
import dev.langchain4j.data.message.AiMessage
import dev.langchain4j.data.message.ToolExecutionResultMessage
import dev.langchain4j.data.message.UserMessage
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class MonsterChatMemoryStoreTest {

    @Test
    fun testInMemoryPersistenceAndRestore() {
        val persistence = InMemoryChatMemoryPersistence()
        val store1 = MonsterChatMemoryStore(persistence)

        val toolReq = ToolExecutionRequest.builder().id("call_1").name("read").arguments("{}").build()
        val messages = listOf(
            UserMessage.from("Read value"),
            AiMessage.from(listOf(toolReq)),
            ToolExecutionResultMessage.from("call_1", "read", "42"),
            AiMessage.from("Value is 42")
        )

        // Save in store 1
        store1.updateMessages("agent-a:session-1", messages)

        // Create a completely new store instance backed by the same persistence
        val store2 = MonsterChatMemoryStore(persistence)
        val restored = store2.getMessages("agent-a:session-1")

        assertEquals(4, restored.size)
        assertTrue(restored[0] is UserMessage)
        assertEquals("Read value", (restored[0] as UserMessage).singleText())
        assertTrue(restored[1] is AiMessage)
        assertTrue(restored[2] is ToolExecutionResultMessage)
        assertEquals("42", (restored[2] as ToolExecutionResultMessage).text())
        assertTrue(restored[3] is AiMessage)
    }

    @Test
    fun testSessionPartitioning() {
        val persistence = InMemoryChatMemoryPersistence()
        val store = MonsterChatMemoryStore(persistence)

        store.updateMessages("agent-a:session-1", listOf(UserMessage.from("Session 1 Msg")))
        store.updateMessages("agent-a:session-2", listOf(UserMessage.from("Session 2 Msg")))

        val s1 = store.getMessages("agent-a:session-1")
        val s2 = store.getMessages("agent-a:session-2")

        assertEquals(1, s1.size)
        assertEquals("Session 1 Msg", (s1[0] as UserMessage).singleText())
        assertEquals(1, s2.size)
        assertEquals("Session 2 Msg", (s2[0] as UserMessage).singleText())
    }

    @Test
    fun testDeleteMessages() {
        val persistence = InMemoryChatMemoryPersistence()
        val store = MonsterChatMemoryStore(persistence)

        store.updateMessages("agent-a:session-1", listOf(UserMessage.from("Hello")))
        assertEquals(1, store.getMessages("agent-a:session-1").size)

        store.deleteMessages("agent-a:session-1")
        assertEquals(0, store.getMessages("agent-a:session-1").size)
    }
}
