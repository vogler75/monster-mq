package at.rocworks

import at.rocworks.agents.ChatModelConfig
import at.rocworks.agents.LangChain4jFactory
import io.vertx.core.json.JsonObject
import org.junit.Assert.assertNotNull
import org.junit.Test

class LangChain4jFactoryTest {

    @Test
    fun testCreateClaudeChatModelWithZeroTemperature() {
        val config = ChatModelConfig(
            provider = "claude",
            model = "claude-sonnet-5",
            apiKey = "dummy-key",
            temperature = 0.0
        )
        val chatModel = LangChain4jFactory.createChatModel(config, JsonObject())
        assertNotNull(chatModel)
    }

    @Test
    fun testCreateClaudeChatModelWithThinking() {
        val config = ChatModelConfig(
            provider = "claude",
            model = "claude-3-7-sonnet",
            apiKey = "dummy-key",
            temperature = 0.7,
            enableThinking = true
        )
        val chatModel = LangChain4jFactory.createChatModel(config, JsonObject())
        assertNotNull(chatModel)
    }

    @Test
    fun testCreateOpenAiChatModelWithZeroTemperature() {
        val config = ChatModelConfig(
            provider = "openai",
            model = "o1-mini",
            apiKey = "dummy-key",
            temperature = 0.0
        )
        val chatModel = LangChain4jFactory.createChatModel(config, JsonObject())
        assertNotNull(chatModel)
    }

    @Test
    fun testCreateGeminiChatModelWithZeroTemperature() {
        val config = ChatModelConfig(
            provider = "gemini",
            model = "gemini-2.0-flash",
            apiKey = "dummy-key",
            temperature = 0.0
        )
        val chatModel = LangChain4jFactory.createChatModel(config, JsonObject())
        assertNotNull(chatModel)
    }

    @Test(expected = IllegalArgumentException::class)
    fun testMissingModelThrowsException() {
        val config = ChatModelConfig(
            provider = "gemini",
            model = null,
            apiKey = "dummy-key"
        )
        LangChain4jFactory.createChatModel(config, JsonObject())
    }

    @Test
    fun testGlobalConfigDefaultModelUsed() {
        val config = ChatModelConfig(
            provider = "gemini",
            model = null,
            apiKey = "dummy-key"
        )
        val globalConfig = JsonObject()
            .put("GenAI", JsonObject()
                .put("Providers", JsonObject()
                    .put("Gemini", JsonObject()
                        .put("Model", "gemini-2.5-pro"))))
        val chatModel = LangChain4jFactory.createChatModel(config, globalConfig)
        assertNotNull(chatModel)
    }
}
