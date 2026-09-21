package at.rocworks

import at.rocworks.genai.decision.OpenRouterDecisionProvider
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class OpenRouterDecisionProviderTest {

    private lateinit var vertx: Vertx

    @Before
    fun setUp() {
        vertx = Vertx.vertx()
    }

    @After
    fun tearDown() {
        val latch = CountDownLatch(1)
        vertx.close().onComplete { latch.countDown() }
        latch.await(5, TimeUnit.SECONDS)
    }

    @Test
    fun testResolveDecisionsUri() {
        val provider = OpenRouterDecisionProvider(vertx, "dummy-key")
        try {
            assertEquals("https://openrouter.ai/api/alpha/decisions", provider.resolveDecisionsUri(null).toString())
            assertEquals("https://openrouter.ai/api/alpha/decisions", provider.resolveDecisionsUri("").toString())
            assertEquals("https://openrouter.ai/api/alpha/decisions", provider.resolveDecisionsUri("https://openrouter.ai/api/v1").toString())
            assertEquals("https://openrouter.ai/api/alpha/decisions", provider.resolveDecisionsUri("https://openrouter.ai/api/v1/").toString())
            assertEquals("https://openrouter.ai/api/alpha/decisions", provider.resolveDecisionsUri("https://openrouter.ai/api/alpha/decisions").toString())
            assertEquals("http://localhost:8080/api/alpha/decisions", provider.resolveDecisionsUri("http://localhost:8080/api").toString())
            assertEquals("http://localhost:8080/custom/api/alpha/decisions", provider.resolveDecisionsUri("http://localhost:8080/custom").toString())
        } finally {
            provider.close()
        }
    }



    @Test
    fun testDecideWithMockServer() {
        val latch = CountDownLatch(1)
        var receivedAuthHeader: String? = null
        var receivedReferer: String? = null
        var receivedTitle: String? = null
        var receivedBody: JsonObject? = null

        val server = vertx.createHttpServer()
        server.requestHandler { req ->
            receivedAuthHeader = req.getHeader("Authorization")
            receivedReferer = req.getHeader("HTTP-Referer")
            receivedTitle = req.getHeader("X-Title")
            req.bodyHandler { buf ->
                receivedBody = buf.toJsonObject()
                val responseJson = JsonObject()
                    .put("model", "typesafe/jev-1.13")
                    .put("decisions", JsonArray().add(JsonObject().put("question", "Pump active?").put("answer", true)))
                req.response()
                    .setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .end(responseJson.encode())
            }
        }

        val serverLatch = CountDownLatch(1)
        var port = 0
        server.listen(0).onComplete { ar ->
            if (ar.succeeded()) {
                port = ar.result().actualPort()
            }
            serverLatch.countDown()
        }
        assertTrue("Server failed to start", serverLatch.await(5, TimeUnit.SECONDS))

        val provider = OpenRouterDecisionProvider(vertx, "test-api-key", "http://localhost:$port/api/alpha/decisions")
        try {
            val questions = JsonObject().put("q1", JsonObject().put("question", "Pump active?").put("type", "boolean"))
            val state = JsonObject().put("temperature", 85.5)

            val future = provider.decide("typesafe/jev-1.13", questions, state, 10)
            val result = future.get(5, TimeUnit.SECONDS)

            assertNotNull(result)
            assertEquals("typesafe/jev-1.13", result.getString("model"))
            assertTrue(result.getJsonArray("decisions").getJsonObject(0).getBoolean("answer"))

            assertEquals("Bearer test-api-key", receivedAuthHeader)
            assertEquals("https://github.com/vogler75/monster-mq", receivedReferer)
            assertEquals("MonsterMQ", receivedTitle)
            assertNotNull(receivedBody)
            assertEquals("typesafe/jev-1.13", receivedBody?.getString("model"))
            assertEquals(85.5, receivedBody?.getJsonObject("state")?.getDouble("temperature") ?: 0.0, 0.001)
        } finally {
            provider.close()
            server.close()
        }
    }

    @Test
    fun testExtractDecisionQuestionsFromDictionary() {
        val prompt = """
        {
          "is_bug": {
            "type": "noul",
            "instructions": "Is this a bug?",
            "criteria": { "true": "Defect", "false": "Feature" }
          },
          "urgency": {
            "type": "score",
            "instructions": "How urgent?",
            "criteria": ["Low", "Medium", "High"]
          }
        }
        """.trimIndent()
        val deviceConfig = at.rocworks.stores.DeviceConfig(
            name = "test-agent",
            namespace = "default",
            nodeId = "*",
            config = JsonObject().put("systemPrompt", prompt).put("model", "typesafe/jev-1.13")
        )
        val executor = at.rocworks.agents.AgentExecutor(deviceConfig)
        val questions = executor.extractDecisionQuestions()
        assertNotNull(questions)
        assertEquals("noul", questions.getJsonObject("is_bug")?.getString("type"))
        assertEquals("score", questions.getJsonObject("urgency")?.getString("type"))
    }

    @Test
    fun testExtractDecisionQuestionsFromArray() {
        val prompt = """
        [
          {
            "id": "temp_critical",
            "type": "boolean",
            "question": "Is temperature critical?",
            "options": ["Normal", "Critical"]
          },
          {
            "id": "action",
            "type": "choice",
            "instructions": "Cooling action",
            "options": ["none", "fan", "shutdown"]
          }
        ]
        """.trimIndent()
        val deviceConfig = at.rocworks.stores.DeviceConfig(
            name = "test-agent-array",
            namespace = "default",
            nodeId = "*",
            config = JsonObject().put("systemPrompt", prompt).put("model", "typesafe/jev-1.13")
        )
        val executor = at.rocworks.agents.AgentExecutor(deviceConfig)
        val questions = executor.extractDecisionQuestions()
        assertNotNull(questions)
        assertEquals("noul", questions.getJsonObject("temp_critical")?.getString("type"))
        assertEquals("Is temperature critical?", questions.getJsonObject("temp_critical")?.getString("instructions"))
        assertEquals("choice", questions.getJsonObject("action")?.getString("type"))
        assertNotNull(questions.getJsonObject("action")?.getJsonObject("criteria"))
    }

    @Test
    fun testIsDecisionAgentOnlyChecksProviderType() {
        // Agent with openrouter-decision provider -> decision agent
        val decisionAgentConfig = at.rocworks.stores.DeviceConfig(
            name = "decision-agent",
            namespace = "default",
            nodeId = "*",
            config = JsonObject().put("provider", "openrouter-decision").put("model", "typesafe/jev-1.13")
        )
        val decisionExecutor = at.rocworks.agents.AgentExecutor(decisionAgentConfig)
        assertTrue(decisionExecutor.isDecisionAgent())

        // Agent with chat openrouter provider, even with jev model or decision tags -> NOT a decision agent!
        val chatAgentConfig = at.rocworks.stores.DeviceConfig(
            name = "chat-agent",
            namespace = "default",
            nodeId = "*",
            config = JsonObject()
                .put("provider", "openrouter")
                .put("model", "typesafe/jev-1.13")
                .put("tags", JsonArray().add("decision"))
        )
        val chatExecutor = at.rocworks.agents.AgentExecutor(chatAgentConfig)
        org.junit.Assert.assertFalse(chatExecutor.isDecisionAgent())

        // ProviderConfig with type "openrouter-decision"
        val provConfig = at.rocworks.agents.GenAiProviderConfig(type = "openrouter-decision")
        assertTrue(chatExecutor.isDecisionAgent(provConfig))

        // ProviderConfig with type "openrouter"
        val chatProvConfig = at.rocworks.agents.GenAiProviderConfig(type = "openrouter")
        org.junit.Assert.assertFalse(chatExecutor.isDecisionAgent(chatProvConfig))
    }
}

