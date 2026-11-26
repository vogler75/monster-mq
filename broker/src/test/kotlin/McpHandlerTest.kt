package at.rocworks.extensions

import at.rocworks.Utils
import at.rocworks.auth.UserManager
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.handlers.SessionHandler
import at.rocworks.stores.MessageStoreMemory
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.logging.Handler
import java.util.logging.Level
import java.util.logging.LogRecord

class McpHandlerTest {

    private lateinit var vertx: Vertx
    private lateinit var archiveHandler: ArchiveHandler
    private lateinit var retainedStore: MessageStoreMemory

    @Before
    fun setUp() {
        vertx = Vertx.vertx()
        archiveHandler = Mockito.mock(ArchiveHandler::class.java)
        Mockito.`when`(archiveHandler.getDeployedArchiveGroups()).thenReturn(emptyMap())
        retainedStore = MessageStoreMemory("mcp-test-retained")
    }

    @After
    fun tearDown() {
        try {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS)
        } catch (_: Exception) {
        }
    }

    private fun captureMcpHandlerLogs(block: () -> Unit): List<LogRecord> {
        val logger = Utils.getLogger(McpHandler::class.java)
        val previousLevel = logger.level
        val records = CopyOnWriteArrayList<LogRecord>()
        val handler = object : Handler() {
            override fun publish(record: LogRecord?) {
                if (record != null) {
                    records.add(record)
                }
            }

            override fun flush() {}
            override fun close() {}
        }
        handler.level = Level.ALL
        logger.level = Level.ALL
        logger.addHandler(handler)
        try {
            block()
        } finally {
            logger.removeHandler(handler)
            logger.level = previousLevel
        }
        return records
    }

    @Test
    fun `roots list_changed and other notifications return null without severe or warning logs`() {
        val mcpHandler = McpHandler(vertx, retainedStore, archiveHandler, null)

        val notifications = listOf(
            JsonObject()
                .put("jsonrpc", "2.0")
                .put("method", "notifications/roots/list_changed"),
            JsonObject()
                .put("jsonrpc", "2.0")
                .put("method", "notifications/roots/list_changed")
                .put("params", JsonObject().put("_meta", JsonObject())),
            JsonObject()
                .put("jsonrpc", "2.0")
                .put("method", "notifications/roots/list_changed")
                .putNull("params"),
            JsonObject()
                .put("jsonrpc", "2.0")
                .put("method", "notifications/roots/list_changed")
                .put("params", JsonArray()),
            JsonObject()
                .put("jsonrpc", "2.0")
                .put("method", "notifications/initialized"),
            JsonObject()
                .put("jsonrpc", "2.0")
                .put("method", "notifications/cancelled")
                .put("params", JsonObject().put("requestId", 42).put("reason", "user cancelled")),
            JsonObject()
                .put("jsonrpc", "2.0")
                .put("method", "notifications/progress")
                .put("params", JsonObject().put("progressToken", "tok-1").put("progress", 50)),
            JsonObject()
                .put("jsonrpc", "2.0")
                .put("method", "notifications/custom/future_event"),
            JsonObject()
                .put("jsonrpc", "2.0")
        )

        val records = captureMcpHandlerLogs {
            for (notification in notifications) {
                val result = mcpHandler.handleRequest(notification)
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(5, TimeUnit.SECONDS)
                assertNull("Expected null response for notification: $notification", result)
            }
        }

        val warningsOrErrors = records.filter { it.level.intValue() >= Level.WARNING.intValue() }
        assertTrue(
            "Expected no WARNING/SEVERE logs for notifications, but got: ${warningsOrErrors.map { "${it.level}: ${it.message}" }}",
            warningsOrErrors.isEmpty()
        )
    }

    @Test
    fun `McpServer returns 202 Accepted for roots list_changed notification over HTTP`() {
        val port = ServerSocket(0).use { it.localPort }
        val sessionHandler = Mockito.mock(SessionHandler::class.java)
        val userManager = Mockito.mock(UserManager::class.java)
        Mockito.`when`(userManager.isUserManagementEnabled()).thenReturn(false)

        val server = McpServer(
            host = "127.0.0.1",
            port = port,
            retainedStore = retainedStore,
            archiveHandler = archiveHandler,
            sessionHandler = sessionHandler,
            userManager = userManager,
            dataCatalogStore = null
        )
        vertx.deployVerticle(server).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS)

        val httpClient = HttpClient.newHttpClient()
        val mcpUri = URI.create("http://127.0.0.1:$port/mcp")

        val records = captureMcpHandlerLogs {
            val notificationRequest = HttpRequest.newBuilder(mcpUri)
                .header("Content-Type", "application/json")
                .POST(
                    HttpRequest.BodyPublishers.ofString(
                        JsonObject()
                            .put("jsonrpc", "2.0")
                            .put("method", "notifications/roots/list_changed")
                            .encode()
                    )
                )
                .build()

            val notificationResponse = httpClient.send(notificationRequest, HttpResponse.BodyHandlers.ofString())
            assertEquals(202, notificationResponse.statusCode())
            assertEquals("", notificationResponse.body())
        }

        val severeLogs = records.filter { it.level == Level.SEVERE }
        assertTrue("Expected no SEVERE logs, got: ${severeLogs.map { it.message }}", severeLogs.isEmpty())

        val pingRequest = HttpRequest.newBuilder(mcpUri)
            .header("Content-Type", "application/json")
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    JsonObject()
                        .put("jsonrpc", "2.0")
                        .put("id", 1)
                        .put("method", "ping")
                        .encode()
                )
            )
            .build()

        val pingResponse = httpClient.send(pingRequest, HttpResponse.BodyHandlers.ofString())
        assertEquals(200, pingResponse.statusCode())
        val pingJson = JsonObject(pingResponse.body())
        assertEquals("2.0", pingJson.getString("jsonrpc"))
        assertEquals(1, pingJson.getInteger("id"))
        assertNotNull(pingJson.getJsonObject("result"))
    }
}
