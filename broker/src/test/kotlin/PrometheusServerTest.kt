package at.rocworks.extensions

import at.rocworks.Const
import at.rocworks.auth.UserManager
import at.rocworks.data.BrokerMessage
import at.rocworks.data.User
import at.rocworks.extensions.graphql.BrokerMetrics
import at.rocworks.handlers.ArchiveGroup
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.stores.*
import io.vertx.core.Future
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
import java.time.Instant
import java.util.Base64
import java.util.concurrent.TimeUnit

class PrometheusServerTest {

    private lateinit var vertx: Vertx
    private var port: Int = 0
    private lateinit var userManager: UserManager
    private lateinit var archiveHandler: ArchiveHandler
    private var server: PrometheusServer? = null
    private val client = HttpClient.newHttpClient()

    private val basicAlice = "Basic " + Base64.getEncoder().encodeToString("alice:secret".toByteArray())
    private val basicAdmin = "Basic " + Base64.getEncoder().encodeToString("admin:secret".toByteArray())

    @Before
    fun setUp() {
        vertx = Vertx.vertx()
        port = ServerSocket(0).use { it.localPort }
        archiveHandler = Mockito.mock(ArchiveHandler::class.java)

        userManager = Mockito.mock(UserManager::class.java) { inv ->
            when (inv.method.name) {
                "isUserManagementEnabled" -> true
                "isAnonymousEnabled" -> true
                "isAdmin" -> inv.getArgument<String>(0) == "admin"
                "authenticate" -> {
                    val u = inv.getArgument<String>(0)
                    Future.succeededFuture(User(u, "secret", enabled = true))
                }
                "canSubscribe" -> {
                    val user = inv.getArgument<String>(0)
                    val topic = inv.getArgument<String>(1)
                    if (user == "admin") true
                    else if (topic.startsWith("allowed/secret")) false
                    else if (topic.startsWith("allowed")) true
                    else false
                }
                else -> Mockito.RETURNS_DEFAULTS.answer(inv)
            }
        }
    }

    @After
    fun tearDown() {
        try {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS)
        } catch (_: Exception) {}
    }

    private fun startServer(metricsStore: IMetricsStore? = null, metricsIntervalSeconds: Int = 10, rawQueryLimit: Int = 10000) {
        val s = PrometheusServer(
            host = "127.0.0.1",
            port = port,
            archiveHandler = archiveHandler,
            userManager = userManager,
            metricsStore = metricsStore,
            rawQueryLimit = rawQueryLimit,
            metricsIntervalSeconds = metricsIntervalSeconds
        )
        server = s
        vertx.deployVerticle(s).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS)
    }

    private fun mockArchiveWithTopics() {
        val store = MessageStoreMemory("test-lastval")
        store.addAll(listOf(
            BrokerMessage(messageId = 0, topicName = "allowed/a", payload = "{\"temp\": 25.5}".toByteArray(),
                qosLevel = 0, isRetain = false, isDup = false, isQueued = false, clientId = "c1"),
            BrokerMessage(messageId = 0, topicName = "allowed/secret/b", payload = "{\"secret_code\": 999}".toByteArray(),
                qosLevel = 0, isRetain = false, isDup = false, isQueued = false, clientId = "c2")
        ))

        val historyRecords = JsonArray()
            .add(JsonObject().put("topic", "allowed/a").put("timestamp", System.currentTimeMillis()).put("payload", "{\"temp\": 25.5}"))
            .add(JsonObject().put("topic", "allowed/secret/b").put("timestamp", System.currentTimeMillis()).put("payload", "{\"secret_code\": 999}"))

        val archiveMock = Mockito.mock(IMessageArchiveExtended::class.java) { inv ->
            when (inv.method.name) {
                "getHistory" -> historyRecords
                "getAggregatedHistory" -> JsonObject().put("columns", JsonArray().add("timestamp").add("allowed/a.temp_avg")).put("rows", JsonArray())
                else -> Mockito.RETURNS_DEFAULTS.answer(inv)
            }
        }

        val group = ArchiveGroup("Default", listOf("#"), false, MessageStoreType.MEMORY, MessageArchiveType.NONE, databaseConfig = JsonObject())
        ArchiveGroup::class.java.getDeclaredField("lastValStore").apply { isAccessible = true }.set(group, store)
        ArchiveGroup::class.java.getDeclaredField("archiveStore").apply { isAccessible = true }.set(group, archiveMock)

        Mockito.`when`(archiveHandler.getDeployedArchiveGroups()).thenReturn(mapOf("Default" to group))
    }

    private fun uri(path: String, query: String? = null): URI {
        return URI("http", null, "127.0.0.1", port, path, query, null)
    }

    // F1: avg_over_time, min_over_time, max_over_time, count_over_time parse and return 200 without crashing
    @Test
    fun testFuncPatternOverTimeFunctions() {
        mockArchiveWithTopics()
        startServer()

        val funcs = listOf("avg_over_time", "min_over_time", "max_over_time", "count_over_time")
        for (fn in funcs) {
            val req = HttpRequest.newBuilder()
                .uri(uri("/api/v1/query_range", "query=$fn(topics{group=\"Default\",topic=\"allowed/a\"}[5m])&start=0&end=100&step=60"))
                .header("Authorization", basicAdmin)
                .GET()
                .build()
            val resp = client.send(req, HttpResponse.BodyHandlers.ofString())
            assertEquals("Expected 200 for $fn, got ${resp.statusCode()}: ${resp.body()}", 200, resp.statusCode())
            val json = JsonObject(resp.body())
            assertEquals("success", json.getString("status"))
        }
    }

    // F2: Broker metrics windowing and limit
    @Test
    fun testBrokerMetricsHistoryWindowAndLimit() {
        var capturedLimit = 0
        val metricsStore = Mockito.mock(IMetricsStore::class.java) { inv ->
            when (inv.method.name) {
                "getBrokerMetricsHistory" -> {
                    capturedLimit = inv.getArgument(4)
                    val s = inv.getArgument<Instant?>(1) ?: Instant.now()
                    val points: List<Pair<Instant, BrokerMetrics>> = (0 until 500).map { i ->
                        s.plusSeconds((i * 10).toLong()) to BrokerMetrics(
                            messagesIn = 0.0, messagesOut = 0.0, nodeSessionCount = 10, clusterSessionCount = 10,
                            queuedMessagesCount = 0L, subscriptionCount = 0, clientNodeMappingSize = 0, topicNodeMappingSize = 0,
                            messageBusIn = 0.0, messageBusOut = 0.0, timestamp = s.toString()
                        )
                    }
                    Future.succeededFuture(points)
                }
                else -> Mockito.RETURNS_DEFAULTS.answer(inv)
            }
        }

        startServer(metricsStore = metricsStore, metricsIntervalSeconds = 10)
        val now = Instant.now()
        val start = now.minusSeconds(5000)

        val req = HttpRequest.newBuilder()
            .uri(uri("/api/v1/query_range", "query=metrics{metric=\"sessions\"}&start=${start.epochSecond}&end=${now.epochSecond}&step=10"))
            .header("Authorization", basicAdmin)
            .GET()
            .build()
        val resp = client.send(req, HttpResponse.BodyHandlers.ofString())
        assertEquals(200, resp.statusCode())
        // limit = (5000/10) + 2 = 502
        assertEquals(502, capturedLimit)
    }

    // F3: GET and POST on /api/v1/labels and /api/v1/series
    @Test
    fun testHttpMethodsForLabelsAndSeries() {
        mockArchiveWithTopics()
        startServer()

        // Labels GET & POST
        val labelsGet = client.send(HttpRequest.newBuilder().uri(uri("/api/v1/labels")).header("Authorization", basicAdmin).GET().build(), HttpResponse.BodyHandlers.ofString())
        val labelsPost = client.send(HttpRequest.newBuilder().uri(uri("/api/v1/labels")).header("Authorization", basicAdmin).POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString())
        assertEquals(200, labelsGet.statusCode())
        assertEquals(200, labelsPost.statusCode())
        assertEquals(labelsGet.body(), labelsPost.body())

        // Series GET & POST
        val seriesGet = client.send(HttpRequest.newBuilder().uri(uri("/api/v1/series", "match[]=topics{topic=\"allowed/a\"}")).header("Authorization", basicAdmin).GET().build(), HttpResponse.BodyHandlers.ofString())
        val seriesPost = client.send(HttpRequest.newBuilder().uri(uri("/api/v1/series", "match[]=topics{topic=\"allowed/a\"}")).header("Authorization", basicAdmin).POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString())
        assertEquals(200, seriesGet.statusCode())
        assertEquals(200, seriesPost.statusCode())
        assertEquals(seriesGet.body(), seriesPost.body())
    }

    // F4: match[]=topics{...} returns only topic series, not metrics
    @Test
    fun testSeriesMetricSelectorFiltersMetrics() {
        mockArchiveWithTopics()
        startServer()

        val req = HttpRequest.newBuilder()
            .uri(uri("/api/v1/series", "match[]=topics{topic=\"allowed/a\"}"))
            .header("Authorization", basicAdmin)
            .GET()
            .build()
        val resp = client.send(req, HttpResponse.BodyHandlers.ofString())
        assertEquals(200, resp.statusCode())
        val data = JsonObject(resp.body()).getJsonArray("data")
        assertTrue(data.size() > 0)
        for (i in 0 until data.size()) {
            val item = data.getJsonObject(i)
            assertEquals("topics", item.getString("__name__"))
            assertFalse(item.containsKey("metric"))
        }
    }

    // F7: ACL filtering for user alice vs admin
    @Test
    fun testReadAclFiltering() {
        mockArchiveWithTopics()
        startServer()

        // 1. /metrics scrape
        val scrapeAlice = client.send(HttpRequest.newBuilder().uri(uri("/metrics")).header("Authorization", basicAlice).GET().build(), HttpResponse.BodyHandlers.ofString())
        assertEquals("Status: ${scrapeAlice.statusCode()}, Body: ${scrapeAlice.body()}", 200, scrapeAlice.statusCode())
        assertTrue("Scrape Alice body: ${scrapeAlice.body()}", scrapeAlice.body().contains("allowed/a"))
        assertFalse(scrapeAlice.body().contains("allowed/secret/b"))

        val scrapeAdmin = client.send(HttpRequest.newBuilder().uri(uri("/metrics")).header("Authorization", basicAdmin).GET().build(), HttpResponse.BodyHandlers.ofString())
        assertEquals(200, scrapeAdmin.statusCode())
        assertTrue(scrapeAdmin.body().contains("allowed/a"))
        assertTrue(scrapeAdmin.body().contains("allowed/secret/b"))

        // 2. /api/v1/label/topic/values
        val topicsAlice = client.send(HttpRequest.newBuilder().uri(uri("/api/v1/label/topic/values")).header("Authorization", basicAlice).GET().build(), HttpResponse.BodyHandlers.ofString())
        assertEquals(200, topicsAlice.statusCode())
        val topicsAliceList = JsonObject(topicsAlice.body()).getJsonArray("data").list
        assertTrue(topicsAliceList.contains("allowed/a"))
        assertFalse(topicsAliceList.contains("allowed/secret/b"))

        val topicsAdmin = client.send(HttpRequest.newBuilder().uri(uri("/api/v1/label/topic/values")).header("Authorization", basicAdmin).GET().build(), HttpResponse.BodyHandlers.ofString())
        assertEquals(200, topicsAdmin.statusCode())
        val topicsAdminList = JsonObject(topicsAdmin.body()).getJsonArray("data").list
        assertTrue(topicsAdminList.contains("allowed/a"))
        assertTrue(topicsAdminList.contains("allowed/secret/b"))

        // 3. /api/v1/label/field/values
        val fieldsAlice = client.send(HttpRequest.newBuilder().uri(uri("/api/v1/label/field/values")).header("Authorization", basicAlice).GET().build(), HttpResponse.BodyHandlers.ofString())
        assertEquals(200, fieldsAlice.statusCode())
        val fieldsAliceList = JsonObject(fieldsAlice.body()).getJsonArray("data").list
        assertTrue(fieldsAliceList.contains("temp"))
        assertFalse(fieldsAliceList.contains("secret_code"))

        // 4. /api/v1/series
        val seriesAlice = client.send(HttpRequest.newBuilder().uri(uri("/api/v1/series", "match[]=topics{}")).header("Authorization", basicAlice).GET().build(), HttpResponse.BodyHandlers.ofString())
        assertEquals(200, seriesAlice.statusCode())
        assertFalse(seriesAlice.body().contains("allowed/secret/b"))
        assertTrue(seriesAlice.body().contains("allowed/a"))

        // 5. /api/v1/query
        val queryDenied = client.send(HttpRequest.newBuilder().uri(uri("/api/v1/query", "query=topics{topic=\"allowed/secret/b\"}")).header("Authorization", basicAlice).GET().build(), HttpResponse.BodyHandlers.ofString())
        assertEquals(200, queryDenied.statusCode())
        assertEquals(0, JsonObject(queryDenied.body()).getJsonObject("data").getJsonArray("result").size())

        val queryAllowed = client.send(HttpRequest.newBuilder().uri(uri("/api/v1/query", "query=topics{topic=\"allowed/a\",field=\"temp\"}")).header("Authorization", basicAlice).GET().build(), HttpResponse.BodyHandlers.ofString())
        assertEquals(200, queryAllowed.statusCode())
        assertEquals(1, JsonObject(queryAllowed.body()).getJsonObject("data").getJsonArray("result").size())

        // 6. /api/v1/query_range
        val rangeDenied = client.send(HttpRequest.newBuilder().uri(uri("/api/v1/query_range", "query=topics{topic=\"allowed/secret/b\"}&start=0&end=100&step=10")).header("Authorization", basicAlice).GET().build(), HttpResponse.BodyHandlers.ofString())
        assertEquals(200, rangeDenied.statusCode())
        assertEquals(0, JsonObject(rangeDenied.body()).getJsonObject("data").getJsonArray("result").size())

        val rangeAllowed = client.send(HttpRequest.newBuilder().uri(uri("/api/v1/query_range", "query=topics{topic=\"allowed/a\",field=\"temp\"}&start=0&end=100&step=10")).header("Authorization", basicAlice).GET().build(), HttpResponse.BodyHandlers.ofString())
        assertEquals(200, rangeAllowed.statusCode())
        val rangeRes = JsonObject(rangeAllowed.body()).getJsonObject("data").getJsonArray("result")
        assertEquals(1, rangeRes.size())
    }
}
