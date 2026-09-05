package at.rocworks.extensions

import at.rocworks.auth.UserManager
import at.rocworks.bus.IMessageBus
import at.rocworks.data.TopicTree
import at.rocworks.schema.*
import io.vertx.core.Context
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicInteger
import at.rocworks.data.BrokerMessage
import at.rocworks.handlers.*
import at.rocworks.stores.*
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.junit.Assert.*
import org.junit.Test
import java.lang.reflect.Proxy
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.TimeUnit

class I3xCatalogHttpTest {
    @Test fun noCatalogRetainsTopicReads() = checkReads(null)
    @Test fun emptyCatalogRetainsTopicReads() = checkReads(emptyList())
    @Test fun catalogIdsReadMappedValuesAlongsideRawTopics() = checkReads(listOf(
        DataCatalogInstance("pump", "pump-type", "Pump", "plant/one", JsonObject())))

    private fun checkReads(instances: List<DataCatalogInstance>?) {
        val vertx = Vertx.vertx()
        val cacheField = TopicSchemaPolicyCache::class.java.getDeclaredField("instance").apply { isAccessible = true }
        val previousCache = cacheField.get(null)
        try {
            val cache = TopicSchemaPolicyCache(vertx, unused(IDeviceConfigStore::class.java))
            val tree = TopicTree<String, CompiledNamespaceEntry>()
            tree.add("plant/#", "plant", CompiledNamespaceEntry("plant", "plant/#", "object-policy",
                JsonSchemaValidator(JsonObject().put("type", "object")), "REJECT"))
            val treeField = TopicSchemaPolicyCache::class.java.getDeclaredField("namespaceTree").apply { isAccessible = true }
            @Suppress("UNCHECKED_CAST")
            (treeField.get(cache) as AtomicReference<TopicTree<String, CompiledNamespaceEntry>>).set(tree)
            TopicSchemaPolicyCache.setInstance(cache)
            val catalogReads = AtomicInteger()

            val config = JsonObject().put("UserManagement", JsonObject().put("Enabled", false).put("StoreType", "SQLITE"))
            val archive = ArchiveHandler(vertx, config, null, null)
            val values = MessageStoreMemory("catalog-test")
            values.addAll(listOf(BrokerMessage(messageId = 0, topicName = "plant/one",
                payload = "42".toByteArray(), qosLevel = 0, isRetain = true, isDup = false, isQueued = false, clientId = "test")))
            val group = ArchiveGroup("Default", listOf("#"), false, MessageStoreType.MEMORY, MessageArchiveType.NONE, databaseConfig = JsonObject())
            ArchiveGroup::class.java.getDeclaredField("lastValStore").apply { isAccessible = true }.set(group, values)
            val field = ArchiveHandler::class.java.getDeclaredField("deployedArchiveGroups").apply { isAccessible = true }
            @Suppress("UNCHECKED_CAST")
            val groups = field.get(archive) as MutableMap<String, ArchiveGroupInfo>
            groups["Default"] = ArchiveGroupInfo(group, "test", true)
            val sessions = SessionHandler(unused(ISessionStoreAsync::class.java), unused(IQueueStoreAsync::class.java),
                unused(IMessageBus::class.java), MessageHandler(values, emptyList()), false)
            val catalog = if (instances == null) null else Proxy.newProxyInstance(IDataCatalogStore::class.java.classLoader,
                arrayOf(IDataCatalogStore::class.java)) { _, method, _ ->
                if (method.name == "getInstances") {
                    assertFalse("Catalog read must not execute on the event loop", Context.isOnEventLoopThread())
                    catalogReads.incrementAndGet()
                    Future.succeededFuture(instances)
                }
                else throw UnsupportedOperationException(method.name)
            } as IDataCatalogStore
            val port = ServerSocket(0).use { it.localPort }
            vertx.deployVerticle(I3xServer("127.0.0.1", port, "test", archive, sessions, null, UserManager(config), catalog))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS)
            val ids = JsonArray().add("plant/one").add("pump").add("missing")
            val request = HttpRequest.newBuilder(URI("http://127.0.0.1:$port/i3x/v1/objects/value"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JsonObject().put("elementIds", ids).encode())).build()
            val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())
            assertEquals(200, response.statusCode())
            val results = JsonObject(response.body()).getJsonArray("results")
            assertTrue(results.getJsonObject(0).getBoolean("success"))
            assertEquals("42", results.getJsonObject(0).getJsonObject("result").getValue("value"))
            assertEquals("Uncertain", results.getJsonObject(0).getJsonObject("result").getString("quality"))
            assertEquals(!instances.isNullOrEmpty(), results.getJsonObject(1).getBoolean("success"))
            if (!instances.isNullOrEmpty()) {
                assertEquals("pump", results.getJsonObject(1).getString("elementId"))
                assertEquals("Uncertain", results.getJsonObject(1).getJsonObject("result").getString("quality"))
                assertEquals(results.getJsonObject(0).getJsonObject("result").getValue("value"),
                    results.getJsonObject(1).getJsonObject("result").getValue("value"))
            }
            assertFalse(results.getJsonObject(2).getBoolean("success"))
            val readsBeforePolling = catalogReads.get()
            val poll = HttpRequest.newBuilder(URI("http://127.0.0.1:$port/i3x/v1/subscriptions/list?clientId=test"))
                .GET().build()
            assertEquals(200, HttpClient.newHttpClient().send(poll, HttpResponse.BodyHandlers.ofString()).statusCode())
            assertEquals("Subscription listing must not read the catalog", readsBeforePolling, catalogReads.get())
        } finally {
            cacheField.set(null, previousCache)
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS)
        }
    }

    private fun <T> unused(type: Class<T>): T = type.cast(Proxy.newProxyInstance(type.classLoader, arrayOf(type)) { _, method, _ ->
        throw UnsupportedOperationException("Unexpected dependency call: ${method.name}")
    })
}
