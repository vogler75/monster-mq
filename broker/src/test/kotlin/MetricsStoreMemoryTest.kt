import at.rocworks.extensions.graphql.BrokerMetrics
import at.rocworks.extensions.graphql.SessionMetrics
import at.rocworks.stores.MetricKind
import at.rocworks.stores.memory.MetricsStoreMemory
import at.rocworks.stores.memory.MetricsStoreNone
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.time.Instant
import java.util.concurrent.TimeUnit

class MetricsStoreMemoryTest {
    private lateinit var vertx: Vertx

    @Before
    fun setUp() {
        vertx = Vertx.vertx()
    }

    @After
    fun tearDown() {
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS)
    }

    @Test
    fun storesLatestAndHistoryInTimestampOrder() {
        val store = MetricsStoreMemory("test", maxHistoryRows = 10)
        store.startStore(vertx).await()

        val t1 = Instant.parse("2026-05-03T10:00:00Z")
        val t2 = Instant.parse("2026-05-03T10:00:01Z")

        store.storeSessionMetrics(t1, "client-1", SessionMetrics(1.0, 2.0, t1.toString())).await()
        store.storeSessionMetrics(t2, "client-1", SessionMetrics(3.0, 4.0, t2.toString())).await()

        val latest = store.getSessionMetrics("client-1", null, null, null).await()
        assertEquals(3.0, latest.messagesIn, 0.0)
        assertEquals(t2.toString(), latest.timestamp)

        val history = store.getSessionMetricsHistory("client-1", null, null, null, 10).await()
        assertEquals(listOf(t2, t1), history.map { it.first })
        assertEquals(listOf(3.0, 1.0), history.map { it.second.messagesIn })
    }

    @Test
    fun replacesSameTimestampAndBoundsRows() {
        val store = MetricsStoreMemory("test", maxHistoryRows = 2)
        store.startStore(vertx).await()

        val t1 = Instant.parse("2026-05-03T10:00:00Z")
        val t2 = Instant.parse("2026-05-03T10:00:01Z")
        val t3 = Instant.parse("2026-05-03T10:00:02Z")

        store.storeMetrics(MetricKind.BROKER, t1, "local", JsonObject().put("messagesIn", 1.0).put("timestamp", t1.toString())).await()
        store.storeMetrics(MetricKind.BROKER, t1, "local", JsonObject().put("messagesIn", 9.0).put("timestamp", t1.toString())).await()
        store.storeMetrics(MetricKind.BROKER, t2, "local", JsonObject().put("messagesIn", 2.0).put("timestamp", t2.toString())).await()
        store.storeMetrics(MetricKind.BROKER, t3, "local", JsonObject().put("messagesIn", 3.0).put("timestamp", t3.toString())).await()

        val history = store.getMetricsHistory(MetricKind.BROKER, "local", null, null, null, 10).await()
        assertEquals(2, history.size)
        assertEquals(listOf(t3, t2), history.map { it.first })
        assertEquals(listOf(3.0, 2.0), history.map { it.second.getDouble("messagesIn") })
    }

    @Test
    fun purgesOlderSamples() {
        val store = MetricsStoreMemory("test", maxHistoryRows = 10)
        store.startStore(vertx).await()

        val old = Instant.parse("2026-05-03T10:00:00Z")
        val kept = Instant.parse("2026-05-03T10:10:00Z")
        val cutoff = Instant.parse("2026-05-03T10:05:00Z")

        store.storeBrokerMetrics(old, "local", brokerMetrics(1.0, old)).await()
        store.storeBrokerMetrics(kept, "local", brokerMetrics(2.0, kept)).await()

        val removed = store.purgeOldMetrics(cutoff).await()
        assertEquals(1L, removed)

        val history = store.getBrokerMetricsHistory("local", null, null, null, 10).await()
        assertEquals(1, history.size)
        assertEquals(kept, history.first().first)
        assertEquals(2.0, history.first().second.messagesIn, 0.0)
    }

    @Test
    fun noneStoreDiscardsHistory() {
        val store = MetricsStoreNone("none")
        store.startStore(vertx).await()

        val timestamp = Instant.parse("2026-05-03T10:00:00Z")
        store.storeBrokerMetrics(timestamp, "local", brokerMetrics(1.0, timestamp)).await()

        assertTrue(store.getBrokerMetricsHistory("local", null, null, null, 10).await().isEmpty())
        assertTrue(store.getLatestMetrics(MetricKind.BROKER, "local", null, null, null).await().isEmpty)
        assertEquals(0L, store.purgeOldMetrics(timestamp).await())
    }

    private fun brokerMetrics(messagesIn: Double, timestamp: Instant) = BrokerMetrics(
        messagesIn = messagesIn,
        messagesOut = 0.0,
        nodeSessionCount = 0,
        clusterSessionCount = 0,
        queuedMessagesCount = 0,
        subscriptionCount = 0,
        clientNodeMappingSize = 0,
        topicNodeMappingSize = 0,
        messageBusIn = 0.0,
        messageBusOut = 0.0,
        timestamp = timestamp.toString()
    )

    private fun <T> io.vertx.core.Future<T>.await(): T =
        toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS)
}
