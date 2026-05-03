package at.rocworks

import at.rocworks.data.BrokerMessage
import at.rocworks.stores.QueueStoreType
import at.rocworks.stores.SessionStoreType
import at.rocworks.stores.sqlite.QueueStoreSQLiteV2
import at.rocworks.stores.sqlite.SQLiteDatabasePath
import at.rocworks.stores.sqlite.SQLiteVerticle
import at.rocworks.stores.sqlite.SessionStoreSQLite
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class MemoryStoreConfigTest {

    @Test
    fun queueStoreFallsBackToDefaultStoreType() {
        val config = JsonObject()
            .put("DefaultStoreType", "POSTGRES")
            .put("SessionStoreType", "MEMORY")

        assertEquals("POSTGRES", Monster.getQueueStoreType(config))
    }

    @Test
    fun queueStorePreservesExplicitPersistentSessionFallback() {
        assertEquals("POSTGRES", Monster.getQueueStoreType(JsonObject().put("SessionStoreType", "POSTGRES")))
        assertEquals("MONGODB", Monster.getQueueStoreType(JsonObject().put("SessionStoreType", "MONGODB")))
        assertEquals("SQLITE", Monster.getQueueStoreType(JsonObject().put("SessionStoreType", "MEMORY")))
    }

    @Test
    fun explicitMemoryStoreTypesAreAccepted() {
        assertEquals(SessionStoreType.MEMORY, SessionStoreType.valueOf("MEMORY"))
        assertEquals(QueueStoreType.MEMORY, QueueStoreType.valueOf("MEMORY"))
        assertEquals("MEMORY", Monster.getSessionStoreType(JsonObject().put("SessionStoreType", "MEMORY")))
        assertEquals("MEMORY", Monster.getQueueStoreType(JsonObject().put("QueueStoreType", "MEMORY")))
    }

    @Test
    fun sqliteBackedMemoryStoresWorkInProcess() {
        val vertx = Vertx.vertx()

        try {
            waitForDeployment(vertx.deployVerticle(SQLiteVerticle()), "sqlite verticle")

            val sessionStore = SessionStoreSQLite(SQLiteDatabasePath.SESSION_MEMORY, SessionStoreType.MEMORY)
            waitForDeployment(vertx.deployVerticle(sessionStore), "memory session store")
            assertEquals(SessionStoreType.MEMORY, sessionStore.getType())

            sessionStore.setClient("client-a", "node-a", cleanSession = false, connected = true, JsonObject())
            assertTrue("Expected in-memory session to be readable", sessionStore.isPresent("client-a"))

            val queueStore = QueueStoreSQLiteV2(SQLiteDatabasePath.QUEUE_MEMORY, 30, QueueStoreType.MEMORY)
            waitForDeployment(vertx.deployVerticle(queueStore), "memory queue store")
            assertEquals(QueueStoreType.MEMORY, queueStore.getType())

            val payload = "payload".toByteArray()
            val message = BrokerMessage(
                messageId = 1,
                topicName = "test/topic",
                payload = payload,
                qosLevel = 1,
                isRetain = false,
                isDup = false,
                isQueued = true,
                clientId = "publisher",
                time = Instant.now()
            )

            queueStore.enqueueMessages(listOf(message to listOf("client-a")))
            val fetched = pollUntilNotNull { queueStore.fetchNextPendingMessage("client-a") }
            assertNotNull("Expected in-memory queued message to be readable", fetched)
            assertEquals("test/topic", fetched!!.topicName)
            assertEquals("payload", String(fetched.payload))
        } finally {
            val closeLatch = CountDownLatch(1)
            vertx.close().onComplete { closeLatch.countDown() }
            closeLatch.await(5, TimeUnit.SECONDS)
        }
    }

    private fun waitForDeployment(future: Future<String>, label: String) {
        val latch = CountDownLatch(1)
        val errorRef = AtomicReference<Throwable?>()
        future.onComplete { result ->
            if (result.failed()) {
                errorRef.set(result.cause())
            }
            latch.countDown()
        }
        assertTrue("Timed out deploying $label", latch.await(5, TimeUnit.SECONDS))
        errorRef.get()?.let { throw AssertionError("Failed to deploy $label", it) }
    }

    private fun <T> pollUntilNotNull(fetch: () -> T?): T? {
        val deadline = System.currentTimeMillis() + 2_000
        var value = fetch()
        while (value == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(25)
            value = fetch()
        }
        return value
    }
}
