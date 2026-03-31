package at.rocworks

import at.rocworks.data.BrokerMessage
import at.rocworks.stores.sqlite.MessageStoreSQLite
import at.rocworks.stores.sqlite.SQLiteVerticle
import io.vertx.core.Vertx
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.File
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class MessageStoreSQLiteTest {

    @Test
    fun exactLookupPreservesRetainedMetadata() {
        val vertx = Vertx.vertx()
        val dbFile = File.createTempFile("monstermq-retained-", ".db")
        dbFile.deleteOnExit()

        try {
            waitForDeployment(vertx.deployVerticle(SQLiteVerticle()), "sqlite verticle")
            val store = MessageStoreSQLite("RetainedMessages", dbFile.absolutePath)
            waitForDeployment(vertx.deployVerticle(store), "message store")

            val topic = "a2a/v1/default/default/discovery/TestAgent"
            val payload = """{"name":"TestAgent","status":"running"}""".toByteArray()
            val createdAt = Instant.ofEpochMilli(1_735_689_600_123L)
            val expiryInterval = 600L
            val message = BrokerMessage(
                messageId = 0,
                topicName = topic,
                payload = payload,
                qosLevel = 1,
                isRetain = true,
                isDup = false,
                isQueued = false,
                clientId = "test-client",
                time = createdAt,
                messageExpiryInterval = expiryInterval
            )

            store.addAll(listOf(message))

            val fetchedRef = AtomicReference<BrokerMessage?>()
            val deadline = System.currentTimeMillis() + 2_000
            while (System.currentTimeMillis() < deadline && fetchedRef.get() == null) {
                val latch = CountDownLatch(1)
                store.getAsync(topic) {
                    fetchedRef.set(it)
                    latch.countDown()
                }
                assertTrue("Timed out waiting for retained lookup callback", latch.await(2, TimeUnit.SECONDS))
                if (fetchedRef.get() == null) {
                    Thread.sleep(25)
                }
            }

            val fetched = fetchedRef.get()
            assertNotNull("Expected retained message to be returned for exact lookup", fetched)
            assertArrayEquals(payload, fetched!!.payload)
            assertTrue("Expected retained flag to be preserved", fetched.isRetain)
            assertEquals(1, fetched.qosLevel)
            assertEquals(createdAt.toEpochMilli(), fetched.time.toEpochMilli())
            assertEquals(expiryInterval, fetched.messageExpiryInterval)
        } finally {
            val closeLatch = CountDownLatch(1)
            vertx.close().onComplete { closeLatch.countDown() }
            closeLatch.await(5, TimeUnit.SECONDS)
        }
    }

    private fun waitForDeployment(future: io.vertx.core.Future<String>, label: String) {
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
}
