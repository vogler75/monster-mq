package at.rocworks

import at.rocworks.data.BrokerMessage
import at.rocworks.stores.MessageStoreMemory
import io.vertx.core.AbstractVerticle
import io.vertx.core.Context
import io.vertx.core.Promise
import io.vertx.core.Vertx
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assert.assertFalse
import org.junit.Test
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class MessageStoreMemoryTest {

    private fun createMessage(topic: String, payload: String = "data"): BrokerMessage {
        return BrokerMessage(
            messageId = 1,
            topicName = topic,
            payload = payload.toByteArray(),
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = "test-client",
            time = Instant.now()
        )
    }

    @Test
    fun testLruEvictionRemovesTopicFromTopicTreeAndBrowse() {
        val store = MessageStoreMemory("test-lru", maxMemoryEntries = 3)

        val msg1 = createMessage("sensor/temp/1")
        val msg2 = createMessage("sensor/temp/2")
        val msg3 = createMessage("sensor/temp/3")
        store.addAll(listOf(msg1, msg2, msg3))

        // All 3 messages should be present
        assertNotNull(store.get("sensor/temp/1"))
        assertNotNull(store.get("sensor/temp/2"))
        assertNotNull(store.get("sensor/temp/3"))

        val topicsBefore = mutableListOf<String>()
        store.findMatchingTopics("#") { topicsBefore.add(it); true }
        assertTrue(topicsBefore.contains("sensor/temp/1"))
        assertTrue(topicsBefore.contains("sensor/temp/2"))
        assertTrue(topicsBefore.contains("sensor/temp/3"))

        // Add 4th message - should evict eldest (sensor/temp/1)
        val msg4 = createMessage("sensor/temp/4")
        store.addAll(listOf(msg4))

        // sensor/temp/1 must be evicted from both store and TopicTree
        assertNull(store.get("sensor/temp/1"))
        assertNotNull(store.get("sensor/temp/2"))
        assertNotNull(store.get("sensor/temp/3"))
        assertNotNull(store.get("sensor/temp/4"))

        val topicsAfter = mutableListOf<String>()
        store.findMatchingTopics("#") { topicsAfter.add(it); true }
        assertFalse("Evicted topic sensor/temp/1 must not appear in browse topics", topicsAfter.contains("sensor/temp/1"))
        assertTrue(topicsAfter.contains("sensor/temp/2"))
        assertTrue(topicsAfter.contains("sensor/temp/3"))
        assertTrue(topicsAfter.contains("sensor/temp/4"))

        // Matching messages lookup should also not return the evicted message
        var foundEvicted = false
        store.findMatchingMessages("sensor/temp/1") { foundEvicted = true; true }
        assertFalse("Evicted message must not be found by findMatchingMessages", foundEvicted)
    }

    @Test
    fun testContinuousWorkloadDoesNotGrowTopicTreeIndefinitely() {
        val maxEntries = 5L
        val store = MessageStoreMemory("test-bound", maxMemoryEntries = maxEntries)

        // Add 100 distinct topics sequentially
        for (i in 1..100) {
            store.addAll(listOf(createMessage("device/d$i")))
        }

        // Only the last 5 topics should exist in store
        for (i in 1..95) {
            assertNull(store.get("device/d$i"))
        }
        for (i in 96..100) {
            assertNotNull(store.get("device/d$i"))
        }

        // TopicTree browsing must only yield the active topics under "device/+"
        val leafTopics = mutableListOf<String>()
        store.findMatchingTopics("device/+") { leafTopics.add(it); true }

        assertEquals(
            "TopicTree must only contain exactly $maxEntries leaf topics after evictions",
            maxEntries.toInt(),
            leafTopics.size
        )
        val expected = (96..100).map { "device/d$it" }.toSet()
        assertEquals(expected, leafTopics.toSet())
    }

    @Test
    fun testAccessOrderLruPreservesAccessedEntry() {
        val store = MessageStoreMemory("test-access-order", maxMemoryEntries = 3)

        store.addAll(listOf(createMessage("t1"), createMessage("t2"), createMessage("t3")))

        // Access t1 (moving it to the end of LRU list)
        val accessed = store.get("t1")
        assertNotNull(accessed)

        // Adding t4 should evict t2 (eldest), NOT t1
        store.addAll(listOf(createMessage("t4")))

        assertNotNull("t1 was accessed and should not be evicted", store.get("t1"))
        assertNull("t2 was eldest and should be evicted", store.get("t2"))
        assertNotNull(store.get("t3"))
        assertNotNull(store.get("t4"))

        val activeTopics = mutableListOf<String>()
        store.findMatchingTopics("#") { activeTopics.add(it); true }
        assertTrue(activeTopics.contains("t1"))
        assertFalse(activeTopics.contains("t2"))
        assertTrue(activeTopics.contains("t3"))
        assertTrue(activeTopics.contains("t4"))
    }

    @Test
    fun testEvictingChildPreservesStoredParent() {
        val store = MessageStoreMemory("test-parent", maxMemoryEntries = 2)
        store.addAll(listOf(createMessage("a/b"), createMessage("a")))
        store.addAll(listOf(createMessage("c")))

        assertNull(store.get("a/b"))
        assertNotNull(store.get("a"))
        assertEquals(setOf("a", "c"), browseTopics(store))
        assertEquals(setOf("a"), matchingTopics(store, "a"))
        assertEquals(setOf("a", "c"), matchingTopics(store, "#"))

        store.delAll(listOf("a", "c"))
        assertTrue("Deleting the remaining messages must prune their topic nodes", browseTopics(store).isEmpty())
    }

    @Test
    fun testBatchReinsertionRestoresEvictedTopic() {
        val store = MessageStoreMemory("test-batch-reinsert", maxMemoryEntries = 2)
        store.addAll(listOf(createMessage("a"), createMessage("b")))
        store.addAll(listOf(createMessage("c"), createMessage("a", "updated")))

        assertNull(store.get("b"))
        assertEquals("updated", store.get("a")?.getPayloadAsString())
        assertEquals(setOf("a", "c"), browseTopics(store))
        assertEquals(setOf("a"), matchingTopics(store, "a"))
        assertEquals(setOf("a", "c"), matchingTopics(store, "#"))
    }

    @Test
    fun testBatchLargerThanCapacityIndexesOnlySurvivingMessages() {
        val store = MessageStoreMemory("test-large-batch", maxMemoryEntries = 2)
        store.addAll(listOf("a", "b", "c", "d").map { createMessage(it) })

        assertNull(store.get("a"))
        assertNull(store.get("b"))
        assertEquals(setOf("c", "d"), browseTopics(store))
        assertEquals(setOf("c", "d"), matchingTopics(store, "#"))
    }

    @Test
    fun testMatchingIncludesStoredParentAndChild() {
        val store = MessageStoreMemory("test-parent-and-child")
        store.addAll(listOf(createMessage("a"), createMessage("a/b")))

        assertEquals(setOf("a", "a/b"), matchingTopics(store, "#"))
        assertEquals(setOf("a", "a/b"), matchingTopics(store, "a/#"))
        assertEquals(setOf("a/b"), matchingTopics(store, "a/+"))

        store.delAll(listOf("a"))
        assertEquals(setOf("a/b"), matchingTopics(store, "#"))
        store.delAll(listOf("a/b"))
        assertTrue(browseTopics(store).isEmpty())
    }

    @Test
    fun testDeployedStoreIgnoresOutdatedLocalNotifications() {
        val vertx = Vertx.vertx()
        val store = MessageStoreMemory("test-deployed", maxMemoryEntries = 2)
        lateinit var storeContext: Context
        try {
            // Run the store on a known deployment context so writes can queue their
            // notifications before the event loop gets a chance to process them.
            val deployment = object : AbstractVerticle() {
                override fun start(startPromise: Promise<Void>) {
                    storeContext = context
                    store.init(vertx, context)
                    store.start(startPromise)
                }
            }
            vertx.deployVerticle(deployment).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS)

            onContext(storeContext) {
                store.addAll(listOf(createMessage("a"), createMessage("b")))
                store.addAll(listOf(createMessage("c"), createMessage("a")))
                store.addAll(listOf(createMessage("d")))
            }
            onContext(storeContext) {
                assertEquals(setOf("a", "d"), browseTopics(store))
                assertEquals(setOf("a", "d"), matchingTopics(store, "#"))
            }

            onContext(storeContext) {
                store.addAll(listOf(createMessage("x"), createMessage("y"), createMessage("z")))
                assertTrue(store.dropStorage())
            }
            onContext(storeContext) {
                assertTrue("Queued adds must not repopulate a dropped index", browseTopics(store).isEmpty())
            }
        } finally {
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS)
        }
    }

    @Test
    fun testConcurrentWritesKeepIndexConsistentWithBoundedStore() {
        val store = MessageStoreMemory("test-concurrent", maxMemoryEntries = 20)
        val executor = Executors.newFixedThreadPool(3)
        try {
            val writers = (1..2).map { writer ->
                executor.submit {
                    repeat(200) { i ->
                        store.addAll(listOf(createMessage("device/$writer-$i")))
                    }
                }
            }
            val reader = executor.submit {
                repeat(200) {
                    store.findTopicsByName("", false, "")
                    store.findTopicsByConfig("description", "", false, "")
                    matchingTopics(store, "#")
                }
            }
            (writers + reader).forEach { it.get(10, TimeUnit.SECONDS) }

            val storedTopics = store.findTopicsByName("", false, "").toSet()
            assertEquals(20, storedTopics.size)
            assertEquals(storedTopics, matchingTopics(store, "#"))
            assertEquals(storedTopics + "device", browseTopics(store))
        } finally {
            executor.shutdownNow()
        }
    }

    private fun onContext(context: Context, action: () -> Unit) {
        val completed = CompletableFuture<Unit>()
        context.runOnContext {
            try {
                action()
                completed.complete(Unit)
            } catch (error: Throwable) {
                completed.completeExceptionally(error)
            }
        }
        completed.get(10, TimeUnit.SECONDS)
    }

    private fun browseTopics(store: MessageStoreMemory): Set<String> {
        val topics = mutableSetOf<String>()
        store.findMatchingTopics("#") { topics.add(it); true }
        return topics
    }

    private fun matchingTopics(store: MessageStoreMemory, filter: String): Set<String> {
        val topics = mutableSetOf<String>()
        store.findMatchingMessages(filter) { topics.add(it.topicName); true }
        return topics
    }

    @Test
    fun testDropStorageClearsStoreAndTopicTree() {
        val store = MessageStoreMemory("test-drop", maxMemoryEntries = 5)
        store.addAll(listOf(createMessage("a/b/1"), createMessage("a/b/2"), createMessage("x/y/z")))

        val topicsBefore = mutableListOf<String>()
        store.findMatchingTopics("#") { topicsBefore.add(it); true }
        assertTrue(topicsBefore.isNotEmpty())

        assertTrue(store.dropStorage())

        assertNull(store.get("a/b/1"))
        assertNull(store.get("a/b/2"))
        assertNull(store.get("x/y/z"))

        val topicsAfter = mutableListOf<String>()
        store.findMatchingTopics("#") { topicsAfter.add(it); true }
        assertTrue("TopicTree must be empty after dropStorage", topicsAfter.isEmpty())
    }
}
