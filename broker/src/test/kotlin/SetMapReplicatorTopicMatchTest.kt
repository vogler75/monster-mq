package at.rocworks

import at.rocworks.cluster.SetMapReplicator
import at.rocworks.data.BrokerMessage
import at.rocworks.data.SubscriptionManager
import io.vertx.core.Vertx
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotSame
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test

class SetMapReplicatorTopicMatchTest {
    private lateinit var vertx: Vertx

    @Before
    fun setUp() {
        vertx = Vertx.vertx()
    }

    @After
    fun tearDown() {
        vertx.close()
    }

    @Test
    fun testFindValuesMatchingTopic() {
        val map = SetMapReplicator(vertx, "test.setmap.match")
        map.addToSet("a/b/c", "node1")
        map.addToSet("a/+/c", "node2")
        map.addToSet("a/#", "node3")
        map.addToSet("x/#", "node4")
        map.addToSet("#", "node5")

        assertEquals(setOf("node1", "node2", "node3", "node5"), map.findValuesMatchingTopic("a/b/c"))
        assertEquals(setOf("node4", "node5"), map.findValuesMatchingTopic("x/y"))
        // Wildcards at the first level do not match $ topics
        assertEquals(emptySet<String>(), map.findValuesMatchingTopic("\$SYS/x"))

        map.removeFromSet("a/+/c", "node2")
        map.removeKey("a/#")
        assertEquals(setOf("node1", "node5"), map.findValuesMatchingTopic("a/b/c"))

        map.removeValueFromAllSets("node5")
        assertEquals(setOf("node1"), map.findValuesMatchingTopic("a/b/c"))

        map.clear()
        assertTrue(map.findValuesMatchingTopic("a/b/c").isEmpty())
    }

    @Test
    fun testCloneOnlyCopiesWhenChanged() {
        val msg = BrokerMessage(messageId = 0, topicName = "t", payload = "p".toByteArray(), qosLevel = 1,
            isRetain = true, clientId = "c")
        assertSame(msg, msg.cloneWithNewQoS(1))
        assertSame(msg, msg.cloneWithNewMessageId(0))
        assertSame(msg, msg.cloneWithRetainFlag(true))
        assertSame(msg, msg.cloneForSubscriber(1, true))

        val copy = msg.cloneForSubscriber(0, false)
        assertNotSame(msg, copy)
        assertEquals(0, copy.qosLevel)
        assertFalse(copy.isRetain)
        assertEquals(msg.messageUuid, copy.messageUuid)
    }

    @Test
    fun testRetainAsPublishedLookupIsPerClient() {
        val manager = SubscriptionManager()
        manager.subscribe("c1", "a/+", 0, retainAsPublished = true)
        manager.subscribe("c2", "a/b", 0, retainAsPublished = false)
        manager.subscribe("c2", "x/#", 0, retainAsPublished = true)

        assertTrue(manager.getRetainAsPublished("c1", "a/b"))
        assertFalse(manager.getRetainAsPublished("c2", "a/b"))
        assertTrue(manager.getRetainAsPublished("c2", "x/y/z"))
        assertFalse(manager.getRetainAsPublished("c3", "a/b"))

        manager.unsubscribe("c1", "a/+")
        assertFalse(manager.getRetainAsPublished("c1", "a/b"))
    }
}
