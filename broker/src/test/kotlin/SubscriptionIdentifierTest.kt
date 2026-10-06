package at.rocworks

import at.rocworks.data.MqttSubscription
import at.rocworks.data.MqttSubscriptionCodec
import at.rocworks.data.SubscriptionManager
import at.rocworks.stores.sqlite.SessionStoreSQLite
import at.rocworks.stores.sqlite.SQLiteVerticle
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.File
import java.sql.DriverManager
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * MQTT v5 Subscription Identifiers (monster-mq#198): index lookup, cluster codec
 * and persistence of the subscription_id column, which the Go edge broker shares.
 */
class SubscriptionIdentifierTest {

    @Test
    fun matchingSubscriptionsReturnAllIdentifiersSorted() {
        val sm = SubscriptionManager()
        sm.subscribe("c1", "plant/+/temp", 1, subscriptionId = 9)
        sm.subscribe("c1", "plant/a/temp", 1, subscriptionId = 3)
        sm.subscribe("c1", "plant/#", 1)  // no identifier
        sm.subscribe("c2", "plant/#", 1, subscriptionId = 5)

        assertEquals(listOf(3, 9), sm.getSubscriptionIdentifiers("c1", "plant/a/temp"))
        assertEquals(listOf(9), sm.getSubscriptionIdentifiers("c1", "plant/b/temp"))
        assertEquals(emptyList<Int>(), sm.getSubscriptionIdentifiers("c1", "plant/a/pressure"))
        assertEquals(listOf(5), sm.getSubscriptionIdentifiers("c2", "plant/a/temp"))
        assertEquals(emptyList<Int>(), sm.getSubscriptionIdentifiers("c3", "plant/a/temp"))
    }

    @Test
    fun resubscribeUnsubscribeAndDisconnectUpdateIdentifiers() {
        val sm = SubscriptionManager()
        sm.subscribe("c1", "a/#", 0, subscriptionId = 1)
        sm.subscribe("c1", "a/b", 0, subscriptionId = 2)

        // A new SUBSCRIBE for the same filter replaces the identifier, 0 removes it
        sm.subscribe("c1", "a/#", 0, subscriptionId = 7)
        assertEquals(listOf(2, 7), sm.getSubscriptionIdentifiers("c1", "a/b"))
        sm.subscribe("c1", "a/#", 0)
        assertEquals(listOf(2), sm.getSubscriptionIdentifiers("c1", "a/b"))

        sm.unsubscribe("c1", "a/b")
        assertEquals(emptyList<Int>(), sm.getSubscriptionIdentifiers("c1", "a/b"))

        sm.subscribe("c1", "x", 0, subscriptionId = 4)
        sm.disconnectClient("c1")
        assertEquals(emptyList<Int>(), sm.getSubscriptionIdentifiers("c1", "x"))
    }

    @Test
    fun codecRoundTripsSubscriptionId() {
        val codec = MqttSubscriptionCodec()
        val sub = MqttSubscription("c1", "a/+", MqttQoS.AT_LEAST_ONCE, noLocal = true, retainHandling = 1, retainAsPublished = true, subscriptionId = 268435455)
        val buffer = Buffer.buffer()
        codec.encodeToWire(buffer, sub)
        assertEquals(sub, codec.decodeFromWire(0, buffer))
    }

    @Test
    fun codecDecodesMessageFromOlderNodeWithoutSubscriptionId() {
        val codec = MqttSubscriptionCodec()
        val buffer = Buffer.buffer()
        codec.encodeToWire(buffer, MqttSubscription("c1", "a/+", MqttQoS.AT_MOST_ONCE, subscriptionId = 42))
        val legacy = buffer.getBuffer(0, buffer.length() - 4)  // layout before the identifier was added
        assertEquals(MqttSubscription("c1", "a/+", MqttQoS.AT_MOST_ONCE), codec.decodeFromWire(0, legacy))
    }

    @Test
    fun sqliteMigratesOldTableAndPersistsSubscriptionId() {
        val dbFile = File.createTempFile("monstermq-sessions-", ".db")
        dbFile.deleteOnExit()
        // Layout from before retain_as_published and subscription_id existed
        DriverManager.getConnection("jdbc:sqlite:${dbFile.absolutePath}").use { conn ->
            conn.createStatement().use { st ->
                st.executeUpdate("""
                    CREATE TABLE subscriptions (
                        client_id TEXT, topic TEXT, qos INTEGER, wildcard BOOLEAN,
                        no_local INTEGER DEFAULT 0, retain_handling INTEGER DEFAULT 0,
                        PRIMARY KEY (client_id, topic))
                """.trimIndent())
                st.executeUpdate("INSERT INTO subscriptions VALUES ('old', 'x/+', 2, 1, 1, 1)")
            }
        }

        val vertx = Vertx.vertx()
        try {
            waitForDeployment(vertx.deployVerticle(SQLiteVerticle()), "sqlite verticle")
            val store = SessionStoreSQLite(dbFile.absolutePath)
            waitForDeployment(vertx.deployVerticle(store), "session store")

            store.addSubscriptions(listOf(
                MqttSubscription("new", "y/#", MqttQoS.AT_LEAST_ONCE, retainAsPublished = true, subscriptionId = 42)
            ))

            val rows = mutableMapOf<String, List<Any>>()
            store.iterateSubscriptions { topic, clientId, qos, noLocal, retainHandling, retainAsPublished, subscriptionId ->
                rows[clientId] = listOf(topic, qos, noLocal, retainHandling, retainAsPublished, subscriptionId)
            }
            assertEquals(listOf("x/+", 2, true, 1, false, 0), rows["old"])
            assertEquals(listOf("y/#", 1, false, 0, true, 42), rows["new"])
        } finally {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS)
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
