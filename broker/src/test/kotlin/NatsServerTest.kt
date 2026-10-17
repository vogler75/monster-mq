package at.rocworks

import at.rocworks.auth.UserManager
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.data.User
import at.rocworks.handlers.SessionHandler
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class NatsServerTest {

    private lateinit var vertx: Vertx
    private var port: Int = 0
    private lateinit var userManager: UserManager
    private lateinit var sessionHandler: SessionHandler

    private val messagesInCount = AtomicInteger(0)
    private val messagesOutCount = AtomicInteger(0)
    private val subscriptions = ConcurrentHashMap<String, MutableSet<String>>() // topic -> set of clientIds

    @Before
    fun setUp() {
        vertx = Vertx.vertx()
        port = ServerSocket(0).use { it.localPort }

        messagesInCount.set(0)
        messagesOutCount.set(0)
        subscriptions.clear()

        userManager = Mockito.mock(UserManager::class.java) { inv ->
            when (inv.method.name) {
                "isUserManagementEnabled" -> true
                "isAnonymousEnabled" -> false
                "isAdmin" -> inv.getArgument<String>(0) == "admin"
                "authenticate" -> {
                    val u = inv.getArgument<String>(0)
                    val p = inv.getArgument<String>(1)
                    if (p == "secret") {
                        Future.succeededFuture(User(u, "secret", enabled = true))
                    } else {
                        Future.succeededFuture(null)
                    }
                }
                "canSubscribe" -> {
                    val user = inv.getArgument<String>(0)
                    val topic = inv.getArgument<String>(1)
                    if (user == "admin") true
                    else if (topic.startsWith("allowed/secret")) false
                    else if (topic.startsWith("allowed") || topic == "a" || topic == "b") true
                    else false
                }
                "canPublish" -> {
                    val user = inv.getArgument<String>(0)
                    val topic = inv.getArgument<String>(1)
                    if (user == "admin") true
                    else if (topic.startsWith("denied")) false
                    else true
                }
                else -> Mockito.RETURNS_DEFAULTS.answer(inv)
            }
        }

        sessionHandler = Mockito.mock(SessionHandler::class.java) { inv ->
            when (inv.method.name) {
                "setClient" -> Future.succeededFuture<Void>()
                "onlineClient" -> null
                "subscribeInternalClient" -> {
                    val cid = inv.getArgument<String>(0)
                    val topic = inv.getArgument<String>(1)
                    subscriptions.getOrPut(topic) { ConcurrentHashMap.newKeySet() }.add(cid)
                    null
                }
                "unsubscribeInternalClient" -> {
                    val cid = inv.getArgument<String>(0)
                    val topic = inv.getArgument<String>(1)
                    subscriptions[topic]?.remove(cid)
                    null
                }
                "publishMessage" -> {
                    val msg = inv.getArgument<BrokerMessage>(0)
                    // Deliver to subscribed clients
                    subscriptions.forEach { (filter, clientIds) ->
                        if (filter == msg.topicName || filter == "#" || (filter == "allowed/#" && msg.topicName.startsWith("allowed/"))) {
                            clientIds.forEach { cid ->
                                vertx.eventBus().publish(EventBusAddresses.Client.messages(cid), msg)
                            }
                        }
                    }
                    null
                }
                "incrementMessagesIn" -> {
                    messagesInCount.incrementAndGet()
                    null
                }
                "incrementMessagesOut" -> {
                    messagesOutCount.incrementAndGet()
                    null
                }
                "unregisterInternalClient" -> null
                "delClient" -> null
                else -> Mockito.RETURNS_DEFAULTS.answer(inv)
            }
        }

        val server = NatsServer(port, sessionHandler, userManager)
        vertx.deployVerticle(server).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS)
    }

    @After
    fun tearDown() {
        try {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS)
        } catch (_: Exception) {}
    }

    private fun connectClient(): Socket {
        val s = Socket("127.0.0.1", port)
        s.soTimeout = 3000
        val reader = BufferedReader(InputStreamReader(s.getInputStream()))
        val info = reader.readLine()
        assertNotNull(info)
        assertTrue(info.startsWith("INFO "))
        return s
    }

    // F1: Rejected PUB/HPUB skips payload
    @Test
    fun testRejectedPubBeforeConnectSkipsPayload() {
        val s = connectClient()
        val out = s.getOutputStream()
        val reader = BufferedReader(InputStreamReader(s.getInputStream()))

        // Send unauthenticated PUB followed immediately by PING
        out.write("PUB a 5\r\nhello\r\nPING\r\n".toByteArray())
        out.flush()

        val err = reader.readLine()
        assertEquals("-ERR 'Authorization Violation'", err)

        val pong = reader.readLine()
        assertEquals("PONG", pong)

        s.close()
    }

    @Test
    fun testRejectedPubAclSkipsPayload() {
        val s = connectClient()
        val out = s.getOutputStream()
        val reader = BufferedReader(InputStreamReader(s.getInputStream()))

        // Authenticate as alice
        out.write("CONNECT {\"user\":\"alice\",\"pass\":\"secret\",\"verbose\":true}\r\n".toByteArray())
        out.flush()
        val ok = reader.readLine()
        assertEquals("+OK", ok)

        // PUB to denied.x with payload, followed by PING
        out.write("PUB denied.x 5\r\nhello\r\nPING\r\n".toByteArray())
        out.flush()

        val err = reader.readLine()
        assertEquals("-ERR 'Permissions Violation for Publish to \"denied.x\"'", err)

        val pong = reader.readLine()
        assertEquals("PONG", pong)

        s.close()
    }

    // F2: SUB with existing SID cleans up previous topic mapping
    @Test
    fun testSubSidReuseCleansUpOldFilter() {
        val s = connectClient()
        val out = s.getOutputStream()
        val reader = BufferedReader(InputStreamReader(s.getInputStream()))

        out.write("CONNECT {\"user\":\"alice\",\"pass\":\"secret\",\"verbose\":true}\r\n".toByteArray())
        out.flush()
        assertEquals("+OK", reader.readLine())

        // Subscribe to a with SID 1
        out.write("SUB a 1\r\n".toByteArray())
        out.flush()

        // Re-use SID 1 for b and sync
        out.write("SUB b 1\r\nPING\r\n".toByteArray())
        out.flush()
        assertEquals("PONG", reader.readLine())

        // Publish to a: must NOT be delivered to SID 1
        val msgA = BrokerMessage(messageId = 0, topicName = "a", payload = "msgA".toByteArray(),
            qosLevel = 0, isRetain = false, isDup = false, isQueued = false, clientId = "test-pub")
        subscriptions["a"]?.forEach { cid ->
            vertx.eventBus().publish(EventBusAddresses.Client.messages(cid), msgA)
        }

        // Publish to b: MUST be delivered to SID 1
        val msgB = BrokerMessage(messageId = 0, topicName = "b", payload = "msgB".toByteArray(),
            qosLevel = 0, isRetain = false, isDup = false, isQueued = false, clientId = "test-pub")
        subscriptions["b"]?.forEach { cid ->
            vertx.eventBus().publish(EventBusAddresses.Client.messages(cid), msgB)
        }

        val deliveredMsg = reader.readLine()
        assertEquals("MSG b 1 4", deliveredMsg)
        val payload = reader.readLine()
        assertEquals("msgB", payload)

        // Now UNSUB 1
        out.write("UNSUB 1\r\nPING\r\n".toByteArray())
        out.flush()
        assertEquals("PONG", reader.readLine())

        // Publish to b again: must not be delivered
        subscriptions["b"]?.forEach { cid ->
            vertx.eventBus().publish(EventBusAddresses.Client.messages(cid), msgB)
        }

        out.write("PING\r\n".toByteArray())
        out.flush()
        assertEquals("PONG", reader.readLine())

        s.close()
    }

    // F3: Pipelined CONNECT and SUB in one write
    @Test
    fun testPipelinedConnectAndSub() {
        val s = connectClient()
        val out = s.getOutputStream()
        val reader = BufferedReader(InputStreamReader(s.getInputStream()))

        // Send CONNECT and SUB in one write
        out.write("CONNECT {\"user\":\"alice\",\"pass\":\"secret\",\"verbose\":false}\r\nSUB allowed.a 1\r\nPING\r\n".toByteArray())
        out.flush()

        assertEquals("PONG", reader.readLine())

        // Now publish to allowed/a
        val msg = BrokerMessage(messageId = 0, topicName = "allowed/a", payload = "hello".toByteArray(),
            qosLevel = 0, isRetain = false, isDup = false, isQueued = false, clientId = "test-pub")
        subscriptions["allowed/a"]?.forEach { cid ->
            vertx.eventBus().publish(EventBusAddresses.Client.messages(cid), msg)
        }

        val header = reader.readLine()
        assertEquals("MSG allowed.a 1 5", header)
        val body = reader.readLine()
        assertEquals("hello", body)

        s.close()
    }

    // F4: UNSUB with max_msgs
    @Test
    fun testUnsubWithMaxMsgs() {
        val s = connectClient()
        val out = s.getOutputStream()
        val reader = BufferedReader(InputStreamReader(s.getInputStream()))

        out.write("CONNECT {\"user\":\"alice\",\"pass\":\"secret\",\"verbose\":false}\r\nSUB allowed.a 1\r\nUNSUB 1 2\r\nPING\r\n".toByteArray())
        out.flush()
        assertEquals("PONG", reader.readLine())

        val msg = BrokerMessage(messageId = 0, topicName = "allowed/a", payload = "test".toByteArray(),
            qosLevel = 0, isRetain = false, isDup = false, isQueued = false, clientId = "test-pub")

        // Deliver msg 1
        subscriptions["allowed/a"]?.forEach { cid -> vertx.eventBus().publish(EventBusAddresses.Client.messages(cid), msg) }
        assertEquals("MSG allowed.a 1 4", reader.readLine())
        assertEquals("test", reader.readLine())

        // Deliver msg 2
        subscriptions["allowed/a"]?.forEach { cid -> vertx.eventBus().publish(EventBusAddresses.Client.messages(cid), msg) }
        assertEquals("MSG allowed.a 1 4", reader.readLine())
        assertEquals("test", reader.readLine())

        // Deliver msg 3: should NOT be delivered because max_msgs (2) was reached
        subscriptions["allowed/a"]?.forEach { cid -> vertx.eventBus().publish(EventBusAddresses.Client.messages(cid), msg) }

        out.write("PING\r\n".toByteArray())
        out.flush()
        assertEquals("PONG", reader.readLine())

        // Test immediate unsubscription when delivered >= max_msgs
        out.write("SUB allowed.a 2\r\nPING\r\n".toByteArray())
        out.flush()
        assertEquals("PONG", reader.readLine())

        // Deliver 3 messages
        for (i in 1..3) {
            subscriptions["allowed/a"]?.forEach { cid -> vertx.eventBus().publish(EventBusAddresses.Client.messages(cid), msg) }
            assertEquals("MSG allowed.a 2 4", reader.readLine())
            assertEquals("test", reader.readLine())
        }

        // UNSUB 2 1: already delivered 3 >= 1, so removed at once
        out.write("UNSUB 2 1\r\nPING\r\n".toByteArray())
        out.flush()
        assertEquals("PONG", reader.readLine())

        // Deliver msg 4: should not arrive
        subscriptions["allowed/a"]?.forEach { cid -> vertx.eventBus().publish(EventBusAddresses.Client.messages(cid), msg) }
        out.write("PING\r\n".toByteArray())
        out.flush()
        assertEquals("PONG", reader.readLine())

        s.close()
    }

    // F5: Session metrics in / out counters
    @Test
    fun testSessionCountersIncrement() {
        val s = connectClient()
        val out = s.getOutputStream()
        val reader = BufferedReader(InputStreamReader(s.getInputStream()))

        out.write("CONNECT {\"user\":\"alice\",\"pass\":\"secret\",\"verbose\":false}\r\nSUB allowed.a 1\r\nPING\r\n".toByteArray())
        out.flush()
        assertEquals("PONG", reader.readLine())

        val initialIn = messagesInCount.get()
        val initialOut = messagesOutCount.get()

        // Publish from client
        out.write("PUB allowed.a 3\r\nxyz\r\n".toByteArray())
        out.flush()

        // Read delivered message
        assertEquals("MSG allowed.a 1 3", reader.readLine())
        assertEquals("xyz", reader.readLine())

        assertEquals(initialIn + 1, messagesInCount.get())
        assertEquals(initialOut + 1, messagesOutCount.get())

        s.close()
    }

    // F6: Delivery-time ACL check
    @Test
    fun testDeliveryTimeAclCheck() {
        val s = connectClient()
        val out = s.getOutputStream()
        val reader = BufferedReader(InputStreamReader(s.getInputStream()))

        out.write("CONNECT {\"user\":\"alice\",\"pass\":\"secret\",\"verbose\":false}\r\nSUB allowed.> 1\r\nPING\r\n".toByteArray())
        out.flush()
        assertEquals("PONG", reader.readLine())

        // 1. Deliver allowed/a -> should be received
        val msgAllowed = BrokerMessage(messageId = 0, topicName = "allowed/a", payload = "ok".toByteArray(),
            qosLevel = 0, isRetain = false, isDup = false, isQueued = false, clientId = "test-pub")
        subscriptions["allowed/#"]?.forEach { cid ->
            vertx.eventBus().publish(EventBusAddresses.Client.messages(cid), msgAllowed)
        }
        assertEquals("MSG allowed.a 1 2", reader.readLine())
        assertEquals("ok", reader.readLine())

        // 2. Deliver allowed/secret/b -> should be dropped by delivery-time ACL
        val msgSecret = BrokerMessage(messageId = 0, topicName = "allowed/secret/b", payload = "secret".toByteArray(),
            qosLevel = 0, isRetain = false, isDup = false, isQueued = false, clientId = "test-pub")
        subscriptions["allowed/#"]?.forEach { cid ->
            vertx.eventBus().publish(EventBusAddresses.Client.messages(cid), msgSecret)
        }

        // Verify with PING that nothing arrived
        out.write("PING\r\n".toByteArray())
        out.flush()
        assertEquals("PONG", reader.readLine())

        s.close()
    }

    // F7: max_payload and control line limits
    @Test
    fun testPayloadAndLineLimits() {
        // 1. Payload > 1048576
        val s1 = connectClient()
        val out1 = s1.getOutputStream()
        val reader1 = BufferedReader(InputStreamReader(s1.getInputStream()))

        out1.write("PUB a 2000000\r\n".toByteArray())
        out1.flush()

        val err = reader1.readLine()
        assertEquals("-ERR 'Maximum Payload Violation'", err)
        // Connection should be closed by server (EOF)
        assertEquals(-1, s1.getInputStream().read())
        s1.close()

        // 2. Line > 65536
        val s2 = connectClient()
        val out2 = s2.getOutputStream()
        val reader2 = BufferedReader(InputStreamReader(s2.getInputStream()))

        val longLine = "PUB " + "x".repeat(70000) + "\r\n"
        out2.write(longLine.toByteArray())
        out2.flush()

        val errLine = reader2.readLine()
        assertEquals("-ERR 'Maximum Control Line Exceeded'", errLine)
        assertEquals(-1, s2.getInputStream().read())
        s2.close()
    }
}
