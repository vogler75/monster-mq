package at.rocworks

import at.rocworks.auth.UserManager
import at.rocworks.data.BrokerMessage
import at.rocworks.data.User
import at.rocworks.extensions.RedisServer
import at.rocworks.extensions.graphql.ArchiveGroupResolver
import at.rocworks.extensions.graphql.GraphQLAuthContext
import at.rocworks.extensions.graphql.AuthorizationResult
import at.rocworks.handlers.ArchiveGroup
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.handlers.SessionHandler
import at.rocworks.stores.MessageStoreMemory
import at.rocworks.stores.MessageStoreType
import at.rocworks.stores.MessageArchiveType
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
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class RedisServerTest {

    private lateinit var vertx: Vertx
    private var port: Int = 0
    private lateinit var userManager: UserManager
    private lateinit var sessionHandler: SessionHandler
    private lateinit var archiveHandler: ArchiveHandler

    private val messagesInCount = AtomicInteger(0)
    private val messagesOutCount = AtomicInteger(0)
    private val listeners = ConcurrentHashMap<String, (BrokerMessage) -> Unit>()
    private val deployedGroups = ConcurrentHashMap<String, ArchiveGroup>()

    private lateinit var defaultGroup: ArchiveGroup
    private lateinit var defaultStore: MessageStoreMemory

    private fun createArchiveGroup(
        name: String,
        redisDbNumber: Int? = null,
        readOnly: Boolean = false,
        store: MessageStoreMemory? = null
    ): ArchiveGroup {
        val ag = ArchiveGroup(
            name = name,
            topicFilter = listOf("#"),
            retainedOnly = false,
            lastValType = MessageStoreType.MEMORY,
            archiveType = MessageArchiveType.NONE,
            redisDbNumber = redisDbNumber,
            databaseConfig = JsonObject(),
            lastValReadOnly = readOnly
        )
        if (store != null) {
            val field = ArchiveGroup::class.java.getDeclaredField("lastValStore")
            field.isAccessible = true
            field.set(ag, store)
        }
        return ag
    }

    @Before
    fun setUp() {
        vertx = Vertx.vertx()
        port = ServerSocket(0).use { it.localPort }

        messagesInCount.set(0)
        messagesOutCount.set(0)
        listeners.clear()
        deployedGroups.clear()

        defaultStore = MessageStoreMemory("default-store", 10000)
        defaultGroup = createArchiveGroup("Default", redisDbNumber = 0, readOnly = false, store = defaultStore)
        deployedGroups["Default"] = defaultGroup

        userManager = Mockito.mock(UserManager::class.java) { inv ->
            when (inv.method.name) {
                "isUserManagementEnabled" -> true
                "isAnonymousEnabled" -> false
                "isAdmin" -> inv.getArgument<String>(0) == "admin"
                "getUser" -> {
                    val u = inv.getArgument<String>(0)
                    User(u, "secret", enabled = true, isAdmin = (u == "admin"))
                }
                "authenticate" -> {
                    val u = inv.getArgument<String>(0)
                    val p = inv.getArgument<String>(1)
                    if (p == "secret") {
                        Future.succeededFuture(User(u, "secret", enabled = true, isAdmin = (u == "admin")))
                    } else {
                        Future.succeededFuture(null)
                    }
                }
                "canSubscribe" -> {
                    val user = inv.getArgument<String>(0)
                    val topic = inv.getArgument<String>(1)
                    if (user == "admin") true
                    else if (topic.startsWith("allowed/secret")) false
                    else true
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
                "onlineClient" -> Future.succeededFuture<Void>()
                "delClient" -> Future.succeededFuture<Void>()
                "incrementMessagesIn" -> {
                    messagesInCount.incrementAndGet()
                    null
                }
                "incrementMessagesOut" -> {
                    messagesOutCount.incrementAndGet()
                    null
                }
                "registerMessageListener" -> {
                    val listenerId = inv.getArgument<String>(0)
                    val callback = inv.getArgument<(BrokerMessage) -> Unit>(2)
                    listeners[listenerId] = callback
                    null
                }
                "unregisterMessageListener" -> {
                    val listenerId = inv.getArgument<String>(0)
                    listeners.remove(listenerId)
                    null
                }
                "publishMessage" -> {
                    val msg = inv.getArgument<BrokerMessage>(0)
                    defaultStore.addAll(listOf(msg))
                    listeners.values.forEach { it(msg) }
                    null
                }
                else -> Mockito.RETURNS_DEFAULTS.answer(inv)
            }
        }

        archiveHandler = Mockito.mock(ArchiveHandler::class.java) { inv ->
            when (inv.method.name) {
                "getDeployedArchiveGroups" -> deployedGroups
                else -> Mockito.RETURNS_DEFAULTS.answer(inv)
            }
        }
    }

    @After
    fun tearDown() {
        val latch = java.util.concurrent.CountDownLatch(1)
        vertx.close().onComplete { latch.countDown() }
        latch.await(5, TimeUnit.SECONDS)
    }

    private fun startRedisServer(
        requireAuth: Boolean = false,
        keysLimit: Int = 10000
    ) {
        val config = JsonObject().put(
            "RedisServer", JsonObject()
                .put("Host", "127.0.0.1")
                .put("Port", port)
                .put("RequireAuth", requireAuth)
                .put("KeysLimit", keysLimit)
        )
        val redisServer = RedisServer(config, archiveHandler, sessionHandler, userManager)
        val latch = java.util.concurrent.CountDownLatch(1)
        vertx.deployVerticle(redisServer).onComplete { latch.countDown() }
        assertTrue("Redis server start timeout", latch.await(5, TimeUnit.SECONDS))
    }

    private inner class RedisClient(timeoutMs: Int = 5000) : AutoCloseable {
        private val socket = Socket("127.0.0.1", port).apply { soTimeout = timeoutMs }
        private val output = socket.getOutputStream()
        private val input = socket.getInputStream()
        private val reader = BufferedReader(InputStreamReader(input, StandardCharsets.UTF_8))

        fun send(raw: String) {
            output.write(raw.toByteArray(StandardCharsets.UTF_8))
            output.flush()
        }

        fun sendCommand(vararg args: String) {
            val sb = StringBuilder()
            sb.append("*").append(args.size).append("\r\n")
            for (arg in args) {
                val bytes = arg.toByteArray(StandardCharsets.UTF_8)
                sb.append("$").append(bytes.size).append("\r\n").append(arg).append("\r\n")
            }
            send(sb.toString())
        }

        fun readResp(): Any? {
            val line = reader.readLine() ?: return null
            return when (line[0]) {
                '+' -> line.substring(1)
                '-' -> "ERR:" + line.substring(1)
                ':' -> line.substring(1).toLong()
                '$' -> {
                    val len = line.substring(1).toInt()
                    if (len == -1) null
                    else {
                        val chars = CharArray(len)
                        var read = 0
                        while (read < len) {
                            val r = reader.read(chars, read, len - read)
                            if (r == -1) break
                            read += r
                        }
                        reader.readLine() // consume trailing \r\n
                        String(chars)
                    }
                }
                '*' -> {
                    val count = line.substring(1).toInt()
                    if (count == -1) null
                    else {
                        val list = mutableListOf<Any?>()
                        repeat(count) {
                            list.add(readResp())
                        }
                        list
                    }
                }
                else -> line
            }
        }

        override fun close() {
            try { socket.close() } catch (_: Exception) {}
        }
    }

    @Test
    fun testF10_GraphQLDuplicateRedisDbNumber() {
        val authContext = Mockito.mock(GraphQLAuthContext::class.java) { inv ->
            if (inv.method.name == "validateFieldAccess") AuthorizationResult.allowed()
            else Mockito.RETURNS_DEFAULTS.answer(inv)
        }
        val configStore = Mockito.mock(at.rocworks.stores.IArchiveConfigStore::class.java)
        Mockito.`when`(archiveHandler.getConfigStore()).thenReturn(configStore)
        val resolver = ArchiveGroupResolver(vertx, archiveHandler, authContext)

        // Create with duplicate redisDbNumber 0
        val createFetcher = resolver.createArchiveGroup()
        val envCreate = Mockito.mock(graphql.schema.DataFetchingEnvironment::class.java)
        Mockito.`when`(envCreate.getArgument<Map<String, Any?>>("input")).thenReturn(
            mapOf(
                "name" to "NewGroup",
                "topicFilter" to listOf("new/#"),
                "lastValType" to "MEMORY",
                "archiveType" to "NONE",
                "redisDbNumber" to 0
            )
        )
        val createResult = createFetcher.get(envCreate).get(5, TimeUnit.SECONDS)
        assertEquals(false, createResult["success"])
        assertEquals("RedisDbNumber 0 is already used by archive group [Default]", createResult["message"])
    }

    @Test
    fun testF7_ReadOnlyLastValStore() {
        deployedGroups["Default"] = createArchiveGroup("Default", redisDbNumber = 0, readOnly = true, store = defaultStore)
        startRedisServer()

        RedisClient().use { client ->
            client.sendCommand("SET", "a", "val")
            assertEquals("ERR:ERR last value store of archive group [Default] is read-only", client.readResp())

            client.sendCommand("MSET", "a", "v1", "b", "v2")
            assertEquals("ERR:ERR last value store of archive group [Default] is read-only", client.readResp())

            client.sendCommand("HSET", "a", "f", "v")
            assertEquals("ERR:ERR last value store of archive group [Default] is read-only", client.readResp())

            client.sendCommand("DEL", "a")
            assertEquals("ERR:ERR last value store of archive group [Default] is read-only", client.readResp())

            client.sendCommand("JSON.DEL", "a")
            assertEquals("ERR:ERR last value store of archive group [Default] is read-only", client.readResp())

            // PUBLISH is unaffected by read-only last value store
            client.sendCommand("PUBLISH", "a", "pubval")
            assertEquals(0L, client.readResp())
        }
    }

    @Test
    fun testF8_MsetValidationAtomic() {
        startRedisServer()

        RedisClient().use { client ->
            // Pair 2 has invalid wildcard in topic
            client.sendCommand("MSET", "valid1", "v1", "bad+topic", "v2")
            assertEquals("ERR:ERR topic must not contain wildcard characters", client.readResp())

            // valid1 must not be set
            client.sendCommand("GET", "valid1")
            assertNull(client.readResp())
        }
    }

    @Test
    fun testF9_ProtocolErrors() {
        startRedisServer()

        // 1. Invalid multibulk length
        RedisClient().use { client ->
            client.send("*-1\r\n")
            val resp = client.readResp()
            assertEquals("ERR:ERR Protocol error: invalid multibulk length", resp)
        }

        // 2. Expected '$'
        RedisClient().use { client ->
            client.send("*1\r\n+inline\r\n")
            val resp = client.readResp()
            assertEquals("ERR:ERR Protocol error: expected '$', got '+'", resp)
        }

        // 3. Negative bulk length
        RedisClient().use { client ->
            client.send("*1\r\n$-5\r\n")
            val resp = client.readResp()
            assertEquals("ERR:ERR Protocol error: invalid bulk length", resp)
        }

        // 4. Too big inline request (> 64 KiB)
        RedisClient().use { client ->
            val bigLine = "A".repeat(65537) + "\r\n"
            client.send(bigLine)
            val resp = client.readResp()
            assertEquals("ERR:ERR Protocol error: too big inline request", resp)
        }
    }

    @Test
    fun testF1_PatternConversionAndGlob() {
        startRedisServer()

        RedisClient().use { client ->
            client.sendCommand("SET", "a/1/c", "v1")
            assertEquals("OK", client.readResp())
            client.sendCommand("SET", "a/2/c", "v2")
            assertEquals("OK", client.readResp())
            client.sendCommand("SET", "a/b1", "v3")
            assertEquals("OK", client.readResp())
            client.sendCommand("SET", "sensor/temp", "v4")
            assertEquals("OK", client.readResp())
            client.sendCommand("SET", "a/x/c1", "v5")
            assertEquals("OK", client.readResp())

            // KEYS a/*/c -> matches a/1/c, a/2/c
            client.sendCommand("KEYS", "a/*/c")
            val res1 = (client.readResp() as List<*>).map { it.toString() }.toSet()
            assertEquals(setOf("a/1/c", "a/2/c"), res1)

            // KEYS a/b* -> glob matches a/b1
            client.sendCommand("KEYS", "a/b*")
            val res2 = (client.readResp() as List<*>).map { it.toString() }.toSet()
            assertEquals(setOf("a/b1"), res2)

            // KEYS a/[xy]/c? -> matches a/x/c1
            client.sendCommand("KEYS", "a/[xy]/c?")
            val res3 = (client.readResp() as List<*>).map { it.toString() }.toSet()
            assertEquals(setOf("a/x/c1"), res3)

            // HSCAN with glob MATCH
            client.sendCommand("HSET", "hkey", "fieldA", "1", "fieldB", "2", "num1", "3")
            assertEquals(3L, client.readResp())

            client.sendCommand("HSCAN", "hkey", "0", "MATCH", "field[A-Z]")
            val hscanRes = client.readResp() as List<*>
            assertEquals("0", hscanRes[0])
            val fields = (hscanRes[1] as List<*>).map { it.toString() }
            assertTrue(fields.contains("fieldA"))
            assertTrue(fields.contains("fieldB"))
            assertFalse(fields.contains("num1"))
        }
    }

    @Test
    fun testF2_ScanCountHint() {
        startRedisServer(keysLimit = 50)

        RedisClient().use { client ->
            for (i in 1..10) {
                client.sendCommand("SET", "k$i", "v$i")
                assertEquals("OK", client.readResp())
            }

            // SCAN with COUNT 2 is a hint, not a cap; returns all 10 keys with cursor 0
            client.sendCommand("SCAN", "0", "COUNT", "2")
            val resp = client.readResp() as List<*>
            assertEquals("0", resp[0])
            val keys = (resp[1] as List<*>).map { it.toString() }
            assertEquals(10, keys.size)
        }
    }

    @Test
    fun testF3_F4_SubscribeUnsubscribe() {
        startRedisServer()

        RedisClient().use { client ->
            // SUBSCRIBE c1 c2
            client.sendCommand("SUBSCRIBE", "c1", "c2")
            assertEquals(listOf("subscribe", "c1", 1L), client.readResp())
            assertEquals(listOf("subscribe", "c2", 2L), client.readResp())

            // Duplicate SUBSCRIBE c1 (F4: repeats count, no duplicate listener)
            client.sendCommand("SUBSCRIBE", "c1")
            assertEquals(listOf("subscribe", "c1", 2L), client.readResp())

            // PSUBSCRIBE p1*
            client.sendCommand("PSUBSCRIBE", "p1*")
            assertEquals(listOf("psubscribe", "p1*", 3L), client.readResp())

            // UNSUBSCRIBE c1 (F3: removes c1, replies with remaining total 2)
            client.sendCommand("UNSUBSCRIBE", "c1")
            assertEquals(listOf("unsubscribe", "c1", 2L), client.readResp())

            // UNSUBSCRIBE c99 (not subscribed -> still replies per name with remaining count 2)
            client.sendCommand("UNSUBSCRIBE", "c99")
            assertEquals(listOf("unsubscribe", "c99", 2L), client.readResp())

            // PUNSUBSCRIBE without args -> removes all pattern subscriptions
            client.sendCommand("PUNSUBSCRIBE")
            assertEquals(listOf("punsubscribe", "p1*", 1L), client.readResp())

            // UNSUBSCRIBE without args -> removes all channel subscriptions
            client.sendCommand("UNSUBSCRIBE")
            assertEquals(listOf("unsubscribe", "c2", 0L), client.readResp())

            // UNSUBSCRIBE without args again -> nothing to remove -> [unsubscribe, null, 0]
            client.sendCommand("UNSUBSCRIBE")
            assertEquals(listOf("unsubscribe", null, 0L), client.readResp())
        }
    }

    @Test
    fun testF5_F6_PipelinedReplyOrderAndSerialization() {
        startRedisServer()

        RedisClient().use { client ->
            // Send pipelined commands
            val sb = StringBuilder()
            for (i in 1..5) {
                sb.append("*4\r\n$4\r\nHSET\r\n$4\r\nhkey\r\n$2\r\nf$i\r\n$2\r\nv$i\r\n")
            }
            sb.append("*2\r\n$7\r\nHGETALL\r\n$4\r\nhkey\r\n")
            client.send(sb.toString())

            // Verify each HSET reply received in exact order
            for (i in 1..5) {
                assertEquals(1L, client.readResp())
            }

            // HGETALL must receive all 5 fields without lost updates
            val all = client.readResp() as List<*>
            assertEquals(10, all.size)
        }
    }

    @Test
    fun testF11_F12_CountersAndDeliveryAcl() {
        startRedisServer(requireAuth = true)

        // Admin connection
        RedisClient().use { adminClient ->
            adminClient.sendCommand("AUTH", "admin", "secret")
            assertEquals("OK", adminClient.readResp())

            // User connection
            RedisClient().use { userClient ->
                userClient.sendCommand("AUTH", "alice", "secret")
                assertEquals("OK", userClient.readResp())

                // alice subscribes to allowed/#
                userClient.sendCommand("PSUBSCRIBE", "allowed/#")
                assertEquals(listOf("psubscribe", "allowed/#", 1L), userClient.readResp())

                val initialIn = messagesInCount.get()
                val initialOut = messagesOutCount.get()

                // Admin publishes to allowed/public
                adminClient.sendCommand("PUBLISH", "allowed/public", "msg1")
                assertEquals(0L, adminClient.readResp())

                // alice should receive it
                val delivered1 = userClient.readResp() as List<*>
                assertEquals("pmessage", delivered1[0])
                assertEquals("allowed/#", delivered1[1])
                assertEquals("allowed/public", delivered1[2])
                assertEquals("msg1", delivered1[3])

                assertEquals(initialIn + 1, messagesInCount.get())
                assertEquals(initialOut + 1, messagesOutCount.get())

                // Admin publishes to allowed/secret (denied by ACL for alice)
                adminClient.sendCommand("PUBLISH", "allowed/secret", "secret_msg")
                assertEquals(0L, adminClient.readResp())

                // In counter incremented on publish
                assertEquals(initialIn + 2, messagesInCount.get())

                // Send another message to allowed/public so we can verify secret_msg was dropped
                adminClient.sendCommand("PUBLISH", "allowed/public", "msg2")
                assertEquals(0L, adminClient.readResp())

                val delivered2 = userClient.readResp() as List<*>
                assertEquals("pmessage", delivered2[0])
                assertEquals("allowed/public", delivered2[2])
                assertEquals("msg2", delivered2[3])

                // Out counter was only incremented for msg2, NOT for secret_msg (F12)
                assertEquals(initialOut + 2, messagesOutCount.get())
            }
        }
    }
}
