package at.rocworks.extensions

import at.rocworks.Utils
import at.rocworks.auth.UserManager
import at.rocworks.data.BrokerMessage
import at.rocworks.data.TopicTree
import at.rocworks.handlers.ArchiveGroup
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.handlers.SessionHandler
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.net.NetServerOptions
import io.vertx.core.net.NetSocket
import java.nio.charset.StandardCharsets
import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicLong

class RedisProtocolException(message: String) : Exception(message)

class RedisServer(
    private val config: JsonObject,
    private val archiveHandler: ArchiveHandler,
    private val sessionHandler: SessionHandler,
    private val userManager: UserManager
) : AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    private val redisConfig = config.getJsonObject("RedisServer", JsonObject())
    private val host = redisConfig.getString("Host", "0.0.0.0")
    private val port = redisConfig.getInteger("Port", 6379)
    private val requireAuth = redisConfig.getBoolean("RequireAuth", true)
    private val defaultDb = redisConfig.getInteger("DefaultDb", 0)
    private val setRetained = redisConfig.getBoolean("SetRetained", false)
    private val publishQos = redisConfig.getInteger("PublishQos", 0).coerceIn(0, 2)
    private val keysLimit = redisConfig.getInteger("KeysLimit", 10000).coerceAtLeast(1)

    private val connectionSequence = AtomicLong()
    private val keyChains = HashMap<String, Future<Void>>()

    companion object {
        private const val REDIS_TYPE_FIELD = "type"
        private const val REDIS_VALUE_FIELD = "value"
        private const val WRONG_TYPE = "WRONGTYPE Operation against a key holding the wrong kind of value"
    }

    private fun <T> withKeyLocks(groupName: String, topics: List<String>, block: () -> Future<T>): Future<T> {
        val sortedKeys = topics.distinct().sorted().map { "$groupName\u0000$it" }
        if (sortedKeys.isEmpty()) {
            return block()
        }
        val currentFutures = sortedKeys.map { lockKey ->
            keyChains[lockKey] ?: Future.succeededFuture<Void>()
        }
        val allPrior = Future.join(currentFutures as List<Future<*>>)
        val resultPromise = Promise.promise<T>()
        val newChain = Promise.promise<Void>()

        sortedKeys.forEach { lockKey ->
            keyChains[lockKey] = newChain.future()
        }

        allPrior.onComplete {
            val future = try {
                block()
            } catch (e: Throwable) {
                Future.failedFuture(e)
            }
            future.onComplete { res ->
                newChain.complete()
                sortedKeys.forEach { lockKey ->
                    if (keyChains[lockKey] == newChain.future()) {
                        keyChains.remove(lockKey)
                    }
                }
                if (res.succeeded()) {
                    resultPromise.complete(res.result())
                } else {
                    resultPromise.fail(res.cause())
                }
            }
        }
        return resultPromise.future()
    }

    override fun start(startPromise: Promise<Void>) {
        val mappedGroups = archiveHandler.getDeployedArchiveGroups().values.mapNotNull { group ->
            group.getRedisDbNumber()?.let { it to group.name }
        }
        val duplicateDbs = mappedGroups.groupBy { it.first }.filter { it.value.size > 1 }
        if (duplicateDbs.isNotEmpty()) {
            startPromise.fail("Duplicate RedisDbNumber mapping: ${duplicateDbs.mapValues { it.value.map { pair -> pair.second } }}")
            return
        }

        val server = vertx.createNetServer(NetServerOptions().setHost(host).setPort(port).setTcpNoDelay(true))
        server.connectHandler { socket ->
            RedisConnection(connectionSequence.incrementAndGet(), socket).start()
        }
        server.listen().onComplete { result ->
            if (result.succeeded()) {
                logger.info("Redis protocol server listening on $host:$port")
                startPromise.complete()
            } else {
                logger.severe("Failed to start Redis protocol server on $host:$port: ${result.cause()?.message}")
                startPromise.fail(result.cause())
            }
        }
    }

    private data class SubKey(val isPattern: Boolean, val name: String)

    private inner class RedisConnection(
        private val id: Long,
        private val socket: NetSocket
    ) {
        private val parser = RespParser()
        private val subscriptions = mutableMapOf<SubKey, String>()
        private val subscriptionSequence = AtomicLong()
        private var closed = false
        private var username: String? = null
        private var authenticated = !requireAuth || !userManager.isUserManagementEnabled()
        private var selectedDb = defaultDb
        private var clientName: String? = null

        private val clientId = "redis-$id"
        private var sessionRegistered = false
        private var commandConsumer: io.vertx.core.eventbus.MessageConsumer<JsonObject>? = null

        private val commandQueue = java.util.ArrayDeque<RespCommand>()
        private var processingCommand = false

        private fun queueCommand(command: RespCommand) {
            commandQueue.add(command)
            processQueue()
        }

        private fun processQueue() {
            if (processingCommand || closed) return
            val command = commandQueue.poll() ?: return
            processingCommand = true

            val promise = Promise.promise<Void>()
            promise.future().onComplete {
                processingCommand = false
                processQueue()
            }

            handle(command, promise)
        }

        private fun registerSession() {
            if (sessionRegistered) return
            sessionRegistered = true

            val information = JsonObject()
            information.put("RemoteAddress", socket.remoteAddress()?.toString() ?: "")
            information.put("LocalAddress", socket.localAddress()?.toString() ?: "")
            information.put("ProtocolVersion", "REDIS")
            information.put("SSL", false)
            username?.let { information.put("User", it) }
            clientName?.let { information.put("ClientName", it) }
            information.put("SelectedDb", selectedDb)

            sessionHandler.setClient(clientId, true, information).onComplete { ar ->
                if (ar.succeeded()) {
                    sessionHandler.onlineClient(clientId)
                }
            }
        }

        private fun updateSessionInfo() {
            if (!sessionRegistered) return
            val information = JsonObject()
            information.put("RemoteAddress", socket.remoteAddress()?.toString() ?: "")
            information.put("LocalAddress", socket.localAddress()?.toString() ?: "")
            information.put("ProtocolVersion", "REDIS")
            information.put("SSL", false)
            username?.let { information.put("User", it) }
            clientName?.let { information.put("ClientName", it) }
            information.put("SelectedDb", selectedDb)

            sessionHandler.setClient(clientId, true, information)
        }

        fun start() {
            socket.handler { buffer ->
                parser.append(buffer)
                try {
                    while (!closed) {
                        val command = parser.next() ?: break
                        queueCommand(command)
                    }
                } catch (e: RedisProtocolException) {
                    writeError(e.message ?: "ERR Protocol error").onComplete {
                        close()
                    }
                } catch (e: Exception) {
                    writeError("ERR Protocol error: ${e.message ?: "unknown"}").onComplete {
                        close()
                    }
                }
            }
            socket.closeHandler { cleanup() }
            socket.exceptionHandler {
                logger.fine("Redis client [$id] socket error: ${it.message}")
                close()
            }

            if (authenticated) {
                registerSession()
            }

            commandConsumer = vertx.eventBus().consumer<JsonObject>(at.rocworks.bus.EventBusAddresses.Client.commands(clientId)) { msg ->
                val command = msg.body()
                if (command.getString(at.rocworks.Const.COMMAND_KEY) == at.rocworks.Const.COMMAND_DISCONNECT) {
                    logger.info("Redis client [$clientId] disconnect command received via EventBus")
                    close()
                    msg.reply(JsonObject().put("Connected", false))
                }
            }
        }

        private fun handle(command: RespCommand, promise: Promise<Void>) {
            if (command.args.isEmpty()) {
                writeError("ERR empty command").onComplete { promise.complete() }
                return
            }

            val name = command.stringArg(0).uppercase()
            val argsStr = if (name == "AUTH") {
                if (command.args.size == 2) "[REDACTED_PASSWORD]"
                else "${command.stringArg(1)} [REDACTED_PASSWORD]"
            } else if (name == "HELLO") {
                val authIndex = command.args.indices.firstOrNull { command.stringArg(it).equals("AUTH", ignoreCase = true) }
                if (authIndex != null && authIndex + 2 < command.args.size) {
                    val authUser = command.stringArg(authIndex + 1)
                    "AUTH $authUser [REDACTED_PASSWORD]"
                } else {
                    (1 until command.args.size).map { command.stringArg(it) }.joinToString(" ")
                }
            } else {
                (1 until command.args.size).map { command.stringArg(it) }.joinToString(" ")
            }

            logger.fine("Redis client [$clientId] executing command: $name $argsStr")

            val allowedBeforeAuth = setOf("AUTH", "HELLO", "PING", "QUIT", "CLIENT")
            if (!authenticated && name !in allowedBeforeAuth) {
                writeError("NOAUTH Authentication required.").onComplete {
                    promise.complete()
                }
                return
            }

            if (name == "AUTH") {
                handleAuth(command, promise)
                return
            }
            if (name == "HELLO") {
                handleHello(command, promise)
                return
            }

            try {
                val future: Future<Void> = when (name) {
                    "PING" -> handlePing(command)
                    "ECHO" -> handleEcho(command)
                    "QUIT" -> handleQuit()
                    "SELECT" -> handleSelect(command)
                    "CLIENT" -> handleClient(command)
                    "COMMAND" -> handleCommand(command)
                    "INFO" -> handleInfo()
                    "GET" -> handleGet(command)
                    "GETRANGE", "SUBSTR" -> handleGetRange(command)
                    "MGET" -> handleMget(command)
                    "SET" -> handleSet(command)
                    "MSET" -> handleMset(command)
                    "HSET" -> handleHset(command)
                    "HGET" -> handleHget(command)
                    "HMGET" -> handleHmget(command)
                    "HGETALL" -> handleHgetall(command)
                    "HDEL" -> handleHdel(command)
                    "HEXISTS" -> handleHexists(command)
                    "HLEN" -> handleHlen(command)
                    "HKEYS" -> handleHkeys(command)
                    "HVALS" -> handleHvals(command)
                    "HSCAN" -> handleHscan(command)
                    "SADD" -> handleSadd(command)
                    "SREM" -> handleSrem(command)
                    "SMEMBERS" -> handleSmembers(command)
                    "SISMEMBER" -> handleSismember(command)
                    "SCARD" -> handleScard(command)
                    "LPUSH" -> handleListPush(command, left = true)
                    "RPUSH" -> handleListPush(command, left = false)
                    "LPOP" -> handleListPop(command, left = true)
                    "RPOP" -> handleListPop(command, left = false)
                    "LRANGE" -> handleLrange(command)
                    "LLEN" -> handleLlen(command)
                    "LINDEX" -> handleLindex(command)
                    "ZADD" -> handleZadd(command)
                    "ZRANGE" -> handleZrange(command)
                    "ZREM" -> handleZrem(command)
                    "ZSCORE" -> handleZscore(command)
                    "ZCARD" -> handleZcard(command)
                    "JSON.SET" -> handleJsonSet(command)
                    "JSON.GET" -> handleJsonGet(command)
                    "JSON.DEL" -> handleJsonDel(command)
                    "JSON.TYPE" -> handleJsonType(command)
                    "DEL" -> handleDel(command)
                    "EXISTS" -> handleExists(command)
                    "TTL", "PTTL" -> handleTtl(command)
                    "SCAN" -> handleScan(command)
                    "KEYS" -> handleKeys(command)
                    "PUBLISH" -> handlePublish(command)
                    "SUBSCRIBE" -> handleSubscribe(command, pattern = false)
                    "PSUBSCRIBE" -> handleSubscribe(command, pattern = true)
                    "UNSUBSCRIBE", "PUNSUBSCRIBE" -> handleUnsubscribe(command)
                    "TYPE" -> handleType(command)
                    else -> writeError("ERR unknown command '$name'")
                }
                future.onComplete {
                    promise.complete()
                }
            } catch (e: Exception) {
                logger.warning("Redis command [$name] failed: ${e.message}")
                writeError("ERR ${e.message ?: "command failed"}").onComplete {
                    promise.complete()
                }
            }
        }

        private fun handlePing(command: RespCommand): Future<Void> {
            return if (command.args.size > 2) {
                writeError("ERR wrong number of arguments for 'ping' command")
            } else if (command.args.size == 2) {
                writeBulk(command.args[1])
            } else {
                writeSimple("PONG")
            }
        }

        private fun handleEcho(command: RespCommand): Future<Void> {
            return if (command.args.size == 2) {
                writeBulk(command.args[1])
            } else {
                writeError("ERR wrong number of arguments for 'echo' command")
            }
        }

        private fun handleQuit(): Future<Void> {
            val future = writeSimple("OK")
            close()
            return future
        }

        private fun handleAuth(command: RespCommand, promise: Promise<Void>) {
            val credentials = when (command.args.size) {
                2 -> "default" to command.stringArg(1)
                3 -> command.stringArg(1) to command.stringArg(2)
                else -> {
                    writeError("ERR wrong number of arguments for 'auth' command").onComplete { promise.complete() }
                    return
                }
            }

            logger.info("Redis client [$clientId] attempting to authenticate as user [${credentials.first}]")

            userManager.authenticate(credentials.first, credentials.second).onComplete { result ->
                if (result.succeeded()) {
                    val user = result.result()
                    if (user != null) {
                        if (user.enabled) {
                            logger.info("Redis client [$clientId] successfully authenticated as user [${user.username}]")
                            username = user.username
                            authenticated = true
                            registerSession()
                            writeSimple("OK").onComplete { promise.complete() }
                        } else {
                            logger.warning("Redis client [$clientId] authentication failed: user [${user.username}] is disabled")
                            writeError("WRONGPASS invalid username-password pair or user is disabled.").onComplete { promise.complete() }
                        }
                    } else {
                        logger.warning("Redis client [$clientId] authentication failed: invalid credentials for user [${credentials.first}]")
                        if (credentials.first == "default") {
                            logger.info("Redis client [$clientId] attempting fallback authentication as default 'Admin' user")
                            userManager.authenticate("Admin", credentials.second).onComplete { fallbackResult ->
                                if (fallbackResult.succeeded()) {
                                    val fallbackUser = fallbackResult.result()
                                    if (fallbackUser != null) {
                                        if (fallbackUser.enabled) {
                                            logger.info("Redis client [$clientId] successfully authenticated via fallback as user [${fallbackUser.username}]")
                                            username = fallbackUser.username
                                            authenticated = true
                                            registerSession()
                                            writeSimple("OK").onComplete { promise.complete() }
                                        } else {
                                            logger.warning("Redis client [$clientId] fallback authentication failed: user [${fallbackUser.username}] is disabled")
                                            writeError("WRONGPASS invalid username-password pair or user is disabled.").onComplete { promise.complete() }
                                        }
                                    } else {
                                        logger.warning("Redis client [$clientId] fallback authentication failed: invalid credentials for user [Admin]")
                                        writeError("WRONGPASS invalid username-password pair or user is disabled.").onComplete { promise.complete() }
                                    }
                                } else {
                                    logger.severe("Redis client [$clientId] fallback authentication failed with error: ${fallbackResult.cause()?.message}")
                                    writeError("WRONGPASS invalid username-password pair or user is disabled.").onComplete { promise.complete() }
                                }
                            }
                        } else {
                            writeError("WRONGPASS invalid username-password pair or user is disabled.").onComplete { promise.complete() }
                        }
                    }
                } else {
                    logger.severe("Redis client [$clientId] authentication failed with error: ${result.cause()?.message}")
                    writeError("WRONGPASS invalid username-password pair or user is disabled.").onComplete { promise.complete() }
                }
            }
        }

        private fun handleHello(command: RespCommand, promise: Promise<Void>) {
            if (command.args.size >= 2) {
                val proto = command.stringArg(1).toIntOrNull()
                if (proto != null && proto > 2) {
                    writeError("NOPROTO sorry, this protocol version is not supported.").onComplete { promise.complete() }
                    return
                }
            }

            val authIndex = command.args.indices.firstOrNull { command.stringArg(it).equals("AUTH", ignoreCase = true) }
            if (authIndex != null && authIndex + 2 < command.args.size) {
                val authUser = command.stringArg(authIndex + 1)
                val authPass = command.stringArg(authIndex + 2)
                logger.info("Redis client [$clientId] HELLO attempting to authenticate as user [$authUser]")
                userManager.authenticate(authUser, authPass).onComplete { result ->
                    if (result.succeeded()) {
                        val user = result.result()
                        if (user != null) {
                            if (user.enabled) {
                                logger.info("Redis client [$clientId] HELLO successfully authenticated as user [${user.username}]")
                                username = user.username
                                authenticated = true
                                registerSession()
                                writeHello().onComplete { promise.complete() }
                            } else {
                                logger.warning("Redis client [$clientId] HELLO authentication failed: user [${user.username}] is disabled")
                                writeError("WRONGPASS invalid username-password pair or user is disabled.").onComplete { promise.complete() }
                            }
                        } else {
                            logger.warning("Redis client [$clientId] HELLO authentication failed: invalid credentials for user [$authUser]")
                            if (authUser == "default") {
                                logger.info("Redis client [$clientId] HELLO attempting fallback authentication as default 'Admin' user")
                                userManager.authenticate("Admin", authPass).onComplete { fallbackResult ->
                                    if (fallbackResult.succeeded()) {
                                        val fallbackUser = fallbackResult.result()
                                        if (fallbackUser != null) {
                                            if (fallbackUser.enabled) {
                                                logger.info("Redis client [$clientId] HELLO successfully authenticated via fallback as user [${fallbackUser.username}]")
                                                username = fallbackUser.username
                                                authenticated = true
                                                registerSession()
                                                writeHello().onComplete { promise.complete() }
                                            } else {
                                                logger.warning("Redis client [$clientId] HELLO fallback authentication failed: user [${fallbackUser.username}] is disabled")
                                                writeError("WRONGPASS invalid username-password pair or user is disabled.").onComplete { promise.complete() }
                                            }
                                        } else {
                                            logger.warning("Redis client [$clientId] HELLO fallback authentication failed: invalid credentials for user [Admin]")
                                            writeError("WRONGPASS invalid username-password pair or user is disabled.").onComplete { promise.complete() }
                                        }
                                    } else {
                                        logger.severe("Redis client [$clientId] HELLO fallback authentication failed with error: ${fallbackResult.cause()?.message}")
                                        writeError("WRONGPASS invalid username-password pair or user is disabled.").onComplete { promise.complete() }
                                    }
                                }
                            } else {
                                writeError("WRONGPASS invalid username-password pair or user is disabled.").onComplete { promise.complete() }
                            }
                        }
                    } else {
                        logger.severe("Redis client [$clientId] HELLO authentication failed with error: ${result.cause()?.message}")
                        writeError("WRONGPASS invalid username-password pair or user is disabled.").onComplete { promise.complete() }
                    }
                }
            } else {
                if (authenticated) {
                    registerSession()
                }
                writeHello().onComplete { promise.complete() }
            }
        }

        private fun writeHello(): Future<Void> {
            return writeArray(
                listOf(
                    RespValue.bulk("server"),
                    RespValue.bulk("monstermq"),
                    RespValue.bulk("version"),
                    RespValue.bulk(at.rocworks.Version.getVersion()),
                    RespValue.bulk("proto"),
                    RespValue.integer(2),
                    RespValue.bulk("id"),
                    RespValue.integer(id),
                    RespValue.bulk("mode"),
                    RespValue.bulk("standalone"),
                    RespValue.bulk("role"),
                    RespValue.bulk("master"),
                    RespValue.bulk("modules"),
                    RespValue.array(emptyList())
                )
            )
        }

        private fun handleSelect(command: RespCommand): Future<Void> {
            if (command.args.size != 2) {
                return writeError("ERR wrong number of arguments for 'select' command")
            }
            val db = command.stringArg(1).toIntOrNull()
            if (db == null || db < 0) {
                return writeError("ERR invalid DB index")
            }
            if (archiveGroupForDb(db) == null) {
                return writeError("ERR DB index is not mapped to an archive group")
            }
            selectedDb = db
            updateSessionInfo()
            return writeSimple("OK")
        }

        private fun handleClient(command: RespCommand): Future<Void> {
            if (command.args.size < 2) {
                return writeError("ERR wrong number of arguments for 'client' command")
            }
            return when (command.stringArg(1).uppercase()) {
                "SETNAME" -> {
                    clientName = command.args.getOrNull(2)?.toString(StandardCharsets.UTF_8)
                    updateSessionInfo()
                    writeSimple("OK")
                }
                "GETNAME" -> writeBulk(clientName?.toByteArray(StandardCharsets.UTF_8))
                "SETINFO" -> writeSimple("OK")
                "ID" -> writeInteger(id)
                else -> writeError("ERR unsupported CLIENT subcommand")
            }
        }

        private fun handleCommand(command: RespCommand): Future<Void> {
            return if (command.args.size >= 2 && command.stringArg(1).equals("COUNT", ignoreCase = true)) {
                writeInteger(30)
            } else if (command.args.size >= 2 && command.stringArg(1).equals("INFO", ignoreCase = true)) {
                writeArray(command.args.drop(2).map { RespValue.nullBulk() })
            } else {
                writeArray(emptyList())
            }
        }

        private fun handleInfo(): Future<Void> {
            val groups = archiveHandler.getDeployedArchiveGroups().values.mapNotNull { it.getRedisDbNumber() }.sorted()
            val body = buildString {
                append("# Server\r\n")
                append("redis_version:7.0.0\r\n")
                append("monstermq_version:${at.rocworks.Version.getVersion()}\r\n")
                append("redis_mode:standalone\r\n")
                append("# Clients\r\n")
                append("connected_clients:1\r\n")
                append("# Keyspace\r\n")
                groups.forEach { append("db$it:keys=0,expires=0,avg_ttl=0\r\n") }
            }
            return writeBulk(body.toByteArray(StandardCharsets.UTF_8))
        }

        private fun handleGet(command: RespCommand): Future<Void> {
            if (command.args.size != 2) {
                return writeError("ERR wrong number of arguments for 'get' command")
            }
            val topic = command.stringArg(1)
            if (!canRead(topic)) {
                return writeError("NOPERM no permission to read topic")
            }
            val group = selectedArchiveGroup() ?: return Future.succeededFuture()
            val store = group.lastValStore ?: return writeNullBulk()
            return vertx.executeBlocking(Callable { store[topic] }).compose(
                { it -> writeBulk(it?.payload) },
                { err -> writeError("ERR ${err.message ?: "get failed"}") }
            )
        }

        private fun handleGetRange(command: RespCommand): Future<Void> {
            if (command.args.size != 4) {
                return writeError("ERR wrong number of arguments for '${command.stringArg(0).lowercase()}' command")
            }
            val topic = command.stringArg(1)
            val start = command.stringArg(2).toLongOrNull()
            val end = command.stringArg(3).toLongOrNull()
            if (start == null || end == null) {
                return writeError("ERR value is not an integer or out of range")
            }
            if (!canRead(topic)) {
                return writeError("NOPERM no permission to read topic")
            }
            val group = selectedArchiveGroup() ?: return Future.succeededFuture()
            val store = group.lastValStore ?: return writeBulk(ByteArray(0))
            return vertx.executeBlocking(Callable { store[topic]?.payload }).compose(
                { payload -> writeBulk(sliceRange(payload, start, end)) },
                { err -> writeError("ERR ${err.message ?: "getrange failed"}") }
            )
        }

        private fun handleMget(command: RespCommand): Future<Void> {
            if (command.args.size < 2) {
                return writeError("ERR wrong number of arguments for 'mget' command")
            }
            val topics = command.args.drop(1).map { it.toString(StandardCharsets.UTF_8) }
            if (topics.any { !canRead(it) }) {
                return writeError("NOPERM no permission to read one or more topics")
            }
            val group = selectedArchiveGroup() ?: return Future.succeededFuture()
            val store = group.lastValStore ?: return writeArray(topics.map { RespValue.nullBulk() })
            return vertx.executeBlocking(Callable {
                topics.map { topic -> store[topic]?.payload }
            }).compose(
                { result -> writeArray(result.map { RespValue.bulk(it) }) },
                { err -> writeError("ERR ${err.message ?: "mget failed"}") }
            )
        }

        private fun handleSet(command: RespCommand): Future<Void> {
            if (command.args.size < 3) {
                return writeError("ERR wrong number of arguments for 'set' command")
            }
            val topic = command.stringArg(1)
            val payload = command.args[2]
            val error = validateWrite(topic, isPublish = false)
            if (error != null) {
                return writeError(error)
            }
            val group = selectedArchiveGroup() ?: return Future.succeededFuture()
            return withKeyLocks(group.name, listOf(topic)) {
                val store = group.lastValStore
                val updateStoreFuture = if (store != null) {
                    vertx.executeBlocking(Callable {
                        val brokerMsg = BrokerMessage(
                            messageId = 0,
                            topicName = topic,
                            payload = payload,
                            qosLevel = publishQos,
                            isRetain = setRetained,
                            isDup = false,
                            isQueued = false,
                            clientId = clientId,
                            senderId = clientId
                        )
                        store.addAll(listOf(brokerMsg))
                    })
                } else {
                    Future.succeededFuture()
                }
                updateStoreFuture.compose {
                    publishTopicUnchecked(topic, payload)
                    writeSimple("OK")
                }.recover { err ->
                    writeError("ERR ${err.message ?: "set failed"}")
                }
            }
        }

        private fun handleMset(command: RespCommand): Future<Void> {
            if (command.args.size < 3 || command.args.size % 2 == 0) {
                return writeError("ERR wrong number of arguments for 'mset' command")
            }
            val group = selectedArchiveGroup() ?: return Future.succeededFuture()
            val pairs = mutableListOf<Pair<String, ByteArray>>()
            var i = 1
            while (i < command.args.size) {
                val topic = command.stringArg(i)
                val payload = command.args[i + 1]
                val error = validateWrite(topic, isPublish = false)
                if (error != null) {
                    return writeError(error)
                }
                pairs.add(topic to payload)
                i += 2
            }
            val topics = pairs.map { it.first }
            return withKeyLocks(group.name, topics) {
                val store = group.lastValStore
                val updateStoreFuture = if (store != null) {
                    vertx.executeBlocking(Callable {
                        val msgs = pairs.map { (t, p) ->
                            BrokerMessage(
                                messageId = 0,
                                topicName = t,
                                payload = p,
                                qosLevel = publishQos,
                                isRetain = setRetained,
                                isDup = false,
                                isQueued = false,
                                clientId = clientId,
                                senderId = clientId
                            )
                        }
                        store.addAll(msgs)
                    })
                } else {
                    Future.succeededFuture()
                }
                updateStoreFuture.compose {
                    pairs.forEach { (t, p) ->
                        publishTopicUnchecked(t, p)
                    }
                    writeSimple("OK")
                }.recover { err ->
                    writeError("ERR ${err.message ?: "mset failed"}")
                }
            }
        }

        private fun handleHset(command: RespCommand): Future<Void> {
            if (command.args.size < 4 || command.args.size % 2 != 0) {
                return writeError("ERR wrong number of arguments for 'hset' command")
            }
            return mutateTypedValue(command.stringArg(1), "hash", JsonObject()) { current ->
                val hash = current as JsonObject
                var added = 0L
                var i = 2
                while (i < command.args.size) {
                    val field = command.stringArg(i)
                    if (!hash.containsKey(field)) added++
                    hash.put(field, command.stringArg(i + 1))
                    i += 2
                }
                hash to RespValue.integer(added)
            }
        }

        private fun handleHget(command: RespCommand): Future<Void> {
            if (command.args.size != 3) {
                return writeError("ERR wrong number of arguments for 'hget' command")
            }
            return readTypedValue(command.stringArg(1), "hash") { value ->
                val hash = value as JsonObject
                writeBulk(hash.getString(command.stringArg(2))?.toByteArray(StandardCharsets.UTF_8))
            }
        }

        private fun handleHmget(command: RespCommand): Future<Void> {
            if (command.args.size < 3) {
                return writeError("ERR wrong number of arguments for 'hmget' command")
            }
            return readTypedValue(command.stringArg(1), "hash") { value ->
                val hash = value as JsonObject
                writeArray(command.args.drop(2).map { field ->
                    RespValue.bulk(hash.getString(field.toString(StandardCharsets.UTF_8))?.toByteArray(StandardCharsets.UTF_8))
                })
            }
        }

        private fun handleHgetall(command: RespCommand): Future<Void> {
            if (command.args.size != 2) {
                return writeError("ERR wrong number of arguments for 'hgetall' command")
            }
            return readTypedValue(command.stringArg(1), "hash") { value ->
                val hash = value as JsonObject
                writeArray(hash.fieldNames().flatMap { field ->
                    listOf(RespValue.bulk(field), RespValue.bulk(hash.getString(field, "")))
                })
            }
        }

        private fun handleHdel(command: RespCommand): Future<Void> {
            if (command.args.size < 3) {
                return writeError("ERR wrong number of arguments for 'hdel' command")
            }
            return mutateTypedValue(command.stringArg(1), "hash", JsonObject()) { current ->
                val hash = current as JsonObject
                var removed = 0L
                command.args.drop(2).forEach { fieldBytes ->
                    val field = fieldBytes.toString(StandardCharsets.UTF_8)
                    if (hash.containsKey(field)) {
                        hash.remove(field)
                        removed++
                    }
                }
                hash to RespValue.integer(removed)
            }
        }

        private fun handleHexists(command: RespCommand): Future<Void> {
            if (command.args.size != 3) {
                return writeError("ERR wrong number of arguments for 'hexists' command")
            }
            return readTypedValue(command.stringArg(1), "hash") { value ->
                writeInteger(if ((value as JsonObject).containsKey(command.stringArg(2))) 1 else 0)
            }
        }

        private fun handleHlen(command: RespCommand): Future<Void> {
            if (command.args.size != 2) {
                return writeError("ERR wrong number of arguments for 'hlen' command")
            }
            return readTypedValue(command.stringArg(1), "hash") { value -> writeInteger((value as JsonObject).size().toLong()) }
        }

        private fun handleHkeys(command: RespCommand): Future<Void> {
            if (command.args.size != 2) {
                return writeError("ERR wrong number of arguments for 'hkeys' command")
            }
            return readTypedValue(command.stringArg(1), "hash") { value ->
                writeArray((value as JsonObject).fieldNames().map { RespValue.bulk(it) })
            }
        }

        private fun handleHvals(command: RespCommand): Future<Void> {
            if (command.args.size != 2) {
                return writeError("ERR wrong number of arguments for 'hvals' command")
            }
            return readTypedValue(command.stringArg(1), "hash") { value ->
                val hash = value as JsonObject
                writeArray(hash.fieldNames().map { RespValue.bulk(hash.getString(it, "")) })
            }
        }

        private fun handleHscan(command: RespCommand): Future<Void> {
            if (command.args.size < 3) {
                return writeError("ERR wrong number of arguments for 'hscan' command")
            }
            val cursor = command.stringArg(2).toLongOrNull()
            if (cursor == null || cursor < 0) {
                return writeError("ERR invalid cursor")
            }
            val match = hashScanOption(command, "MATCH")
            val count = hashScanOption(command, "COUNT")?.toIntOrNull()?.coerceAtLeast(1) ?: Int.MAX_VALUE

            return readTypedValue(command.stringArg(1), "hash") { value ->
                val hash = value as JsonObject
                val fields = hash.fieldNames()
                    .asSequence()
                    .filter { field -> match == null || globMatches(match, field) }
                    .drop(cursor.toInt())
                    .take(count)
                    .toList()

                val nextCursor = if (cursor.toInt() + fields.size >= hash.fieldNames().count { match == null || globMatches(match, it) }) "0"
                    else (cursor.toInt() + fields.size).toString()

                writeArray(listOf(
                    RespValue.bulk(nextCursor),
                    RespValue.array(fields.flatMap { field ->
                        listOf(RespValue.bulk(field), RespValue.bulk(hash.getString(field, "")))
                    })
                ))
            }
        }

        private fun handleSadd(command: RespCommand): Future<Void> {
            if (command.args.size < 3) {
                return writeError("ERR wrong number of arguments for 'sadd' command")
            }
            return mutateTypedValue(command.stringArg(1), "set", JsonArray()) { current ->
                val set = jsonArrayToLinkedSet(current as JsonArray)
                var added = 0L
                command.args.drop(2).forEach { memberBytes ->
                    if (set.add(memberBytes.toString(StandardCharsets.UTF_8))) added++
                }
                JsonArray(set.toList()) to RespValue.integer(added)
            }
        }

        private fun handleSrem(command: RespCommand): Future<Void> {
            if (command.args.size < 3) {
                return writeError("ERR wrong number of arguments for 'srem' command")
            }
            return mutateTypedValue(command.stringArg(1), "set", JsonArray()) { current ->
                val set = jsonArrayToLinkedSet(current as JsonArray)
                var removed = 0L
                command.args.drop(2).forEach { memberBytes ->
                    if (set.remove(memberBytes.toString(StandardCharsets.UTF_8))) removed++
                }
                JsonArray(set.toList()) to RespValue.integer(removed)
            }
        }

        private fun handleSmembers(command: RespCommand): Future<Void> {
            if (command.args.size != 2) {
                return writeError("ERR wrong number of arguments for 'smembers' command")
            }
            return readTypedValue(command.stringArg(1), "set") { value ->
                writeArray((value as JsonArray).map { RespValue.bulk(it.toString()) })
            }
        }

        private fun handleSismember(command: RespCommand): Future<Void> {
            if (command.args.size != 3) {
                return writeError("ERR wrong number of arguments for 'sismember' command")
            }
            return readTypedValue(command.stringArg(1), "set") { value ->
                writeInteger(if (jsonArrayToLinkedSet(value as JsonArray).contains(command.stringArg(2))) 1 else 0)
            }
        }

        private fun handleScard(command: RespCommand): Future<Void> {
            if (command.args.size != 2) {
                return writeError("ERR wrong number of arguments for 'scard' command")
            }
            return readTypedValue(command.stringArg(1), "set") { value -> writeInteger((value as JsonArray).size().toLong()) }
        }

        private fun handleListPush(command: RespCommand, left: Boolean): Future<Void> {
            if (command.args.size < 3) {
                return writeError("ERR wrong number of arguments for '${command.stringArg(0).lowercase()}' command")
            }
            return mutateTypedValue(command.stringArg(1), "list", JsonArray()) { current ->
                val values = (current as JsonArray).map { it.toString() }.toMutableList()
                val incoming = command.args.drop(2).map { it.toString(StandardCharsets.UTF_8) }
                if (left) incoming.forEach { values.add(0, it) } else values.addAll(incoming)
                JsonArray(values) to RespValue.integer(values.size.toLong())
            }
        }

        private fun handleListPop(command: RespCommand, left: Boolean): Future<Void> {
            if (command.args.size != 2) {
                return writeError("ERR wrong number of arguments for '${command.stringArg(0).lowercase()}' command")
            }
            return mutateTypedValue(command.stringArg(1), "list", JsonArray()) { current ->
                val values = (current as JsonArray).map { it.toString() }.toMutableList()
                val popped = if (values.isEmpty()) null else if (left) values.removeAt(0) else values.removeAt(values.lastIndex)
                JsonArray(values) to RespValue.bulk(popped?.toByteArray(StandardCharsets.UTF_8))
            }
        }

        private fun handleLrange(command: RespCommand): Future<Void> {
            if (command.args.size != 4) {
                return writeError("ERR wrong number of arguments for 'lrange' command")
            }
            val start = command.stringArg(2).toLongOrNull()
            val end = command.stringArg(3).toLongOrNull()
            if (start == null || end == null) {
                return writeError("ERR value is not an integer or out of range")
            }
            return readTypedValue(command.stringArg(1), "list") { value ->
                val values = (value as JsonArray).map { it.toString() }
                writeArray(sliceList(values, start, end).map { RespValue.bulk(it) })
            }
        }

        private fun handleLlen(command: RespCommand): Future<Void> {
            if (command.args.size != 2) {
                return writeError("ERR wrong number of arguments for 'llen' command")
            }
            return readTypedValue(command.stringArg(1), "list") { value -> writeInteger((value as JsonArray).size().toLong()) }
        }

        private fun handleLindex(command: RespCommand): Future<Void> {
            if (command.args.size != 3) {
                return writeError("ERR wrong number of arguments for 'lindex' command")
            }
            val index = command.stringArg(2).toLongOrNull()
            if (index == null) {
                return writeError("ERR value is not an integer or out of range")
            }
            return readTypedValue(command.stringArg(1), "list") { value ->
                val values = (value as JsonArray).map { it.toString() }
                val normalized = if (index < 0) values.size + index else index
                writeBulk(values.getOrNull(normalized.toInt())?.toByteArray(StandardCharsets.UTF_8))
            }
        }

        private fun handleZadd(command: RespCommand): Future<Void> {
            if (command.args.size < 4 || command.args.size % 2 != 0) {
                return writeError("ERR wrong number of arguments for 'zadd' command")
            }
            var validateIndex = 2
            while (validateIndex < command.args.size) {
                if (command.stringArg(validateIndex).toDoubleOrNull() == null) {
                    return writeError("ERR value is not a valid float")
                }
                validateIndex += 2
            }
            return mutateTypedValue(command.stringArg(1), "zset", JsonObject()) { current ->
                val zset = current as JsonObject
                var added = 0L
                var i = 2
                while (i < command.args.size) {
                    val score = command.stringArg(i).toDouble()
                    val member = command.stringArg(i + 1)
                    if (!zset.containsKey(member)) added++
                    zset.put(member, score)
                    i += 2
                }
                zset to RespValue.integer(added)
            }
        }

        private fun handleZrange(command: RespCommand): Future<Void> {
            if (command.args.size < 4) {
                return writeError("ERR wrong number of arguments for 'zrange' command")
            }
            val start = command.stringArg(2).toLongOrNull()
            val end = command.stringArg(3).toLongOrNull()
            if (start == null || end == null) {
                return writeError("ERR value is not an integer or out of range")
            }
            val withScores = command.args.drop(4).any { it.toString(StandardCharsets.UTF_8).equals("WITHSCORES", ignoreCase = true) }
            return readTypedValue(command.stringArg(1), "zset") { value ->
                val entries = sortedZsetEntries(value as JsonObject)
                val selected = sliceList(entries, start, end)
                val response = if (withScores) {
                    selected.flatMap { listOf(RespValue.bulk(it.first), RespValue.bulk(formatScore(it.second))) }
                } else {
                    selected.map { RespValue.bulk(it.first) }
                }
                writeArray(response)
            }
        }

        private fun handleZrem(command: RespCommand): Future<Void> {
            if (command.args.size < 3) {
                return writeError("ERR wrong number of arguments for 'zrem' command")
            }
            return mutateTypedValue(command.stringArg(1), "zset", JsonObject()) { current ->
                val zset = current as JsonObject
                var removed = 0L
                command.args.drop(2).forEach { memberBytes ->
                    val member = memberBytes.toString(StandardCharsets.UTF_8)
                    if (zset.containsKey(member)) {
                        zset.remove(member)
                        removed++
                    }
                }
                zset to RespValue.integer(removed)
            }
        }

        private fun handleZscore(command: RespCommand): Future<Void> {
            if (command.args.size != 3) {
                return writeError("ERR wrong number of arguments for 'zscore' command")
            }
            return readTypedValue(command.stringArg(1), "zset") { value ->
                val score = (value as JsonObject).getDouble(command.stringArg(2))
                writeBulk(score?.let { formatScore(it).toByteArray(StandardCharsets.UTF_8) })
            }
        }

        private fun handleZcard(command: RespCommand): Future<Void> {
            if (command.args.size != 2) {
                return writeError("ERR wrong number of arguments for 'zcard' command")
            }
            return readTypedValue(command.stringArg(1), "zset") { value -> writeInteger((value as JsonObject).size().toLong()) }
        }

        private fun handleJsonSet(command: RespCommand): Future<Void> {
            if (command.args.size != 4) {
                return writeError("ERR wrong number of arguments for 'json.set' command")
            }
            if (!isRootJsonPath(command.stringArg(2))) {
                return writeError("ERR only root JSON path '$' or '.' is supported")
            }
            val parsed = try {
                Json.decodeValue(command.stringArg(3))
            } catch (e: Exception) {
                return writeError("ERR invalid JSON")
            }
            return mutateTypedValue(command.stringArg(1), "json", JsonObject()) {
                parsed to RespValue.bulk("OK")
            }
        }

        private fun handleJsonGet(command: RespCommand): Future<Void> {
            if (command.args.size !in 2..3) {
                return writeError("ERR wrong number of arguments for 'json.get' command")
            }
            if (command.args.size == 3 && !isRootJsonPath(command.stringArg(2))) {
                return writeError("ERR only root JSON path '$' or '.' is supported")
            }
            val topic = command.stringArg(1)
            if (!canRead(topic)) {
                return writeError("NOPERM no permission to read topic")
            }
            return readStored(topic).compose { stored ->
                when {
                    stored == null -> writeNullBulk()
                    stored.type != "json" -> writeError(WRONG_TYPE)
                    else -> writeBulk(Json.encode(stored.value))
                }
            }
        }

        private fun handleJsonDel(command: RespCommand): Future<Void> {
            if (command.args.size !in 2..3) {
                return writeError("ERR wrong number of arguments for 'json.del' command")
            }
            if (command.args.size == 3 && !isRootJsonPath(command.stringArg(2))) {
                return writeError("ERR only root JSON path '$' or '.' is supported")
            }
            val topic = command.stringArg(1)
            if (!canWrite(topic)) {
                return writeError("NOPERM no permission to delete topic")
            }
            val group = selectedArchiveGroup() ?: return Future.succeededFuture()
            if (group.lastValReadOnly) {
                return writeError("ERR last value store of archive group [${group.name}] is read-only")
            }
            val store = group.lastValStore ?: return writeInteger(0)
            return withKeyLocks(group.name, listOf(topic)) {
                vertx.executeBlocking(Callable {
                    val stored = decodeStoredValue(store[topic]?.payload)
                    when {
                        stored == null -> 0
                        stored.type != "json" -> -1
                        else -> {
                            store.delAll(listOf(topic))
                            1
                        }
                    }
                }).compose({ res ->
                    when (res) {
                        -1 -> writeError(WRONG_TYPE)
                        1 -> writeInteger(1)
                        else -> writeInteger(0)
                    }
                }, { err ->
                    writeError("ERR ${err.message ?: "json.del failed"}")
                })
            }
        }

        private fun handleJsonType(command: RespCommand): Future<Void> {
            if (command.args.size !in 2..3) {
                return writeError("ERR wrong number of arguments for 'json.type' command")
            }
            if (command.args.size == 3 && !isRootJsonPath(command.stringArg(2))) {
                return writeError("ERR only root JSON path '$' or '.' is supported")
            }
            val topic = command.stringArg(1)
            if (!canRead(topic)) {
                return writeError("NOPERM no permission to read topic")
            }
            return readStored(topic).compose { stored ->
                when {
                    stored == null -> writeNullBulk()
                    stored.type != "json" -> writeError(WRONG_TYPE)
                    else -> writeBulk(jsonTypeName(stored.value).toByteArray(StandardCharsets.UTF_8))
                }
            }
        }

        private fun handleDel(command: RespCommand): Future<Void> {
            if (command.args.size < 2) {
                return writeError("ERR wrong number of arguments for 'del' command")
            }
            val topics = command.args.drop(1).map { it.toString(StandardCharsets.UTF_8) }
            if (topics.any { !canWrite(it) }) {
                return writeError("NOPERM no permission to delete one or more topics")
            }
            val group = selectedArchiveGroup() ?: return Future.succeededFuture()
            if (group.lastValReadOnly) {
                return writeError("ERR last value store of archive group [${group.name}] is read-only")
            }
            val store = group.lastValStore ?: return writeInteger(0)
            return withKeyLocks(group.name, topics) {
                vertx.executeBlocking(Callable {
                    val existing = topics.count { store[it] != null }
                    store.delAll(topics)
                    existing
                }).compose(
                    { count -> writeInteger(count.toLong()) },
                    { err -> writeError("ERR ${err.message ?: "del failed"}") }
                )
            }
        }

        private fun handleExists(command: RespCommand): Future<Void> {
            if (command.args.size < 2) {
                return writeError("ERR wrong number of arguments for 'exists' command")
            }
            val topics = command.args.drop(1).map { it.toString(StandardCharsets.UTF_8) }
            if (topics.any { !canRead(it) }) {
                return writeError("NOPERM no permission to read one or more topics")
            }
            val group = selectedArchiveGroup() ?: return Future.succeededFuture()
            val store = group.lastValStore ?: return writeInteger(0)
            return vertx.executeBlocking(Callable { topics.count { store[it] != null } }).compose(
                { count -> writeInteger(count.toLong()) },
                { err -> writeError("ERR ${err.message ?: "exists failed"}") }
            )
        }

        private fun handleTtl(command: RespCommand): Future<Void> {
            if (command.args.size != 2) {
                return writeError("ERR wrong number of arguments for '${command.stringArg(0).lowercase()}' command")
            }
            val topic = command.stringArg(1)
            if (!canRead(topic)) {
                return writeError("NOPERM no permission to read topic")
            }
            val group = selectedArchiveGroup() ?: return Future.succeededFuture()
            val store = group.lastValStore ?: return writeInteger(-2)
            return vertx.executeBlocking(Callable { store[topic] != null }).compose(
                { exists -> writeInteger(if (exists) -1 else -2) },
                { err -> writeError("ERR ${err.message ?: "ttl failed"}") }
            )
        }

        private fun handleScan(command: RespCommand): Future<Void> {
            val match = scanMatch(command) ?: "#"
            // F2: SCAN COUNT is a hint, not a cap. Reply all matches up to KeysLimit, cursor 0.
            val group = selectedArchiveGroup() ?: return Future.succeededFuture()
            val store = group.lastValStore ?: return writeArray(listOf(RespValue.bulk("0"), RespValue.array(emptyList())))
            val (topicPattern, postFilter) = redisPatternToMqtt(match)
            return vertx.executeBlocking(Callable {
                val topics = mutableListOf<String>()
                store.findMatchingTopics(topicPattern) { topic ->
                    if (postFilter == null || postFilter(topic)) {
                        if (canRead(topic)) topics.add(topic)
                    }
                    topics.size < keysLimit
                }
                topics
            }).compose(
                { result ->
                    writeArray(listOf(
                        RespValue.bulk("0"),
                        RespValue.array(result.map { RespValue.bulk(it) })
                    ))
                },
                { err -> writeError("ERR ${err.message ?: "scan failed"}") }
            )
        }

        private fun handleKeys(command: RespCommand): Future<Void> {
            if (command.args.size != 2) {
                return writeError("ERR wrong number of arguments for 'keys' command")
            }
            val (pattern, postFilter) = redisPatternToMqtt(command.stringArg(1))
            val group = selectedArchiveGroup() ?: return Future.succeededFuture()
            val store = group.lastValStore ?: return writeArray(emptyList())
            return vertx.executeBlocking(Callable {
                val topics = mutableListOf<String>()
                store.findMatchingTopics(pattern) { topic ->
                    if (postFilter == null || postFilter(topic)) {
                        if (canRead(topic)) topics.add(topic)
                    }
                    topics.size < keysLimit
                }
                topics
            }).compose(
                { result -> writeArray(result.map { RespValue.bulk(it) }) },
                { err -> writeError("ERR ${err.message ?: "keys failed"}") }
            )
        }

        private fun handlePublish(command: RespCommand): Future<Void> {
            if (command.args.size != 3) {
                return writeError("ERR wrong number of arguments for 'publish' command")
            }
            val topic = command.stringArg(1)
            val payload = command.args[2]
            val error = validateWrite(topic, isPublish = true)
            if (error != null) {
                return writeError(error)
            }
            publishTopicUnchecked(topic, payload)
            return writeInteger(0)
        }

        private fun handleSubscribe(command: RespCommand, pattern: Boolean): Future<Void> {
            val cmdName = if (pattern) "psubscribe" else "subscribe"
            if (command.args.size < 2) {
                return writeError("ERR wrong number of arguments for '$cmdName' command")
            }
            val filters = command.args.drop(1).map { it.toString(StandardCharsets.UTF_8) }
            if (filters.any { !canRead(if (pattern) redisPatternToMqtt(it).first else it) }) {
                return writeError("NOPERM no permission to subscribe to one or more topics")
            }
            val writeFutures = mutableListOf<Future<Void>>()
            filters.forEach { requested ->
                val subKey = SubKey(pattern, requested)
                val existingListenerId = subscriptions[subKey]
                if (existingListenerId == null) {
                    val (mqttFilter, postFilter) = if (pattern) redisPatternToMqtt(requested) else Pair(requested, null)
                    val listenerId = "redis-$id-${subscriptionSequence.incrementAndGet()}"
                    subscriptions[subKey] = listenerId
                    sessionHandler.registerMessageListener(listenerId, listOf(mqttFilter)) { message ->
                        if (closed) return@registerMessageListener
                        if (postFilter != null && !postFilter(message.topicName)) return@registerMessageListener
                        if (!canRead(message.topicName)) return@registerMessageListener
                        sessionHandler.incrementMessagesOut(clientId)
                        if (pattern) {
                            writeArray(listOf(
                                RespValue.bulk("pmessage"),
                                RespValue.bulk(requested),
                                RespValue.bulk(message.topicName),
                                RespValue.bulk(message.payload)
                            ))
                        } else {
                            writeArray(listOf(
                                RespValue.bulk("message"),
                                RespValue.bulk(message.topicName),
                                RespValue.bulk(message.payload)
                            ))
                        }
                    }
                }
                writeFutures.add(writeArray(listOf(
                    RespValue.bulk(cmdName),
                    RespValue.bulk(requested),
                    RespValue.integer(subscriptions.size.toLong())
                )))
            }
            return Future.all(writeFutures as List<Future<*>>).mapEmpty()
        }

        private fun handleUnsubscribe(command: RespCommand): Future<Void> {
            val isPattern = command.stringArg(0).uppercase() == "PUNSUBSCRIBE"
            val cmdName = if (isPattern) "punsubscribe" else "unsubscribe"
            val writeFutures = mutableListOf<Future<Void>>()

            if (command.args.size > 1) {
                val targets = command.args.drop(1).map { it.toString(StandardCharsets.UTF_8) }
                targets.forEach { target ->
                    val key = SubKey(isPattern, target)
                    val listenerId = subscriptions.remove(key)
                    if (listenerId != null) {
                        sessionHandler.unregisterMessageListener(listenerId)
                    }
                    writeFutures.add(writeArray(listOf(
                        RespValue.bulk(cmdName),
                        RespValue.bulk(target),
                        RespValue.integer(subscriptions.size.toLong())
                    )))
                }
            } else {
                val toRemove = subscriptions.keys.filter { it.isPattern == isPattern }.toList()
                if (toRemove.isEmpty()) {
                    writeFutures.add(writeArray(listOf(
                        RespValue.bulk(cmdName),
                        RespValue.nullBulk(),
                        RespValue.integer(subscriptions.size.toLong())
                    )))
                } else {
                    toRemove.forEach { key ->
                        val listenerId = subscriptions.remove(key)
                        if (listenerId != null) {
                            sessionHandler.unregisterMessageListener(listenerId)
                        }
                        writeFutures.add(writeArray(listOf(
                            RespValue.bulk(cmdName),
                            RespValue.bulk(key.name),
                            RespValue.integer(subscriptions.size.toLong())
                        )))
                    }
                }
            }
            return Future.all(writeFutures as List<Future<*>>).mapEmpty()
        }

        private fun publishTopicUnchecked(topic: String, payload: ByteArray) {
            val message = BrokerMessage(
                messageId = 0,
                topicName = topic,
                payload = payload,
                qosLevel = publishQos,
                isRetain = setRetained,
                isDup = false,
                isQueued = false,
                clientId = clientId,
                senderId = clientId
            )
            sessionHandler.publishMessage(message)
            sessionHandler.incrementMessagesIn(clientId)
        }

        private fun validateWrite(topic: String, isPublish: Boolean = false): String? {
            if (topic.contains('+') || topic.contains('#')) return "ERR topic must not contain wildcard characters"
            val group = archiveGroupForDb(selectedDb) ?: return "ERR selected DB is not mapped to an archive group"
            if (group.topicFilter.isNotEmpty() && !group.filterTree.isTopicNameMatching(topic)) {
                return "ERR topic is outside selected archive group topic filter"
            }
            if (!isPublish && group.lastValReadOnly) {
                return "ERR last value store of archive group [${group.name}] is read-only"
            }
            if (!canWrite(topic)) return "NOPERM no permission to publish topic"
            return null
        }

        private fun canRead(topic: String): Boolean {
            if (!userManager.isUserManagementEnabled()) return true
            val user = username ?: "Anonymous"
            return userManager.canSubscribe(user, topic, "redis-server-$id")
        }

        private fun canWrite(topic: String): Boolean {
            if (!userManager.isUserManagementEnabled()) return true
            val user = username ?: "Anonymous"
            return userManager.canPublish(user, topic, "redis-server-$id")
        }

        private fun selectedArchiveGroup(): ArchiveGroup? {
            val group = archiveGroupForDb(selectedDb)
            if (group == null) writeError("ERR selected DB is not mapped to an archive group")
            return group
        }

        private fun archiveGroupForDb(db: Int): ArchiveGroup? {
            return archiveHandler.getDeployedArchiveGroups().values.firstOrNull { it.getRedisDbNumber() == db }
        }

        private fun scanMatch(command: RespCommand): String? {
            var i = 2
            while (i + 1 < command.args.size) {
                if (command.stringArg(i).equals("MATCH", ignoreCase = true)) return command.stringArg(i + 1)
                i += 2
            }
            return null
        }

        private fun scanCount(command: RespCommand): Int? {
            var i = 2
            while (i + 1 < command.args.size) {
                if (command.stringArg(i).equals("COUNT", ignoreCase = true)) return command.stringArg(i + 1).toIntOrNull()
                i += 2
            }
            return null
        }

        private fun hashScanOption(command: RespCommand, option: String): String? {
            var i = 3
            while (i + 1 < command.args.size) {
                if (command.stringArg(i).equals(option, ignoreCase = true)) return command.stringArg(i + 1)
                i += 2
            }
            return null
        }

        private fun redisPatternToMqtt(pattern: String): Pair<String, ((String) -> Boolean)?> {
            if (pattern == "*") return Pair("#", null)
            if (pattern.contains('+') || pattern.contains('#')) return Pair(pattern, null)

            if (!hasUnescapedMeta(pattern)) {
                return Pair(unescape(pattern), null)
            }

            val segments = pattern.split('/')
            val segmentsWithMeta = segments.map { hasUnescapedMeta(it) }
            val allMetaAreExactStar = segments.indices.all { i ->
                !segmentsWithMeta[i] || segments[i] == "*"
            }

            if (allMetaAreExactStar) {
                val newSegments = segments.indices.map { i ->
                    if (segments[i] == "*") {
                        if (i == segments.lastIndex) "#" else "+"
                    } else {
                        unescape(segments[i])
                    }
                }
                val filter = newSegments.joinToString("/")
                val postFilter: ((String) -> Boolean)? = if (segments.last() == "*") {
                    val minLevels = segments.size
                    { topic -> topic.split('/').size >= minLevels }
                } else {
                    null
                }
                return Pair(filter, postFilter)
            }

            val firstMetaIndex = segments.indices.first { segmentsWithMeta[it] }
            val prefix = segments.subList(0, firstMetaIndex).map { unescape(it) }.joinToString("/")
            val filter = if (prefix.isEmpty()) "#" else "$prefix/#"
            val postFilter: (String) -> Boolean = { topic -> globMatches(pattern, topic) }
            return Pair(filter, postFilter)
        }

        private fun hasUnescapedMeta(s: String): Boolean {
            var i = 0
            while (i < s.length) {
                if (s[i] == '\\') {
                    i += 2
                    continue
                }
                if (s[i] == '*' || s[i] == '?' || s[i] == '[') {
                    return true
                }
                i++
            }
            return false
        }

        private fun unescape(s: String): String {
            val sb = StringBuilder()
            var i = 0
            while (i < s.length) {
                if (s[i] == '\\' && i + 1 < s.length) {
                    i++
                    sb.append(s[i])
                } else {
                    sb.append(s[i])
                }
                i++
            }
            return sb.toString()
        }

        private fun globMatches(pattern: String, value: String): Boolean {
            return globToRegex(pattern).matches(value)
        }

        private fun globToRegex(pattern: String): Regex {
            val sb = StringBuilder("^")
            var i = 0
            val n = pattern.length
            while (i < n) {
                val c = pattern[i]
                when (c) {
                    '\\' -> {
                        i++
                        if (i < n) {
                            sb.append(Regex.escape(pattern[i].toString()))
                        } else {
                            sb.append(Regex.escape("\\"))
                        }
                    }
                    '*' -> sb.append(".*")
                    '?' -> sb.append(".")
                    '[' -> {
                        var j = i + 1
                        if (j < n && pattern[j] == '^') j++
                        if (j < n && pattern[j] == ']') j++
                        while (j < n && pattern[j] != ']') {
                            if (pattern[j] == '\\' && j + 1 < n) j++
                            j++
                        }
                        if (j < n && pattern[j] == ']') {
                            val inner = pattern.substring(i + 1, j)
                            if (inner.startsWith("^")) {
                                sb.append("[^")
                                escapeCharClass(inner.substring(1), sb)
                                sb.append("]")
                            } else {
                                sb.append("[")
                                escapeCharClass(inner, sb)
                                sb.append("]")
                            }
                            i = j
                        } else {
                            sb.append(Regex.escape("["))
                        }
                    }
                    else -> sb.append(Regex.escape(c.toString()))
                }
                i++
            }
            sb.append("$")
            return Regex(sb.toString(), RegexOption.DOT_MATCHES_ALL)
        }

        private fun escapeCharClass(inner: String, sb: StringBuilder) {
            var k = 0
            while (k < inner.length) {
                val ch = inner[k]
                if (ch == '\\' && k + 1 < inner.length) {
                    sb.append('\\').append(inner[k + 1])
                    k += 2
                } else if (ch == '-' && k > 0 && k + 1 < inner.length) {
                    sb.append('-')
                    k++
                } else if (ch == '[' || ch == ']' || ch == '-' || ch == '&') {
                    sb.append('\\').append(ch)
                    k++
                } else {
                    sb.append(ch)
                    k++
                }
            }
        }

        private fun sliceRange(payload: ByteArray?, rawStart: Long, rawEnd: Long): ByteArray {
            if (payload == null || payload.isEmpty()) return ByteArray(0)

            val size = payload.size.toLong()
            var start = if (rawStart < 0) size + rawStart else rawStart
            var end = if (rawEnd < 0) size + rawEnd else rawEnd

            if (start < 0) start = 0
            if (end < 0 || start >= size || start > end) return ByteArray(0)
            if (end >= size) end = size - 1

            return payload.copyOfRange(start.toInt(), end.toInt() + 1)
        }

        private fun handleType(command: RespCommand): Future<Void> {
            if (command.args.size != 2) {
                return writeError("ERR wrong number of arguments for 'type' command")
            }
            val topic = command.stringArg(1)
            if (!canRead(topic)) {
                return writeError("NOPERM no permission to read topic")
            }
            return readStored(topic).compose { stored ->
                writeSimple(stored?.type ?: "none")
            }
        }

        private fun readTypedValue(topic: String, expectedType: String, onSuccess: (Any?) -> Future<Void>): Future<Void> {
            if (!canRead(topic)) {
                return writeError("NOPERM no permission to read topic")
            }
            return readStored(topic).compose { stored ->
                when {
                    stored == null -> onSuccess(defaultValueForType(expectedType))
                    stored.type != expectedType -> writeError(WRONG_TYPE)
                    else -> onSuccess(stored.value)
                }
            }.recover { err ->
                writeError("ERR ${err.message ?: "read failed"}")
            }
        }

        private fun mutateTypedValue(
            topic: String,
            expectedType: String,
            defaultValue: Any,
            mutation: (Any) -> Pair<Any?, RespValue>
        ): Future<Void> {
            val error = validateWrite(topic, isPublish = false)
            if (error != null) {
                return writeError(error)
            }
            val group = selectedArchiveGroup() ?: return Future.succeededFuture()
            return withKeyLocks(group.name, listOf(topic)) {
                readStored(topic).compose { stored ->
                    if (stored != null && stored.type != expectedType) {
                        writeError(WRONG_TYPE)
                    } else {
                        val current = stored?.value ?: defaultValue
                        val (newValue, response) = mutation(current)
                        val encodedPayload = encodeEnvelope(expectedType, newValue)
                        val store = group.lastValStore
                        val updateStoreFuture = if (store != null) {
                            vertx.executeBlocking(Callable {
                                val brokerMsg = BrokerMessage(
                                    messageId = 0,
                                    topicName = topic,
                                    payload = encodedPayload,
                                    qosLevel = publishQos,
                                    isRetain = setRetained,
                                    isDup = false,
                                    isQueued = false,
                                    clientId = clientId,
                                    senderId = clientId
                                )
                                store.addAll(listOf(brokerMsg))
                            })
                        } else {
                            Future.succeededFuture()
                        }
                        updateStoreFuture.compose {
                            publishTopicUnchecked(topic, encodedPayload)
                            writeRespValue(response)
                        }
                    }
                }.recover { err ->
                    writeError("ERR ${err.message ?: "write failed"}")
                }
            }
        }

        private fun readStored(topic: String): Future<RedisStoredValue?> {
            val group = selectedArchiveGroup() ?: return Future.succeededFuture(null)
            val store = group.lastValStore ?: return Future.succeededFuture(null)
            return vertx.executeBlocking(Callable { store[topic]?.payload }).map { payload ->
                decodeStoredValue(payload)
            }
        }

        private fun decodeStoredValue(payload: ByteArray?): RedisStoredValue? {
            if (payload == null) return null
            val text = payload.toString(StandardCharsets.UTF_8)
            return try {
                val obj = JsonObject(text)
                val type = obj.getString(REDIS_TYPE_FIELD)
                if (type != null && obj.containsKey(REDIS_VALUE_FIELD)) {
                    RedisStoredValue(type, obj.getValue(REDIS_VALUE_FIELD))
                } else {
                    RedisStoredValue("string", payload)
                }
            } catch (_: Exception) {
                RedisStoredValue("string", payload)
            }
        }

        private fun encodeEnvelope(type: String, value: Any?): ByteArray {
            return JsonObject()
                .put(REDIS_TYPE_FIELD, type)
                .put(REDIS_VALUE_FIELD, value)
                .encode()
                .toByteArray(StandardCharsets.UTF_8)
        }

        private fun defaultValueForType(type: String): Any? {
            return when (type) {
                "hash", "zset" -> JsonObject()
                "set", "list" -> JsonArray()
                "json" -> null
                else -> null
            }
        }

        private fun jsonArrayToLinkedSet(array: JsonArray): LinkedHashSet<String> {
            return LinkedHashSet(array.map { it.toString() })
        }

        private fun <T> sliceList(values: List<T>, rawStart: Long, rawEnd: Long): List<T> {
            if (values.isEmpty()) return emptyList()
            val size = values.size.toLong()
            var start = if (rawStart < 0) size + rawStart else rawStart
            var end = if (rawEnd < 0) size + rawEnd else rawEnd
            if (start < 0) start = 0
            if (end < 0 || start >= size || start > end) return emptyList()
            if (end >= size) end = size - 1
            return values.subList(start.toInt(), end.toInt() + 1)
        }

        private fun sortedZsetEntries(zset: JsonObject): List<Pair<String, Double>> {
            return zset.fieldNames()
                .map { it to (zset.getDouble(it) ?: 0.0) }
                .sortedWith(compareBy<Pair<String, Double>> { it.second }.thenBy { it.first })
        }

        private fun formatScore(score: Double): String {
            return if (score % 1.0 == 0.0) score.toLong().toString() else score.toString()
        }

        private fun isRootJsonPath(path: String): Boolean = path == "$" || path == "."

        private fun jsonTypeName(value: Any?): String {
            return when (value) {
                null -> "null"
                is JsonObject, is Map<*, *> -> "object"
                is JsonArray, is List<*> -> "array"
                is String -> "string"
                is Number -> "number"
                is Boolean -> "boolean"
                else -> "string"
            }
        }

        private fun cleanup() {
            if (closed) return
            closed = true
            commandQueue.clear()
            subscriptions.values.toList().forEach { sessionHandler.unregisterMessageListener(it) }
            subscriptions.clear()
            if (sessionRegistered) {
                sessionHandler.delClient(clientId)
            }
            commandConsumer?.unregister()
        }

        private fun close() {
            cleanup()
            socket.close()
        }

        private fun writeSimple(value: String): Future<Void> = socket.write("+$value\r\n")
        private fun writeError(value: String): Future<Void> = socket.write("-$value\r\n")
        private fun writeInteger(value: Long): Future<Void> = socket.write(":$value\r\n")
        private fun writeNullBulk(): Future<Void> = socket.write("${'$'}-1\r\n")
        private fun writeBulk(value: String): Future<Void> = writeBulk(value.toByteArray(StandardCharsets.UTF_8))
        private fun writeBulk(value: ByteArray?): Future<Void> {
            if (value == null) {
                return writeNullBulk()
            }
            return socket.write(Buffer.buffer().appendString("${'$'}${value.size}\r\n").appendBytes(value).appendString("\r\n"))
        }

        private fun writeArray(values: List<RespValue>): Future<Void> {
            val buffer = Buffer.buffer().appendString("*${values.size}\r\n")
            values.forEach { appendRespValue(buffer, it) }
            return socket.write(buffer)
        }

        private fun writeRespValue(value: RespValue): Future<Void> {
            val buffer = Buffer.buffer()
            appendRespValue(buffer, value)
            return socket.write(buffer)
        }

        private fun appendRespValue(buffer: Buffer, value: RespValue) {
            when (value) {
                is RespValue.Bulk -> {
                    val bytes = value.value
                    if (bytes == null) buffer.appendString("${'$'}-1\r\n")
                    else buffer.appendString("${'$'}${bytes.size}\r\n").appendBytes(bytes).appendString("\r\n")
                }
                is RespValue.Integer -> buffer.appendString(":${value.value}\r\n")
                is RespValue.Array -> {
                    buffer.appendString("*${value.values.size}\r\n")
                    value.values.forEach { appendRespValue(buffer, it) }
                }
            }
        }
    }

    private data class RespCommand(val args: List<ByteArray>) {
        fun stringArg(index: Int): String = args[index].toString(StandardCharsets.UTF_8)
    }

    private data class RedisStoredValue(val type: String, val value: Any?)

    private sealed class RespValue {
        data class Bulk(val value: ByteArray?) : RespValue()
        data class Integer(val value: Long) : RespValue()
        data class Array(val values: List<RespValue>) : RespValue()

        companion object {
            fun bulk(value: String): RespValue = Bulk(value.toByteArray(StandardCharsets.UTF_8))
            fun bulk(value: ByteArray?): RespValue = Bulk(value)
            fun integer(value: Long): RespValue = Integer(value)
            fun array(values: List<RespValue>): RespValue = Array(values)
            fun nullBulk(): RespValue = Bulk(null)
        }
    }

    private class RespParser {
        private var buffer: Buffer = Buffer.buffer()

        fun append(data: Buffer) {
            buffer.appendBuffer(data)
        }

        fun next(): RespCommand? {
            if (buffer.length() == 0) return null
            val parsed = if (buffer.getByte(0).toInt().toChar() == '*') {
                parseArray(0)
            } else {
                parseInline(0)
            } ?: return null

            buffer = if (parsed.nextIndex >= buffer.length()) Buffer.buffer() else buffer.getBuffer(parsed.nextIndex, buffer.length())
            return RespCommand(parsed.args)
        }

        private fun parseArray(start: Int): Parsed? {
            val firstLineEnd = findCrlf(start) ?: run {
                if (buffer.length() - start > 65536) {
                    throw RedisProtocolException("ERR Protocol error: too big inline request")
                }
                return null
            }
            val count = buffer.getString(start + 1, firstLineEnd).toIntOrNull()
            if (count == null || count < 0 || count > 1048576) {
                throw RedisProtocolException("ERR Protocol error: invalid multibulk length")
            }
            var index = firstLineEnd + 2
            val args = ArrayList<ByteArray>(count)
            repeat(count) {
                if (index >= buffer.length()) return null
                val leadChar = buffer.getByte(index).toInt().toChar()
                if (leadChar != '$') {
                    throw RedisProtocolException("ERR Protocol error: expected '$', got '$leadChar'")
                }
                val lenEnd = findCrlf(index) ?: run {
                    if (buffer.length() - index > 65536) {
                        throw RedisProtocolException("ERR Protocol error: too big inline request")
                    }
                    return null
                }
                val len = buffer.getString(index + 1, lenEnd).toIntOrNull()
                if (len == null || len < 0) {
                    throw RedisProtocolException("ERR Protocol error: invalid bulk length")
                }
                index = lenEnd + 2
                if (index + len + 2 > buffer.length()) return null
                args.add(buffer.getBytes(index, index + len))
                index += len
                if (buffer.getByte(index).toInt().toChar() != '\r' || buffer.getByte(index + 1).toInt().toChar() != '\n') {
                    throw RedisProtocolException("ERR Protocol error: invalid bulk length")
                }
                index += 2
            }
            return Parsed(args, index)
        }

        private fun parseInline(start: Int): Parsed? {
            val end = findCrlf(start)
            if (end == null) {
                if (buffer.length() - start > 65536) {
                    throw RedisProtocolException("ERR Protocol error: too big inline request")
                }
                return null
            }
            if (end - start > 65536) {
                throw RedisProtocolException("ERR Protocol error: too big inline request")
            }
            val line = buffer.getString(start, end)
            val args = line.trim().split(Regex("\\s+"))
                .filter { it.isNotEmpty() }
                .map { it.toByteArray(StandardCharsets.UTF_8) }
            return Parsed(args, end + 2)
        }

        private fun findCrlf(start: Int): Int? {
            var i = start
            while (i + 1 < buffer.length()) {
                if (buffer.getByte(i).toInt().toChar() == '\r' && buffer.getByte(i + 1).toInt().toChar() == '\n') return i
                i++
            }
            return null
        }

        private data class Parsed(val args: List<ByteArray>, val nextIndex: Int)
    }
}
