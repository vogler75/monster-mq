package at.rocworks.extensions

import at.rocworks.Utils
import at.rocworks.auth.UserManager
import at.rocworks.data.BrokerMessage
import at.rocworks.data.TopicTree
import at.rocworks.handlers.ArchiveGroup
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.handlers.SessionHandler
import io.vertx.core.AbstractVerticle
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

    companion object {
        private const val REDIS_TYPE_FIELD = "type"
        private const val REDIS_VALUE_FIELD = "value"
        private const val WRONG_TYPE = "WRONGTYPE Operation against a key holding the wrong kind of value"
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

    private inner class RedisConnection(
        private val id: Long,
        private val socket: NetSocket
    ) {
        private val parser = RespParser()
        private val listenerIds = mutableSetOf<String>()
        private var closed = false
        private var username: String? = null
        private var authenticated = !requireAuth || !userManager.isUserManagementEnabled()
        private var selectedDb = defaultDb
        private var clientName: String? = null

        fun start() {
            socket.handler { buffer ->
                parser.append(buffer)
                while (!closed) {
                    val command = parser.next() ?: break
                    handle(command)
                }
            }
            socket.closeHandler { cleanup() }
            socket.exceptionHandler {
                logger.fine("Redis client [$id] socket error: ${it.message}")
                close()
            }
        }

        private fun handle(command: RespCommand) {
            if (command.args.isEmpty()) {
                writeError("ERR empty command")
                return
            }

            val name = command.stringArg(0).uppercase()
            val allowedBeforeAuth = setOf("AUTH", "HELLO", "PING", "QUIT", "CLIENT")
            if (!authenticated && name !in allowedBeforeAuth) {
                writeError("NOAUTH Authentication required.")
                return
            }

            try {
                when (name) {
                    "PING" -> handlePing(command)
                    "ECHO" -> if (command.args.size == 2) writeBulk(command.args[1]) else writeError("ERR wrong number of arguments for 'echo' command")
                    "QUIT" -> {
                        writeSimple("OK")
                        close()
                    }
                    "AUTH" -> handleAuth(command)
                    "HELLO" -> handleHello(command)
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
                    "TTL" -> handleTtl(command)
                    "PTTL" -> handleTtl(command)
                    "SCAN" -> handleScan(command)
                    "KEYS" -> handleKeys(command)
                    "PUBLISH" -> handlePublish(command)
                    "SUBSCRIBE" -> handleSubscribe(command, pattern = false)
                    "PSUBSCRIBE" -> handleSubscribe(command, pattern = true)
                    "UNSUBSCRIBE", "PUNSUBSCRIBE" -> handleUnsubscribe(command)
                    "TYPE" -> handleType(command)
                    else -> writeError("ERR unknown command '$name'")
                }
            } catch (e: Exception) {
                logger.warning("Redis command [$name] failed: ${e.message}")
                writeError("ERR ${e.message ?: "command failed"}")
            }
        }

        private fun handlePing(command: RespCommand) {
            if (command.args.size > 2) {
                writeError("ERR wrong number of arguments for 'ping' command")
            } else if (command.args.size == 2) {
                writeBulk(command.args[1])
            } else {
                writeSimple("PONG")
            }
        }

        private fun handleAuth(command: RespCommand) {
            val credentials = when (command.args.size) {
                2 -> "default" to command.stringArg(1)
                3 -> command.stringArg(1) to command.stringArg(2)
                else -> {
                    writeError("ERR wrong number of arguments for 'auth' command")
                    return
                }
            }

            userManager.authenticate(credentials.first, credentials.second).onComplete { result ->
                val user = if (result.succeeded()) result.result() else null
                if (user != null && user.enabled) {
                    username = user.username
                    authenticated = true
                    writeSimple("OK")
                } else {
                    writeError("WRONGPASS invalid username-password pair or user is disabled.")
                }
            }
        }

        private fun handleHello(command: RespCommand) {
            if (command.args.size >= 2) {
                val proto = command.stringArg(1).toIntOrNull()
                if (proto != null && proto > 2) {
                    writeError("NOPROTO sorry, this protocol version is not supported.")
                    return
                }
            }

            val authIndex = command.args.indices.firstOrNull { command.stringArg(it).equals("AUTH", ignoreCase = true) }
            if (authIndex != null && authIndex + 2 < command.args.size) {
                userManager.authenticate(command.stringArg(authIndex + 1), command.stringArg(authIndex + 2)).onComplete { result ->
                    val user = if (result.succeeded()) result.result() else null
                    if (user != null && user.enabled) {
                        username = user.username
                        authenticated = true
                        writeHello()
                    } else {
                        writeError("WRONGPASS invalid username-password pair or user is disabled.")
                    }
                }
            } else {
                writeHello()
            }
        }

        private fun writeHello() {
            writeArray(
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

        private fun handleSelect(command: RespCommand) {
            if (command.args.size != 2) {
                writeError("ERR wrong number of arguments for 'select' command")
                return
            }
            val db = command.stringArg(1).toIntOrNull()
            if (db == null || db < 0) {
                writeError("ERR invalid DB index")
                return
            }
            if (archiveGroupForDb(db) == null) {
                writeError("ERR DB index is not mapped to an archive group")
                return
            }
            selectedDb = db
            writeSimple("OK")
        }

        private fun handleClient(command: RespCommand) {
            if (command.args.size < 2) {
                writeError("ERR wrong number of arguments for 'client' command")
                return
            }
            when (command.stringArg(1).uppercase()) {
                "SETNAME" -> {
                    clientName = command.args.getOrNull(2)?.toString(StandardCharsets.UTF_8)
                    writeSimple("OK")
                }
                "GETNAME" -> writeBulk(clientName?.toByteArray(StandardCharsets.UTF_8))
                "SETINFO" -> writeSimple("OK")
                "ID" -> writeInteger(id)
                else -> writeError("ERR unsupported CLIENT subcommand")
            }
        }

        private fun handleCommand(command: RespCommand) {
            if (command.args.size >= 2 && command.stringArg(1).equals("COUNT", ignoreCase = true)) {
                writeInteger(30)
            } else if (command.args.size >= 2 && command.stringArg(1).equals("INFO", ignoreCase = true)) {
                writeArray(command.args.drop(2).map { RespValue.nullBulk() })
            } else {
                writeArray(emptyList())
            }
        }

        private fun handleInfo() {
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
            writeBulk(body.toByteArray(StandardCharsets.UTF_8))
        }

        private fun handleGet(command: RespCommand) {
            if (command.args.size != 2) {
                writeError("ERR wrong number of arguments for 'get' command")
                return
            }
            val topic = command.stringArg(1)
            if (!canRead(topic)) {
                writeError("NOPERM no permission to read topic")
                return
            }
            val group = selectedArchiveGroup() ?: return
            val store = group.lastValStore ?: run {
                writeNullBulk()
                return
            }
            vertx.executeBlocking(Callable { store[topic] }).onComplete {
                val payload = if (it.succeeded()) it.result()?.payload else null
                writeBulk(payload)
            }
        }

        private fun handleGetRange(command: RespCommand) {
            if (command.args.size != 4) {
                writeError("ERR wrong number of arguments for '${command.stringArg(0).lowercase()}' command")
                return
            }
            val topic = command.stringArg(1)
            val start = command.stringArg(2).toLongOrNull()
            val end = command.stringArg(3).toLongOrNull()
            if (start == null || end == null) {
                writeError("ERR value is not an integer or out of range")
                return
            }
            if (!canRead(topic)) {
                writeError("NOPERM no permission to read topic")
                return
            }
            val group = selectedArchiveGroup() ?: return
            val store = group.lastValStore ?: run {
                writeBulk(ByteArray(0))
                return
            }
            vertx.executeBlocking(Callable { store[topic]?.payload }).onComplete { result ->
                if (result.failed()) {
                    writeError("ERR ${result.cause()?.message ?: "getrange failed"}")
                    return@onComplete
                }
                writeBulk(sliceRange(result.result(), start, end))
            }
        }

        private fun handleMget(command: RespCommand) {
            if (command.args.size < 2) {
                writeError("ERR wrong number of arguments for 'mget' command")
                return
            }
            val topics = command.args.drop(1).map { it.toString(StandardCharsets.UTF_8) }
            if (topics.any { !canRead(it) }) {
                writeError("NOPERM no permission to read one or more topics")
                return
            }
            val group = selectedArchiveGroup() ?: return
            val store = group.lastValStore ?: run {
                writeArray(topics.map { RespValue.nullBulk() })
                return
            }
            vertx.executeBlocking(Callable {
                topics.map { topic -> store[topic]?.payload }
            }).onComplete { result ->
                if (result.succeeded()) {
                    writeArray(result.result().map { RespValue.bulk(it) })
                } else {
                    writeError("ERR ${result.cause()?.message ?: "mget failed"}")
                }
            }
        }

        private fun handleSet(command: RespCommand) {
            if (command.args.size < 3) {
                writeError("ERR wrong number of arguments for 'set' command")
                return
            }
            publishTopic(command.stringArg(1), command.args[2]) {
                writeSimple("OK")
            }
        }

        private fun handleMset(command: RespCommand) {
            if (command.args.size < 3 || command.args.size % 2 == 0) {
                writeError("ERR wrong number of arguments for 'mset' command")
                return
            }
            var i = 1
            while (i < command.args.size) {
                val topic = command.stringArg(i)
                val payload = command.args[i + 1]
                val error = validateWrite(topic)
                if (error != null) {
                    writeError(error)
                    return
                }
                publishTopicUnchecked(topic, payload)
                i += 2
            }
            writeSimple("OK")
        }

        private fun handleHset(command: RespCommand) {
            if (command.args.size < 4 || command.args.size % 2 != 0) {
                writeError("ERR wrong number of arguments for 'hset' command")
                return
            }
            mutateTypedValue(command.stringArg(1), "hash", JsonObject()) { current ->
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

        private fun handleHget(command: RespCommand) {
            if (command.args.size != 3) {
                writeError("ERR wrong number of arguments for 'hget' command")
                return
            }
            readTypedValue(command.stringArg(1), "hash") { value ->
                val hash = value as JsonObject
                writeBulk(hash.getString(command.stringArg(2))?.toByteArray(StandardCharsets.UTF_8))
            }
        }

        private fun handleHmget(command: RespCommand) {
            if (command.args.size < 3) {
                writeError("ERR wrong number of arguments for 'hmget' command")
                return
            }
            readTypedValue(command.stringArg(1), "hash") { value ->
                val hash = value as JsonObject
                writeArray(command.args.drop(2).map { field ->
                    RespValue.bulk(hash.getString(field.toString(StandardCharsets.UTF_8))?.toByteArray(StandardCharsets.UTF_8))
                })
            }
        }

        private fun handleHgetall(command: RespCommand) {
            if (command.args.size != 2) {
                writeError("ERR wrong number of arguments for 'hgetall' command")
                return
            }
            readTypedValue(command.stringArg(1), "hash") { value ->
                val hash = value as JsonObject
                writeArray(hash.fieldNames().flatMap { field ->
                    listOf(RespValue.bulk(field), RespValue.bulk(hash.getString(field, "")))
                })
            }
        }

        private fun handleHdel(command: RespCommand) {
            if (command.args.size < 3) {
                writeError("ERR wrong number of arguments for 'hdel' command")
                return
            }
            mutateTypedValue(command.stringArg(1), "hash", JsonObject()) { current ->
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

        private fun handleHexists(command: RespCommand) {
            if (command.args.size != 3) {
                writeError("ERR wrong number of arguments for 'hexists' command")
                return
            }
            readTypedValue(command.stringArg(1), "hash") { value ->
                writeInteger(if ((value as JsonObject).containsKey(command.stringArg(2))) 1 else 0)
            }
        }

        private fun handleHlen(command: RespCommand) {
            if (command.args.size != 2) {
                writeError("ERR wrong number of arguments for 'hlen' command")
                return
            }
            readTypedValue(command.stringArg(1), "hash") { value -> writeInteger((value as JsonObject).size().toLong()) }
        }

        private fun handleHkeys(command: RespCommand) {
            if (command.args.size != 2) {
                writeError("ERR wrong number of arguments for 'hkeys' command")
                return
            }
            readTypedValue(command.stringArg(1), "hash") { value ->
                writeArray((value as JsonObject).fieldNames().map { RespValue.bulk(it) })
            }
        }

        private fun handleHvals(command: RespCommand) {
            if (command.args.size != 2) {
                writeError("ERR wrong number of arguments for 'hvals' command")
                return
            }
            readTypedValue(command.stringArg(1), "hash") { value ->
                val hash = value as JsonObject
                writeArray(hash.fieldNames().map { RespValue.bulk(hash.getString(it, "")) })
            }
        }

        private fun handleHscan(command: RespCommand) {
            if (command.args.size < 3) {
                writeError("ERR wrong number of arguments for 'hscan' command")
                return
            }
            val cursor = command.stringArg(2).toLongOrNull()
            if (cursor == null || cursor < 0) {
                writeError("ERR invalid cursor")
                return
            }
            val match = hashScanOption(command, "MATCH")
            val count = hashScanOption(command, "COUNT")?.toIntOrNull()?.coerceAtLeast(1) ?: Int.MAX_VALUE

            readTypedValue(command.stringArg(1), "hash") { value ->
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

        private fun handleSadd(command: RespCommand) {
            if (command.args.size < 3) {
                writeError("ERR wrong number of arguments for 'sadd' command")
                return
            }
            mutateTypedValue(command.stringArg(1), "set", JsonArray()) { current ->
                val set = jsonArrayToLinkedSet(current as JsonArray)
                var added = 0L
                command.args.drop(2).forEach { memberBytes ->
                    if (set.add(memberBytes.toString(StandardCharsets.UTF_8))) added++
                }
                JsonArray(set.toList()) to RespValue.integer(added)
            }
        }

        private fun handleSrem(command: RespCommand) {
            if (command.args.size < 3) {
                writeError("ERR wrong number of arguments for 'srem' command")
                return
            }
            mutateTypedValue(command.stringArg(1), "set", JsonArray()) { current ->
                val set = jsonArrayToLinkedSet(current as JsonArray)
                var removed = 0L
                command.args.drop(2).forEach { memberBytes ->
                    if (set.remove(memberBytes.toString(StandardCharsets.UTF_8))) removed++
                }
                JsonArray(set.toList()) to RespValue.integer(removed)
            }
        }

        private fun handleSmembers(command: RespCommand) {
            if (command.args.size != 2) {
                writeError("ERR wrong number of arguments for 'smembers' command")
                return
            }
            readTypedValue(command.stringArg(1), "set") { value ->
                writeArray((value as JsonArray).map { RespValue.bulk(it.toString()) })
            }
        }

        private fun handleSismember(command: RespCommand) {
            if (command.args.size != 3) {
                writeError("ERR wrong number of arguments for 'sismember' command")
                return
            }
            readTypedValue(command.stringArg(1), "set") { value ->
                writeInteger(if (jsonArrayToLinkedSet(value as JsonArray).contains(command.stringArg(2))) 1 else 0)
            }
        }

        private fun handleScard(command: RespCommand) {
            if (command.args.size != 2) {
                writeError("ERR wrong number of arguments for 'scard' command")
                return
            }
            readTypedValue(command.stringArg(1), "set") { value -> writeInteger((value as JsonArray).size().toLong()) }
        }

        private fun handleListPush(command: RespCommand, left: Boolean) {
            if (command.args.size < 3) {
                writeError("ERR wrong number of arguments for '${command.stringArg(0).lowercase()}' command")
                return
            }
            mutateTypedValue(command.stringArg(1), "list", JsonArray()) { current ->
                val values = (current as JsonArray).map { it.toString() }.toMutableList()
                val incoming = command.args.drop(2).map { it.toString(StandardCharsets.UTF_8) }
                if (left) incoming.forEach { values.add(0, it) } else values.addAll(incoming)
                JsonArray(values) to RespValue.integer(values.size.toLong())
            }
        }

        private fun handleListPop(command: RespCommand, left: Boolean) {
            if (command.args.size != 2) {
                writeError("ERR wrong number of arguments for '${command.stringArg(0).lowercase()}' command")
                return
            }
            mutateTypedValue(command.stringArg(1), "list", JsonArray()) { current ->
                val values = (current as JsonArray).map { it.toString() }.toMutableList()
                val popped = if (values.isEmpty()) null else if (left) values.removeAt(0) else values.removeAt(values.lastIndex)
                JsonArray(values) to RespValue.bulk(popped?.toByteArray(StandardCharsets.UTF_8))
            }
        }

        private fun handleLrange(command: RespCommand) {
            if (command.args.size != 4) {
                writeError("ERR wrong number of arguments for 'lrange' command")
                return
            }
            val start = command.stringArg(2).toLongOrNull()
            val end = command.stringArg(3).toLongOrNull()
            if (start == null || end == null) {
                writeError("ERR value is not an integer or out of range")
                return
            }
            readTypedValue(command.stringArg(1), "list") { value ->
                val values = (value as JsonArray).map { it.toString() }
                writeArray(sliceList(values, start, end).map { RespValue.bulk(it) })
            }
        }

        private fun handleLlen(command: RespCommand) {
            if (command.args.size != 2) {
                writeError("ERR wrong number of arguments for 'llen' command")
                return
            }
            readTypedValue(command.stringArg(1), "list") { value -> writeInteger((value as JsonArray).size().toLong()) }
        }

        private fun handleLindex(command: RespCommand) {
            if (command.args.size != 3) {
                writeError("ERR wrong number of arguments for 'lindex' command")
                return
            }
            val index = command.stringArg(2).toLongOrNull()
            if (index == null) {
                writeError("ERR value is not an integer or out of range")
                return
            }
            readTypedValue(command.stringArg(1), "list") { value ->
                val values = (value as JsonArray).map { it.toString() }
                val normalized = if (index < 0) values.size + index else index
                writeBulk(values.getOrNull(normalized.toInt())?.toByteArray(StandardCharsets.UTF_8))
            }
        }

        private fun handleZadd(command: RespCommand) {
            if (command.args.size < 4 || command.args.size % 2 != 0) {
                writeError("ERR wrong number of arguments for 'zadd' command")
                return
            }
            var validateIndex = 2
            while (validateIndex < command.args.size) {
                if (command.stringArg(validateIndex).toDoubleOrNull() == null) {
                    writeError("ERR value is not a valid float")
                    return
                }
                validateIndex += 2
            }
            mutateTypedValue(command.stringArg(1), "zset", JsonObject()) { current ->
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

        private fun handleZrange(command: RespCommand) {
            if (command.args.size < 4) {
                writeError("ERR wrong number of arguments for 'zrange' command")
                return
            }
            val start = command.stringArg(2).toLongOrNull()
            val end = command.stringArg(3).toLongOrNull()
            if (start == null || end == null) {
                writeError("ERR value is not an integer or out of range")
                return
            }
            val withScores = command.args.drop(4).any { it.toString(StandardCharsets.UTF_8).equals("WITHSCORES", ignoreCase = true) }
            readTypedValue(command.stringArg(1), "zset") { value ->
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

        private fun handleZrem(command: RespCommand) {
            if (command.args.size < 3) {
                writeError("ERR wrong number of arguments for 'zrem' command")
                return
            }
            mutateTypedValue(command.stringArg(1), "zset", JsonObject()) { current ->
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

        private fun handleZscore(command: RespCommand) {
            if (command.args.size != 3) {
                writeError("ERR wrong number of arguments for 'zscore' command")
                return
            }
            readTypedValue(command.stringArg(1), "zset") { value ->
                val score = (value as JsonObject).getDouble(command.stringArg(2))
                writeBulk(score?.let { formatScore(it).toByteArray(StandardCharsets.UTF_8) })
            }
        }

        private fun handleZcard(command: RespCommand) {
            if (command.args.size != 2) {
                writeError("ERR wrong number of arguments for 'zcard' command")
                return
            }
            readTypedValue(command.stringArg(1), "zset") { value -> writeInteger((value as JsonObject).size().toLong()) }
        }

        private fun handleJsonSet(command: RespCommand) {
            if (command.args.size != 4) {
                writeError("ERR wrong number of arguments for 'json.set' command")
                return
            }
            if (!isRootJsonPath(command.stringArg(2))) {
                writeError("ERR only root JSON path '$' or '.' is supported")
                return
            }
            val parsed = try {
                Json.decodeValue(command.stringArg(3))
            } catch (e: Exception) {
                writeError("ERR invalid JSON")
                return
            }
            mutateTypedValue(command.stringArg(1), "json", JsonObject()) {
                parsed to RespValue.bulk("OK")
            }
        }

        private fun handleJsonGet(command: RespCommand) {
            if (command.args.size !in 2..3) {
                writeError("ERR wrong number of arguments for 'json.get' command")
                return
            }
            if (command.args.size == 3 && !isRootJsonPath(command.stringArg(2))) {
                writeError("ERR only root JSON path '$' or '.' is supported")
                return
            }
            val topic = command.stringArg(1)
            if (!canRead(topic)) {
                writeError("NOPERM no permission to read topic")
                return
            }
            readStored(topic) { stored ->
                when {
                    stored == null -> writeNullBulk()
                    stored.type != "json" -> writeError(WRONG_TYPE)
                    else -> writeBulk(Json.encode(stored.value))
                }
            }
        }

        private fun handleJsonDel(command: RespCommand) {
            if (command.args.size !in 2..3) {
                writeError("ERR wrong number of arguments for 'json.del' command")
                return
            }
            if (command.args.size == 3 && !isRootJsonPath(command.stringArg(2))) {
                writeError("ERR only root JSON path '$' or '.' is supported")
                return
            }
            val topic = command.stringArg(1)
            if (!canWrite(topic)) {
                writeError("NOPERM no permission to delete topic")
                return
            }
            val group = selectedArchiveGroup() ?: return
            val store = group.lastValStore ?: run {
                writeInteger(0)
                return
            }
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
            }).onComplete { result ->
                if (result.failed()) {
                    writeError("ERR ${result.cause()?.message ?: "json.del failed"}")
                } else {
                    when (result.result()) {
                        -1 -> writeError(WRONG_TYPE)
                        1 -> writeInteger(1)
                        else -> writeInteger(0)
                    }
                }
            }
        }

        private fun handleJsonType(command: RespCommand) {
            if (command.args.size !in 2..3) {
                writeError("ERR wrong number of arguments for 'json.type' command")
                return
            }
            if (command.args.size == 3 && !isRootJsonPath(command.stringArg(2))) {
                writeError("ERR only root JSON path '$' or '.' is supported")
                return
            }
            val topic = command.stringArg(1)
            if (!canRead(topic)) {
                writeError("NOPERM no permission to read topic")
                return
            }
            readStored(topic) { stored ->
                when {
                    stored == null -> writeNullBulk()
                    stored.type != "json" -> writeError(WRONG_TYPE)
                    else -> writeBulk(jsonTypeName(stored.value).toByteArray(StandardCharsets.UTF_8))
                }
            }
        }

        private fun handleDel(command: RespCommand) {
            if (command.args.size < 2) {
                writeError("ERR wrong number of arguments for 'del' command")
                return
            }
            val topics = command.args.drop(1).map { it.toString(StandardCharsets.UTF_8) }
            if (topics.any { !canWrite(it) }) {
                writeError("NOPERM no permission to delete one or more topics")
                return
            }
            val group = selectedArchiveGroup() ?: return
            val store = group.lastValStore ?: run {
                writeInteger(0)
                return
            }
            vertx.executeBlocking(Callable {
                val existing = topics.count { store[it] != null }
                store.delAll(topics)
                existing
            }).onComplete {
                if (it.succeeded()) writeInteger(it.result().toLong()) else writeError("ERR ${it.cause()?.message ?: "del failed"}")
            }
        }

        private fun handleExists(command: RespCommand) {
            if (command.args.size < 2) {
                writeError("ERR wrong number of arguments for 'exists' command")
                return
            }
            val topics = command.args.drop(1).map { it.toString(StandardCharsets.UTF_8) }
            if (topics.any { !canRead(it) }) {
                writeError("NOPERM no permission to read one or more topics")
                return
            }
            val group = selectedArchiveGroup() ?: return
            val store = group.lastValStore ?: run {
                writeInteger(0)
                return
            }
            vertx.executeBlocking(Callable { topics.count { store[it] != null } }).onComplete {
                if (it.succeeded()) writeInteger(it.result().toLong()) else writeError("ERR ${it.cause()?.message ?: "exists failed"}")
            }
        }

        private fun handleTtl(command: RespCommand) {
            if (command.args.size != 2) {
                writeError("ERR wrong number of arguments for '${command.stringArg(0).lowercase()}' command")
                return
            }
            val topic = command.stringArg(1)
            if (!canRead(topic)) {
                writeError("NOPERM no permission to read topic")
                return
            }
            val group = selectedArchiveGroup() ?: return
            val store = group.lastValStore ?: run {
                writeInteger(-2)
                return
            }
            vertx.executeBlocking(Callable { store[topic] != null }).onComplete {
                if (it.succeeded()) {
                    writeInteger(if (it.result()) -1 else -2)
                } else {
                    writeError("ERR ${it.cause()?.message ?: "ttl failed"}")
                }
            }
        }

        private fun handleScan(command: RespCommand) {
            val match = scanMatch(command) ?: "#"
            val count = scanCount(command) ?: keysLimit
            val group = selectedArchiveGroup() ?: return
            val store = group.lastValStore ?: run {
                writeArray(listOf(RespValue.bulk("0"), RespValue.array(emptyList())))
                return
            }
            val topicPattern = redisPatternToMqtt(match)
            vertx.executeBlocking(Callable {
                val topics = mutableListOf<String>()
                store.findMatchingTopics(topicPattern) { topic ->
                    if (canRead(topic)) topics.add(topic)
                    topics.size < count.coerceAtMost(keysLimit)
                }
                topics
            }).onComplete { result ->
                if (result.succeeded()) {
                    writeArray(listOf(
                        RespValue.bulk("0"),
                        RespValue.array(result.result().map { RespValue.bulk(it) })
                    ))
                } else {
                    writeError("ERR ${result.cause()?.message ?: "scan failed"}")
                }
            }
        }

        private fun handleKeys(command: RespCommand) {
            if (command.args.size != 2) {
                writeError("ERR wrong number of arguments for 'keys' command")
                return
            }
            val pattern = redisPatternToMqtt(command.stringArg(1))
            val group = selectedArchiveGroup() ?: return
            val store = group.lastValStore ?: run {
                writeArray(emptyList())
                return
            }
            vertx.executeBlocking(Callable {
                val topics = mutableListOf<String>()
                store.findMatchingTopics(pattern) { topic ->
                    if (canRead(topic)) topics.add(topic)
                    topics.size < keysLimit
                }
                topics
            }).onComplete { result ->
                if (result.succeeded()) writeArray(result.result().map { RespValue.bulk(it) })
                else writeError("ERR ${result.cause()?.message ?: "keys failed"}")
            }
        }

        private fun handlePublish(command: RespCommand) {
            if (command.args.size != 3) {
                writeError("ERR wrong number of arguments for 'publish' command")
                return
            }
            publishTopic(command.stringArg(1), command.args[2]) {
                writeInteger(0)
            }
        }

        private fun handleSubscribe(command: RespCommand, pattern: Boolean) {
            if (command.args.size < 2) {
                writeError("ERR wrong number of arguments for '${if (pattern) "psubscribe" else "subscribe"}' command")
                return
            }
            val filters = command.args.drop(1).map { it.toString(StandardCharsets.UTF_8) }
            if (filters.any { !canRead(if (pattern) redisPatternToMqtt(it) else it) }) {
                writeError("NOPERM no permission to subscribe to one or more topics")
                return
            }
            filters.forEach { requested ->
                val mqttFilter = if (pattern) redisPatternToMqtt(requested) else requested
                val listenerId = "redis-$id-${listenerIds.size + 1}"
                listenerIds.add(listenerId)
                sessionHandler.registerMessageListener(listenerId, listOf(mqttFilter)) { message ->
                    if (closed) return@registerMessageListener
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
                writeArray(listOf(
                    RespValue.bulk(if (pattern) "psubscribe" else "subscribe"),
                    RespValue.bulk(requested),
                    RespValue.integer(listenerIds.size.toLong())
                ))
            }
        }

        private fun handleUnsubscribe(command: RespCommand) {
            val count = listenerIds.size
            listenerIds.toList().forEach {
                sessionHandler.unregisterMessageListener(it)
                listenerIds.remove(it)
            }
            writeArray(listOf(
                RespValue.bulk(command.stringArg(0).lowercase()),
                RespValue.nullBulk(),
                RespValue.integer(0)
            ))
            logger.fine("Redis client [$id] removed $count pub/sub listeners")
        }

        private fun publishTopic(topic: String, payload: ByteArray, onSuccess: () -> Unit) {
            val error = validateWrite(topic)
            if (error != null) {
                writeError(error)
                return
            }
            publishTopicUnchecked(topic, payload)
            onSuccess()
        }

        private fun publishTopicUnchecked(topic: String, payload: ByteArray) {
            val clientId = "redis-server-$id"
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
        }

        private fun validateWrite(topic: String): String? {
            if (topic.contains('+') || topic.contains('#')) return "ERR topic must not contain wildcard characters"
            val group = archiveGroupForDb(selectedDb) ?: return "ERR selected DB is not mapped to an archive group"
            if (group.topicFilter.isNotEmpty() && !group.filterTree.isTopicNameMatching(topic)) {
                return "ERR topic is outside selected archive group topic filter"
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

        private fun redisPatternToMqtt(pattern: String): String {
            if (pattern == "*") return "#"
            if (pattern.contains('+') || pattern.contains('#')) return pattern
            return pattern.replace("*", "#")
        }

        private fun globMatches(pattern: String, value: String): Boolean {
            val regex = buildString {
                append("^")
                pattern.forEach { ch ->
                    when (ch) {
                        '*' -> append(".*")
                        '?' -> append(".")
                        else -> append(Regex.escape(ch.toString()))
                    }
                }
                append("$")
            }
            return Regex(regex).matches(value)
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

        private fun handleType(command: RespCommand) {
            if (command.args.size != 2) {
                writeError("ERR wrong number of arguments for 'type' command")
                return
            }
            val topic = command.stringArg(1)
            if (!canRead(topic)) {
                writeError("NOPERM no permission to read topic")
                return
            }
            readStored(topic) { stored ->
                writeSimple(stored?.type ?: "none")
            }
        }

        private fun readTypedValue(topic: String, expectedType: String, onSuccess: (Any?) -> Unit) {
            if (!canRead(topic)) {
                writeError("NOPERM no permission to read topic")
                return
            }
            readStored(topic) { stored ->
                when {
                    stored == null -> onSuccess(defaultValueForType(expectedType))
                    stored.type != expectedType -> writeError(WRONG_TYPE)
                    else -> onSuccess(stored.value)
                }
            }
        }

        private fun mutateTypedValue(
            topic: String,
            expectedType: String,
            defaultValue: Any,
            mutation: (Any) -> Pair<Any?, RespValue>
        ) {
            val error = validateWrite(topic)
            if (error != null) {
                writeError(error)
                return
            }
            readStored(topic) { stored ->
                if (stored != null && stored.type != expectedType) {
                    writeError(WRONG_TYPE)
                    return@readStored
                }

                val current = stored?.value ?: defaultValue
                val (newValue, response) = mutation(current)
                publishTopicUnchecked(topic, encodeEnvelope(expectedType, newValue))
                writeRespValue(response)
            }
        }

        private fun readStored(topic: String, onResult: (RedisStoredValue?) -> Unit) {
            val group = selectedArchiveGroup() ?: return
            val store = group.lastValStore ?: run {
                onResult(null)
                return
            }
            vertx.executeBlocking(Callable { store[topic]?.payload }).onComplete { result ->
                if (result.failed()) {
                    writeError("ERR ${result.cause()?.message ?: "read failed"}")
                } else {
                    onResult(decodeStoredValue(result.result()))
                }
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
            closed = true
            listenerIds.toList().forEach { sessionHandler.unregisterMessageListener(it) }
            listenerIds.clear()
        }

        private fun close() {
            cleanup()
            socket.close()
        }

        private fun writeSimple(value: String) = socket.write("+$value\r\n")
        private fun writeError(value: String) = socket.write("-$value\r\n")
        private fun writeInteger(value: Long) = socket.write(":$value\r\n")
        private fun writeNullBulk() = socket.write("${'$'}-1\r\n")
        private fun writeBulk(value: String) = writeBulk(value.toByteArray(StandardCharsets.UTF_8))
        private fun writeBulk(value: ByteArray?) {
            if (value == null) {
                writeNullBulk()
                return
            }
            socket.write(Buffer.buffer().appendString("${'$'}${value.size}\r\n").appendBytes(value).appendString("\r\n"))
        }

        private fun writeArray(values: List<RespValue>) {
            val buffer = Buffer.buffer().appendString("*${values.size}\r\n")
            values.forEach { appendRespValue(buffer, it) }
            socket.write(buffer)
        }

        private fun writeRespValue(value: RespValue) {
            val buffer = Buffer.buffer()
            appendRespValue(buffer, value)
            socket.write(buffer)
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
            val firstLineEnd = findCrlf(start) ?: return null
            val count = buffer.getString(start + 1, firstLineEnd).toIntOrNull() ?: throw IllegalArgumentException("invalid multibulk length")
            var index = firstLineEnd + 2
            val args = ArrayList<ByteArray>(count)
            repeat(count) {
                if (index >= buffer.length()) return null
                if (buffer.getByte(index).toInt().toChar() != '$') throw IllegalArgumentException("expected bulk string")
                val lenEnd = findCrlf(index) ?: return null
                val len = buffer.getString(index + 1, lenEnd).toIntOrNull() ?: throw IllegalArgumentException("invalid bulk length")
                index = lenEnd + 2
                if (len < 0) {
                    args.add(ByteArray(0))
                } else {
                    if (index + len + 2 > buffer.length()) return null
                    args.add(buffer.getBytes(index, index + len))
                    index += len
                    if (buffer.getByte(index).toInt().toChar() != '\r' || buffer.getByte(index + 1).toInt().toChar() != '\n') {
                        throw IllegalArgumentException("invalid bulk terminator")
                    }
                    index += 2
                }
            }
            return Parsed(args, index)
        }

        private fun parseInline(start: Int): Parsed? {
            val end = findCrlf(start) ?: return null
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
