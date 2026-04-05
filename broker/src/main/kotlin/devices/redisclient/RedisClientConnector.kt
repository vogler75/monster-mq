package at.rocworks.devices.redisclient

import at.rocworks.Const
import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.devices.RedisClientAddress
import at.rocworks.stores.devices.RedisClientConfig
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import io.vertx.redis.client.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Level
import java.util.logging.Logger

/**
 * RedisClientConnector
 *
 * Per-device bidirectional bridge between the local MQTT broker and a Redis server.
 *
 * Outbound (MQTT -> Redis): Subscribes to local MQTT topics as an internal client and
 *   publishes received [BrokerMessage]s to the mapped Redis channel or key.
 *
 * Inbound  (Redis -> MQTT): Creates Redis Pub/Sub subscriptions for each SUBSCRIBE address
 *   and republishes arriving messages to the local MQTT broker. For KV_SYNC addresses,
 *   periodically polls Redis keys and publishes values to MQTT.
 */
class RedisClientConnector : AbstractVerticle() {
    private val logger: Logger = Utils.getLogger(this::class.java)

    private lateinit var device: DeviceConfig
    private lateinit var cfg: RedisClientConfig

    // Redis client instances
    private var redisClient: Redis? = null         // Main client for commands/publishing
    private var redisSubClient: Redis? = null      // Dedicated Pub/Sub subscriber connection
    private var redisSubConnection: RedisConnection? = null

    private var isConnected = false
    private var reconnectTimerId: Long? = null
    private val kvPollTimerIds = CopyOnWriteArrayList<Long>()

    // Metrics
    private val messagesIn = AtomicLong(0)   // Redis -> MQTT
    private val messagesOut = AtomicLong(0)  // MQTT -> Redis
    private val errors = AtomicLong(0)
    private var lastMetricsReset = System.currentTimeMillis()

    // Internal MQTT client identity for the outbound direction
    private val internalClientId get() = "redisclient-${device.name}"

    // KV_SYNC change detection: stores last-seen payload per key (keyed by Redis key name)
    private val kvLastValues = ConcurrentHashMap<String, ByteArray>()

    // -- Outbound write batching --
    // Buffers outbound Redis writes and flushes them as MSET (for KV) or pipelined PUBLISH commands.
    // This avoids overwhelming the Redis connection pool with individual requests.
    private data class PendingWrite(val redisKey: String, val payload: ByteArray, val isKvSync: Boolean)
    private val writeBatch = mutableListOf<PendingWrite>()
    private var batchTimerId: Long? = null
    private val batchFlushIntervalMs = 100L  // flush every 100ms
    private val batchMaxSize = 200           // or when batch reaches this size

    // -- Lifecycle --

    override fun start(startPromise: Promise<Void>) {
        try {
            val deviceJson = config().getJsonObject("device")
            device = DeviceConfig.fromJsonObject(deviceJson)
            cfg = RedisClientConfig.fromJson(device.config)

            val validationErrors = cfg.validate()
            if (validationErrors.isNotEmpty()) {
                startPromise.fail("RedisClient config errors: ${validationErrors.joinToString(", ")}")
                return
            }

            logger.info("Starting RedisClientConnector for device '${device.name}' host=${cfg.host}:${cfg.port}")

            // Set up outbound direction (MQTT -> Redis) immediately so messages queue while we connect
            setupOutboundSubscriptions()

            // Register metrics endpoint
            setupMetricsEndpoint()

            // Start promise completes immediately; Redis connection happens in the background
            startPromise.complete()

            // Connect to Redis asynchronously
            connectToRedis()
        } catch (e: Exception) {
            logger.log(Level.SEVERE, "Failed to start RedisClientConnector: ${e.message}", e)
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Stopping RedisClientConnector for device '${device.name}'")

        // Cancel any pending reconnect timer
        reconnectTimerId?.let { vertx.cancelTimer(it); reconnectTimerId = null }

        // Cancel batch flush timer and flush remaining
        batchTimerId?.let { vertx.cancelTimer(it); batchTimerId = null }
        flushWriteBatch()

        // Cancel KV poll timers
        kvPollTimerIds.forEach { vertx.cancelTimer(it) }
        kvPollTimerIds.clear()

        // Unsubscribe internal MQTT client
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler != null) {
            cfg.addresses.filter { it.isPublish() || it.isKvSync() }.forEach { addr ->
                sessionHandler.unsubscribeInternalClient(internalClientId, addr.mqttTopic)
            }
            sessionHandler.unregisterInternalClient(internalClientId)
        }

        // Close Redis connections
        disconnectFromRedis()
        logger.info("RedisClientConnector '${device.name}' stopped")
        stopPromise.complete()
    }

    // -- Redis connection management --

    private fun connectToRedis() {
        try {
            logger.info("Connecting to Redis for device '${device.name}': ${cfg.host}:${cfg.port}")

            val connString = cfg.buildConnectionString()
            val options = RedisOptions()
                .setConnectionString(connString)
                .setMaxPoolSize(cfg.maxPoolSize)
                .setMaxWaitingHandlers(cfg.maxPoolSize * 128)

            if (cfg.useSsl && cfg.sslTrustAll) {
                options.netClientOptions.isTrustAll = true
            }

            // Create main client for commands
            val mainClient = Redis.createClient(vertx, options)
            redisClient = mainClient

            // Verify connection with a PING
            mainClient.send(Request.cmd(Command.PING)).onSuccess { _: Response? ->
                logger.info("Connected to Redis for device '${device.name}'")
                isConnected = true

                // Start batch flush timer
                if (batchTimerId == null) {
                    batchTimerId = vertx.setPeriodic(batchFlushIntervalMs) { flushWriteBatch() }
                }

                // Set up Pub/Sub subscriber on a dedicated connection
                setupSubscriberConnection(connString, options)

                // Set up KV_SYNC polling
                setupKvSyncPolling()
            }.onFailure { e ->
                logger.warning("Redis connection failed for '${device.name}': ${e.message}")
                isConnected = false
                scheduleReconnect()
            }
        } catch (e: Exception) {
            logger.log(Level.WARNING, "Redis connection failed for '${device.name}': ${e.message}")
            isConnected = false
            scheduleReconnect()
        }
    }

    private fun setupSubscriberConnection(connString: String, options: RedisOptions) {
        val subscribeAddresses = cfg.addresses.filter { it.isSubscribe() }
        if (subscribeAddresses.isEmpty()) return

        val subOptions = RedisOptions()
            .setConnectionString(connString)
            .setMaxPoolSize(1)

        if (cfg.useSsl && cfg.sslTrustAll) {
            subOptions.netClientOptions.isTrustAll = true
        }

        val subClient = Redis.createClient(vertx, subOptions)
        redisSubClient = subClient

        subClient.connect().onSuccess { conn ->
            redisSubConnection = conn
            logger.info("Redis subscriber connection established for '${device.name}'")

            // Set up message handler for incoming Pub/Sub messages
            conn.handler { response ->
                handleIncomingPubSubMessage(response)
            }

            // Subscribe to all SUBSCRIBE addresses
            setupInboundSubscriptions(conn)
        }.onFailure { e ->
            logger.warning("Redis subscriber connection failed for '${device.name}': ${e.message}")
            errors.incrementAndGet()
        }
    }

    private fun disconnectFromRedis() {
        reconnectTimerId?.let { vertx.cancelTimer(it); reconnectTimerId = null }
        try {
            redisSubConnection?.close()
            redisSubClient?.close()
            redisClient?.close()
        } catch (e: Exception) {
            logger.warning("Error closing Redis connection for '${device.name}': ${e.message}")
        } finally {
            redisSubConnection = null
            redisSubClient = null
            redisClient = null
            isConnected = false
        }
    }

    private fun scheduleReconnect() {
        if (reconnectTimerId != null) return
        reconnectTimerId = vertx.setTimer(cfg.reconnectDelayMs) {
            reconnectTimerId = null
            if (!isConnected) {
                logger.info("Retrying Redis connection for device '${device.name}'...")
                disconnectFromRedis()
                connectToRedis()
            }
        }
    }

    // -- Inbound direction: Redis -> MQTT --

    private fun setupInboundSubscriptions(conn: RedisConnection) {
        val subscribeAddresses = cfg.addresses.filter { it.isSubscribe() }

        subscribeAddresses.forEach { addr ->
            try {
                val request = if (addr.usePatternSubscribe) {
                    Request.cmd(Command.PSUBSCRIBE).arg(addr.redisChannel)
                } else {
                    Request.cmd(Command.SUBSCRIBE).arg(addr.redisChannel)
                }
                conn.send(request).onSuccess { _: Response? ->
                    logger.info("Subscribed to Redis ${if (addr.usePatternSubscribe) "pattern" else "channel"} '${addr.redisChannel}' for device '${device.name}'")
                }.onFailure { e ->
                    errors.incrementAndGet()
                    logger.severe("Failed to subscribe to '${addr.redisChannel}' for '${device.name}': ${e.message}")
                }
            } catch (e: Exception) {
                errors.incrementAndGet()
                logger.log(Level.SEVERE, "Failed to subscribe to '${addr.redisChannel}' for '${device.name}': ${e.message}", e)
            }
        }
    }

    private fun handleIncomingPubSubMessage(response: Response) {
        try {
            // Redis Pub/Sub messages come as multi-bulk responses:
            // [message, channel, payload] for SUBSCRIBE
            // [pmessage, pattern, channel, payload] for PSUBSCRIBE
            if (response.size() < 3) return

            val type = response[0].toString()
            val channel: String
            val payload: ByteArray

            when (type) {
                "message" -> {
                    channel = response[1].toString()
                    payload = responseToBytes(response[2])
                }
                "pmessage" -> {
                    // response[1] is the pattern, response[2] is the actual channel
                    channel = response[2].toString()
                    payload = responseToBytes(response[3])
                }
                else -> return // subscribe/psubscribe confirmations, etc.
            }

            // Find the matching address config
            val addr = cfg.addresses.filter { it.isSubscribe() }.firstOrNull { a ->
                if (a.usePatternSubscribe) {
                    // For pattern subscriptions, the pattern itself matches
                    true // Redis already matched for us
                } else {
                    a.redisChannel == channel
                }
            } ?: return

            val mqttTopic = addr.redisToMqttTopic(channel)
            val brokerMsg = BrokerMessage(
                messageId = 0,
                topicName = mqttTopic,
                payload = payload,
                qosLevel = addr.qos,
                isRetain = false,
                isDup = false,
                isQueued = false,
                clientId = internalClientId,
                senderId = internalClientId
            )
            Monster.getSessionHandler()?.publishMessage(brokerMsg)
            messagesIn.incrementAndGet()
        } catch (e: Exception) {
            errors.incrementAndGet()
            logger.warning("Error forwarding Redis->MQTT for '${device.name}': ${e.message}")
        }
    }

    // -- KV_SYNC polling --

    private fun setupKvSyncPolling() {
        val kvAddresses = cfg.addresses.filter { it.isKvSync() && it.kvPollIntervalMs > 0 }
        kvAddresses.forEach { addr ->
            val timerId = vertx.setPeriodic(addr.kvPollIntervalMs) {
                pollRedisKey(addr)
            }
            kvPollTimerIds.add(timerId)
        }
    }

    private fun pollRedisKey(addr: RedisClientAddress) {
        val client = redisClient ?: return
        if (!isConnected) return

        if (addr.usePatternMatch) {
            // Use SCAN to discover matching keys; track seen keys to detect removals
            val seenKeys = ConcurrentHashMap.newKeySet<String>()
            scanAndGetKeys(client, addr, "0", seenKeys)
        } else {
            // Single key: detect type and read accordingly
            readKeyByType(client, addr.redisChannel, addr)
        }
    }

    private fun scanAndGetKeys(client: Redis, addr: RedisClientAddress, cursor: String, seenKeys: MutableSet<String>) {
        client.send(Request.cmd(Command.SCAN).arg(cursor).arg("MATCH").arg(addr.redisChannel).arg("COUNT").arg("100"))
            .onSuccess { response: Response? ->
                val newCursor = response!![0].toString()
                val keys = response[1]

                for (i in 0 until keys.size()) {
                    val key = keys[i].toString()
                    seenKeys.add(key)
                    readKeyByType(client, key, addr)
                }

                if (newCursor != "0") {
                    // Continue scanning
                    scanAndGetKeys(client, addr, newCursor, seenKeys)
                } else {
                    // SCAN complete: remove cached values for keys no longer returned
                    if (addr.publishOnChangeOnly) {
                        val prefix = addr.redisChannel.replace(Regex("[*?].*"), "")
                        val staleKeys = kvLastValues.keys().toList().filter { k ->
                            k.startsWith(prefix) && k !in seenKeys
                        }
                        staleKeys.forEach { kvLastValues.remove(it) }
                    }
                }
            }.onFailure { e ->
                errors.incrementAndGet()
                logger.fine { "KV_SYNC SCAN failed for '${addr.redisChannel}': ${e.message}" }
            }
    }

    /**
     * Detect the key type with TYPE and read using the appropriate command:
     *   string  -> GET
     *   hash    -> HGETALL (serialized as JSON object)
     *   list    -> LRANGE 0 -1 (serialized as JSON array)
     *   set     -> SMEMBERS (serialized as JSON array)
     *   zset    -> ZRANGE 0 -1 WITHSCORES (serialized as JSON object)
     *   ReJSON  -> JSON.GET (returned as-is)
     */
    private fun readKeyByType(client: Redis, key: String, addr: RedisClientAddress) {
        client.send(Request.cmd(Command.TYPE).arg(key)).onSuccess { typeResponse: Response? ->
            val keyType = typeResponse?.toString() ?: "none"
            when (keyType) {
                "string" -> readStringKey(client, key, addr)
                "hash" -> readHashKey(client, key, addr)
                "list" -> readListKey(client, key, addr)
                "set" -> readSetKey(client, key, addr)
                "zset" -> readZsetKey(client, key, addr)
                "ReJSON-RL" -> readJsonKey(client, key, addr)
                "none" -> {} // key doesn't exist
                else -> {
                    // Try JSON.GET as fallback for unknown types (RedisJSON module)
                    readJsonKey(client, key, addr)
                }
            }
        }.onFailure { e ->
            errors.incrementAndGet()
            logger.fine { "KV_SYNC TYPE failed for '$key': ${e.message}" }
        }
    }

    private fun readStringKey(client: Redis, key: String, addr: RedisClientAddress) {
        client.send(Request.cmd(Command.GET).arg(key)).onSuccess { response: Response? ->
            if (response != null) {
                publishKvValue(key, responseToBytes(response), addr)
            }
        }.onFailure { e ->
            errors.incrementAndGet()
            logger.fine { "KV_SYNC GET failed for '$key': ${e.message}" }
        }
    }

    private fun readHashKey(client: Redis, key: String, addr: RedisClientAddress) {
        client.send(Request.cmd(Command.HGETALL).arg(key)).onSuccess { response: Response? ->
            if (response != null && response.size() > 0) {
                val json = JsonObject()
                var i = 0
                while (i < response.size() - 1) {
                    json.put(response[i].toString(), response[i + 1].toString())
                    i += 2
                }
                publishKvValue(key, json.encode().toByteArray(), addr)
            }
        }.onFailure { e ->
            errors.incrementAndGet()
            logger.fine { "KV_SYNC HGETALL failed for '$key': ${e.message}" }
        }
    }

    private fun readListKey(client: Redis, key: String, addr: RedisClientAddress) {
        client.send(Request.cmd(Command.LRANGE).arg(key).arg("0").arg("-1")).onSuccess { response: Response? ->
            if (response != null) {
                val arr = io.vertx.core.json.JsonArray()
                for (i in 0 until response.size()) {
                    arr.add(response[i].toString())
                }
                publishKvValue(key, arr.encode().toByteArray(), addr)
            }
        }.onFailure { e ->
            errors.incrementAndGet()
            logger.fine { "KV_SYNC LRANGE failed for '$key': ${e.message}" }
        }
    }

    private fun readSetKey(client: Redis, key: String, addr: RedisClientAddress) {
        client.send(Request.cmd(Command.SMEMBERS).arg(key)).onSuccess { response: Response? ->
            if (response != null) {
                val arr = io.vertx.core.json.JsonArray()
                for (i in 0 until response.size()) {
                    arr.add(response[i].toString())
                }
                publishKvValue(key, arr.encode().toByteArray(), addr)
            }
        }.onFailure { e ->
            errors.incrementAndGet()
            logger.fine { "KV_SYNC SMEMBERS failed for '$key': ${e.message}" }
        }
    }

    private fun readZsetKey(client: Redis, key: String, addr: RedisClientAddress) {
        client.send(Request.cmd(Command.ZRANGE).arg(key).arg("0").arg("-1").arg("WITHSCORES")).onSuccess { response: Response? ->
            if (response != null) {
                val json = JsonObject()
                var i = 0
                while (i < response.size() - 1) {
                    json.put(response[i].toString(), response[i + 1].toString().toDoubleOrNull() ?: 0.0)
                    i += 2
                }
                publishKvValue(key, json.encode().toByteArray(), addr)
            }
        }.onFailure { e ->
            errors.incrementAndGet()
            logger.fine { "KV_SYNC ZRANGE failed for '$key': ${e.message}" }
        }
    }

    private fun readJsonKey(client: Redis, key: String, addr: RedisClientAddress) {
        client.send(Request.cmd(Command.create("JSON.GET")).arg(key)).onSuccess { response: Response? ->
            if (response != null) {
                publishKvValue(key, responseToBytes(response), addr)
            }
        }.onFailure { e ->
            // JSON.GET not available (no RedisJSON module) — try plain GET as last resort
            readStringKey(client, key, addr)
        }
    }

    /**
     * Publish a polled KV value to MQTT. When [RedisClientAddress.publishOnChangeOnly] is true,
     * the value is compared to the last-seen value for this key and only published if it changed.
     * Stale keys (removed from Redis) are cleaned up in [scanAndGetKeys] after a full SCAN cycle.
     */
    private fun publishKvValue(key: String, payload: ByteArray, addr: RedisClientAddress) {
        if (addr.publishOnChangeOnly) {
            val previous = kvLastValues.put(key, payload)
            if (previous != null && previous.contentEquals(payload)) return // unchanged
        }
        publishToMqtt(addr.redisToMqttTopic(key), payload, addr.qos)
        messagesIn.incrementAndGet()
    }

    private fun publishToMqtt(topic: String, payload: ByteArray, qos: Int) {
        val brokerMsg = BrokerMessage(
            messageId = 0,
            topicName = topic,
            payload = payload,
            qosLevel = qos,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = internalClientId,
            senderId = internalClientId
        )
        Monster.getSessionHandler()?.publishMessage(brokerMsg)
    }

    // -- Outbound direction: MQTT -> Redis --

    private fun setupOutboundSubscriptions() {
        val outboundAddresses = cfg.addresses.filter { it.isPublish() || it.isKvSync() }
        if (outboundAddresses.isEmpty()) return

        // Consume local MQTT messages via the internal client's EventBus address
        vertx.eventBus().consumer<Any>(EventBusAddresses.Client.messages(internalClientId)) { busMsg ->
            try {
                when (val body = busMsg.body()) {
                    is BrokerMessage -> handleOutboundMessage(body)
                    is at.rocworks.data.BulkClientMessage -> body.messages.forEach { handleOutboundMessage(it) }
                    else -> logger.warning("Unknown message type from EventBus: ${body?.javaClass?.simpleName}")
                }
            } catch (e: Exception) {
                errors.incrementAndGet()
                logger.warning("Error processing outbound message for '${device.name}': ${e.message}")
            }
        }

        // Register internal subscriptions with the broker session handler
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler != null) {
            outboundAddresses.forEach { addr ->
                logger.info("Internal MQTT subscription for outbound bridge '$internalClientId' -> topic '${addr.mqttTopic}'")
                sessionHandler.subscribeInternalClient(internalClientId, addr.mqttTopic, addr.qos)
            }
        } else {
            logger.severe("SessionHandler not available for outbound Redis subscriptions")
        }
    }

    private fun handleOutboundMessage(msg: BrokerMessage) {
        logger.finer { "Outbound message for '${device.name}': topic='${msg.topicName}' sender='${msg.senderId}' client='${msg.clientId}'" }

        // Loop prevention: skip messages we published ourselves
        if (cfg.loopPrevention && (msg.senderId == internalClientId || msg.clientId == internalClientId)) return

        if (!isConnected) {
            logger.fine { "Redis not connected, dropping outbound message on '${msg.topicName}'" }
            return
        }

        // Forward to all matching outbound addresses (not just the first)
        val matchingAddrs = cfg.addresses
            .filter { it.isPublish() || it.isKvSync() }
            .filter { addr -> mqttTopicMatchesFilter(msg.topicName, addr.mqttTopic) }
        if (matchingAddrs.isEmpty()) return

        matchingAddrs.forEach { matchingAddr ->
            try {
                val redisTarget = matchingAddr.mqttToRedisChannel(msg.topicName)
                writeBatch.add(PendingWrite(redisTarget, msg.payload, matchingAddr.isKvSync()))

                // Flush immediately if batch is full
                if (writeBatch.size >= batchMaxSize) {
                    flushWriteBatch()
                }
            } catch (e: Exception) {
                errors.incrementAndGet()
                logger.warning("Failed to buffer outbound message for '${device.name}': ${e.message}")
            }
        }
    }

    /**
     * Flush pending outbound writes to Redis.
     * KV_SYNC SET writes are batched into a single MSET command.
     * PUBLISH writes are sent as pipelined commands via RedisAPI.batch().
     */
    private fun flushWriteBatch() {
        if (writeBatch.isEmpty()) return

        val client = redisClient
        if (client == null || !isConnected) {
            writeBatch.clear()
            return
        }

        // Drain the buffer
        val pending = ArrayList(writeBatch)
        writeBatch.clear()

        // Split into KV (SET) and Pub/Sub (PUBLISH) writes
        val kvWrites = pending.filter { it.isKvSync }
        val pubWrites = pending.filter { !it.isKvSync }

        // Batch KV writes as MSET (one round-trip for all key-value pairs)
        if (kvWrites.isNotEmpty()) {
            val msetRequest = Request.cmd(Command.MSET)
            kvWrites.forEach { write ->
                msetRequest.arg(write.redisKey).arg(write.payload)
            }
            client.send(msetRequest).onSuccess { _: Response? ->
                messagesOut.addAndGet(kvWrites.size.toLong())
                logger.fine { "MQTT->Redis MSET ${kvWrites.size} keys for '${device.name}'" }
            }.onFailure { e ->
                errors.incrementAndGet()
                logger.warning("Failed to MSET ${kvWrites.size} keys for '${device.name}': ${e.message}")
            }
        }

        // Pipeline PUBLISH writes using batch()
        if (pubWrites.isNotEmpty()) {
            val requests = pubWrites.map { write ->
                Request.cmd(Command.PUBLISH).arg(write.redisKey).arg(write.payload)
            }
            client.batch(requests).onSuccess { responses: List<Response?> ->
                val successCount = responses.count { it != null }
                messagesOut.addAndGet(successCount.toLong())
                logger.fine { "MQTT->Redis PUBLISH batch ${pubWrites.size} messages for '${device.name}'" }
            }.onFailure { e ->
                errors.incrementAndGet()
                logger.warning("Failed to PUBLISH batch ${pubWrites.size} messages for '${device.name}': ${e.message}")
            }
        }
    }

    // -- Metrics endpoint --

    private fun setupMetricsEndpoint() {
        val addr = EventBusAddresses.RedisBridge.connectorMetrics(device.name)
        vertx.eventBus().consumer<JsonObject>(addr) { msg ->
            try {
                val now = System.currentTimeMillis()
                val elapsedMs = now - lastMetricsReset
                val elapsedSec = if (elapsedMs > 0) elapsedMs / 1000.0 else 1.0
                val inCount = messagesIn.getAndSet(0)
                val outCount = messagesOut.getAndSet(0)
                val errCount = errors.get()
                lastMetricsReset = now
                msg.reply(
                    JsonObject()
                        .put("device", device.name)
                        .put("messagesInRate", inCount / elapsedSec)
                        .put("messagesOutRate", outCount / elapsedSec)
                        .put("errors", errCount)
                        .put("elapsedMs", elapsedMs)
                        .put("connected", isConnected)
                )
            } catch (e: Exception) {
                msg.fail(500, e.message)
            }
        }
    }

    // -- Utilities --

    private fun responseToBytes(response: Response): ByteArray {
        return try {
            response.toBytes()
        } catch (e: Exception) {
            response.toString().toByteArray()
        }
    }

    /**
     * MQTT topic filter matching (supports + and # wildcards).
     */
    private fun mqttTopicMatchesFilter(topic: String, filter: String): Boolean {
        if (filter == "#") return true
        val topicParts = topic.split("/")
        val filterParts = filter.split("/")
        return matchParts(topicParts, filterParts, 0, 0)
    }

    private fun matchParts(topic: List<String>, filter: List<String>, ti: Int, fi: Int): Boolean {
        if (fi == filter.size) return ti == topic.size
        val fp = filter[fi]
        return when {
            fp == "#" -> true
            ti == topic.size -> false
            fp == "+" || fp == topic[ti] -> matchParts(topic, filter, ti + 1, fi + 1)
            else -> false
        }
    }
}
