package at.rocworks.bus

import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.data.TopicTree
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import io.vertx.core.json.JsonArray
import io.zenoh.Config
import io.zenoh.Session
import io.zenoh.Zenoh
import io.zenoh.bytes.Encoding
import io.zenoh.bytes.ZBytes
import io.zenoh.handlers.Callback
import io.zenoh.keyexpr.KeyExpr
import io.zenoh.pubsub.CallbackSubscriber
import io.zenoh.pubsub.PutOptions
import io.zenoh.qos.Reliability
import io.zenoh.sample.Sample
import java.util.LinkedHashMap
import java.util.concurrent.Callable

class MessageBusZenoh(
    private val brokerId: String,
    private val zenohConfig: JsonObject
) : AbstractVerticle(), IMessageBus {
    override val isExternalTransport: Boolean = true

    private val logger = Utils.getLogger(this::class.java)
    private val mode = zenohConfig.getString("Mode", "peer").lowercase()
    private val endpoints = zenohConfig.getJsonArray("Connect")?.map { it.toString() } ?: emptyList()
    private val prefix = zenohConfig.getString("Prefix", "").trim('/')
    private val localPrefix = zenohConfig.getString("LocalPrefix", "").trim('/')
    private val allow = zenohConfig.getJsonArray("Allow")?.map { it.toString() } ?: listOf("#")
    private val deny = zenohConfig.getJsonArray("Deny")?.map { it.toString() } ?: listOf("\$SYS/#")
    private val dedupConfig = zenohConfig.getJsonObject("Deduplication", JsonObject())
    private val cacheSize = dedupConfig.getInteger("CacheSize", 100_000)
    private val ttlMs = dedupConfig.getLong("TtlSeconds", 300L) * 1_000L

    private var session: Session? = null
    private val subscribers = mutableListOf<CallbackSubscriber>()
    @Volatile private var callback: ((BrokerMessage) -> Unit)? = null

    private val seenMessages = object : LinkedHashMap<String, Long>(1024, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, Long>?): Boolean = size > cacheSize
    }

    init {
        require(brokerId.isNotBlank()) { "NodeName must not be blank when Zenoh is enabled" }
        require(mode in setOf("client", "peer")) { "Zenoh.Mode must be client or peer" }
        require(cacheSize > 0) { "Zenoh.Deduplication.CacheSize must be greater than zero" }
        require(ttlMs > 0) { "Zenoh.Deduplication.TtlSeconds must be greater than zero" }
        if (mode == "client") require(endpoints.isNotEmpty()) { "Zenoh.Connect requires at least one locator in client mode" }
    }

    override fun start(startPromise: Promise<Void>) {
        vertx.executeBlocking(Callable<Void?> {
            val configJson = JsonObject().put("mode", mode)
            if (endpoints.isNotEmpty()) {
                val endpointsArray = JsonArray()
                endpoints.forEach { endpointsArray.add(it) }
                configJson.put("connect", JsonObject().put("endpoints", endpointsArray))
            }
            val config = Config.fromJson(configJson.encode())
            val opened = Zenoh.open(config)
            try {
                val subscriptionKeys = ZenohTopicMapper.minimalFilters(allow)
                    .mapNotNull { ZenohTopicMapper.subscriptionKey(it, localPrefix, prefix) }
                val declared = subscriptionKeys.map { key ->
                    opened.declareSubscriber(KeyExpr.tryFrom(key), Callback<Sample> { sample -> handleSample(sample) })
                }
                session = opened
                subscribers.addAll(declared)
            } catch (error: Exception) {
                opened.close()
                throw error
            }
            null
        }).onSuccess {
            logger.info("Zenoh federation connected as [$brokerId] in [$mode] mode to $endpoints using ${subscribers.map { it.keyExpr }}")
            startPromise.complete()
        }.onFailure { error ->
            startPromise.fail("Unable to start Zenoh federation: ${error.message}")
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        vertx.executeBlocking(Callable<Void?> {
            subscribers.forEach { it.close() }
            subscribers.clear()
            session?.close()
            session = null
            null
        }).onComplete { stopPromise.complete() }
    }

    override fun subscribeToMessageBus(callback: (BrokerMessage) -> Unit): Future<Void> {
        this.callback = callback
        return Future.succeededFuture()
    }

    override fun publishMessageToBus(message: BrokerMessage) {
        if (!isAllowed(message.topicName)) return
        val zenohKey = ZenohTopicMapper.mapToZenohKey(message.topicName, localPrefix, prefix) ?: return
        remember(message.messageUuid)

        try {
            val options = PutOptions().apply {
                encoding = Encoding.APPLICATION_OCTET_STREAM
                reliability = Reliability.RELIABLE
                attachment = ZBytes.from(ZenohMessageEnvelope.encode(brokerId, message))
            }
            session?.put(KeyExpr.tryFrom(zenohKey), ZBytes.from(message.payload), options)
        } catch (error: Exception) {
            logger.warning("Failed to publish [${message.topicName}] to Zenoh: ${error.message}")
        }
    }

    private fun handleSample(sample: Sample) {
        try {
            val key = sample.keyExpr.toString()
            val topic = ZenohTopicMapper.mapToMqttTopic(key, localPrefix, prefix) ?: return
            if (!isAllowed(topic)) return

            val decoded = ZenohMessageEnvelope.decode(topic, sample.payload.toBytes(), sample.attachment?.toBytes())
            if (decoded.origin == brokerId || !remember(decoded.message.messageUuid)) return
            vertx.runOnContext { callback?.invoke(decoded.message) }
        } catch (error: Exception) {
            logger.warning("Failed to consume Zenoh sample: ${error.message}")
        }
    }

    private fun isAllowed(topic: String): Boolean =
        allow.any { TopicTree.matches(it, topic) } && deny.none { TopicTree.matches(it, topic) }

    private fun remember(messageUuid: String): Boolean = synchronized(seenMessages) {
        val now = System.currentTimeMillis()
        val iterator = seenMessages.entries.iterator()
        while (iterator.hasNext()) {
            if (now - iterator.next().value > ttlMs) iterator.remove()
        }
        if (seenMessages.containsKey(messageUuid)) false else {
            seenMessages[messageUuid] = now
            true
        }
    }
}
