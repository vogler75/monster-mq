package at.rocworks.bus

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.data.TopicTree
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import io.vertx.core.json.JsonArray
import io.zenoh.Zenoh
import io.zenoh.Config
import io.zenoh.Session
import io.zenoh.keyexpr.KeyExpr
import io.zenoh.pubsub.Subscriber
import io.zenoh.pubsub.PutOptions
import io.zenoh.bytes.ZBytes
import io.zenoh.sample.Sample
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger
import java.util.Base64
import java.util.concurrent.Callable

class MessageBusZenoh(private val configJson: JsonObject) : AbstractVerticle(), IMessageBus {
    override val isExternal = true
    private val logger = Utils.getLogger(this::class.java)

    private var session: Session? = null
    private var subscriber: Subscriber? = null
    private var cleanupTimerId: Long? = null

    private val prefix: String
    private val allowFilters: List<String>
    private val denyFilters: List<String>
    private val cacheSize: Int
    private val ttlSeconds: Long

    // Bounded cache for deduplication
    private val seenMessageUuids = ConcurrentHashMap<String, Long>()

    init {
        val zenoh = configJson.getJsonObject("Zenoh", JsonObject())
        prefix = zenoh.getString("Prefix", "monstermq/mqtt").trimEnd('/')
        
        allowFilters = zenoh.getJsonArray("Allow")?.map { it.toString() } ?: listOf("#")
        denyFilters = zenoh.getJsonArray("Deny")?.map { it.toString() } ?: listOf("\$SYS/#")
        
        val dedup = zenoh.getJsonObject("Deduplication", JsonObject())
        cacheSize = dedup.getInteger("CacheSize", 100000)
        ttlSeconds = dedup.getLong("TtlSeconds", 300L)
    }

    override fun start(startPromise: Promise<Void>) {
        logger.info("Initializing Zenoh Message Bus...")
        
        // Execute blocking Zenoh session opening on worker thread using Callable
        vertx.executeBlocking(Callable<Session> {
            val zenohConfig = JsonObject()
            val zenohProps = configJson.getJsonObject("Zenoh", JsonObject())
            val mode = zenohProps.getString("Mode", "client")
            zenohConfig.put("mode", mode)

            val connectEndpoints = zenohProps.getJsonArray("Connect") ?: JsonArray()
            if (connectEndpoints.size() > 0) {
                zenohConfig.put("connect", JsonObject().put("endpoints", connectEndpoints))
                // Disable multicast scouting when static endpoints are provided
                zenohConfig.put("scouting", JsonObject().put("multicast", JsonObject().put("enabled", false)))
            }

            val config = Config.fromJson(zenohConfig.encode())
            Zenoh.open(config)
        }).onSuccess { zenohSession ->
            this.session = zenohSession
            logger.info("Zenoh session established successfully.")
            
            // Periodically clean deduplication cache
            cleanupTimerId = vertx.setPeriodic(10000) {
                cleanDeduplicationCache()
            }
            
            startPromise.complete()
        }.onFailure { err ->
            logger.severe("Failed to establish Zenoh session: ${err.message}")
            startPromise.fail(err)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Stopping Zenoh Message Bus...")
        
        cleanupTimerId?.let { vertx.cancelTimer(it) }
        
        vertx.executeBlocking(Callable<Any?> {
            subscriber?.undeclare()
            session?.close()
            null
        }).onComplete {
            logger.info("Zenoh session closed.")
            stopPromise.complete()
        }
    }

    override fun subscribeToMessageBus(callback: (BrokerMessage) -> Unit): Future<Void> {
        val sessionRef = session ?: return Future.failedFuture("Zenoh session is not initialized")
        val promise = Promise.promise<Void>()

        vertx.executeBlocking(Callable<Any?> {
            val keyExpr = KeyExpr.tryFrom("$prefix/**")
            subscriber = sessionRef.declareSubscriber(keyExpr) { sample ->
                try {
                    handleZenohSample(sample, callback)
                } catch (e: Exception) {
                    logger.warning("Error processing received Zenoh publication: ${e.message}")
                    e.printStackTrace()
                }
            }
            null
        }).onComplete { res ->
            if (res.succeeded()) {
                promise.complete()
            } else {
                promise.fail(res.cause())
            }
        }

        return promise.future()
    }

    override fun publishMessageToBus(message: BrokerMessage) {
        val sessionRef = session ?: return
        
        if (!isTopicAllowed(message.topicName)) {
            return
        }

        val localNodeId = Monster.getClusterNodeId(vertx)
        val zenohKey = "$prefix/${message.topicName}"

        // Build metadata JSON envelope
        val metadata = JsonObject()
            .put("messageUuid", message.messageUuid)
            .put("clientId", message.clientId)
            .put("qosLevel", message.qosLevel)
            .put("isRetain", message.isRetain)
            .put("senderId", message.senderId)
            .put("originNodeId", localNodeId)
            .put("time", message.time.toEpochMilli())

        // Save MQTT 5.0 properties if present
        message.messageExpiryInterval?.let { metadata.put("messageExpiryInterval", it) }
        message.payloadFormatIndicator?.let { metadata.put("payloadFormatIndicator", it) }
        message.contentType?.let { metadata.put("contentType", it) }
        message.responseTopic?.let { metadata.put("responseTopic", it) }
        message.correlationData?.let { metadata.put("correlationData", Base64.getEncoder().encodeToString(it)) }
        
        message.userProperties?.let { props ->
            val userPropsJson = JsonObject()
            props.forEach { (k, v) -> userPropsJson.put(k, v) }
            metadata.put("userProperties", userPropsJson)
        }

        // Publish to Zenoh on a Vert.x worker thread
        vertx.executeBlocking(Callable<Any?> {
            val payloadBytes = ZBytes.from(message.payload)
            val putOptions = PutOptions().setAttachment(metadata.encode())
            
            sessionRef.put(KeyExpr.tryFrom(zenohKey), payloadBytes, putOptions)
            null
        }).onFailure { err ->
            logger.warning("Failed to publish message ${message.messageUuid} to Zenoh: ${err.message}")
        }
    }

    private fun handleZenohSample(sample: Sample, callback: (BrokerMessage) -> Unit) {
        val zenohKey = sample.keyExpr.toString()
        if (!zenohKey.startsWith("$prefix/")) {
            return
        }

        val mqttTopic = zenohKey.substring(prefix.length + 1)
        if (!isTopicAllowed(mqttTopic)) {
            return
        }

        // Parse payload
        val payload = sample.payload.toBytes()

        // Extract metadata from attachment if present
        val attachment = sample.attachment
        val metadataJson = if (attachment != null) {
            try {
                JsonObject(attachment.toString())
            } catch (e: Exception) {
                null
            }
        } else null

        val localNodeId = Monster.getClusterNodeId(vertx)
        
        val originNodeId = metadataJson?.getString("originNodeId") ?: "zenoh-native"
        if (originNodeId == localNodeId) {
            // Loop prevention: ignore messages we published
            return
        }

        val messageUuid = metadataJson?.getString("messageUuid") ?: Utils.getUuid()
        
        // Deduplication check
        if (recordAndCheckDuplicate(messageUuid)) {
            return
        }

        val clientId = metadataJson?.getString("clientId") ?: "zenoh"
        val qos = metadataJson?.getInteger("qosLevel") ?: 0
        val isRetain = metadataJson?.getBoolean("isRetain") ?: false
        val senderId = metadataJson?.getString("senderId") ?: "zenoh"
        val timeMillis = metadataJson?.getLong("time") ?: System.currentTimeMillis()

        // MQTT 5 properties
        val messageExpiryInterval = metadataJson?.getLong("messageExpiryInterval")
        val payloadFormatIndicator = metadataJson?.getInteger("payloadFormatIndicator")
        val contentType = metadataJson?.getString("contentType")
        val responseTopic = metadataJson?.getString("responseTopic")
        
        val correlationData = metadataJson?.getString("correlationData")?.let {
            try {
                Base64.getDecoder().decode(it)
            } catch (e: Exception) {
                null
            }
        }

        val userProperties = metadataJson?.getJsonObject("userProperties")?.let { userPropsJson ->
            val map = mutableMapOf<String, String>()
            userPropsJson.fieldNames().forEach { name ->
                map[name] = userPropsJson.getString(name)
            }
            map.toMap()
        }

        val brokerMessage = BrokerMessage(
            messageUuid = messageUuid,
            messageId = 0,
            topicName = mqttTopic,
            payload = payload,
            qosLevel = qos,
            isRetain = isRetain,
            isDup = false,
            isQueued = false,
            clientId = clientId,
            senderId = senderId,
            time = Instant.ofEpochMilli(timeMillis),
            messageExpiryInterval = messageExpiryInterval,
            payloadFormatIndicator = payloadFormatIndicator,
            contentType = contentType,
            responseTopic = responseTopic,
            correlationData = correlationData,
            userProperties = userProperties,
            originNodeId = originNodeId
        )

        // Process message via session handler
        callback(brokerMessage)
    }

    internal fun isTopicAllowed(topic: String): Boolean {
        val matchesAllow = allowFilters.any { TopicTree.matches(it, topic) }
        val matchesDeny = denyFilters.any { TopicTree.matches(it, topic) }
        return matchesAllow && !matchesDeny
    }

    private fun recordAndCheckDuplicate(uuid: String): Boolean {
        val now = System.currentTimeMillis()
        if (seenMessageUuids.containsKey(uuid)) {
            return true
        }
        if (seenMessageUuids.size >= cacheSize) {
            cleanDeduplicationCache()
            if (seenMessageUuids.size >= cacheSize) {
                seenMessageUuids.clear()
            }
        }
        seenMessageUuids[uuid] = now
        return false
    }

    private fun cleanDeduplicationCache() {
        val now = System.currentTimeMillis()
        val expiryMs = ttlSeconds * 1000
        val iterator = seenMessageUuids.entries.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            if (now - entry.value > expiryMs) {
                iterator.remove()
            }
        }
    }
}
