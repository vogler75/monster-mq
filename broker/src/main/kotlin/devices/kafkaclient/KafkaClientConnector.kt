package at.rocworks.devices.kafkaclient

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.data.BrokerMessageCodec
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.devices.KafkaClientConfig
import at.rocworks.stores.devices.PayloadFormat
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.kafka.client.consumer.KafkaConsumer
import io.vertx.kafka.client.producer.KafkaProducer
import io.vertx.kafka.client.producer.KafkaProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Level
import java.util.logging.Logger

/**
 * KafkaClientConnector
 *
 * Per-device bidirectional bridge between the local MQTT broker and a Kafka cluster.
 *
 * Inbound (Kafka → MQTT):
 * Consumes records from a Kafka topic and republishes them to the local MQTT broker.
 * Topic is derived from device.namespace.
 * Supports four payload formats (DEFAULT, JSON, BINARY, TEXT).
 *
 * Outbound (MQTT → Kafka):
 * Subscribes to local MQTT topic filters and publishes/writes them to an external Kafka topic.
 * Dest Kafka topic defaults to device.namespace but can be overridden.
 */
class KafkaClientConnector : AbstractVerticle() {
    private val logger: Logger = Utils.getLogger(this::class.java)

    private lateinit var device: DeviceConfig
    private lateinit var cfg: KafkaClientConfig

    // Metrics
    private val messagesIn = AtomicLong(0)
    private val messagesOut = AtomicLong(0)
    private val messagesDropped = AtomicLong(0)
    private val errors = AtomicLong(0)
    private var lastRecordTs: Long = 0
    private var lastMetricsResetTime: Long = System.currentTimeMillis()
    private var lastMessagesInSnapshot: Long = 0
    private var lastMessagesOutSnapshot: Long = 0

    private var kafkaConsumer: KafkaConsumer<String, Any>? = null
    private var kafkaProducer: KafkaProducer<String, Any>? = null

    private val internalClientId get() = "kafkaclient-${device.name}"

    override fun start(startPromise: Promise<Void>) {
        try {
            val cfgJson = config().getJsonObject("device")
            device = DeviceConfig.fromJsonObject(cfgJson)

            // Parse Kafka client config from device.config. Supports either direct fields or nested under "kafka".
            val kafkaConfigJson = device.config.getJsonObject("kafka") ?: device.config
            cfg = KafkaClientConfig.fromJson(kafkaConfigJson)
            val validationErrors = cfg.validate()
            if (validationErrors.isNotEmpty()) {
                throw IllegalArgumentException("KafkaClient config errors: ${validationErrors.joinToString(", ")}")
            }

            val kafkaTopic = device.namespace // Derive Kafka topic from device namespace
            logger.info("Starting KafkaClientConnector for device ${device.name} topic='${kafkaTopic}' format='${cfg.payloadFormat}' inboundEnabled=${cfg.inboundEnabled} outboundEnabled=${cfg.outboundEnabled}")

            if (cfg.inboundEnabled) {
                setupKafkaConsumer()
            }

            if (cfg.outboundEnabled) {
                setupKafkaProducer()
                setupOutboundSubscriptions()
            }

            setupMetricsEndpoint()

            startPromise.complete()
        } catch (e: Exception) {
            logger.log(Level.SEVERE, "Failed to start KafkaClientConnector: ${e.message}", e)
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        try {
            val futures = mutableListOf<Future<*>>()
            kafkaConsumer?.let { futures.add(it.close()) }
            kafkaProducer?.let { futures.add(it.close()) }

            val sessionHandler = Monster.getSessionHandler()
            if (sessionHandler != null && cfg.outboundEnabled) {
                cfg.outboundTopicFilters.forEach { filter ->
                    sessionHandler.unsubscribeInternalClient(internalClientId, filter)
                }
                sessionHandler.unregisterInternalClient(internalClientId)
            }

            if (futures.isEmpty()) {
                stopPromise.complete()
            } else {
                Future.all(futures).onComplete { stopPromise.complete() }
            }
        } catch (_: Exception) {
            stopPromise.complete()
        }
    }

    private fun setupKafkaConsumer() {
        // Base config
        val consumerConfig = mutableMapOf<String, String>()
        consumerConfig["bootstrap.servers"] = cfg.bootstrapServers
        consumerConfig["group.id"] = cfg.groupId
        consumerConfig["client.id"] = "kafkaclient-${device.name}"
        consumerConfig["enable.auto.commit"] = "true"
        consumerConfig["auto.offset.reset"] = "latest"

        // Key deserializer always String
        consumerConfig["key.deserializer"] = StringDeserializer::class.java.name

        // Value deserializer based on payload format
        val useStringValue = cfg.payloadFormat == PayloadFormat.TEXT || cfg.payloadFormat == PayloadFormat.JSON
        consumerConfig["value.deserializer"] = if (useStringValue) StringDeserializer::class.java.name else ByteArrayDeserializer::class.java.name

        // Merge extra config (override defaults)
        consumerConfig.putAll(cfg.extraConsumerConfig)

        kafkaConsumer = KafkaConsumer.create(vertx, consumerConfig)

        kafkaConsumer!!.handler { record ->
            try {
                val key = record.key()
                val value = record.value()
                logger.fine { "Kafka record received: key=${key?.take(200)}, valueType=${value?.javaClass?.simpleName}, partition=${record.partition()}, offset=${record.offset()}" }

                when (cfg.payloadFormat) {
                    PayloadFormat.BINARY, PayloadFormat.TEXT -> {
                        // topic = key, drop if key null
                        if (key == null) {
                            logger.fine { "Dropping record with null key (format=${cfg.payloadFormat})" }
                            messagesDropped.incrementAndGet(); return@handler
                        }
                        val payloadBytes: ByteArray = when (cfg.payloadFormat) {
                            PayloadFormat.BINARY -> value as? ByteArray ?: run { messagesDropped.incrementAndGet(); return@handler }
                            PayloadFormat.TEXT -> (value as? String)?.toByteArray(Charsets.UTF_8) ?: run { messagesDropped.incrementAndGet(); return@handler }
                            else -> ByteArray(0)
                        }
                        val topic = applyDestinationPrefix(applyTopicKeyRegex(key))
                        publishPlain(topic, payloadBytes)
                    }
                    PayloadFormat.DEFAULT, PayloadFormat.JSON -> {
                        // For DEFAULT/JSON we expect the value to be an encoded BrokerMessage (binary or JSON serialized).
                        // If JSON, it's a JsonObject representation of BrokerMessage fields.
                        try {
                            when (value) {
                                is ByteArray -> {
                                    // Decode using BrokerMessageCodec
                                    try {
                                        val buffer = Buffer.buffer(value)
                                        val decoded = BrokerMessageCodec().decodeFromWire(0, buffer)
                                        publishDecoded(decoded)
                                    } catch (e: Exception) {
                                        logger.log(Level.WARNING, "Failed to decode binary data from topic '${device.namespace}' as BrokerMessageCodec format: ${e.message} — check the payloadFormat setting (current: ${cfg.payloadFormat}). Trying JSON fallback...")
                                        try {
                                            val json = JsonObject(String(value, Charsets.UTF_8))
                                            val topic = json.getString("topicName") ?: json.getString("topic") ?: run { messagesDropped.incrementAndGet(); return@handler }
                                            val payloadBase64 = json.getString("payloadBase64")
                                            val payloadText = json.getString("payload")
                                            val payloadBytes = when {
                                                payloadBase64 != null -> java.util.Base64.getDecoder().decode(payloadBase64)
                                                payloadText != null -> payloadText.toByteArray(Charsets.UTF_8)
                                                else -> ByteArray(0)
                                            }
                                            val decoded = BrokerMessage(
                                                messageUuid = json.getString("messageUuid") ?: Utils.getUuid(),
                                                messageId = json.getInteger("messageId", 0),
                                                topicName = topic,
                                                payload = payloadBytes,
                                                qosLevel = json.getInteger("qosLevel", 0),
                                                isRetain = json.getBoolean("isRetain", false),
                                                isDup = json.getBoolean("isDup", false),
                                                isQueued = json.getBoolean("isQueued", false),
                                                clientId = json.getString("clientId") ?: "kafkaclient-${device.name}",
                                                time = try { Instant.parse(json.getString("time")) } catch (_: Exception) { Instant.now() }
                                            )
                                            publishDecoded(decoded)
                                        } catch (e2: Exception) {
                                            logger.log(Level.WARNING, "JSON fallback also failed for topic '${device.namespace}': ${e2.message} — skipping message.")
                                            messagesDropped.incrementAndGet(); errors.incrementAndGet()
                                        }
                                    }
                                }
                                is String -> {
                                    // Treat as JSON serialized BrokerMessage
                                    val json = JsonObject(value)
                                    val topic = json.getString("topicName") ?: json.getString("topic") ?: run { messagesDropped.incrementAndGet(); return@handler }
                                    val payloadBase64 = json.getString("payloadBase64")
                                    val payloadText = json.getString("payload")
                                    val payloadBytes = when {
                                        payloadBase64 != null -> java.util.Base64.getDecoder().decode(payloadBase64)
                                        payloadText != null -> payloadText.toByteArray(Charsets.UTF_8)
                                        else -> ByteArray(0)
                                    }
                                    val decoded = BrokerMessage(
                                        messageUuid = json.getString("messageUuid") ?: Utils.getUuid(),
                                        messageId = json.getInteger("messageId", 0),
                                        topicName = topic,
                                        payload = payloadBytes,
                                        qosLevel = json.getInteger("qosLevel", 0),
                                        isRetain = json.getBoolean("isRetain", false),
                                        isDup = json.getBoolean("isDup", false),
                                        isQueued = json.getBoolean("isQueued", false),
                                        clientId = json.getString("clientId") ?: "kafkaclient-${device.name}",
                                        time = try { Instant.parse(json.getString("time")) } catch (_: Exception) { Instant.now() }
                                    )
                                    publishDecoded(decoded)
                                }
                                else -> {
                                    messagesDropped.incrementAndGet(); return@handler
                                }
                            }
                        } catch (e: Exception) {
                            messagesDropped.incrementAndGet(); errors.incrementAndGet();
                            logger.log(Level.WARNING, "Failed to decode DEFAULT/JSON payload: ${e.message}")
                        }
                    }
                    else -> {
                        messagesDropped.incrementAndGet()
                    }
                }
                messagesIn.incrementAndGet()
                lastRecordTs = System.currentTimeMillis()
            } catch (ex: Exception) {
                errors.incrementAndGet()
                logger.log(Level.WARNING, "Error processing Kafka record: ${ex.message}", ex)
            }
        }

        kafkaConsumer!!.exceptionHandler { ex ->
            errors.incrementAndGet()
            logger.log(Level.WARNING, "Kafka consumer error: ${ex.message}", ex)
        }

        val kafkaTopic = device.namespace
        kafkaConsumer!!.subscribe(kafkaTopic).onComplete { ar ->
            if (ar.succeeded()) {
                logger.info("KafkaClientConnector subscribed to $kafkaTopic")
            } else {
                logger.severe("Failed to subscribe to $kafkaTopic: ${ar.cause().message}")
            }
        }
    }

    private fun setupKafkaProducer() {
        val producerConfig = mutableMapOf<String, String>()
        producerConfig["bootstrap.servers"] = cfg.bootstrapServers
        producerConfig["client.id"] = "kafkaclient-producer-${device.name}"
        producerConfig["key.serializer"] = StringSerializer::class.java.name

        // Value serializer based on payload format
        val useStringValue = cfg.payloadFormat == PayloadFormat.TEXT || cfg.payloadFormat == PayloadFormat.JSON
        producerConfig["value.serializer"] = if (useStringValue) StringSerializer::class.java.name else ByteArraySerializer::class.java.name

        // Merge extra config (override defaults)
        producerConfig.putAll(cfg.extraProducerConfig)

        kafkaProducer = KafkaProducer.create(vertx, producerConfig)

        kafkaProducer!!.exceptionHandler { ex ->
            errors.incrementAndGet()
            logger.log(Level.WARNING, "Kafka producer error for device ${device.name}: ${ex.message}", ex)
        }
    }

    private fun setupOutboundSubscriptions() {
        if (cfg.outboundTopicFilters.isEmpty()) return

        // Consume local MQTT messages via EventBus
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
            cfg.outboundTopicFilters.forEach { filter ->
                logger.info("Internal MQTT subscription for outbound Kafka bridge '$internalClientId' -> topic '$filter'")
                sessionHandler.subscribeInternalClient(internalClientId, filter, 0)
            }
        } else {
            logger.severe("SessionHandler not available for outbound Kafka subscriptions")
        }
    }

    private fun applyOutboundTopicKeyRegex(key: String): String {
        val pattern = cfg.outboundTopicKeyRegex ?: return key
        val replacement = cfg.outboundTopicKeyReplacement ?: return key
        return Regex(pattern).replace(key, replacement)
    }

    private fun handleOutboundMessage(msg: BrokerMessage) {
        logger.finer { "Outbound message for '${device.name}': topic='${msg.topicName}' sender='${msg.senderId}' client='${msg.clientId}'" }

        // Loop prevention: skip messages we published ourselves
        if (msg.senderId == internalClientId || msg.clientId == internalClientId) return

        val producer = kafkaProducer ?: return
        val destTopic = cfg.outboundKafkaTopic?.takeIf { it.isNotBlank() } ?: device.namespace
        val recordKey = applyOutboundTopicKeyRegex(msg.topicName)

        try {
            when (cfg.payloadFormat) {
                PayloadFormat.DEFAULT -> {
                    // Encode entire BrokerMessage using BrokerMessageCodec
                    val codec = BrokerMessageCodec()
                    val buffer = Buffer.buffer()
                    codec.encodeToWire(buffer, msg)
                    val record = KafkaProducerRecord.create<String, Any>(destTopic, recordKey, buffer.bytes)
                    producer.send(record)
                }
                PayloadFormat.JSON -> {
                    // Serialize BrokerMessage to JSON
                    val json = JsonObject()
                        .put("messageUuid", msg.messageUuid)
                        .put("messageId", msg.messageId)
                        .put("topic", msg.topicName)
                        .put("payloadBase64", java.util.Base64.getEncoder().encodeToString(msg.payload))
                        .put("qosLevel", msg.qosLevel)
                        .put("isRetain", msg.isRetain)
                        .put("isDup", msg.isDup)
                        .put("isQueued", msg.isQueued)
                        .put("clientId", msg.clientId)
                        .put("time", msg.time.toString())
                    val record = KafkaProducerRecord.create<String, Any>(destTopic, recordKey, json.encode())
                    producer.send(record)
                }
                PayloadFormat.BINARY -> {
                    // Raw payload bytes -> Kafka record value
                    val record = KafkaProducerRecord.create<String, Any>(destTopic, recordKey, msg.payload)
                    producer.send(record)
                }
                PayloadFormat.TEXT -> {
                    // Plain string -> Kafka record value
                    val text = String(msg.payload, Charsets.UTF_8)
                    val record = KafkaProducerRecord.create<String, Any>(destTopic, recordKey, text)
                    producer.send(record)
                }
            }
            messagesOut.incrementAndGet()
            lastRecordTs = System.currentTimeMillis()
        } catch (e: Exception) {
            errors.incrementAndGet()
            logger.log(Level.WARNING, "Failed to publish outbound message to Kafka for device ${device.name}: ${e.message}", e)
        }
    }

    private fun publishPlain(topic: String, payload: ByteArray) {
        try {
            val sessionHandler = Monster.getSessionHandler() ?: run {
                logger.warning("SessionHandler not available, cannot publish to $topic")
                return
            }
            val msg = BrokerMessage(
                messageUuid = Utils.getUuid(),
                messageId = 0,
                topicName = topic,
                payload = payload,
                qosLevel = 0,
                isRetain = false,
                isDup = false,
                isQueued = false,
                clientId = "kafkaclient-${device.name}",
                time = Instant.now(),
                senderId = Monster.getClusterNodeId(vertx)
            )
            logger.fine { "Publishing to MQTT topic=$topic payloadSize=${payload.size}" }
            sessionHandler.publishMessage(msg)
        } catch (e: Exception) {
            errors.incrementAndGet()
            logger.log(Level.WARNING, "Failed to publish message: ${e.message}", e)
        }
    }

    private fun applyTopicKeyRegex(key: String): String {
        val pattern = cfg.topicKeyRegex ?: return key
        val replacement = cfg.topicKeyReplacement ?: return key
        return Regex(pattern).replace(key, replacement)
    }

    private fun applyDestinationPrefix(topic: String): String {
        val prefix = cfg.destinationTopicPrefix
        return if (prefix == null) topic else prefix + topic.trimStart('/')
    }

    private fun publishDecoded(msg: BrokerMessage) {
        try {
            val sessionHandler = Monster.getSessionHandler() ?: run {
                logger.warning("SessionHandler not available, cannot publish decoded message to ${msg.topicName}")
                return
            }
            // Overwrite sender to current node to reflect re-publication origin
            val republished = BrokerMessage(
                messageUuid = msg.messageUuid,
                messageId = msg.messageId,
                topicName = applyDestinationPrefix(msg.topicName),
                payload = msg.payload,
                qosLevel = msg.qosLevel,
                isRetain = msg.isRetain,
                isDup = msg.isDup,
                isQueued = msg.isQueued,
                clientId = msg.clientId,
                time = msg.time,
                senderId = Monster.getClusterNodeId(vertx)
            )
            logger.fine { "Publishing decoded message to MQTT topic=${republished.topicName} payloadSize=${republished.payload.size}" }
            sessionHandler.publishMessage(republished)
        } catch (e: Exception) {
            errors.incrementAndGet()
            logger.log(Level.WARNING, "Failed to publish decoded message: ${e.message}", e)
        }
    }

    private fun setupMetricsEndpoint() {
        val address = EventBusAddresses.KafkaBridge.connectorMetrics(device.name)
        vertx.eventBus().consumer<JsonObject>(address) { msg ->
            try {
                val now = System.currentTimeMillis()
                val totalIn = messagesIn.get()
                val totalOut = messagesOut.get()
                val deltaIn = totalIn - lastMessagesInSnapshot
                val deltaOut = totalOut - lastMessagesOutSnapshot
                val elapsedSec = (now - lastMetricsResetTime) / 1000.0
                val inRate = if (elapsedSec > 0) deltaIn / elapsedSec else 0.0
                val outRate = if (elapsedSec > 0) deltaOut / elapsedSec else 0.0
                val obj = JsonObject()
                    .put("device", device.name)
                    // Cumulative counters (backward compatibility / diagnostics)
                    .put("messagesIn", totalIn)
                    .put("messagesOut", totalOut)
                    .put("messagesDropped", messagesDropped.get())
                    .put("errors", errors.get())
                    .put("lastRecordTs", lastRecordTs)
                    // New standardized rate fields (align with MQTT & OPC UA connectors)
                    .put("messagesInRate", inRate)
                    .put("messagesOutRate", outRate)
                    // Legacy field kept temporarily for compatibility
                    .put("ratePerSec", inRate)
                msg.reply(obj)
                // Reset snapshot each metrics call
                lastMessagesInSnapshot = totalIn
                lastMessagesOutSnapshot = totalOut
                lastMetricsResetTime = now
            } catch (e: Exception) {
                msg.fail(500, e.message)
            }
        }
    }
}
