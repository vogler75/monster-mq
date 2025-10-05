package at.rocworks.devices.kafkaclient

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.DeviceConfig
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import io.vertx.kafka.client.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Instant
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Level
import java.util.logging.Logger

/**
 * KafkaClientConnector
 *
 * Consumes records from a Kafka topic and republishes them to the local MQTT broker.
 * Supports four payload formats:
 * - DEFAULT / JSON: expect internal encoded MqttMessage in record value (not implemented yet, placeholder)
 * - BINARY: raw value bytes -> MQTT payload, MQTT topic = record key (drop if key null)
 * - TEXT: UTF-8 value text -> MQTT payload bytes, MQTT topic = record key (drop if key null)
 */
class KafkaClientConnector : AbstractVerticle() {
    private val logger: Logger = Utils.getLogger(this::class.java)

    private lateinit var device: DeviceConfig
    private lateinit var cfg: KafkaClientConfig

    // Metrics
    private val messagesIn = AtomicLong(0)
    private val messagesDropped = AtomicLong(0)
    private val errors = AtomicLong(0)
    private var lastRecordTs: Long = 0
    private var lastMetricsResetTime: Long = System.currentTimeMillis()
    private var lastMessagesInSnapshot: Long = 0

    private var kafkaConsumer: KafkaConsumer<String, Any>? = null

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
            logger.info("Starting KafkaClientConnector for device ${device.name} topic='${kafkaTopic}' format='${cfg.payloadFormat}'")

            setupKafkaConsumer()
            setupMetricsEndpoint()

            startPromise.complete()
        } catch (e: Exception) {
            logger.log(Level.SEVERE, "Failed to start KafkaClientConnector: ${e.message}", e)
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        try {
            kafkaConsumer?.close()?.onComplete { stopPromise.complete() } ?: stopPromise.complete()
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

                when (cfg.payloadFormat) {
                    PayloadFormat.BINARY, PayloadFormat.TEXT -> {
                        // topic = key, drop if key null
                        if (key == null) {
                            messagesDropped.incrementAndGet(); return@handler
                        }
                        val payloadBytes: ByteArray = when (cfg.payloadFormat) {
                            PayloadFormat.BINARY -> value as? ByteArray ?: run { messagesDropped.incrementAndGet(); return@handler }
                            PayloadFormat.TEXT -> (value as? String)?.toByteArray(Charsets.UTF_8) ?: run { messagesDropped.incrementAndGet(); return@handler }
                            else -> ByteArray(0)
                        }
                        val topic = applyDestinationPrefix(key)
                        publishPlain(topic, payloadBytes)
                    }
                    PayloadFormat.DEFAULT, PayloadFormat.JSON -> {
                        // For DEFAULT/JSON we expect the value to be an encoded BrokerMessage (binary or JSON serialized).
                        // If JSON, it's a JsonObject representation of BrokerMessage fields.
                        try {
                            when (value) {
                                is ByteArray -> {
                                    // Decode using BrokerMessageCodec
                                    val buffer = io.vertx.core.buffer.Buffer.buffer(value)
                                    val decoded = at.rocworks.data.BrokerMessageCodec().decodeFromWire(0, buffer)
                                    publishDecoded(decoded)
                                }
                                is String -> {
                                    // Treat as JSON serialized BrokerMessage
                                    val json = io.vertx.core.json.JsonObject(value)
                                    val topic = json.getString("topicName") ?: json.getString("topic") ?: run { messagesDropped.incrementAndGet(); return@handler }
                                    val payloadBase64 = json.getString("payloadBase64")
                                    val payloadText = json.getString("payload")
                                    val payloadBytes = when {
                                        payloadBase64 != null -> java.util.Base64.getDecoder().decode(payloadBase64)
                                        payloadText != null -> payloadText.toByteArray(Charsets.UTF_8)
                                        else -> ByteArray(0)
                                    }
                                    val decoded = at.rocworks.data.BrokerMessage(
                                        messageUuid = json.getString("messageUuid") ?: Utils.getUuid(),
                                        messageId = json.getInteger("messageId", 0),
                                        topicName = topic,
                                        payload = payloadBytes,
                                        qosLevel = json.getInteger("qosLevel", 0),
                                        isRetain = json.getBoolean("isRetain", false),
                                        isDup = json.getBoolean("isDup", false),
                                        isQueued = json.getBoolean("isQueued", false),
                                        clientId = json.getString("clientId") ?: "kafkaclient-${device.name}",
                                        time = try { java.time.Instant.parse(json.getString("time")) } catch (_: Exception) { java.time.Instant.now() }
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

    private fun publishPlain(topic: String, payload: ByteArray) {
        try {
            val sessionHandler = Monster.getSessionHandler() ?: return
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
                sender = Monster.getClusterNodeId(vertx)
            )
            sessionHandler.publishMessage(msg)
        } catch (e: Exception) {
            errors.incrementAndGet()
            logger.log(Level.WARNING, "Failed to publish message: ${e.message}", e)
        }
    }

    private fun applyDestinationPrefix(topic: String): String {
        val prefix = cfg.destinationTopicPrefix
        return if (prefix == null) topic else prefix + topic.trimStart('/')
    }

    private fun publishDecoded(msg: BrokerMessage) {
        try {
            val sessionHandler = Monster.getSessionHandler() ?: return
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
                sender = Monster.getClusterNodeId(vertx)
            )
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
                val deltaIn = totalIn - lastMessagesInSnapshot
                val elapsedSec = (now - lastMetricsResetTime) / 1000.0
                val inRate = if (elapsedSec > 0) deltaIn / elapsedSec else 0.0
                val outRate = 0.0 // Currently no outbound path implemented
                val obj = JsonObject()
                    .put("device", device.name)
                    // Cumulative counters (backward compatibility / diagnostics)
                    .put("messagesIn", totalIn)
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
                lastMetricsResetTime = now
            } catch (e: Exception) {
                msg.fail(500, e.message)
            }
        }
    }
}
