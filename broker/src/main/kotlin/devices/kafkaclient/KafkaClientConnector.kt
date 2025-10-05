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

            logger.info("Starting KafkaClientConnector for device ${device.name} topic='${cfg.topic}' format='${cfg.payloadFormat}'")

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
        consumerConfig["client.id"] = cfg.clientId
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
                        publishPlain(key, payloadBytes)
                    }
                    PayloadFormat.DEFAULT, PayloadFormat.JSON -> {
                        // Placeholder: For now treat like BINARY/TEXT deciding by actual value type, but allowing key transform + fallback
                        val transformedTopic = KafkaClientKeyTopicTransformer.transform(key, cfg.keyTransform, cfg.keyFallbackTopic)
                        if (transformedTopic == null) {
                            messagesDropped.incrementAndGet(); return@handler
                        }
                        val payloadBytes: ByteArray = when (val v = value) {
                            is ByteArray -> v
                            is String -> v.toByteArray(Charsets.UTF_8)
                            else -> return@handler
                        }
                        publishPlain(transformedTopic, payloadBytes)
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

        kafkaConsumer!!.subscribe(cfg.topic).onComplete { ar ->
            if (ar.succeeded()) {
                logger.info("KafkaClientConnector subscribed to ${cfg.topic}")
            } else {
                logger.severe("Failed to subscribe to ${cfg.topic}: ${ar.cause().message}")
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
                qosLevel = cfg.qos,
                isRetain = cfg.retain,
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

    private fun setupMetricsEndpoint() {
        val address = EventBusAddresses.KafkaBridge.connectorMetrics(device.name)
        vertx.eventBus().consumer<JsonObject>(address) { msg ->
            try {
                val now = System.currentTimeMillis()
                val totalIn = messagesIn.get()
                val deltaIn = totalIn - lastMessagesInSnapshot
                val elapsedSec = (now - lastMetricsResetTime) / 1000.0
                val rate = if (elapsedSec > 0) deltaIn / elapsedSec else 0.0
                val obj = JsonObject()
                    .put("device", device.name)
                    .put("messagesIn", totalIn)
                    .put("messagesDropped", messagesDropped.get())
                    .put("errors", errors.get())
                    .put("lastRecordTs", lastRecordTs)
                    .put("ratePerSec", rate)
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
