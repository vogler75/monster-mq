package at.rocworks.stores

import at.rocworks.Utils
import at.rocworks.bus.KafkaConfigBuilder
import at.rocworks.data.BrokerMessage
import at.rocworks.data.BrokerMessageCodec
import at.rocworks.data.PurgeResult
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.kafka.client.producer.KafkaProducer
import io.vertx.kafka.client.producer.KafkaProducerRecord
import java.time.Instant
import java.util.concurrent.Callable
import kotlin.collections.forEach

class MessageArchiveKafka(
    private val name: String,
    private val bootstrapServers: String,
    private val userConfig: JsonObject? = null,
    private val payloadFormat: PayloadFormat = PayloadFormat.DEFAULT
): AbstractVerticle(), IMessageArchive {
    private val logger = Utils.getLogger(this::class.java, name)
    private val topicName = name.removeSuffix("Archive")
    private var kafkaProducer: KafkaProducer<String, ByteArray>? = null
    private var isConnected: Boolean = false

    override fun getName(): String = name
    override fun getType() = MessageArchiveType.KAFKA

    override fun start(startPromise: Promise<Void>) {
        vertx.executeBlocking(Callable {
            try {
                // Build producer config using KafkaConfigBuilder
                val configProducer = KafkaConfigBuilder.buildProducerConfig(bootstrapServers, userConfig)

                kafkaProducer = KafkaProducer.create(vertx, configProducer)

                // Test connectivity by trying to get metadata
                testKafkaConnectivity()

                logger.info("Kafka message archive [$name] started successfully")
                startPromise.complete()
            } catch (e: Exception) {
                logger.severe("Failed to start Kafka message archive [$name]: ${e.message}")
                isConnected = false
                kafkaProducer?.close()
                kafkaProducer = null
                startPromise.fail(e)
            }
        })
    }

    override fun stop(stopPromise: Promise<Void>) {
        try {
            isConnected = false
            kafkaProducer?.let { producer ->
                // Close the Kafka producer
                producer.close()
                logger.info("Kafka message archive [$name] stopped successfully")
            }
            kafkaProducer = null
            stopPromise.complete()
        } catch (e: Exception) {
            logger.warning("Error stopping Kafka message archive [$name]: ${e.message}")
            kafkaProducer = null
            stopPromise.complete() // Complete anyway to avoid hanging
        }
    }

    private fun testKafkaConnectivity() {
        try {
            // Test connectivity by sending a test record to verify Kafka is reachable
            kafkaProducer?.write(KafkaProducerRecord.create("__test_connectivity_$topicName", "test", "test".toByteArray()))?.result()
            isConnected = true
            logger.fine { "Kafka connectivity test passed for [$name]" }
        } catch (e: Exception) {
            isConnected = false
            logger.warning("Kafka connectivity test failed for [$name]: ${e.message}")
            throw e
        }
    }

    override fun addHistory(messages: List<BrokerMessage>) {
        messages.forEach { message ->
            val data = when (payloadFormat) {
                PayloadFormat.DEFAULT -> {
                    val codec = BrokerMessageCodec()
                    val buffer = Buffer.buffer()
                    codec.encodeToWire(buffer, message)
                    buffer.bytes
                }
                PayloadFormat.JSON -> {
                    val jsonObject = JsonObject()
                        .put("messageUuid", message.messageUuid)
                        .put("messageId", message.messageId)
                        .put("topicName", message.topicName)
                        .put("qosLevel", message.qosLevel)
                        .put("isRetain", message.isRetain)
                        .put("isDup", message.isDup)
                        .put("isQueued", message.isQueued)
                        .put("clientId", message.clientId)
                        .put("time", message.time.toString())
                        .put("sender", message.senderId)

                    // Handle payload - check if it's valid JSON
                    val payloadJsonValue = message.getPayloadAsJsonValue()
                    if (payloadJsonValue != null) {
                        jsonObject.put("payload_json", payloadJsonValue)
                    } else {
                        jsonObject.put("payload_base64", message.getPayloadAsBase64())
                    }

                    jsonObject.toString().toByteArray()
                }
            }
            kafkaProducer?.write(KafkaProducerRecord.create(topicName, message.topicName, data))
        }
    }

    override fun purgeOldMessages(olderThan: Instant): PurgeResult {
        logger.warning("purgeOldMessages not yet implemented for Kafka message archive [$name]")
        // TODO: Implement message purging for Kafka archives
        return PurgeResult(0, 0)
    }

    override fun dropStorage(): Boolean {
        logger.warning("dropStorage not implemented for Kafka message archive [$name] - Kafka topics are managed externally")
        // Note: Kafka topics should be managed through Kafka administration tools
        // We cannot drop Kafka topics from within the application safely
        return true
    }

    override fun getConnectionStatus(): Boolean {
        return try {
            isConnected && kafkaProducer != null
        } catch (e: Exception) {
            logger.fine { "Kafka connection check failed: ${e.message}" }
            false
        }
    }
}