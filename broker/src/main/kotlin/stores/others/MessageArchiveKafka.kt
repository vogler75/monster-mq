package at.rocworks.stores

import at.rocworks.Utils
import at.rocworks.bus.KafkaConfigBuilder
import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttMessageCodec
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
    private val userConfig: JsonObject? = null
): AbstractVerticle(), IMessageArchive {
    private val logger = Utils.getLogger(this::class.java, name)
    private val topicName = name
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
            logger.fine("Kafka connectivity test passed for [$name]")
        } catch (e: Exception) {
            isConnected = false
            logger.warning("Kafka connectivity test failed for [$name]: ${e.message}")
            throw e
        }
    }

    override fun addHistory(messages: List<MqttMessage>) {
        val codec = MqttMessageCodec()
        messages.forEach { message ->
            val buffer = Buffer.buffer()
            codec.encodeToWire(buffer, message)
            kafkaProducer?.write(KafkaProducerRecord.create(topicName, message.topicName, buffer.bytes))
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
            logger.fine("Kafka connection check failed: ${e.message}")
            false
        }
    }
}