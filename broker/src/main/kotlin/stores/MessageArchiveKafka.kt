package at.rocworks.stores

import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttMessageCodec
import at.rocworks.stores.PurgeResult
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonArray
import io.vertx.kafka.client.producer.KafkaProducer
import io.vertx.kafka.client.producer.KafkaProducerRecord
import java.time.Instant
import java.util.concurrent.Callable
import kotlin.collections.forEach

class MessageArchiveKafka(
    private val name: String,
    private val bootstrapServers: String
): AbstractVerticle(), IMessageArchive {
    private val logger = Utils.getLogger(this::class.java, name)
    private val topicName = name
    private var kafkaProducer: KafkaProducer<String, ByteArray>? = null

    override fun getName(): String = name
    override fun getType() = MessageArchiveType.KAFKA

    override fun start(startPromise: Promise<Void>) {
        vertx.executeBlocking(Callable {
            val configProducer: MutableMap<String, String> = HashMap()
            configProducer["bootstrap.servers"] = bootstrapServers
            configProducer["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
            configProducer["value.serializer"] = "org.apache.kafka.common.serialization.ByteArraySerializer"
            configProducer["acks"] = "1"

            kafkaProducer = KafkaProducer.create(vertx, configProducer)

            logger.info("Message store [$name] is ready [${Utils.getCurrentFunctionName()}]")
            startPromise.complete()
        })
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
}