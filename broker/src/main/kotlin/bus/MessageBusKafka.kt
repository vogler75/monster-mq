package at.rocworks.bus

import at.rocworks.data.BrokerMessage
import at.rocworks.data.BrokerMessageCodec
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.kafka.client.consumer.KafkaConsumer
import io.vertx.kafka.client.producer.KafkaProducer
import io.vertx.kafka.client.producer.KafkaProducerRecord

class MessageBusKafka(
    private val bootstrapServers: String,
    private val topicName: String,
    private val userConfig: JsonObject? = null
): AbstractVerticle(), IMessageBus {
    private val groupId = "Monster"
    private val configConsumer: Map<String, String>
    private var kafkaProducer: KafkaProducer<String, ByteArray>? = null
    private var kafkaConsumer: KafkaConsumer<String, ByteArray>? = null

    init {
        // Build consumer config at initialization time
        configConsumer = KafkaConfigBuilder.buildConsumerConfig(bootstrapServers, groupId, userConfig)
    }

    override fun start(startPromise: Promise<Void>) {
        // Build producer config
        val configProducer = KafkaConfigBuilder.buildProducerConfig(bootstrapServers, userConfig)

        kafkaProducer = KafkaProducer.create(vertx, configProducer)
        startPromise.complete()
    }

    override fun subscribeToMessageBus(callback: (BrokerMessage)->Unit): Future<Void> {
        try {
            val codec = BrokerMessageCodec()
            KafkaConsumer.create<String, ByteArray>(vertx, configConsumer).let { consumer ->
                this.kafkaConsumer = consumer
                consumer.handler { record ->
                    callback(codec.decodeFromWire(0, Buffer.buffer(record.value())))
                }
                consumer.subscribe(topicName)
            }
        } catch (e: Exception) {
            e.printStackTrace()
        }

        return Future.succeededFuture()
    }

    override fun publishMessageToBus(message: BrokerMessage) {
            val codec = BrokerMessageCodec()
            val buffer = Buffer.buffer()
            codec.encodeToWire(buffer, message)
            val record = KafkaProducerRecord.create<String, ByteArray>(topicName, message.topicName, buffer.bytes)
            kafkaProducer?.send(record)
    }
}