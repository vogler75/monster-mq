package at.rocworks.bus

import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttMessageCodec
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.kafka.client.consumer.KafkaConsumer
import io.vertx.kafka.client.producer.KafkaProducer
import io.vertx.kafka.client.producer.KafkaProducerRecord

class MessageBusKafka(
    private val bootstrapServers: String,
    private val topicName: String
): AbstractVerticle(), IMessageBus {
    private val groupId = "Monster"
    private val configConsumer: MutableMap<String, String> = HashMap()
    private var kafkaProducer: KafkaProducer<String, ByteArray>? = null
    private var kafkaConsumer: KafkaConsumer<String, ByteArray>? = null

    override fun start(startPromise: Promise<Void>) {
        val configProducer: MutableMap<String, String> = HashMap()
        configProducer["bootstrap.servers"] = bootstrapServers
        configProducer["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        configProducer["value.serializer"] = "org.apache.kafka.common.serialization.ByteArraySerializer"
        configProducer["acks"] = "1"

        kafkaProducer = KafkaProducer.create(vertx, configProducer)

        configConsumer["bootstrap.servers"] = bootstrapServers
        configConsumer["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        configConsumer["value.deserializer"] = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
        configConsumer["group.id"] = groupId
        configConsumer["auto.offset.reset"] = "earliest"
        configConsumer["enable.auto.commit"] = "true"
        startPromise.complete()
    }

    override fun subscribeToMessageBus(callback: (MqttMessage)->Unit): Future<Void> {
        try {
            val codec = MqttMessageCodec()
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

    override fun publishMessageToBus(message: MqttMessage) {
            val codec = MqttMessageCodec()
            val buffer = Buffer.buffer()
            codec.encodeToWire(buffer, message)
            val record = KafkaProducerRecord.create<String, ByteArray>(topicName, message.topicName, buffer.bytes)
            kafkaProducer?.send(record)
    }
}