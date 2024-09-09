package at.rocworks

import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttMessageCodec
import at.rocworks.stores.MessageHandler
import at.rocworks.stores.SessionHandler
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.kafka.client.consumer.KafkaConsumer
import io.vertx.kafka.client.producer.KafkaProducer
import io.vertx.kafka.client.producer.KafkaProducerRecord

class DistributorKafka(
    sessionHandler: SessionHandler,
    messageHandler: MessageHandler,
    private val bootstrapServers: String,
    private val kafkaTopicName: String
): Distributor(sessionHandler, messageHandler) {

    private val kafkaGroupId = "Monster"

    private var kafkaProducer: KafkaProducer<String, ByteArray>? = null
    private var kafkaConsumer: KafkaConsumer<String, ByteArray>? = null

    override fun start(startPromise: Promise<Void>?) {
        super.start(startPromise)

        val configProducer: MutableMap<String, String> = HashMap()
        configProducer["bootstrap.servers"] = bootstrapServers
        configProducer["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        configProducer["value.serializer"] = "org.apache.kafka.common.serialization.ByteArraySerializer"
        configProducer["acks"] = "1"

        kafkaProducer = KafkaProducer.create(vertx, configProducer)

        val configConsumer: MutableMap<String, String> = HashMap()
        configConsumer["bootstrap.servers"] = bootstrapServers
        configConsumer["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        configConsumer["value.deserializer"] = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
        configConsumer["group.id"] = kafkaGroupId
        configConsumer["auto.offset.reset"] = "earliest"
        configConsumer["enable.auto.commit"] = "true"

        try {
            val codec = MqttMessageCodec()
            KafkaConsumer.create<String, ByteArray>(vertx, configConsumer).let { consumer ->
                this.kafkaConsumer = consumer
                consumer.handler { record ->
                    consumeMessageFromBus(codec.decodeFromWire(0, Buffer.buffer(record.value())))
                }
                consumer.subscribe(kafkaTopicName)
            }
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    override fun publishMessageToBus(message: MqttMessage) {
        val codec = MqttMessageCodec()
        val buffer = Buffer.buffer()
        codec.encodeToWire(buffer, message)
        val record = KafkaProducerRecord.create<String, ByteArray>(kafkaTopicName, message.topicName, buffer.bytes)
        kafkaProducer?.send(record)
    }
}