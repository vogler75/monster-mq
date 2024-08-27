package at.rocworks

import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttMessageCodec
import at.rocworks.shared.RetainedMessages
import at.rocworks.shared.SubscriptionTable
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.kafka.client.consumer.KafkaConsumer
import io.vertx.kafka.client.producer.KafkaProducer
import io.vertx.kafka.client.producer.KafkaProducerRecord

class DistributorKafka(
    private val subscriptionTable: SubscriptionTable,
    private val retainedMessages: RetainedMessages,
    private val kafkaBootstrapServers: String
): Distributor(subscriptionTable, retainedMessages) {

    private var kafkaProducer: KafkaProducer<String, ByteArray>? = null
    private var kafkaConsumer: KafkaConsumer<String, ByteArray>? = null

    override fun start(startPromise: Promise<Void>?) {
        super.start(startPromise)

        val configProducer: MutableMap<String, String> = HashMap()
        configProducer["bootstrap.servers"] = kafkaBootstrapServers
        configProducer["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        configProducer["value.serializer"] = "org.apache.kafka.common.serialization.ByteArraySerializer"
        configProducer["acks"] = "1"

        kafkaProducer = KafkaProducer.create(vertx, configProducer)

        val configConsumer: MutableMap<String, String> = HashMap()
        configConsumer["bootstrap.servers"] = kafkaBootstrapServers
        configConsumer["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        configConsumer["value.deserializer"] = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
        configConsumer["group.id"] = "monster"
        configConsumer["auto.offset.reset"] = "earliest"
        configConsumer["enable.auto.commit"] = "true"

        try {
            val codec = MqttMessageCodec()
            KafkaConsumer.create<String, ByteArray>(vertx, configConsumer).let { consumer ->
                this.kafkaConsumer = consumer
                consumer.handler { record ->
                    consumeMessageFromBus(codec.decodeFromWire(0, Buffer.buffer(record.value())))
                }
                consumer.subscribe("monster")
            }
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    override fun publishMessageToBus(message: MqttMessage) {
        val codec = MqttMessageCodec()
        val buffer = Buffer.buffer()
        codec.encodeToWire(buffer, message)
        val record = KafkaProducerRecord.create<String, ByteArray>("monster", message.topicName, buffer.bytes)
        kafkaProducer?.send(record)
    }
}