package at.rocworks

import at.rocworks.data.*
import at.rocworks.shared.RetainedMessages
import at.rocworks.shared.SubscriptionTable
import io.vertx.core.AbstractVerticle
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.kafka.client.consumer.KafkaConsumer
import io.vertx.kafka.client.producer.KafkaProducer
import io.vertx.kafka.client.producer.KafkaProducerRecord

import java.util.logging.Level
import java.util.logging.Logger

class Distributor(
    private val subscriptionTable: SubscriptionTable,
    private val retainedMessages: RetainedMessages,
    private val useKafkaAsMessageBus: Boolean,
    private val kafkaBootstrapServers: String
): AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    companion object {
        const val COMMAND_KEY = "C"
        const val COMMAND_SUBSCRIBE = "S"
        const val COMMAND_UNSUBSCRIBE = "U"
        const val COMMAND_CLEANSESSION = "C"
    }

    init {
        logger.level = Level.INFO
    }

    private fun getDistributorCommandAddress() = "${Const.GLOBAL_DISTRIBUTOR_NAMESPACE}/${deploymentID()}/C"
    private fun getDistributorMessageAddress() = "${Const.GLOBAL_DISTRIBUTOR_NAMESPACE}/${deploymentID()}/M"

    private var kafkaProducer: KafkaProducer<String, ByteArray>? = null
    private var kafkaConsumer: KafkaConsumer<String, ByteArray>? = null

    override fun start() {
        vertx.eventBus().consumer<JsonObject>(getDistributorCommandAddress()) { message ->
            message.body()?.let { payload ->
                logger.finest { "Received request [${payload}]" }
                when (payload.getString(COMMAND_KEY)) {
                    COMMAND_SUBSCRIBE -> subscribeCommand(message)
                    COMMAND_UNSUBSCRIBE -> unsubscribeCommand(message)
                    COMMAND_CLEANSESSION -> cleanSessionCommand(message)
                }
            }
        }

        vertx.eventBus().consumer<MqttMessage>(getDistributorMessageAddress()) { message ->
            message.body()?.let { payload ->
                logger.finest { "Received message [${payload.topicName}] retained [${payload.isRetain}]" }
                consumeMessage(payload)
            }
        }

        if (useKafkaAsMessageBus) {
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
                        consumeMessage(codec.decodeFromWire(0, Buffer.buffer(record.value())))

                    }
                    consumer.subscribe("monster")
                }
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }

    override fun stop() {

    }

    //----------------------------------------------------------------------------------------------------

    fun subscribeRequest(client: MqttClient, topicName: MqttTopicName) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_SUBSCRIBE)
            .put(Const.TOPIC_KEY, topicName.identifier)
            .put(Const.CLIENT_KEY, client.getClientId().identifier)
        vertx.eventBus().request<Boolean>(getDistributorCommandAddress(), request) {
            if (!it.succeeded())  logger.severe("Unsubscribe request failed: ${it.cause()}")
        }
    }

    private fun subscribeCommand(command: Message<JsonObject>) {
        val clientId = MqttClientId(command.body().getString(Const.CLIENT_KEY))
        val topicName = MqttTopicName(command.body().getString(Const.TOPIC_KEY))

        retainedMessages.findMatching(topicName) { message ->
            logger.finer { "Publish retained message [${message.topicName}]" }
            MqttClient.sendMessageToClient(vertx, clientId, message)
        }.onComplete {
            logger.info("Retained messages published [${it.result()}].")
            subscriptionTable.addSubscription(MqttSubscription(clientId, topicName))
            command.reply(true)
        }

    }

    //----------------------------------------------------------------------------------------------------

    fun unsubscribeRequest(client: MqttClient, topicName: MqttTopicName) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_UNSUBSCRIBE)
            .put(Const.TOPIC_KEY, topicName.identifier)
            .put(Const.CLIENT_KEY, client.deploymentID())
        vertx.eventBus().request<Boolean>(getDistributorCommandAddress(), request) {
            if (!it.succeeded()) logger.severe("Unsubscribe request failed: ${it.cause()}")
        }
    }

    private fun unsubscribeCommand(command: Message<JsonObject>) {
        val clientId = MqttClientId(command.body().getString(Const.CLIENT_KEY))
        val topicName = MqttTopicName(command.body().getString(Const.TOPIC_KEY))
        subscriptionTable.removeSubscription(MqttSubscription(clientId, topicName))
        command.reply(true)
    }

    //----------------------------------------------------------------------------------------------------

    fun cleanSessionRequest(client: MqttClient) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_CLEANSESSION)
            .put(Const.CLIENT_KEY, client.getClientId().identifier)

        vertx.eventBus().request<Boolean>(getDistributorCommandAddress(), request) {
            if (!it.succeeded()) logger.severe("Clean session request failed: ${it.cause()}")
        }
    }

    private fun cleanSessionCommand(command: Message<JsonObject>) {
        val clientId = MqttClientId(command.body().getString(Const.CLIENT_KEY))
        subscriptionTable.removeClient(clientId)
        command.reply(true)
    }

    //----------------------------------------------------------------------------------------------------

    val publishMessageToBus : (MqttMessage) -> Unit = if (useKafkaAsMessageBus) ::publishMessageKafkaBus else ::publishMessageEventBus

    fun publishMessage(message: MqttMessage) {
        publishMessageToBus(message)
        if (message.isRetain) retainedMessages.saveMessage(message)
    }

    private fun publishMessageEventBus(message: MqttMessage) {
        vertx.eventBus().publish(getDistributorMessageAddress(), message)
    }

    private fun publishMessageKafkaBus(message: MqttMessage) {
        val codec = MqttMessageCodec()
        val buffer = Buffer.buffer()
        codec.encodeToWire(buffer, message)
        val record = KafkaProducerRecord.create<String, ByteArray>("monster", message.topicName, buffer.bytes)
        kafkaProducer?.send(record)
    }

    private fun consumeMessage(message: MqttMessage) {
        val topicName = MqttTopicName(message.topicName)
        subscriptionTable.findClients(topicName).forEach {
            MqttClient.sendMessageToClient(vertx, it, message)
        }
    }
}