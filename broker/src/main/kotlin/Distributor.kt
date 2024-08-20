package at.rocworks

import at.rocworks.data.MqttClientId
import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttSubscription
import at.rocworks.data.MqttTopicName
import at.rocworks.shared.RetainedMessages
import at.rocworks.shared.SubscriptionTable
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.eventbus.Message
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.core.shareddata.AsyncMap

import java.util.logging.Level
import java.util.logging.Logger

class Distributor(
    private val subscriptionTable: SubscriptionTable,
    private val retainedMessages: RetainedMessages
): AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    companion object {
        const val COMMAND_KEY = "C"
        const val COMMAND_SUBSCRIBE = "S"
        const val COMMAND_UNSUBSCRIBE = "U"
        const val COMMAND_CLEANSESSION = "C"
    }

    init {
        logger.level = Level.ALL
    }

    private fun getDistributorCommandAddress() = "${Const.GLOBAL_DISTRIBUTOR_NAMESPACE}/${deploymentID()}/C"
    private fun getDistributorMessageAddress() = "${Const.GLOBAL_DISTRIBUTOR_NAMESPACE}/${deploymentID()}/M"

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
                distributeMessageToClients(payload)
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

    fun publishMessage(message: MqttMessage) {
        vertx.eventBus().publish(getDistributorMessageAddress(), message)
        if (message.isRetain) {
            logger.finer { "Save retained topic [${message.topicName}]" }
            val topicName = MqttTopicName(message.topicName)
            retainedMessages.saveMessage(topicName, message)
        }
    }

    private fun distributeMessageToClients(message: MqttMessage) {
        val topicName = MqttTopicName(message.topicName)
        subscriptionTable.findClients(topicName).forEach {
            MqttClient.sendMessageToClient(vertx, it, message)
        }
    }

    //----------------------------------------------------------------------------------------------------

    private fun <K,V> getMap(name: String): Future<AsyncMap<K, V>> {
        val promise = Promise.promise<AsyncMap<K, V>>()
        val sharedData = vertx.sharedData()
        if (vertx.isClustered) {
            sharedData.getClusterWideMap<K, V>("TopicMap") { it ->
                if (it.succeeded()) {
                    promise.complete(it.result())
                } else {
                    println("Failed to access the shared map [$name]: ${it.cause()}")
                    promise.fail(it.cause())
                }
            }
        } else {
            sharedData.getAsyncMap<K, V>("TopicMap") {
                if (it.succeeded()) {
                    promise.complete(it.result())
                } else {
                    println("Failed to access the shared map [$name]: ${it.cause()}")
                    promise.fail(it.cause())
                }
            }
        }
        return promise.future()
    }
}