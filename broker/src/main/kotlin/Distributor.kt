package at.rocworks

import at.rocworks.data.*
import at.rocworks.stores.RetainedMessageHandler
import at.rocworks.stores.SubscriptionTableAsyncMap
import io.vertx.core.AbstractVerticle
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject

import java.util.logging.Logger

abstract class Distributor(
    private val subscriptionTable: SubscriptionTableAsyncMap,
    private val retainedMessageHandler: RetainedMessageHandler
): AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    companion object {
        const val COMMAND_KEY = "C"
        const val COMMAND_SUBSCRIBE = "S"
        const val COMMAND_UNSUBSCRIBE = "U"
        const val COMMAND_CLEANSESSION = "C"
    }

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    private fun getDistributorCommandAddress() = "${Const.GLOBAL_DISTRIBUTOR_NAMESPACE}/${deploymentID()}/C"
    protected fun getDistributorMessageAddress() = "${Const.GLOBAL_DISTRIBUTOR_NAMESPACE}/${deploymentID()}/M"

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
    }

    //----------------------------------------------------------------------------------------------------

    fun subscribeRequest(client: MqttClient, topicName: String) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_SUBSCRIBE)
            .put(Const.TOPIC_KEY, topicName)
            .put(Const.CLIENT_KEY, client.getClientId())
        vertx.eventBus().request<Boolean>(getDistributorCommandAddress(), request) {
            if (!it.succeeded())  logger.severe("Subscribe request failed: ${it.cause()}")
        }
    }

    private fun subscribeCommand(command: Message<JsonObject>) {
        val clientId = command.body().getString(Const.CLIENT_KEY)
        val topicName = command.body().getString(Const.TOPIC_KEY)

        retainedMessageHandler.findMatching(topicName) { message ->
            logger.finest { "Publish retained message [${message.topicName}]" }
            MqttClient.sendMessageToClient(vertx, clientId, message)
        }.onComplete {
            logger.info("Retained messages published [${it.result()}].")
            subscriptionTable.addSubscription(MqttSubscription(clientId, topicName))
            command.reply(true)
        }

    }

    //----------------------------------------------------------------------------------------------------

    fun unsubscribeRequest(client: MqttClient, topicName: String) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_UNSUBSCRIBE)
            .put(Const.TOPIC_KEY, topicName)
            .put(Const.CLIENT_KEY, client.deploymentID())
        vertx.eventBus().request<Boolean>(getDistributorCommandAddress(), request) {
            if (!it.succeeded()) logger.severe("Unsubscribe request failed: ${it.cause()}")
        }
    }

    private fun unsubscribeCommand(command: Message<JsonObject>) {
        val clientId = command.body().getString(Const.CLIENT_KEY)
        val topicName = command.body().getString(Const.TOPIC_KEY)
        subscriptionTable.removeSubscription(MqttSubscription(clientId, topicName))
        command.reply(true)
    }

    //----------------------------------------------------------------------------------------------------

    fun cleanSessionRequest(client: MqttClient) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_CLEANSESSION)
            .put(Const.CLIENT_KEY, client.getClientId())

        vertx.eventBus().request<Boolean>(getDistributorCommandAddress(), request) {
            if (!it.succeeded()) logger.severe("Clean session request failed: ${it.cause()}")
        }
    }

    private fun cleanSessionCommand(command: Message<JsonObject>) {
        val clientId = command.body().getString(Const.CLIENT_KEY)
        subscriptionTable.removeClient(clientId)
        command.reply(true)
    }

    //----------------------------------------------------------------------------------------------------

    fun publishMessage(message: MqttMessage) {
        publishMessageToBus(message)
        if (message.isRetain) retainedMessageHandler.saveMessage(message)
    }

    abstract fun publishMessageToBus(message: MqttMessage)

    protected fun consumeMessageFromBus(message: MqttMessage) {
        subscriptionTable.findClients(message.topicName).forEach {
            MqttClient.sendMessageToClient(vertx, it, message)
        }
    }
}