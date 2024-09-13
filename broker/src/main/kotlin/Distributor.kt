package at.rocworks

import at.rocworks.data.*
import at.rocworks.stores.MessageHandler
import at.rocworks.stores.SessionHandler
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import java.time.Instant

import java.util.logging.Logger

abstract class Distributor(
    val sessionHandler: SessionHandler,
    private val messageHandler: MessageHandler
): AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    companion object {
        const val COMMAND_KEY = "C"
        const val COMMAND_SUBSCRIBE = "S"
        const val COMMAND_UNSUBSCRIBE = "U"
    }

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    private fun getDistributorCommandAddress() = "${Const.GLOBAL_DISTRIBUTOR_NAMESPACE}/${deploymentID()}/C"
    protected fun getDistributorMessageAddress() = "${Const.GLOBAL_DISTRIBUTOR_NAMESPACE}/${deploymentID()}/M"

    override fun start() {
        vertx.eventBus().consumer<JsonObject>(getDistributorCommandAddress()) { message ->
            message.body()?.let { payload ->
                logger.finest { "Received request [${payload}] [${Utils.getCurrentFunctionName()}]" }
                when (payload.getString(COMMAND_KEY)) {
                    COMMAND_SUBSCRIBE -> subscribeCommand(message)
                    COMMAND_UNSUBSCRIBE -> unsubscribeCommand(message)
                }
            }
        }
    }

    //----------------------------------------------------------------------------------------------------

    private fun sendMessageToClient(clientId: String, message: MqttMessage) {
        when (message.qosLevel) {
            0 -> vertx.eventBus().send(MqttClient.getMessages0Address(clientId), message)
            1 -> vertx.eventBus().send(MqttClient.getMessages1Address(clientId), message)
            2 -> vertx.eventBus().send(MqttClient.getMessages2Address(clientId), message)
            else -> logger.severe { "Unknown QoS level [${message.qosLevel}] [${Utils.getCurrentFunctionName()}]" }
        }
    }

    //----------------------------------------------------------------------------------------------------

    fun subscribeRequest(client: MqttClient, topicName: String, qos: MqttQoS) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_SUBSCRIBE)
            .put(Const.TOPIC_KEY, topicName)
            .put(Const.CLIENT_KEY, client.clientId)
            .put(Const.QOS_KEY, qos.value())
        vertx.eventBus().request<Boolean>(getDistributorCommandAddress(), request) {
            if (!it.succeeded())  logger.severe("Subscribe request failed [${it.cause()}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    private fun subscribeCommand(command: Message<JsonObject>) {
        val clientId = command.body().getString(Const.CLIENT_KEY)
        val topicName = command.body().getString(Const.TOPIC_KEY)
        val qos = MqttQoS.valueOf(command.body().getInteger(Const.QOS_KEY))

        messageHandler.findMatching(topicName) { message ->
            logger.finest { "Publish retained message [${message.topicName}] [${Utils.getCurrentFunctionName()}]" }
            sendMessageToClient(clientId, message)
        }.onComplete {
            logger.finest { "Retained messages published [${it.result()}] [${Utils.getCurrentFunctionName()}]" }
            sessionHandler.addSubscription(MqttSubscription(clientId, topicName, qos))
            command.reply(true)
        }
    }

    //----------------------------------------------------------------------------------------------------

    fun unsubscribeRequest(client: MqttClient, topicName: String) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_UNSUBSCRIBE)
            .put(Const.TOPIC_KEY, topicName)
            .put(Const.CLIENT_KEY, client.clientId)
        vertx.eventBus().request<Boolean>(getDistributorCommandAddress(), request) {
            if (!it.succeeded()) logger.severe("Unsubscribe request failed [${it.cause()}] [${Utils.getCurrentFunctionName()}]")
        }
    }

    private fun unsubscribeCommand(command: Message<JsonObject>) {
        val clientId = command.body().getString(Const.CLIENT_KEY)
        val topicName = command.body().getString(Const.TOPIC_KEY)
        sessionHandler.delSubscription(MqttSubscription(clientId, topicName, MqttQoS.FAILURE /* not needed */))
        command.reply(true)
    }

    //----------------------------------------------------------------------------------------------------

    fun publishMessage(message: MqttMessage) {
        publishMessageToBus(message)
        if (message.isRetain)
            messageHandler.saveMessage(message)
    }

    fun publishMessageCompleted(message: MqttMessage) {
        sessionHandler.removeMessage(message.messageUuid)
    }

    abstract fun publishMessageToBus(message: MqttMessage)

    protected fun consumeMessageFromBus(message: MqttMessage) {
        val (online, offline) = sessionHandler.findClients(message.topicName).partition {
            sessionHandler.isConnected(it.first)
        }
        online.forEach {
            // Potentially downgrade QoS
            if (it.second < message.qosLevel) {
                logger.finest { "Subscription based downgrading QoS from [${message.qosLevel}] to [${it.second}] [${Utils.getCurrentFunctionName()}]" }
                val newMessage = message.cloneWithNewQoS(it.second)
                sendMessageToClient(it.first, newMessage)
            } else {
                sendMessageToClient(it.first, message)
            }
        }
        logger.info { "Message sent to [${online.size}] clients. Now enqueuing for [${offline.size}] offline sessions [${Utils.getCurrentFunctionName()}]" }
        if (offline.isNotEmpty() && message.qosLevel > 0) {
            sessionHandler.enqueueMessage(message, offline.map { it.first })
        }
    }
}