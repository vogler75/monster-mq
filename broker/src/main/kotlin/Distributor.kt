package at.rocworks

import at.rocworks.codecs.MqttClientId
import at.rocworks.codecs.MqttMessage
import at.rocworks.codecs.MqttTopicName
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.core.shareddata.AsyncMap

import java.util.logging.Level
import java.util.logging.Logger

class Distributor(private val retainedMessages: RetainedMessages): AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    companion object {
        const val COMMAND_KEY = "C"
        const val COMMAND_SUBSCRIBE = "S"
        const val COMMAND_UNSUBSCRIBE = "U"
        const val COMMAND_CLEANSESSION = "C"
    }

    private val clientSubscriptions = mutableMapOf<MqttClientId, MutableSet<MqttTopicName>>() // clientId to topics
    private val subscriptionsTree = TopicTree()

    init {
        logger.level = Level.ALL
    }

    private fun getDistributorNamespace() = "${Const.GLOBAL_DISTRIBUTOR_NAMESPACE}/${deploymentID()}"

    override fun start() {
        vertx.eventBus().consumer<JsonObject>(Const.GLOBAL_DISTRIBUTOR_NAMESPACE) {
            logger.finest { "Received request [${it.body()}]" }
            when (it.body().getString(COMMAND_KEY)) {
                COMMAND_SUBSCRIBE -> subscribeCommand(it)
                COMMAND_UNSUBSCRIBE -> unsubscribeCommand(it)
                COMMAND_CLEANSESSION -> cleanSessionCommand(it)
            }
        }

        vertx.eventBus().consumer<Any>(getDistributorNamespace()) { message ->
            message.body().let { payload ->
                if (payload is MqttMessage) {
                    logger.finest { "Received message [${payload.topicName}] retained [${payload.isRetain}]" }
                    distributeMessageToClients(payload)
                } else {
                    logger.warning("Received unexpected message of type [${payload::class.simpleName}]")
                }
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
            .put(Const.BROKER_KEY, client.getDistributorId())
        vertx.eventBus().publish(Const.GLOBAL_DISTRIBUTOR_NAMESPACE, request)
    }

    private fun subscribeCommand(command: Message<JsonObject>) {
        val topicName = MqttTopicName(command.body().getString(Const.TOPIC_KEY))
        val clientId = MqttClientId(command.body().getString(Const.CLIENT_KEY))
        val distributorId = command.body().getString(Const.BROKER_KEY)

        fun subscribe() {
            subscriptionsTree.add(topicName, clientId)
            clientSubscriptions.getOrPut(clientId) { hashSetOf() }.add(topicName)
            logger.fine { subscriptionsTree.toString() }
            command.reply(true)
        }

        if (distributorId == deploymentID()) {
            retainedMessages.findMatching(topicName) { message ->
                logger.finer { "Publish retained message [${message.topicName}]" }
                MqttClient.sendMessageToClient(vertx, clientId, message)
            }.onComplete {
                logger.info("Retained messages published.")
                subscribe()
            }
        } else subscribe()
    }

    //----------------------------------------------------------------------------------------------------

    fun unsubscribeRequest(client: MqttClient, topicName: MqttTopicName, result: (Boolean)->Unit) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_UNSUBSCRIBE)
            .put(Const.TOPIC_KEY, topicName.identifier)
            .put(Const.CLIENT_KEY, client.deploymentID())
        vertx.eventBus().request(getDistributorNamespace(), request) {
            if (it.succeeded()) result(it.result().body())
            else logger.severe("Unsubscribe request failed: ${it.cause()}")
        }
    }

    private fun unsubscribeCommand(command: Message<JsonObject>) {
        val topicName = MqttTopicName(command.body().getString(Const.TOPIC_KEY))
        val clientId = MqttClientId(command.body().getString(Const.CLIENT_KEY))
        clientSubscriptions[clientId]?.remove(topicName)
        subscriptionsTree.del(topicName, clientId)
        logger.fine(subscriptionsTree.toString())
        command.reply(true)
    }

    //----------------------------------------------------------------------------------------------------

    fun cleanSessionRequest(client: MqttClient, result: (Boolean)->Unit) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_CLEANSESSION)
            .put(Const.CLIENT_KEY, client.getClientId().identifier)

        vertx.eventBus().request(Const.GLOBAL_DISTRIBUTOR_NAMESPACE, request) {
            if (it.succeeded()) result(it.result().body())
            else logger.severe("Clean session request failed: ${it.cause()}")
        }
    }

    private fun cleanSessionCommand(command: Message<JsonObject>) {
        val clientId = MqttClientId(command.body().getString(Const.CLIENT_KEY))
        clientSubscriptions.remove(clientId)?.forEach { topic ->
            subscriptionsTree.del(topic, clientId)
        }
        command.reply(true)
    }

    //----------------------------------------------------------------------------------------------------

    fun publishMessage(message: MqttMessage) {
        vertx.eventBus().publish(getDistributorNamespace(), message)
        if (message.isRetain) {
            logger.finer { "Save retained topic [${message.topicName}]" }
            val topicName = MqttTopicName(message.topicName)
            retainedMessages.saveMessage(topicName, message)
        }
    }

    private fun distributeMessageToClients(message: MqttMessage) {
        val topicName = MqttTopicName(message.topicName)
        subscriptionsTree.findClientsOfTopicName(topicName).toSet().forEach {
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