package at.rocworks

import at.rocworks.codecs.MqttMessage
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.core.shareddata.AsyncMap
import java.util.concurrent.Callable

import java.util.logging.Level
import java.util.logging.Logger

class Distributor: AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    companion object {
        const val COMMAND_KEY = "C"
        const val COMMAND_SUBSCRIBE = "S"
        const val COMMAND_UNSUBSCRIBE = "U"
        const val COMMAND_CLEANSESSION = "C"
    }

    private var retainedMessages = MessageStore("RetainedMessages")

    private val clientSubscriptions = mutableMapOf<ClientId, MutableSet<TopicName>>() // clientId to topics
    private val subscriptionsTree = TopicTree()

    private lateinit var distBusAddr: String
    private lateinit var topicBusAddr: String

    init {
        logger.level = Level.INFO
    }

    override fun start(startPromise: Promise<Void>) {
        distBusAddr = Const.getDistBusAddr(this.deploymentID())
        topicBusAddr = Const.getTopicBusAddr(this.deploymentID())

        vertx.deployVerticle(retainedMessages).onSuccess {
            startPromise.complete()
        }.onFailure {
            startPromise.fail(it)
        }

        vertx.eventBus().consumer<JsonObject>(distBusAddr) {
            logger.finest { "Received request [${it.body()}]" }
            requestHandler(it)
        }

        vertx.eventBus().consumer<Any>(topicBusAddr) { message ->
            message.body().let { payload ->
                if (payload is MqttMessage) {
                    logger.finest { "Received message [${payload.topicName}] retained [${payload.isRetain}]" }
                    distributeMessage(payload)
                } else {
                    logger.warning("Received unexpected message of type [${payload::class.simpleName}]")
                }
            }
        }
    }

    override fun stop() {
        vertx.undeploy(retainedMessages.deploymentID())
    }

    private fun requestHandler(it: Message<JsonObject>) {
        when (it.body().getString(COMMAND_KEY)) {
            COMMAND_SUBSCRIBE -> subscribeCommand(it)
            COMMAND_UNSUBSCRIBE -> unsubscribeCommand(it)
            COMMAND_CLEANSESSION -> cleanSessionCommand(it)
        }
    }

    //----------------------------------------------------------------------------------------------------

    fun subscribeRequest(client: MonsterClient, topicName: TopicName, result: (Boolean)->Unit) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_SUBSCRIBE)
            .put(Const.TOPIC_KEY, topicName.identifier)
            .put(Const.CLIENT_KEY, client.deploymentID())
        vertx.eventBus().request(distBusAddr, request) {
            if (it.succeeded()) result(it.result().body())
            else logger.severe("Subscribe request failed: ${it.cause()}")
        }
    }

    private fun subscribeCommand(command: Message<JsonObject>) {
        val topicName = TopicName(command.body().getString(Const.TOPIC_KEY))
        val clientId = ClientId(command.body().getString(Const.CLIENT_KEY))

        retainedMessages.findMatching(topicName) { message ->
            logger.finer { "Publish retained message [${message.topicName}]" }
            vertx.eventBus().publish(Const.getClientBusAddr(clientId), message)
        }.onComplete {
            logger.info("Retained messages published.")
            clientSubscriptions.getOrPut(clientId) { hashSetOf() }.add(topicName)
            subscriptionsTree.add(topicName, clientId)
            command.reply(true)
        }
        logger.fine(subscriptionsTree.toString())
    }

    //----------------------------------------------------------------------------------------------------

    fun unsubscribeRequest(client: MonsterClient, topicName: TopicName, result: (Boolean)->Unit) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_UNSUBSCRIBE)
            .put(Const.TOPIC_KEY, topicName.identifier)
            .put(Const.CLIENT_KEY, client.deploymentID())
        vertx.eventBus().request(distBusAddr, request) {
            if (it.succeeded()) result(it.result().body())
            else logger.severe("Unsubscribe request failed: ${it.cause()}")
        }
    }

    private fun unsubscribeCommand(command: Message<JsonObject>) {
        val topicName = TopicName(command.body().getString(Const.TOPIC_KEY))
        val clientId = ClientId(command.body().getString(Const.CLIENT_KEY))
        clientSubscriptions[clientId]?.remove(topicName)
        subscriptionsTree.del(topicName, clientId)
        logger.fine(subscriptionsTree.toString())
        command.reply(true)
    }

    //----------------------------------------------------------------------------------------------------

    fun cleanSessionRequest(client: MonsterClient, result: (Boolean)->Unit) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_CLEANSESSION)
            .put(Const.CLIENT_KEY, client.getClientId().identifier)

        vertx.eventBus().request(distBusAddr, request) {
            if (it.succeeded()) result(it.result().body())
            else logger.severe("Clean session request failed: ${it.cause()}")
        }
    }

    private fun cleanSessionCommand(command: Message<JsonObject>) {
        val clientId = ClientId(command.body().getString(Const.CLIENT_KEY))
        clientSubscriptions.remove(clientId)?.forEach { topic ->
            subscriptionsTree.del(topic, clientId)
        }
        command.reply(true)
    }

    //----------------------------------------------------------------------------------------------------

    fun publishMessage(message: MqttMessage) {
        vertx.eventBus().publish(topicBusAddr, message)
        if (message.isRetain) {
            logger.finer("Save retained topic [${message.topicName}]")
            retainedMessages.saveMessage(TopicName(message.topicName), message)
        }
    }

    private fun distributeMessage(message: MqttMessage) {
        val topicName = TopicName(message.topicName)
        if (message.isRetain) {
            logger.finer("Index retained topic [${message.topicName}]")
            retainedMessages.addTopicToIndex(topicName)
        }
        subscriptionsTree.findClientsOfTopicName(topicName).toSet().forEach {
            vertx.eventBus().publish(Const.getClientBusAddr(it), message)
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