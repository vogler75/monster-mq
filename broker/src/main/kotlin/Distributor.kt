package at.rocworks

import at.rocworks.codecs.MqttMessage
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.core.shareddata.AsyncMap

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

    private var retainedMessages: AsyncMap<String, MqttMessage>? = null // topic to message

    private val subscriptionsFlat = mutableMapOf<String, MutableSet<String>>() // topic to clientIds
    private val subscriptionsTree = TopicTree()

    private lateinit var distBusAddr: String
    private lateinit var topicBusAddr: String

    init {
        logger.level = Level.INFO
    }

    override fun start(startPromise: Promise<Void>) {
        distBusAddr = Const.getDistBusAddr(this.deploymentID())
        topicBusAddr = Const.getTopicBusAddr(this.deploymentID())

        val map1 = getMap<String, MqttMessage>("RetainedMessages").onComplete {
            logger.info("Retained message store: "+it.succeeded())
            retainedMessages = it.result()
        }

        vertx.eventBus().consumer<JsonObject>(distBusAddr) {
            logger.finest { "Received request [${it.body()}]" }
            requestHandler(it)
        }

        vertx.eventBus().consumer<Any>(topicBusAddr) { message ->
            message.body().let { payload ->
                if (payload is MqttMessage) {
                    logger.finest { "Received message [${payload.topicName}]" }
                    distributeMessage(payload)
                } else {
                    logger.warning("Received unexpected message of type [${payload::class.simpleName}]")
                }
            }
        }

        Future.all(listOf(map1)).onComplete {
            startPromise.complete()
        }
    }

    private fun requestHandler(it: Message<JsonObject>) {
        when (it.body().getString(COMMAND_KEY)) {
            COMMAND_SUBSCRIBE -> subscribeCommand(it)
            COMMAND_UNSUBSCRIBE -> unsubscribeCommand(it)
            COMMAND_CLEANSESSION -> cleanSessionCommand(it)
        }
    }

    //----------------------------------------------------------------------------------------------------

    fun subscribeRequest(client: MonsterClient, topicName: String, result: (Boolean)->Unit) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_SUBSCRIBE)
            .put(Const.TOPIC_KEY, topicName)
            .put(Const.CLIENT_KEY, client.deploymentID())
        vertx.eventBus().request(distBusAddr, request) {
            if (it.succeeded()) result(it.result().body())
            else logger.severe("Subscribe request failed: ${it.cause()}")
        }
    }

    private fun subscribeCommand(it: Message<JsonObject>) {
        val topicName = it.body().getString(Const.TOPIC_KEY)
        val clientId = it.body().getString(Const.CLIENT_KEY)
        subscriptionsFlat.getOrPut(topicName) { hashSetOf() }.add(clientId)
        subscriptionsTree.add(topicName, clientId)
        sendRetainedMessages(topicName, clientId)
        logger.fine(subscriptionsTree.toString())
        it.reply(true)
    }

    private fun sendRetainedMessages(topicName: String, clientId: String) { // TODO: must be optimized
        retainedMessages?.apply {
            keys().onComplete { topics ->
                topics.result().filter { Const.topicMatches(topicName, it) }.forEach { topic ->
                    get(topic).onComplete { value ->
                        val message = value.result()
                        logger.finest { "Publish retained message [${message.topicName}] to [${clientId}]" }
                        vertx.eventBus().publish(Const.getClientBusAddr(clientId), message)
                    }
                }
            }
        }
    }

    //----------------------------------------------------------------------------------------------------

    fun unsubscribeRequest(client: MonsterClient, topicName: String, result: (Boolean)->Unit) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_UNSUBSCRIBE)
            .put(Const.TOPIC_KEY, topicName)
            .put(Const.CLIENT_KEY, client.deploymentID())
        vertx.eventBus().request(distBusAddr, request) {
            if (it.succeeded()) result(it.result().body())
            else logger.severe("Unsubscribe request failed: ${it.cause()}")
        }
    }

    private fun unsubscribeCommand(command: Message<JsonObject>) {
        val topicName = command.body().getString(Const.TOPIC_KEY)
        val clientId = command.body().getString(Const.CLIENT_KEY)
        subscriptionsFlat[topicName]?.remove(clientId)
        subscriptionsTree.del(topicName, clientId)
        logger.fine(subscriptionsTree.toString())
        command.reply(true)
    }

    //----------------------------------------------------------------------------------------------------

    fun cleanSessionRequest(client: MonsterClient, result: (Boolean)->Unit) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_CLEANSESSION)
            .put(Const.CLIENT_KEY, client.deploymentID())
        vertx.eventBus().request(distBusAddr, request) {
            if (it.succeeded()) result(it.result().body())
            else logger.severe("Clean session request failed: ${it.cause()}")
        }
    }

    private fun cleanSessionCommand(command: Message<JsonObject>) {
        val clientId = command.body().getString(Const.CLIENT_KEY)
        subscriptionsFlat.forEach {
            if (it.value.remove(clientId)) {
                subscriptionsTree.del(it.key, clientId)
            }
        }
        command.reply(true)
    }

    //----------------------------------------------------------------------------------------------------

    fun publishMessage(message: MqttMessage) {
        vertx.eventBus().publish(topicBusAddr, message)
        if (message.isRetain) retainedMessages?.apply {
            logger.finest { "Save retained message for [${message.topicName}]" }
            put(message.topicName, message).onComplete {
                logger.finest { "Save retained message for [${message.topicName}] completed [${it.succeeded()}]" }
            }
        }
    }

    private fun distributeMessage(message: MqttMessage) {
        subscriptionsTree.findClients(message.topicName).toSet().forEach {
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