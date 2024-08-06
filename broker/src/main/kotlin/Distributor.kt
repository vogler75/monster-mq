package at.rocworks

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.core.shareddata.AsyncMap
import io.vertx.mqtt.messages.MqttPublishMessage
import io.vertx.mqtt.messages.impl.MqttPublishMessageImpl

import java.time.Instant
import java.util.logging.Logger

class Distributor: AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    companion object {
        const val COMMAND_KEY = "C"
        const val COMMAND_SUBSCRIBE = "S"
        const val COMMAND_UNSUBSCRIBE = "U"
        const val COMMAND_CLEAN = "C"
    }

    private var globalWildcardSubscriptions: AsyncMap<String, String>? = null
    private var globalRetainedMessages: AsyncMap<String, Buffer>? = null

    private val subscriptionsFlat = mutableMapOf<String, MutableSet<String>>() // topic to clientIds
    private val subscriptionsTree = TopicTree()

    private fun getDistBusAddr() = Const.getDistBusAddr(this.deploymentID())
    private fun getTopicBusAddr() = Const.getTopicBusAddr(this.deploymentID())


    override fun start(startPromise: Promise<Void>) {
        val f1 = getMap<String, String>("WildcardSubscriptions").onComplete {
            globalWildcardSubscriptions = it.result()
        }

        val f2 = getMap<String, Buffer>("RetainedMessages").onComplete {
            globalRetainedMessages = it.result()
        }

        vertx.eventBus().consumer<JsonObject>(getDistBusAddr()) {
            logger.finest { "Received request [${it.body()}]" }
            requestHandler(it)
        }

        vertx.eventBus().consumer<MqttPublishMessageImpl>(getTopicBusAddr()) {
            logger.finest { "Received message [${it.body().topicName()}]" }
            distributeMessage(it.body())
        }

        Future.all(f1, f2).onComplete {
            startPromise.complete()
        }
    }

    private fun requestHandler(it: Message<JsonObject>) {
        val command = it.body().getString(COMMAND_KEY)
        when (command) {
            COMMAND_SUBSCRIBE -> {
                val topicName = it.body().getString(Const.TOPIC_KEY)
                val clientId = it.body().getString(Const.CLIENT_KEY)
                subscriptionsFlat.getOrPut(topicName) { hashSetOf() }.add(clientId)
                subscriptionsTree.add(topicName, clientId)
                it.reply(true)
            }

            COMMAND_UNSUBSCRIBE -> {
                val topicName = it.body().getString(Const.TOPIC_KEY)
                val clientId = it.body().getString(Const.CLIENT_KEY)
                subscriptionsFlat[topicName]?.remove(clientId)
                subscriptionsTree.del(topicName, clientId)
                it.reply(true)
            }

            COMMAND_CLEAN -> {
                val clientId = it.body().getString(Const.CLIENT_KEY)
                subscriptionsFlat.forEach {
                    if (it.value.remove(clientId)) {
                        subscriptionsTree.del(it.key, clientId)
                    }
                }
                it.reply(true)
            }
        }
    }

    fun subscribeRequest(
        client: MonsterClient,
        topicName: String,
        result: (Boolean)->Unit
    ) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_SUBSCRIBE)
            .put(Const.TOPIC_KEY, topicName)
            .put(Const.CLIENT_KEY, client.deploymentID())
        vertx.eventBus().request(getDistBusAddr(), request) {
            result(it.result().body())
        }
    }

    fun unsubscribeRequest(
        client: MonsterClient,
        topicName: String,
        result: (Boolean)->Unit
    ) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_UNSUBSCRIBE)
            .put(Const.TOPIC_KEY, topicName)
            .put(Const.CLIENT_KEY, client.deploymentID())
        vertx.eventBus().request(getDistBusAddr(), request) {
            result(it.result().body())
        }
    }

    fun cleanSessionRequest(
        client: MonsterClient,
        result: (Boolean)->Unit
    ) {
        val request = JsonObject()
            .put(COMMAND_KEY, COMMAND_CLEAN)
            .put(Const.CLIENT_KEY, client.deploymentID())
        vertx.eventBus().request(getDistBusAddr(), request) {
            result(it.result().body())
        }
    }

    fun publishMessage(message: MqttPublishMessage) {
        vertx.eventBus().publish(getTopicBusAddr(), message)
    }

    private fun distributeMessage(message: MqttPublishMessage) {
        //subscriptionsFlat[message.topicName()]?.forEach { clientId ->
        //    vertx.eventBus().publish(Const.getClientBusAddr(clientId), message)
        //}
        subscriptionsTree.find(message.topicName()).forEach {
            vertx.eventBus().publish(Const.getClientBusAddr(it), message)
        }
    }

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

    fun testMap() {
        globalWildcardSubscriptions?.apply {
            // Get data from the shared map
            get("key") { getResult ->
                if (getResult.succeeded()) {
                    println("Value for 'key': ${getResult.result()}")
                } else {
                    println("Failed to get data: ${getResult.cause()}")
                }
            }

            val value = Instant.now().toEpochMilli().toString()
            println("New value $value")

            // Put data into the shared map
            put("key", value) { putResult ->
                if (putResult.succeeded()) {
                    println("Data added to the shared map")
                } else {
                    println("Failed to put data: ${putResult.cause()}")
                }
            }
        }
    }
}