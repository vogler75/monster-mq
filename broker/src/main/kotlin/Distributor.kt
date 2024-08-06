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

    private val subscriptions = mutableMapOf<String, MutableSet<String>>() // topic to clients

    fun getDistBusAddr() = Const.getDistBusAddr(this.deploymentID())
    fun getTopicBusAddr() = Const.getTopicBusAddr(this.deploymentID())

    data class TopicNode (
        val children: MutableMap<String, TopicNode> = mutableMapOf()
    )

    class TopicTree {
        private val root = TopicNode()

        fun addTopicName(topicName: String) {
            val xs = topicName.split("/")
            if (xs.isEmpty()) return
            else addTopicNode(root, xs.first(), xs.drop(1))
        }

        fun printTree() = printTreeNode(root)

        private fun printTreeNode(node: TopicNode, level: Int = 0) {
            node.children.forEach {
                println(" ".repeat(level) + " "+it.key)
                printTreeNode(it.value, level + 1)
            }
        }

        private fun addTopicNode(node: TopicNode, topic: String, rest: List<String>) {
            val next = node.children.getOrPut(topic) { TopicNode() }
            if (rest.isNotEmpty()) addTopicNode(next, rest.first(), rest.drop(1))
        }
    }

    private val topicTree = TopicTree()

    override fun start(startPromise: Promise<Void>) {
        val f1 = getMap<String, String>("WildcardSubscriptions").onComplete {
            globalWildcardSubscriptions = it.result()
        }

        val f2 = getMap<String, Buffer>("RetainedMessages").onComplete {
            globalRetainedMessages = it.result()
        }

        vertx.eventBus().consumer<JsonObject>(getDistBusAddr()) {
            logger.info("Received request [${it.body()}]")
            requestHandler(it)
        }

        vertx.eventBus().consumer<MqttPublishMessageImpl>(getTopicBusAddr()) {
            logger.info("Received message [${it.body().topicName()}]")
            distributeMessage(it.body())
        }

        Future.all(f1, f2).onComplete {
            startPromise.complete()
        }
    }

    private fun distributeMessage(message: MqttPublishMessage) {
        subscriptions[message.topicName()]?.forEach { clientId ->
            vertx.eventBus().publish(Const.getClientBusAddr(clientId), message)
        }
    }

    private fun requestHandler(it: Message<JsonObject>) {
        val command = it.body().getString(COMMAND_KEY)
        when (command) {
            COMMAND_SUBSCRIBE -> {
                val topicName = it.body().getString(Const.TOPIC_KEY)
                val clientId = it.body().getString(Const.CLIENT_KEY)
                subscriptions.getOrPut(topicName) { hashSetOf() }.add(clientId)
                if (Const.isWildCardTopic(topicName)) {
                    topicTree.addTopicName(topicName)
                    topicTree.printTree()
                }
                it.reply(true)
            }

            COMMAND_UNSUBSCRIBE -> {
                val topicName = it.body().getString(Const.TOPIC_KEY)
                val clientId = it.body().getString(Const.CLIENT_KEY)
                val xs = subscriptions[topicName] ?: hashSetOf()
                xs.remove(clientId)
                it.reply(true)
            }

            COMMAND_CLEAN -> {
                val clientId = it.body().getString(Const.CLIENT_KEY)
                subscriptions.forEach { it.value.remove(clientId) }
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