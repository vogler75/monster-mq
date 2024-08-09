package at.rocworks

import at.rocworks.codecs.MqttMessage
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.shareddata.AsyncMap
import java.util.logging.Level
import java.util.logging.Logger

class MessageStore(private val name: String): AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    private var messages: AsyncMap<TopicName, MqttMessage>? = null // topic to message
    private val tree = TopicTree()

    init {
        logger.level = Level.INFO
    }

    override fun start(startPromise: Promise<Void>) {
        getMap<TopicName, MqttMessage>(name).onComplete {
            logger.info("Get message store [$name] [${it.succeeded()}]")
            if (it.succeeded()) {
                messages = it.result()
                startPromise.complete()
            }
            else startPromise.fail(it.cause())
        }
    }

    fun saveMessage(topicName: TopicName, message: MqttMessage) {
        messages?.apply {
            put(topicName, message).onComplete {
                tree.add(topicName)
                logger.finest { "Saved message for [$topicName] completed [${it.succeeded()}]" }
            }
        }
    }

    private fun sendMessages_OLD(topicName: TopicName, clientId: ClientId) { // TODO: must be optimized
        logger.finer("Send messages [$topicName] to client [$clientId]")
        messages?.apply {
            keys().onComplete { topics ->
                logger.finer("Got [${topics.result()?.size}] topics.")
                val filtered = topics.result().filter { it.matchesToWildcard(topicName) }
                logger.finer("Filtered [${filtered.size}] topics.")
                filtered.forEach { topic ->
                    get(topic).onComplete { value ->
                        val message = value.result()
                        logger.finest { "Publish message [${message.topicName}] to [${clientId}]" }
                        vertx.eventBus().publish(Const.getClientBusAddr(clientId), message)
                    }
                }
            }
        }
    }

    fun findMatching(topicName: TopicName, callback: (message: MqttMessage)->Unit): Future<Unit> {
        val promise = Promise.promise<Unit>()
        messages?.let { messages ->
            val topics = tree.findMatchingTopicNames(topicName)
            Future.all(topics.map { topic ->
                messages.get(topic).onSuccess(callback)
            }).onComplete {
                promise.complete()
            }
        } ?: run {
            promise.fail("No message store initialized!")
        }
        return promise.future()
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
}