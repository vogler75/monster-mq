package at.rocworks

import at.rocworks.codecs.MqttMessage
import at.rocworks.codecs.MqttTopicName
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.shareddata.AsyncMap
import java.util.concurrent.Callable
import java.util.logging.Level
import java.util.logging.Logger

class MessageStore(private val name: String): AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    private val index = TopicTree()
    private var messages: AsyncMap<MqttTopicName, MqttMessage>? = null // topic to message

    init {
        logger.level = Level.INFO
    }

    override fun start(startPromise: Promise<Void>) {
        getMap<MqttTopicName, MqttMessage>(name).onSuccess { messages ->
            logger.info("Indexing message store [$name].")
            this.messages = messages
            messages.keys()
                .onSuccess { keys ->
                    keys.forEach(index::add)
                    logger.info("Indexing message store [$name] finished.")
                    startPromise.complete()
                }
                .onFailure(startPromise::fail)
        }.onFailure(startPromise::fail)
    }

    fun saveMessage(topicName: MqttTopicName, message: MqttMessage): Future<Void> {
        messages?.let { messages ->
            return messages.put(topicName, message).onComplete {
                logger.finest { "Saved message for [$topicName] completed [${it.succeeded()}]" }
            }
        } ?: run {
            return Future.failedFuture("Message store is null!")
        }
    }

    fun addTopicToIndex(topicName: MqttTopicName) = index.add(topicName)

    fun findMatching(topicName: MqttTopicName, callback: (message: MqttMessage)->Unit): Future<Unit> {
        val promise = Promise.promise<Unit>()
        vertx.executeBlocking(Callable {
            messages?.let { messages ->
                val topics = index.findMatchingTopicNames(topicName)
                Future.all(topics.map { topic ->
                    logger.finest { "Found matching topic [$topic] for [$topicName]" }
                    messages.get(topic).onSuccess { message ->
                        if (message!=null) callback(message)
                        else logger.finest { "Stored message for [$topic] is null!" } // it could be a node inside the tree index where we haven't stored a value
                    }
                }).onComplete {
                    promise.complete()
                }
            } ?: run {
                promise.fail("No message store initialized!")
            }
        })
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