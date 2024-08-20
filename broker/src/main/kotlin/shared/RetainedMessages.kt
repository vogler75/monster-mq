package at.rocworks.shared

import at.rocworks.Const
import at.rocworks.data.TopicTree
import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttTopicName
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.shareddata.AsyncMap
import java.util.concurrent.Callable
import java.util.logging.Level
import java.util.logging.Logger

class RetainedMessages(): AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)
    private val name = "Retained"
    private val index = TopicTree<Void>()
    private var messages: AsyncMap<MqttTopicName, MqttMessage>? = null

    init {
        logger.level = Level.ALL
    }

    private val addAddress = Const.GLOBAL_RETAINED_MESSAGES_NAMESPACE +"/A"
    private val delAddress = Const.GLOBAL_RETAINED_MESSAGES_NAMESPACE +"/D"

    override fun start(startPromise: Promise<Void>) {
        vertx.eventBus().consumer<MqttTopicName>(addAddress) {
            index.add(it.body())
        }

        vertx.eventBus().consumer<MqttTopicName>(delAddress) {
            index.del(it.body())
        }

        Const.getMap<MqttTopicName, MqttMessage>(vertx, name).onSuccess { messages ->
            logger.info("Indexing message store [$name].")
            this.messages = messages
            messages.keys()
                .onSuccess { keys ->
                    keys.forEach(index::add)
                    logger.info("Indexing message store [$name] finished.")
                    startPromise.complete()
                }
                .onFailure {
                    logger.severe("Error in getting keys of [$name] [${it.message}]")
                    startPromise.fail(it)
                }
        }.onFailure {
            logger.severe("Error in getting map [$name] [${it.message}]")
            startPromise.fail(it)
        }
    }

    fun saveMessage(topicName: MqttTopicName, message: MqttMessage): Future<Void> {
        val promise = Promise.promise<Void>()
        messages?.let { messages ->
            if (message.payload.isEmpty()) {
                messages.remove(topicName).onComplete {
                    vertx.eventBus().publish(delAddress, topicName)
                    logger.finest { "Removed retained message for [$topicName] completed [${it.succeeded()}]" }
                    promise.complete()
                }.onFailure(promise::fail)
            }
            else {
                messages.put(topicName, message).onComplete {
                    vertx.eventBus().publish(addAddress, topicName)
                    logger.finest { "Saved retained message for [$topicName] completed [${it.succeeded()}]" }
                    promise.complete()
                }.onFailure(promise::fail)
            }
        } ?: run {
            promise.fail("Message store is null!")
        }
        return promise.future()
    }

    fun findMatching(topicName: MqttTopicName, callback: (message: MqttMessage)->Unit): Future<Int> {
        val promise = Promise.promise<Int>()
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
                    promise.complete(topics.size)
                }
            } ?: run {
                promise.fail("No message store initialized!")
            }
        })
        return promise.future()
    }
}