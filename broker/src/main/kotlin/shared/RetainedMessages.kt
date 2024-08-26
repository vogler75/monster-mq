package at.rocworks.shared

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttTopicName
import at.rocworks.data.TopicTreeCache
import com.hazelcast.core.HazelcastInstance
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.shareddata.AsyncMap
import java.util.concurrent.Callable
import java.util.logging.Level
import java.util.logging.Logger

class RetainedMessages(hazelcastInstance: HazelcastInstance): AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)
    private val name = "Retained"
    private val index = TopicTreeCache(hazelcastInstance, name)
    private var messages: AsyncMap<String, MqttMessage>? = null // key as MqttTopicName does not work

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    //private val addAddress = Const.GLOBAL_RETAINED_MESSAGES_NAMESPACE +"/A"
    //private val delAddress = Const.GLOBAL_RETAINED_MESSAGES_NAMESPACE +"/D"
    //private val saveAddress = Const.GLOBAL_RETAINED_MESSAGES_NAMESPACE +"/S"

    override fun start(startPromise: Promise<Void>) {
        Utils.getMap<String, MqttMessage>(vertx, name).onSuccess { messages ->
            logger.info("Startup message store [$name].")
            this.messages = messages
            startPromise.complete()
        }.onFailure {
            logger.severe("Error in getting map [$name] [${it.message}]")
            startPromise.fail(it)
        }
    }

    fun saveMessage(message: MqttMessage): Future<Void> {
        logger.finer { "Save retained topic [${message.topicName}]" }
        val promise = Promise.promise<Void>()
        messages?.let { messages ->
            val topicName = MqttTopicName(message.topicName)
            if (message.payload.isEmpty()) {
                messages.remove(topicName.identifier).onComplete {
                    index.add(topicName)
                    logger.finest { "Removed retained message for [$topicName] completed [${it.succeeded()}]" }
                    promise.complete()
                }.onFailure(promise::fail)
            }
            else {
                messages.put(topicName.identifier, message).onComplete {
                    index.add(topicName)
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
                var counter = 0
                try {
                    index.findMatchingTopicNames(topicName) { topic ->
                        logger.finest { "Found matching topic [$topic] for [$topicName]" }
                        messages.get(topic.identifier).onSuccess { message ->
                            if (message != null) callback(message)
                            else logger.finest { "Stored message for [$topic] is null!" } // it could be a node inside the tree index where we haven't stored a value
                        }
                        if (++counter>=10000) { // TODO: configurable or timed
                            logger.warning("Maximum retained messages sent.")
                            false
                        } else true
                    }
                } catch (e: Exception) {
                    e.printStackTrace()
                }
                logger.fine { "Found [$counter] matching retained messages for [$topicName]." }
                promise.complete(counter)
            } ?: run {
                promise.fail("Message store not initialized!")
            }
        })
        return promise.future()
    }

    fun findMatching1(topicName: MqttTopicName, callback: (message: MqttMessage)->Unit): Future<Int> {
        val promise = Promise.promise<Int>()
        vertx.executeBlocking(Callable {
            messages?.let { messages ->
                val topics = index.findMatchingTopicNames(topicName)
                logger.fine { "Found [${topics.size}] matching retained messages for [$topicName]." }
                Future.all(topics.map { topic ->
                    logger.finest { "Found matching topic [$topic] for [$topicName]" }
                    messages.get(topic.identifier).onSuccess { message ->
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