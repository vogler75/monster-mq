package at.rocworks.shared

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttTopicName
import at.rocworks.data.TopicTreeCache
import at.rocworks.data.TopicTreeCache.Node
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.shareddata.AsyncMap
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.concurrent.thread

class RetainedMessages(hazelcastInstance: HazelcastInstance): AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)
    private val name = "Retained"

    private val queues: List<ArrayBlockingQueue<MqttMessage>>

    private val index = TopicTreeCache(hazelcastInstance, "$name-Index")
    private val messages: IMap<String, MqttMessage> = hazelcastInstance.getMap("$name-Data")

    init {
        logger.level = Const.DEBUG_LEVEL
        queues = List(10) {
            ArrayBlockingQueue<MqttMessage>(10000)
        }
    }

    override fun start() {
        queues.forEachIndexed { index, queue -> writerThread( index, queue) }
    }

    private fun writerThread(nr: Int, queue: ArrayBlockingQueue<MqttMessage>) = thread(start = true) {
        logger.info("Start thread...")
        vertx.setPeriodic(1000) {
            logger.info("Retained message queue [$nr] size [${queue.size}]")
        }
        while (true) {
            queue.poll(100, TimeUnit.MILLISECONDS)?.let { message ->
                val topicName = MqttTopicName(message.topicName)
                if (message.payload.isEmpty()) {
                    index.del(topicName)
                    messages.remove(topicName.identifier)
                } else {
                    index.add(topicName)
                    messages.put(topicName.identifier, message)
                }
            }
        }
    }



    var idx=0
    fun saveMessage(message: MqttMessage): Future<Void> {
        logger.finest { "Save retained topic [${message.topicName}]" }
        try {
            queues[idx].add(message)
        } catch (e: IllegalStateException) {
            logger.severe(e.message)
        }
        if (++idx==queues.size) idx=0
        return Future.succeededFuture()
    }

    fun findMatching(topicName: MqttTopicName, callback: (message: MqttMessage)->Unit): Future<Int> {
        val promise = Promise.promise<Int>()
        vertx.executeBlocking(Callable {
            var counter = 0
            try {
                index.findMatchingTopicNames(topicName) { topic ->
                    logger.finest { "Found matching topic [$topic] for [$topicName]" }
                    val message = messages.get(topic.identifier)
                    if (message != null) callback(message)
                    else logger.finest { "Stored message for [$topic] is null!" } // it could be a node inside the tree index where we haven't stored a value
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
        })
        return promise.future()
    }
}