package at.rocworks.shared

import at.rocworks.Const
import at.rocworks.data.TopicTree
import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttTopicName
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlin.concurrent.thread

class RetainedMessages(
    private val index: TopicTree,
    private val store: MutableMap<String, MqttMessage>
): AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)
    private val queues: List<ArrayBlockingQueue<MqttMessage>>

    init {
        logger.level = Const.DEBUG_LEVEL
        val queuesCount = 1 // TODO: configurable
        queues = List(queuesCount) { ArrayBlockingQueue<MqttMessage>(10000) }
    }

    override fun start() {
        queues.forEachIndexed { index, queue -> writerThread(index, queue) }
    }

    private fun writerThread(nr: Int, queue: ArrayBlockingQueue<MqttMessage>) = thread(start = true) {
        logger.info("Start thread...")
        vertx.setPeriodic(1000) {
            if (queue.size > 0)
                logger.info("Retained message queue [$nr] size [${queue.size}]")
        }
        while (true) {
            queue.poll(100, TimeUnit.MILLISECONDS)?.let { message ->
                val topicName = MqttTopicName(message.topicName)
                if (message.payload.isEmpty()) {
                    index.del(topicName)
                    store.remove(topicName.identifier)
                } else {
                    index.add(topicName)
                    store.put(topicName.identifier, message)
                }
            }
        }
    }

    private var queueIdx=0
    fun saveMessage(message: MqttMessage): Future<Void> {
        logger.finest { "Save retained topic [${message.topicName}]" }
        try {
            queues[queueIdx].add(message)
        } catch (e: IllegalStateException) {

        }
        if (queues.size > 1 && ++queueIdx==queues.size) queueIdx=0
        return Future.succeededFuture()
    }

    fun findMatching(topicName: MqttTopicName, callback: (message: MqttMessage)->Unit): Future<Int> {
        val promise = Promise.promise<Int>()
        vertx.executeBlocking(Callable {
            var counter = 0
            try {
                index.findMatchingTopicNames(topicName) { topic ->
                    logger.finest { "Found matching topic [$topic] for [$topicName]" }
                    val message = store.get(topic.identifier)
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