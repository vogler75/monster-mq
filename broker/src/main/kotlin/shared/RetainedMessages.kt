package at.rocworks.shared

import at.rocworks.Const
import at.rocworks.data.TopicTree
import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttTopicName
import com.sun.jmx.remote.internal.ArrayQueue
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

    private val maxRetainedMessagesSentToClient = 0 // 100_000 // TODO: configurable or timed

    init {
        logger.level = Const.DEBUG_LEVEL
        val queuesCount = 1 // TODO: configurable
        queues = List(queuesCount) { ArrayBlockingQueue<MqttMessage>(100_000) } // TODO: configurable
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

        /*
         var point: DataPoint? = pollWait()
            while (point != null) {
                if (point.value.sourceTime.epochSecond > 0) {
                    outputBlock.add(point)
                    handler(point)
                }
                point = if (outputBlock.size < writeParameterBlockSize) pollNoWait() else null
            }
         */
        val addBlock = arrayListOf<MqttMessage>()
        val delBlock = arrayListOf<MqttMessage>()

        fun addMessage(message: MqttMessage) {
            if (message.payload.isEmpty())
                delBlock.add(message)
            else
                addBlock.add(message)
        }

        while (true) {
            queue.poll(100, TimeUnit.MILLISECONDS)?.let { message ->
                addMessage(message)
                while (queue.poll()?.let(::addMessage) != null && addBlock.size < 1000 && delBlock.size < 1000) {
                    // nothing to do here
                }

                delBlock.forEach { // there is no delAll
                    val topicName = MqttTopicName(it.topicName)
                    index.del(topicName)
                    store.remove(topicName.identifier)
                }

                index.addAll(addBlock.map { it.topicName }.distinct().map { MqttTopicName(it) })
                store.putAll(addBlock.map { Pair(it.topicName, it) })
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
                    if (message != null) {
                        counter++
                        callback(message)
                    } else { // it could be a node inside the tree index where we haven't stored a value
                        logger.finest { "Stored message for [$topic] is null!" }
                    }
                    if (maxRetainedMessagesSentToClient>0 && counter>=maxRetainedMessagesSentToClient) {
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