package at.rocworks.shared

import at.rocworks.Const
import at.rocworks.data.*
import at.rocworks.stores.IMessageStore
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlin.concurrent.thread

class RetainedMessageHandler(private val store: IMessageStore): AbstractVerticle() {
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
                while (queue.poll()?.let(::addMessage) != null
                    && addBlock.size < 1000
                    && delBlock.size < 1000) {
                    // nothing to do here
                }

                if (delBlock.size > 0) {
                    store.delAll(delBlock)
                    delBlock.clear()
                }

                if (addBlock.size > 0) {
                    store.addAll(addBlock)
                    addBlock.clear()
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
            // TODO: Alert
        }
        if (queues.size > 1 && ++queueIdx==queues.size) queueIdx=0
        return Future.succeededFuture()
    }

    fun findMatching(topicName: String, callback: (message: MqttMessage)->Unit): Future<Int> {
        val promise = Promise.promise<Int>()
        vertx.executeBlocking(Callable {
            var counter = 0
            try {
                store.findMatchingMessages(topicName) { message ->
                    logger.finest { "Found matching message [${message.topicName}] for [$topicName]" }
                    counter++
                    callback(message)
                    if (maxRetainedMessagesSentToClient > 0 && counter >= maxRetainedMessagesSentToClient) {
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