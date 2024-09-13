package at.rocworks.stores

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.*
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlin.concurrent.thread

class MessageHandler(private val store: IMessageStore): AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java, store.getName())

    private val addQueue: ArrayBlockingQueue<MqttMessage> = ArrayBlockingQueue<MqttMessage>(100_000)
    private val delQueue: ArrayBlockingQueue<MqttMessage> = ArrayBlockingQueue<MqttMessage>(100_000)

    private val maxRetainedMessagesSentToClient = 0 // 100_000 // TODO: configurable or timed
    private val maxWriteBlockSize = 4000 // TODO: configurable

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun start() {
        logger.info("Start handler [${Utils.getCurrentFunctionName()}]")
        writerThread(addQueue, store::addAll)
        writerThread(delQueue, store::delAll)
    }

    private fun writerThread(queue: ArrayBlockingQueue<MqttMessage>, execute: (List<MqttMessage>)->Unit)
    = thread(start = true) {
        logger.info("Start thread [${Utils.getCurrentFunctionName()}]")
        vertx.setPeriodic(1000) {
            if (queue.size > 0)
                logger.info("Message queue size [${queue.size}] [${Utils.getCurrentFunctionName()}]")
        }

        val block = arrayListOf<MqttMessage>()

        while (true) {
            queue.poll(100, TimeUnit.MILLISECONDS)?.let { message ->
                block.add(message)
                while (queue.poll()?.let(block::add) != null
                    && block.size < maxWriteBlockSize) {
                    // nothing to do here
                }

                if (block.size > 0) {
                    execute(block)
                    block.clear()
                }
            }
        }
    }

    fun saveMessage(message: MqttMessage): Future<Void> {
        logger.finest { "Save topic [${message.topicName}] [${Utils.getCurrentFunctionName()}]" }
        try {
            if (message.payload.isEmpty())
                delQueue.add(message)
            else
                addQueue.add(message)
        } catch (e: IllegalStateException) {
            // TODO: Alert
        }
        return Future.succeededFuture()
    }

    fun findMatching(topicName: String, callback: (message: MqttMessage)->Unit): Future<Int> {
        val promise = Promise.promise<Int>()
        vertx.executeBlocking(Callable {
            var counter = 0
            try {
                store.findMatchingMessages(topicName) { message ->
                    logger.finest { "Found matching message [${message.topicName}] for [$topicName] [${Utils.getCurrentFunctionName()}]" }
                    counter++
                    callback(message)
                    if (maxRetainedMessagesSentToClient > 0 && counter >= maxRetainedMessagesSentToClient) {
                        logger.warning("Maximum messages sent [${Utils.getCurrentFunctionName()}]")
                        false
                    } else true
                }
            } catch (e: Exception) {
                e.printStackTrace()
            }
            logger.fine { "Found [$counter] matching messages for [$topicName] [${Utils.getCurrentFunctionName()}]" }
            promise.complete(counter)
        })
        return promise.future()
    }
}