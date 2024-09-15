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
import kotlin.concurrent.thread

class MessageHandler(
    private val retainedStore: IMessageStore,
    private val retainedStoreHistory: Boolean,
    private val lastValueStore: IMessageStore?,
    private val lastValueStoreHistory: Boolean
): AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    private val retainedAddQueue: ArrayBlockingQueue<MqttMessage> = ArrayBlockingQueue<MqttMessage>(100_000) // TODO: configurable
    private val retainedDelQueue: ArrayBlockingQueue<MqttMessage> = ArrayBlockingQueue<MqttMessage>(100_000) // TODO: configurable
    private val lastValueQueue: ArrayBlockingQueue<MqttMessage> = ArrayBlockingQueue<MqttMessage>(100_000) // TODO: configurable

    private val maxWriteBlockSize = 4000 // TODO: configurable

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun start() {
        logger.info("Start handler [${Utils.getCurrentFunctionName()}]")
        // Retained messages
        writerThread("RA", retainedAddQueue, retainedStore::addAll) // TODO: add and remove in different threads is dangerous because of message order
        writerThread("RD", retainedDelQueue, retainedStore::delAll)

        // Retained messages history
        writerThread("RH", retainedAddQueue, retainedStore::addAllHistory)

        // Last value messages
        if (lastValueStore != null) {
            writerThread("LV", lastValueQueue, lastValueStore::addAll) // Last value messages
            writerThread("HV", lastValueQueue, lastValueStore::addAllHistory) // Last value messages history
        }
    }

    private fun writerThread(name: String, queue: ArrayBlockingQueue<MqttMessage>, execute: (List<MqttMessage>)->Unit)
    = thread(start = true) {
        logger.info("Start [$name] thread [${Utils.getCurrentFunctionName()}]")
        vertx.setPeriodic(1000) {
            if (queue.size > 0)
                logger.info("Queue [$name] size [${queue.size}] [${Utils.getCurrentFunctionName()}]")
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
        if (message.isRetain) {
            try {
                if (message.payload.isEmpty())
                    retainedDelQueue.add(message)
                else {
                    retainedAddQueue.add(message)
                    if (retainedStoreHistory) {
                        retainedAddQueue.add(message)
                    }
                }
            } catch (e: IllegalStateException) {
                // TODO: Alert when queue is full
            }
        }
        if (lastValueStore != null) {
            try {
                lastValueQueue.add(message)
                if (lastValueStoreHistory) {
                    lastValueQueue.add(message)
                }
            } catch (e: IllegalStateException) {
                // TODO: Alert when queue is full
            }
        }
        return Future.succeededFuture()
    }

    fun findMatching(topicName: String, max: Int, callback: (message: MqttMessage)->Unit): Future<Int> {
        val promise = Promise.promise<Int>()
        vertx.executeBlocking(Callable {
            var counter = 0
            try {
                retainedStore.findMatchingMessages(topicName) { message ->
                    logger.finest { "Found matching message [${message.topicName}] for [$topicName] [${Utils.getCurrentFunctionName()}]" }
                    counter++
                    callback(message)
                    if (max > 0 && counter > max) {
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