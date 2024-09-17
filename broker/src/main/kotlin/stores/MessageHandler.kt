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
    private val retainedArchive: IMessageArchive?,
    private val lastValueFilter: List<String>,
    private val lastValueStore: IMessageStore?,
    private val lastValueArchive: IMessageArchive?
): AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    private val retainedQueueStore: ArrayBlockingQueue<MqttMessage> = ArrayBlockingQueue(100_000) // TODO: configurable
    private val retainedQueueArchive: ArrayBlockingQueue<MqttMessage> = ArrayBlockingQueue(100_000) // TODO: configurable

    private val lastValueQueueStore: ArrayBlockingQueue<MqttMessage> = ArrayBlockingQueue(100_000) // TODO: configurable
    private val lastValueQueueArchive: ArrayBlockingQueue<MqttMessage> = ArrayBlockingQueue(100_000) // TODO: configurable

    private val maxWriteBlockSize = 4000 // TODO: configurable

    private val lastValueFilterTree = TopicTree<Boolean, Boolean>()

    init {
        logger.level = Const.DEBUG_LEVEL
        lastValueFilter.forEach { lastValueFilterTree.add(it, true, true) }
    }

    override fun start() {
        logger.info("Start handler [${Utils.getCurrentFunctionName()}]")
        writerThread("RM", retainedQueueStore, ::retainedQueueWriter)
        writerThread("LV", lastValueQueueStore, ::lastValueQueueWriter)
        retainedArchive?.let { writerThread("RMA", retainedQueueArchive, it::addAllHistory) }
        lastValueArchive?.let { writerThread("LVA", lastValueQueueArchive, it::addAllHistory) }
    }

    private fun retainedQueueWriter(list: List<MqttMessage>) {
        val set = mutableSetOf<String>()
        val add = arrayListOf<MqttMessage>()
        val del = arrayListOf<String>()
        var i = list.size-1
        while (i >= 0) {
            val it = list[i]; i--
            if (!set.contains(it.topicName)) {
                set.add(it.topicName)
                if (it.payload.isEmpty())
                    del.add(it.topicName)
                else
                    add.add(it)
            }
        }
        if (add.isNotEmpty()) retainedStore.addAll(add)
        if (del.isNotEmpty()) retainedStore.delAll(del)
    }

    private fun lastValueQueueWriter(list: List<MqttMessage>) {
        val set = mutableSetOf<String>()
        val add = arrayListOf<MqttMessage>()
        var i = list.size-1
        while (i >= 0) {
            val it = list[i]; i--
            if (!set.contains(it.topicName)) {
                set.add(it.topicName)
                add.add(it)
            }
        }
        lastValueStore?.addAll(add)
    }

    private fun <T> writerThread(name: String, queue: ArrayBlockingQueue<T>, execute: (List<T>)->Unit)
    = thread(start = true) {
        logger.info("Start [$name] thread [${Utils.getCurrentFunctionName()}]")
        vertx.setPeriodic(1000) {
            if (queue.size > 100) // TODO: configurable
                logger.info("Queue [$name] size [${queue.size}] [${Utils.getCurrentFunctionName()}]")
        }

        val block = arrayListOf<T>()

        var lastCheckTime = System.currentTimeMillis()
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

                val currentTime = System.currentTimeMillis()
                if (currentTime - lastCheckTime >= 1000 && queue.size > 100) { // TODO: configurable
                    logger.info("Queue [$name] size [${queue.size}] [${Utils.getCurrentFunctionName()}]")
                    lastCheckTime = currentTime
                }
            }
        }
    }

    fun saveMessage(message: MqttMessage): Future<Void> {
        if (message.isRetain) {
            try {
                retainedQueueStore.add(message)
            } catch (e: IllegalStateException) {
                // TODO: handle exception
            }
            if (retainedArchive != null) {
                try {
                    retainedQueueArchive.add(message)
                } catch (e: IllegalStateException) {
                    // TODO: handle exception
                }
            }
        }

        fun processLastValue() {
            if (lastValueStore != null) {
                try {
                    lastValueQueueStore.add(message)
                } catch (e: IllegalStateException) {
                    // TODO: handle exception
                }
            }
            if (lastValueArchive != null) {
                try {
                    lastValueQueueArchive.add(message)
                } catch (e: IllegalStateException) {
                    // TODO: handle exception
                }
            }
        }

        if (lastValueFilter.isEmpty()) processLastValue()
        else {
            val matched = lastValueFilterTree.isTopicNameMatching(message.topicName)
            if (matched) processLastValue()
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