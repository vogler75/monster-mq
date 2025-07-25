package at.rocworks.handlers

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.*
import at.rocworks.stores.ArchiveGroup
import at.rocworks.stores.IMessageStore
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

class MessageHandler(
    private val retainedStore: IMessageStore,
    private val archiveGroups: List<ArchiveGroup>
): AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    private val retainedQueueStore: ArrayBlockingQueue<MqttMessage> = ArrayBlockingQueue(100_000) // TODO: configurable

    private val archiveQueues = mutableMapOf<String, ArrayBlockingQueue<MqttMessage>>()

    private val maxWriteBlockSize = 4000 // TODO: configurable

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun start() {
        logger.info("Start message handler [${Utils.getCurrentFunctionName()}]")
        writerThread("RM", retainedQueueStore, ::retainedQueueWriter)
        archiveGroups.forEach { group ->
            val queue = ArrayBlockingQueue<MqttMessage>(100_000) // TODO: configurable
            archiveQueues[group.name] = queue
            writerThread("AG-${group.name}", queue) { list ->
                group.lastValStore?.addAll(getLastMessages(list))
                group.archiveStore?.addHistory(list)
            }
        }
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

    private fun getLastMessages(list: List<MqttMessage>): List<MqttMessage> {
        val map = mutableMapOf<String, MqttMessage>()
        var i = list.size-1
        while (i >= 0) {
            val it = list[i]; i--
            if (!map.containsKey(it.topicName)) map[it.topicName] = it
        }
        return map.values.toList()
    }

    private fun <T> writerThread(name: String, queue: ArrayBlockingQueue<T>, execute: (List<T>)->Unit)
    = thread(start = true) {
        logger.finer("Start [$name] thread [${Utils.getCurrentFunctionName()}]")
        val block = arrayListOf<T>()
        var lastCheckTime = System.currentTimeMillis()
        while (true) {
            queue.poll(100, TimeUnit.MILLISECONDS)?.let { message ->
                block.add(message)
                while (queue.poll()?.let(block::add) != null
                    && block.size < maxWriteBlockSize) {
                    // nothing to do here
                }

                if (block.isNotEmpty()) {
                    execute(block)
                    block.clear()
                }

                val currentTime = System.currentTimeMillis()
                if (currentTime - lastCheckTime >= 1000 && queue.size > 1000) { // TODO: configurable
                    logger.warning("Queue [$name] size [${queue.size}] [${Utils.getCurrentFunctionName()}]")
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
        }

        archiveGroups.forEach {
            val queue = archiveQueues[it.name] ?: return@forEach
            if ((!it.retainedOnly || message.isRetain) &&
                (it.topicFilter.isEmpty() || it.filterTree.isTopicNameMatching(message.topicName))) {
                try {
                    queue.add(message)
                } catch (e: IllegalStateException) {
                    // TODO: handle exception
                }
            }
        }

        return Future.succeededFuture()
    }

    fun findRetainedMessages(topicName: String, max: Int, callback: (message: MqttMessage)->Unit): Future<Int> {
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