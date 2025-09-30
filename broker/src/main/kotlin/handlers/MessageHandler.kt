package at.rocworks.handlers

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.*
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

    private val retainedQueueStore: ArrayBlockingQueue<BrokerMessage> = ArrayBlockingQueue(100_000) // TODO: configurable

    private val archiveQueues = mutableMapOf<String, ArrayBlockingQueue<BrokerMessage>>()

    // Runtime list of active archive groups (includes both startup and dynamically added ones)
    private val activeArchiveGroups = mutableMapOf<String, ArchiveGroup>()

    private val maxWriteBlockSize = 4000 // TODO: configurable

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun start() {
        logger.info("Start message handler [${Utils.getCurrentFunctionName()}]")
        writerThread("RM", retainedQueueStore, ::retainedQueueWriter)
        archiveGroups.forEach { group ->
            registerArchiveGroup(group)
        }
    }

    /**
     * Register an archive group for message routing (both startup and runtime)
     */
    fun registerArchiveGroup(archiveGroup: ArchiveGroup) {
        logger.info("Registering archive group [${archiveGroup.name}] with MessageHandler")

        // Add to active groups
        activeArchiveGroups[archiveGroup.name] = archiveGroup

        // Create queue for this archive group
        val queue = ArrayBlockingQueue<BrokerMessage>(100_000) // TODO: configurable
        archiveQueues[archiveGroup.name] = queue

        // Start writer thread for this archive group
        writerThread("AG-${archiveGroup.name}", queue) { list ->
            archiveGroup.lastValStore?.addAll(getLastMessages(list))
            archiveGroup.archiveStore?.addHistory(list)
        }

        logger.info("Archive group [${archiveGroup.name}] registered successfully")
    }

    /**
     * Unregister an archive group from message routing
     */
    fun unregisterArchiveGroup(archiveGroupName: String) {
        logger.info("Unregistering archive group [$archiveGroupName] from MessageHandler")

        // Remove from active groups
        activeArchiveGroups.remove(archiveGroupName)

        // Remove queue (the writer thread will terminate when queue is empty and not used)
        archiveQueues.remove(archiveGroupName)

        logger.info("Archive group [$archiveGroupName] unregistered successfully")
    }

    private fun retainedQueueWriter(list: List<BrokerMessage>) {
        val set = mutableSetOf<String>()
        val add = arrayListOf<BrokerMessage>()
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

    private fun getLastMessages(list: List<BrokerMessage>): List<BrokerMessage> {
        val map = mutableMapOf<String, BrokerMessage>()
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
                if (currentTime - lastCheckTime >= 1000 && queue.size > 5000) { // TODO: configurable
                    logger.warning("Queue [$name] size [${queue.size}] [${Utils.getCurrentFunctionName()}]")
                    lastCheckTime = currentTime
                }
            }
        }
    }

    fun saveMessage(message: BrokerMessage): Future<Void> {
        if (message.isRetain) {
            try {
                retainedQueueStore.add(message)
            } catch (e: IllegalStateException) {
                // TODO: handle exception
            }
        }

        activeArchiveGroups.values.forEach { archiveGroup ->
            val queue = archiveQueues[archiveGroup.name] ?: return@forEach
            if ((!archiveGroup.retainedOnly || message.isRetain) &&
                (archiveGroup.topicFilter.isEmpty() || archiveGroup.filterTree.isTopicNameMatching(message.topicName))) {
                try {
                    queue.add(message)
                } catch (e: IllegalStateException) {
                    // TODO: handle exception
                }
            }
        }

        return Future.succeededFuture()
    }

    fun findRetainedMessages(topicName: String, max: Int, callback: (message: BrokerMessage)->Unit): Future<Int> {
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