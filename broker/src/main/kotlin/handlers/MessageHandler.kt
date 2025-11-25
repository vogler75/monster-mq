package at.rocworks.handlers

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.*
import at.rocworks.stores.IMessageStore
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
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

    // Metrics tracking: messages written per archive group
    private val archiveWriteCounters = mutableMapOf<String, AtomicLong>()
    private val archiveWriteCountersSnapshot = mutableMapOf<String, Long>()
    private val archiveTimestampSnapshot = mutableMapOf<String, Long>()

    // Buffer size averaging
    private val archiveBufferSizeAccumulator = mutableMapOf<String, AtomicLong>()
    private val archiveBufferSampleCount = mutableMapOf<String, AtomicLong>()

    private val maxWriteBlockSize = 4000 // TODO: configurable

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun start() {
        logger.info("Start message handler.")
        writerThread("RM", retainedQueueStore, ::retainedQueueWriter)
        archiveGroups.forEach { group ->
            registerArchiveGroup(group)
        }

        // Setup event bus handlers for archive metrics
        setupArchiveMetricsHandlers()

        // Start periodic buffer size sampling (every 100ms to get good average)
        vertx.setPeriodic(100) {
            sampleArchiveBufferSizes()
        }
    }

    private fun sampleArchiveBufferSizes() {
        activeArchiveGroups.keys.forEach { groupName ->
            val bufferSize = archiveQueues[groupName]?.size ?: 0
            archiveBufferSizeAccumulator[groupName]?.addAndGet(bufferSize.toLong())
            archiveBufferSampleCount[groupName]?.incrementAndGet()
        }
    }

    private fun setupArchiveMetricsHandlers() {
        // Handler for archive groups list
        vertx.eventBus().consumer<JsonObject>(EventBusAddresses.Archive.GROUPS_LIST) { message ->
            val groupNames = io.vertx.core.json.JsonArray(activeArchiveGroups.keys.toList())
            message.reply(JsonObject().put("groups", groupNames))
        }

        // Handler for metrics collection for each archive group
        activeArchiveGroups.keys.forEach { groupName ->
            vertx.eventBus().consumer<JsonObject>(EventBusAddresses.Archive.groupMetrics(groupName)) { message ->
                val metrics = getArchiveGroupMetricsAndReset(groupName)
                message.reply(metrics)
            }

            vertx.eventBus().consumer<JsonObject>(EventBusAddresses.Archive.groupBufferSize(groupName)) { message ->
                val bufferSize = archiveQueues[groupName]?.size ?: 0
                message.reply(JsonObject().put("bufferSize", bufferSize))
            }
        }
    }

    private fun getArchiveGroupMetricsAndReset(groupName: String): JsonObject {
        val counter = archiveWriteCounters[groupName] ?: return JsonObject()
        val currentCount = counter.get()
        val lastSnapshot = archiveWriteCountersSnapshot[groupName] ?: 0L
        val valuesSinceLastReset = currentCount - lastSnapshot

        // Calculate time delta
        val currentTimestamp = System.currentTimeMillis()
        val lastTimestamp = archiveTimestampSnapshot[groupName] ?: currentTimestamp
        val elapsedSeconds = (currentTimestamp - lastTimestamp) / 1000.0

        // Calculate values per second (rate)
        val valuesPerSecond = if (elapsedSeconds > 0) {
            valuesSinceLastReset / elapsedSeconds
        } else {
            0.0
        }

        // Calculate average buffer size
        val bufferAccum = archiveBufferSizeAccumulator[groupName]?.get() ?: 0L
        val sampleCount = archiveBufferSampleCount[groupName]?.get() ?: 0L
        val avgBufferSize = if (sampleCount > 0) {
            (bufferAccum.toDouble() / sampleCount.toDouble()).toInt()
        } else {
            archiveQueues[groupName]?.size ?: 0
        }

        // Update snapshots for next calculation
        archiveWriteCountersSnapshot[groupName] = currentCount
        archiveTimestampSnapshot[groupName] = currentTimestamp
        archiveBufferSizeAccumulator[groupName]?.set(0)
        archiveBufferSampleCount[groupName]?.set(0)

        return JsonObject()
            .put("messagesOut", valuesPerSecond)
            .put("bufferSize", avgBufferSize)
    }

    /**
     * Register an archive group for message routing (both startup and runtime)
     */
    fun registerArchiveGroup(archiveGroup: ArchiveGroup) {
        logger.info("Registering archive group [${archiveGroup.name}] with MessageHandler")

        // Add to active groups
        activeArchiveGroups[archiveGroup.name] = archiveGroup

        // Initialize metrics counters
        archiveWriteCounters[archiveGroup.name] = AtomicLong(0)
        archiveWriteCountersSnapshot[archiveGroup.name] = 0L
        archiveTimestampSnapshot[archiveGroup.name] = System.currentTimeMillis()

        // Initialize buffer size tracking
        archiveBufferSizeAccumulator[archiveGroup.name] = AtomicLong(0)
        archiveBufferSampleCount[archiveGroup.name] = AtomicLong(0)

        // Create queue for this archive group
        val queue = ArrayBlockingQueue<BrokerMessage>(100_000) // TODO: configurable
        archiveQueues[archiveGroup.name] = queue

        // Start writer thread for this archive group
        writerThread("AG-${archiveGroup.name}", queue) { list ->
            archiveGroup.lastValStore?.addAll(getLastMessages(list))
            archiveGroup.archiveStore?.addHistory(list)
            // Track number of messages written
            archiveWriteCounters[archiveGroup.name]?.addAndGet(list.size.toLong())
        }

        // Register event bus handlers for this archive group (for dynamically added groups)
        if (vertx != null) {
            vertx.eventBus().consumer<JsonObject>(EventBusAddresses.Archive.groupMetrics(archiveGroup.name)) { message ->
                val metrics = getArchiveGroupMetricsAndReset(archiveGroup.name)
                message.reply(metrics)
            }

            vertx.eventBus().consumer<JsonObject>(EventBusAddresses.Archive.groupBufferSize(archiveGroup.name)) { message ->
                val bufferSize = archiveQueues[archiveGroup.name]?.size ?: 0
                message.reply(JsonObject().put("bufferSize", bufferSize))
            }
        }

        logger.info("Archive group [${archiveGroup.name}] registered successfully")
    }

    /**
     * Unregister an archive group from message routing
     */
    fun unregisterArchiveGroup(archiveGroupName: String) {
        logger.info("Unregister archive group [$archiveGroupName] from MessageHandler")

        // Remove from active groups
        activeArchiveGroups.remove(archiveGroupName)

        // Remove metrics tracking
        archiveWriteCounters.remove(archiveGroupName)
        archiveWriteCountersSnapshot.remove(archiveGroupName)
        archiveTimestampSnapshot.remove(archiveGroupName)

        // Remove buffer size tracking
        archiveBufferSizeAccumulator.remove(archiveGroupName)
        archiveBufferSampleCount.remove(archiveGroupName)

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