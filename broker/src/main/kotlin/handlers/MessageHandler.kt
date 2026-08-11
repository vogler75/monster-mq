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
import at.rocworks.queue.IMessageQueue
import at.rocworks.queue.MessageQueueDisk
import at.rocworks.queue.MessageQueueMemory
import at.rocworks.queue.MessageQueueNone
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

class MessageHandler(
    private val retainedStore: IMessageStore,
    private val archiveGroups: List<ArchiveGroup>
): AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    private val retainedQueueStore: ArrayBlockingQueue<BrokerMessage> = ArrayBlockingQueue(100_000) // TODO: configurable

    private val archiveQueues = mutableMapOf<String, IMessageQueue>()
    private val archiveWriterThreadsStop = mutableMapOf<String, AtomicBoolean>()

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


    override fun start() {
        logger.fine("Start message handler.")
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
            val bufferSize = archiveQueues[groupName]?.getSize() ?: 0
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
                val bufferSize = archiveQueues[groupName]?.getSize() ?: 0
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
            archiveQueues[groupName]?.getSize() ?: 0
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
        logger.info("Registering archive group [${archiveGroup.name}] with MessageHandler (QueueType: ${archiveGroup.queueType})")

        // Add to active groups
        activeArchiveGroups[archiveGroup.name] = archiveGroup

        // Initialize metrics counters
        archiveWriteCounters[archiveGroup.name] = AtomicLong(0)
        archiveWriteCountersSnapshot[archiveGroup.name] = 0L
        archiveTimestampSnapshot[archiveGroup.name] = System.currentTimeMillis()

        // Initialize buffer size tracking
        archiveBufferSizeAccumulator[archiveGroup.name] = AtomicLong(0)
        archiveBufferSampleCount[archiveGroup.name] = AtomicLong(0)

        // Create queue and writer thread only if this archive group has an active archiveStore
        if (archiveGroup.archiveStore != null) {
            val queue: IMessageQueue = when (archiveGroup.queueType) {
                "DISK" -> MessageQueueDisk(
                    queueName = "archive",
                    deviceName = archiveGroup.name,
                    logger = logger,
                    queueSize = archiveGroup.queueSize,
                    blockSize = archiveGroup.bulkSize,
                    pollTimeout = archiveGroup.bulkTimeoutMs,
                    diskPath = archiveGroup.queueDiskPath
                )
                "MEMORY" -> MessageQueueMemory(
                    logger = logger,
                    queueSize = archiveGroup.queueSize,
                    blockSize = archiveGroup.bulkSize,
                    pollTimeout = archiveGroup.bulkTimeoutMs
                )
                else -> MessageQueueNone(
                    logger = logger,
                    queueSize = archiveGroup.queueSize,
                    blockSize = archiveGroup.bulkSize,
                    pollTimeout = archiveGroup.bulkTimeoutMs
                )
            }
            archiveQueues[archiveGroup.name] = queue

            val stopFlag = AtomicBoolean(false)
            archiveWriterThreadsStop[archiveGroup.name] = stopFlag

            // Start writer thread for this archive group
            archiveGroupWriterThread("AG-${archiveGroup.name}", archiveGroup, queue, stopFlag)
        }

        // Register event bus handlers for this archive group (for dynamically added groups)
        if (vertx != null) {
            vertx.eventBus().consumer<JsonObject>(EventBusAddresses.Archive.groupMetrics(archiveGroup.name)) { message ->
                val metrics = getArchiveGroupMetricsAndReset(archiveGroup.name)
                message.reply(metrics)
            }

            vertx.eventBus().consumer<JsonObject>(EventBusAddresses.Archive.groupBufferSize(archiveGroup.name)) { message ->
                val bufferSize = archiveQueues[archiveGroup.name]?.getSize() ?: 0
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

        // Stop thread and close queue
        archiveWriterThreadsStop.remove(archiveGroupName)?.set(true)
        archiveQueues.remove(archiveGroupName)?.close()

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

    private fun archiveGroupWriterThread(
        threadName: String,
        archiveGroup: ArchiveGroup,
        queue: IMessageQueue,
        stopFlag: AtomicBoolean
    ) = thread(start = true, name = threadName) {
        logger.fine("Start [$threadName] thread [${Utils.getCurrentFunctionName()}]")
        var lastErrorLog = 0L

        while (!stopFlag.get()) {
            try {
                val polledMessages = mutableListOf<BrokerMessage>()
                val blockSize = queue.pollBlock { msg ->
                    polledMessages.add(msg)
                }

                if (blockSize > 0 && polledMessages.isNotEmpty()) {
                    try {
                        archiveGroup.archiveStore?.addHistory(polledMessages)
                        queue.pollCommit()
                        archiveWriteCounters[archiveGroup.name]?.addAndGet(polledMessages.size.toLong())
                    } catch (e: Exception) {
                        val now = System.currentTimeMillis()
                        if (now - lastErrorLog > 5000) {
                            if (archiveGroup.queueType == "NONE") {
                                logger.warning("Error writing batch to archive [$threadName]: ${e.message} (Store-and-Forward disabled)")
                            } else {
                                logger.warning("Error writing batch to archive [$threadName]: ${e.message}. Retrying batch via Store-and-Forward...")
                            }
                            lastErrorLog = now
                        }
                        if (archiveGroup.queueType != "NONE") {
                            Thread.sleep(1000)
                        }
                    }
                }
            } catch (e: Exception) {
                logger.warning("Error in archive writer thread [$threadName]: ${e.message}")
                Thread.sleep(500)
            }
        }
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
            if ((!archiveGroup.retainedOnly || message.isRetain) &&
                (archiveGroup.topicFilter.isEmpty() || archiveGroup.filterTree.isTopicNameMatching(message.topicName))) {
                
                archiveGroup.lastValStore?.let { lastValStore ->
                    try {
                        lastValStore.addAll(listOf(message))
                    } catch (e: Exception) {
                        logger.warning("Error updating last value store for group [${archiveGroup.name}]: ${e.message}")
                    }
                }

                if (archiveGroup.archiveStore != null) {
                    val queue = archiveQueues[archiveGroup.name]
                    if (queue != null) {
                        try {
                            queue.add(message)
                        } catch (e: IllegalStateException) {
                            // TODO: handle exception
                        }
                    }
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