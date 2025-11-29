package at.rocworks.handlers

import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import io.vertx.core.Vertx
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger

/**
 * Worker thread for processing publish batches in parallel.
 * Each worker independently:
 * 1. Receives batches from queue
 * 2. Groups messages by topic
 * 3. Performs subscription lookups
 * 4. Forwards to clients
 *
 * This allows multiple batches to be processed concurrently,
 * reducing contention and improving throughput.
 */
class PublishWorker(
    val id: Int,
    private val vertx: Vertx,
    private val sessionHandler: SessionHandler
) {
    private val logger: Logger = Utils.getLogger(this::class.java)
    private val batchQueue = ArrayBlockingQueue<PublishBatch>(1000)

    // Metrics
    val batchesProcessed = AtomicLong(0)
    val messagesProcessed = AtomicLong(0)
    val totalProcessingTimeMs = AtomicLong(0)

    /**
     * Enqueue a batch for processing.
     * Returns immediately (non-blocking).
     */
    fun enqueueBatch(batch: PublishBatch): Boolean {
        return try {
            batchQueue.add(batch)
            true
        } catch (e: IllegalStateException) {
            logger.warning("PublishWorker[$id] queue overflow, batch with ${batch.messages.size} messages dropped")
            false
        }
    }

    /**
     * Start the worker thread in a background thread (not Vert.x worker thread).
     * Uses a separate executor service to avoid Vert.x blocked thread checker warnings.
     * This worker is meant to run indefinitely, blocking on queue.poll().
     */
    fun start(executor: ExecutorService) {
        executor.submit {
            try {
                processBatches()
            } catch (e: Exception) {
                logger.severe("PublishWorker[$id] error: ${e.message}")
                e.printStackTrace()
            }
        }
    }

    /**
     * Main loop: continuously process batches from queue.
     */
    private fun processBatches() {
        logger.fine("PublishWorker[$id] started")

        try {
            while (!Thread.currentThread().isInterrupted) {
                try {
                    // Wait for batch with 100ms timeout
                    val batch = batchQueue.poll(100, TimeUnit.MILLISECONDS) ?: continue
                    processSingleBatch(batch)
                } catch (e: InterruptedException) {
                    logger.warning("PublishWorker[$id] interrupted, stopping")
                    Thread.currentThread().interrupt()
                    break
                } catch (e: Exception) {
                    logger.severe("PublishWorker[$id] error processing batch: ${e.message}")
                }
            }
        } finally {
            logger.fine("PublishWorker[$id] stopped")
        }
    }

    /**
     * Process a single batch.
     * Groups messages by topic, then processes each topic.
     */
    private fun processSingleBatch(batch: PublishBatch) {
        val startTime = System.currentTimeMillis()

        try {
            // Step 1: Group messages by topic
            val messagesByTopic = batch.messages.groupBy { it.topicName }
            logger.finest { "Worker[$id] processing batch: ${batch.messages.size} messages, ${messagesByTopic.size} unique topics" }

            // Step 2: For each topic, process all messages together
            messagesByTopic.forEach { (topicName, messages) ->
                sessionHandler.processTopic(topicName, messages)
            }

            val duration = System.currentTimeMillis() - startTime
            batchesProcessed.incrementAndGet()
            messagesProcessed.addAndGet(batch.messages.size.toLong())
            totalProcessingTimeMs.addAndGet(duration)

            logger.finest { "Worker[$id] batch processed: ${batch.messages.size} messages in ${duration}ms" }
        } catch (e: Exception) {
            logger.severe("Worker[$id] error processing batch: ${e.message}")
            e.printStackTrace()
        }
    }

    /**
     * Get average batch processing time in milliseconds.
     */
    fun getAvgProcessingTimeMs(): Long {
        val batches = batchesProcessed.get()
        return if (batches > 0) totalProcessingTimeMs.get() / batches else 0
    }

    /**
     * Get throughput in messages per second.
     */
    fun getThroughputMsgs(): Long {
        val batches = batchesProcessed.get()
        val messages = messagesProcessed.get()
        return if (messages > 0) messages * 1000 / Math.max(totalProcessingTimeMs.get(), 1) else 0
    }

    /**
     * Get current queue size (number of pending batches).
     */
    fun getQueueSize(): Int {
        return batchQueue.size
    }

    /**
     * Get formatted status string for logging.
     */
    fun getStatusString(): String {
        val batches = batchesProcessed.get()
        val messages = messagesProcessed.get()
        val avgTime = getAvgProcessingTimeMs()
        val throughput = getThroughputMsgs()
        val queueSize = getQueueSize()

        return "Worker[$id]: batches=$batches, msgs=$messages, avg=${avgTime}ms, throughput=${throughput}msg/s, queue=$queueSize"
    }
}

/**
 * Immutable publish batch.
 */
data class PublishBatch(
    val messages: List<BrokerMessage>,
    val createdAt: Long = System.currentTimeMillis()
)

/**
 * Pool of PublishWorker threads.
 * Manages worker lifecycle and round-robin batch distribution.
 *
 * Uses a dedicated ExecutorService (not Vert.x worker threads) to avoid
 * blocked thread checker warnings, since workers do blocking poll() indefinitely.
 */
class PublishWorkerPool(
    val vertx: Vertx,
    val sessionHandler: SessionHandler,
    val numWorkers: Int = 4
) {
    private val logger: Logger = Utils.getLogger(this::class.java)
    val workers = List(numWorkers) { id -> PublishWorker(id, vertx, sessionHandler) }

    // Dedicated executor service for workers (not Vert.x managed)
    // Uses daemon threads so application can shutdown without waiting for workers
    private val executor: ExecutorService = Executors.newFixedThreadPool(
        numWorkers,
        { runnable ->
            Thread(runnable, "PublishWorker-${System.nanoTime()}").apply {
                isDaemon = true  // Daemon threads won't block JVM shutdown
                priority = Thread.NORM_PRIORITY
            }
        }
    )

    private var roundRobinIndex = 0
    private val lock = java.util.concurrent.locks.ReentrantLock()

    // For per-second rate calculations
    private var lastMetricsTime = System.currentTimeMillis()
    private var lastBatchesProcessed = 0L
    private var lastMessagesProcessed = 0L
    private val lastWorkerMessagesProcessed = LongArray(numWorkers)

    /**
     * Distribute a batch to the next available worker (round-robin).
     */
    fun sendBatch(batch: PublishBatch): Boolean {
        lock.lock()
        try {
            val workerIndex = roundRobinIndex % workers.size
            roundRobinIndex = (roundRobinIndex + 1) % workers.size

            val success = workers[workerIndex].enqueueBatch(batch)
            if (!success) {
                logger.warning("Worker[$workerIndex] queue full, batch with ${batch.messages.size} messages dropped")
            }
            return success
        } finally {
            lock.unlock()
        }
    }

    /**
     * Start all workers in the executor service.
     */
    fun start() {
        workers.forEach { worker ->
            worker.start(executor)
        }
        logger.fine("Started PublishWorkerPool with $numWorkers workers (dedicated executor service, not Vert.x managed)")
    }

    /**
     * Shutdown the worker pool gracefully.
     * Should be called during verticle stop.
     *
     * Since workers are daemon threads, they will be killed when JVM exits.
     * This method attempts graceful shutdown but doesn't block indefinitely.
     */
    fun shutdown() {
        logger.fine("Shutting down PublishWorker...")
        try {
            // Request graceful shutdown (no new tasks accepted)
            executor.shutdownNow()
            logger.fine("PublishWorkerPool force shutdown complete")
        } catch (e: InterruptedException) {
            logger.warning("Interrupted waiting for PublishWorker shutdown: ${e.message}")
        }
    }

    /**
     * Get pool-wide metrics.
     */
    fun getMetrics(): WorkerPoolMetrics {
        var totalBatches = 0L
        var totalMessages = 0L
        var totalTime = 0L

        workers.forEach { worker ->
            totalBatches += worker.batchesProcessed.get()
            totalMessages += worker.messagesProcessed.get()
            totalTime += worker.totalProcessingTimeMs.get()
        }

        return WorkerPoolMetrics(
            workersCount = numWorkers,
            totalBatches = totalBatches,
            totalMessages = totalMessages,
            totalProcessingTimeMs = totalTime,
            avgBatchSize = if (totalBatches > 0) totalMessages / totalBatches else 0,
            avgProcessingTimeMs = if (totalBatches > 0) totalTime / totalBatches else 0,
            throughputMsgs = if (totalTime > 0) totalMessages * 1000 / totalTime else 0
        )
    }

    /**
     * Per-worker metrics.
     */
    fun getWorkerMetrics(workerId: Int): WorkerMetrics? {
        if (workerId < 0 || workerId >= workers.size) return null
        val worker = workers[workerId]
        return WorkerMetrics(
            workerId = workerId,
            batchesProcessed = worker.batchesProcessed.get(),
            messagesProcessed = worker.messagesProcessed.get(),
            avgProcessingTimeMs = worker.getAvgProcessingTimeMs(),
            throughputMsgs = worker.getThroughputMsgs()
        )
    }

    /**
     * Log all worker load/metrics in a human-readable format.
     */
    fun logWorkerLoad() {
        val poolMetrics = getMetrics()
        logger.finer("=== PublishWorkerPool Load ===")
        logger.finer("Pool Summary: totalBatches=${poolMetrics.totalBatches}, totalMsgs=${poolMetrics.totalMessages}, totalTime=${poolMetrics.totalProcessingTimeMs}ms, avgBatchSize=${poolMetrics.avgBatchSize}, poolThroughput=${poolMetrics.throughputMsgs}msg/s")

        workers.forEach { worker ->
            logger.finer(worker.getStatusString())
        }
    }

    /**
     * Publish worker pool metrics to MQTT $SYS topic.
     * Shows per-second rates instead of cumulative values.
     */
    fun publishMetrics(nodeName: String) {
        val currentTime = System.currentTimeMillis()
        val timeDeltaMs = currentTime - lastMetricsTime
        val timeDeltaSec = if (timeDeltaMs > 0) timeDeltaMs / 1000.0 else 1.0

        val currentBatches = workers.sumOf { it.batchesProcessed.get() }
        val currentMessages = workers.sumOf { it.messagesProcessed.get() }

        val batchesDelta = currentBatches - lastBatchesProcessed
        val messagesDelta = currentMessages - lastMessagesProcessed

        val batchesPerSec = (batchesDelta / timeDeltaSec).toLong()
        val messagesPerSec = (messagesDelta / timeDeltaSec).toLong()

        lastMetricsTime = currentTime
        lastBatchesProcessed = currentBatches
        lastMessagesProcessed = currentMessages

        val metricsJson = buildString {
            append("{")
            append("\"workersCount\":${workers.size},")
            append("\"batchesPerSec\":$batchesPerSec,")
            append("\"messagesPerSec\":$messagesPerSec,")
            append("\"workers\":[")
            append(workers.mapIndexed { idx, worker ->
                val currentWorkerMessages = worker.messagesProcessed.get()
                val workerMessagesDelta = currentWorkerMessages - lastWorkerMessagesProcessed[idx]
                val workerMessagesPerSec = (workerMessagesDelta / timeDeltaSec).toLong()
                lastWorkerMessagesProcessed[idx] = currentWorkerMessages
                "{\"id\":$idx,\"messagesPerSec\":$workerMessagesPerSec,\"queueSize\":${worker.getQueueSize()}}"
            }.joinToString(","))
            append("]")
            append("}")
        }

        val message = BrokerMessage("", "\$SYS/brokers/$nodeName/processing", metricsJson)

        sessionHandler.publishMessage(message)
    }

    /**
     * Get all worker status strings.
     * Useful for programmatic access to worker metrics.
     */
    fun getWorkerStatusStrings(): List<String> {
        return workers.map { it.getStatusString() }
    }
}

/**
 * Pool-level metrics.
 */
data class WorkerPoolMetrics(
    val workersCount: Int,
    val totalBatches: Long,
    val totalMessages: Long,
    val totalProcessingTimeMs: Long,
    val avgBatchSize: Long,
    val avgProcessingTimeMs: Long,
    val throughputMsgs: Long
)

/**
 * Per-worker metrics.
 */
data class WorkerMetrics(
    val workerId: Int,
    val batchesProcessed: Long,
    val messagesProcessed: Long,
    val avgProcessingTimeMs: Long,
    val throughputMsgs: Long
)
