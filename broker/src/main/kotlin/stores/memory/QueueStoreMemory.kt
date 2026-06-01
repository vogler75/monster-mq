package at.rocworks.stores.memory

import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.IQueueStoreSync
import at.rocworks.stores.QueueStoreType
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import java.util.concurrent.ConcurrentHashMap
import java.util.LinkedHashMap

/**
 * High-performance, ultra-reliable in-memory queue store implementing visibility timeouts (vt).
 * In-memory queue store with zero database lock contention.
 */
class QueueStoreMemory(
    private val visibilityTimeoutSeconds: Int = 30
) : AbstractVerticle(), IQueueStoreSync {

    private val logger = Utils.getLogger(this::class.java)
    private val queues = ConcurrentHashMap<String, MemoryQueue>()

    private class MemoryQueue {
        val messages = LinkedHashMap<String, MemoryQueuedMessage>()
    }

    class MemoryQueuedMessage(
        val message: BrokerMessage,
        @Volatile var vt: Long,
        @Volatile var readCt: Int = 0
    )

    override fun getType(): QueueStoreType = QueueStoreType.MEMORY

    override fun start(startPromise: Promise<Void>) {
        logger.info("Memory queue store initialized and ready")
        startPromise.complete()
    }

    override fun enqueueMessages(messages: List<Pair<BrokerMessage, List<String>>>) {
        if (messages.isEmpty()) return
        val now = System.currentTimeMillis() / 1000
        messages.forEach { (message, clientIds) ->
            val queuedMessage = if (!message.isQueued) {
                BrokerMessage(
                    messageUuid = message.messageUuid,
                    messageId = message.messageId,
                    topicName = message.topicName,
                    payload = message.payload,
                    qosLevel = message.qosLevel,
                    isRetain = message.isRetain,
                    isDup = message.isDup,
                    isQueued = true,
                    clientId = message.clientId,
                    senderId = message.senderId,
                    time = message.time,
                    messageExpiryInterval = message.messageExpiryInterval,
                    payloadFormatIndicator = message.payloadFormatIndicator,
                    contentType = message.contentType,
                    responseTopic = message.responseTopic,
                    correlationData = message.correlationData,
                    userProperties = message.userProperties
                )
            } else {
                message
            }
            clientIds.forEach { clientId ->
                val queue = queues.computeIfAbsent(clientId) { MemoryQueue() }
                synchronized(queue) {
                    queue.messages[queuedMessage.messageUuid] = MemoryQueuedMessage(queuedMessage, now)
                }
            }
        }
        logger.fine { "Enqueued ${messages.size} messages in-memory for ${messages.sumOf { it.second.size }} clients" }
    }

    override fun dequeueMessages(clientId: String, callback: (BrokerMessage) -> Boolean) {
        val queue = queues[clientId] ?: return
        val now = System.currentTimeMillis() / 1000
        val currentTimeMillis = System.currentTimeMillis()
        val readyMessages = synchronized(queue) {
            queue.messages.values
                .asSequence()
                .filter { it.vt <= now && !isExpired(it.message, currentTimeMillis) }
                .map { it.message }
                .toList()
        }
        for (message in readyMessages) {
            val proceed = callback(message)
            if (!proceed) break
        }
    }

    override fun removeMessages(messages: List<Pair<String, String>>) {
        if (messages.isEmpty()) return
        messages.groupBy({ it.first }, { it.second }).forEach { (clientId, messageUuids) ->
            val queue = queues[clientId] ?: return@forEach
            synchronized(queue) {
                messageUuids.forEach { queue.messages.remove(it) }
                if (queue.messages.isEmpty()) {
                    queues.remove(clientId, queue)
                }
            }
        }
    }

    override fun fetchNextPendingMessage(clientId: String): BrokerMessage? {
        return fetchPendingMessages(clientId, 1).firstOrNull()
    }

    override fun fetchPendingMessages(clientId: String, limit: Int): List<BrokerMessage> {
        val queue = queues[clientId] ?: return emptyList()
        val now = System.currentTimeMillis() / 1000
        val currentTimeMillis = System.currentTimeMillis()
        return synchronized(queue) {
            val result = ArrayList<BrokerMessage>(limit)
            for (qm in queue.messages.values) {
                if (qm.vt <= now && !isExpired(qm.message, currentTimeMillis)) {
                    result.add(qm.message)
                    if (result.size >= limit) break
                }
            }
            result
        }
    }

    override fun fetchAndLockPendingMessages(clientId: String, limit: Int): List<BrokerMessage> {
        val queue = queues[clientId] ?: return emptyList()
        val now = System.currentTimeMillis() / 1000
        val currentTimeMillis = System.currentTimeMillis()
        val vtFuture = now + visibilityTimeoutSeconds
        return synchronized(queue) {
            val result = ArrayList<BrokerMessage>(limit)
            for (qm in queue.messages.values) {
                if (qm.vt <= now && !isExpired(qm.message, currentTimeMillis)) {
                    qm.vt = vtFuture
                    qm.readCt++
                    result.add(qm.message)
                    if (result.size >= limit) break
                }
            }
            result
        }
    }

    override fun markMessageInFlight(clientId: String, messageUuid: String) {
        val queue = queues[clientId] ?: return
        val now = System.currentTimeMillis() / 1000
        val vtFuture = now + visibilityTimeoutSeconds
        synchronized(queue) {
            val qm = queue.messages[messageUuid]
            if (qm != null && qm.vt <= now) {
                qm.vt = vtFuture
                qm.readCt++
            }
        }
    }

    override fun markMessagesInFlight(clientId: String, messageUuids: List<String>) {
        if (messageUuids.isEmpty()) return
        val queue = queues[clientId] ?: return
        val now = System.currentTimeMillis() / 1000
        val vtFuture = now + visibilityTimeoutSeconds
        synchronized(queue) {
            messageUuids.forEach { messageUuid ->
                val qm = queue.messages[messageUuid]
                if (qm != null && qm.vt <= now) {
                    qm.vt = vtFuture
                    qm.readCt++
                }
            }
        }
    }

    override fun markMessageDelivered(clientId: String, messageUuid: String) {
        val queue = queues[clientId] ?: return
        synchronized(queue) {
            queue.messages.remove(messageUuid)
            if (queue.messages.isEmpty()) {
                queues.remove(clientId, queue)
            }
        }
    }

    override fun resetInFlightMessages(clientId: String) {
        val queue = queues[clientId] ?: return
        val now = System.currentTimeMillis() / 1000
        synchronized(queue) {
            queue.messages.values.forEach { qm ->
                if (qm.vt > now) {
                    qm.vt = now
                    qm.readCt = 0
                }
            }
        }
    }

    override fun purgeDeliveredMessages(): Int = 0

    override fun purgeExpiredMessages(): Int {
        val currentTimeMillis = System.currentTimeMillis()
        var totalRemoved = 0
        queues.forEach { (clientId, queue) ->
            synchronized(queue) {
                val sizeBefore = queue.messages.size
                queue.messages.entries.removeIf { isExpired(it.value.message, currentTimeMillis) }
                totalRemoved += (sizeBefore - queue.messages.size)
                if (queue.messages.isEmpty()) {
                    queues.remove(clientId, queue)
                }
            }
        }
        return totalRemoved
    }

    override fun purgeQueuedMessages() {
        // No-op
    }

    override fun deleteClientMessages(clientId: String) {
        queues.remove(clientId)
    }

    override fun countQueuedMessages(): Long {
        return queues.values.sumOf { queue ->
            synchronized(queue) {
                queue.messages.size
            }
        }.toLong()
    }

    override fun countQueuedMessagesForClient(clientId: String): Long {
        val now = System.currentTimeMillis() / 1000
        val queue = queues[clientId] ?: return 0L
        return synchronized(queue) {
            queue.messages.values.count { it.vt <= now }.toLong()
        }
    }

    private fun isExpired(msg: BrokerMessage, currentTimeMillis: Long): Boolean {
        val expiry = msg.messageExpiryInterval
        return if (expiry != null && expiry >= 0) {
            val ageSeconds = (currentTimeMillis - msg.time.toEpochMilli()) / 1000
            ageSeconds >= expiry
        } else {
            false
        }
    }
}
