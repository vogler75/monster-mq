package at.rocworks.stores.memory

import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.IQueueStoreSync
import at.rocworks.stores.QueueStoreType
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * High-performance, ultra-reliable in-memory queue store implementing visibility timeouts (vt).
 * In-memory queue store with zero database lock contention.
 */
class QueueStoreMemory(
    private val visibilityTimeoutSeconds: Int = 30
) : AbstractVerticle(), IQueueStoreSync {

    private val logger = Utils.getLogger(this::class.java)
    private val queues = ConcurrentHashMap<String, CopyOnWriteArrayList<MemoryQueuedMessage>>()

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
                val queue = queues.computeIfAbsent(clientId) { CopyOnWriteArrayList() }
                queue.add(MemoryQueuedMessage(queuedMessage, now))
            }
        }
        logger.fine { "Enqueued ${messages.size} messages in-memory for ${messages.sumOf { it.second.size }} clients" }
    }

    override fun dequeueMessages(clientId: String, callback: (BrokerMessage) -> Boolean) {
        val queue = queues[clientId] ?: return
        val now = System.currentTimeMillis() / 1000
        val currentTimeMillis = System.currentTimeMillis()
        for (qm in queue) {
            if (qm.vt <= now && !isExpired(qm.message, currentTimeMillis)) {
                val proceed = callback(qm.message)
                if (!proceed) break
            }
        }
    }

    override fun removeMessages(messages: List<Pair<String, String>>) {
        if (messages.isEmpty()) return
        messages.forEach { (clientId, messageUuid) ->
            queues[clientId]?.removeIf { it.message.messageUuid == messageUuid }
        }
    }

    override fun fetchNextPendingMessage(clientId: String): BrokerMessage? {
        return fetchPendingMessages(clientId, 1).firstOrNull()
    }

    override fun fetchPendingMessages(clientId: String, limit: Int): List<BrokerMessage> {
        val queue = queues[clientId] ?: return emptyList()
        val now = System.currentTimeMillis() / 1000
        val currentTimeMillis = System.currentTimeMillis()
        val result = mutableListOf<BrokerMessage>()
        for (qm in queue) {
            if (qm.vt <= now && !isExpired(qm.message, currentTimeMillis)) {
                result.add(qm.message)
                if (result.size >= limit) break
            }
        }
        return result
    }

    override fun fetchAndLockPendingMessages(clientId: String, limit: Int): List<BrokerMessage> {
        val queue = queues[clientId] ?: return emptyList()
        val now = System.currentTimeMillis() / 1000
        val currentTimeMillis = System.currentTimeMillis()
        val vtFuture = now + visibilityTimeoutSeconds
        val result = mutableListOf<BrokerMessage>()
        for (qm in queue) {
            if (qm.vt <= now && !isExpired(qm.message, currentTimeMillis)) {
                qm.vt = vtFuture
                qm.readCt++
                result.add(qm.message)
                if (result.size >= limit) break
            }
        }
        return result
    }

    override fun markMessageInFlight(clientId: String, messageUuid: String) {
        val queue = queues[clientId] ?: return
        val now = System.currentTimeMillis() / 1000
        val vtFuture = now + visibilityTimeoutSeconds
        val qm = queue.find { it.message.messageUuid == messageUuid }
        if (qm != null && qm.vt <= now) {
            qm.vt = vtFuture
            qm.readCt++
        }
    }

    override fun markMessagesInFlight(clientId: String, messageUuids: List<String>) {
        if (messageUuids.isEmpty()) return
        val queue = queues[clientId] ?: return
        val now = System.currentTimeMillis() / 1000
        val vtFuture = now + visibilityTimeoutSeconds
        val uuidSet = messageUuids.toSet()
        queue.forEach { qm ->
            if (uuidSet.contains(qm.message.messageUuid) && qm.vt <= now) {
                qm.vt = vtFuture
                qm.readCt++
            }
        }
    }

    override fun markMessageDelivered(clientId: String, messageUuid: String) {
        queues[clientId]?.removeIf { it.message.messageUuid == messageUuid }
    }

    override fun resetInFlightMessages(clientId: String) {
        val queue = queues[clientId] ?: return
        val now = System.currentTimeMillis() / 1000
        queue.forEach { qm ->
            if (qm.vt > now) {
                qm.vt = now
                qm.readCt = 0
            }
        }
    }

    override fun purgeDeliveredMessages(): Int = 0

    override fun purgeExpiredMessages(): Int {
        val currentTimeMillis = System.currentTimeMillis()
        var totalRemoved = 0
        queues.values.forEach { queue ->
            val sizeBefore = queue.size
            queue.removeIf { isExpired(it.message, currentTimeMillis) }
            totalRemoved += (sizeBefore - queue.size)
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
        return queues.values.sumOf { it.size }.toLong()
    }

    override fun countQueuedMessagesForClient(clientId: String): Long {
        val now = System.currentTimeMillis() / 1000
        return queues[clientId]?.count { it.vt <= now }?.toLong() ?: 0L
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
