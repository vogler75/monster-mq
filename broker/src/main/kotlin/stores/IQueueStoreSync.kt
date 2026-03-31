package at.rocworks.stores

import at.rocworks.data.BrokerMessage

enum class QueueStoreType {
    POSTGRES,
    POSTGRES_V2,
    MONGODB,
    SQLITE
}

interface IQueueStoreSync {
    fun getType(): QueueStoreType

    fun enqueueMessages(messages: List<Pair<BrokerMessage, List<String>>>)
    fun dequeueMessages(clientId: String, callback: (BrokerMessage) -> Boolean)
    fun removeMessages(messages: List<Pair<String, String>>)

    // Fetch ONE pending message for queue-first delivery (returns null if none)
    fun fetchNextPendingMessage(clientId: String): BrokerMessage?

    // Fetch multiple pending messages for bulk delivery (returns empty list if none)
    fun fetchPendingMessages(clientId: String, limit: Int): List<BrokerMessage>

    // Atomically fetch pending messages and mark them in-flight (returns empty list if none)
    // Default implementation falls back to separate fetch + mark for backends that don't support atomic CTE
    fun fetchAndLockPendingMessages(clientId: String, limit: Int): List<BrokerMessage> {
        val messages = fetchPendingMessages(clientId, limit)
        if (messages.isNotEmpty()) {
            markMessagesInFlight(clientId, messages.map { it.messageUuid })
        }
        return messages
    }

    // Status-based message tracking for QoS 1+ delivery
    fun markMessageInFlight(clientId: String, messageUuid: String)
    fun markMessagesInFlight(clientId: String, messageUuids: List<String>)
    fun markMessageDelivered(clientId: String, messageUuid: String)
    fun resetInFlightMessages(clientId: String)
    fun purgeDeliveredMessages(): Int
    fun purgeExpiredMessages(): Int

    fun purgeQueuedMessages()
    fun deleteClientMessages(clientId: String)

    fun countQueuedMessages(): Long
    fun countQueuedMessagesForClient(clientId: String): Long
}
