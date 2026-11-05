package at.rocworks.stores

import at.rocworks.data.BrokerMessage

enum class QueueStoreType {
    MEMORY,
    POSTGRES,
    MONGODB,
    SQLITE
}

/**
 * Outbound QoS 1/2 delivery state of a queued message: the packet id it was sent with and the stage
 * (1 = PUBLISH sent, 2 = PUBREC received, PUBREL pending). Kept across reconnects so the message is
 * re-sent with DUP=1 and the same packet id, or the PUBREL is re-sent ([MQTT-4.4.0-1], [MQTT-3.3.1-1]).
 */
data class DeliveryState(val clientId: String, val messageUuid: String, val packetId: Int, val stage: Int)

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
    // Must keep the delivery state (packet id, stage)
    fun resetInFlightMessages(clientId: String)
    fun updateDeliveryStates(states: List<DeliveryState>)
    fun purgeDeliveredMessages(): Int
    fun purgeExpiredMessages(): Int

    fun purgeQueuedMessages()
    fun deleteClientMessages(clientId: String)

    fun countQueuedMessages(): Long
    fun countQueuedMessagesForClient(clientId: String): Long
}
