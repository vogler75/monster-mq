package at.rocworks.stores

import at.rocworks.data.BrokerMessage
import at.rocworks.data.MqttSubscription
import io.vertx.core.json.JsonObject

enum class SessionStoreType {
    POSTGRES,
    CRATEDB,
    MONGODB,
    SQLITE
}

interface ISessionStoreSync {
    fun getType(): SessionStoreType

    fun iterateOfflineClients(callback: (clientId: String) -> Unit)
    fun iterateConnectedClients(callback: (clientId: String, nodeId: String) -> Unit)
    fun iterateAllSessions(callback: (clientId: String, nodeId: String, connected: Boolean, cleanSession: Boolean) -> Unit)
    fun iterateNodeClients(nodeId: String, callback: (clientId: String, cleanSession: Boolean, lastWill: BrokerMessage) -> Unit)
    fun iterateSubscriptions(callback: (topic: String, clientId: String, qos: Int, noLocal: Boolean, retainHandling: Int, retainAsPublished: Boolean) -> Unit)

    fun setClient(clientId: String, nodeId: String, cleanSession: Boolean, connected: Boolean, information: JsonObject)
    fun setLastWill(clientId: String, message: BrokerMessage?)

    fun setConnected(clientId: String, connected: Boolean)
    fun isConnected(clientId: String): Boolean
    fun isPresent(clientId: String): Boolean

    fun addSubscriptions(subscriptions: List<MqttSubscription>)
    fun delSubscriptions(subscriptions: List<MqttSubscription>)
    fun delClient(clientId: String, callback: (MqttSubscription) -> Unit)

    fun enqueueMessages(messages: List<Pair<BrokerMessage, List<String>>>)
    fun dequeueMessages(clientId: String, callback: (BrokerMessage) -> Boolean)
    fun removeMessages(messages: List<Pair<String, String>>)

    // Fetch ONE pending message for queue-first delivery (returns null if none)
    fun fetchNextPendingMessage(clientId: String): BrokerMessage?

    // Fetch multiple pending messages for bulk delivery (returns empty list if none)
    fun fetchPendingMessages(clientId: String, limit: Int): List<BrokerMessage>

    // Status-based message tracking for QoS 1+ delivery
    fun markMessageInFlight(clientId: String, messageUuid: String)
    fun markMessagesInFlight(clientId: String, messageUuids: List<String>)
    fun markMessageDelivered(clientId: String, messageUuid: String)
    fun resetInFlightMessages(clientId: String)
    fun purgeDeliveredMessages(): Int
    fun purgeExpiredMessages(): Int

    fun purgeQueuedMessages()
    fun purgeSessions()

    fun countQueuedMessages(): Long
    fun countQueuedMessagesForClient(clientId: String): Long
}