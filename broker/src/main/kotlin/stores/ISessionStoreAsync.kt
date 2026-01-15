package at.rocworks.stores

import at.rocworks.data.BrokerMessage
import at.rocworks.data.MqttSubscription
import io.vertx.core.Future
import io.vertx.core.json.JsonObject

interface ISessionStoreAsync {
    fun getType(): SessionStoreType

    val sync: ISessionStoreSync

    fun iterateOfflineClients(callback: (clientId: String) -> Unit): Future<Void>
    fun iterateConnectedClients(callback: (clientId: String, nodeId: String) -> Unit): Future<Void>
    fun iterateAllSessions(callback: (clientId: String, nodeId: String, connected: Boolean, cleanSession: Boolean) -> Unit): Future<Void>
    fun iterateNodeClients(nodeId: String, callback: (clientId: String, cleanSession: Boolean, lastWill: BrokerMessage) -> Unit): Future<Void>
    fun iterateSubscriptions(callback: (topic: String, clientId: String, qos: Int) -> Unit): Future<Void>

    fun setClient(clientId: String, nodeId: String, cleanSession: Boolean, connected: Boolean, information: JsonObject): Future<Void>
    fun setLastWill(clientId: String, message: BrokerMessage?): Future<Void>

    fun setConnected(clientId: String, connected: Boolean): Future<Void>
    fun isConnected(clientId: String): Future<Boolean>
    fun isPresent(clientId: String): Future<Boolean>

    fun addSubscriptions(subscriptions: List<MqttSubscription>): Future<Void>
    fun delSubscriptions(subscriptions: List<MqttSubscription>): Future<Void>
    fun delClient(clientId: String, callback: (MqttSubscription) -> Unit): Future<Void>

    fun enqueueMessages(messages: List<Pair<BrokerMessage, List<String>>>): Future<Void>
    fun dequeueMessages(clientId: String, callback: (BrokerMessage) -> Boolean): Future<Void>
    fun removeMessages(messages: List<Pair<String, String>>): Future<Void>

    // Fetch ONE pending message for queue-first delivery (returns null if none)
    fun fetchNextPendingMessage(clientId: String): Future<BrokerMessage?>

    // Status-based message tracking for QoS 1+ delivery
    fun markMessageInFlight(clientId: String, messageUuid: String): Future<Void>
    fun markMessagesInFlight(clientId: String, messageUuids: List<String>): Future<Void>
    fun markMessageDelivered(clientId: String, messageUuid: String): Future<Void>
    fun resetInFlightMessages(clientId: String): Future<Void>
    fun purgeDeliveredMessages(): Future<Int>

    fun purgeQueuedMessages(): Future<Void>
    fun purgeSessions(): Future<Void>

    fun countQueuedMessages(): Future<Long>
    fun countQueuedMessagesForClient(clientId: String): Future<Long>

}
