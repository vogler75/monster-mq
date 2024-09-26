package at.rocworks.stores

import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttSubscription
import io.vertx.core.Future
import io.vertx.core.json.JsonObject

interface ISessionStoreAsync {
    fun getType(): SessionStoreType

    val sync: ISessionStore

    fun iterateOfflineClients(callback: (clientId: String) -> Unit): Future<Void>
    fun iterateNodeClients(nodeId: String, callback: (clientId: String, cleanSession: Boolean, lastWill: MqttMessage) -> Unit): Future<Void>
    fun iterateSubscriptions(callback: (topic: String, clientId: String, qos: Int) -> Unit): Future<Void>

    fun setClient(clientId: String, nodeId: String, cleanSession: Boolean, connected: Boolean, information: JsonObject): Future<Void>
    fun setLastWill(clientId: String, message: MqttMessage?): Future<Void>

    fun setConnected(clientId: String, connected: Boolean): Future<Void>
    fun isConnected(clientId: String): Future<Boolean>
    fun isPresent(clientId: String): Future<Boolean>

    fun addSubscriptions(subscriptions: List<MqttSubscription>): Future<Void>
    fun delSubscriptions(subscriptions: List<MqttSubscription>): Future<Void>
    fun delClient(clientId: String, callback: (MqttSubscription) -> Unit): Future<Void>

    fun enqueueMessages(messages: List<Pair<MqttMessage, List<String>>>): Future<Void>
    fun dequeueMessages(clientId: String, callback: (MqttMessage) -> Boolean): Future<Void>
    fun removeMessages(messages: List<Pair<String, String>>): Future<Void>

    fun purgeQueuedMessages(): Future<Void>
    fun purgeSessions(): Future<Void>

}
