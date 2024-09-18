package at.rocworks.stores

import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttSubscription
import io.vertx.core.Future
import io.vertx.core.json.JsonObject

enum class SessionStoreType {
    POSTGRES,
    CRATEDB
}

interface ISessionStore {
    fun getType(): SessionStoreType
    fun iterateOfflineClients(callback: (clientId: String)->Unit)
    fun iterateSubscriptions(callback: (topic: String, clientId: String, qos: Int)->Unit)
    fun setClient(clientId: String, cleanSession: Boolean, connected: Boolean, information: JsonObject)
    fun setConnected(clientId: String, connected: Boolean)
    fun isConnected(clientId: String): Boolean
    fun isPresent(clientId: String): Boolean
    fun setLastWill(clientId: String, message: MqttMessage?)
    fun addSubscriptions(subscriptions: List<MqttSubscription>)
    fun delSubscriptions(subscriptions: List<MqttSubscription>)
    fun delClient(clientId: String, callback: (MqttSubscription)->Unit)
    fun enqueueMessages(messages: List<Pair<MqttMessage, List<String>>>)
    fun dequeueMessages(clientId: String, callback: (MqttMessage)->Unit)
    fun removeMessages(messages: List<Pair<String, String>>)
}