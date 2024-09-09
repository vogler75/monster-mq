package at.rocworks.stores

import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttSubscription
import at.rocworks.data.TopicTree

enum class SessionStoreType {
    MEMORY,
    POSTGRES
}

interface ISessionStore {
    fun getType(): SessionStoreType
    fun buildIndex(index: TopicTree)
    fun putClient(clientId: String, cleanSession: Boolean, connected: Boolean)
    fun setConnected(clientId: String, connected: Boolean)
    fun isConnected(clientId: String): Boolean
    fun setLastWill(clientId: String, topic: String, message: MqttMessage)
    fun addSubscriptions(subscriptions: List<MqttSubscription>)
    fun delSubscriptions(subscriptions: List<MqttSubscription>)
    fun delClient(clientId: String, callback: (MqttSubscription)->Unit)
    fun enqueueMessages(clientIds: List<String>, messages: List<MqttMessage>)
    fun dequeueMessages(clientId: String, callback: (MqttMessage)->Unit)
}