package at.rocworks.stores

import at.rocworks.data.MqttSubscription

enum class SubscriptionTableType {
    ASYNCMAP,
    POSTGRES
}

interface ISubscriptionTable {
    fun getType(): SubscriptionTableType
    fun addSubscription(subscription: MqttSubscription)
    fun removeSubscription(subscription: MqttSubscription)
    fun removeClient(clientId: String)
    fun findClients(topicName: String): Set<String>
}