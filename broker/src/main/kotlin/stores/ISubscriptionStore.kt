package at.rocworks.stores

import at.rocworks.data.MqttSubscription
import at.rocworks.data.TopicTree

enum class SubscriptionTableType {
    MEMORY,
    POSTGRES
}

interface ISubscriptionTable {
    fun getType(): SubscriptionTableType
    fun populateIndex(index: TopicTree)
    fun addSubscriptions(subscriptions: List<MqttSubscription>)
    fun removeSubscriptions(subscriptions: List<MqttSubscription>)
    fun removeClient(clientId: String, callback: (MqttSubscription)->Unit)
}