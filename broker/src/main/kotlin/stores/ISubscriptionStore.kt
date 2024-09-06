package at.rocworks.stores

import at.rocworks.data.MqttSubscription
import at.rocworks.data.TopicTree

enum class SubscriptionStoreType {
    MEMORY,
    POSTGRES
}

interface ISubscriptionStore {
    fun getType(): SubscriptionStoreType
    fun populateIndex(index: TopicTree)
    fun addAll(subscriptions: List<MqttSubscription>)
    fun delAll(subscriptions: List<MqttSubscription>)
    fun delClient(clientId: String, callback: (MqttSubscription)->Unit)
}