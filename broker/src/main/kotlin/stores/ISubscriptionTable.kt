package at.rocworks.stores

import at.rocworks.data.MqttSubscription

interface ISubscriptionTable {
    fun addSubscription(subscription: MqttSubscription)
    fun removeSubscription(subscription: MqttSubscription)
    fun removeClient(clientId: String)
    fun findClients(topicName: String): Set<String>
}