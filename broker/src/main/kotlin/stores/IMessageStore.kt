package at.rocworks.stores

import at.rocworks.data.MqttMessage
import io.vertx.core.AbstractVerticle

interface IMessageStore {
    operator fun get(topic: String): MqttMessage?

    fun addAll(messages: List<MqttMessage>)
    fun delAll(messages: List<MqttMessage>)

    fun findMatchingTopicNames(topicName: String, callback: (String)->Boolean)
}