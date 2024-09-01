package at.rocworks.stores

import at.rocworks.data.MqttMessage
import com.hazelcast.core.HazelcastInstance
import io.vertx.core.AbstractVerticle

class MessageStorePostgres(
    private val name: String,
    private val hazelcast: HazelcastInstance
): AbstractVerticle(), IMessageStore {
    override fun get(topic: String): MqttMessage? {
        TODO("Not yet implemented")
    }

    override fun addAll(messages: List<MqttMessage>) {
        TODO("Not yet implemented")
    }

    override fun delAll(messages: List<MqttMessage>) {
        TODO("Not yet implemented")
    }

    override fun findMatchingTopicNames(topicName: String, callback: (String) -> Boolean) {
        TODO("Not yet implemented")
    }


}