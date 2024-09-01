package at.rocworks.stores

import at.rocworks.data.MqttMessage
import at.rocworks.data.TopicTree
import com.hazelcast.core.HazelcastInstance
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Callable
import java.util.logging.Logger

class MessageStoreHazelcast(
    private val name: String,
    private val hazelcast: HazelcastInstance
): AbstractVerticle(), IMessageStore {
    private val logger = Logger.getLogger(this.javaClass.simpleName+"/"+name)

    private val index = TopicTree()
    private val store = hazelcast.getMap<String, MqttMessage>(name)

    private val addAddress = "$name/A"
    private val delAddress = "$name/D"

    override fun start(startPromise: Promise<Void>) {
        vertx.executeBlocking(Callable {
            vertx.eventBus().consumer<JsonArray>(addAddress) {
                index.addAll(it.body().map { it.toString() })
            }
            vertx.eventBus().consumer<JsonArray>(delAddress) {
                index.delAll(it.body().map { it.toString() })
            }
            logger.info("Indexing [$name] message store...")
            store.keys.forEach { index.add(it) }
            logger.info("Indexing [$name] message store finished.")
            startPromise.complete()
        })
    }

    override fun get(topic: String): MqttMessage? = store[topic]

    override fun addAll(messages: List<MqttMessage>) {
        val startTime = Instant.now()

        val topics = messages.map { it.topicName }.distinct()
        vertx.eventBus().publish(addAddress, JsonArray(topics))
        store.putAllAsync(messages.associateBy { it.topicName })

        val duration = Duration.between(startTime, Instant.now()).toMillis()
        logger.finest { "Write block of size [${messages.size}] to map took [$duration]" }
    }

    override fun delAll(messages: List<MqttMessage>) {
        val topics = messages.map { it.topicName }.distinct()
        vertx.eventBus().publish(delAddress, JsonArray(topics))
        topics.forEach { store.remove(it) } // there is no delAll
    }

    override fun findMatchingTopicNames(topicName: String, callback: (String) -> Boolean) {
        index.findMatchingTopicNames(topicName, callback)
    }
}