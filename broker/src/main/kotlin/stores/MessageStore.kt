package at.rocworks.stores

import at.rocworks.data.MqttMessage
import at.rocworks.data.TopicTree
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import java.util.concurrent.Callable
import java.util.logging.Logger

class MessageStore(private val name: String): AbstractVerticle(), IMessageStore {
    private val logger = Logger.getLogger(this.javaClass.simpleName+"/"+name)

    private val index = TopicTree()
    private val store = getStore()

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

    fun getStore(): MutableMap<String, MqttMessage> = mutableMapOf<String, MqttMessage>()

    override fun get(topic: String): MqttMessage? = store[topic]

    override fun addAll(messages: List<MqttMessage>) {
        val topics = messages.map { it.topicName }.distinct()
        vertx.eventBus().publish(addAddress, JsonArray(topics))
        store.putAll(messages.map { Pair(it.topicName, it) })
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