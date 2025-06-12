package at.rocworks.stores

import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.data.TopicTree
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import java.util.concurrent.Callable

class MessageStoreMemory(private val name: String): AbstractVerticle(), IMessageStore {
    private val logger = Utils.getLogger(this::class.java, name)

    private val index = TopicTree<Void, Void>()
    private val store = getStore()

    private val addAddress = "$name/A"
    private val delAddress = "$name/D"

    override fun getName(): String = name
    override fun getType(): MessageStoreType = MessageStoreType.MEMORY

    override fun start(startPromise: Promise<Void>) {
        vertx.executeBlocking(Callable {
            vertx.eventBus().consumer<JsonArray>(addAddress) {
                index.addAll(it.body().map { it.toString() })
            }
            vertx.eventBus().consumer<JsonArray>(delAddress) {
                index.delAll(it.body().map { it.toString() })
            }
            logger.info("Indexing [$name] message store [${Utils.getCurrentFunctionName()}]")
            store.keys.forEach { index.add(it) }
            logger.info("Indexing [$name] message store finished [${Utils.getCurrentFunctionName()}]")
            startPromise.complete()
        })
    }

    private fun getStore(): MutableMap<String, MqttMessage> = mutableMapOf<String, MqttMessage>()

    override fun get(topicName: String): MqttMessage? = store[topicName]

    override fun addAll(messages: List<MqttMessage>) {
        val topics = messages.map { it.topicName }.distinct()
        vertx.eventBus().publish(addAddress, JsonArray(topics))
        store.putAll(messages.map { Pair(it.topicName, it) })
    }

    override fun delAll(topics: List<String>) {
        vertx.eventBus().publish(delAddress, JsonArray(topics))
        topics.forEach { store.remove(it) } // there is no delAll
    }

    override fun findMatchingMessages(topicName: String, callback: (MqttMessage) -> Boolean) {
        index.findMatchingTopicNames(topicName) { foundTopicName ->
            val message = store[foundTopicName]
            if (message != null) callback(message)
            else true
        }
    }

    override fun findTopicsByName(name: String, ignoreCase: Boolean, namespace: String): List<String> {
        TODO("Not yet implemented")
    }

    override fun findTopicsByConfig(config: String, description: String, ignoreCase: Boolean, namespace: String): List<TopicAndConfig> {
        TODO("Not yet implemented")
    }
}