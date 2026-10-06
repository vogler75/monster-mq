package at.rocworks.peerlink

import at.rocworks.bus.IMessageBus
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.IMessageStore
import at.rocworks.stores.MessageStoreType
import at.rocworks.data.PurgeResult
import io.vertx.core.Future
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

class TestMessageStore : IMessageStore {
    val store = ConcurrentHashMap<String, BrokerMessage>()
    override fun getName(): String = "test"
    override fun getType(): MessageStoreType = MessageStoreType.MEMORY
    override fun get(topicName: String): BrokerMessage? = store[topicName]
    override fun getAsync(topicName: String, callback: (BrokerMessage?) -> Unit) = callback(store[topicName])
    override fun addAll(messages: List<BrokerMessage>) { messages.forEach { store[it.topicName] = it } }
    override fun delAll(topics: List<String>) { topics.forEach { store.remove(it) } }
    override fun findMatchingMessages(topicName: String, callback: (BrokerMessage) -> Boolean) {}
    override fun findMatchingTopics(topicPattern: String, callback: (String) -> Boolean) {}
    override fun purgeOldMessages(olderThan: Instant): PurgeResult = PurgeResult(0, 0)
    override fun dropStorage(): Boolean = true
    override fun getConnectionStatus(): Boolean = true
    override suspend fun tableExists(): Boolean = true
    override suspend fun createTable(): Boolean = true
}

class TestMessageBus : IMessageBus {
    override fun subscribeToMessageBus(callback: (BrokerMessage) -> Unit): Future<Void> = Future.succeededFuture()
    override fun publishMessageToBus(message: BrokerMessage) {}
    override val isExternalTransport: Boolean = false
    override fun rememberMessageUuid(messageUuid: String): Boolean = true
}
