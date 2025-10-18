package at.rocworks.stores

import at.rocworks.data.BrokerMessage
import at.rocworks.data.PurgeResult
import java.time.Instant

object MessageStoreNone : IMessageStore {
    override fun getName(): String = "NONE"
    override fun getType(): MessageStoreType = MessageStoreType.NONE

    override fun get(topicName: String): BrokerMessage? = null
    override fun getAsync(topicName: String, callback: (BrokerMessage?) -> Unit) = callback(null)

    override fun addAll(messages: List<BrokerMessage>) {}

    override fun delAll(topics: List<String>) {}

    override fun findMatchingMessages(topicName: String, callback: (BrokerMessage) -> Boolean) {}

    override fun findMatchingTopics(topicPattern: String, callback: (String) -> Boolean) {}

    override fun purgeOldMessages(olderThan: Instant): PurgeResult = PurgeResult(0, 0)

    override fun dropStorage(): Boolean = true

    override fun getConnectionStatus(): Boolean = false // NONE store reports as not connected

    override suspend fun tableExists(): Boolean = true // NONE store has no table requirements

    override suspend fun createTable(): Boolean {
        // NONE stores don't require table creation
        return true
    }
}