package at.rocworks.stores

import at.rocworks.data.MqttMessage
import at.rocworks.data.PurgeResult
import java.time.Instant

object MessageStoreNone : IMessageStore {
    override fun getName(): String = "NONE"
    override fun getType(): MessageStoreType = MessageStoreType.NONE
    
    override fun get(topicName: String): MqttMessage? = null
    override fun getAsync(topicName: String, callback: (MqttMessage?) -> Unit) = callback(null)
    
    override fun addAll(messages: List<MqttMessage>) {}
    
    override fun delAll(topics: List<String>) {}
    
    override fun findMatchingMessages(topicName: String, callback: (MqttMessage) -> Boolean) {}
    
    override fun purgeOldMessages(olderThan: Instant): PurgeResult = PurgeResult(0, 0)

    override fun dropStorage(): Boolean = true

    override fun getConnectionStatus(): Boolean = false // NONE store reports as not connected
}