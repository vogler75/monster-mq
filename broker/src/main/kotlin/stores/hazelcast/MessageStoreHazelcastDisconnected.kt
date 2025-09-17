package at.rocworks.stores

import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import at.rocworks.data.PurgeResult
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import java.time.Instant

/**
 * A disconnected Hazelcast store that represents the state when clustering is disabled
 * but HAZELCAST store type is requested. This store reports as not connected and
 * provides no-op implementations for all operations.
 */
class MessageStoreHazelcastDisconnected(
    private val name: String
): AbstractVerticle(), IMessageStore {
    private val logger = Utils.getLogger(this::class.java, name)

    override fun getName(): String = name
    override fun getType(): MessageStoreType = MessageStoreType.HAZELCAST

    override fun start(startPromise: Promise<Void>) {
        logger.warning("Hazelcast MessageStore [$name] not connected - clustering is disabled. Use -cluster flag to enable Hazelcast.")
        startPromise.complete()
    }

    override fun get(topicName: String): MqttMessage? = null

    override fun getAsync(topicName: String, callback: (MqttMessage?) -> Unit) {
        callback(null)
    }

    override fun addAll(messages: List<MqttMessage>) {
        logger.fine("Ignoring addAll operation - Hazelcast store [$name] not connected")
    }

    override fun delAll(topics: List<String>) {
        logger.fine("Ignoring delAll operation - Hazelcast store [$name] not connected")
    }

    override fun findMatchingMessages(topicName: String, callback: (MqttMessage) -> Boolean) {
        // No messages to find when disconnected
    }

    override fun purgeOldMessages(olderThan: Instant): PurgeResult {
        logger.fine("Ignoring purge operation - Hazelcast store [$name] not connected")
        return PurgeResult(0, 0)
    }

    override fun dropStorage(): Boolean {
        logger.fine("Ignoring dropStorage operation - Hazelcast store [$name] not connected")
        return true
    }

    override fun getConnectionStatus(): Boolean {
        return false // Always return false - not connected
    }
}