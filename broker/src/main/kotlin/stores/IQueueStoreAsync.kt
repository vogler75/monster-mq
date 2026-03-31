package at.rocworks.stores

import at.rocworks.data.BrokerMessage
import io.vertx.core.Future

interface IQueueStoreAsync {
    fun getType(): QueueStoreType

    val sync: IQueueStoreSync

    fun enqueueMessages(messages: List<Pair<BrokerMessage, List<String>>>): Future<Void>
    fun dequeueMessages(clientId: String, callback: (BrokerMessage) -> Boolean): Future<Void>
    fun removeMessages(messages: List<Pair<String, String>>): Future<Void>

    fun fetchNextPendingMessage(clientId: String): Future<BrokerMessage?>
    fun fetchPendingMessages(clientId: String, limit: Int): Future<List<BrokerMessage>>
    fun fetchAndLockPendingMessages(clientId: String, limit: Int): Future<List<BrokerMessage>>

    fun markMessageInFlight(clientId: String, messageUuid: String): Future<Void>
    fun markMessagesInFlight(clientId: String, messageUuids: List<String>): Future<Void>
    fun markMessageDelivered(clientId: String, messageUuid: String): Future<Void>
    fun resetInFlightMessages(clientId: String): Future<Void>
    fun purgeDeliveredMessages(): Future<Int>
    fun purgeExpiredMessages(): Future<Int>

    fun purgeQueuedMessages(): Future<Void>
    fun deleteClientMessages(clientId: String): Future<Void>

    fun countQueuedMessages(): Future<Long>
    fun countQueuedMessagesForClient(clientId: String): Future<Long>
}
