package at.rocworks.stores

import at.rocworks.data.BrokerMessage
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import java.util.concurrent.Callable

class QueueStoreAsync(private val store: IQueueStoreSync): AbstractVerticle(), IQueueStoreAsync {

    override fun getType(): QueueStoreType = store.getType()

    override val sync: IQueueStoreSync get() = store

    override fun enqueueMessages(messages: List<Pair<BrokerMessage, List<String>>>): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.enqueueMessages(messages)
            promise.complete()
        })
        return promise.future()
    }

    override fun dequeueMessages(clientId: String, callback: (BrokerMessage) -> Boolean): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.dequeueMessages(clientId, callback)
            promise.complete()
        })
        return promise.future()
    }

    override fun removeMessages(messages: List<Pair<String, String>>): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.removeMessages(messages)
            promise.complete()
        })
        return promise.future()
    }

    override fun fetchNextPendingMessage(clientId: String): Future<BrokerMessage?> {
        val promise = Promise.promise<BrokerMessage?>()
        vertx.executeBlocking(Callable {
            promise.complete(store.fetchNextPendingMessage(clientId))
        })
        return promise.future()
    }

    override fun fetchPendingMessages(clientId: String, limit: Int): Future<List<BrokerMessage>> {
        val promise = Promise.promise<List<BrokerMessage>>()
        vertx.executeBlocking(Callable {
            promise.complete(store.fetchPendingMessages(clientId, limit))
        })
        return promise.future()
    }

    override fun fetchAndLockPendingMessages(clientId: String, limit: Int): Future<List<BrokerMessage>> {
        val promise = Promise.promise<List<BrokerMessage>>()
        vertx.executeBlocking(Callable {
            promise.complete(store.fetchAndLockPendingMessages(clientId, limit))
        })
        return promise.future()
    }

    override fun markMessageInFlight(clientId: String, messageUuid: String): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.markMessageInFlight(clientId, messageUuid)
            promise.complete()
        })
        return promise.future()
    }

    override fun markMessagesInFlight(clientId: String, messageUuids: List<String>): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.markMessagesInFlight(clientId, messageUuids)
            promise.complete()
        })
        return promise.future()
    }

    override fun markMessageDelivered(clientId: String, messageUuid: String): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.markMessageDelivered(clientId, messageUuid)
            promise.complete()
        })
        return promise.future()
    }

    override fun resetInFlightMessages(clientId: String): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.resetInFlightMessages(clientId)
            promise.complete()
        })
        return promise.future()
    }

    override fun purgeDeliveredMessages(): Future<Int> {
        val promise = Promise.promise<Int>()
        vertx.executeBlocking(Callable {
            promise.complete(store.purgeDeliveredMessages())
        })
        return promise.future()
    }

    override fun purgeExpiredMessages(): Future<Int> {
        val promise = Promise.promise<Int>()
        vertx.executeBlocking(Callable {
            promise.complete(store.purgeExpiredMessages())
        })
        return promise.future()
    }

    override fun purgeQueuedMessages(): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.purgeQueuedMessages()
            promise.complete()
        })
        return promise.future()
    }

    override fun deleteClientMessages(clientId: String): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.deleteClientMessages(clientId)
            promise.complete()
        })
        return promise.future()
    }

    override fun countQueuedMessages(): Future<Long> {
        val promise = Promise.promise<Long>()
        vertx.executeBlocking(Callable {
            promise.complete(store.countQueuedMessages())
        })
        return promise.future()
    }

    override fun countQueuedMessagesForClient(clientId: String): Future<Long> {
        val promise = Promise.promise<Long>()
        vertx.executeBlocking(Callable {
            promise.complete(store.countQueuedMessagesForClient(clientId))
        })
        return promise.future()
    }
}
