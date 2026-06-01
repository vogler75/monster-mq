package at.rocworks.stores

import at.rocworks.data.BrokerMessage
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.WorkerExecutor
import java.util.concurrent.Callable

class QueueStoreAsync(private val store: IQueueStoreSync): AbstractVerticle(), IQueueStoreAsync {
    private lateinit var dbExecutor: WorkerExecutor

    override fun getType(): QueueStoreType = store.getType()

    override val sync: IQueueStoreSync get() = store

    override fun start(startPromise: Promise<Void>) {
        dbExecutor = vertx.createSharedWorkerExecutor("QueueStoreExecutor", 1)
        startPromise.complete()
    }

    override fun stop(stopPromise: Promise<Void>) {
        if (::dbExecutor.isInitialized) {
            dbExecutor.close()
        }
        stopPromise.complete()
    }

    override fun enqueueMessages(messages: List<Pair<BrokerMessage, List<String>>>): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.enqueueMessages(messages)
            null
        })
    }

    override fun dequeueMessages(clientId: String, callback: (BrokerMessage) -> Boolean): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.dequeueMessages(clientId, callback)
            null
        })
    }

    override fun removeMessages(messages: List<Pair<String, String>>): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.removeMessages(messages)
            null
        })
    }

    override fun fetchNextPendingMessage(clientId: String): Future<BrokerMessage?> {
        return dbExecutor.executeBlocking(Callable {
            store.fetchNextPendingMessage(clientId)
        })
    }

    override fun fetchPendingMessages(clientId: String, limit: Int): Future<List<BrokerMessage>> {
        return dbExecutor.executeBlocking(Callable {
            store.fetchPendingMessages(clientId, limit)
        })
    }

    override fun fetchAndLockPendingMessages(clientId: String, limit: Int): Future<List<BrokerMessage>> {
        return dbExecutor.executeBlocking(Callable {
            store.fetchAndLockPendingMessages(clientId, limit)
        })
    }

    override fun markMessageInFlight(clientId: String, messageUuid: String): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.markMessageInFlight(clientId, messageUuid)
            null
        })
    }

    override fun markMessagesInFlight(clientId: String, messageUuids: List<String>): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.markMessagesInFlight(clientId, messageUuids)
            null
        })
    }

    override fun markMessageDelivered(clientId: String, messageUuid: String): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.markMessageDelivered(clientId, messageUuid)
            null
        })
    }

    override fun resetInFlightMessages(clientId: String): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.resetInFlightMessages(clientId)
            null
        })
    }

    override fun purgeDeliveredMessages(): Future<Int> {
        return dbExecutor.executeBlocking(Callable {
            store.purgeDeliveredMessages()
        })
    }

    override fun purgeExpiredMessages(): Future<Int> {
        return dbExecutor.executeBlocking(Callable {
            store.purgeExpiredMessages()
        })
    }

    override fun purgeQueuedMessages(): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.purgeQueuedMessages()
            null
        })
    }

    override fun deleteClientMessages(clientId: String): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.deleteClientMessages(clientId)
            null
        })
    }

    override fun countQueuedMessages(): Future<Long> {
        return dbExecutor.executeBlocking(Callable {
            store.countQueuedMessages()
        })
    }

    override fun countQueuedMessagesForClient(clientId: String): Future<Long> {
        return dbExecutor.executeBlocking(Callable {
            store.countQueuedMessagesForClient(clientId)
        })
    }
}
