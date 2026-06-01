package at.rocworks.stores

import at.rocworks.data.BrokerMessage
import at.rocworks.data.MqttSubscription
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.WorkerExecutor
import io.vertx.core.json.JsonObject
import java.util.concurrent.Callable

class SessionStoreAsync(private val store: ISessionStoreSync): AbstractVerticle(), ISessionStoreAsync {
    private lateinit var dbExecutor: WorkerExecutor

    override fun getType(): SessionStoreType {
        return store.getType()
    }

    override val sync: ISessionStoreSync
        get() = store

    override fun start(startPromise: Promise<Void>) {
        dbExecutor = vertx.createSharedWorkerExecutor("SessionStoreExecutor", 1)
        startPromise.complete()
    }

    override fun stop(stopPromise: Promise<Void>) {
        if (::dbExecutor.isInitialized) {
            dbExecutor.close()
        }
        stopPromise.complete()
    }

    override fun iterateOfflineClients(callback: (clientId: String) -> Unit): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.iterateOfflineClients(callback)
            null
        })
    }

    override fun iterateConnectedClients(callback: (clientId: String, nodeId: String) -> Unit): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.iterateConnectedClients(callback)
            null
        })
    }

    override fun iterateAllSessions(callback: (clientId: String, nodeId: String, connected: Boolean, cleanSession: Boolean) -> Unit): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.iterateAllSessions(callback)
            null
        })
    }

    override fun iterateNodeClients(nodeId: String, callback: (clientId: String, cleanSession: Boolean, lastWill: BrokerMessage) -> Unit): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.iterateNodeClients(nodeId, callback)
            null
        })
    }

    override fun iterateSubscriptions(callback: (topic: String, clientId: String, qos: Int, noLocal: Boolean, retainHandling: Int, retainAsPublished: Boolean) -> Unit): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.iterateSubscriptions(callback)
            null
        })
    }

    override fun setClient(clientId: String, nodeId: String, cleanSession: Boolean, connected: Boolean, information: JsonObject): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.setClient(clientId, nodeId, cleanSession, connected, information)
            null
        })
    }

    override fun setLastWill(clientId: String, message: BrokerMessage?): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.setLastWill(clientId, message)
            null
        })
    }

    override fun setConnected(clientId: String, connected: Boolean): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.setConnected(clientId, connected)
            null
        })
    }

    override fun isConnected(clientId: String): Future<Boolean> {
        return dbExecutor.executeBlocking(Callable {
            store.isConnected(clientId)
        })
    }

    override fun isPresent(clientId: String): Future<Boolean> {
        return dbExecutor.executeBlocking(Callable {
            store.isPresent(clientId)
        })
    }

    override fun addSubscriptions(subscriptions: List<MqttSubscription>): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.addSubscriptions(subscriptions)
            null
        })
    }

    override fun delSubscriptions(subscriptions: List<MqttSubscription>): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.delSubscriptions(subscriptions)
            null
        })
    }

    override fun delClient(clientId: String, callback: (MqttSubscription)->Unit): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.delClient(clientId, callback)
            null
        })
    }

    override fun purgeSessions(): Future<Void> {
        return dbExecutor.executeBlocking(Callable {
            store.purgeSessions()
            null
        })
    }
}
