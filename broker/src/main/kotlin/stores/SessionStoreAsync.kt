package at.rocworks.stores

import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttSubscription
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.util.concurrent.Callable

class SessionStoreAsync(private val store: ISessionStore): AbstractVerticle(), ISessionStoreAsync {

    override fun getType(): SessionStoreType {
        return store.getType()
    }

    override val sync: ISessionStore
        get() = store

    override fun iterateOfflineClients(callback: (clientId: String) -> Unit): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.iterateOfflineClients(callback)
            promise.complete()
        })
        return promise.future()
    }

    override fun iterateNodeClients(nodeId: String, callback: (clientId: String, cleanSession: Boolean, lastWill: MqttMessage) -> Unit): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.iterateNodeClients(nodeId, callback)
            promise.complete()
        })
        return promise.future()
    }

    override fun iterateSubscriptions(callback: (topic: String, clientId: String, qos: Int) -> Unit): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.iterateSubscriptions(callback)
            promise.complete()
        })
        return promise.future()
    }

    override fun setClient(clientId: String, nodeId: String, cleanSession: Boolean, connected: Boolean, information: JsonObject): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.setClient(clientId, nodeId, cleanSession, connected, information)
            promise.complete()
        })
        return promise.future()
    }

    override fun setLastWill(clientId: String, message: MqttMessage?): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.setLastWill(clientId, message)
            promise.complete()
        })
        return promise.future()
    }

    override fun setConnected(clientId: String, connected: Boolean): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.setConnected(clientId, connected)
            promise.complete()
        })
        return promise.future()
    }

    override fun isConnected(clientId: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()
        vertx.executeBlocking(Callable {
            promise.complete(store.isConnected(clientId))
        })
        return promise.future()
    }

    override fun isPresent(clientId: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()
        vertx.executeBlocking(Callable {
            promise.complete(store.isPresent(clientId))
        })
        return promise.future()
    }

    override fun addSubscriptions(subscriptions: List<MqttSubscription>): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.addSubscriptions(subscriptions)
            promise.complete()
        })
        return promise.future()
    }

    override fun delSubscriptions(subscriptions: List<MqttSubscription>): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.delSubscriptions(subscriptions)
            promise.complete()
        })
        return promise.future()
    }

    override fun delClient(clientId: String, callback: (MqttSubscription)->Unit): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.delClient(clientId, callback)
            promise.complete()
        })
        return promise.future()
    }

    override fun enqueueMessages(messages: List<Pair<MqttMessage, List<String>>>): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.enqueueMessages(messages)
            promise.complete()
        })
        return promise.future()
    }

    override fun dequeueMessages(clientId: String, callback: (MqttMessage)->Boolean): Future<Void> {
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

    override fun purgeQueuedMessages(): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.purgeQueuedMessages()
            promise.complete()
        })
        return promise.future()
    }

    override fun purgeSessions(): Future<Void> {
        val promise = Promise.promise<Void>()
        vertx.executeBlocking(Callable {
            store.purgeSessions()
            promise.complete()
        })
        return promise.future()
    }
}