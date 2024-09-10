package at.rocworks.stores

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.*
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.shareddata.AsyncMap
import java.util.logging.Logger

class SessionStoreAsyncMap: AbstractVerticle(), ISessionStore {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    private val sessionTableName = "Sessions"
    private val subscriptionTableName = "Subscriptions"

    private var sessionTable: AsyncMap<String, MqttSession>? = null // clientId to sessionId
    private var subscriptionTable: AsyncMap<String, MutableSet<Pair<String, MqttQoS>>>? = null // clientId to topicName

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun getType(): SessionStoreType = SessionStoreType.MEMORY

    private val readyPromise: Promise<Void> = Promise.promise()
    override fun storeReady(): Future<Void> = readyPromise.future()

    override fun start(startPromise: Promise<Void>) {
        Future.all(listOf(
            Utils.getMap<String, MqttSession>(vertx, sessionTableName).onSuccess { sessions ->
                logger.info("Indexing session table [$sessionTableName].")
                this.sessionTable = sessions
            }.onFailure {
                logger.severe("Error in getting map [$sessionTableName] [${it.message}]")
            },
            Utils.getMap<String, MutableSet<Pair<String, MqttQoS>>>(vertx, subscriptionTableName).onSuccess { subscriptions ->
                logger.info("Indexing subscription table [$subscriptionTableName].")
                this.subscriptionTable = subscriptions
            }.onFailure {
                logger.severe("Error in getting map [$subscriptionTableName] [${it.message}]")
            }
        )).onComplete {
            readyPromise.complete()
            startPromise.complete()
        }
    }

    override fun buildIndex(index: TopicTree<MqttClientQoS>) {
        subscriptionTable?.let { table ->
            table.keys()
                .onSuccess { clients ->
                    Future.all(clients.map { client ->
                        table.get(client).onComplete { topics ->
                            topics.result().forEach { index.add(it.first, MqttClientQoS(client, it.second)) }
                        }
                    }).onComplete {
                        logger.info("Indexing subscription table [$subscriptionTableName] finished.")
                    }
                }
                .onFailure {
                    logger.severe("Error in getting keys of [$subscriptionTableName] [${it.message}]")
                }
        }
    }

    override fun offlineClients(offline: MutableSet<String>) {
        TODO("Not yet implemented")
    }

    override fun setClient(clientId: String, cleanSession: Boolean, connected: Boolean) {
        sessionTable?.put(clientId, MqttSession(clientId, cleanSession = cleanSession, connected = connected))
    }

    override fun setConnected(clientId: String, connected: Boolean) {
        sessionTable?.get(clientId)?.onSuccess { session ->
            session.connected = connected
            sessionTable?.put(clientId, session)
        }
    }

    override fun isConnected(clientId: String): Boolean {
        return sessionTable?.get(clientId)?.result()?.connected ?: false
    }

    override fun setLastWill(clientId: String, topic: String, message: MqttMessage) {
        sessionTable?.get(clientId)?.onSuccess { session ->
            session.lastWill = message
            sessionTable?.put(clientId, session)
        }
    }

    override fun addSubscriptions(subscriptions: List<MqttSubscription>) {
        subscriptionTable?.let { table ->
            subscriptions.forEach { subscription ->
                table.get(subscription.clientId).onComplete { client ->
                    val item = subscription.topicName to subscription.qos
                    client.result()?.add(item) ?: run {
                        table.put(subscription.clientId, hashSetOf(item))
                    }
                }.onFailure {
                    logger.severe(it.message)
                }
            }
        }
    }

    override fun delSubscriptions(subscriptions: List<MqttSubscription>) {
        subscriptionTable?.let { table ->
            subscriptions.forEach { subscription ->
                table.get(subscription.clientId).onComplete { client ->
                    client.result()?.remove(subscription.topicName to subscription.qos)
                }.onFailure {
                    logger.severe(it.message)
                }
            }
        }
    }

    override fun delClient(clientId: String, callback: (MqttSubscription)->Unit) {
        subscriptionTable?.remove(clientId)?.onSuccess { topics: MutableSet<Pair<String, MqttQoS>>? ->
            topics?.forEach { topic ->
                callback(MqttSubscription(clientId, topic.first, topic.second))
            }
        }
    }

    override fun enqueueMessages(messages: List<Pair<MqttMessage, List<String>>>) {
        TODO("Not yet implemented")
    }

    override fun dequeueMessages(clientId: String, callback: (MqttMessage)->Unit) {
        TODO("Not yet implemented")
    }
}