package at.rocworks.stores

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttSubscription
import at.rocworks.data.TopicTree
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.shareddata.AsyncMap
import java.util.logging.Logger

class SubscriptionStoreAsyncMap: AbstractVerticle(), ISubscriptionStore {
    private val logger = Logger.getLogger(this.javaClass.simpleName)
    private val tableName = "Subscriptions"

    private var table: AsyncMap<String, MutableSet<String>>? = null // clientId to topicName

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun getType(): SubscriptionStoreType = SubscriptionStoreType.MEMORY

    override fun start(startPromise: Promise<Void>) {
        Utils.getMap<String, MutableSet<String>>(vertx, tableName).onSuccess { subscriptions ->
            logger.info("Indexing subscription table [$tableName].")
            this.table = subscriptions
        }.onFailure {
            logger.severe("Error in getting map [$tableName] [${it.message}]")
            startPromise.fail(it)
        }
    }

    override fun populateIndex(index: TopicTree) {
        table?.let { table ->
            table.keys()
                .onSuccess { clients ->
                    Future.all(clients.map { client ->
                        table.get(client).onComplete { topics ->
                            topics.result().forEach { index.add(it, client) }
                        }
                    }).onComplete {
                        logger.info("Indexing subscription table [$tableName] finished.")
                    }
                }
                .onFailure {
                    logger.severe("Error in getting keys of [$tableName] [${it.message}]")
                }
        }
    }

    override fun addSubscriptions(subscriptions: List<MqttSubscription>) {
        table?.let { table ->
            subscriptions.forEach { subscription ->
                table.get(subscription.clientId).onComplete { client ->
                    client.result()?.add(subscription.topicName) ?: run {
                        table.put(subscription.clientId, hashSetOf(subscription.topicName))
                    }
                }.onFailure {
                    logger.severe(it.message)
                }
            }
        }
    }

    override fun removeSubscriptions(subscriptions: List<MqttSubscription>) {
        table?.let { table ->
            subscriptions.forEach { subscription ->
                table.get(subscription.clientId).onComplete { client ->
                    client.result()?.remove(subscription.topicName)
                }.onFailure {
                    logger.severe(it.message)
                }
            }
        }
    }

    override fun removeClient(clientId: String, callback: (MqttSubscription)->Unit) {
        table?.remove(clientId)?.onSuccess { topics: MutableSet<String>? ->
            topics?.forEach { topic ->
                callback(MqttSubscription(clientId, topic))
            }
        }
    }
}