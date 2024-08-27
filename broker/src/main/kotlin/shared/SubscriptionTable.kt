package at.rocworks.shared

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttClientId
import at.rocworks.data.MqttSubscription
import at.rocworks.data.MqttTopicName
import at.rocworks.data.TopicTreeLocal
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.shareddata.AsyncMap
import java.util.logging.Logger

class SubscriptionTable: AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)
    private val name = "Subscriptions"
    private val index = TopicTreeLocal()
    private var table: AsyncMap<String, MutableSet<MqttTopicName>>? = null // key as MqttClientId does not work

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    private val addAddress = Const.GLOBAL_SUBSCRIPTION_TABLE_NAMESPACE +"/A"
    private val delAddress = Const.GLOBAL_SUBSCRIPTION_TABLE_NAMESPACE +"/D"

    override fun start(startPromise: Promise<Void>) {
        vertx.eventBus().consumer<MqttSubscription>(addAddress) {
            index.add(it.body().topicName, it.body().clientId.identifier)
        }

        vertx.eventBus().consumer<MqttSubscription>(delAddress) {
            index.del(it.body().topicName, it.body().clientId.identifier)
        }

        Utils.getMap<String, MutableSet<MqttTopicName>>(vertx, name).onSuccess { subscriptions ->
            logger.info("Indexing subscription table [$name].")
            this.table = subscriptions
            subscriptions.keys()
                .onSuccess { clients ->
                    Future.all(clients.map { client ->
                        subscriptions.get(client).onComplete { topics ->
                            topics.result().forEach { index.add(it, client) }
                        }
                    }).onComplete {
                        logger.info("Indexing subscription table [$name] finished.")
                        startPromise.complete()
                    }
               }
                .onFailure {
                    logger.severe("Error in getting keys of [$name] [${it.message}]")
                    startPromise.fail(it)
                }
        }.onFailure {
            logger.severe("Error in getting map [$name] [${it.message}]")
            startPromise.fail(it)
        }
    }

    fun addSubscription(subscription: MqttSubscription) {
        vertx.eventBus().publish(addAddress, subscription)
        table?.let { subscriptions ->
            subscriptions.get(subscription.clientId.identifier).onComplete { client ->
                client.result()?.add(subscription.topicName) ?: run {
                    subscriptions.put(subscription.clientId.identifier, hashSetOf(subscription.topicName))
                }
            }.onFailure {
               logger.severe(it.message)
            }
        }
    }

    fun removeSubscription(subscription: MqttSubscription) {
        vertx.eventBus().publish(delAddress, subscription)
        table?.let { subscriptions ->
            subscriptions.get(subscription.clientId.identifier).onComplete { client ->
                client.result()?.remove(subscription.topicName)
            }.onFailure {
                logger.severe(it.message)
            }
        }
    }

    fun removeClient(clientId: MqttClientId) {
        table?.remove(clientId.identifier)?.onSuccess { topics: MutableSet<MqttTopicName>? ->
            topics?.forEach { topic ->
                vertx.eventBus().publish(delAddress, MqttSubscription(clientId, topic))
            }
        }
    }

    fun findClients(topicName: MqttTopicName) = index.findDataOfTopicName(topicName).toSet()

}