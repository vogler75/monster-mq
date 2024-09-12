package at.rocworks.stores

import at.rocworks.Const
import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttSubscription
import at.rocworks.data.TopicTree
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlin.concurrent.thread

class SessionHandler(private val store: ISessionStore): AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    private val index = TopicTree<String, Int>() // Topic index with client and QoS
    private val offline = mutableSetOf<String>() // Offline clients

    private val subAddQueue: ArrayBlockingQueue<MqttSubscription> = ArrayBlockingQueue(10_000) // TODO: configurable
    private val subDelQueue: ArrayBlockingQueue<MqttSubscription> = ArrayBlockingQueue(10_000) // TODO: configurable

    private val messageQueue: ArrayBlockingQueue<Pair<MqttMessage, List<String>>> = ArrayBlockingQueue(10_000) // TODO: configurable

    private val subscriptionAddAddress = Const.GLOBAL_SUBSCRIPTION_TABLE_NAMESPACE+"/A"
    private val subscriptionDelAddress = Const.GLOBAL_SUBSCRIPTION_TABLE_NAMESPACE+"/D"

    private val clientOnlineAddress = Const.GLOBAL_CLIENT_TABLE_NAMESPACE+"/C"
    private val clientOfflineAddress = Const.GLOBAL_CLIENT_TABLE_NAMESPACE+"/D"

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun start(startPromise: Promise<Void>) {
        logger.info("Start session handler...")
        vertx.eventBus().consumer<MqttSubscription>(subscriptionAddAddress) {
            index.add(it.body().topicName, it.body().clientId, it.body().qos.value())
        }
        vertx.eventBus().consumer<MqttSubscription>(subscriptionDelAddress) {
            index.del(it.body().topicName, it.body().clientId)
        }
        vertx.eventBus().consumer<String>(clientOnlineAddress) {
            offline.remove(it.body())
        }
        vertx.eventBus().consumer<String>(clientOfflineAddress) {
            offline.add(it.body())
        }

        queueWorkerThread("SubAddQueue", subAddQueue, 1000, store::addSubscriptions)
        queueWorkerThread("SubDelQueue", subDelQueue, 1000, store::delSubscriptions)
        queueWorkerThread("MessageQueue", messageQueue, 1000, store::enqueueMessages)

        store.storeReady().onSuccess {
            store.buildIndex(index)
            store.offlineClients(offline)
            startPromise.complete()
        }
    }

    private fun <T> queueWorkerThread(
        name: String,
        queue: ArrayBlockingQueue<T>,
        blockSize: Int,
        execute: (block: List<T>)->Unit
    ) = thread(start = true) {
        logger.info("Start [$name] thread")
        vertx.setPeriodic(1000) {
            if (queue.size > 0)
                logger.info("Queue [$name] size [${queue.size}]")
        }

        val block = arrayListOf<T>()

        while (true) {
            queue.poll(100, TimeUnit.MILLISECONDS)?.let { item ->
                block.add(item)
                while (queue.poll()?.let(block::add) != null && block.size < blockSize) {
                    // nothing to do here
                }
                if (block.size > 0) {
                    logger.finest("Queue [$name] block with size [${block.size}]")
                    execute(block)
                    block.clear()
                }
            }
        }
    }

    fun setClient(clientId: String, cleanSession: Boolean, connected: Boolean) {
        vertx.eventBus().publish(if (connected) clientOnlineAddress else clientOfflineAddress, clientId)
        store.setClient(clientId, cleanSession, connected)
    }

    fun delClient(clientId: String) {
        vertx.eventBus().publish(clientOfflineAddress, clientId)
        store.delClient(clientId) { subscription ->
            logger.finest { "Delete subscription [$subscription]" }
            vertx.eventBus().publish(subscriptionDelAddress, subscription)
        }
    }

    fun pauseClient(clientId: String) {
        vertx.eventBus().publish(clientOfflineAddress, clientId)
        store.setConnected(clientId, false)
    }

    fun isConnected(clientId: String): Boolean {
        return !offline.contains(clientId)
    }

    fun enqueueMessage(message: MqttMessage, clientIds: List<String>) {
        messageQueue.add(Pair(message, clientIds))
    }

    fun dequeueMessages(clientId: String, callback: (MqttMessage)->Unit) {
        store.dequeueMessages(clientId, callback)
    }

    fun addSubscription(subscription: MqttSubscription) {
        vertx.eventBus().publish(subscriptionAddAddress, subscription)
        try {
            subAddQueue.add(subscription)
        } catch (e: IllegalStateException) {
            // TODO: Alert
        }
    }

    fun delSubscription(subscription: MqttSubscription) {
        vertx.eventBus().publish(subscriptionDelAddress, subscription)
        try {
            subDelQueue.add(subscription)
        } catch (e: IllegalStateException) {
            // TODO: Alert
        }
    }

    fun findClients(topicName: String): Set<Pair<String, Int>> {
        val result = index.findDataOfTopicName(topicName).toSet()
        logger.finest { "Found [${result.size}] clients." }
        return result
    }
}