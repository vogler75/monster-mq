package at.rocworks.stores

import at.rocworks.Const
import at.rocworks.data.MqttSubscription
import at.rocworks.data.TopicTree
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlin.concurrent.thread

class SubscriptionHandler(private val table: ISubscriptionStore): AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    private val index = TopicTree()

    private val addQueue: ArrayBlockingQueue<MqttSubscription> = ArrayBlockingQueue(10_000) // TODO: configurable
    private val delQueue: ArrayBlockingQueue<MqttSubscription> = ArrayBlockingQueue(10_000) // TODO: configurable

    private val addAddress = Const.GLOBAL_SUBSCRIPTION_TABLE_NAMESPACE +"/A"
    private val delAddress = Const.GLOBAL_SUBSCRIPTION_TABLE_NAMESPACE +"/D"

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun start(startPromise: Promise<Void>) {
        logger.info("Start handler...")
        vertx.eventBus().consumer<MqttSubscription>(addAddress) {
            index.add(it.body().topicName, it.body().clientId)
        }
        vertx.eventBus().consumer<MqttSubscription>(delAddress) {
            index.del(it.body().topicName, it.body().clientId)
        }
        workerThread("Add", addQueue, table::addSubscriptions)
        workerThread("Del", delQueue, table::removeSubscriptions)
        startPromise.complete()
    }

    private fun workerThread(
        name: String,
        queue: ArrayBlockingQueue<MqttSubscription>,
        execute: (block: List<MqttSubscription>)->Unit
    ) = thread(start = true) {
        logger.info("Start [$name] thread")
        vertx.setPeriodic(1000) {
            if (queue.size > 0)
                logger.info("Queue [$name] size [${queue.size}]")
        }

        val block = arrayListOf<MqttSubscription>()

        while (true) {
            queue.poll(100, TimeUnit.MILLISECONDS)?.let { subscription ->
                block.add(subscription)
                while (queue.poll()?.let(block::add) != null && block.size < 1000) {
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


    fun addSubscription(subscription: MqttSubscription) {
        vertx.eventBus().publish(addAddress, subscription)
        try {
            addQueue.add(subscription)
        } catch (e: IllegalStateException) {
            // TODO: Alert
        }
    }

    fun removeSubscription(subscription: MqttSubscription) {
        vertx.eventBus().publish(delAddress, subscription)
        try {
            delQueue.add(subscription)
        } catch (e: IllegalStateException) {
            // TODO: Alert
        }
    }

    fun removeClient(clientId: String) {
        table.removeClient(clientId) { subscription ->
            vertx.eventBus().publish(delAddress, subscription)
        }
    }

    fun findClients(topicName: String): Set<String> {
        val result = index.findDataOfTopicName(topicName).toSet()
        logger.finest { "Found [${result.size}] clients." }
        return result
    }
}