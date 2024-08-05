package at.rocworks

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.core.shareddata.AsyncMap

import java.time.Instant
import java.util.logging.Logger

class Distributor: AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    private var globalWildcardSubscriptions: AsyncMap<String, String>? = null
    private var globalRetainedMessages: AsyncMap<String, Buffer>? = null

    data class TopicNode (
        val children: MutableMap<String, TopicNode> = mutableMapOf()
    )

    class TopicTree {
        private val root = TopicNode()

        fun addTopicName(topicName: String) {
            val xs = topicName.split("/")
            if (xs.isEmpty()) return
            else addTopicNode(root, xs.first(), xs.drop(1))
        }

        fun printTree() = printTreeNode(root)

        private fun printTreeNode(node: TopicNode, level: Int = 0) {
            node.children.forEach {
                println(" ".repeat(level) + " "+it.key)
                printTreeNode(it.value, level + 1)
            }
        }

        private fun addTopicNode(node: TopicNode, topic: String, rest: List<String>) {
            val next = node.children.getOrPut(topic) { TopicNode() }
            if (rest.isNotEmpty()) addTopicNode(next, rest.first(), rest.drop(1))
        }
    }

    private val topicTree = TopicTree()

    override fun start(startPromise: Promise<Void>) {
        val f1 = getMap<String, String>("WildcardSubscriptions").onComplete {
            globalWildcardSubscriptions = it.result()
        }

        val f2 = getMap<String, Buffer>("RetainedMessages").onComplete {
            globalRetainedMessages = it.result()
        }

        vertx.eventBus().consumer(Const.DIST_SUBSCRIBE_REQUEST) {
            logger.info("Subscribe request [${it.body()}]")
            val topicName = it.body()
            val address = Const.getTopicBusAddr(topicName)
            if (Const.isWildCardTopic(topicName)) {
                topicTree.addTopicName(topicName)
                topicTree.printTree()
            }
            it.reply(address)
        }

        Future.all(f1, f2).onComplete {
            startPromise.complete()
        }
    }

    private fun <K,V> getMap(name: String): Future<AsyncMap<K, V>> {
        val promise = Promise.promise<AsyncMap<K, V>>()
        val sharedData = vertx.sharedData()
        if (vertx.isClustered) {
            sharedData.getClusterWideMap<K, V>("TopicMap") { it ->
                if (it.succeeded()) {
                    promise.complete(it.result())
                } else {
                    println("Failed to access the shared map [$name]: ${it.cause()}")
                    promise.fail(it.cause())
                }
            }
        } else {
            sharedData.getAsyncMap<K, V>("TopicMap") {
                if (it.succeeded()) {
                    promise.complete(it.result())
                } else {
                    println("Failed to access the shared map [$name]: ${it.cause()}")
                    promise.fail(it.cause())
                }
            }
        }
        return promise.future()
    }

    fun testMap() {
        globalWildcardSubscriptions?.apply {
            // Get data from the shared map
            get("key") { getResult ->
                if (getResult.succeeded()) {
                    println("Value for 'key': ${getResult.result()}")
                } else {
                    println("Failed to get data: ${getResult.cause()}")
                }
            }

            val value = Instant.now().toEpochMilli().toString()
            println("New value $value")

            // Put data into the shared map
            put("key", value) { putResult ->
                if (putResult.succeeded()) {
                    println("Data added to the shared map")
                } else {
                    println("Failed to put data: ${putResult.cause()}")
                }
            }
        }
    }
}