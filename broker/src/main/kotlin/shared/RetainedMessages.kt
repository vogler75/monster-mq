package at.rocworks.shared

import at.rocworks.Const
import at.rocworks.data.*
import com.hazelcast.map.IMap
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlin.concurrent.thread

class RetainedMessages(
    private val index: TopicTree,
    private val store: MutableMap<String, MqttMessage>
): AbstractVerticle() {
    private val logger = Logger.getLogger(this.javaClass.simpleName)
    private val queues: List<ArrayBlockingQueue<MqttMessage>>

    private val maxRetainedMessagesSentToClient = 0 // 100_000 // TODO: configurable or timed

    private val addAddress = Const.GLOBAL_RETAINED_MESSAGES_NAMESPACE +"/A"
    private val delAddress = Const.GLOBAL_RETAINED_MESSAGES_NAMESPACE +"/D"

    init {
        logger.level = Const.DEBUG_LEVEL
        val queuesCount = 1 // TODO: configurable
        queues = List(queuesCount) { ArrayBlockingQueue<MqttMessage>(100_000) } // TODO: configurable
    }

    override fun start(startPromise: Promise<Void>) {
        vertx.executeBlocking(Callable {
            logger.info("Index type [${index.getType()}]")
            if (index.getType() == TopicTreeType.LOCAL) {
                vertx.eventBus().consumer<JsonArray>(addAddress) {
                    index.addAll(it.body().map { it.toString() })
                }

                vertx.eventBus().consumer<JsonArray>(delAddress) {
                    index.delAll(it.body().map { it.toString() })
                }

                logger.info("Indexing retained message store...")
                store.keys.forEach { index.add(it) }
                logger.info("Indexing retained message store finished.")
            }

            queues.forEachIndexed { index, queue -> writerThread(index, queue) }

            startPromise.complete()
        })
    }

    private fun writerThread(nr: Int, queue: ArrayBlockingQueue<MqttMessage>) = thread(start = true) {
        logger.info("Start thread...")
        vertx.setPeriodic(1000) {
            if (queue.size > 0)
                logger.info("Retained message queue [$nr] size [${queue.size}]")
        }

        val addBlock = arrayListOf<MqttMessage>()
        val delBlock = arrayListOf<MqttMessage>()

        fun addMessage(message: MqttMessage) {
            if (message.payload.isEmpty())
                delBlock.add(message)
            else
                addBlock.add(message)
        }

        while (true) {
            queue.poll(100, TimeUnit.MILLISECONDS)?.let { message ->
                addMessage(message)
                while (queue.poll()?.let(::addMessage) != null
                    && addBlock.size < 1000
                    && delBlock.size < 1000) {
                    // nothing to do here
                }

                if (delBlock.size > 0) {
                    val topics = delBlock.map { it.topicName }.distinct()
                    when (index.getType()) {
                        TopicTreeType.LOCAL -> vertx.eventBus().publish(delAddress, JsonArray(topics))
                        TopicTreeType.DISTRIBUTED -> index.delAll(topics)
                    }
                    topics.forEach { store.remove(it) } // there is no delAll
                    delBlock.clear()
                }

                if (addBlock.size > 0) {
                    val startTime = Instant.now()

                    val topics = addBlock.map { it.topicName }.distinct()
                    when (index.getType()) {
                        TopicTreeType.LOCAL -> vertx.eventBus().publish(addAddress, JsonArray(topics))
                        TopicTreeType.DISTRIBUTED -> index.addAll(topics)
                    }
                    val duration1 = Duration.between(startTime, Instant.now()).toMillis()

                    if (store is IMap) store.putAllAsync(addBlock.associateBy { it.topicName })
                    else store.putAll(addBlock.map { Pair(it.topicName, it) }) // needs 450ms for 1000 entries

                    val duration2 = Duration.between(startTime, Instant.now()).toMillis()
                    logger.finest { "Write block of size [${addBlock.size}] to map took [$duration1] [$duration2]" }
                    addBlock.clear()
                }
            }
        }
    }

    private var queueIdx=0
    fun saveMessage(message: MqttMessage): Future<Void> {
        logger.finest { "Save retained topic [${message.topicName}]" }
        try {
            queues[queueIdx].add(message)
        } catch (e: IllegalStateException) {
            // TODO: Alert
        }
        if (queues.size > 1 && ++queueIdx==queues.size) queueIdx=0
        return Future.succeededFuture()
    }

    fun findMatching(topicName: String, callback: (message: MqttMessage)->Unit): Future<Int> {
        val promise = Promise.promise<Int>()
        vertx.executeBlocking(Callable {
            var counter = 0
            try {
                index.findMatchingTopicNames(topicName) { topic ->
                    logger.finest { "Found matching topic [$topic] for [$topicName]" }
                    val message = store[topic]
                    if (message != null) {
                        counter++
                        callback(message)
                    } else { // it could be a node inside the tree index where we haven't stored a value
                        logger.finest { "Stored message for [$topic] is null!" }
                    }
                    if (maxRetainedMessagesSentToClient > 0 && counter >= maxRetainedMessagesSentToClient) {
                        logger.warning("Maximum retained messages sent.")
                        false
                    } else true
                }
            } catch (e: Exception) {
                e.printStackTrace()
            }
            logger.fine { "Found [$counter] matching retained messages for [$topicName]." }
            promise.complete(counter)
        })
        return promise.future()
    }
}