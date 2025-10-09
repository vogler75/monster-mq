package at.rocworks.extensions.graphql

import at.rocworks.Utils
import at.rocworks.bus.IMessageBus
import at.rocworks.data.BrokerMessage
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger

class SubscriptionResolver(
    private val vertx: Vertx,
    private val messageBus: IMessageBus
) {
    companion object {
        private val logger: Logger = Utils.getLogger(SubscriptionResolver::class.java)
        private const val GRAPHQL_CLIENT_ID = "graphql-subscription"
    }

    fun topicUpdates(): DataFetcher<Publisher<TopicUpdate>> {
        return DataFetcher { env ->
            val topicFilter = env.getArgument<String>("topicFilter")
            val format = env.getArgument<DataFormat?>("format") ?: DataFormat.JSON

            TopicUpdatePublisher(vertx, messageBus, listOf(topicFilter ?: "#"), format)
        }
    }

    fun multiTopicUpdates(): DataFetcher<Publisher<TopicUpdate>> {
        return DataFetcher { env ->
            val topicFilters = env.getArgument<List<String>>("topicFilters") ?: emptyList()
            val format = env.getArgument<DataFormat?>("format") ?: DataFormat.JSON

            TopicUpdatePublisher(vertx, messageBus, topicFilters, format)
        }
    }

    private class TopicUpdatePublisher(
        private val vertx: Vertx,
        private val messageBus: IMessageBus,
        private val topicFilters: List<String>,
        private val format: DataFormat
    ) : Publisher<TopicUpdate> {
        
        private val subscribers = ConcurrentHashMap<Subscriber<in TopicUpdate>, SubscriptionHandler>()

        override fun subscribe(subscriber: Subscriber<in TopicUpdate>) {
            val handler = SubscriptionHandler(vertx, messageBus, topicFilters, format, subscriber)
            subscribers[subscriber] = handler
            subscriber.onSubscribe(handler)
        }
    }

    private class SubscriptionHandler(
        private val vertx: Vertx,
        private val messageBus: IMessageBus,
        private val topicFilters: List<String>,
        private val format: DataFormat,
        private val subscriber: Subscriber<in TopicUpdate>
    ) : Subscription {
        
        private val cancelled = AtomicBoolean(false)
        private val consumerIds = mutableListOf<String>()
        private val requested = AtomicLong(0L)
        private val pendingMessages = mutableListOf<TopicUpdate>()
        private val subscriptionId = "$GRAPHQL_CLIENT_ID-${System.currentTimeMillis()}"

        init {
            // Subscribe to the message bus for all messages
            messageBus.subscribeToMessageBus { message ->
                if (!cancelled.get()) {
                    // Check if message matches any of our topic filters
                    if (topicFilters.any { filter -> matchesTopicFilter(message.topicName, filter) }) {
                        handleMessage(message)
                    }
                }
            }.onSuccess {
                logger.fine { "GraphQL subscription created for topic filters: $topicFilters" }
            }.onFailure { err ->
                logger.severe("Failed to create GraphQL subscription: ${err.message}")
                subscriber.onError(err)
            }
        }

        private fun matchesTopicFilter(topic: String, filter: String): Boolean {
            val topicParts = topic.split("/")
            val filterParts = filter.split("/")
            
            if (filterParts.last() == "#") {
                // Multi-level wildcard at the end
                val filterPrefix = filterParts.dropLast(1)
                return topicParts.size >= filterPrefix.size && 
                       topicParts.take(filterPrefix.size) == filterPrefix
            }
            
            if (topicParts.size != filterParts.size) {
                return false
            }
            
            return topicParts.zip(filterParts).all { (topicPart, filterPart) ->
                filterPart == "+" || topicPart == filterPart
            }
        }

        private fun handleMessage(message: BrokerMessage) {
            vertx.runOnContext {
                if (cancelled.get()) return@runOnContext

                val (payload, actualFormat) = PayloadConverter.autoDetectAndEncode(
                    message.payload,
                    format
                )
                
                val update = TopicUpdate(
                    topic = message.topicName,
                    payload = payload,
                    format = actualFormat,
                    timestamp = message.time.toEpochMilli(),
                    qos = message.qosLevel,
                    retained = message.isRetain,
                    clientId = message.clientId
                )

                synchronized(this) {
                    val currentRequested = requested.get()
                    if (currentRequested > 0) {
                        subscriber.onNext(update)
                        requested.decrementAndGet()
                    } else {
                        pendingMessages.add(update)
                    }
                }
            }
        }

        override fun request(n: Long) {
            if (cancelled.get()) return
            
            synchronized(this) {
                requested.addAndGet(n)
                
                // Send pending messages
                while (requested.get() > 0 && pendingMessages.isNotEmpty()) {
                    val update = pendingMessages.removeAt(0)
                    subscriber.onNext(update)
                    requested.decrementAndGet()
                }
            }
        }

        override fun cancel() {
            if (cancelled.compareAndSet(false, true)) {
                // Clear pending messages
                consumerIds.clear()
                pendingMessages.clear()
                logger.fine { "GraphQL subscription cancelled" }
            }
        }
    }
}