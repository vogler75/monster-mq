package at.rocworks.extensions.graphql

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.bus.IMessageBus
import at.rocworks.data.BrokerMessage
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
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

    fun systemLogs(): DataFetcher<Publisher<SystemLogEntry>> {
        return DataFetcher { env ->
            val node = env.getArgument<String?>("node") ?: "+"
            val levelArg = env.getArgument<Any?>("level")
            val levels = when (levelArg) {
                is List<*> -> levelArg.filterIsInstance<String>()
                is String -> listOf(levelArg)
                null -> listOf("+")
                else -> listOf("+")
            }
            logger.info("SubscriptionResolver.systemLogs - levelArg: $levelArg, levels: $levels")
            val loggerFilter = env.getArgument<String?>("logger")
            val threadFilter = env.getArgument<Long?>("thread")
            val sourceClassFilter = env.getArgument<String?>("sourceClass")
            val sourceMethodFilter = env.getArgument<String?>("sourceMethod")
            val messageFilter = env.getArgument<String?>("message")
            
            SystemLogsPublisher(vertx, node, levels, loggerFilter, threadFilter, 
                               sourceClassFilter, sourceMethodFilter, messageFilter)
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

    private class SystemLogsPublisher(
        private val vertx: Vertx,
        private val nodeFilter: String,
        private val levelFilters: List<String>,
        private val loggerFilter: String?,
        private val threadFilter: Long?,
        private val sourceClassFilter: String?,
        private val sourceMethodFilter: String?,
        private val messageFilter: String?
    ) : Publisher<SystemLogEntry> {
        
        private val subscribers = ConcurrentHashMap<Subscriber<in SystemLogEntry>, SystemLogsHandler>()

        override fun subscribe(subscriber: Subscriber<in SystemLogEntry>) {
            val handler = SystemLogsHandler(
                vertx,
                nodeFilter,
                levelFilters,
                loggerFilter,
                threadFilter,
                sourceClassFilter,
                sourceMethodFilter,
                messageFilter,
                subscriber
            )
            subscribers[subscriber] = handler
            subscriber.onSubscribe(handler)
        }
    }

    private class SystemLogsHandler(
        private val vertx: Vertx,
        private val nodeFilter: String,
        private val levelFilters: List<String>,
        private val loggerFilter: String?,
        private val threadFilter: Long?,
        private val sourceClassFilter: String?,
        private val sourceMethodFilter: String?,
        private val messageFilter: String?,
        private val subscriber: Subscriber<in SystemLogEntry>
    ) : Subscription {
        
        private val cancelled = AtomicBoolean(false)
        private val requested = AtomicLong(0L)
        private val pendingLogs = mutableListOf<SystemLogEntry>()
        
        // Compile regex patterns once for efficiency
        private val loggerRegex = loggerFilter?.let { runCatching { Regex(it) }.getOrNull() }
        private val sourceClassRegex = sourceClassFilter?.let { runCatching { Regex(it) }.getOrNull() }
        private val sourceMethodRegex = sourceMethodFilter?.let { runCatching { Regex(it) }.getOrNull() }
        private val messageRegex = messageFilter?.let { runCatching { Regex(it) }.getOrNull() }

        init {
            // Subscribe to broadcast event bus for system logs
            vertx.eventBus().consumer<BrokerMessage>(EventBusAddresses.Cluster.BROADCAST) { message ->
                message.body()?.let { brokerMessage ->
                    if (!cancelled.get() && brokerMessage.topicName.startsWith("${Const.SYS_TOPIC_NAME}/${Const.LOG_TOPIC_NAME}/")) {
                        handleLogMessage(brokerMessage)
                    }
                }
            }
            logger.fine { "GraphQL system logs subscription created (node: $nodeFilter, levels: $levelFilters)" }
        }

        private fun handleLogMessage(message: BrokerMessage) {
            try {
                // Parse JSON payload
                val logJson = JsonObject(String(message.payload))
                
                // Extract fields
                val node = logJson.getString("node") ?: return
                val level = logJson.getString("level") ?: return
                val loggerName = logJson.getString("logger") ?: return
                val logMessage = logJson.getString("message") ?: return
                val thread = logJson.getLong("thread") ?: return
                val timestamp = logJson.getString("timestamp") ?: return
                
                // Apply topic-level filters (node and level)
                if (!matchesTopicFilter(node, nodeFilter)) return
                
                // Check if level matches any of the level filters
                val levelMatches = levelFilters.contains("+") || levelFilters.any { matchesTopicFilter(level, it) }
                logger.fine("Subscription: level='$level', levelFilters=$levelFilters, matches=$levelMatches")
                if (!levelMatches) return
                
                // Apply field filters with regex support
                if (loggerFilter != null) {
                    if (loggerRegex != null) {
                        if (!loggerRegex.containsMatchIn(loggerName)) return
                    } else {
                        if (loggerName != loggerFilter) return
                    }
                }
                
                if (threadFilter != null && thread != threadFilter) return
                
                val sourceClass = logJson.getString("sourceClass")
                if (sourceClassFilter != null && sourceClass != null) {
                    if (sourceClassRegex != null) {
                        if (!sourceClassRegex.containsMatchIn(sourceClass)) return
                    } else {
                        if (sourceClass != sourceClassFilter) return
                    }
                }
                
                val sourceMethod = logJson.getString("sourceMethod")
                if (sourceMethodFilter != null && sourceMethod != null) {
                    if (sourceMethodRegex != null) {
                        if (!sourceMethodRegex.containsMatchIn(sourceMethod)) return
                    } else {
                        if (sourceMethod != sourceMethodFilter) return
                    }
                }
                
                if (messageFilter != null) {
                    if (messageRegex != null) {
                        if (!messageRegex.containsMatchIn(logMessage)) return
                    } else {
                        if (!logMessage.contains(messageFilter)) return
                    }
                }
                
                // Extract optional fields
                val parameters = logJson.getJsonArray("parameters")?.map { it.toString() }
                val exceptionJson = logJson.getJsonObject("exception")
                val exception = exceptionJson?.let {
                    ExceptionInfo(
                        `class` = it.getString("class") ?: "",
                        message = it.getString("message"),
                        stackTrace = it.getString("stackTrace") ?: ""
                    )
                }
                
                // Create log entry
                val logEntry = SystemLogEntry(
                    timestamp = timestamp,
                    level = level,
                    logger = loggerName,
                    message = logMessage,
                    thread = thread,
                    node = node,
                    sourceClass = sourceClass,
                    sourceMethod = sourceMethod,
                    parameters = parameters,
                    exception = exception
                )
                
                // Send to subscriber
                vertx.runOnContext {
                    if (cancelled.get()) return@runOnContext
                    
                    synchronized(this) {
                        val currentRequested = requested.get()
                        if (currentRequested > 0) {
                            subscriber.onNext(logEntry)
                            requested.decrementAndGet()
                        } else {
                            pendingLogs.add(logEntry)
                        }
                    }
                }
            } catch (e: Exception) {
                logger.warning("Error processing log message: ${e.message}")
            }
        }
        
        private fun matchesTopicFilter(value: String, filter: String): Boolean {
            return filter == "+" || filter == "#" || value == filter
        }

        override fun request(n: Long) {
            if (cancelled.get()) return
            
            synchronized(this) {
                requested.addAndGet(n)
                
                // Send pending logs
                while (requested.get() > 0 && pendingLogs.isNotEmpty()) {
                    val logEntry = pendingLogs.removeAt(0)
                    subscriber.onNext(logEntry)
                    requested.decrementAndGet()
                }
            }
        }

        override fun cancel() {
            if (cancelled.compareAndSet(false, true)) {
                pendingLogs.clear()
                logger.fine { "GraphQL system logs subscription cancelled" }
            }
        }
    }
}