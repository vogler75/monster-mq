package at.rocworks.extensions.graphql

import at.rocworks.Const
import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
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
    private val vertx: Vertx
) {
    companion object {
        private val logger: Logger = Utils.getLogger(SubscriptionResolver::class.java)
        private const val GRAPHQL_CLIENT_ID = "graphql-subscription"
    }

    fun topicUpdates(): DataFetcher<Publisher<TopicUpdate>> {
        return DataFetcher { env ->
            // Support both old single topicFilter (backward compatibility) and new topicFilters array
            val topicFilters = when {
                env.containsArgument("topicFilters") -> {
                    env.getArgument<List<String>>("topicFilters") ?: emptyList()
                }
                env.containsArgument("topicFilter") -> {
                    val filter = env.getArgument<String>("topicFilter")
                    listOf(filter ?: "#")
                }
                else -> listOf("#")
            }
            val format = env.getArgument<DataFormat?>("format") ?: DataFormat.JSON

            TopicUpdatePublisher(vertx, topicFilters, format)
        }
    }

    fun topicUpdatesBulk(): DataFetcher<Publisher<TopicUpdateBulk>> {
        return DataFetcher { env ->
            val topicFilters = env.getArgument<List<String>>("topicFilters") ?: emptyList()
            val format = env.getArgument<DataFormat?>("format") ?: DataFormat.JSON
            val timeoutMs = env.getArgument<Int?>("timeoutMs") ?: 1000
            val maxSize = env.getArgument<Int?>("maxSize") ?: 100

            if (timeoutMs <= 0) throw IllegalArgumentException("timeoutMs must be > 0")
            if (maxSize <= 0) throw IllegalArgumentException("maxSize must be > 0")

            BulkTopicUpdatePublisher(vertx, topicFilters, format, timeoutMs.toLong(), maxSize)
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
            logger.fine("SubscriptionResolver.systemLogs - levelArg: $levelArg, levels: $levels")
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
        private val topicFilters: List<String>,
        private val format: DataFormat
    ) : Publisher<TopicUpdate> {

        private val subscribers = ConcurrentHashMap<Subscriber<in TopicUpdate>, SubscriptionHandler>()

        override fun subscribe(subscriber: Subscriber<in TopicUpdate>) {
            val handler = SubscriptionHandler(vertx, topicFilters, format, subscriber)
            subscribers[subscriber] = handler
            subscriber.onSubscribe(handler)
        }
    }

    private class SubscriptionHandler(
        private val vertx: Vertx,
        private val topicFilters: List<String>,
        private val format: DataFormat,
        private val subscriber: Subscriber<in TopicUpdate>
    ) : Subscription {

        private val cancelled = AtomicBoolean(false)
        private val requested = AtomicLong(0L)
        private val pendingMessages = mutableListOf<TopicUpdate>()
        private val subscriptionId = "$GRAPHQL_CLIENT_ID-${System.currentTimeMillis()}"

        init {
            // Register with SessionHandler to receive MQTT messages
            val sessionHandler = Monster.getSessionHandler()
            if (sessionHandler != null) {
                sessionHandler.registerMessageListener(subscriptionId, topicFilters) { message ->
                    if (!cancelled.get()) {
                        handleMessage(message)
                    }
                }
                logger.fine { "GraphQL subscription created for topic filters: $topicFilters (id: $subscriptionId)" }
            } else {
                logger.severe("SessionHandler not available for GraphQL subscription")
                subscriber.onError(RuntimeException("SessionHandler not available"))
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
                // Unregister from SessionHandler
                Monster.getSessionHandler()?.unregisterMessageListener(subscriptionId)
                pendingMessages.clear()
                logger.fine { "GraphQL subscription cancelled (id: $subscriptionId)" }
            }
        }
    }

    private class BulkTopicUpdatePublisher(
        private val vertx: Vertx,
        private val topicFilters: List<String>,
        private val format: DataFormat,
        private val timeoutMs: Long,
        private val maxSize: Int
    ) : Publisher<TopicUpdateBulk> {

        private val subscribers = ConcurrentHashMap<Subscriber<in TopicUpdateBulk>, BulkSubscriptionHandler>()

        override fun subscribe(subscriber: Subscriber<in TopicUpdateBulk>) {
            val handler = BulkSubscriptionHandler(vertx, topicFilters, format, timeoutMs, maxSize, subscriber)
            subscribers[subscriber] = handler
            subscriber.onSubscribe(handler)
        }
    }

    private class BulkSubscriptionHandler(
        private val vertx: Vertx,
        private val topicFilters: List<String>,
        private val format: DataFormat,
        private val timeoutMs: Long,
        private val maxSize: Int,
        private val subscriber: Subscriber<in TopicUpdateBulk>
    ) : Subscription {

        private val cancelled = AtomicBoolean(false)
        private val requested = AtomicLong(0L)
        private val pendingBulks = mutableListOf<TopicUpdateBulk>()
        private val messageBuffer = mutableListOf<TopicUpdate>()
        private val subscriptionId = "$GRAPHQL_CLIENT_ID-bulk-${System.currentTimeMillis()}"
        private var timeoutTimerId: Long? = null
        private var bufferStartTime: Long = 0

        init {
            // Register with SessionHandler to receive MQTT messages
            val sessionHandler = Monster.getSessionHandler()
            if (sessionHandler != null) {
                sessionHandler.registerMessageListener(subscriptionId, topicFilters) { message ->
                    if (!cancelled.get()) {
                        handleMessage(message)
                    }
                }
                logger.fine { "GraphQL bulk subscription created for topic filters: $topicFilters (id: $subscriptionId)" }
            } else {
                logger.severe("SessionHandler not available for GraphQL bulk subscription")
                subscriber.onError(RuntimeException("SessionHandler not available"))
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
                    // Initialize buffer start time if this is the first message
                    if (messageBuffer.isEmpty()) {
                        bufferStartTime = System.currentTimeMillis()
                        // Schedule timeout flush
                        scheduleTimeoutFlush()
                    }

                    messageBuffer.add(update)

                    // Check if we hit max size
                    if (messageBuffer.size >= maxSize) {
                        flushBuffer()
                    }
                }
            }
        }

        private fun scheduleTimeoutFlush() {
            // Cancel any existing timer
            timeoutTimerId?.let { vertx.cancelTimer(it) }

            // Schedule new timer
            timeoutTimerId = vertx.setTimer(timeoutMs) {
                synchronized(this) {
                    if (!cancelled.get() && messageBuffer.isNotEmpty()) {
                        flushBuffer()
                    }
                }
            }
        }

        private fun flushBuffer() {
            if (messageBuffer.isEmpty()) return

            val bulk = TopicUpdateBulk(
                updates = messageBuffer.toList(),
                count = messageBuffer.size,
                timestamp = System.currentTimeMillis()
            )

            messageBuffer.clear()
            timeoutTimerId?.let { vertx.cancelTimer(it) }
            timeoutTimerId = null

            val currentRequested = requested.get()
            if (currentRequested > 0) {
                subscriber.onNext(bulk)
                requested.decrementAndGet()
            } else {
                pendingBulks.add(bulk)
            }
        }

        override fun request(n: Long) {
            if (cancelled.get()) return

            synchronized(this) {
                requested.addAndGet(n)

                // Send pending bulks
                while (requested.get() > 0 && pendingBulks.isNotEmpty()) {
                    val bulk = pendingBulks.removeAt(0)
                    subscriber.onNext(bulk)
                    requested.decrementAndGet()
                }
            }
        }

        override fun cancel() {
            if (cancelled.compareAndSet(false, true)) {
                // Cancel timeout timer
                timeoutTimerId?.let { vertx.cancelTimer(it) }

                // Flush remaining buffer
                synchronized(this) {
                    if (messageBuffer.isNotEmpty()) {
                        flushBuffer()
                    }
                }

                // Unregister from SessionHandler
                Monster.getSessionHandler()?.unregisterMessageListener(subscriptionId)
                pendingBulks.clear()
                logger.fine { "GraphQL bulk subscription cancelled (id: $subscriptionId)" }
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