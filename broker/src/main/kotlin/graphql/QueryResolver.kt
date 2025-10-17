package at.rocworks.extensions.graphql

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.handlers.ArchiveGroup
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.stores.IMessageArchiveExtended
import at.rocworks.stores.IMessageStore
import graphql.schema.DataFetcher
import graphql.GraphQLException
import io.vertx.core.Vertx
import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * Topic data class for GraphQL browseTopics query
 */
data class Topic(
    val name: String
)

class QueryResolver(
    private val vertx: Vertx,
    private val retainedStore: IMessageStore?,
    private val archiveHandler: ArchiveHandler?,
    private val authContext: GraphQLAuthContext
) {
    companion object {
        private val logger: Logger = Utils.getLogger(QueryResolver::class.java)
    }

    private fun getCurrentArchiveGroups(): Map<String, ArchiveGroup> {
        return archiveHandler?.getDeployedArchiveGroups() ?: emptyMap()
    }

    fun currentValue(): DataFetcher<CompletableFuture<TopicValue?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<TopicValue?>()
            
            val topic = env.getArgument<String>("topic") ?: return@DataFetcher future.apply { complete(null) }
            
            // Get auth context from thread-local service (may be null for anonymous users)
            val userAuthContext: AuthContext? = AuthContextService.getAuthContext()
            
            // Check subscribe permission for this specific topic (works with anonymous users)
            if (!authContext.canSubscribeToTopic(userAuthContext, topic)) {
                future.completeExceptionally(GraphQLException("No subscribe permission for topic: $topic"))
                return@DataFetcher future
            }
            val format = env.getArgument<DataFormat>("format") ?: DataFormat.JSON
            val archiveGroupName = env.getArgument<String>("archiveGroup") ?: "Default"

            // Find the specified archive group with a LastValueStore
            val lastValueStore = getCurrentArchiveGroups()[archiveGroupName]?.lastValStore

            if (lastValueStore == null) {
                logger.warning("No LastValueStore configured for archive group '$archiveGroupName'")
                future.complete(null)
                return@DataFetcher future
            }

            // Use getAsync for all stores (now available in all implementations)
            lastValueStore.getAsync(topic) { message ->
                if (message != null) {
                    val (payload, actualFormat) = PayloadConverter.autoDetectAndEncode(
                        message.payload, 
                        format
                    )
                    future.complete(
                        TopicValue(
                            topic = message.topicName,
                            payload = payload,
                            format = actualFormat,
                            timestamp = message.time.toEpochMilli(),
                            qos = message.qosLevel
                        )
                    )
                } else {
                    future.complete(null)
                }
            }

            future
        }
    }

    fun currentValues(): DataFetcher<CompletableFuture<List<TopicValue>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<TopicValue>>()
            
            val topicFilter = env.getArgument<String>("topicFilter") ?: "#"
            
            // Get auth context from thread-local service (may be null for anonymous users)
            val userAuthContext: AuthContext? = AuthContextService.getAuthContext()
            
            // Check subscribe permission for the topic filter (works with anonymous users)
            if (!authContext.canSubscribeToTopic(userAuthContext, topicFilter)) {
                future.completeExceptionally(GraphQLException("No subscribe permission for topic filter: $topicFilter"))
                return@DataFetcher future
            }
            val format = env.getArgument<DataFormat>("format") ?: DataFormat.JSON
            val limit = env.getArgument<Int>("limit") ?: 100
            val archiveGroupName = env.getArgument<String>("archiveGroup") ?: "Default"

            // Find the specified archive group with a LastValueStore
            val lastValueStore = getCurrentArchiveGroups()[archiveGroupName]?.lastValStore

            if (lastValueStore == null) {
                logger.warning("No LastValueStore configured for archive group '$archiveGroupName'")
                future.complete(emptyList())
                return@DataFetcher future
            }

            val values = mutableListOf<TopicValue>()
            var count = 0
            var completed = false
            lastValueStore.findMatchingMessages(topicFilter) { message ->
                if (!completed && count < limit) {
                    val (payload, actualFormat) = PayloadConverter.autoDetectAndEncode(
                        message.payload,
                        format
                    )
                    values.add(
                        TopicValue(
                            topic = message.topicName,
                            payload = payload,
                            format = actualFormat,
                            timestamp = message.time.toEpochMilli(),
                            qos = message.qosLevel
                        )
                    )
                    count++
                    if (count >= limit) {
                        completed = true
                        future.complete(values)
                        false // stop processing
                    } else {
                        true // continue processing
                    }
                } else {
                    false // stop processing
                }
            }
            
            // Complete with results after async search finishes
            vertx.setTimer(100) {
                if (!completed) {
                    completed = true
                    future.complete(values)
                }
            }

            future
        }
    }

    fun retainedMessage(): DataFetcher<CompletableFuture<RetainedMessage?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<RetainedMessage?>()
            
            val topic = env.getArgument<String>("topic") ?: return@DataFetcher future.apply { complete(null) }
            // Get auth context from thread-local service
            val userAuthContext: AuthContext? = AuthContextService.getAuthContext()
            
            // Check subscribe permission for this specific topic
            if (!authContext.canSubscribeToTopic(userAuthContext, topic)) {
                future.completeExceptionally(GraphQLException("No subscribe permission for topic: $topic"))
                return@DataFetcher future
            }
            val format = env.getArgument<DataFormat>("format") ?: DataFormat.JSON

            if (retainedStore == null) {
                logger.warning("No RetainedStore configured")
                future.complete(null)
                return@DataFetcher future
            }

            // Use findMatchingMessages to get the exact topic (works async)
            var found = false
            retainedStore.findMatchingMessages(topic) { message ->
                if (!found && message.topicName == topic) {
                    found = true
                    val (payload, actualFormat) = PayloadConverter.autoDetectAndEncode(
                        message.payload,
                        format
                    )
                    future.complete(
                        RetainedMessage(
                            topic = message.topicName,
                            payload = payload,
                            format = actualFormat,
                            timestamp = message.time.toEpochMilli(),
                            qos = message.qosLevel
                        )
                    )
                    true // stop processing
                } else {
                    false // continue searching
                }
            }
            
            // If not found after async search, complete with null
            vertx.setTimer(100) {
                if (!found) {
                    future.complete(null)
                }
            }

            future
        }
    }

    fun retainedMessages(): DataFetcher<CompletableFuture<List<RetainedMessage>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<RetainedMessage>>()
            
            val topicFilter = env.getArgument<String>("topicFilter") ?: "#"
            // Get auth context from thread-local service
            val userAuthContext: AuthContext? = AuthContextService.getAuthContext()
            
            // Check subscribe permission for the topic filter
            if (!authContext.canSubscribeToTopic(userAuthContext, topicFilter)) {
                future.completeExceptionally(GraphQLException("No subscribe permission for topic filter: $topicFilter"))
                return@DataFetcher future
            }
            val format = env.getArgument<DataFormat>("format") ?: DataFormat.JSON
            val limit = env.getArgument<Int>("limit") ?: 100

            if (retainedStore == null) {
                logger.warning("No RetainedStore configured")
                future.complete(emptyList())
                return@DataFetcher future
            }

            val retained = mutableListOf<RetainedMessage>()
            var count = 0
            var completed = false
            retainedStore.findMatchingMessages(topicFilter) { message ->
                if (!completed && count < limit) {
                    val (payload, actualFormat) = PayloadConverter.autoDetectAndEncode(
                        message.payload,
                        format
                    )
                    retained.add(
                        RetainedMessage(
                            topic = message.topicName,
                            payload = payload,
                            format = actualFormat,
                            timestamp = message.time.toEpochMilli(),
                            qos = message.qosLevel
                        )
                    )
                    count++
                    if (count >= limit) {
                        completed = true
                        future.complete(retained)
                        false // stop processing
                    } else {
                        true // continue processing
                    }
                } else {
                    false // stop processing
                }
            }
            
            // Complete with results after async search finishes
            vertx.setTimer(100) {
                if (!completed) {
                    completed = true
                    future.complete(retained)
                }
            }

            future
        }
    }

    fun archivedMessages(): DataFetcher<CompletableFuture<List<ArchivedMessage>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<ArchivedMessage>>()
            
            val topicFilter = env.getArgument<String>("topicFilter") ?: "#"
            // Get auth context from thread-local service
            val userAuthContext: AuthContext? = AuthContextService.getAuthContext()
            
            // Check subscribe permission for the topic filter
            if (!authContext.canSubscribeToTopic(userAuthContext, topicFilter)) {
                future.completeExceptionally(GraphQLException("No subscribe permission for topic filter: $topicFilter"))
                return@DataFetcher future
            }
            val startTimeStr = env.getArgument<String>("startTime")
            val endTimeStr = env.getArgument<String>("endTime")
            val format = env.getArgument<DataFormat>("format") ?: DataFormat.JSON
            val limit = env.getArgument<Int>("limit") ?: 100
            val archiveGroupName = env.getArgument<String>("archiveGroup") ?: "Default"

            // Find the specified archive group with an extended archive store
            val archiveStore = getCurrentArchiveGroups()[archiveGroupName]?.archiveStore as? IMessageArchiveExtended

            if (archiveStore == null) {
                logger.warning("No extended archive store configured for archive group '$archiveGroupName'")
                future.complete(emptyList())
                return@DataFetcher future
            }

            // Validate ISO datetime strings but keep original format
            startTimeStr?.let { 
                try {
                    Instant.parse(it) // Just validate, don't store the result
                } catch (e: DateTimeParseException) {
                    logger.warning("Invalid startTime format: $it, expected ISO-8601 format (e.g., 2024-01-01T00:00:00.000Z)")
                    return@DataFetcher future.apply { 
                        completeExceptionally(IllegalArgumentException("Invalid startTime format: expected ISO-8601 (e.g., 2024-01-01T00:00:00.000Z)")) 
                    }
                }
            }
            endTimeStr?.let { 
                try {
                    Instant.parse(it) // Just validate, don't store the result
                } catch (e: DateTimeParseException) {
                    logger.warning("Invalid endTime format: $it, expected ISO-8601 format (e.g., 2024-01-01T00:00:00.000Z)")
                    return@DataFetcher future.apply { 
                        completeExceptionally(IllegalArgumentException("Invalid endTime format: expected ISO-8601 (e.g., 2024-01-01T00:00:00.000Z)")) 
                    }
                }
            }
            
            val jsonArray = archiveStore.getHistory(
                topic = topicFilter,
                startTime = startTimeStr?.let { Instant.parse(it) },
                endTime = endTimeStr?.let { Instant.parse(it) },
                limit = limit
            )
            
            val archived = mutableListOf<ArchivedMessage>()
            for (i in 0 until jsonArray.size()) {
                val obj = jsonArray.getJsonObject(i)
                val payloadBytes = when {
                    format == DataFormat.JSON && obj.containsKey("payload_json") && obj.getString("payload_json") != null -> {
                        // For JSON format, prefer the payload_json column if available
                        obj.getString("payload_json").toByteArray(Charsets.UTF_8)
                    }
                    obj.containsKey("payload_base64") -> {
                        java.util.Base64.getDecoder().decode(obj.getString("payload_base64"))
                    }
                    else -> {
                        obj.getString("payload", "").toByteArray()
                    }
                }
                
                val (payload, actualFormat) = PayloadConverter.autoDetectAndEncode(
                    payloadBytes,
                    format
                )
                
                archived.add(
                    ArchivedMessage(
                        topic = obj.getString("topic") ?: "",
                        payload = payload,
                        format = actualFormat,
                        timestamp = obj.getLong("timestamp", 0L),
                        qos = obj.getInteger("qos", 0),
                        clientId = obj.getString("client_id")
                    )
                )
            }
            future.complete(archived)

            future
        }
    }

    fun systemLogs(): DataFetcher<CompletableFuture<List<SystemLogEntry>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<SystemLogEntry>>()
            
            val archiveGroupName = env.getArgument<String>("archiveGroup") ?: "Syslogs"
            val limit = env.getArgument<Int>("limit") ?: 100
            val orderByTime = env.getArgument<String>("orderByTime") ?: "DESC"
            val lastMinutes = env.getArgument<Int?>("lastMinutes")
            
            // Get time parameters
            var startTimeStr = env.getArgument<String?>("startTime")
            var endTimeStr = env.getArgument<String?>("endTime")
            
            // Handle lastMinutes option
            if (lastMinutes != null) {
                val now = Instant.now()
                endTimeStr = endTimeStr ?: now.toString()
                
                // Calculate startTime based on endTime and lastMinutes
                val endTime = try {
                    Instant.parse(endTimeStr)
                } catch (e: DateTimeParseException) {
                    logger.warning("Invalid endTime format: $endTimeStr, expected ISO-8601 format")
                    return@DataFetcher future.apply { 
                        completeExceptionally(IllegalArgumentException("Invalid endTime format: expected ISO-8601 (e.g., 2024-01-01T00:00:00.000Z)")) 
                    }
                }
                
                startTimeStr = endTime.minusSeconds(lastMinutes.toLong() * 60).toString()
            } else {
                // If no lastMinutes, use endTime or default to now
                if (endTimeStr == null) {
                    endTimeStr = Instant.now().toString()
                }
            }
            
            // Get optional filters
            val nodeFilter = env.getArgument<String?>("node")
            val levelFilter = env.getArgument<String?>("level")
            val loggerFilter = env.getArgument<String?>("logger")
            val sourceClassFilter = env.getArgument<String?>("sourceClass")
            val sourceMethodFilter = env.getArgument<String?>("sourceMethod")
            val messageFilter = env.getArgument<String?>("message")

            // Find the specified archive group with an extended archive store
            val archiveStore = getCurrentArchiveGroups()[archiveGroupName]?.archiveStore as? IMessageArchiveExtended

            if (archiveStore == null) {
                logger.warning("No extended archive store configured for archive group '$archiveGroupName'")
                future.complete(emptyList())
                return@DataFetcher future
            }

            // Validate ISO datetime strings
            startTimeStr?.let { 
                try {
                    Instant.parse(it)
                } catch (e: DateTimeParseException) {
                    logger.warning("Invalid startTime format: $it, expected ISO-8601 format")
                    return@DataFetcher future.apply { 
                        completeExceptionally(IllegalArgumentException("Invalid startTime format: expected ISO-8601 (e.g., 2024-01-01T00:00:00.000Z)")) 
                    }
                }
            }
            endTimeStr?.let { 
                try {
                    Instant.parse(it)
                } catch (e: DateTimeParseException) {
                    logger.warning("Invalid endTime format: $it, expected ISO-8601 format")
                    return@DataFetcher future.apply { 
                        completeExceptionally(IllegalArgumentException("Invalid endTime format: expected ISO-8601 (e.g., 2024-01-01T00:00:00.000Z)")) 
                    }
                }
            }
            
            // Query the archive store - use "syslogs/#" as topic filter to get all system logs
            val jsonArray = archiveStore.getHistory(
                topic = "${Const.SYS_TOPIC_NAME}/${Const.LOG_TOPIC_NAME}/%",
                startTime = startTimeStr?.let { Instant.parse(it) },
                endTime = endTimeStr.let { Instant.parse(it) },
                limit = limit
            )
            
            val logs = mutableListOf<SystemLogEntry>()
            for (i in 0 until jsonArray.size()) {
                val obj = jsonArray.getJsonObject(i)
                
                // Parse the JSON payload
                val payloadJson = when {
                    obj.containsKey("payload_json") && obj.getString("payload_json") != null -> {
                        obj.getString("payload_json")
                    }
                    obj.containsKey("payload_base64") -> {
                        val bytes = java.util.Base64.getDecoder().decode(obj.getString("payload_base64"))
                        String(bytes, Charsets.UTF_8)
                    }
                    else -> {
                        obj.getString("payload", "{}")
                    }
                }
                
                try {
                    val logData = io.vertx.core.json.JsonObject(payloadJson)
                    
                    // Apply filters
                    if (nodeFilter != null && logData.getString("node") != nodeFilter) continue
                    if (levelFilter != null && logData.getString("level") != levelFilter) continue
                    if (loggerFilter != null && !logData.getString("logger", "").matches(Regex(loggerFilter))) continue
                    if (sourceClassFilter != null && !logData.getString("sourceClass", "").matches(Regex(sourceClassFilter))) continue
                    if (sourceMethodFilter != null && !logData.getString("sourceMethod", "").matches(Regex(sourceMethodFilter))) continue
                    if (messageFilter != null && !logData.getString("message", "").matches(Regex(messageFilter))) continue
                    
                    // Parse exception if present
                    val exception = logData.getJsonObject("exception")?.let { exc ->
                        ExceptionInfo(
                            `class` = exc.getString("class") ?: "",
                            message = exc.getString("message"),
                            stackTrace = exc.getString("stackTrace") ?: ""
                        )
                    }
                    
                    // Parse parameters if present
                    val parameters = logData.getJsonArray("parameters")?.map { it.toString() }
                    
                    logs.add(
                        SystemLogEntry(
                            timestamp = logData.getString("timestamp") ?: "",
                            level = logData.getString("level") ?: "",
                            logger = logData.getString("logger") ?: "",
                            message = logData.getString("message") ?: "",
                            thread = logData.getLong("thread") ?: 0L,
                            node = logData.getString("node") ?: "",
                            sourceClass = logData.getString("sourceClass"),
                            sourceMethod = logData.getString("sourceMethod"),
                            parameters = parameters,
                            exception = exception
                        )
                    )
                } catch (e: Exception) {
                    logger.warning("Failed to parse log entry: ${e.message}")
                    // Skip malformed log entries
                }
            }
            
            // Sort by time if needed (database might return in different order)
            val sortedLogs = if (orderByTime == "ASC") {
                logs.sortedBy { it.timestamp }
            } else {
                logs.sortedByDescending { it.timestamp }
            }
            
            future.complete(sortedLogs)

            future
        }
    }

    fun searchTopics(): DataFetcher<CompletableFuture<List<String>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<String>>()
            
            // Get auth context from thread-local service (may be null for anonymous users)
            val userAuthContext: AuthContext? = AuthContextService.getAuthContext()
            
            // Check if user has global subscribe permission (as specified by the user)
            if (!authContext.hasGlobalSubscribePermission(userAuthContext)) {
                future.completeExceptionally(GraphQLException("No global subscribe permission required for searchTopics"))
                return@DataFetcher future
            }
            
            val pattern = env.getArgument<String>("pattern") ?: return@DataFetcher future.apply { complete(emptyList()) }
            val limit = env.getArgument<Int>("limit") ?: 100
            val archiveGroupName = env.getArgument<String>("archiveGroup") ?: "Default"

            // Find the specified archive group with a LastValueStore
            val lastValueStore = getCurrentArchiveGroups()[archiveGroupName]?.lastValStore

            if (lastValueStore == null) {
                logger.warning("No LastValueStore configured for archive group '$archiveGroupName'")
                future.complete(emptyList())
                return@DataFetcher future
            }

            val topicNames = mutableListOf<String>()
            var count = 0
            var completed = false
            
            // Convert pattern to topic filter format
            val topicFilter = if (pattern.contains("*")) {
                pattern.replace("*", "#")
            } else if (!pattern.contains("#") && !pattern.contains("+")) {
                "$pattern/#" // Search for topics starting with pattern
            } else {
                pattern
            }
            
            lastValueStore.findMatchingMessages(topicFilter) { message ->
                if (!completed && count < limit) {
                    val topicName = message.topicName
                    // Check if topic matches the search pattern
                    if (matchesSearchPattern(topicName, pattern)) {
                        if (!topicNames.contains(topicName)) { // Avoid duplicates
                            topicNames.add(topicName)
                            count++
                        }
                    }
                    
                    if (count >= limit) {
                        completed = true
                        future.complete(topicNames.sorted())
                        false // stop processing
                    } else {
                        true // continue processing
                    }
                } else {
                    false // stop processing
                }
            }
            
            // Complete with results after async search finishes
            vertx.setTimer(100) {
                if (!completed) {
                    completed = true
                    future.complete(topicNames.sorted())
                }
            }

            future
        }
    }
    
    /**
     * Check if a topic name matches a search pattern
     * Supports * as wildcard (converted to regex)
     */
    private fun matchesSearchPattern(topicName: String, pattern: String): Boolean {
        return if (pattern.contains("*")) {
            val regex = pattern.replace("*", ".*").toRegex(RegexOption.IGNORE_CASE)
            regex.matches(topicName)
        } else {
            topicName.contains(pattern, ignoreCase = true)
        }
    }

    fun browseTopics(): DataFetcher<CompletableFuture<List<Topic>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Topic>>()

            val topicPattern = env.getArgument<String>("topic") ?: "+"
            // Get auth context from thread-local service (may be null for anonymous users)
            val userAuthContext: AuthContext? = AuthContextService.getAuthContext()

            // Check subscribe permission for the topic pattern (works with anonymous users)
            if (!authContext.canSubscribeToTopic(userAuthContext, topicPattern)) {
                future.completeExceptionally(GraphQLException("No subscribe permission for topic pattern: $topicPattern"))
                return@DataFetcher future
            }

            val archiveGroupName = env.getArgument<String>("archiveGroup") ?: "Default"

            // Find the specified archive group with a LastValueStore
            val lastValueStore = getCurrentArchiveGroups()[archiveGroupName]?.lastValStore

            if (lastValueStore == null) {
                logger.warning("No LastValueStore configured for archive group '$archiveGroupName'")
                future.complete(emptyList())
                return@DataFetcher future
            }

            // Parse the topic pattern to determine browsing strategy
            val topicLevels = topicPattern.split("/")
            val hasWildcard = topicPattern.contains("+") || topicPattern.contains("#")

            if (!hasWildcard) {
                // Exact topic - just return it if it exists
                lastValueStore.getAsync(topicPattern) { message ->
                    if (message != null) {
                        future.complete(listOf(Topic(topicPattern)))
                    } else {
                        future.complete(emptyList())
                    }
                }
                return@DataFetcher future
            }

            // Use the efficient topic browsing method
            val topicNames = mutableSetOf<String>()
            var completed = false

            lastValueStore.findMatchingTopics(topicPattern) { topicName ->
                if (!completed) {
                    if (!topicNames.contains(topicName)) {
                        topicNames.add(topicName)
                    }
                    // Continue processing
                    true
                } else {
                    false // stop processing
                }
            }

            // Complete with results after async search finishes
            vertx.setTimer(50) { // Reduced timer since we're more efficient now
                if (!completed) {
                    completed = true
                    val topics = topicNames.sorted().map { Topic(it) }
                    future.complete(topics)
                }
            }

            future
        }
    }


    fun topicValue(): DataFetcher<CompletableFuture<TopicValue?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<TopicValue?>()
            val source = env.getSource<Topic?>()
            val format = env.getArgument<DataFormat>("format") ?: DataFormat.JSON
            val archiveGroupName = "Default" // Use default archive group for Topic field resolver

            if (source == null) {
                future.complete(null)
                return@DataFetcher future
            }

            // Get auth context from thread-local service (may be null for anonymous users)
            val userAuthContext: AuthContext? = AuthContextService.getAuthContext()

            // Check subscribe permission for this specific topic (works with anonymous users)
            if (!authContext.canSubscribeToTopic(userAuthContext, source.name)) {
                future.completeExceptionally(GraphQLException("No subscribe permission for topic: ${source.name}"))
                return@DataFetcher future
            }

            // Find the specified archive group with a LastValueStore
            val lastValueStore = getCurrentArchiveGroups()[archiveGroupName]?.lastValStore

            if (lastValueStore == null) {
                logger.warning("No LastValueStore configured for archive group '$archiveGroupName'")
                future.complete(null)
                return@DataFetcher future
            }

            // Use getAsync for all stores (now available in all implementations)
            lastValueStore.getAsync(source.name) { message ->
                if (message != null) {
                    val (payload, actualFormat) = PayloadConverter.autoDetectAndEncode(
                        message.payload,
                        format
                    )
                    future.complete(
                        TopicValue(
                            topic = message.topicName,
                            payload = payload,
                            format = actualFormat,
                            timestamp = message.time.toEpochMilli(),
                            qos = message.qosLevel
                        )
                    )
                } else {
                    future.complete(null)
                }
            }

            future
        }
    }

}