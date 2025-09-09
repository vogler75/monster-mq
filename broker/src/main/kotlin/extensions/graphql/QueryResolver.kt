package at.rocworks.extensions.graphql

import at.rocworks.data.MqttMessage
import at.rocworks.stores.ArchiveGroup
import at.rocworks.stores.IMessageArchiveExtended
import at.rocworks.stores.IMessageStore
import graphql.schema.DataFetcher
import graphql.schema.DataFetchingEnvironment
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

class QueryResolver(
    private val vertx: Vertx,
    private val retainedStore: IMessageStore?,
    private val archiveGroups: Map<String, ArchiveGroup>
) {
    companion object {
        private val logger: Logger = Logger.getLogger(QueryResolver::class.java.name)
    }

    fun currentValue(): DataFetcher<CompletableFuture<TopicValue?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<TopicValue?>()
            
            val topic = env.getArgument<String>("topic") ?: return@DataFetcher future.apply { complete(null) }
            val format = env.getArgument<DataFormat>("format") ?: DataFormat.JSON
            val archiveGroupName = env.getArgument<String>("archiveGroup") ?: "Default"

            // Find the specified archive group with a LastValueStore
            val lastValueStore = archiveGroups[archiveGroupName]?.lastValStore

            if (lastValueStore == null) {
                logger.warning("No LastValueStore configured for archive group '$archiveGroupName'")
                future.complete(null)
                return@DataFetcher future
            }

            // Handle SQLite stores asynchronously
            if (lastValueStore.getType().name == "SQLITE") {
                handleSQLiteCurrentValue(lastValueStore, topic, format, future)
            } else {
                // Try direct get first (more efficient for exact matches)
                val directMessage = lastValueStore.get(topic)
                if (directMessage != null) {
                    val (payload, actualFormat) = PayloadConverter.autoDetectAndEncode(
                        directMessage.payload, 
                        format
                    )
                    future.complete(
                        TopicValue(
                            topic = directMessage.topicName,
                            payload = payload,
                            format = actualFormat,
                            timestamp = directMessage.time.toEpochMilli(),
                            qos = directMessage.qosLevel
                        )
                    )
                } else {
                // Fallback to findMatchingMessages for wildcard-capable stores
                var found = false
                lastValueStore.findMatchingMessages(topic) { message ->
                    if (!found && message.topicName == topic) {
                        found = true
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
                }
            }

            future
        }
    }

    fun currentValues(): DataFetcher<CompletableFuture<List<TopicValue>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<TopicValue>>()
            
            val topicFilter = env.getArgument<String>("topicFilter") ?: "#"
            val format = env.getArgument<DataFormat>("format") ?: DataFormat.JSON
            val limit = env.getArgument<Int>("limit") ?: 100
            val archiveGroupName = env.getArgument<String>("archiveGroup") ?: "Default"

            // Find the specified archive group with a LastValueStore
            val lastValueStore = archiveGroups[archiveGroupName]?.lastValStore

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
            val startTimeStr = env.getArgument<String>("startTime")
            val endTimeStr = env.getArgument<String>("endTime")
            val format = env.getArgument<DataFormat>("format") ?: DataFormat.JSON
            val limit = env.getArgument<Int>("limit") ?: 100
            val archiveGroupName = env.getArgument<String>("archiveGroup") ?: "Default"

            // Find the specified archive group with an extended archive store
            val archiveStore = archiveGroups[archiveGroupName]?.archiveStore as? IMessageArchiveExtended

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

    fun searchTopics(): DataFetcher<CompletableFuture<List<String>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<String>>()
            
            val pattern = env.getArgument<String>("pattern") ?: return@DataFetcher future.apply { complete(emptyList()) }
            val limit = env.getArgument<Int>("limit") ?: 100
            val archiveGroupName = env.getArgument<String>("archiveGroup") ?: "Default"

            // Find the specified archive group with a LastValueStore
            val lastValueStore = archiveGroups[archiveGroupName]?.lastValStore

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

    /**
     * Handle SQLite currentValue queries asynchronously to avoid blocking
     */
    private fun handleSQLiteCurrentValue(
        lastValueStore: IMessageStore,
        topic: String, 
        format: DataFormat,
        future: CompletableFuture<TopicValue?>
    ) {
        // Cast to SQLite store to access async methods
        val sqliteStore = lastValueStore as at.rocworks.stores.sqlite.MessageStoreSQLite
        
        // Use the new async getAsync method
        sqliteStore.getAsync(topic) { message ->
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
    }
}