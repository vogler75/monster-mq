package at.rocworks.extensions.graphql

import at.rocworks.bus.IMessageBus
import at.rocworks.data.MqttMessage
import at.rocworks.stores.ISessionStoreAsync
import graphql.schema.DataFetcher
import graphql.GraphQLException
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

class MutationResolver(
    private val vertx: Vertx,
    private val messageBus: IMessageBus,
    private val messageHandler: at.rocworks.handlers.MessageHandler,
    private val sessionStore: ISessionStoreAsync,
    private val sessionHandler: at.rocworks.handlers.SessionHandler,
    private val authContext: GraphQLAuthContext
) {
    companion object {
        private val logger: Logger = Logger.getLogger(MutationResolver::class.java.name)
        private const val GRAPHQL_CLIENT_ID = "graphql-api"
    }

    fun publish(): DataFetcher<CompletableFuture<PublishResult>> {
        return DataFetcher { env ->
            val future = CompletableFuture<PublishResult>()
            
            val input = env.getArgument<Map<String, Any>>("input") ?: throw IllegalArgumentException("Input is required")
            val topic = input["topic"] as? String ?: throw IllegalArgumentException("Topic is required")
            
            // Get auth context from thread-local service (may be null for anonymous users)
            val userAuthContext: AuthContext? = AuthContextService.getAuthContext()
            
            // Check publish permission for this specific topic
            if (!authContext.canPublishToTopic(userAuthContext, topic)) {
                future.complete(
                    PublishResult(
                        success = false,
                        topic = topic,
                        timestamp = System.currentTimeMillis(),
                        error = "No publish permission for topic: $topic"
                    )
                )
                return@DataFetcher future
            }
            val payloadStr = input["payload"] as? String ?: throw IllegalArgumentException("Payload is required")
            val format = DataFormat.valueOf(input["format"]?.toString() ?: "JSON")
            val qos = (input["qos"] as? Int) ?: 0
            val retained = (input["retained"] as? Boolean) ?: false

            try {
                // Decode payload based on format
                val payloadBytes = PayloadConverter.decode(payloadStr, format)
                
                // Create MQTT message
                val message = MqttMessage(
                    messageUuid = at.rocworks.Utils.getUuid(),
                    messageId = 0,
                    topicName = topic,
                    payload = payloadBytes,
                    qosLevel = qos,
                    isRetain = retained,
                    isDup = false,
                    isQueued = false,
                    clientId = GRAPHQL_CLIENT_ID
                )

                // Publish message through message bus
                messageBus.publishMessageToBus(message)
                // Save message to LastValueStore and Archive (just like MQTT does)
                messageHandler.saveMessage(message)
                future.complete(
                    PublishResult(
                        success = true,
                        topic = topic,
                        timestamp = message.time.toEpochMilli()
                    )
                )
            } catch (e: Exception) {
                logger.severe("Error processing publish request: ${e.message}")
                future.complete(
                    PublishResult(
                        success = false,
                        topic = topic,
                        timestamp = System.currentTimeMillis(),
                        error = e.message
                    )
                )
            }

            future
        }
    }

    fun publishBatch(): DataFetcher<CompletableFuture<List<PublishResult>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<PublishResult>>()
            
            val inputs = env.getArgument<List<Map<String, Any>>>("inputs") ?: emptyList()
            
            // Get auth context from thread-local service (may be null for anonymous users)
            val userAuthContext: AuthContext? = AuthContextService.getAuthContext()
            val results = mutableListOf<PublishResult>()
            val futures = mutableListOf<CompletableFuture<PublishResult>>()

            inputs.forEach { input ->
                val topic = input["topic"] as? String ?: ""
                val payloadStr = input["payload"] as? String ?: ""
                val format = DataFormat.valueOf(input["format"]?.toString() ?: "JSON")
                val qos = (input["qos"] as? Int) ?: 0
                val retained = (input["retained"] as? Boolean) ?: false

                val itemFuture = CompletableFuture<PublishResult>()
                futures.add(itemFuture)

                // Check publish permission for this specific topic
                if (!authContext.canPublishToTopic(userAuthContext, topic)) {
                    itemFuture.complete(
                        PublishResult(
                            success = false,
                            topic = topic,
                            timestamp = System.currentTimeMillis(),
                            error = "No publish permission for topic: $topic"
                        )
                    )
                    return@forEach
                }

                try {
                    // Decode payload based on format
                    val payloadBytes = PayloadConverter.decode(payloadStr, format)
                    
                    // Create MQTT message
                    val message = MqttMessage(
                        messageUuid = at.rocworks.Utils.getUuid(),
                        messageId = 0,
                        topicName = topic,
                        payload = payloadBytes,
                        qosLevel = qos,
                        isRetain = retained,
                        isDup = false,
                        isQueued = false,
                        clientId = GRAPHQL_CLIENT_ID
                    )

                    // Publish message through message bus
                    messageBus.publishMessageToBus(message)
                    // Save message to LastValueStore and Archive (just like MQTT does)
                    messageHandler.saveMessage(message)
                    itemFuture.complete(
                        PublishResult(
                            success = true,
                            topic = topic,
                            timestamp = message.time.toEpochMilli()
                        )
                    )
                } catch (e: Exception) {
                    logger.severe("Error processing publish request for $topic: ${e.message}")
                    itemFuture.complete(
                        PublishResult(
                            success = false,
                            topic = topic,
                            timestamp = System.currentTimeMillis(),
                            error = e.message
                        )
                    )
                }
            }

            // Wait for all publishes to complete
            CompletableFuture.allOf(*futures.toTypedArray()).thenApply {
                futures.forEach { f ->
                    results.add(f.get())
                }
                future.complete(results)
            }

            future
        }
    }

    fun purgeQueuedMessages(): DataFetcher<CompletableFuture<PurgeResult>> {
        return DataFetcher { env ->
            val future = CompletableFuture<PurgeResult>()
            val clientId = env.getArgument<String?>("clientId")

            // Check authorization - requires admin privileges
            val authResult = authContext.validateFieldAccess(env)
            if (!authResult.allowed) {
                future.complete(
                    PurgeResult(
                        success = false,
                        message = authResult.errorMessage ?: "Admin privileges required",
                        deletedCount = 0L
                    )
                )
                return@DataFetcher future
            }

            try {
                if (clientId != null) {
                    // Purge queued messages for specific client using delClient
                    // Get count before deleting
                    val deletedCount = sessionStore.sync.countQueuedMessagesForClient(clientId)
                    sessionStore.sync.delClient(clientId) { subscription ->
                        // We don't need the subscription callback but it's required
                    }

                    future.complete(
                        PurgeResult(
                            success = true,
                            message = "Successfully purged queued messages for client: $clientId",
                            deletedCount = deletedCount
                        )
                    )
                } else {
                    // Purge all queued messages using existing purgeQueuedMessages function
                    val countBefore = sessionStore.sync.countQueuedMessages()
                    sessionStore.sync.purgeQueuedMessages()

                    future.complete(
                        PurgeResult(
                            success = true,
                            message = "Successfully purged all queued messages",
                            deletedCount = countBefore
                        )
                    )
                }
            } catch (e: Exception) {
                logger.severe("Error purging queued messages: ${e.message}")
                future.complete(
                    PurgeResult(
                        success = false,
                        message = "Error: ${e.message}",
                        deletedCount = 0L
                    )
                )
            }

            future
        }
    }
}