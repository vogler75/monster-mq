package at.rocworks.extensions.graphql

import at.rocworks.Utils
import at.rocworks.bus.IMessageBus
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.ISessionStoreAsync
import at.rocworks.stores.IDeviceConfigStore
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
    private val authContext: GraphQLAuthContext,
    private val deviceStore: IDeviceConfigStore?
) {
    companion object {
        private val logger: Logger = Utils.getLogger(MutationResolver::class.java)
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
                val message = BrokerMessage(
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

                // Publish message through session handler (uses targeted messaging)
                sessionHandler.publishMessage(message)
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
                    val message = BrokerMessage(
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

                    // Publish message through session handler (uses targeted messaging)
                    sessionHandler.publishMessage(message)
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
                    // Purge queued messages for specific client using async calls
                    sessionStore.countQueuedMessagesForClient(clientId).onComplete { countResult ->
                        if (countResult.succeeded()) {
                            val deletedCount = countResult.result()
                            sessionStore.delClient(clientId) { subscription ->
                                // We don't need the subscription callback but it's required
                            }.onComplete { delResult ->
                                if (delResult.succeeded()) {
                                    future.complete(
                                        PurgeResult(
                                            success = true,
                                            message = "Successfully purged queued messages for client: $clientId",
                                            deletedCount = deletedCount
                                        )
                                    )
                                } else {
                                    future.complete(
                                        PurgeResult(
                                            success = false,
                                            message = "Failed to purge messages for client $clientId: ${delResult.cause()?.message}",
                                            deletedCount = 0L
                                        )
                                    )
                                }
                            }
                        } else {
                            future.complete(
                                PurgeResult(
                                    success = false,
                                    message = "Failed to count messages for client $clientId: ${countResult.cause()?.message}",
                                    deletedCount = 0L
                                )
                            )
                        }
                    }
                } else {
                    // Purge all queued messages using async calls
                    sessionStore.countQueuedMessages().onComplete { countResult ->
                        if (countResult.succeeded()) {
                            val countBefore = countResult.result()
                            sessionStore.purgeQueuedMessages().onComplete { purgeResult ->
                                if (purgeResult.succeeded()) {
                                    future.complete(
                                        PurgeResult(
                                            success = true,
                                            message = "Successfully purged all queued messages",
                                            deletedCount = countBefore
                                        )
                                    )
                                } else {
                                    future.complete(
                                        PurgeResult(
                                            success = false,
                                            message = "Failed to purge messages: ${purgeResult.cause()?.message}",
                                            deletedCount = 0L
                                        )
                                    )
                                }
                            }
                        } else {
                            future.complete(
                                PurgeResult(
                                    success = false,
                                    message = "Failed to count messages: ${countResult.cause()?.message}",
                                    deletedCount = 0L
                                )
                            )
                        }
                    }
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

    fun importDevices(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()

            if (deviceStore == null) {
                future.completeExceptionally(GraphQLException("Device store is not available"))
                return@DataFetcher future
            }

            val configs = env.getArgument<List<Map<String, Any?>>?>("configs")
            if (configs.isNullOrEmpty()) {
                future.completeExceptionally(GraphQLException("Configs list is required and cannot be empty"))
                return@DataFetcher future
            }

            // Force enabled=false for all imported devices (safety: prevent accidental activation)
            val configsWithDisabled = configs.map { config ->
                config.toMutableMap().apply { put("enabled", false) }
            }

            deviceStore.importConfigs(configsWithDisabled).onComplete { result ->
                if (result.succeeded()) {
                    val importResult = result.result()
                    val resultMap = mapOf(
                        "success" to (importResult.imported > 0),
                        "imported" to importResult.imported,
                        "failed" to importResult.failed,
                        "total" to configs.size,
                        "errors" to importResult.errors
                    )
                    future.complete(resultMap)
                } else {
                    logger.severe("Failed to import devices: ${result.cause()?.message}")
                    future.completeExceptionally(GraphQLException("Failed to import devices: ${result.cause()?.message}"))
                }
            }

            future
        }
    }
}