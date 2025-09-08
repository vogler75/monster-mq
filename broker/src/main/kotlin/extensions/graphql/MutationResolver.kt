package at.rocworks.extensions.graphql

import at.rocworks.bus.IMessageBus
import at.rocworks.data.MqttMessage
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

class MutationResolver(
    private val vertx: Vertx,
    private val messageBus: IMessageBus
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
}