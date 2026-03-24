package at.rocworks.graphql

import at.rocworks.Utils
import at.rocworks.agents.GenAiProviderConfig
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

class GenAiProviderMutations(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(GenAiProviderMutations::class.java)

    fun createProvider(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            try {
                val name = env.getArgument<String>("name")!!
                if (!name.matches(Regex("^[a-zA-Z0-9_-]+$"))) {
                    future.completeExceptionally(RuntimeException(
                        "Invalid provider name '$name': only letters, digits, underscores, and hyphens are allowed"
                    ))
                    return@DataFetcher future
                }
                val input = env.getArgument<Map<String, Any>>("input")!!
                val deviceConfig = DeviceConfig(
                    name = name,
                    namespace = "genai",
                    nodeId = "*",
                    config = inputToProviderConfig(input).toJsonObject(),
                    enabled = input["enabled"] as? Boolean ?: true,
                    type = DeviceConfig.DEVICE_TYPE_GENAI_PROVIDER,
                    createdAt = Instant.now(),
                    updatedAt = Instant.now()
                )
                deviceStore.saveDevice(deviceConfig).onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(GenAiProviderQueries.providerToMap(result.result()))
                    } else {
                        future.completeExceptionally(result.cause())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error creating GenAI provider: ${e.message}")
                future.completeExceptionally(e)
            }
            future
        }
    }

    fun updateProvider(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            try {
                val name = env.getArgument<String>("name")!!
                val input = env.getArgument<Map<String, Any>>("input")!!
                deviceStore.getDevice(name).onComplete { getResult ->
                    if (getResult.succeeded() && getResult.result() != null &&
                        getResult.result()!!.type == DeviceConfig.DEVICE_TYPE_GENAI_PROVIDER) {
                        val existing = getResult.result()!!
                        val existingCfg = GenAiProviderConfig.fromJsonObject(existing.config)
                        val updatedCfg = GenAiProviderConfig(
                            type           = input["type"] as? String ?: existingCfg.type,
                            model          = if (input.containsKey("model")) input["model"] as? String else existingCfg.model,
                            apiKey         = if (input.containsKey("apiKey")) input["apiKey"] as? String else existingCfg.apiKey,
                            endpoint       = if (input.containsKey("endpoint")) input["endpoint"] as? String else existingCfg.endpoint,
                            serviceVersion = if (input.containsKey("serviceVersion")) input["serviceVersion"] as? String else existingCfg.serviceVersion,
                            baseUrl        = if (input.containsKey("baseUrl")) input["baseUrl"] as? String else existingCfg.baseUrl,
                            temperature    = (input["temperature"] as? Number)?.toDouble() ?: existingCfg.temperature,
                            maxTokens      = if (input.containsKey("maxTokens")) (input["maxTokens"] as? Number)?.toInt() else existingCfg.maxTokens
                        )
                        val updated = existing.copy(
                            config    = updatedCfg.toJsonObject(),
                            enabled   = input["enabled"] as? Boolean ?: existing.enabled,
                            updatedAt = Instant.now()
                        )
                        deviceStore.saveDevice(updated).onComplete { saveResult ->
                            if (saveResult.succeeded()) {
                                future.complete(GenAiProviderQueries.providerToMap(saveResult.result()))
                            } else {
                                future.completeExceptionally(saveResult.cause())
                            }
                        }
                    } else {
                        future.completeExceptionally(RuntimeException("GenAI provider not found: $name"))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error updating GenAI provider: ${e.message}")
                future.completeExceptionally(e)
            }
            future
        }
    }

    fun deleteProvider(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Boolean>()
            try {
                val name = env.getArgument<String>("name")!!
                deviceStore.deleteDevice(name).onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(result.result())
                    } else {
                        future.completeExceptionally(result.cause())
                    }
                }
            } catch (e: Exception) {
                future.completeExceptionally(e)
            }
            future
        }
    }

    private fun inputToProviderConfig(input: Map<String, Any>): GenAiProviderConfig {
        return GenAiProviderConfig(
            type           = input["type"] as? String ?: "gemini",
            model          = input["model"] as? String,
            apiKey         = input["apiKey"] as? String,
            endpoint       = input["endpoint"] as? String,
            serviceVersion = input["serviceVersion"] as? String,
            baseUrl        = input["baseUrl"] as? String,
            temperature    = (input["temperature"] as? Number)?.toDouble() ?: 0.7,
            maxTokens      = (input["maxTokens"] as? Number)?.toInt()
        )
    }
}
