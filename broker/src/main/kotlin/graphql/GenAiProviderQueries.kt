package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Features
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

class GenAiProviderQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore,
    private val config: JsonObject = JsonObject()
) {
    private val logger: Logger = Utils.getLogger(GenAiProviderQueries::class.java)

    fun genAiProviders(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher { _ ->
            val future = CompletableFuture<List<Map<String, Any?>>>()
            if (!Monster.isFeatureEnabled(Features.GenAi))
                return@DataFetcher future.apply { complete(emptyList()) }
            deviceStore.getAllDevices().onComplete { result ->
                val dbProviders = if (result.succeeded()) {
                    result.result()
                        .filter { it.type == DeviceConfig.DEVICE_TYPE_GENAI_PROVIDER }
                        .map { providerToMap(it) }
                } else {
                    logger.severe("Error fetching GenAI providers: ${result.cause()?.message}")
                    emptyList()
                }
                // Config.yaml providers appear first (system-level), DB providers after
                future.complete(getConfigProviders() + dbProviders)
            }
            future
        }
    }

    fun genAiProvider(): DataFetcher<CompletableFuture<Map<String, Any?>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>?>()
            if (!Monster.isFeatureEnabled(Features.GenAi))
                return@DataFetcher future.apply { complete(null) }
            val name = env.getArgument<String>("name")!!
            deviceStore.getDevice(name).onComplete { result ->
                if (result.succeeded() && result.result() != null &&
                    result.result()!!.type == DeviceConfig.DEVICE_TYPE_GENAI_PROVIDER) {
                    future.complete(providerToMap(result.result()!!))
                } else {
                    // Fall back to config.yaml providers
                    val configProvider = getConfigProviders().find { it["name"] == name }
                    future.complete(configProvider)
                }
            }
            future
        }
    }

    private fun getConfigProviders(): List<Map<String, Any?>> {
        val providers = config.getJsonObject("GenAI")?.getJsonObject("Providers") ?: return emptyList()
        val keyToType = mapOf(
            "AzureOpenAI" to "azure-openai",
            "Gemini"      to "gemini",
            "Claude"      to "claude",
            "OpenAI"      to "openai",
            "Ollama"      to "ollama"
        )
        return providers.fieldNames().mapNotNull { key ->
            val type = keyToType[key] ?: return@mapNotNull null
            val section = providers.getJsonObject(key, JsonObject())
            mapOf(
                "name"           to key,
                "type"           to type,
                "model"          to section.getString("Model"),
                "apiKey"         to if (!section.getString("ApiKey").isNullOrBlank()) "***" else null,
                "endpoint"       to section.getString("Endpoint"),
                "serviceVersion" to section.getString("ServiceVersion"),
                "baseUrl"        to section.getString("BaseUrl"),
                "temperature"    to (section.getDouble("Temperature") ?: 0.7),
                "maxTokens"      to section.getInteger("MaxTokens"),
                "enabled"        to true,
                "source"         to "config",
                "createdAt"      to Instant.EPOCH.toString(),
                "updatedAt"      to Instant.EPOCH.toString()
            )
        }
    }

    companion object {
        fun providerToMap(device: DeviceConfig): Map<String, Any?> {
            val cfg = GenAiProviderConfig.fromJsonObject(device.config)
            return mapOf(
                "name"           to device.name,
                "type"           to cfg.type,
                "model"          to cfg.model,
                "apiKey"         to if (!cfg.apiKey.isNullOrBlank()) "***" else null,
                "endpoint"       to cfg.endpoint,
                "serviceVersion" to cfg.serviceVersion,
                "baseUrl"        to cfg.baseUrl,
                "temperature"    to cfg.temperature,
                "maxTokens"      to cfg.maxTokens,
                "enabled"        to device.enabled,
                "source"         to "database",
                "createdAt"      to device.createdAt.toString(),
                "updatedAt"      to device.updatedAt.toString()
            )
        }
    }
}
