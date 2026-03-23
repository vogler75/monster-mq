package at.rocworks.graphql

import at.rocworks.Utils
import at.rocworks.schema.JsonSchemaValidator
import at.rocworks.schema.TopicSchemaPolicyCache
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import graphql.schema.DataFetcher
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL query resolvers for Topic Schema Governance
 */
class TopicSchemaQueries(
    private val deviceStore: IDeviceConfigStore,
    private val policyCache: TopicSchemaPolicyCache
) {
    private val logger: Logger = Utils.getLogger(TopicSchemaQueries::class.java)

    // ---- Schema Policy queries (reusable schema definitions) ----

    fun topicSchemaPolicies(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher {
            val future = CompletableFuture<List<Map<String, Any?>>>()
            deviceStore.getAllDevices().onComplete { result ->
                if (result.succeeded()) {
                    val policies = result.result()
                        .filter { it.type == DeviceConfig.DEVICE_TYPE_TOPIC_SCHEMA_POLICY }
                        .map { policyToMap(it) }
                    future.complete(policies)
                } else {
                    logger.severe("Error fetching schema policies: ${result.cause()?.message}")
                    future.complete(emptyList())
                }
            }
            future
        }
    }

    fun topicSchemaPolicy(): DataFetcher<CompletableFuture<Map<String, Any?>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>?>()
            val name = env.getArgument<String?>("name") ?: run {
                future.complete(null); return@DataFetcher future
            }
            deviceStore.getDevice(name).onComplete { result ->
                if (result.succeeded()) {
                    val device = result.result()
                    if (device != null && device.type == DeviceConfig.DEVICE_TYPE_TOPIC_SCHEMA_POLICY) {
                        future.complete(policyToMap(device))
                    } else {
                        future.complete(null)
                    }
                } else {
                    logger.severe("Error fetching schema policy: ${result.cause()?.message}")
                    future.complete(null)
                }
            }
            future
        }
    }

    fun topicSchemaValidate(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            val policyName = env.getArgument<String?>("policyName") ?: run {
                future.complete(validationError("", "CONFIG_ERROR", "policyName is required"))
                return@DataFetcher future
            }
            val payload = env.getArgument<String?>("payload") ?: run {
                future.complete(validationError(policyName, "CONFIG_ERROR", "payload is required"))
                return@DataFetcher future
            }

            deviceStore.getDevice(policyName).onComplete { result ->
                if (result.succeeded()) {
                    val device = result.result()
                    if (device == null || device.type != DeviceConfig.DEVICE_TYPE_TOPIC_SCHEMA_POLICY) {
                        future.complete(validationError(policyName, "CONFIG_ERROR", "Policy '$policyName' not found"))
                        return@onComplete
                    }

                    val schemaObj = device.config.getJsonObject("jsonSchema")
                    if (schemaObj == null) {
                        future.complete(validationError(policyName, "CONFIG_ERROR", "Policy has no JSON Schema"))
                        return@onComplete
                    }

                    try {
                        val validator = JsonSchemaValidator(schemaObj)
                        val vr = validator.validate(payload)
                        future.complete(mapOf(
                            "policyName" to policyName, "topicFilter" to "",
                            "valid" to vr.valid, "errorCategory" to vr.errorCategory,
                            "errorDetail" to vr.errorDetail
                        ))
                    } catch (e: Exception) {
                        future.complete(validationError(policyName, "CONFIG_ERROR", "Invalid schema: ${e.message}"))
                    }
                } else {
                    future.complete(validationError(policyName, "CONFIG_ERROR",
                        "Database error: ${result.cause()?.message}"))
                }
            }
            future
        }
    }

    // ---- Namespace queries (topic filter -> schema bindings) ----

    fun topicNamespaces(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher {
            val future = CompletableFuture<List<Map<String, Any?>>>()
            deviceStore.getAllDevices().onComplete { result ->
                if (result.succeeded()) {
                    val namespaces = result.result()
                        .filter { it.type == DeviceConfig.DEVICE_TYPE_TOPIC_NAMESPACE }
                        .map { namespaceToMap(it) }
                    future.complete(namespaces)
                } else {
                    logger.severe("Error fetching topic namespaces: ${result.cause()?.message}")
                    future.complete(emptyList())
                }
            }
            future
        }
    }

    fun topicNamespace(): DataFetcher<CompletableFuture<Map<String, Any?>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>?>()
            val name = env.getArgument<String?>("name") ?: run {
                future.complete(null); return@DataFetcher future
            }
            deviceStore.getDevice(name).onComplete { result ->
                if (result.succeeded()) {
                    val device = result.result()
                    if (device != null && device.type == DeviceConfig.DEVICE_TYPE_TOPIC_NAMESPACE) {
                        future.complete(namespaceToMap(device))
                    } else {
                        future.complete(null)
                    }
                } else {
                    logger.severe("Error fetching topic namespace: ${result.cause()?.message}")
                    future.complete(null)
                }
            }
            future
        }
    }

    fun topicNamespaceMatch(): DataFetcher<CompletableFuture<Map<String, Any?>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>?>()
            val topic = env.getArgument<String?>("topic") ?: run {
                future.complete(null); return@DataFetcher future
            }
            val matched = policyCache.matchNamespace(topic)
            if (matched != null) {
                deviceStore.getDevice(matched.namespaceName).onComplete { result ->
                    if (result.succeeded()) {
                        val device = result.result()
                        if (device != null) {
                            future.complete(namespaceToMap(device))
                        } else {
                            future.complete(null)
                        }
                    } else {
                        future.complete(null)
                    }
                }
            } else {
                future.complete(null)
            }
            future
        }
    }

    // ---- Helpers ----

    private fun validationError(policyName: String, category: String, detail: String): Map<String, Any?> = mapOf(
        "policyName" to policyName, "topicFilter" to "", "valid" to false,
        "errorCategory" to category, "errorDetail" to detail
    )

    private fun policyToMap(device: DeviceConfig): Map<String, Any?> {
        val config = device.config
        return mapOf(
            "name" to device.name,
            "jsonSchema" to (config.getJsonObject("jsonSchema")?.map ?: emptyMap<String, Any>()),
            "payloadType" to config.getString("payloadType", "JSON"),
            "contentType" to config.getString("contentType"),
            "version" to config.getString("version"),
            "description" to config.getString("description"),
            "examples" to config.getJsonArray("examples")?.list,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString()
        )
    }

    private fun namespaceToMap(device: DeviceConfig): Map<String, Any?> {
        val config = device.config
        return mapOf(
            "name" to device.name,
            "topicFilter" to config.getString("topicFilter", ""),
            "schemaPolicyName" to config.getString("schemaPolicyName", ""),
            "enabled" to device.enabled,
            "enforcementMode" to config.getString("enforcementMode", "REJECT"),
            "description" to config.getString("description"),
            "tags" to config.getJsonArray("tags")?.list,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString()
        )
    }
}
