package at.rocworks.graphql

import at.rocworks.Utils
import at.rocworks.schema.JsonSchemaValidator
import at.rocworks.schema.TopicSchemaPolicyCache
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import graphql.schema.DataFetcher
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL mutation resolvers for Topic Schema Governance.
 *
 * TopicSchemaPolicy = reusable JSON Schema definition
 * TopicNamespace    = binds an MQTT topic filter to a schema policy
 */
class TopicSchemaMutations(
    private val deviceStore: IDeviceConfigStore,
    private val policyCache: TopicSchemaPolicyCache
) {
    private val logger: Logger = Utils.getLogger(TopicSchemaMutations::class.java)

    // ---- Schema Policy Mutations (reusable schema definitions) ----

    fun createPolicy(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            try {
                val input = env.getArgument<Map<String, Any>>("input")
                    ?: return@DataFetcher future.apply { complete(errorResult("Input is required")) }

                val name = input["name"] as? String ?: return@DataFetcher future.apply { complete(errorResult("name is required")) }

                val errors = validatePolicyInput(name, input)
                if (errors.isNotEmpty()) {
                    future.complete(mapOf("success" to false, "errors" to errors))
                    return@DataFetcher future
                }

                deviceStore.getDevice(name).onComplete { existingResult ->
                    if (existingResult.failed()) {
                        future.complete(errorResult("Database error: ${existingResult.cause()?.message}"))
                        return@onComplete
                    }
                    if (existingResult.result() != null) {
                        future.complete(errorResult("Schema policy with name '$name' already exists"))
                        return@onComplete
                    }

                    val config = buildPolicyConfig(input)
                    val device = DeviceConfig(
                        name = name,
                        namespace = "schema",
                        nodeId = "",
                        config = config,
                        enabled = true,
                        type = DeviceConfig.DEVICE_TYPE_TOPIC_SCHEMA_POLICY
                    )

                    deviceStore.saveDevice(device).onComplete { saveResult ->
                        if (saveResult.succeeded()) {
                            policyCache.publishReloadEvent()
                            future.complete(mapOf(
                                "success" to true,
                                "policy" to policyToMap(saveResult.result()),
                                "errors" to emptyList<String>()
                            ))
                        } else {
                            future.complete(errorResult("Failed to save policy: ${saveResult.cause()?.message}"))
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error creating schema policy: ${e.message}")
                future.complete(errorResult("Failed to create policy: ${e.message}"))
            }
            future
        }
    }

    fun updatePolicy(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            try {
                val name = env.getArgument<String>("name") ?: return@DataFetcher future.apply { complete(errorResult("name is required")) }
                val input = env.getArgument<Map<String, Any>>("input") ?: return@DataFetcher future.apply { complete(errorResult("input is required")) }

                val errors = validatePolicyInput(name, input)
                if (errors.isNotEmpty()) {
                    future.complete(mapOf("success" to false, "errors" to errors))
                    return@DataFetcher future
                }

                deviceStore.getDevice(name).onComplete { existingResult ->
                    if (existingResult.failed()) {
                        future.complete(errorResult("Database error: ${existingResult.cause()?.message}"))
                        return@onComplete
                    }
                    val existing = existingResult.result()
                    if (existing == null || existing.type != DeviceConfig.DEVICE_TYPE_TOPIC_SCHEMA_POLICY) {
                        future.complete(errorResult("Schema policy '$name' not found"))
                        return@onComplete
                    }

                    val config = buildPolicyConfig(input)
                    val updatedDevice = DeviceConfig(
                        name = input["name"] as? String ?: name,
                        namespace = "schema",
                        nodeId = existing.nodeId,
                        config = config,
                        enabled = true,
                        type = DeviceConfig.DEVICE_TYPE_TOPIC_SCHEMA_POLICY,
                        createdAt = existing.createdAt,
                        updatedAt = Instant.now()
                    )

                    deviceStore.saveDevice(updatedDevice).onComplete { saveResult ->
                        if (saveResult.succeeded()) {
                            policyCache.publishReloadEvent()
                            future.complete(mapOf(
                                "success" to true,
                                "policy" to policyToMap(saveResult.result()),
                                "errors" to emptyList<String>()
                            ))
                        } else {
                            future.complete(errorResult("Failed to update policy: ${saveResult.cause()?.message}"))
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error updating schema policy: ${e.message}")
                future.complete(errorResult("Failed to update policy: ${e.message}"))
            }
            future
        }
    }

    fun deletePolicy(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Boolean>()
            val name = env.getArgument<String>("name") ?: return@DataFetcher future.apply { complete(false) }

            deviceStore.deleteDevice(name).onComplete { result ->
                if (result.succeeded() && result.result()) {
                    policyCache.publishReloadEvent()
                    logger.info("Deleted schema policy: $name")
                    future.complete(true)
                } else {
                    future.complete(false)
                }
            }
            future
        }
    }

    // ---- Namespace Mutations (topic filter -> schema bindings) ----

    fun createNamespace(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            try {
                val input = env.getArgument<Map<String, Any>>("input")
                    ?: return@DataFetcher future.apply { complete(errorResult("Input is required")) }

                val name = input["name"] as? String ?: return@DataFetcher future.apply { complete(errorResult("name is required")) }
                val topicFilter = input["topicFilter"] as? String ?: return@DataFetcher future.apply { complete(errorResult("topicFilter is required")) }
                val schemaPolicyName = input["schemaPolicyName"] as? String ?: return@DataFetcher future.apply { complete(errorResult("schemaPolicyName is required")) }

                val errors = validateNamespaceInput(name, topicFilter)
                if (errors.isNotEmpty()) {
                    future.complete(mapOf("success" to false, "errors" to errors))
                    return@DataFetcher future
                }

                // Verify the referenced schema policy exists
                deviceStore.getDevice(schemaPolicyName).onComplete { policyResult ->
                    if (policyResult.failed()) {
                        future.complete(errorResult("Database error: ${policyResult.cause()?.message}"))
                        return@onComplete
                    }
                    val policyDevice = policyResult.result()
                    if (policyDevice == null || policyDevice.type != DeviceConfig.DEVICE_TYPE_TOPIC_SCHEMA_POLICY) {
                        future.complete(errorResult("Schema policy '$schemaPolicyName' not found"))
                        return@onComplete
                    }

                    // Check if namespace name already exists
                    deviceStore.getDevice(name).onComplete { existingResult ->
                        if (existingResult.failed()) {
                            future.complete(errorResult("Database error: ${existingResult.cause()?.message}"))
                            return@onComplete
                        }
                        if (existingResult.result() != null) {
                            future.complete(errorResult("Namespace with name '$name' already exists"))
                            return@onComplete
                        }

                        val config = buildNamespaceConfig(input)
                        val device = DeviceConfig(
                            name = name,
                            namespace = topicFilter,
                            nodeId = "",
                            config = config,
                            enabled = input["enabled"] as? Boolean ?: true,
                            type = DeviceConfig.DEVICE_TYPE_TOPIC_NAMESPACE
                        )

                        deviceStore.saveDevice(device).onComplete { saveResult ->
                            if (saveResult.succeeded()) {
                                policyCache.publishReloadEvent()
                                future.complete(mapOf(
                                    "success" to true,
                                    "namespace" to namespaceToMap(saveResult.result()),
                                    "errors" to emptyList<String>()
                                ))
                            } else {
                                future.complete(errorResult("Failed to save namespace: ${saveResult.cause()?.message}"))
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error creating namespace: ${e.message}")
                future.complete(errorResult("Failed to create namespace: ${e.message}"))
            }
            future
        }
    }

    fun updateNamespace(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            try {
                val name = env.getArgument<String>("name") ?: return@DataFetcher future.apply { complete(errorResult("name is required")) }
                val input = env.getArgument<Map<String, Any>>("input") ?: return@DataFetcher future.apply { complete(errorResult("input is required")) }

                val topicFilter = input["topicFilter"] as? String ?: return@DataFetcher future.apply { complete(errorResult("topicFilter is required")) }
                val schemaPolicyName = input["schemaPolicyName"] as? String ?: return@DataFetcher future.apply { complete(errorResult("schemaPolicyName is required")) }

                val errors = validateNamespaceInput(name, topicFilter)
                if (errors.isNotEmpty()) {
                    future.complete(mapOf("success" to false, "errors" to errors))
                    return@DataFetcher future
                }

                // Verify the referenced schema policy exists
                deviceStore.getDevice(schemaPolicyName).onComplete { policyResult ->
                    if (policyResult.failed()) {
                        future.complete(errorResult("Database error: ${policyResult.cause()?.message}"))
                        return@onComplete
                    }
                    val policyDevice = policyResult.result()
                    if (policyDevice == null || policyDevice.type != DeviceConfig.DEVICE_TYPE_TOPIC_SCHEMA_POLICY) {
                        future.complete(errorResult("Schema policy '$schemaPolicyName' not found"))
                        return@onComplete
                    }

                    deviceStore.getDevice(name).onComplete { existingResult ->
                        if (existingResult.failed()) {
                            future.complete(errorResult("Database error: ${existingResult.cause()?.message}"))
                            return@onComplete
                        }
                        val existing = existingResult.result()
                        if (existing == null || existing.type != DeviceConfig.DEVICE_TYPE_TOPIC_NAMESPACE) {
                            future.complete(errorResult("Namespace '$name' not found"))
                            return@onComplete
                        }

                        val config = buildNamespaceConfig(input)
                        val updatedDevice = DeviceConfig(
                            name = input["name"] as? String ?: name,
                            namespace = topicFilter,
                            nodeId = existing.nodeId,
                            config = config,
                            enabled = input["enabled"] as? Boolean ?: existing.enabled,
                            type = DeviceConfig.DEVICE_TYPE_TOPIC_NAMESPACE,
                            createdAt = existing.createdAt,
                            updatedAt = Instant.now()
                        )

                        deviceStore.saveDevice(updatedDevice).onComplete { saveResult ->
                            if (saveResult.succeeded()) {
                                policyCache.publishReloadEvent()
                                future.complete(mapOf(
                                    "success" to true,
                                    "namespace" to namespaceToMap(saveResult.result()),
                                    "errors" to emptyList<String>()
                                ))
                            } else {
                                future.complete(errorResult("Failed to update namespace: ${saveResult.cause()?.message}"))
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error updating namespace: ${e.message}")
                future.complete(errorResult("Failed to update namespace: ${e.message}"))
            }
            future
        }
    }

    fun deleteNamespace(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Boolean>()
            val name = env.getArgument<String>("name") ?: return@DataFetcher future.apply { complete(false) }

            deviceStore.deleteDevice(name).onComplete { result ->
                if (result.succeeded() && result.result()) {
                    policyCache.publishReloadEvent()
                    logger.info("Deleted namespace: $name")
                    future.complete(true)
                } else {
                    future.complete(false)
                }
            }
            future
        }
    }

    fun toggleNamespace(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            val name = env.getArgument<String>("name") ?: return@DataFetcher future.apply { complete(errorResult("name is required")) }
            val enabled = env.getArgument<Boolean>("enabled") ?: return@DataFetcher future.apply { complete(errorResult("enabled is required")) }

            deviceStore.toggleDevice(name, enabled).onComplete { result ->
                if (result.succeeded() && result.result() != null) {
                    policyCache.publishReloadEvent()
                    logger.info("Toggled namespace '$name' to enabled=$enabled")
                    future.complete(mapOf(
                        "success" to true,
                        "namespace" to namespaceToMap(result.result()!!),
                        "errors" to emptyList<String>()
                    ))
                } else {
                    future.complete(errorResult("Namespace '$name' not found"))
                }
            }
            future
        }
    }

    // ---- Helpers ----

    private fun validatePolicyInput(name: String, input: Map<String, Any>): List<String> {
        val errors = mutableListOf<String>()
        if (name.isBlank()) errors.add("name cannot be blank")
        if (!name.matches(Regex("^[a-zA-Z0-9_-]+$"))) errors.add("name can only contain letters, numbers, underscores, and hyphens")

        val jsonSchemaRaw = input["jsonSchema"]
        if (jsonSchemaRaw != null) {
            try {
                val schemaObj = when (jsonSchemaRaw) {
                    is Map<*, *> -> JsonObject(@Suppress("UNCHECKED_CAST") (jsonSchemaRaw as Map<String, Any>))
                    is JsonObject -> jsonSchemaRaw
                    else -> throw IllegalArgumentException("jsonSchema must be a JSON object")
                }
                JsonSchemaValidator(schemaObj) // compile to catch errors early
            } catch (e: Exception) {
                errors.add("Invalid JSON Schema: ${e.message}")
            }
        } else {
            errors.add("jsonSchema is required")
        }
        return errors
    }

    private fun validateNamespaceInput(name: String, topicFilter: String): List<String> {
        val errors = mutableListOf<String>()
        if (name.isBlank()) errors.add("name cannot be blank")
        if (!name.matches(Regex("^[a-zA-Z0-9_-]+$"))) errors.add("name can only contain letters, numbers, underscores, and hyphens")
        if (topicFilter.isBlank()) errors.add("topicFilter cannot be blank")
        return errors
    }

    @Suppress("UNCHECKED_CAST")
    private fun buildPolicyConfig(input: Map<String, Any>): JsonObject {
        val config = JsonObject()
        config.put("payloadType", input["payloadType"] as? String ?: "JSON")

        val jsonSchemaRaw = input["jsonSchema"]
        val schemaObj = when (jsonSchemaRaw) {
            is Map<*, *> -> JsonObject(jsonSchemaRaw as Map<String, Any>)
            is JsonObject -> jsonSchemaRaw
            else -> JsonObject()
        }
        config.put("jsonSchema", schemaObj)

        (input["contentType"] as? String)?.let { config.put("contentType", it) }
        (input["version"] as? String)?.let { config.put("version", it) }
        (input["description"] as? String)?.let { config.put("description", it) }
        (input["examples"] as? List<*>)?.let { examples ->
            config.put("examples", JsonArray(examples.mapNotNull { it as? String }))
        }
        return config
    }

    private fun buildNamespaceConfig(input: Map<String, Any>): JsonObject {
        val config = JsonObject()
        config.put("topicFilter", input["topicFilter"] as? String ?: "")
        config.put("schemaPolicyName", input["schemaPolicyName"] as? String ?: "")
        config.put("enforcementMode", input["enforcementMode"] as? String ?: "REJECT")
        (input["description"] as? String)?.let { config.put("description", it) }
        @Suppress("UNCHECKED_CAST")
        (input["tags"] as? List<*>)?.let { tags ->
            config.put("tags", JsonArray(tags.mapNotNull { it as? String }))
        }
        return config
    }

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

    private fun errorResult(message: String): Map<String, Any> = mapOf(
        "success" to false,
        "errors" to listOf(message)
    )
}
