package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Features
import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigRequest
import at.rocworks.stores.IDeviceConfigStore
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL mutations for HMI device configuration management
 */
class HmiClientConfigMutations(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(HmiClientConfigMutations::class.java)
    private val queries = HmiClientConfigQueries(vertx, deviceStore)

    fun hmi(): DataFetcher<Map<String, Any>> {
        return DataFetcher {
            if (!Monster.isFeatureEnabled(Features.Hmi)) mapOf("success" to false, "message" to "Feature Hmi disabled")
            else mapOf()
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun create(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            if (!Monster.isFeatureEnabled(Features.Hmi))
                return@DataFetcher future.apply { complete(mapOf("success" to false, "message" to "Feature Hmi disabled")) }

            try {
                val input = env.getArgument<Map<String, Any>>("input")
                    ?: return@DataFetcher future.apply { complete(mapOf("success" to false, "message" to "Input is required")) }

                val name = input["name"] as? String ?: ""
                val nodeId = (input["nodeId"] as? String)?.takeIf { it.isNotBlank() } ?: "local"
                val enabled = input["enabled"] as? Boolean ?: true
                val configInput = input["config"] as? Map<String, Any> ?: emptyMap()

                val configJson = JsonObject()
                configInput["urlPath"]?.let { configJson.put("urlPath", it) }
                configInput["isMain"]?.let { configJson.put("isMain", it) }
                configInput["title"]?.let { configJson.put("title", it) }
                configInput["description"]?.let { configJson.put("description", it) }
                configInput["entryPoint"]?.let { configJson.put("entryPoint", it) }

                val request = DeviceConfigRequest(
                    name = name,
                    namespace = name,
                    nodeId = nodeId,
                    config = configJson,
                    enabled = enabled,
                    type = DeviceConfig.DEVICE_TYPE_HMI
                )

                val validationErrors = request.validate()
                if (validationErrors.isNotEmpty()) {
                    return@DataFetcher future.apply {
                        complete(mapOf("success" to false, "message" to validationErrors.joinToString(", ")))
                    }
                }

                deviceStore.saveDevice(request.toDeviceConfig()).onComplete { result ->
                    if (result.succeeded()) {
                        val saved = result.result()
                        future.complete(mapOf("success" to true, "hmi" to queries.deviceToMap(saved)))
                    } else {
                        logger.severe("Error saving HMI $name: ${result.cause()?.message}")
                        future.complete(mapOf("success" to false, "message" to (result.cause()?.message ?: "Failed to save HMI")))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Exception in create HMI: ${e.message}")
                future.complete(mapOf("success" to false, "message" to (e.message ?: "Internal error")))
            }

            future
        }
    }

    fun update(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return create()
    }

    fun delete(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            if (!Monster.isFeatureEnabled(Features.Hmi))
                return@DataFetcher future.apply { complete(mapOf("success" to false, "message" to "Feature Hmi disabled")) }

            try {
                val name = env.getArgument<String>("name")
                    ?: return@DataFetcher future.apply { complete(mapOf("success" to false, "message" to "Name is required")) }

                if (name == "main") {
                    return@DataFetcher future.apply { complete(mapOf("success" to false, "message" to "Cannot delete default 'main' HMI")) }
                }

                deviceStore.deleteDevice(name).onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(mapOf("success" to true))
                    } else {
                        logger.severe("Error deleting HMI $name: ${result.cause()?.message}")
                        future.complete(mapOf("success" to false, "message" to (result.cause()?.message ?: "Failed to delete HMI")))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Exception in delete HMI: ${e.message}")
                future.complete(mapOf("success" to false, "message" to (e.message ?: "Internal error")))
            }

            future
        }
    }

    fun toggle(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            if (!Monster.isFeatureEnabled(Features.Hmi))
                return@DataFetcher future.apply { complete(mapOf("success" to false, "message" to "Feature Hmi disabled")) }

            try {
                val name = env.getArgument<String>("name")
                    ?: return@DataFetcher future.apply { complete(mapOf("success" to false, "message" to "Name is required")) }
                val enabled = env.getArgument<Boolean>("enabled") ?: true

                deviceStore.toggleDevice(name, enabled).onComplete { result ->
                    if (result.succeeded()) {
                        val toggled = result.result()
                        if (toggled != null) {
                            future.complete(mapOf("success" to true, "hmi" to queries.deviceToMap(toggled)))
                        } else {
                            future.complete(mapOf("success" to false, "message" to "HMI $name not found"))
                        }
                    } else {
                        logger.severe("Error toggling HMI $name: ${result.cause()?.message}")
                        future.complete(mapOf("success" to false, "message" to (result.cause()?.message ?: "Failed to toggle HMI")))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Exception in toggle HMI: ${e.message}")
                future.complete(mapOf("success" to false, "message" to (e.message ?: "Internal error")))
            }

            future
        }
    }

    fun start(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return toggle()
    }

    fun stop(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            val name = env.getArgument<String>("name") ?: return@DataFetcher future.apply { complete(mapOf("success" to false, "message" to "Name is required")) }
            deviceStore.toggleDevice(name, false).onComplete { result ->
                val device = result.result()
                if (result.succeeded() && device != null) {
                    future.complete(mapOf("success" to true, "hmi" to queries.deviceToMap(device)))
                } else {
                    future.complete(mapOf("success" to false, "message" to (result.cause()?.message ?: "Failed to stop HMI")))
                }
            }
            future
        }
    }

    fun reassign(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            if (!Monster.isFeatureEnabled(Features.Hmi))
                return@DataFetcher future.apply { complete(mapOf("success" to false, "message" to "Feature Hmi disabled")) }

            try {
                val name = env.getArgument<String>("name")
                    ?: return@DataFetcher future.apply { complete(mapOf("success" to false, "message" to "Name is required")) }
                val nodeId = env.getArgument<String>("nodeId")
                    ?: return@DataFetcher future.apply { complete(mapOf("success" to false, "message" to "NodeId is required")) }

                deviceStore.reassignDevice(name, nodeId).onComplete { result ->
                    if (result.succeeded()) {
                        val reassigned = result.result()
                        if (reassigned != null) {
                            future.complete(mapOf("success" to true, "hmi" to queries.deviceToMap(reassigned)))
                        } else {
                            future.complete(mapOf("success" to false, "message" to "HMI $name not found"))
                        }
                    } else {
                        logger.severe("Error reassigning HMI $name: ${result.cause()?.message}")
                        future.complete(mapOf("success" to false, "message" to (result.cause()?.message ?: "Failed to reassign HMI")))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Exception in reassign HMI: ${e.message}")
                future.complete(mapOf("success" to false, "message" to (e.message ?: "Internal error")))
            }

            future
        }
    }
}
