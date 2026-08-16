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
    private val deviceStore: IDeviceConfigStore,
    private val config: io.vertx.core.json.JsonObject = io.vertx.core.json.JsonObject()
) {
    private val logger: Logger = Utils.getLogger(HmiClientConfigMutations::class.java)
    private val queries = HmiClientConfigQueries(vertx, deviceStore, config)
    private val hmiPath: String?
        get() = Monster.getHmiPath(config)

    fun hmi(): DataFetcher<Map<String, Any>> {
        return DataFetcher {
            if (!Monster.isFeatureEnabled(Features.Hmi)) mapOf("success" to false, "message" to "Feature Hmi disabled")
            else mapOf()
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun unsetOtherMainHmis(currentName: String): io.vertx.core.Future<Void> {
        return deviceStore.getDevicesByType(DeviceConfig.DEVICE_TYPE_HMI).compose { hmiDevices ->
            val futures = mutableListOf<io.vertx.core.Future<*>>()
            for (dev in hmiDevices) {
                if (dev.name != currentName && dev.config.getBoolean("isMain", false)) {
                    dev.config.put("isMain", false)
                    val urlPath = dev.config.getString("urlPath")
                    if (urlPath.isNullOrEmpty()) {
                        dev.config.put("urlPath", dev.name)
                    }
                    futures.add(deviceStore.saveDevice(dev))
                }
            }
            if (futures.isEmpty()) {
                io.vertx.core.Future.succeededFuture()
            } else {
                io.vertx.core.Future.all(futures as List<io.vertx.core.Future<*>>).compose { io.vertx.core.Future.succeededFuture() }
            }
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

                val isMain = configInput["isMain"] as? Boolean ?: false
                val configJson = JsonObject()
                configInput["urlPath"]?.let { configJson.put("urlPath", it) }
                configJson.put("isMain", isMain)
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

                val saveFuture = if (isMain) {
                    unsetOtherMainHmis(name).compose { deviceStore.saveDevice(request.toDeviceConfig()) }
                } else {
                    deviceStore.saveDevice(request.toDeviceConfig())
                }

                saveFuture.onComplete { result ->
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

                deviceStore.deleteDevice(name).onComplete { result ->
                    if (result.succeeded()) {
                        val basePath = hmiPath
                        if (basePath != null) {
                            val targetDir = java.io.File(basePath, name)
                            if (targetDir.exists()) {
                                targetDir.deleteRecursively()
                            }
                        }
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

    fun uploadZip(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()
            if (!Monster.isFeatureEnabled(Features.Hmi))
                return@DataFetcher future.apply { complete(mapOf("success" to false, "message" to "Feature Hmi disabled")) }

            try {
                val name = env.getArgument<String>("name") ?: return@DataFetcher future.apply { complete(mapOf("success" to false, "message" to "Name is required")) }
                val zipBase64 = env.getArgument<String>("zipBase64") ?: return@DataFetcher future.apply { complete(mapOf("success" to false, "message" to "zipBase64 is required")) }
                val setAsMain = env.getArgument<Boolean>("setAsMain") ?: false

                val zipBytes = java.util.Base64.getDecoder().decode(zipBase64)
                val basePath = hmiPath ?: return@DataFetcher future.apply { complete(mapOf("success" to false, "message" to "HMI.Path is missing in configuration")) }
                val targetDir = java.io.File(basePath, name)
                if (targetDir.exists()) {
                    targetDir.deleteRecursively()
                }
                targetDir.mkdirs()

                val canonicalTargetDir = targetDir.canonicalFile
                java.util.zip.ZipInputStream(java.io.ByteArrayInputStream(zipBytes)).use { zis ->
                    var entry = zis.nextEntry
                    while (entry != null) {
                        val entryFile = java.io.File(targetDir, entry.name).canonicalFile
                        if (!entryFile.path.startsWith(canonicalTargetDir.path)) {
                            entry = zis.nextEntry
                            continue
                        }
                        if (entry.isDirectory) {
                            entryFile.mkdirs()
                        } else {
                            entryFile.parentFile?.mkdirs()
                            entryFile.outputStream().use { os -> zis.copyTo(os) }
                        }
                        zis.closeEntry()
                        entry = zis.nextEntry
                    }
                }

                val config = JsonObject()
                    .put("isMain", setAsMain)
                    .put("urlPath", if (setAsMain) "" else name)
                    .put("entryPoint", "index.html")

                val device = DeviceConfig(
                    name = name,
                    namespace = name,
                    nodeId = "local",
                    type = DeviceConfig.DEVICE_TYPE_HMI,
                    enabled = true,
                    config = config
                )

                val saveFuture = if (setAsMain) {
                    unsetOtherMainHmis(name).compose { deviceStore.saveDevice(device) }
                } else {
                    deviceStore.saveDevice(device)
                }

                saveFuture.onComplete { result ->
                    if (result.succeeded()) {
                        future.complete(mapOf("success" to true, "hmi" to queries.deviceToMap(device)))
                    } else {
                        future.complete(mapOf("success" to false, "message" to (result.cause()?.message ?: "Failed to save device config")))
                    }
                }
            } catch (e: Exception) {
                logger.severe("Exception in uploadZip HMI: ${e.message}")
                future.complete(mapOf("success" to false, "message" to (e.message ?: "Internal error")))
            }

            future
        }
    }
}
