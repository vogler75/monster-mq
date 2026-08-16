package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Features
import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL queries for HMI device configuration management
 */
class HmiClientConfigQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore,
    private val config: io.vertx.core.json.JsonObject = io.vertx.core.json.JsonObject()
) {
    private val logger: Logger = Utils.getLogger(HmiClientConfigQueries::class.java)
    private val hmiPath: String?
        get() = Monster.getHmiPath(config)

    fun hmis(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any>>>()
            if (!Monster.isFeatureEnabled(Features.Hmi))
                return@DataFetcher future.apply { complete(emptyList()) }

            try {
                val name = env.getArgument<String>("name")
                val nodeId = env.getArgument<String>("nodeId")

                when {
                    name != null && nodeId != null -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_HMI && it.name == name }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching HMIs: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    name != null -> {
                        deviceStore.getDevice(name).onComplete { result ->
                            if (result.succeeded()) {
                                val device = result.result()
                                if (device != null && device.type == DeviceConfig.DEVICE_TYPE_HMI) {
                                    future.complete(listOf(deviceToMap(device)))
                                } else {
                                    future.complete(emptyList())
                                }
                            } else {
                                logger.severe("Error fetching HMI: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    nodeId != null -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_HMI }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching HMIs by node: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    else -> {
                        deviceStore.getDevicesByType(DeviceConfig.DEVICE_TYPE_HMI).onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching all HMIs: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Exception in hmis query: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    fun hmi(): DataFetcher<CompletableFuture<Map<String, Any>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>?>()
            if (!Monster.isFeatureEnabled(Features.Hmi))
                return@DataFetcher future.apply { complete(null) }

            try {
                val name = env.getArgument<String>("name")
                    ?: return@DataFetcher future.apply { complete(null) }

                deviceStore.getDevice(name).onComplete { result ->
                    if (result.succeeded()) {
                        val device = result.result()
                        if (device != null && device.type == DeviceConfig.DEVICE_TYPE_HMI) {
                            future.complete(deviceToMap(device))
                        } else {
                            future.complete(null)
                        }
                    } else {
                        logger.severe("Error fetching HMI $name: ${result.cause()?.message}")
                        future.complete(null)
                    }
                }
            } catch (e: Exception) {
                logger.severe("Exception in hmi query: ${e.message}")
                future.complete(null)
            }

            future
        }
    }

    fun hmiFiles(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any>>>()
            if (!Monster.isFeatureEnabled(Features.Hmi))
                return@DataFetcher future.apply { complete(emptyList()) }

            try {
                val name = env.getArgument<String>("name") ?: return@DataFetcher future.apply { complete(emptyList()) }
                val basePath = hmiPath ?: return@DataFetcher future.apply { complete(emptyList()) }
                val hmiDir = java.io.File(basePath, name)
                if (!hmiDir.exists() || !hmiDir.isDirectory) {
                    return@DataFetcher future.apply { complete(emptyList()) }
                }

                val files = mutableListOf<Map<String, Any>>()
                val baseCanonical = hmiDir.canonicalFile
                hmiDir.walkTopDown().filter { it.isFile }.forEach { file ->
                    val relPath = file.canonicalFile.path.removePrefix(baseCanonical.path).removePrefix(java.io.File.separator).replace('\\', '/')
                    files.add(mapOf(
                        "path" to relPath,
                        "sizeBytes" to file.length()
                    ))
                }
                future.complete(files)
            } catch (e: Exception) {
                logger.severe("Exception in hmiFiles query: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    fun exportHmiZip(): DataFetcher<CompletableFuture<String>> {
        return DataFetcher { env ->
            val future = CompletableFuture<String>()
            if (!Monster.isFeatureEnabled(Features.Hmi))
                return@DataFetcher future.apply { complete("") }

            try {
                val name = env.getArgument<String>("name") ?: return@DataFetcher future.apply { complete("") }
                val basePath = hmiPath ?: return@DataFetcher future.apply { complete("") }
                val hmiDir = java.io.File(basePath, name)
                if (!hmiDir.exists() || !hmiDir.isDirectory) {
                    return@DataFetcher future.apply { complete("") }
                }

                val baos = java.io.ByteArrayOutputStream()
                java.util.zip.ZipOutputStream(baos).use { zos ->
                    val baseCanonical = hmiDir.canonicalFile
                    hmiDir.walkTopDown().filter { it.isFile }.forEach { file ->
                        val relPath = file.canonicalFile.path.removePrefix(baseCanonical.path).removePrefix(java.io.File.separator).replace('\\', '/')
                        val entry = java.util.zip.ZipEntry(relPath)
                        zos.putNextEntry(entry)
                        file.inputStream().use { input -> input.copyTo(zos) }
                        zos.closeEntry()
                    }
                }
                val b64 = java.util.Base64.getEncoder().encodeToString(baos.toByteArray())
                future.complete(b64)
            } catch (e: Exception) {
                logger.severe("Exception in exportHmiZip query: ${e.message}")
                future.complete("")
            }

            future
        }
    }

    fun deviceToMap(device: DeviceConfig): Map<String, Any> {
        val cfg = device.config
        val isMain = cfg.getBoolean("isMain", device.name == "main")
        val urlPath = cfg.getString("urlPath", if (isMain) "" else device.name)
        val title = cfg.getString("title", device.name)
        val description = cfg.getString("description", "")
        val entryPoint = cfg.getString("entryPoint", "index.html")

        val configMap = mapOf(
            "urlPath" to urlPath,
            "isMain" to isMain,
            "title" to title,
            "description" to description,
            "entryPoint" to entryPoint
        )

        val currentNodeId = Monster.getClusterNodeId(vertx)
        val isOnCurrentNode = device.isAssignedToNode(currentNodeId)

        val basePath = hmiPath
        var fileCount = 0
        var sizeBytes = 0L
        if (basePath != null) {
            val hmiDir = java.io.File(basePath, device.name)
            if (hmiDir.exists() && hmiDir.isDirectory) {
                hmiDir.walkTopDown().filter { it.isFile }.forEach { file ->
                    fileCount++
                    sizeBytes += file.length()
                }
            }
        }

        return mapOf(
            "name" to device.name,
            "nodeId" to device.nodeId,
            "enabled" to device.enabled,
            "config" to configMap,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString(),
            "isOnCurrentNode" to isOnCurrentNode,
            "fileCount" to fileCount,
            "sizeBytes" to sizeBytes
        )
    }
}
