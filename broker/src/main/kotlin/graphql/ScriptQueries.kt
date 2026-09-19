package at.rocworks.graphql

import at.rocworks.Features
import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.devices.script.ScriptExtension
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.ScriptConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL queries for standalone Script devices.
 */
class ScriptQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(ScriptQueries::class.java)

    fun scripts(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any?>>>()
            if (!Monster.isFeatureEnabled(Features.PythonScripts)) {
                return@DataFetcher future.apply { complete(emptyList()) }
            }

            val nameFilter = env.getArgument<String>("name")
            val nodeIdFilter = env.getArgument<String>("nodeId")

            deviceStore.getAllDevices().onComplete { res ->
                if (res.succeeded()) {
                    val currentNodeId = Monster.getClusterNodeId(vertx)
                    val list = res.result()
                        .filter { it.type == DeviceConfig.DEVICE_TYPE_SCRIPT }
                        .filter { nameFilter.isNullOrBlank() || it.name == nameFilter }
                        .filter { nodeIdFilter.isNullOrBlank() || it.nodeId == nodeIdFilter }
                        .map { deviceToScript(it, currentNodeId) }
                    future.complete(list)
                } else {
                    logger.severe("Failed to query scripts: ${res.cause()?.message}")
                    future.complete(emptyList())
                }
            }

            future
        }
    }

    fun script(): DataFetcher<CompletableFuture<Map<String, Any?>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>?>()
            if (!Monster.isFeatureEnabled(Features.PythonScripts)) {
                return@DataFetcher future.apply { complete(null) }
            }

            val name = env.getArgument<String>("name") ?: ""
            deviceStore.getDevice(name).onComplete { res ->
                if (res.succeeded() && res.result() != null) {
                    val dev = res.result()!!
                    if (dev.type == DeviceConfig.DEVICE_TYPE_SCRIPT) {
                        val currentNodeId = Monster.getClusterNodeId(vertx)
                        future.complete(deviceToScript(dev, currentNodeId))
                    } else {
                        future.complete(null)
                    }
                } else {
                    future.complete(null)
                }
            }

            future
        }
    }

    private fun loadDoc(filename: String): String {
        return try {
            val resourcePath = "docs/$filename"
            val stream = this::class.java.classLoader.getResourceAsStream(resourcePath)
            if (stream != null) {
                stream.bufferedReader().use { it.readText() }
            } else {
                val candidate1 = java.io.File("src/main/resources/docs/$filename")
                if (candidate1.exists()) candidate1.readText()
                else {
                    val candidate2 = java.io.File("broker/src/main/resources/docs/$filename")
                    if (candidate2.exists()) candidate2.readText() else ""
                }
            }
        } catch (e: Exception) {
            logger.warning("Failed to load script doc $filename: ${e.message}")
            ""
        }
    }

    private fun getLanguageDocumentation(lang: String?): String {
        return when (lang?.lowercase()?.trim()) {
            "javascript", "js" -> loadDoc("broker-script-javascript.md")
            "python", "py" -> loadDoc("broker-script-python.md")
            else -> loadDoc("broker-script-python.md")
        }
    }

    private fun getLanguageSkill(lang: String?): String {
        return when (lang?.lowercase()?.trim()) {
            "javascript", "js" -> loadDoc("broker-script-javascript-skill.md")
            "python", "py" -> loadDoc("broker-script-python-skill.md")
            else -> loadDoc("broker-script-python-skill.md")
        }
    }

    fun scriptLanguages(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher { _ ->
            val future = CompletableFuture<List<Map<String, Any?>>>()
            if (!Monster.isFeatureEnabled(Features.PythonScripts)) {
                return@DataFetcher future.apply { complete(emptyList()) }
            }
            val list = listOf(
                mapOf(
                    "name" to "python",
                    "displayName" to "Python (GraalPy / Truffle)",
                    "description" to "Full Python 3 runtime powered by GraalVM Truffle.",
                    "isDefault" to true,
                    "documentation" to getLanguageDocumentation("python"),
                    "skill" to getLanguageSkill("python")
                ),
                mapOf(
                    "name" to "javascript",
                    "displayName" to "JavaScript (GraalJS / Truffle)",
                    "description" to "Modern ECMAScript JavaScript runtime powered by GraalJS.",
                    "isDefault" to false,
                    "documentation" to getLanguageDocumentation("javascript"),
                    "skill" to getLanguageSkill("javascript")
                )
            )
            future.complete(list)
            future
        }
    }

    fun scriptDocumentation(): DataFetcher<CompletableFuture<String>> {
        return DataFetcher { env ->
            val future = CompletableFuture<String>()
            if (!Monster.isFeatureEnabled(Features.PythonScripts)) {
                return@DataFetcher future.apply { complete("") }
            }
            val lang = env.getArgument<String>("language")
            future.complete(getLanguageDocumentation(lang))
            future
        }
    }

    fun scriptSkill(): DataFetcher<CompletableFuture<String>> {
        return DataFetcher { env ->
            val future = CompletableFuture<String>()
            if (!Monster.isFeatureEnabled(Features.PythonScripts)) {
                return@DataFetcher future.apply { complete("") }
            }
            val lang = env.getArgument<String>("language")
            future.complete(getLanguageSkill(lang))
            future
        }
    }

    companion object {
        private val isoFormatter = DateTimeFormatter.ISO_INSTANT

        fun deviceToScript(device: DeviceConfig, currentNodeId: String): Map<String, Any?> {
            val cfg = try {
                ScriptConfig.fromJsonObject(device.config)
            } catch (e: Exception) {
                ScriptConfig()
            }

            val connector = ScriptExtension.getInstance()?.getConnector(device.name)
            val execCount = connector?.executionCount?.get() ?: 0L
            val errCount = connector?.errorCount?.get() ?: 0L
            val lastTime = connector?.lastExecutionTime
            val lastStatus = connector?.lastExecutionStatus
            val logs = connector?.recentLogs?.getLogs() ?: emptyList()

            val configMap = mapOf(
                "language" to cfg.language,
                "script" to cfg.script,
                "triggerType" to cfg.triggerType,
                "topicFilters" to cfg.topicFilters,
                "triggerOnChangeOnly" to cfg.triggerOnChangeOnly,
                "timerIntervalMs" to cfg.timerIntervalMs,
                "instanceMode" to cfg.instanceMode,
                "timeoutMs" to cfg.timeoutMs,
                "description" to cfg.description
            )

            return mapOf(
                "name" to device.name,
                "namespace" to device.namespace,
                "nodeId" to device.nodeId,
                "enabled" to device.enabled,
                "config" to configMap,
                "createdAt" to isoFormatter.format(device.createdAt.atZone(ZoneOffset.UTC)),
                "updatedAt" to isoFormatter.format(device.updatedAt.atZone(ZoneOffset.UTC)),
                "isOnCurrentNode" to (device.nodeId == "*" || device.nodeId == "local" || device.nodeId == currentNodeId),
                "executionCount" to execCount,
                "errorCount" to errCount,
                "lastExecutionTime" to lastTime,
                "lastExecutionStatus" to lastStatus,
                "recentLogs" to logs
            )
        }
    }
}
