package at.rocworks.graphql

import at.rocworks.Features
import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.devices.script.ScriptEngine
import at.rocworks.devices.script.ScriptExtension
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.ScriptConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL mutations for standalone Script devices.
 */
class ScriptMutations(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(ScriptMutations::class.java)

    private fun failResult(msg: String): Map<String, Any?> = mapOf(
        "script" to null,
        "success" to false,
        "errors" to listOf(msg)
    )

    fun create(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            if (!Monster.isFeatureEnabled(Features.PythonScripts)) {
                return@DataFetcher future.apply { complete(failResult("PythonScripts feature is not enabled")) }
            }

            try {
                val input = env.getArgument<Map<String, Any>>("input")!!
                val name = (input["name"] as String).trim()
                if (name.isEmpty()) {
                    return@DataFetcher future.apply { complete(failResult("Script name cannot be empty")) }
                }

                deviceStore.getDevice(name).onComplete { checkRes ->
                    if (checkRes.succeeded() && checkRes.result() != null) {
                        future.complete(failResult("Script '$name' already exists"))
                        return@onComplete
                    }

                    val namespace = (input["namespace"] as? String)?.ifBlank { "script" } ?: "script"
                    val nodeId = (input["nodeId"] as? String)?.ifBlank { "*" } ?: "*"
                    val enabled = input["enabled"] as? Boolean ?: true
                    @Suppress("UNCHECKED_CAST")
                    val configInput = input["config"] as Map<String, Any>
                    val scriptConfig = parseScriptConfig(configInput)

                    // Validate compilation
                    try {
                        ScriptEngine(name, scriptConfig)
                    } catch (e: Exception) {
                        future.complete(failResult("Script compilation error: ${e.message}"))
                        return@onComplete
                    }

                    val device = DeviceConfig(
                        name = name,
                        namespace = namespace,
                        nodeId = nodeId,
                        type = DeviceConfig.DEVICE_TYPE_SCRIPT,
                        enabled = enabled,
                        config = scriptConfig.toJsonObject(),
                        createdAt = Instant.now(),
                        updatedAt = Instant.now()
                    )

                    deviceStore.saveDevice(device).onComplete { saveRes ->
                        if (saveRes.succeeded()) {
                            notifyConfigChange("create", name, enabled)
                            val currentNodeId = Monster.getClusterNodeId(vertx)
                            future.complete(mapOf(
                                "script" to ScriptQueries.deviceToScript(device, currentNodeId),
                                "success" to true,
                                "errors" to emptyList<String>()
                            ))
                        } else {
                            future.complete(failResult("Failed to save script: ${saveRes.cause()?.message}"))
                        }
                    }
                }
            } catch (e: Exception) {
                future.complete(failResult("Error creating script: ${e.message}"))
            }

            future
        }
    }

    fun update(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            if (!Monster.isFeatureEnabled(Features.PythonScripts)) {
                return@DataFetcher future.apply { complete(failResult("PythonScripts feature is not enabled")) }
            }

            try {
                val name = env.getArgument<String>("name") ?: ""
                val input = env.getArgument<Map<String, Any>>("input")!!

                if (name.isBlank()) {
                    return@DataFetcher future.apply { complete(failResult("Script name cannot be blank")) }
                }

                deviceStore.getDevice(name).onComplete { checkRes ->
                    if (checkRes.failed() || checkRes.result() == null || checkRes.result()!!.type != DeviceConfig.DEVICE_TYPE_SCRIPT) {
                        future.complete(failResult("Script '$name' not found"))
                        return@onComplete
                    }

                    val existing = checkRes.result()!!
                    val namespace = (input["namespace"] as? String)?.ifBlank { existing.namespace } ?: existing.namespace
                    val nodeId = (input["nodeId"] as? String)?.ifBlank { existing.nodeId } ?: existing.nodeId
                    val enabled = input["enabled"] as? Boolean ?: existing.enabled
                    @Suppress("UNCHECKED_CAST")
                    val configInput = input["config"] as Map<String, Any>
                    val scriptConfig = parseScriptConfig(configInput)

                    // Validate compilation
                    try {
                        ScriptEngine(name, scriptConfig)
                    } catch (e: Exception) {
                        future.complete(failResult("Script compilation error: ${e.message}"))
                        return@onComplete
                    }

                    val updated = existing.copy(
                        namespace = namespace,
                        nodeId = nodeId,
                        enabled = enabled,
                        config = scriptConfig.toJsonObject(),
                        updatedAt = Instant.now()
                    )

                    deviceStore.saveDevice(updated).onComplete { saveRes ->
                        if (saveRes.succeeded()) {
                            notifyConfigChange("update", name, enabled)
                            val currentNodeId = Monster.getClusterNodeId(vertx)
                            future.complete(mapOf(
                                "script" to ScriptQueries.deviceToScript(updated, currentNodeId),
                                "success" to true,
                                "errors" to emptyList<String>()
                            ))
                        } else {
                            future.complete(failResult("Failed to update script: ${saveRes.cause()?.message}"))
                        }
                    }
                }
            } catch (e: Exception) {
                future.complete(failResult("Error updating script: ${e.message}"))
            }

            future
        }
    }

    fun delete(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Boolean>()
            if (!Monster.isFeatureEnabled(Features.PythonScripts)) {
                return@DataFetcher future.apply { complete(false) }
            }

            val name = env.getArgument<String>("name") ?: ""
            deviceStore.getDevice(name).onComplete { checkRes ->
                if (checkRes.succeeded() && checkRes.result() != null && checkRes.result()!!.type == DeviceConfig.DEVICE_TYPE_SCRIPT) {
                    deviceStore.deleteDevice(name).onComplete { delRes ->
                        if (delRes.succeeded()) {
                            notifyConfigChange("delete", name, false)
                            future.complete(true)
                        } else {
                            future.complete(false)
                        }
                    }
                } else {
                    future.complete(false)
                }
            }

            future
        }
    }

    fun toggle(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            if (!Monster.isFeatureEnabled(Features.PythonScripts)) {
                return@DataFetcher future.apply { complete(failResult("PythonScripts feature is not enabled")) }
            }

            val name = env.getArgument<String>("name") ?: ""
            val enabled = env.getArgument<Boolean>("enabled") ?: false

            deviceStore.getDevice(name).onComplete { checkRes ->
                if (checkRes.succeeded() && checkRes.result() != null && checkRes.result()!!.type == DeviceConfig.DEVICE_TYPE_SCRIPT) {
                    val existing = checkRes.result()!!
                    val updated = existing.copy(enabled = enabled, updatedAt = Instant.now())

                    deviceStore.saveDevice(updated).onComplete { saveRes ->
                        if (saveRes.succeeded()) {
                            notifyConfigChange("toggle", name, enabled)
                            val currentNodeId = Monster.getClusterNodeId(vertx)
                            future.complete(mapOf(
                                "script" to ScriptQueries.deviceToScript(updated, currentNodeId),
                                "success" to true,
                                "errors" to emptyList<String>()
                            ))
                        } else {
                            future.complete(failResult("Failed to toggle script: ${saveRes.cause()?.message}"))
                        }
                    }
                } else {
                    future.complete(failResult("Script '$name' not found"))
                }
            }

            future
        }
    }

    fun start(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val toggleFetcher = toggle()
            val name = env.getArgument<String>("name")
            val modifiedEnv = graphql.schema.DataFetchingEnvironmentImpl.newDataFetchingEnvironment(env)
                .arguments(mapOf("name" to name, "enabled" to true))
                .build()
            toggleFetcher.get(modifiedEnv)
        }
    }

    fun stop(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val toggleFetcher = toggle()
            val name = env.getArgument<String>("name")
            val modifiedEnv = graphql.schema.DataFetchingEnvironmentImpl.newDataFetchingEnvironment(env)
                .arguments(mapOf("name" to name, "enabled" to false))
                .build()
            toggleFetcher.get(modifiedEnv)
        }
    }

    fun test(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            if (!Monster.isFeatureEnabled(Features.PythonScripts)) {
                return@DataFetcher future.apply {
                    complete(mapOf(
                        "success" to false,
                        "returnValue" to null,
                        "outputMessages" to emptyList<Any>(),
                        "logs" to emptyList<String>(),
                        "errors" to listOf("PythonScripts feature is not enabled"),
                        "executionTimeMs" to 0f
                    ))
                }
            }

            try {
                val input = env.getArgument<Map<String, Any>>("input")!!
                val testTopic = env.getArgument<String>("testTopic")
                val testPayload = env.getArgument<String>("testPayload")
                val testArgsStr = env.getArgument<String>("testArgs")

                val name = input["name"] as? String ?: "TestScript"
                @Suppress("UNCHECKED_CAST")
                val configInput = input["config"] as Map<String, Any>
                val scriptConfig = parseScriptConfig(configInput)

                val parsedArgs = if (!testArgsStr.isNullOrBlank()) {
                    try {
                        JsonObject(testArgsStr).map
                    } catch (e: Exception) {
                        return@DataFetcher future.apply {
                            complete(mapOf(
                                "success" to false,
                                "returnValue" to null,
                                "outputMessages" to emptyList<Any>(),
                                "logs" to emptyList<String>(),
                                "errors" to listOf("testArgs is not valid JSON: ${e.message}"),
                                "executionTimeMs" to 0f
                            ))
                        }
                    }
                } else null

                // Run sandbox execution on worker thread
                vertx.executeBlocking(java.util.concurrent.Callable {
                    val ext = ScriptExtension.getInstance()
                    val res = if (ext != null) {
                        ext.testScript(name, scriptConfig, testTopic, testPayload, parsedArgs)
                    } else {
                        val transientExt = ScriptExtension()
                        transientExt.testScript(name, scriptConfig, testTopic, testPayload, parsedArgs)
                    }

                    val outputMsgs = res.outputMessages.map { m ->
                        mapOf(
                            "topic" to m.topic,
                            "payload" to m.payload,
                            "qos" to m.qos,
                            "retain" to m.retain
                        )
                    }

                    mapOf(
                        "success" to res.success,
                        "returnValue" to res.returnValue?.toString(),
                        "outputMessages" to outputMsgs,
                        "logs" to res.logs,
                        "errors" to res.errors,
                        "executionTimeMs" to res.executionTimeMs
                    )
                }).onComplete { res ->
                    if (res.succeeded()) future.complete(res.result())
                    else future.complete(mapOf(
                        "success" to false,
                        "returnValue" to null,
                        "outputMessages" to emptyList<Any>(),
                        "logs" to emptyList<String>(),
                        "errors" to listOf("Test execution error: ${res.cause()?.message}"),
                        "executionTimeMs" to 0f
                    ))
                }

            } catch (e: Exception) {
                future.complete(mapOf(
                    "success" to false,
                    "returnValue" to null,
                    "outputMessages" to emptyList<Any>(),
                    "logs" to emptyList<String>(),
                    "errors" to listOf("Test setup error: ${e.message}"),
                    "executionTimeMs" to 0f
                ))
            }

            future
        }
    }

    private fun parseScriptConfig(map: Map<String, Any>): ScriptConfig {
        @Suppress("UNCHECKED_CAST")
        val filters = (map["topicFilters"] as? List<String>) ?: emptyList()
        return ScriptConfig(
            language = (map["language"] as? String) ?: ScriptConfig.DEFAULT_LANGUAGE,
            script = (map["script"] as? String) ?: "",
            triggerType = (map["triggerType"] as? String) ?: ScriptConfig.TRIGGER_TOPIC,
            topicFilters = filters,
            triggerOnChangeOnly = map["triggerOnChangeOnly"] as? Boolean ?: false,
            timerIntervalMs = (map["timerIntervalMs"] as? Number)?.toInt() ?: 0,
            instanceMode = (map["instanceMode"] as? String) ?: ScriptConfig.MODE_SINGLETON,
            timeoutMs = (map["timeoutMs"] as? Number)?.toInt() ?: ScriptConfig.DEFAULT_TIMEOUT_MS,
            description = map["description"] as? String
        )
    }

    private fun notifyConfigChange(op: String, name: String, enabled: Boolean) {
        val json = JsonObject().put("operation", op).put("name", name).put("enabled", enabled)
        vertx.eventBus().publish(ScriptExtension.ADDRESS_DEVICE_CONFIG_CHANGED, json)
    }
}
