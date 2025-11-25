package at.rocworks.extensions.graphql

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.stores.*
import at.rocworks.stores.postgres.*
import at.rocworks.stores.cratedb.*
import at.rocworks.stores.mongodb.*
import at.rocworks.stores.sqlite.*
import graphql.schema.DataFetcher
import graphql.schema.DataFetchingEnvironment
import at.rocworks.handlers.ArchiveGroup
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.core.json.JsonArray
import java.util.concurrent.CompletableFuture

class ArchiveGroupResolver(
    private val vertx: Vertx,
    private val archiveHandler: ArchiveHandler,
    private val authContext: GraphQLAuthContext
) {
    private val logger = Utils.getLogger(this::class.java)

    // Query Resolvers

    fun archiveGroups(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any?>>>()

            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future

            // Extract filter parameters
            val enabledFilter = env.getArgument<Boolean?>("enabled")
            val lastValTypeEquals = env.getArgument<String?>("lastValTypeEquals")
            val lastValTypeNotEquals = env.getArgument<String?>("lastValTypeNotEquals")

            try {
                val configStore = archiveHandler.getConfigStore()
                if (configStore == null) {
                    // No database config store - return runtime status from ArchiveHandler
                    val archiveGroupsJson = archiveHandler.listArchiveGroups()
                    var result = archiveGroupsJson.list.map { json ->
                        val jsonObj = json as JsonObject
                        mapOf(
                            "name" to jsonObj.getString("name"),
                            "enabled" to jsonObj.getBoolean("enabled"),
                            "deployed" to jsonObj.getBoolean("deployed"),
                            "deploymentId" to jsonObj.getString("deploymentId"),
                            "topicFilter" to jsonObj.getJsonArray("topicFilter")?.list,
                            "retainedOnly" to jsonObj.getBoolean("retainedOnly"),
                            "lastValType" to jsonObj.getString("lastValType"),
                            "archiveType" to jsonObj.getString("archiveType"),
                            "lastValRetention" to jsonObj.getString("lastValRetention"),
                            "archiveRetention" to jsonObj.getString("archiveRetention"),
                             "purgeInterval" to jsonObj.getString("purgeInterval"),
                              "payloadFormat" to jsonObj.getString("payloadFormat", "DEFAULT")
                        )
                    }

                    // Apply filters
                    if (enabledFilter != null) {
                        result = result.filter { it["enabled"] == enabledFilter }
                    }
                    if (lastValTypeEquals != null) {
                        result = result.filter { it["lastValType"] == lastValTypeEquals }
                    }
                    if (lastValTypeNotEquals != null) {
                        result = result.filter { it["lastValType"] != lastValTypeNotEquals }
                    }

                    future.complete(result)
                } else {
                    // Get from database using async call
                    configStore.getAllArchiveGroups().onComplete { asyncResult ->
                        if (asyncResult.succeeded()) {
                            try {
                                val archiveGroupConfigs = asyncResult.result()
                                var result = archiveGroupConfigs.map { config ->
                                    // Get runtime status for this archive group
                                    val runtimeStatus = archiveHandler.getArchiveGroupStatus(config.archiveGroup.name)
                                    mapOf(
                                        "name" to config.archiveGroup.name,
                                        "enabled" to config.enabled,
                                        "deployed" to runtimeStatus.getBoolean("deployed", false),
                                        "deploymentId" to runtimeStatus.getString("deploymentId"),
                                        "topicFilter" to config.archiveGroup.topicFilter,
                                        "retainedOnly" to config.archiveGroup.retainedOnly,
                                        "lastValType" to config.archiveGroup.getLastValType().name,
                                        "archiveType" to config.archiveGroup.getArchiveType().name,
                                        "lastValRetention" to config.archiveGroup.getLastValRetention(),
                                        "archiveRetention" to config.archiveGroup.getArchiveRetention(),
                                         "purgeInterval" to config.archiveGroup.getPurgeInterval(),
                                          "payloadFormat" to config.archiveGroup.payloadFormat.name
                                    )
                                }

                                // Apply filters
                                if (enabledFilter != null) {
                                    result = result.filter { it["enabled"] == enabledFilter }
                                }
                                if (lastValTypeEquals != null) {
                                    result = result.filter { it["lastValType"] == lastValTypeEquals }
                                }
                                if (lastValTypeNotEquals != null) {
                                    result = result.filter { it["lastValType"] != lastValTypeNotEquals }
                                }

                                future.complete(result)
                            } catch (e: Exception) {
                                future.completeExceptionally(e)
                            }
                        } else {
                            future.completeExceptionally(asyncResult.cause())
                        }
                    }
                }
            } catch (e: Exception) {
                future.completeExceptionally(e)
            }

            future
        }
    }

    fun archiveGroup(): DataFetcher<CompletableFuture<Map<String, Any?>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>?>()

            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future

            val name = env.getArgument<String>("name") ?: ""

            try {
                val configStore = archiveHandler.getConfigStore()
                if (configStore == null) {
                    // No database config store - get runtime status from ArchiveHandler
                    val runtimeStatus = archiveHandler.getArchiveGroupStatus(name)
                    val result = if (!runtimeStatus.isEmpty) {
                        mapOf(
                            "name" to runtimeStatus.getString("name"),
                            "enabled" to runtimeStatus.getBoolean("enabled"),
                            "deployed" to runtimeStatus.getBoolean("deployed"),
                            "deploymentId" to runtimeStatus.getString("deploymentId"),
                            "topicFilter" to runtimeStatus.getJsonArray("topicFilter")?.list,
                            "retainedOnly" to runtimeStatus.getBoolean("retainedOnly"),
                            "lastValType" to runtimeStatus.getString("lastValType"),
                            "archiveType" to runtimeStatus.getString("archiveType"),
                            "lastValRetention" to runtimeStatus.getString("lastValRetention"),
                            "archiveRetention" to runtimeStatus.getString("archiveRetention"),
                             "purgeInterval" to runtimeStatus.getString("purgeInterval"),
                              "payloadFormat" to runtimeStatus.getString("payloadFormat", "DEFAULT")
                        )
                    } else {
                        null
                    }
                    future.complete(result)
                } else {
                    // Get from database using async call
                    configStore.getArchiveGroup(name).onComplete { asyncResult ->
                        if (asyncResult.succeeded()) {
                            try {
                                val archiveGroup = asyncResult.result()
                                val result = if (archiveGroup != null) {
                                    val runtimeStatus = archiveHandler.getArchiveGroupStatus(name)
                                    mapOf(
                                        "name" to archiveGroup.archiveGroup.name,
                                        "enabled" to archiveGroup.enabled,
                                        "deployed" to runtimeStatus.getBoolean("deployed", false),
                                        "deploymentId" to runtimeStatus.getString("deploymentId"),
                                        "topicFilter" to archiveGroup.archiveGroup.topicFilter,
                                        "retainedOnly" to archiveGroup.archiveGroup.retainedOnly,
                                        "lastValType" to archiveGroup.archiveGroup.getLastValType().name,
                                        "archiveType" to archiveGroup.archiveGroup.getArchiveType().name,
                                        "lastValRetention" to archiveGroup.archiveGroup.getLastValRetention(),
                                        "archiveRetention" to archiveGroup.archiveGroup.getArchiveRetention(),
                                         "purgeInterval" to archiveGroup.archiveGroup.getPurgeInterval(),
                                         "payloadFormat" to archiveGroup.archiveGroup.payloadFormat.name
                                    )
                                } else {
                                    null
                                }
                                future.complete(result)
                            } catch (e: Exception) {
                                future.completeExceptionally(e)
                            }
                        } else {
                            future.completeExceptionally(asyncResult.cause())
                        }
                    }
                }
            } catch (e: Exception) {
                future.completeExceptionally(e)
            }

            future
        }
    }

    // Mutation Resolvers

    fun createArchiveGroup(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()

            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future

            val input = env.getArgument<Map<String, Any?>>("input") ?: mapOf()

            try {
                val configStore = archiveHandler.getConfigStore()
                if (configStore == null) {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "No ConfigStore available - archive groups must be configured in YAML"
                    ))
                    return@DataFetcher future
                }

                // Extract and validate input
                val name = input["name"] as? String ?: run {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "Invalid input: Name is required"
                    ))
                    return@DataFetcher future
                }
                val topicFilter = (input["topicFilter"] as? List<*>)?.map { it.toString() } ?: run {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "Invalid input: Topic filter is required"
                    ))
                    return@DataFetcher future
                }
                val retainedOnly = input["retainedOnly"] as? Boolean ?: false
                val lastValTypeStr = input["lastValType"] as? String ?: run {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "Invalid input: LastValType is required"
                    ))
                    return@DataFetcher future
                }
                val archiveTypeStr = input["archiveType"] as? String ?: run {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "Invalid input: ArchiveType is required"
                    ))
                    return@DataFetcher future
                }
                 val payloadFormatStr = input["payloadFormat"] as? String ?: "DEFAULT"

                // Parse optional durations
                val lastValRetention = input["lastValRetention"] as? String
                val archiveRetention = input["archiveRetention"] as? String
                val purgeInterval = input["purgeInterval"] as? String

                // Parse enum values
                val lastValType = try {
                    MessageStoreType.valueOf(lastValTypeStr)
                } catch (e: Exception) {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "Invalid input: Invalid lastValType: $lastValTypeStr"
                    ))
                    return@DataFetcher future
                }

                val archiveType = try {
                    MessageArchiveType.valueOf(archiveTypeStr)
                } catch (e: Exception) {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "Invalid input: Invalid archiveType: $archiveTypeStr"
                    ))
                    return@DataFetcher future
                }

                 val payloadFormat = PayloadFormat.parse(payloadFormatStr)

                // Parse durations to milliseconds
                val lastValRetentionMs = try {
                    lastValRetention?.let { Utils.parseDuration(it) }
                } catch (e: Exception) {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "Invalid input: Invalid lastValRetention duration: $lastValRetention"
                    ))
                    return@DataFetcher future
                }
                val archiveRetentionMs = try {
                    archiveRetention?.let { Utils.parseDuration(it) }
                } catch (e: Exception) {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "Invalid input: Invalid archiveRetention duration: $archiveRetention"
                    ))
                    return@DataFetcher future
                }
                val purgeIntervalMs = try {
                    purgeInterval?.let { Utils.parseDuration(it) }
                } catch (e: Exception) {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "Invalid input: Invalid purgeInterval duration: $purgeInterval"
                    ))
                    return@DataFetcher future
                }

                // Create the ArchiveGroup
                val archiveGroup = ArchiveGroup(
                    name = name,
                    topicFilter = topicFilter,
                    retainedOnly = retainedOnly,
                    lastValType = lastValType,
                    archiveType = archiveType,
                    payloadFormat = payloadFormat,
                    lastValRetentionMs = lastValRetentionMs,
                    archiveRetentionMs = archiveRetentionMs,
                    purgeIntervalMs = purgeIntervalMs,
                    lastValRetentionStr = lastValRetention,
                    archiveRetentionStr = archiveRetention,
                    purgeIntervalStr = purgeInterval,
                    databaseConfig = JsonObject() // Will be populated from config
                )

                // Save to database (disabled by default) using async call
                configStore.saveArchiveGroup(archiveGroup, enabled = false).onComplete { asyncResult ->
                    if (asyncResult.succeeded() && asyncResult.result()) {
                        future.complete(mapOf(
                            "success" to true,
                            "message" to "Archive group '$name' created successfully (disabled by default)",
                                         "archiveGroup" to mapOf(
                                             "name" to name,
                                             "enabled" to false,
                                             "deployed" to false,
                                             "topicFilter" to topicFilter,
                                             "retainedOnly" to retainedOnly,
                                             "lastValType" to lastValType.name,
                                             "archiveType" to archiveType.name,
                                              "payloadFormat" to payloadFormat.name,
                                 "lastValRetention" to lastValRetention,
                                 "archiveRetention" to archiveRetention,
                                 "purgeInterval" to purgeInterval
                             )
                        ))
                    } else {
                        future.complete(mapOf(
                            "success" to false,
                            "message" to "Failed to save archive group to database"
                        ))
                    }
                }
            } catch (e: Exception) {
                future.complete(mapOf(
                    "success" to false,
                    "message" to "Error creating archive group: ${e.message}"
                ))
            }

            future
        }
    }

    fun updateArchiveGroup(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()

            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future

            val input = env.getArgument<Map<String, Any?>>("input") ?: mapOf()

            try {
                val configStore = archiveHandler.getConfigStore()
                if (configStore == null) {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "No ConfigStore available - cannot update archive group"
                    ))
                    return@DataFetcher future
                }

                // Extract name from input
                val name = input["name"] as? String ?: run {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "Invalid input: Name is required"
                    ))
                    return@DataFetcher future
                }

                // First, check if the archive group exists
                configStore.getArchiveGroup(name).onComplete { asyncResult ->
                    if (asyncResult.succeeded()) {
                        val existingGroup = asyncResult.result()
                        if (existingGroup == null) {
                            future.complete(mapOf(
                                "success" to false,
                                "message" to "Archive group '$name' not found"
                            ))
                            return@onComplete
                        }

                        // Check if the archive group is disabled (must be disabled to update)
                        if (existingGroup.enabled) {
                            future.complete(mapOf(
                                "success" to false,
                                "message" to "Archive group '$name' must be disabled before it can be updated. Use disableArchiveGroup mutation first."
                            ))
                            return@onComplete
                        }

                        // Also check if it's currently deployed/running (extra safety check)
                        val runtimeStatus = archiveHandler.getArchiveGroupStatus(name)
                        if (runtimeStatus.getBoolean("deployed", false)) {
                            future.complete(mapOf(
                                "success" to false,
                                "message" to "Archive group '$name' is currently running. Please disable it first before updating."
                            ))
                            return@onComplete
                        }

                        try {
                            // Extract and validate updated values (keeping existing values if not provided)
                            val existingArchiveGroup = existingGroup.archiveGroup

                            val topicFilter = (input["topicFilter"] as? List<*>)?.map { it.toString() }
                                ?: existingArchiveGroup.topicFilter
                            val retainedOnly = input["retainedOnly"] as? Boolean
                                ?: existingArchiveGroup.retainedOnly

                            // Parse enum values if provided, otherwise keep existing
                            val lastValTypeStr = input["lastValType"] as? String
                            val lastValType = if (lastValTypeStr != null) {
                                try {
                                    MessageStoreType.valueOf(lastValTypeStr)
                                } catch (e: Exception) {
                                    future.complete(mapOf(
                                        "success" to false,
                                        "message" to "Invalid input: Invalid lastValType: $lastValTypeStr"
                                    ))
                                    return@onComplete
                                }
                            } else {
                                existingArchiveGroup.getLastValType()
                            }

                            val archiveTypeStr = input["archiveType"] as? String
                            val archiveType = if (archiveTypeStr != null) {
                                try {
                                    MessageArchiveType.valueOf(archiveTypeStr)
                                } catch (e: Exception) {
                                    future.complete(mapOf(
                                        "success" to false,
                                        "message" to "Invalid input: Invalid archiveType: $archiveTypeStr"
                                    ))
                                    return@onComplete
                                }
                             } else {
                                 existingArchiveGroup.getArchiveType()
                             }

                             val payloadFormatStr = input["payloadFormat"] as? String
                             val payloadFormat = if (payloadFormatStr != null) {
                                 PayloadFormat.parse(payloadFormatStr)
                             } else {
                                 existingArchiveGroup.payloadFormat
                             }

                             // Parse optional durations if provided, otherwise keep existing
                            val lastValRetention = input["lastValRetention"] as? String
                            val archiveRetention = input["archiveRetention"] as? String
                            val purgeInterval = input["purgeInterval"] as? String

                            val lastValRetentionMs = if (lastValRetention != null) {
                                try {
                                    Utils.parseDuration(lastValRetention)
                                } catch (e: Exception) {
                                    future.complete(mapOf(
                                        "success" to false,
                                        "message" to "Invalid input: Invalid lastValRetention duration: $lastValRetention"
                                    ))
                                    return@onComplete
                                }
                            } else {
                                existingArchiveGroup.getLastValRetentionMs()
                            }
                            val archiveRetentionMs = if (archiveRetention != null) {
                                try {
                                    Utils.parseDuration(archiveRetention)
                                } catch (e: Exception) {
                                    future.complete(mapOf(
                                        "success" to false,
                                        "message" to "Invalid input: Invalid archiveRetention duration: $archiveRetention"
                                    ))
                                    return@onComplete
                                }
                            } else {
                                existingArchiveGroup.getArchiveRetentionMs()
                            }
                            val purgeIntervalMs = if (purgeInterval != null) {
                                try {
                                    Utils.parseDuration(purgeInterval)
                                } catch (e: Exception) {
                                    future.complete(mapOf(
                                        "success" to false,
                                        "message" to "Invalid input: Invalid purgeInterval duration: $purgeInterval"
                                    ))
                                    return@onComplete
                                }
                            } else {
                                existingArchiveGroup.getPurgeIntervalMs()
                            }

                             // Create the updated ArchiveGroup
                             val updatedArchiveGroup = ArchiveGroup(
                                 name = name,
                                 topicFilter = topicFilter,
                                 retainedOnly = retainedOnly,
                                 lastValType = lastValType,
                                 archiveType = archiveType,
                                 payloadFormat = payloadFormat,
                                 lastValRetentionMs = lastValRetentionMs,
                                 archiveRetentionMs = archiveRetentionMs,
                                 purgeIntervalMs = purgeIntervalMs,
                                 lastValRetentionStr = lastValRetention ?: existingArchiveGroup.getLastValRetention(),
                                 archiveRetentionStr = archiveRetention ?: existingArchiveGroup.getArchiveRetention(),
                                 purgeIntervalStr = purgeInterval ?: existingArchiveGroup.getPurgeInterval(),
                                 databaseConfig = JsonObject() // Will be populated from config
                             )

                            // Update in database (keep the same enabled state - false) using async call
                            configStore.updateArchiveGroup(updatedArchiveGroup, enabled = false).onComplete { updateResult ->
                                if (updateResult.succeeded() && updateResult.result()) {
                                    future.complete(mapOf(
                                        "success" to true,
                                        "message" to "Archive group '$name' updated successfully",
                                        "archiveGroup" to mapOf(
                                            "name" to name,
                                            "enabled" to false,
                                            "deployed" to false,
                                            "topicFilter" to topicFilter,
                                            "retainedOnly" to retainedOnly,
                                            "lastValType" to lastValType.name,
                                            "archiveType" to archiveType.name,
                                            "lastValRetention" to lastValRetention,
                                            "archiveRetention" to archiveRetention,
                                            "purgeInterval" to purgeInterval
                                        )
                                    ))
                                } else {
                                    future.complete(mapOf(
                                        "success" to false,
                                        "message" to "Failed to update archive group in database"
                                    ))
                                }
                            }
                        } catch (e: Exception) {
                            future.complete(mapOf(
                                "success" to false,
                                "message" to "Error updating archive group: ${e.message}"
                            ))
                        }
                    } else {
                        future.complete(mapOf(
                            "success" to false,
                            "message" to "Error fetching archive group: ${asyncResult.cause()?.message}"
                        ))
                    }
                }
            } catch (e: Exception) {
                future.complete(mapOf(
                    "success" to false,
                    "message" to "Error updating archive group: ${e.message}"
                ))
            }

            future
        }
    }

    fun deleteArchiveGroup(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()

            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future

            val name = env.getArgument<String>("name") ?: ""

            try {
                val configStore = archiveHandler.getConfigStore()
                if (configStore == null) {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "No ConfigStore available - cannot delete archive group"
                    ))
                    return@DataFetcher future
                }

                // First, check if the archive group exists using async call
                configStore.getArchiveGroup(name).onComplete { asyncResult ->
                    if (asyncResult.succeeded()) {
                        val existingGroup = asyncResult.result()
                        if (existingGroup == null) {
                            future.complete(mapOf(
                                "success" to false,
                                "message" to "Archive group '$name' not found"
                            ))
                            return@onComplete
                        }

                        // Check if the archive group is disabled (must be disabled to delete)
                        if (existingGroup.enabled) {
                            future.complete(mapOf(
                                "success" to false,
                                "message" to "Archive group '$name' must be disabled before it can be deleted. Use disableArchiveGroup mutation first."
                            ))
                            return@onComplete
                        }

                        // Also check if it's currently deployed/running (extra safety check)
                        val runtimeStatus = archiveHandler.getArchiveGroupStatus(name)
                        if (runtimeStatus.getBoolean("deployed", false)) {
                            future.complete(mapOf(
                                "success" to false,
                                "message" to "Archive group '$name' is currently running. Please disable it first before deletion."
                            ))
                            return@onComplete
                        }

                        // Delete from database first using async call
                        configStore.deleteArchiveGroup(name).onComplete { deleteResult ->
                            if (deleteResult.succeeded() && deleteResult.result()) {
                                // Note: Skip storage cleanup for now to prevent blocking
                                // Storage cleanup can be done separately as a background task
                                logger.info("Archive group '$name' deleted from database. Storage cleanup skipped to prevent blocking.")

                                future.complete(mapOf(
                                    "success" to true,
                                    "message" to "Archive group '$name' deleted successfully (storage cleanup deferred)",
                                    "archiveGroup" to mapOf(
                                        "name" to name,
                                        "enabled" to false,
                                        "deployed" to false
                                    )
                                ))
                            } else {
                                future.complete(mapOf(
                                    "success" to false,
                                    "message" to "Failed to delete archive group '$name' from database"
                                ))
                            }
                        }
                    } else {
                        future.complete(mapOf(
                            "success" to false,
                            "message" to "Error fetching archive group: ${asyncResult.cause()?.message}"
                        ))
                    }
                }
            } catch (e: Exception) {
                logger.warning("Error deleting archive group '$name': ${e.message}")
                future.complete(mapOf(
                    "success" to false,
                    "message" to "Error deleting archive group: ${e.message}"
                ))
            }

            future
        }
    }

    fun enableArchiveGroup(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()

            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future

            val name = env.getArgument<String>("name") ?: ""

            try {
                val configStore = archiveHandler.getConfigStore()
                if (configStore == null) {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "No ConfigStore available - cannot enable archive group"
                    ))
                    return@DataFetcher future
                }

                // First, check if the archive group exists using async call
                configStore.getArchiveGroup(name).onComplete { asyncResult ->
                    if (asyncResult.succeeded()) {
                        val existingGroup = asyncResult.result()
                        if (existingGroup == null) {
                            future.complete(mapOf(
                                "success" to false,
                                "message" to "Archive group '$name' not found"
                            ))
                            return@onComplete
                        }

                        // Update the enabled flag in database using async call
                        configStore.updateArchiveGroup(existingGroup.archiveGroup, enabled = true).onComplete { updateResult ->
                            if (updateResult.succeeded() && updateResult.result()) {
                                // Now start the archive group asynchronously
                                archiveHandler.startArchiveGroup(name).onComplete { startResult ->
                                    if (startResult.succeeded() && startResult.result()) {
                                        future.complete(mapOf(
                                            "success" to true,
                                            "message" to "Archive group '$name' enabled and started successfully",
                                            "archiveGroup" to mapOf(
                                                "name" to name,
                                                "enabled" to true,
                                                "deployed" to true
                                            )
                                        ))
                                    } else {
                                        // If start failed, revert the database change using async call
                                        configStore.updateArchiveGroup(existingGroup.archiveGroup, enabled = false).onComplete { revertResult ->
                                            future.complete(mapOf(
                                                "success" to false,
                                                "message" to "Failed to start archive group '$name': ${startResult.cause()?.message ?: "Unknown error"}"
                                            ))
                                        }
                                    }
                                }
                            } else {
                                future.complete(mapOf(
                                    "success" to false,
                                    "message" to "Failed to update archive group in database"
                                ))
                            }
                        }
                    } else {
                        future.complete(mapOf(
                            "success" to false,
                            "message" to "Error fetching archive group: ${asyncResult.cause()?.message}"
                        ))
                    }
                }
            } catch (e: Exception) {
                future.complete(mapOf(
                    "success" to false,
                    "message" to "Error enabling archive group: ${e.message}"
                ))
            }

            future
        }
    }

    fun disableArchiveGroup(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()

            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future

            val name = env.getArgument<String>("name") ?: ""

            try {
                val configStore = archiveHandler.getConfigStore()
                if (configStore == null) {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "No ConfigStore available - cannot disable archive group"
                    ))
                    return@DataFetcher future
                }

                // First, check if the archive group exists using async call
                configStore.getArchiveGroup(name).onComplete { asyncResult ->
                    if (asyncResult.succeeded()) {
                        val existingGroup = asyncResult.result()
                        if (existingGroup == null) {
                            future.complete(mapOf(
                                "success" to false,
                                "message" to "Archive group '$name' not found"
                            ))
                            return@onComplete
                        }

                        // First stop the archive group asynchronously
                        archiveHandler.stopArchiveGroup(name).onComplete { stopResult ->
                            // Update the enabled flag in database regardless of stop result
                            // (we want to disable it in DB even if it wasn't running)
                            configStore.updateArchiveGroup(existingGroup.archiveGroup, enabled = false).onComplete { updateResult ->
                                if (updateResult.succeeded() && updateResult.result()) {
                                    future.complete(mapOf(
                                        "success" to true,
                                        "message" to if (stopResult.succeeded() && stopResult.result()) {
                                            "Archive group '$name' stopped and disabled successfully"
                                        } else {
                                            "Archive group '$name' disabled in database (was not running or stop failed)"
                                        },
                                        "archiveGroup" to mapOf(
                                            "name" to name,
                                            "enabled" to false,
                                            "deployed" to false
                                        )
                                    ))
                                } else {
                                    future.complete(mapOf(
                                        "success" to false,
                                        "message" to "Failed to update archive group in database"
                                    ))
                                }
                            }
                        }
                    } else {
                        future.complete(mapOf(
                            "success" to false,
                            "message" to "Error fetching archive group: ${asyncResult.cause()?.message}"
                        ))
                    }
                }
            } catch (e: Exception) {
                future.complete(mapOf(
                    "success" to false,
                    "message" to "Error disabling archive group: ${e.message}"
                ))
            }

            future
        }
    }

    // Helper methods

    private fun createMessageStore(storeType: MessageStoreType, storeName: String, databaseConfig: JsonObject): IMessageStore? {
        return when (storeType) {
            MessageStoreType.NONE -> MessageStoreNone
            MessageStoreType.MEMORY -> MessageStoreMemory(storeName)
            MessageStoreType.HAZELCAST -> null // Cannot create without Hazelcast instance
            MessageStoreType.POSTGRES -> {
                val postgres = databaseConfig.getJsonObject("Postgres")
                MessageStorePostgres(
                    storeName,
                    postgres.getString("Url"),
                    postgres.getString("User"),
                    postgres.getString("Pass")
                )
            }
            MessageStoreType.CRATEDB -> {
                val cratedb = databaseConfig.getJsonObject("CrateDB")
                MessageStoreCrateDB(
                    storeName,
                    cratedb.getString("Url"),
                    cratedb.getString("User"),
                    cratedb.getString("Pass")
                )
            }
            MessageStoreType.MONGODB -> {
                val mongodb = databaseConfig.getJsonObject("MongoDB")
                MessageStoreMongoDB(
                    storeName,
                    mongodb.getString("Url"),
                    mongodb.getString("Database", "monstermq")
                )
            }
            MessageStoreType.SQLITE -> {
                val sqlite = databaseConfig.getJsonObject("SQLite")
                val archiveName = storeName.removeSuffix("Lastval")
                val dbPath = "${sqlite.getString("Path", Const.SQLITE_DEFAULT_PATH)}/monstermq-${archiveName}-lastval.db"
                MessageStoreSQLite(
                    storeName,
                    dbPath
                )
            }
        }
    }

    private fun createMessageArchive(archiveType: MessageArchiveType, archiveName: String, databaseConfig: JsonObject): IMessageArchive? {
        return when (archiveType) {
            MessageArchiveType.NONE -> MessageArchiveNone
            MessageArchiveType.POSTGRES -> {
                val postgres = databaseConfig.getJsonObject("Postgres")
                MessageArchivePostgres(
                    archiveName,
                    postgres.getString("Url"),
                    postgres.getString("User"),
                    postgres.getString("Pass")
                )
            }
            MessageArchiveType.CRATEDB -> {
                val cratedb = databaseConfig.getJsonObject("CrateDB")
                MessageArchiveCrateDB(
                    archiveName,
                    cratedb.getString("Url"),
                    cratedb.getString("User"),
                    cratedb.getString("Pass")
                )
            }
            MessageArchiveType.MONGODB -> {
                val mongodb = databaseConfig.getJsonObject("MongoDB")
                MessageArchiveMongoDB(
                    archiveName,
                    mongodb.getString("Url"),
                    mongodb.getString("Database", "monstermq")
                )
            }
            MessageArchiveType.KAFKA -> {
                val kafka = databaseConfig.getJsonObject("Kafka")
                val bootstrapServers = kafka?.getString("Servers") ?: "localhost:9092"
                val kafkaConfig = kafka?.getJsonObject("Config")
                MessageArchiveKafka(archiveName, bootstrapServers, kafkaConfig)
            }
            MessageArchiveType.SQLITE -> {
                val sqlite = databaseConfig.getJsonObject("SQLite")
                val dbPath = "${sqlite.getString("Path", Const.SQLITE_DEFAULT_PATH)}/monstermq-${archiveName}-archive.db"
                MessageArchiveSQLite(
                    archiveName,
                    dbPath
                )
            }
        }
    }


    // Field Resolvers

    fun connectionStatus(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any?>>>()

            // Check authorization - requires admin privileges
            if (!checkAuthorization(env, future)) return@DataFetcher future

            try {
                val archiveGroupInfo = env.getSource<Map<String, Any?>>()
                val archiveGroupName = archiveGroupInfo?.get("name") as? String
                    ?: run {
                        future.complete(emptyList())
                        return@DataFetcher future
                    }

                logger.fine { "Getting connection status for archive group [$archiveGroupName] from all nodes" }

                // Use event bus to collect connection status from all nodes
                val requestData = JsonObject().put("name", archiveGroupName)

                // Collect status from all cluster nodes
                collectClusterConnectionStatus(archiveGroupName) { nodeStatuses ->
                    logger.fine { "Retrieved connection status for archive group [$archiveGroupName] from ${nodeStatuses.size} nodes" }
                    future.complete(nodeStatuses)
                }
            } catch (e: Exception) {
                logger.severe("Error getting connection status for archive group: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    private fun collectClusterConnectionStatus(archiveGroupName: String, callback: (List<Map<String, Any?>>) -> Unit) {
        // In a cluster environment, we would need to collect from all nodes
        // For now, implement single-node collection with proper cluster detection

        val nodeStatuses = mutableListOf<Map<String, Any?>>()
        val localNodeId = System.getProperty("nodeId", "local")

        // Get local node status
        val requestData = JsonObject().put("name", archiveGroupName)

        vertx.eventBus().request<JsonObject>(ArchiveHandler.ARCHIVE_CONNECTION_STATUS, requestData).onComplete { asyncResult ->
            if (asyncResult.succeeded()) {
                try {
                    val response = asyncResult.result().body()

                    // Check if this is a cluster-wide response (has connectionStatus as array)
                    // or single node response (has connectionStatus as object)
                    val connectionStatusValue = response.getValue("connectionStatus")
                    if (connectionStatusValue is JsonArray) {
                        // Multi-node response format
                        connectionStatusValue.forEach { item ->
                            val statusObj = item as JsonObject
                            nodeStatuses.add(mapOf(
                                "nodeId" to statusObj.getString("nodeId", "unknown"),
                                "messageArchive" to statusObj.getBoolean("messageArchive", false),
                                "lastValueStore" to statusObj.getBoolean("lastValueStore", false),
                                "error" to statusObj.getString("error"),
                                "timestamp" to (statusObj.getLong("timestamp") ?: System.currentTimeMillis())
                            ))
                        }
                    } else if (connectionStatusValue is JsonObject) {
                        // Single node response format (current ArchiveHandler format)
                        nodeStatuses.add(mapOf(
                            "nodeId" to localNodeId,
                            "messageArchive" to connectionStatusValue.getBoolean("messageArchive", false),
                            "lastValueStore" to connectionStatusValue.getBoolean("lastValueStore", false),
                            "error" to response.getString("error"),
                            "timestamp" to System.currentTimeMillis()
                        ))
                    } else {
                        // Fallback for any other format
                        nodeStatuses.add(mapOf(
                            "nodeId" to localNodeId,
                            "messageArchive" to false,
                            "lastValueStore" to false,
                            "error" to "Invalid response format",
                            "timestamp" to System.currentTimeMillis()
                        ))
                    }

                    callback(nodeStatuses)
                } catch (e: Exception) {
                    logger.severe("Error parsing connection status response for archive group [$archiveGroupName]: ${e.message}")
                    // Fallback to local status
                    callback(listOf(getLocalConnectionStatus(archiveGroupName)))
                }
            } else {
                logger.warning("Failed to get connection status for archive group [$archiveGroupName]: ${asyncResult.cause()?.message}")
                // Fallback to local status
                callback(listOf(getLocalConnectionStatus(archiveGroupName)))
            }
        }
    }

    private fun getLocalConnectionStatus(archiveGroupName: String): Map<String, Any?> {
        val localNodeId = System.getProperty("nodeId", "local")
        val status = archiveHandler.getArchiveGroupConnectionStatus(archiveGroupName)

        return mapOf(
            "nodeId" to localNodeId,
            "messageArchive" to (status.getJsonObject("connectionStatus")?.getBoolean("messageArchive") ?: false),
            "lastValueStore" to (status.getJsonObject("connectionStatus")?.getBoolean("lastValueStore") ?: false),
            "error" to status.getString("error"),
            "timestamp" to System.currentTimeMillis()
        )
    }

    // Helper function to check authorization
    private fun checkAuthorization(env: DataFetchingEnvironment, future: CompletableFuture<*>): Boolean {
        val authResult = authContext.validateFieldAccess(env)
        if (!authResult.allowed) {
            future.completeExceptionally(Exception(authResult.errorMessage ?: "Unauthorized"))
            return false
        }
        return true
    }
}