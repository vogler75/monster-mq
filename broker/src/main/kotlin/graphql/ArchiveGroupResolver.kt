package at.rocworks.extensions.graphql

import at.rocworks.Utils
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.stores.*
import at.rocworks.stores.postgres.*
import at.rocworks.stores.cratedb.*
import at.rocworks.stores.mongodb.*
import at.rocworks.stores.sqlite.*
import at.rocworks.utils.DurationParser
import graphql.schema.DataFetcher
import graphql.schema.DataFetchingEnvironment
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.runBlocking
import java.util.concurrent.CompletableFuture
import java.sql.DriverManager
import com.mongodb.client.MongoClients
import com.mongodb.ConnectionString

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

            vertx.executeBlocking<List<Map<String, Any?>>> {
                runBlocking {
                    try {
                        val configStore = archiveHandler.getConfigStore()
                        if (configStore == null) {
                            // No database config store - return runtime status from ArchiveHandler
                            val archiveGroupsJson = archiveHandler.listArchiveGroups()
                            archiveGroupsJson.list.map { json ->
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
                                    "purgeInterval" to jsonObj.getString("purgeInterval")
                                )
                            }
                        } else {
                            // Get from database
                            val archiveGroupConfigs = configStore.getAllArchiveGroups()
                            archiveGroupConfigs.map { config ->
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
                                    "purgeInterval" to config.archiveGroup.getPurgeInterval()
                                )
                            }
                        }
                    } catch (e: Exception) {
                        throw e
                    }
                }
            }.onComplete { result ->
                if (result.succeeded()) {
                    future.complete(result.result())
                } else {
                    future.completeExceptionally(result.cause())
                }
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

            vertx.executeBlocking<Map<String, Any?>?> {
                runBlocking {
                    try {
                        val configStore = archiveHandler.getConfigStore()
                        if (configStore == null) {
                            // No database config store - get runtime status from ArchiveHandler
                            val runtimeStatus = archiveHandler.getArchiveGroupStatus(name)
                            if (!runtimeStatus.isEmpty) {
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
                                    "purgeInterval" to runtimeStatus.getString("purgeInterval")
                                )
                            } else {
                                null
                            }
                        } else {
                            // Get from database
                            val archiveGroup = configStore.getArchiveGroup(name)
                            if (archiveGroup != null) {
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
                                    "purgeInterval" to archiveGroup.archiveGroup.getPurgeInterval()
                                )
                            } else {
                                null
                            }
                        }
                    } catch (e: Exception) {
                        throw e
                    }
                }
            }.onComplete { result ->
                if (result.succeeded()) {
                    future.complete(result.result())
                } else {
                    future.completeExceptionally(result.cause())
                }
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

            vertx.executeBlocking<Map<String, Any?>> {
                runBlocking {
                    try {
                        val configStore = archiveHandler.getConfigStore()
                        if (configStore == null) {
                            return@runBlocking mapOf(
                                "success" to false,
                                "message" to "No ConfigStore available - archive groups must be configured in YAML"
                            )
                        }

                        // Extract and validate input
                        val name = input["name"] as? String ?: throw IllegalArgumentException("Name is required")
                        val topicFilter = (input["topicFilter"] as? List<*>)?.map { it.toString() }
                            ?: throw IllegalArgumentException("Topic filter is required")
                        val retainedOnly = input["retainedOnly"] as? Boolean ?: false
                        val lastValTypeStr = input["lastValType"] as? String
                            ?: throw IllegalArgumentException("LastValType is required")
                        val archiveTypeStr = input["archiveType"] as? String
                            ?: throw IllegalArgumentException("ArchiveType is required")

                        // Parse optional durations
                        val lastValRetention = input["lastValRetention"] as? String
                        val archiveRetention = input["archiveRetention"] as? String
                        val purgeInterval = input["purgeInterval"] as? String

                        // Parse enum values
                        val lastValType = try {
                            MessageStoreType.valueOf(lastValTypeStr)
                        } catch (e: Exception) {
                            throw IllegalArgumentException("Invalid lastValType: $lastValTypeStr")
                        }

                        val archiveType = try {
                            MessageArchiveType.valueOf(archiveTypeStr)
                        } catch (e: Exception) {
                            throw IllegalArgumentException("Invalid archiveType: $archiveTypeStr")
                        }

                        // Parse durations to milliseconds
                        val lastValRetentionMs = lastValRetention?.let {
                            DurationParser.parse(it)
                        }
                        val archiveRetentionMs = archiveRetention?.let {
                            DurationParser.parse(it)
                        }
                        val purgeIntervalMs = purgeInterval?.let {
                            DurationParser.parse(it)
                        }

                        // Create the ArchiveGroup
                        val archiveGroup = ArchiveGroup(
                            name = name,
                            topicFilter = topicFilter,
                            retainedOnly = retainedOnly,
                            lastValType = lastValType,
                            archiveType = archiveType,
                            lastValRetentionMs = lastValRetentionMs,
                            archiveRetentionMs = archiveRetentionMs,
                            purgeIntervalMs = purgeIntervalMs,
                            lastValRetentionStr = lastValRetention,
                            archiveRetentionStr = archiveRetention,
                            purgeIntervalStr = purgeInterval,
                            databaseConfig = JsonObject() // Will be populated from config
                        )

                        // Save to database (disabled by default)
                        val success = configStore.saveArchiveGroup(archiveGroup, enabled = false)

                        if (success) {
                            mapOf(
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
                                    "lastValRetention" to lastValRetention,
                                    "archiveRetention" to archiveRetention,
                                    "purgeInterval" to purgeInterval
                                )
                            )
                        } else {
                            mapOf(
                                "success" to false,
                                "message" to "Failed to save archive group to database"
                            )
                        }
                    } catch (e: IllegalArgumentException) {
                        mapOf(
                            "success" to false,
                            "message" to "Invalid input: ${e.message}"
                        )
                    } catch (e: Exception) {
                        mapOf(
                            "success" to false,
                            "message" to "Error creating archive group: ${e.message}"
                        )
                    }
                }
            }.onComplete { result ->
                if (result.succeeded()) {
                    future.complete(result.result())
                } else {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "Error: ${result.cause()?.message}"
                    ))
                }
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


            vertx.executeBlocking<Map<String, Any?>> {
                runBlocking {
                    try {
                        val configStore = archiveHandler.getConfigStore()
                        if (configStore == null) {
                            return@runBlocking mapOf(
                                "success" to false,
                                "message" to "No ConfigStore available - cannot update archive group"
                            )
                        }

                        // Extract name from input
                        val name = input["name"] as? String ?: throw IllegalArgumentException("Name is required")

                        // First, check if the archive group exists
                        val existingGroup = configStore.getArchiveGroup(name)
                        if (existingGroup == null) {
                            return@runBlocking mapOf(
                                "success" to false,
                                "message" to "Archive group '$name' not found"
                            )
                        }

                        // Check if the archive group is disabled (must be disabled to update)
                        if (existingGroup.enabled) {
                            return@runBlocking mapOf(
                                "success" to false,
                                "message" to "Archive group '$name' must be disabled before it can be updated. Use disableArchiveGroup mutation first."
                            )
                        }

                        // Also check if it's currently deployed/running (extra safety check)
                        val runtimeStatus = archiveHandler.getArchiveGroupStatus(name)
                        if (runtimeStatus.getBoolean("deployed", false)) {
                            return@runBlocking mapOf(
                                "success" to false,
                                "message" to "Archive group '$name' is currently running. Please disable it first before updating."
                            )
                        }

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
                                throw IllegalArgumentException("Invalid lastValType: $lastValTypeStr")
                            }
                        } else {
                            existingArchiveGroup.getLastValType()
                        }

                        val archiveTypeStr = input["archiveType"] as? String
                        val archiveType = if (archiveTypeStr != null) {
                            try {
                                MessageArchiveType.valueOf(archiveTypeStr)
                            } catch (e: Exception) {
                                throw IllegalArgumentException("Invalid archiveType: $archiveTypeStr")
                            }
                        } else {
                            existingArchiveGroup.getArchiveType()
                        }

                        // Parse optional durations if provided, otherwise keep existing
                        val lastValRetention = input["lastValRetention"] as? String
                        val archiveRetention = input["archiveRetention"] as? String
                        val purgeInterval = input["purgeInterval"] as? String

                        val lastValRetentionMs = if (lastValRetention != null) {
                            DurationParser.parse(lastValRetention)
                        } else {
                            existingArchiveGroup.getLastValRetentionMs()
                        }
                        val archiveRetentionMs = if (archiveRetention != null) {
                            DurationParser.parse(archiveRetention)
                        } else {
                            existingArchiveGroup.getArchiveRetentionMs()
                        }
                        val purgeIntervalMs = if (purgeInterval != null) {
                            DurationParser.parse(purgeInterval)
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
                            lastValRetentionMs = lastValRetentionMs,
                            archiveRetentionMs = archiveRetentionMs,
                            purgeIntervalMs = purgeIntervalMs,
                            lastValRetentionStr = lastValRetention ?: existingArchiveGroup.getLastValRetention(),
                            archiveRetentionStr = archiveRetention ?: existingArchiveGroup.getArchiveRetention(),
                            purgeIntervalStr = purgeInterval ?: existingArchiveGroup.getPurgeInterval(),
                            databaseConfig = JsonObject() // Will be populated from config
                        )

                        // Update in database (keep the same enabled state - false)
                        val success = configStore.updateArchiveGroup(updatedArchiveGroup, enabled = false)

                        if (success) {
                            mapOf(
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
                            )
                        } else {
                            mapOf(
                                "success" to false,
                                "message" to "Failed to update archive group in database"
                            )
                        }
                    } catch (e: IllegalArgumentException) {
                        mapOf(
                            "success" to false,
                            "message" to "Invalid input: ${e.message}"
                        )
                    } catch (e: Exception) {
                        mapOf(
                            "success" to false,
                            "message" to "Error updating archive group: ${e.message}"
                        )
                    }
                }
            }.onComplete { result ->
                if (result.succeeded()) {
                    future.complete(result.result())
                } else {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "Error: ${result.cause()?.message}"
                    ))
                }
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

                // First, check if the archive group exists
                val existingGroup = configStore.getArchiveGroup(name)
                if (existingGroup == null) {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "Archive group '$name' not found"
                    ))
                    return@DataFetcher future
                }

                // Check if the archive group is disabled (must be disabled to delete)
                if (existingGroup.enabled) {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "Archive group '$name' must be disabled before it can be deleted. Use disableArchiveGroup mutation first."
                    ))
                    return@DataFetcher future
                }

                // Also check if it's currently deployed/running (extra safety check)
                val runtimeStatus = archiveHandler.getArchiveGroupStatus(name)
                if (runtimeStatus.getBoolean("deployed", false)) {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "Archive group '$name' is currently running. Please disable it first before deletion."
                    ))
                    return@DataFetcher future
                }

                // Delete from database first
                val deleteSuccess = configStore.deleteArchiveGroup(name)
                if (!deleteSuccess) {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "Failed to delete archive group '$name' from database"
                    ))
                    return@DataFetcher future
                }

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

                // First, check if the archive group exists
                val existingGroup = configStore.getArchiveGroup(name)
                if (existingGroup == null) {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "Archive group '$name' not found"
                    ))
                    return@DataFetcher future
                }

                // Update the enabled flag in database
                val updateSuccess = configStore.updateArchiveGroup(existingGroup.archiveGroup, enabled = true)
                if (!updateSuccess) {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "Failed to update archive group in database"
                    ))
                    return@DataFetcher future
                }

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
                        // If start failed, revert the database change
                        configStore.updateArchiveGroup(existingGroup.archiveGroup, enabled = false)
                        future.complete(mapOf(
                            "success" to false,
                            "message" to "Failed to start archive group '$name': ${startResult.cause()?.message ?: "Unknown error"}"
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

                // First, check if the archive group exists
                val existingGroup = configStore.getArchiveGroup(name)
                if (existingGroup == null) {
                    future.complete(mapOf(
                        "success" to false,
                        "message" to "Archive group '$name' not found"
                    ))
                    return@DataFetcher future
                }

                // First stop the archive group asynchronously
                archiveHandler.stopArchiveGroup(name).onComplete { stopResult ->
                    // Update the enabled flag in database regardless of stop result
                    // (we want to disable it in DB even if it wasn't running)
                    val updateSuccess = configStore.updateArchiveGroup(existingGroup.archiveGroup, enabled = false)

                    if (updateSuccess) {
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
                MessageStoreSQLite(
                    storeName,
                    sqlite.getString("Path", "monstermq.db")
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
                MessageArchiveKafka(archiveName, bootstrapServers)
            }
            MessageArchiveType.SQLITE -> {
                val sqlite = databaseConfig.getJsonObject("SQLite")
                MessageArchiveSQLite(
                    archiveName,
                    sqlite.getString("Path", "monstermq.db")
                )
            }
        }
    }

    private fun dropTable(storageType: String, tableName: String, databaseConfig: JsonObject): Boolean {
        val lowercaseTableName = tableName.lowercase()
        return when (storageType) {
            "NONE", "MEMORY", "HAZELCAST", "KAFKA" -> true
            "POSTGRES" -> {
                val postgres = databaseConfig.getJsonObject("Postgres")
                dropPostgresTable(lowercaseTableName, postgres)
            }
            "CRATEDB" -> {
                val cratedb = databaseConfig.getJsonObject("CrateDB")
                dropCrateTable(lowercaseTableName, cratedb)
            }
            "MONGODB" -> {
                val mongodb = databaseConfig.getJsonObject("MongoDB")
                dropMongoCollection(lowercaseTableName, mongodb)
            }
            "SQLITE" -> {
                val sqlite = databaseConfig.getJsonObject("SQLite")
                dropSQLiteTable(lowercaseTableName, sqlite)
            }
            else -> {
                logger.warning("Unknown storage type: $storageType")
                false
            }
        }
    }

    private fun dropPostgresTable(tableName: String, config: JsonObject): Boolean {
        return try {
            val url = config.getString("Url")
            val user = config.getString("User")
            val pass = config.getString("Pass")

            DriverManager.getConnection(url, user, pass).use { connection ->
                connection.autoCommit = false
                val sql = "DROP TABLE IF EXISTS $tableName CASCADE"
                connection.prepareStatement(sql).use { statement ->
                    statement.executeUpdate()
                }
                connection.commit()
                logger.info("Dropped PostgreSQL table [$tableName]")
                true
            }
        } catch (e: Exception) {
            logger.severe("Error dropping PostgreSQL table [$tableName]: ${e.message}")
            false
        }
    }

    private fun dropCrateTable(tableName: String, config: JsonObject): Boolean {
        return try {
            val url = config.getString("Url")
            val user = config.getString("User")
            val pass = config.getString("Pass")

            DriverManager.getConnection(url, user, pass).use { connection ->
                connection.autoCommit = false
                val sql = "DROP TABLE IF EXISTS $tableName"
                connection.prepareStatement(sql).use { statement ->
                    statement.executeUpdate()
                }
                connection.commit()
                logger.info("Dropped CrateDB table [$tableName]")
                true
            }
        } catch (e: Exception) {
            logger.severe("Error dropping CrateDB table [$tableName]: ${e.message}")
            false
        }
    }

    private fun dropMongoCollection(collectionName: String, config: JsonObject): Boolean {
        return try {
            val url = config.getString("Url")
            val database = config.getString("Database", "monstermq")

            val client = MongoClients.create(ConnectionString(url))
            try {
                val db = client.getDatabase(database)
                val collection = db.getCollection(collectionName)
                collection.drop()
                logger.info("Dropped MongoDB collection [$collectionName]")
                true
            } finally {
                client.close()
            }
        } catch (e: Exception) {
            logger.severe("Error dropping MongoDB collection [$collectionName]: ${e.message}")
            false
        }
    }

    private fun dropSQLiteTable(tableName: String, config: JsonObject): Boolean {
        return try {
            val path = config.getString("Path", "monstermq.db")
            val url = "jdbc:sqlite:$path"

            DriverManager.getConnection(url).use { connection ->
                val sql = "DROP TABLE IF EXISTS $tableName"
                connection.prepareStatement(sql).use { statement ->
                    statement.executeUpdate()
                }
                logger.info("Dropped SQLite table [$tableName]")
                true
            }
        } catch (e: Exception) {
            logger.severe("Error dropping SQLite table [$tableName]: ${e.message}")
            false
        }
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