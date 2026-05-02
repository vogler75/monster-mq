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
    private val archiveGroupNameRegex = Regex("[A-Za-z][A-Za-z0-9_]{0,62}")

    private fun sanitizeUrl(url: String): String {
        return url.replace(Regex("(://[^:@/]+):([^@/]+)@"), "$1@")
    }

    private fun builtInConnections(): List<DatabaseConnectionConfig> {
        val databaseConfig = archiveHandler.createDatabaseConfig()
        val connections = mutableListOf<DatabaseConnectionConfig>()
        databaseConfig.getJsonObject("Postgres")?.let {
            val url = it.getString("Url")
            if (!url.isNullOrBlank()) {
                connections.add(DatabaseConnectionConfig(
                    name = "Default",
                    type = DatabaseConnectionType.POSTGRES,
                    url = url,
                    username = it.getString("User"),
                    schema = it.getString("Schema"),
                    readOnly = true
                ))
            }
        }
        databaseConfig.getJsonObject("MongoDB")?.let {
            val url = it.getString("Url")
            if (!url.isNullOrBlank()) {
                connections.add(DatabaseConnectionConfig(
                    name = "Default",
                    type = DatabaseConnectionType.MONGODB,
                    url = url,
                    database = it.getString("Database", "monstermq"),
                    readOnly = true
                ))
            }
        }
        return connections
    }

    private fun connectionToMap(connection: DatabaseConnectionConfig): Map<String, Any?> {
        return mapOf(
            "name" to connection.name,
            "type" to connection.type.name,
            "url" to sanitizeUrl(connection.url),
            "username" to connection.username,
            "database" to connection.database,
            "schema" to connection.schema,
            "readOnly" to connection.readOnly,
            "createdAt" to connection.createdAt,
            "updatedAt" to connection.updatedAt
        )
    }

    private fun validateConnectionSelection(
        configStore: IArchiveConfigStore,
        selectedName: String?,
        lastValType: MessageStoreType,
        archiveType: MessageArchiveType
    ): io.vertx.core.Future<String?> {
        if (selectedName.isNullOrBlank()) return io.vertx.core.Future.succeededFuture(null)

        val requiredTypes = setOfNotNull(
            when (lastValType) {
                MessageStoreType.POSTGRES -> DatabaseConnectionType.POSTGRES
                MessageStoreType.MONGODB -> DatabaseConnectionType.MONGODB
                else -> null
            },
            when (archiveType) {
                MessageArchiveType.POSTGRES -> DatabaseConnectionType.POSTGRES
                MessageArchiveType.MONGODB -> DatabaseConnectionType.MONGODB
                else -> null
            }
        )

        if (requiredTypes.isEmpty()) {
            return io.vertx.core.Future.succeededFuture("A database connection can only be selected for PostgreSQL or MongoDB storage")
        }
        if (requiredTypes.size > 1) {
            return io.vertx.core.Future.succeededFuture(
                if (selectedName == "Default") null
                else "Mixed PostgreSQL and MongoDB storage cannot use a named database connection; leave the selection empty to use config-file defaults"
            )
        }

        builtInConnections().firstOrNull { it.name == selectedName && it.type == requiredTypes.first() }?.let {
            return io.vertx.core.Future.succeededFuture(if (it.type == requiredTypes.first()) null else "Selected database connection '$selectedName' is ${it.type}, but ${requiredTypes.first()} is required")
        }

        return configStore.getDatabaseConnection(selectedName).map { connection ->
            when {
                connection == null -> "Database connection '$selectedName' not found"
                connection.type != requiredTypes.first() -> "Selected database connection '$selectedName' is ${connection.type}, but ${requiredTypes.first()} is required"
                else -> null
            }
        }
    }

    private fun validateArchiveGroupName(name: String): String? {
        return if (archiveGroupNameRegex.matches(name)) {
            null
        } else {
            "Invalid input: invalid group name \"$name\": must match [A-Za-z][A-Za-z0-9_]{0,62}"
        }
    }

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
                            "databaseConnectionName" to jsonObj.getString("databaseConnectionName"),
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
                                        "databaseConnectionName" to config.archiveGroup.getDatabaseConnectionName(),
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
                            "databaseConnectionName" to runtimeStatus.getString("databaseConnectionName"),
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
                                        "databaseConnectionName" to archiveGroup.archiveGroup.getDatabaseConnectionName(),
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

    fun databaseConnections(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any?>>>()
            if (!checkAuthorization(env, future)) return@DataFetcher future

            val typeFilter = env.getArgument<String?>("type")
            val builtIns = builtInConnections()
            val configStore = archiveHandler.getConfigStore()
            if (configStore == null) {
                future.complete(builtIns
                    .filter { typeFilter == null || it.type.name == typeFilter }
                    .map { connectionToMap(it) })
                return@DataFetcher future
            }

            configStore.getAllDatabaseConnections().onComplete { result ->
                if (result.succeeded()) {
                    val all = (builtIns + result.result())
                        .filter { typeFilter == null || it.type.name == typeFilter }
                        .sortedWith(compareBy<DatabaseConnectionConfig> { !it.readOnly }.thenBy { it.name })
                    future.complete(all.map { connectionToMap(it) })
                } else {
                    future.completeExceptionally(result.cause())
                }
            }
            future
        }
    }

    fun databaseConnectionNames(): DataFetcher<CompletableFuture<List<String>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<String>>()
            if (!checkAuthorization(env, future)) return@DataFetcher future

            val typeFilter = env.getArgument<String?>("type")
            val builtIns = builtInConnections()
                .filter { typeFilter == null || it.type.name == typeFilter }
            val configStore = archiveHandler.getConfigStore()
            if (configStore == null) {
                future.complete(builtIns.map { it.name }.distinct())
                return@DataFetcher future
            }

            configStore.getAllDatabaseConnections().onComplete { result ->
                if (result.succeeded()) {
                    val names = (builtIns + result.result())
                        .filter { typeFilter == null || it.type.name == typeFilter }
                        .map { it.name }
                        .distinct()
                    future.complete(names)
                } else {
                    future.completeExceptionally(result.cause())
                }
            }
            future
        }
    }

    fun databaseConnection(): DataFetcher<CompletableFuture<Map<String, Any?>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>?>()
            if (!checkAuthorization(env, future)) return@DataFetcher future

            val name = env.getArgument<String>("name") ?: ""
            builtInConnections().firstOrNull { it.name == name }?.let {
                future.complete(connectionToMap(it))
                return@DataFetcher future
            }

            val configStore = archiveHandler.getConfigStore()
            if (configStore == null) {
                future.complete(null)
                return@DataFetcher future
            }

            configStore.getDatabaseConnection(name).onComplete { result ->
                if (result.succeeded()) {
                    future.complete(result.result()?.let { connectionToMap(it) })
                } else {
                    future.completeExceptionally(result.cause())
                }
            }
            future
        }
    }

    // Mutation Resolvers

    fun createDatabaseConnection(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            if (!checkAuthorization(env, future)) return@DataFetcher future

            val input = env.getArgument<Map<String, Any?>>("input") ?: emptyMap()
            val configStore = archiveHandler.getConfigStore()
            if (configStore == null) {
                future.complete(mapOf("success" to false, "message" to "No ConfigStore available - database connections require a ConfigStore"))
                return@DataFetcher future
            }

            try {
                val name = (input["name"] as? String)?.trim().orEmpty()
                val type = DatabaseConnectionType.valueOf(input["type"] as? String ?: "")
                val url = (input["url"] as? String)?.trim().orEmpty()
                if (name.isBlank() || url.isBlank()) {
                    future.complete(mapOf("success" to false, "message" to "Name and URL are required"))
                    return@DataFetcher future
                }
                if (name == "Default") {
                    future.complete(mapOf("success" to false, "message" to "Built-in default connections are read-only"))
                    return@DataFetcher future
                }

                val connection = DatabaseConnectionConfig(
                    name = name,
                    type = type,
                    url = url,
                    username = input["username"] as? String,
                    password = input["password"] as? String,
                    database = input["database"] as? String,
                    schema = input["schema"] as? String
                )
                configStore.getDatabaseConnection(name).onComplete { existingResult ->
                    if (existingResult.succeeded() && existingResult.result() != null) {
                        future.complete(mapOf("success" to false, "message" to "Database connection '$name' already exists"))
                        return@onComplete
                    }
                    configStore.saveDatabaseConnection(connection).onComplete { result ->
                        if (result.succeeded() && result.result()) {
                            future.complete(mapOf("success" to true, "message" to "Database connection '$name' created", "connection" to connectionToMap(connection)))
                        } else {
                            future.complete(mapOf("success" to false, "message" to "Failed to save database connection '$name'"))
                        }
                    }
                }
            } catch (e: Exception) {
                future.complete(mapOf("success" to false, "message" to "Error creating database connection: ${e.message}"))
            }

            future
        }
    }

    fun updateDatabaseConnection(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            if (!checkAuthorization(env, future)) return@DataFetcher future

            val input = env.getArgument<Map<String, Any?>>("input") ?: emptyMap()
            val name = (input["name"] as? String)?.trim().orEmpty()
            val configStore = archiveHandler.getConfigStore()
            if (configStore == null) {
                future.complete(mapOf("success" to false, "message" to "No ConfigStore available - database connections require a ConfigStore"))
                return@DataFetcher future
            }
            if (name == "Default") {
                future.complete(mapOf("success" to false, "message" to "Built-in default connections are read-only"))
                return@DataFetcher future
            }

            configStore.getDatabaseConnection(name).onComplete { getResult ->
                if (getResult.failed() || getResult.result() == null) {
                    future.complete(mapOf("success" to false, "message" to "Database connection '$name' not found"))
                    return@onComplete
                }

                try {
                    val existing = getResult.result()!!
                    val type = (input["type"] as? String)?.let { DatabaseConnectionType.valueOf(it) } ?: existing.type
                    val password = (input["password"] as? String)?.takeIf { it.isNotEmpty() } ?: existing.password
                    val updated = DatabaseConnectionConfig(
                        name = name,
                        type = type,
                        url = (input["url"] as? String)?.takeIf { it.isNotBlank() } ?: existing.url,
                        username = input["username"] as? String ?: existing.username,
                        password = password,
                        database = input["database"] as? String ?: existing.database,
                        schema = input["schema"] as? String ?: existing.schema,
                        createdAt = existing.createdAt
                    )
                    configStore.saveDatabaseConnection(updated).onComplete { saveResult ->
                        if (saveResult.succeeded() && saveResult.result()) {
                            future.complete(mapOf("success" to true, "message" to "Database connection '$name' updated", "connection" to connectionToMap(updated)))
                        } else {
                            future.complete(mapOf("success" to false, "message" to "Failed to update database connection '$name'"))
                        }
                    }
                } catch (e: Exception) {
                    future.complete(mapOf("success" to false, "message" to "Error updating database connection: ${e.message}"))
                }
            }
            future
        }
    }

    fun deleteDatabaseConnection(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            if (!checkAuthorization(env, future)) return@DataFetcher future

            val name = env.getArgument<String>("name") ?: ""
            val configStore = archiveHandler.getConfigStore()
            if (configStore == null) {
                future.complete(mapOf("success" to false, "message" to "No ConfigStore available - database connections require a ConfigStore"))
                return@DataFetcher future
            }
            if (name == "Default") {
                future.complete(mapOf("success" to false, "message" to "Built-in default connections are read-only"))
                return@DataFetcher future
            }

            configStore.getAllArchiveGroups().onComplete { groupsResult ->
                if (groupsResult.succeeded() && groupsResult.result().any { it.archiveGroup.getDatabaseConnectionName() == name }) {
                    future.complete(mapOf("success" to false, "message" to "Database connection '$name' is used by one or more archive groups"))
                    return@onComplete
                }
                configStore.deleteDatabaseConnection(name).onComplete { deleteResult ->
                    if (deleteResult.succeeded() && deleteResult.result()) {
                        future.complete(mapOf("success" to true, "message" to "Database connection '$name' deleted"))
                    } else {
                        future.complete(mapOf("success" to false, "message" to "Failed to delete database connection '$name'"))
                    }
                }
            }
            future
        }
    }

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
                validateArchiveGroupName(name)?.let { message ->
                    future.complete(mapOf(
                        "success" to false,
                        "message" to message
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
                val databaseConnectionName = (input["databaseConnectionName"] as? String)?.takeIf { it.isNotBlank() }

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
                    databaseConnectionName = databaseConnectionName,
                    databaseConfig = JsonObject() // Will be populated from config
                )

                validateConnectionSelection(configStore, databaseConnectionName, lastValType, archiveType).onComplete { validation ->
                    val validationMessage = validation.result()
                    if (validation.failed() || validationMessage != null) {
                        future.complete(mapOf(
                            "success" to false,
                            "message" to (validationMessage ?: validation.cause()?.message ?: "Invalid database connection selection")
                        ))
                        return@onComplete
                    }

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
                                    "databaseConnectionName" to databaseConnectionName,
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
                validateArchiveGroupName(name)?.let { message ->
                    future.complete(mapOf(
                        "success" to false,
                        "message" to message
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
                             val databaseConnectionName = if (input.containsKey("databaseConnectionName")) {
                                 (input["databaseConnectionName"] as? String)?.takeIf { it.isNotBlank() }
                             } else {
                                 existingArchiveGroup.getDatabaseConnectionName()
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
                                 databaseConnectionName = databaseConnectionName,
                                 databaseConfig = JsonObject() // Will be populated from config
                             )

                            validateConnectionSelection(configStore, databaseConnectionName, lastValType, archiveType).onComplete { validation ->
                                val validationMessage = validation.result()
                                if (validation.failed() || validationMessage != null) {
                                    future.complete(mapOf(
                                        "success" to false,
                                        "message" to (validationMessage ?: validation.cause()?.message ?: "Invalid database connection selection")
                                    ))
                                    return@onComplete
                                }

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
                                                "databaseConnectionName" to databaseConnectionName,
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
