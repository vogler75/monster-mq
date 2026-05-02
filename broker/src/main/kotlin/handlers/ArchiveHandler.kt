package at.rocworks.handlers

import at.rocworks.Utils
import at.rocworks.stores.*
import at.rocworks.Monster
import at.rocworks.stores.ArchiveConfigStoreFactory
import at.rocworks.stores.MessageArchiveType
import at.rocworks.stores.cratedb.MessageArchiveCrateDB
import at.rocworks.stores.mongodb.MessageArchiveMongoDB
import at.rocworks.stores.postgres.MessageArchivePostgres
import at.rocworks.stores.sqlite.MessageArchiveSQLite
import at.rocworks.stores.MessageArchiveKafka
import at.rocworks.stores.PayloadFormat
import io.vertx.core.*
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Callable
import java.util.logging.Logger


class ArchiveHandler(
    private val vertx: Vertx,
    private val configJson: JsonObject,
    private val archiveConfigsFile: String?,
    private val archiveConfigsMergeFile: String?,
    private val isClustered: Boolean = false
) {
    private val logger: Logger = Utils.getLogger(this::class.java)

    // Track all deployed ArchiveGroups with their deployment IDs
    private val deployedArchiveGroups = ConcurrentHashMap<String, ArchiveGroupInfo>()

    // Keep reference to deployed ConfigStore if using database
    private var deployedConfigStore: IArchiveConfigStore? = null

    // Reference to MessageHandler for dynamic registration
    private var messageHandler: MessageHandler? = null

    companion object {
        // Event bus addresses for ArchiveGroup management
        const val ARCHIVE_START = "mq.cluster.archive.start"
        const val ARCHIVE_STOP = "mq.cluster.archive.stop"
        const val ARCHIVE_STATUS = "mq.cluster.archive.status"
        const val ARCHIVE_LIST = "mq.cluster.archive.list"
        const val ARCHIVE_CONNECTION_STATUS = "mq.cluster.archive.connection.status"
        const val ARCHIVE_EVENTS = "mq.cluster.archive.events"
    }

    fun initialize(): Future<List<ArchiveGroup>> {
        val promise = Promise.promise<List<ArchiveGroup>>()

        // Setup message bus event handlers
        setupEventHandlers()

        // Load archive configs if specified
        val configFile = archiveConfigsFile ?: archiveConfigsMergeFile
        if (configFile != null) {
            val fullSync = archiveConfigsFile != null
            loadArchiveConfigs(configFile, fullSync).onComplete { loadResult ->
                if (loadResult.succeeded()) {
                    deployArchiveGroups().onComplete(promise)
                } else {
                    promise.fail(loadResult.cause())
                }
            }
        } else {
            deployArchiveGroups().onComplete(promise)
        }

        return promise.future()
    }

    private fun setupEventHandlers() {
        val eventBus = vertx.eventBus()

        // Handle archive start commands
        eventBus.consumer<JsonObject>(ARCHIVE_START) { message ->
            handleArchiveStart(message)
        }

        // Handle archive stop commands
        eventBus.consumer<JsonObject>(ARCHIVE_STOP) { message ->
            handleArchiveStop(message)
        }

        // Handle archive status requests
        eventBus.consumer<JsonObject>(ARCHIVE_STATUS) { message ->
            handleArchiveStatus(message)
        }

        // Handle archive list requests
        eventBus.consumer<JsonObject>(ARCHIVE_LIST) { message ->
            handleArchiveList(message)
        }

        // Handle archive connection status requests
        eventBus.consumer<JsonObject>(ARCHIVE_CONNECTION_STATUS) { message ->
            handleArchiveConnectionStatus(message)
        }

        // Handle archive events from cluster nodes (start/stop broadcasts)
        eventBus.consumer<JsonObject>(ARCHIVE_EVENTS) { message ->
            handleArchiveEvent(message)
        }

        logger.fine("ArchiveHandler event handlers registered")
    }

    private fun loadArchiveConfigs(file: String, fullSync: Boolean): Future<Void> {
        val promise = Promise.promise<Void>()

        try {
            val mode = if (fullSync) "full sync" else "merge"
            logger.fine("Loading archive configuration from: $file (mode: $mode)")

            vertx.fileSystem().readFile(file).onComplete { result ->
                if (result.succeeded()) {
                    try {
                        val archiveGroups = JsonArray(result.result().toString())

                        if (!archiveGroups.isEmpty) {
                            val configStoreType = Monster.getConfigStoreType(configJson)

                            if (configStoreType != "NONE") {
                                // Import into database
                                importArchiveConfigToDatabase(archiveGroups, configStoreType, fullSync).onComplete { importResult ->
                                    if (importResult.succeeded()) {
                                        promise.complete()
                                    } else {
                                        promise.fail(importResult.cause())
                                    }
                                }
                            } else {
                                // Load into memory (ArchiveGroups key for in-memory mode)
                                configJson.put("ArchiveGroups", archiveGroups)
                                logger.fine("Loaded ${archiveGroups.size()} archive groups from $file into memory")
                                promise.complete()
                            }
                        } else {
                            logger.warning("No archive groups found in $file")
                            promise.complete()
                        }
                    } catch (e: Exception) {
                        logger.severe("Failed to parse JSON from $file: ${e.message}")
                        promise.fail(e)
                    }
                } else {
                    logger.severe("Failed to read archive config file $file: ${result.cause()?.message}")
                    promise.fail(result.cause())
                }
            }
        } catch (e: Exception) {
            logger.severe("Error loading archive config from $file: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }


    private fun importArchiveConfigToDatabase(archiveGroups: JsonArray, configStoreType: String, fullSync: Boolean = false): Future<Void> {
        val promise = Promise.promise<Void>()
        val configStore = ArchiveConfigStoreFactory.createConfigStore(configJson, configStoreType)

        if (configStore == null) {
            logger.severe("Failed to create ConfigStore of type $configStoreType for import")
            promise.fail("Failed to create ConfigStore of type $configStoreType for import")
            return promise.future()
        }

        // Deploy ConfigStore temporarily for import
        val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
        vertx.deployVerticle(configStore as AbstractVerticle, options).onComplete { deployResult ->
            if (deployResult.succeeded()) {
                logger.fine("ConfigStore deployed for import")
                // Store reference to deployed ConfigStore
                deployedConfigStore = configStore

                // Create database configuration object
                val databaseConfig = createDatabaseConfig()

                // Import each archive group
                val configsList = archiveGroups.filterIsInstance<JsonObject>()
                val importedNames = mutableSetOf<String>()
                val importFutures = configsList.map { config ->
                    try {
                        val archiveGroup = ArchiveGroup.fromConfig(config, databaseConfig, isClustered)
                        val enabled = config.getBoolean("Enabled", true)
                        importedNames.add(archiveGroup.name)

                        configStore.saveArchiveGroup(archiveGroup, enabled).map { success ->
                            if (success) {
                                logger.fine("Imported ArchiveGroup [${archiveGroup.name}] to database")
                                1
                            } else {
                                logger.warning("Failed to import ArchiveGroup [${archiveGroup.name}] to database")
                                0
                            }
                        }
                    } catch (e: Exception) {
                        logger.severe("Error importing archive group: ${e.message}")
                        Future.succeededFuture(0)
                    }
                }

                Future.all<Any>(importFutures).onComplete { importResult ->
                    val importedCount = importFutures.mapNotNull {
                        if (it.succeeded()) it.result() as Int else null
                    }.sum()

                    val file = archiveConfigsFile ?: archiveConfigsMergeFile ?: "unknown"
                    logger.fine("Successfully imported $importedCount archive groups from $file to database")

                    if (fullSync) {
                        // Delete orphan groups not present in the file
                        deleteOrphanArchiveGroups(configStore, importedNames).onComplete {
                            // Undeploy the temporary ConfigStore
                            vertx.undeploy(deployResult.result()).onComplete { undeployResult ->
                                if (!undeployResult.succeeded()) {
                                    logger.warning("Failed to undeploy temporary ConfigStore: ${undeployResult.cause()?.message}")
                                }
                                promise.complete()
                            }
                        }
                    } else {
                        // Undeploy the temporary ConfigStore
                        vertx.undeploy(deployResult.result()).onComplete { undeployResult ->
                            if (!undeployResult.succeeded()) {
                                logger.warning("Failed to undeploy temporary ConfigStore: ${undeployResult.cause()?.message}")
                            }
                            promise.complete()
                        }
                    }
                }
            } else {
                logger.severe("Failed to deploy ConfigStore for import: ${deployResult.cause()?.message}")
                promise.fail(deployResult.cause())
            }
        }

        return promise.future()
    }

    private fun deleteOrphanArchiveGroups(configStore: IArchiveConfigStore, importedNames: Set<String>): Future<Void> {
        val promise = Promise.promise<Void>()

        configStore.getAllArchiveGroups().onComplete { getAllResult ->
            if (getAllResult.succeeded()) {
                val existingGroups = getAllResult.result()
                val orphanNames = existingGroups
                    .map { it.archiveGroup.name }
                    .filter { it !in importedNames }

                if (orphanNames.isEmpty()) {
                    promise.complete()
                    return@onComplete
                }

                val deleteFutures = orphanNames.map { name ->
                    configStore.deleteArchiveGroup(name).map { success ->
                        if (success) {
                            logger.fine("Deleted orphan ArchiveGroup [$name] (full sync)")
                        } else {
                            logger.warning("Failed to delete orphan ArchiveGroup [$name]")
                        }
                    }
                }

                Future.all<Any>(deleteFutures).onComplete {
                    logger.fine("Full sync cleanup: deleted ${orphanNames.size} orphan archive groups: $orphanNames")
                    promise.complete()
                }
            } else {
                logger.warning("Failed to get all archive groups for orphan cleanup: ${getAllResult.cause()?.message}")
                promise.complete() // Don't fail the import because of cleanup failure
            }
        }

        return promise.future()
    }

    private fun deployArchiveGroups(): Future<List<ArchiveGroup>> {
        val promise = Promise.promise<List<ArchiveGroup>>()

        // Load from database
        val configStoreType = Monster.getConfigStoreType(configJson)
        loadArchiveGroupsFromDatabase(configStoreType, promise)

        return promise.future()
    }

    private fun loadArchiveGroupsFromDatabase(configStoreType: String, promise: Promise<List<ArchiveGroup>>) {
        // If ConfigStoreType is NONE, skip archive group initialization
        if (configStoreType.uppercase() == "NONE") {
            logger.info("ConfigStoreType is NONE, skipping archive group initialization")
            promise.complete(emptyList())
            return
        }

        val configStore = ArchiveConfigStoreFactory.createConfigStore(configJson, configStoreType)

        if (configStore == null) {
            logger.severe("Failed to create ConfigStore of type $configStoreType")
            promise.fail("Failed to create ConfigStore of type $configStoreType")
            return
        }

        // Deploy ConfigStore
        val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
        vertx.deployVerticle(configStore as AbstractVerticle, options).onComplete { deployResult ->
            if (deployResult.succeeded()) {
                logger.fine("ConfigStore deployed successfully")
                // Store reference to deployed ConfigStore
                deployedConfigStore = configStore

                // Create database configuration object first
                val databaseConfig = createDatabaseConfig()

                // Check if "Default" archive group exists, create if not
                configStore.getArchiveGroup("Default").onComplete { getDefaultResult ->
                    if (getDefaultResult.succeeded() && getDefaultResult.result() == null) {
                        logger.fine("Default archive group not found, creating it")
                        val defaultArchiveGroup = ArchiveGroup(
                            name = "Default",
                            topicFilter = listOf("#"),
                            retainedOnly = false,
                            lastValType = MessageStoreType.MEMORY,
                            archiveType = MessageArchiveType.NONE,
                            payloadFormat = PayloadFormat.DEFAULT,
                            lastValRetentionMs = 3600000L,
                            archiveRetentionMs = 3600000L,
                            purgeIntervalMs = 3600000L,
                            lastValRetentionStr = "1h",
                            archiveRetentionStr = "1h",
                            purgeIntervalStr = "1h",
                            databaseConfig = databaseConfig
                        )
                        configStore.saveArchiveGroup(defaultArchiveGroup, enabled = false).onComplete { saveResult ->
                            if (saveResult.succeeded() && saveResult.result()) {
                                logger.fine("Default archive group created successfully")
                            } else {
                                logger.warning("Failed to create default archive group")
                            }
                        }
                    }
                }

                // Load archive groups from database
                configStore.getAllArchiveGroups().onComplete { getAllResult ->
                    if (getAllResult.succeeded()) {
                        val archiveGroupConfigs = getAllResult.result().filter { it.enabled }
                        val resolveFutures = archiveGroupConfigs.map { config ->
                            val ag = config.archiveGroup
                            resolveDatabaseConfig(ag, configStore).map { resolvedConfig ->
                                recreateArchiveGroup(ag, resolvedConfig)
                            }
                        }

                        if (resolveFutures.isEmpty()) {
                            logger.fine("No enabled archive groups found in database")
                            promise.complete(emptyList())
                            return@onComplete
                        }

                        Future.all<Any>(resolveFutures.map { it.recover { Future.succeededFuture(null) } }).onComplete {
                            val archiveGroups = resolveFutures.mapNotNull { if (it.succeeded()) it.result() else null }
                            resolveFutures.forEachIndexed { index, future ->
                                if (future.failed()) {
                                    logger.severe("Skipping ArchiveGroup [${archiveGroupConfigs[index].archiveGroup.name}] during startup: ${future.cause()?.message}")
                                }
                            }

                            if (archiveGroups.isEmpty()) {
                                logger.warning("No enabled archive groups could be started")
                                promise.complete(emptyList())
                                return@onComplete
                            }

                            // Deploy each archive group
                            val deploymentFutures: List<Future<ArchiveGroup?>> = archiveGroups.map { archiveGroup ->
                                val archiveGroupOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)

                                vertx.deployVerticle(archiveGroup, archiveGroupOptions)
                                    .map { deploymentId ->
                                        // Track the deployment
                                        val archiveInfo = ArchiveGroupInfo(archiveGroup, deploymentId, true)
                                        deployedArchiveGroups[archiveGroup.name] = archiveInfo

                                        logger.fine("ArchiveGroup [${archiveGroup.name}] deployed successfully from database with ID: $deploymentId")
                                        archiveGroup as ArchiveGroup?
                                    }
                                    .recover { error ->
                                        logger.severe("Skipping ArchiveGroup [${archiveGroup.name}] during startup: ${error.message}")
                                    Future.succeededFuture<ArchiveGroup?>(null)
                                    }
                            }

                            @Suppress("UNCHECKED_CAST")
                            Future.all<Any>(deploymentFutures as List<Future<Any>>).onComplete { result ->
                                if (result.succeeded()) {
                                    val deployedGroups = deploymentFutures.mapNotNull {
                                        if (it.succeeded()) it.result() else null
                                    }
                                    promise.complete(deployedGroups)
                                } else {
                                    promise.fail(result.cause())
                                }
                            }
                        }
                    } else {
                        logger.severe("Failed to load archive groups from database: ${getAllResult.cause()?.message}")
                        promise.fail(getAllResult.cause())
                    }
                }
            } else {
                logger.severe("Failed to deploy ConfigStore: ${deployResult.cause()?.message}")
                promise.fail(deployResult.cause())
            }
        }
    }



    fun createDatabaseConfig(): JsonObject {
        val databaseConfig = JsonObject()
        configJson.getJsonObject("Postgres")?.let { databaseConfig.put("Postgres", it) }
        configJson.getJsonObject("CrateDB")?.let { databaseConfig.put("CrateDB", it) }
        configJson.getJsonObject("MongoDB")?.let { databaseConfig.put("MongoDB", it) }
        configJson.getJsonObject("SQLite")?.let { databaseConfig.put("SQLite", it) }
        configJson.getJsonObject("Kafka")?.let { databaseConfig.put("Kafka", it) }
        return databaseConfig
    }

    private fun mongoUrlWithCredentials(url: String, username: String?, password: String?): String {
        if (username.isNullOrBlank() || password.isNullOrBlank()) return url
        val schemeIndex = url.indexOf("://")
        if (schemeIndex < 0) return url

        val authorityStart = schemeIndex + 3
        val pathStart = listOf(
            url.indexOf('/', authorityStart),
            url.indexOf('?', authorityStart)
        ).filter { it >= 0 }.minOrNull() ?: url.length

        val authority = url.substring(authorityStart, pathStart)
        val hostAuthority = authority.substringAfterLast("@")

        val encodedUser = URLEncoder.encode(username, StandardCharsets.UTF_8)
        val encodedPass = URLEncoder.encode(password, StandardCharsets.UTF_8)
        return url.substring(0, authorityStart) + "$encodedUser:$encodedPass@$hostAuthority" + url.substring(pathStart)
    }

    private fun resolveDatabaseConfig(archiveGroup: ArchiveGroup, configStore: IArchiveConfigStore? = deployedConfigStore): Future<JsonObject> {
        val selectedName = archiveGroup.getDatabaseConnectionName()
        val baseConfig = createDatabaseConfig()
        if (selectedName.isNullOrBlank()) {
            return Future.succeededFuture(baseConfig)
        }

        val requiredTypes = setOfNotNull(
            when (archiveGroup.getLastValType()) {
                MessageStoreType.POSTGRES -> DatabaseConnectionType.POSTGRES
                MessageStoreType.MONGODB -> DatabaseConnectionType.MONGODB
                else -> null
            },
            when (archiveGroup.getArchiveType()) {
                MessageArchiveType.POSTGRES -> DatabaseConnectionType.POSTGRES
                MessageArchiveType.MONGODB -> DatabaseConnectionType.MONGODB
                else -> null
            }
        )

        fun apply(connection: DatabaseConnectionConfig?): JsonObject {
            if (connection == null) {
                throw IllegalArgumentException("Database connection '$selectedName' not found for archive group '${archiveGroup.name}'")
            }

            if (requiredTypes.size > 1 && connection.name != "Default") {
                throw IllegalArgumentException("Archive group '${archiveGroup.name}' cannot use one selected database connection for mixed Postgres and MongoDB stores")
            }
            if (requiredTypes.isNotEmpty() && connection.type !in requiredTypes) {
                throw IllegalArgumentException("Archive group '${archiveGroup.name}' selected ${connection.type} connection '$selectedName' but requires ${requiredTypes.first()}")
            }

            when (connection.type) {
                DatabaseConnectionType.POSTGRES -> baseConfig.put("Postgres", JsonObject()
                    .put("Url", connection.url)
                    .put("User", connection.username ?: "")
                    .put("Pass", connection.password ?: "")
                    .put("Schema", connection.schema)
                )
                DatabaseConnectionType.MONGODB -> baseConfig.put("MongoDB", JsonObject()
                    .put("Url", mongoUrlWithCredentials(connection.url, connection.username, connection.password))
                    .put("User", connection.username)
                    .put("Pass", connection.password)
                    .put("Database", connection.database ?: "monstermq")
                )
            }
            return baseConfig
        }

        if (selectedName == "Default") {
            if (requiredTypes.size > 1) {
                val missingTypes = requiredTypes.filter {
                    when (it) {
                        DatabaseConnectionType.POSTGRES -> configJson.getJsonObject("Postgres")?.getString("Url").isNullOrBlank()
                        DatabaseConnectionType.MONGODB -> configJson.getJsonObject("MongoDB")?.getString("Url").isNullOrBlank()
                    }
                }
                return if (missingTypes.isEmpty()) {
                    Future.succeededFuture(baseConfig)
                } else {
                    Future.failedFuture("Default database connection is not configured for ${missingTypes.joinToString(" and ")}")
                }
            }

            val requiredType = requiredTypes.firstOrNull()
                ?: return Future.failedFuture("Default database connection can only be used for Postgres or MongoDB stores")
            val connection = when (requiredType) {
                DatabaseConnectionType.POSTGRES -> configJson.getJsonObject("Postgres")?.let {
                    val url = it.getString("Url")
                    if (url.isNullOrEmpty()) null else DatabaseConnectionConfig(
                        name = "Default",
                        type = DatabaseConnectionType.POSTGRES,
                        url = url,
                        username = it.getString("User"),
                        password = it.getString("Pass"),
                        schema = it.getString("Schema"),
                        readOnly = true
                    )
                }
                DatabaseConnectionType.MONGODB -> configJson.getJsonObject("MongoDB")?.let {
                    val url = it.getString("Url")
                    if (url.isNullOrEmpty()) null else DatabaseConnectionConfig(
                        name = "Default",
                        type = DatabaseConnectionType.MONGODB,
                        url = url,
                        database = it.getString("Database", "monstermq"),
                        readOnly = true
                    )
                }
            }
            return Future.succeededFuture(apply(connection))
        }

        if (configStore == null) {
            return Future.failedFuture("No ConfigStore available to resolve database connection '$selectedName'")
        }

        return configStore.getDatabaseConnection(selectedName).map { apply(it) }
    }

    private fun recreateArchiveGroup(ag: ArchiveGroup, databaseConfig: JsonObject): ArchiveGroup {
        return ArchiveGroup(
            name = ag.name,
            topicFilter = ag.topicFilter,
            retainedOnly = ag.retainedOnly,
            lastValType = ag.getLastValType(),
            archiveType = ag.getArchiveType(),
            payloadFormat = ag.payloadFormat,
            lastValRetentionMs = ag.getLastValRetentionMs(),
            archiveRetentionMs = ag.getArchiveRetentionMs(),
            purgeIntervalMs = ag.getPurgeIntervalMs(),
            lastValRetentionStr = ag.getLastValRetention(),
            archiveRetentionStr = ag.getArchiveRetention(),
            purgeIntervalStr = ag.getPurgeInterval(),
            databaseConnectionName = ag.getDatabaseConnectionName(),
            databaseConfig = databaseConfig
        )
    }

    fun getMessageArchive(
        name: String,
        storeType: MessageArchiveType,
        postgresUrl: String, postgresUser: String, postgresPass: String,
        crateDbUrl: String, crateDbUser: String, crateDbPass: String,
        mongoDbUrl: String, mongoDbDatabase: String,
        sqlitePath: String,
        kafkaServers: String
    ): Pair<IMessageArchive?, Future<Void>> {
        val promise = Promise.promise<Void>()

        val archive = when (storeType) {
            MessageArchiveType.NONE -> {
                promise.complete()
                MessageArchiveNone
            }
            MessageArchiveType.POSTGRES -> {
                val store = MessageArchivePostgres(
                    name,
                    postgresUrl, postgresUser, postgresPass
                )
                val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onComplete { promise.complete() }
                store
            }
            MessageArchiveType.CRATEDB -> {
                val store = MessageArchiveCrateDB(
                    name,
                    crateDbUrl, crateDbUser, crateDbPass
                )
                val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onComplete { promise.complete() }
                store
            }
            MessageArchiveType.MONGODB -> {
                val store = MessageArchiveMongoDB(
                    name,
                    mongoDbUrl, mongoDbDatabase
                )
                val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onComplete { promise.complete() }
                store
            }
            MessageArchiveType.SQLITE -> {
                val store = MessageArchiveSQLite(name, sqlitePath)
                val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onComplete { promise.complete() }
                store
            }
            MessageArchiveType.KAFKA -> {
                val kafkaConfig = configJson.getJsonObject("Kafka")?.getJsonObject("Config")
                val store = MessageArchiveKafka(name, kafkaServers, kafkaConfig, payloadFormat = PayloadFormat.DEFAULT)
                val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onSuccess { promise.complete() }.onFailure { promise.fail(it) }
                store
            }
        }

        return archive to promise.future()
    }

    // Runtime ArchiveGroup Management Methods

    fun startArchiveGroup(name: String, shouldBroadcast: Boolean = true): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        // Check if already deployed
        if (deployedArchiveGroups.containsKey(name)) {
            logger.finer("ArchiveGroup [$name] is already deployed")
            promise.complete(true) // Return true since it's already deployed
            return promise.future()
        }

        // Load from database/config and deploy
        val configStoreType = Monster.getConfigStoreType(configJson)
        if (configStoreType != "NONE") {
            // Load from database
            loadArchiveGroupFromDatabase(name, configStoreType).onComplete { result ->
                if (result.succeeded() && result.result() != null) {
                    deployArchiveGroupRuntime(result.result()!!).onComplete { deployResult ->
                        if (deployResult.succeeded()) {
                            logger.fine("ArchiveGroup [$name] deployed successfully - database connections will be established in background")
                            promise.complete(true)
                            // Broadcast to cluster (unless this is a cluster event replication)
                            if (shouldBroadcast) {
                                broadcastArchiveGroupEvent("STARTED", name)
                            }
                        } else {
                            logger.severe("Failed to deploy ArchiveGroup [$name]: ${deployResult.cause()?.message}")
                            promise.complete(false) // Don't fail the future, just return false
                        }
                    }
                } else {
                    logger.warning("ArchiveGroup [$name] not found in database")
                    promise.complete(false)
                }
            }
        } else {
            // Load from YAML config
            loadArchiveGroupFromConfig(name).onComplete { result ->
                if (result.succeeded() && result.result() != null) {
                    deployArchiveGroupRuntime(result.result()!!).onComplete { deployResult ->
                        if (deployResult.succeeded()) {
                            logger.fine("ArchiveGroup [$name] deployed successfully - database connections will be established in background")
                            promise.complete(true)
                            // Broadcast to cluster (unless this is a cluster event replication)
                            if (shouldBroadcast) {
                                broadcastArchiveGroupEvent("STARTED", name)
                            }
                        } else {
                            logger.severe("Failed to deploy ArchiveGroup [$name]: ${deployResult.cause()?.message}")
                            promise.complete(false) // Don't fail the future, just return false
                        }
                    }
                } else {
                    logger.warning("ArchiveGroup [$name] not found in config")
                    promise.complete(false)
                }
            }
        }

        return promise.future()
    }

    fun stopArchiveGroup(name: String, shouldBroadcast: Boolean = true): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        val archiveInfo = deployedArchiveGroups[name]
        if (archiveInfo == null) {
            logger.finer("ArchiveGroup [$name] is not deployed")
            promise.complete(true) // Return true since it's already stopped
            return promise.future()
        }

        logger.fine("Stopping ArchiveGroup [$name] with deployment ID: ${archiveInfo.deploymentId}")

        // Always undeploy the verticle, regardless of connection state
        // Use a shorter timeout to prevent hanging
        val undeployPromise = Promise.promise<String>()

        vertx.undeploy(archiveInfo.deploymentId).onComplete { result ->
            if (!undeployPromise.future().isComplete) {
                if (result.succeeded()) {
                    logger.fine("ArchiveGroup [$name] undeployed successfully")
                    undeployPromise.complete(archiveInfo.deploymentId)
                } else {
                    logger.warning("Undeploy of ArchiveGroup [$name] failed but proceeding with cleanup: ${result.cause()?.message}")
                    // Even if undeploy fails, we proceed with cleanup
                    undeployPromise.complete(archiveInfo.deploymentId)
                }
            }
        }

        // Apply timeout to undeploy operation
        vertx.setTimer(5000) { // 5 second timeout
            if (!undeployPromise.future().isComplete) {
                logger.warning("Undeploy of ArchiveGroup [$name] timed out after 5 seconds, proceeding with cleanup")
                undeployPromise.complete(archiveInfo.deploymentId)
            }
        }

        undeployPromise.future().onComplete {
            // Always clean up tracking, regardless of undeploy success
            deployedArchiveGroups.remove(name)
            logger.fine("ArchiveGroup [$name] stopped and removed from tracking")

            // Unregister from MessageHandler
            messageHandler?.unregisterArchiveGroup(name)

            promise.complete(true)
            // Broadcast to cluster (unless this is a cluster event replication)
            if (shouldBroadcast) {
                broadcastArchiveGroupEvent("STOPPED", name)
            }
        }

        return promise.future()
    }

    fun getArchiveGroupStatus(name: String): JsonObject {
        val archiveInfo = deployedArchiveGroups[name]
        return JsonObject().apply {
            put("name", name)
            put("deployed", archiveInfo != null)
            if (archiveInfo != null) {
                put("deploymentId", archiveInfo.deploymentId)
                put("enabled", archiveInfo.enabled)
                put("topicFilter", JsonArray(archiveInfo.archiveGroup.topicFilter))
                put("retainedOnly", archiveInfo.archiveGroup.retainedOnly)
                put("lastValType", archiveInfo.archiveGroup.getLastValType().name)
                put("archiveType", archiveInfo.archiveGroup.getArchiveType().name)
                put("databaseConnectionName", archiveInfo.archiveGroup.getDatabaseConnectionName())
                put("payloadFormat", archiveInfo.archiveGroup.payloadFormat.name)
                put("lastValRetention", archiveInfo.archiveGroup.getLastValRetentionMs()?.toString())
                put("archiveRetention", archiveInfo.archiveGroup.getArchiveRetentionMs()?.toString())
                put("purgeInterval", archiveInfo.archiveGroup.getPurgeIntervalMs()?.toString())
            }
        }
    }

    fun getConfigStore(): IArchiveConfigStore? {
        // Return the deployed ConfigStore instance if available
        // Otherwise return null (no database configuration)
        return deployedConfigStore
    }

    /**
     * Set the MessageHandler reference for dynamic archive group registration
     */
    fun setMessageHandler(messageHandler: MessageHandler) {
        this.messageHandler = messageHandler
        logger.fine("MessageHandler reference set for dynamic archive group registration")
    }

    fun listArchiveGroups(): JsonArray {
        val result = JsonArray()
        deployedArchiveGroups.values.forEach { archiveInfo ->
            result.add(JsonObject().apply {
                put("name", archiveInfo.archiveGroup.name)
                put("deploymentId", archiveInfo.deploymentId)
                put("enabled", archiveInfo.enabled)
                put("deployed", true)
                put("topicFilter", JsonArray(archiveInfo.archiveGroup.topicFilter))
                put("retainedOnly", archiveInfo.archiveGroup.retainedOnly)
                put("lastValType", archiveInfo.archiveGroup.getLastValType().name)
                put("archiveType", archiveInfo.archiveGroup.getArchiveType().name)
                put("databaseConnectionName", archiveInfo.archiveGroup.getDatabaseConnectionName())
                put("payloadFormat", archiveInfo.archiveGroup.payloadFormat.name)
                put("lastValRetention", archiveInfo.archiveGroup.getLastValRetentionMs()?.toString())
                put("archiveRetention", archiveInfo.archiveGroup.getArchiveRetentionMs()?.toString())
                put("purgeInterval", archiveInfo.archiveGroup.getPurgeIntervalMs()?.toString())
            })
        }
        return result
    }

    // Event Handler Methods

    private fun handleArchiveEvent(message: Message<JsonObject>) {
        val event = message.body().getString("event")
        val archiveGroupName = message.body().getString("archiveGroup")
        val sourceNodeId = message.body().getString("nodeId", "")
        val currentNodeId = Monster.getClusterNodeId(vertx)

        if (event == null || archiveGroupName == null) {
            logger.warning("Invalid archive event: event=$event, archiveGroup=$archiveGroupName")
            return
        }

        // Skip events from the same node to avoid duplicate processing (only in local event bus)
        if (!Monster.isClustered() && sourceNodeId == currentNodeId) {
            logger.finer("Skipping archive event from same node in non-clustered mode: event=$event, archiveGroup=$archiveGroupName")
            return
        }

        when (event) {
            "STARTED" -> {
                logger.fine("Received cluster event to start ArchiveGroup [$archiveGroupName]")
                startArchiveGroup(archiveGroupName, shouldBroadcast = false).onComplete { result ->
                    if (!result.succeeded()) {
                        logger.finer("ArchiveGroup [$archiveGroupName] already started or start failed: ${result.cause()?.message}")
                    }
                }
            }
            "STOPPED" -> {
                logger.fine("Received cluster event to stop ArchiveGroup [$archiveGroupName]")
                stopArchiveGroup(archiveGroupName, shouldBroadcast = false).onComplete { result ->
                    if (!result.succeeded()) {
                        logger.finer("ArchiveGroup [$archiveGroupName] already stopped or stop failed: ${result.cause()?.message}")
                    }
                }
            }
            else -> {
                logger.warning("Unknown archive event type: $event")
            }
        }
    }

    private fun handleArchiveStart(message: Message<JsonObject>) {
        val name = message.body().getString("name")
        if (name != null) {
            startArchiveGroup(name).onComplete { result ->
                if (result.succeeded()) {
                    message.reply(JsonObject().put("success", true).put("name", name))
                } else {
                    message.reply(JsonObject().put("success", false).put("error", result.cause()?.message))
                }
            }
        } else {
            message.reply(JsonObject().put("success", false).put("error", "Name parameter missing"))
        }
    }

    private fun handleArchiveStop(message: Message<JsonObject>) {
        val name = message.body().getString("name")
        if (name != null) {
            stopArchiveGroup(name).onComplete { result ->
                if (result.succeeded()) {
                    message.reply(JsonObject().put("success", true).put("name", name))
                } else {
                    message.reply(JsonObject().put("success", false).put("error", result.cause()?.message))
                }
            }
        } else {
            message.reply(JsonObject().put("success", false).put("error", "Name parameter missing"))
        }
    }

    private fun handleArchiveStatus(message: Message<JsonObject>) {
        val name = message.body().getString("name")
        if (name != null) {
            message.reply(getArchiveGroupStatus(name))
        } else {
            message.reply(JsonObject().put("error", "Name parameter missing"))
        }
    }

    private fun handleArchiveList(message: Message<JsonObject>) {
        message.reply(JsonObject().put("archiveGroups", listArchiveGroups()))
    }

    private fun handleArchiveConnectionStatus(message: Message<JsonObject>) {
        val name = message.body().getString("name")

        // Execute connection status checks asynchronously to avoid blocking the event loop
        vertx.executeBlocking(Callable {
            if (name != null) {
                getArchiveGroupConnectionStatus(name)
            } else {
                // Get connection status for all deployed archive groups
                val allConnectionStatus = getAllArchiveGroupConnectionStatus()
                JsonObject().put("connectionStatus", allConnectionStatus)
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                message.reply(result.result())
            } else {
                logger.warning("Error getting connection status: ${result.cause()?.message}")
                message.reply(JsonObject()
                    .put("success", false)
                    .put("error", "Failed to get connection status: ${result.cause()?.message}"))
            }
        }
    }

    // Helper Methods

    private fun deployArchiveGroupRuntime(archiveGroup: ArchiveGroup): Future<String> {
        val promise = Promise.promise<String>()
        val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)

        vertx.deployVerticle(archiveGroup, options).onComplete { result ->
            if (result.succeeded()) {
                val deploymentId = result.result()
                val archiveInfo = ArchiveGroupInfo(archiveGroup, deploymentId, true)
                deployedArchiveGroups[archiveGroup.name] = archiveInfo
                logger.fine("ArchiveGroup [${archiveGroup.name}] deployed at runtime with ID: $deploymentId")

                // Register with MessageHandler for message routing
                messageHandler?.registerArchiveGroup(archiveGroup)
                    ?: logger.warning("MessageHandler not available - archive group [${archiveGroup.name}] won't receive messages")

                promise.complete(deploymentId)
            } else {
                logger.severe("Failed to deploy ArchiveGroup [${archiveGroup.name}] at runtime: ${result.cause()?.message}")
                promise.fail(result.cause())
            }
        }

        return promise.future()
    }

    private fun loadArchiveGroupFromDatabase(name: String, configStoreType: String): Future<ArchiveGroup?> {
        val promise = Promise.promise<ArchiveGroup?>()

        // Use the deployed ConfigStore if available
        val configStore = deployedConfigStore
        if (configStore != null) {
            // ConfigStore is already deployed, use it directly
            configStore.getArchiveGroup(name).onComplete { getResult ->
                if (getResult.succeeded()) {
                    val archiveGroupConfig = getResult.result()
                    if (archiveGroupConfig != null) {
                        // Don't check enabled flag here - we want to load it regardless
                        val ag = archiveGroupConfig.archiveGroup
                        resolveDatabaseConfig(ag, configStore).onComplete { resolveResult ->
                            if (resolveResult.succeeded()) {
                                promise.complete(recreateArchiveGroup(ag, resolveResult.result()))
                            } else {
                                promise.fail(resolveResult.cause())
                            }
                        }
                    } else {
                        promise.complete(null)
                    }
                } else {
                    logger.severe("Failed to get archive group from ConfigStore: ${getResult.cause()?.message}")
                    promise.complete(null)
                }
            }
            return promise.future()
        }

        // Fallback: create a new ConfigStore (shouldn't normally happen)
        val newConfigStore = ArchiveConfigStoreFactory.createConfigStore(configJson, configStoreType)

        if (newConfigStore == null) {
            promise.fail("Failed to create ConfigStore of type $configStoreType")
            return promise.future()
        }

        val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
        vertx.deployVerticle(newConfigStore as AbstractVerticle, options).onComplete { deployResult ->
            if (deployResult.succeeded()) {
                newConfigStore.getArchiveGroup(name).onComplete { getResult ->
                    if (getResult.succeeded()) {
                        val archiveGroupConfig = getResult.result()
                        if (archiveGroupConfig != null) {
                            // Don't check enabled flag here - we want to load it regardless
                            val ag = archiveGroupConfig.archiveGroup
                            resolveDatabaseConfig(ag, newConfigStore).onComplete { resolveResult ->
                                if (resolveResult.succeeded()) {
                                    promise.complete(recreateArchiveGroup(ag, resolveResult.result()))
                                } else {
                                    promise.fail(resolveResult.cause())
                                }
                                vertx.undeploy(deployResult.result())
                            }
                        } else {
                            promise.complete(null)
                            vertx.undeploy(deployResult.result())
                        }
                    } else {
                        logger.severe("Failed to get archive group from temporary ConfigStore: ${getResult.cause()?.message}")
                        promise.complete(null)
                        vertx.undeploy(deployResult.result())
                    }
                }
            } else {
                promise.fail(deployResult.cause())
            }
        }

        return promise.future()
    }

    private fun loadArchiveGroupFromConfig(name: String): Future<ArchiveGroup?> {
        val promise = Promise.promise<ArchiveGroup?>()

        val archiveGroupConfigs = configJson.getJsonArray("ArchiveGroups", JsonArray())
            .filterIsInstance<JsonObject>()
            .filter { it.getString("Name") == name && it.getBoolean("Enabled", true) }

        if (archiveGroupConfigs.isNotEmpty()) {
            val config = archiveGroupConfigs.first()
            val databaseConfig = createDatabaseConfig()
            val archiveGroup = ArchiveGroup.fromConfig(config, databaseConfig, isClustered)
            promise.complete(archiveGroup)
        } else {
            promise.complete(null)
        }

        return promise.future()
    }

    private fun broadcastArchiveGroupEvent(event: String, archiveGroupName: String) {
        val currentNodeId = Monster.getClusterNodeId(vertx)
        val eventData = JsonObject().apply {
            put("event", event)
            put("archiveGroup", archiveGroupName)
            put("timestamp", System.currentTimeMillis())
            put("nodeId", currentNodeId)
        }

        vertx.eventBus().publish("mq.cluster.archive.events", eventData)
        logger.finer("Broadcasted archive event: $event for ArchiveGroup [$archiveGroupName] from node $currentNodeId")
    }

    fun getDeployedArchiveGroups(): Map<String, ArchiveGroup> {
        return deployedArchiveGroups.mapValues { it.value.archiveGroup }
    }

    fun getArchiveGroupConnectionStatus(name: String): JsonObject {
        val archiveInfo = deployedArchiveGroups[name]
        if (archiveInfo == null) {
            return JsonObject()
                .put("success", false)
                .put("name", name)
                .put("error", "ArchiveGroup not deployed")
        }

        return try {
            val archiveGroup = archiveInfo.archiveGroup
            val connectionStatus = mutableMapOf<String, Boolean>()

            // Check message archive connection
            archiveGroup.archiveStore?.let { archive ->
                connectionStatus["messageArchive"] = archive.getConnectionStatus()
            }

            // Check last value store connection if it exists
            archiveGroup.lastValStore?.let { store ->
                connectionStatus["lastValueStore"] = store.getConnectionStatus()
            }

            JsonObject()
                .put("success", true)
                .put("name", name)
                .put("enabled", archiveInfo.enabled)
                .put("type", archiveGroup.archiveStore?.getType()?.toString() ?: "NONE")
                .put("connectionStatus", JsonObject(connectionStatus as Map<String, Any>))
        } catch (e: Exception) {
            logger.warning("Error checking connection status for ArchiveGroup [$name]: ${e.message}")
            JsonObject()
                .put("success", false)
                .put("name", name)
                .put("error", "Failed to check connection status: ${e.message}")
        }
    }

    private fun getAllArchiveGroupConnectionStatus(): JsonArray {
        val result = JsonArray()

        deployedArchiveGroups.forEach { (name, _) ->
            result.add(getArchiveGroupConnectionStatus(name))
        }

        return result
    }
}
