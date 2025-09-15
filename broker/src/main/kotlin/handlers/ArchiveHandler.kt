package at.rocworks.handlers

import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.handlers.MessageHandler
import at.rocworks.stores.*
import at.rocworks.stores.ConfigStoreFactory
import at.rocworks.stores.MessageStoreType
import at.rocworks.stores.MessageArchiveType
import at.rocworks.stores.cratedb.MessageArchiveCrateDB
import at.rocworks.stores.mongodb.MessageArchiveMongoDB
import at.rocworks.stores.postgres.MessageArchivePostgres
import at.rocworks.stores.sqlite.MessageArchiveSQLite
import at.rocworks.stores.MessageArchiveKafka
import io.vertx.config.ConfigRetriever
import io.vertx.config.ConfigRetrieverOptions
import io.vertx.config.ConfigStoreOptions
import io.vertx.core.*
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

data class ArchiveGroupInfo(
    val archiveGroup: ArchiveGroup,
    val deploymentId: String,
    val enabled: Boolean
)

class ArchiveHandler(
    private val vertx: Vertx,
    private val configJson: JsonObject,
    private val archiveConfigFile: String?
) {
    private val logger: Logger = Utils.getLogger(this::class.java)

    // Track all deployed ArchiveGroups with their deployment IDs
    private val deployedArchiveGroups = ConcurrentHashMap<String, ArchiveGroupInfo>()

    // Keep reference to deployed ConfigStore if using database
    private var deployedConfigStore: IConfigStore? = null

    // Reference to MessageHandler for dynamic registration
    private var messageHandler: MessageHandler? = null

    companion object {
        // Event bus addresses for ArchiveGroup management
        const val ARCHIVE_START = "mq.cluster.archive.start"
        const val ARCHIVE_STOP = "mq.cluster.archive.stop"
        const val ARCHIVE_STATUS = "mq.cluster.archive.status"
        const val ARCHIVE_LIST = "mq.cluster.archive.list"
    }

    fun initialize(): Future<List<ArchiveGroup>> {
        val promise = Promise.promise<List<ArchiveGroup>>()

        // Setup message bus event handlers
        setupEventHandlers()

        // Load archive config if specified
        if (archiveConfigFile != null) {
            loadArchiveConfig().onComplete { loadResult ->
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

        logger.info("ArchiveHandler event handlers registered")
    }

    private fun loadArchiveConfig(): Future<Void> {
        val promise = Promise.promise<Void>()

        try {
            logger.info("Loading archive configuration from: $archiveConfigFile")

            val archiveConfigRetriever = ConfigRetriever.create(
                vertx,
                ConfigRetrieverOptions().addStore(
                    ConfigStoreOptions()
                        .setType("file")
                        .setFormat("yaml")
                        .setConfig(JsonObject().put("path", archiveConfigFile))
                )
            )

            archiveConfigRetriever.config.onComplete { result ->
                if (result.succeeded()) {
                    val archiveConfig = result.result()
                    val archiveGroups = archiveConfig.getJsonArray("ArchiveGroups")

                    if (archiveGroups != null && !archiveGroups.isEmpty) {
                        val configStoreType = configJson.getString("ConfigStoreType")

                        if (configStoreType != null) {
                            // Import into database
                            importArchiveConfigToDatabase(archiveGroups, configStoreType).onComplete { importResult ->
                                if (importResult.succeeded()) {
                                    promise.complete()
                                } else {
                                    promise.fail(importResult.cause())
                                }
                            }
                        } else {
                            // Merge or replace ArchiveGroups in main config (existing behavior)
                            configJson.put("ArchiveGroups", archiveGroups)
                            logger.info("Loaded ${archiveGroups.size()} archive groups from $archiveConfigFile into memory")
                            promise.complete()
                        }
                    } else {
                        logger.warning("No ArchiveGroups found in $archiveConfigFile")
                        promise.complete()
                    }
                } else {
                    logger.severe("Failed to load archive config from $archiveConfigFile: ${result.cause()?.message}")
                    promise.fail(result.cause())
                }
            }
        } catch (e: Exception) {
            logger.severe("Error loading archive config from $archiveConfigFile: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    private fun importArchiveConfigToDatabase(archiveGroups: JsonArray, configStoreType: String): Future<Void> {
        val promise = Promise.promise<Void>()
        val configStore = ConfigStoreFactory.createConfigStore(configJson, configStoreType)

        if (configStore == null) {
            logger.severe("Failed to create ConfigStore of type $configStoreType for import")
            promise.fail("Failed to create ConfigStore of type $configStoreType for import")
            return promise.future()
        }

        // Deploy ConfigStore temporarily for import
        val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
        vertx.deployVerticle(configStore as AbstractVerticle, options).onComplete { deployResult ->
            if (deployResult.succeeded()) {
                logger.info("ConfigStore deployed for import")
                // Store reference to deployed ConfigStore
                deployedConfigStore = configStore

                // Create database configuration object
                val databaseConfig = createDatabaseConfig()

                // Import each archive group
                var importedCount = 0
                archiveGroups.filterIsInstance<JsonObject>().forEach { config ->
                    try {
                        val archiveGroup = ArchiveGroup.fromConfig(config, databaseConfig)
                        val enabled = config.getBoolean("Enabled", true)

                        val success = configStore.saveArchiveGroup(archiveGroup, enabled)
                        if (success) {
                            importedCount++
                            logger.info("Imported ArchiveGroup [${archiveGroup.name}] to database")
                        } else {
                            logger.warning("Failed to import ArchiveGroup [${archiveGroup.name}] to database")
                        }
                    } catch (e: Exception) {
                        logger.severe("Error importing archive group: ${e.message}")
                    }
                }

                logger.info("Successfully imported $importedCount archive groups from $archiveConfigFile to database")

                // Undeploy the temporary ConfigStore
                vertx.undeploy(deployResult.result()).onComplete { undeployResult ->
                    if (undeployResult.succeeded()) {
                        logger.info("Temporary ConfigStore undeployed after import")
                        promise.complete()
                    } else {
                        logger.warning("Failed to undeploy temporary ConfigStore: ${undeployResult.cause()?.message}")
                        promise.complete() // Still complete successfully even if undeploy fails
                    }
                }
            } else {
                logger.severe("Failed to deploy ConfigStore for import: ${deployResult.cause()?.message}")
                promise.fail(deployResult.cause())
            }
        }

        return promise.future()
    }

    private fun deployArchiveGroups(): Future<List<ArchiveGroup>> {
        val promise = Promise.promise<List<ArchiveGroup>>()

        // Check if ConfigStore is configured
        val configStoreType = configJson.getString("ConfigStoreType")

        if (configStoreType != null) {
            // Load from database
            loadArchiveGroupsFromDatabase(configStoreType, promise)
        } else {
            // Load from YAML configuration (existing behavior)
            loadArchiveGroupsFromConfig(promise)
        }

        return promise.future()
    }

    private fun loadArchiveGroupsFromDatabase(configStoreType: String, promise: Promise<List<ArchiveGroup>>) {
        val configStore = ConfigStoreFactory.createConfigStore(configJson, configStoreType)

        if (configStore == null) {
            logger.severe("Failed to create ConfigStore of type $configStoreType")
            promise.fail("Failed to create ConfigStore of type $configStoreType")
            return
        }

        // Deploy ConfigStore
        val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
        vertx.deployVerticle(configStore as AbstractVerticle, options).onComplete { deployResult ->
            if (deployResult.succeeded()) {
                logger.info("ConfigStore deployed successfully")
                // Store reference to deployed ConfigStore
                deployedConfigStore = configStore

                // Create database configuration object first
                val databaseConfig = createDatabaseConfig()

                // Load archive groups from database
                val archiveGroupConfigs = configStore.getAllArchiveGroups().filter { it.enabled }
                val archiveGroups = archiveGroupConfigs.map { config ->
                    // Recreate ArchiveGroup with proper database config
                    val ag = config.archiveGroup
                    ArchiveGroup(
                        name = ag.name,
                        topicFilter = ag.topicFilter,
                        retainedOnly = ag.retainedOnly,
                        lastValType = MessageStoreType.POSTGRES, // Hard-coded for now
                        archiveType = MessageArchiveType.POSTGRES, // Hard-coded for now
                        lastValRetentionMs = ag.getLastValRetentionMs(),
                        archiveRetentionMs = ag.getArchiveRetentionMs(),
                        purgeIntervalMs = ag.getPurgeIntervalMs(),
                        databaseConfig = databaseConfig
                    )
                }

                if (archiveGroups.isEmpty()) {
                    logger.info("No enabled archive groups found in database")
                    promise.complete(emptyList())
                    return@onComplete
                }

                // Deploy each archive group
                val deploymentFutures: List<Future<ArchiveGroup>> = archiveGroups.map { archiveGroup ->
                    val archiveGroupOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)

                    vertx.deployVerticle(archiveGroup, archiveGroupOptions).map { deploymentId ->
                        // Track the deployment
                        val archiveInfo = ArchiveGroupInfo(archiveGroup, deploymentId, true)
                        deployedArchiveGroups[archiveGroup.name] = archiveInfo

                        logger.info("ArchiveGroup [${archiveGroup.name}] deployed successfully from database with ID: $deploymentId")
                        archiveGroup
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
            } else {
                logger.severe("Failed to deploy ConfigStore: ${deployResult.cause()?.message}")
                promise.fail(deployResult.cause())
            }
        }
    }

    private fun loadArchiveGroupsFromConfig(promise: Promise<List<ArchiveGroup>>) {
        val archiveGroupConfigs = configJson.getJsonArray("ArchiveGroups", JsonArray())
            .filterIsInstance<JsonObject>()
            .filter { it.getBoolean("Enabled") }

        if (archiveGroupConfigs.isEmpty()) {
            promise.complete(emptyList())
            return
        }

        // Create database configuration object with all database configs
        val databaseConfig = createDatabaseConfig()

        val deploymentFutures: List<Future<ArchiveGroup>> = archiveGroupConfigs.map { config ->
            val archiveGroup = ArchiveGroup.fromConfig(config, databaseConfig)
            val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)

            vertx.deployVerticle(archiveGroup, options).map { deploymentId ->
                // Track the deployment
                val archiveInfo = ArchiveGroupInfo(archiveGroup, deploymentId, true)
                deployedArchiveGroups[archiveGroup.name] = archiveInfo

                logger.info("ArchiveGroup [${archiveGroup.name}] deployed successfully with ID: $deploymentId")
                archiveGroup
            }
        }

        @Suppress("UNCHECKED_CAST")
        Future.all<Any>(deploymentFutures as List<Future<Any>>).onComplete { result ->
            if (result.succeeded()) {
                val archiveGroups = deploymentFutures.mapNotNull {
                    if (it.succeeded()) it.result() else null
                }
                promise.complete(archiveGroups)
            } else {
                promise.fail(result.cause())
            }
        }
    }

    private fun createDatabaseConfig(): JsonObject {
        val databaseConfig = JsonObject()
        configJson.getJsonObject("Postgres")?.let { databaseConfig.put("Postgres", it) }
        configJson.getJsonObject("CrateDB")?.let { databaseConfig.put("CrateDB", it) }
        configJson.getJsonObject("MongoDB")?.let { databaseConfig.put("MongoDB", it) }
        configJson.getJsonObject("SQLite")?.let { databaseConfig.put("SQLite", it) }
        configJson.getJsonObject("Kafka")?.let { databaseConfig.put("Kafka", it) }
        return databaseConfig
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
                val store = MessageArchiveKafka(name, kafkaServers)
                val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onSuccess { promise.complete() }.onFailure { promise.fail(it) }
                store
            }
        }

        return archive to promise.future()
    }

    // Runtime ArchiveGroup Management Methods

    fun startArchiveGroup(name: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        // Check if already deployed
        if (deployedArchiveGroups.containsKey(name)) {
            logger.warning("ArchiveGroup [$name] is already deployed")
            promise.complete(false)
            return promise.future()
        }

        // Load from database/config and deploy
        val configStoreType = configJson.getString("ConfigStoreType")
        if (configStoreType != null) {
            // Load from database
            loadArchiveGroupFromDatabase(name, configStoreType).onComplete { result ->
                if (result.succeeded() && result.result() != null) {
                    deployArchiveGroupRuntime(result.result()!!).onComplete { deployResult ->
                        if (deployResult.succeeded()) {
                            promise.complete(true)
                            // Broadcast to cluster
                            broadcastArchiveGroupEvent("STARTED", name)
                        } else {
                            promise.fail(deployResult.cause())
                        }
                    }
                } else {
                    promise.fail("ArchiveGroup [$name] not found in database")
                }
            }
        } else {
            // Load from YAML config
            loadArchiveGroupFromConfig(name).onComplete { result ->
                if (result.succeeded() && result.result() != null) {
                    deployArchiveGroupRuntime(result.result()!!).onComplete { deployResult ->
                        if (deployResult.succeeded()) {
                            promise.complete(true)
                            // Broadcast to cluster
                            broadcastArchiveGroupEvent("STARTED", name)
                        } else {
                            promise.fail(deployResult.cause())
                        }
                    }
                } else {
                    promise.fail("ArchiveGroup [$name] not found in config")
                }
            }
        }

        return promise.future()
    }

    fun stopArchiveGroup(name: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        val archiveInfo = deployedArchiveGroups[name]
        if (archiveInfo == null) {
            logger.warning("ArchiveGroup [$name] is not deployed")
            promise.complete(false)
            return promise.future()
        }

        vertx.undeploy(archiveInfo.deploymentId).onComplete { result ->
            if (result.succeeded()) {
                deployedArchiveGroups.remove(name)
                logger.info("ArchiveGroup [$name] stopped successfully")

                // Unregister from MessageHandler
                messageHandler?.unregisterArchiveGroup(name)

                promise.complete(true)
                // Broadcast to cluster
                broadcastArchiveGroupEvent("STOPPED", name)
            } else {
                logger.severe("Failed to stop ArchiveGroup [$name]: ${result.cause()?.message}")
                promise.fail(result.cause())
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
            }
        }
    }

    fun getConfigStore(): IConfigStore? {
        // Return the deployed ConfigStore instance if available
        // Otherwise return null (no database configuration)
        return deployedConfigStore
    }

    /**
     * Set the MessageHandler reference for dynamic archive group registration
     */
    fun setMessageHandler(messageHandler: MessageHandler) {
        this.messageHandler = messageHandler
        logger.info("MessageHandler reference set for dynamic archive group registration")
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
                put("lastValRetention", archiveInfo.archiveGroup.getLastValRetentionMs()?.toString())
                put("archiveRetention", archiveInfo.archiveGroup.getArchiveRetentionMs()?.toString())
                put("purgeInterval", archiveInfo.archiveGroup.getPurgeIntervalMs()?.toString())
            })
        }
        return result
    }

    // Event Handler Methods

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

    // Helper Methods

    private fun deployArchiveGroupRuntime(archiveGroup: ArchiveGroup): Future<String> {
        val promise = Promise.promise<String>()
        val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)

        vertx.deployVerticle(archiveGroup, options).onComplete { result ->
            if (result.succeeded()) {
                val deploymentId = result.result()
                val archiveInfo = ArchiveGroupInfo(archiveGroup, deploymentId, true)
                deployedArchiveGroups[archiveGroup.name] = archiveInfo
                logger.info("ArchiveGroup [${archiveGroup.name}] deployed at runtime with ID: $deploymentId")

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
            val archiveGroupConfig = configStore.getArchiveGroup(name)
            if (archiveGroupConfig != null) {
                // Don't check enabled flag here - we want to load it regardless
                val databaseConfig = createDatabaseConfig()
                val ag = archiveGroupConfig.archiveGroup
                val archiveGroup = ArchiveGroup(
                    name = ag.name,
                    topicFilter = ag.topicFilter,
                    retainedOnly = ag.retainedOnly,
                    lastValType = ag.getLastValType(),
                    archiveType = ag.getArchiveType(),
                    lastValRetentionMs = ag.getLastValRetentionMs(),
                    archiveRetentionMs = ag.getArchiveRetentionMs(),
                    purgeIntervalMs = ag.getPurgeIntervalMs(),
                    databaseConfig = databaseConfig
                )
                promise.complete(archiveGroup)
            } else {
                promise.complete(null)
            }
            return promise.future()
        }

        // Fallback: create a new ConfigStore (shouldn't normally happen)
        val newConfigStore = ConfigStoreFactory.createConfigStore(configJson, configStoreType)

        if (newConfigStore == null) {
            promise.fail("Failed to create ConfigStore of type $configStoreType")
            return promise.future()
        }

        val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
        vertx.deployVerticle(newConfigStore as AbstractVerticle, options).onComplete { deployResult ->
            if (deployResult.succeeded()) {
                val archiveGroupConfig = newConfigStore.getArchiveGroup(name)
                if (archiveGroupConfig != null) {
                    // Don't check enabled flag here - we want to load it regardless
                    val databaseConfig = createDatabaseConfig()
                    val ag = archiveGroupConfig.archiveGroup
                    val archiveGroup = ArchiveGroup(
                        name = ag.name,
                        topicFilter = ag.topicFilter,
                        retainedOnly = ag.retainedOnly,
                        lastValType = ag.getLastValType(),
                        archiveType = ag.getArchiveType(),
                        lastValRetentionMs = ag.getLastValRetentionMs(),
                        archiveRetentionMs = ag.getArchiveRetentionMs(),
                        purgeIntervalMs = ag.getPurgeIntervalMs(),
                        databaseConfig = databaseConfig
                    )
                    promise.complete(archiveGroup)
                } else {
                    promise.complete(null)
                }

                // Cleanup temporary ConfigStore
                vertx.undeploy(deployResult.result())
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
            val archiveGroup = ArchiveGroup.fromConfig(config, databaseConfig)
            promise.complete(archiveGroup)
        } else {
            promise.complete(null)
        }

        return promise.future()
    }

    private fun broadcastArchiveGroupEvent(event: String, archiveGroupName: String) {
        val eventData = JsonObject().apply {
            put("event", event)
            put("archiveGroup", archiveGroupName)
            put("timestamp", System.currentTimeMillis())
            put("nodeId", "local") // TODO: Get actual node ID
        }

        vertx.eventBus().publish("mq.cluster.archive.events", eventData)
        logger.info("Broadcasted archive event: $event for ArchiveGroup [$archiveGroupName]")
    }
}