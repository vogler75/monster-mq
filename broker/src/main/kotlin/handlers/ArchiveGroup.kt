package at.rocworks.handlers

import at.rocworks.Utils
import at.rocworks.data.PurgeResult
import at.rocworks.data.TopicTree
import at.rocworks.stores.IMessageArchive
import at.rocworks.stores.IMessageStore
import at.rocworks.stores.MessageArchiveKafka
import at.rocworks.stores.MessageArchiveNone
import at.rocworks.stores.MessageArchiveType
import at.rocworks.stores.MessageStoreHazelcastDisconnected
import at.rocworks.stores.MessageStoreMemory
import at.rocworks.stores.MessageStoreNone
import at.rocworks.stores.MessageStoreType
import at.rocworks.stores.cratedb.MessageArchiveCrateDB
import at.rocworks.stores.cratedb.MessageStoreCrateDB
import at.rocworks.stores.mongodb.MessageArchiveMongoDB
import at.rocworks.stores.mongodb.MessageStoreMongoDB
import at.rocworks.stores.postgres.MessageArchivePostgres
import at.rocworks.stores.postgres.MessageStorePostgres
import at.rocworks.stores.sqlite.MessageArchiveSQLite
import at.rocworks.stores.sqlite.MessageStoreSQLite
import at.rocworks.utils.DurationParser
import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.ThreadingModel
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.Callable

class ArchiveGroup(
    val name: String,
    val topicFilter: List<String>,
    val retainedOnly: Boolean,
    private val lastValType: MessageStoreType,
    private val archiveType: MessageArchiveType,
    private val lastValRetentionMs: Long? = null,
    private val archiveRetentionMs: Long? = null,
    private val purgeIntervalMs: Long? = null,
    private val lastValRetentionStr: String? = null,
    private val archiveRetentionStr: String? = null,
    private val purgeIntervalStr: String? = null,
    private val databaseConfig: JsonObject
) : AbstractVerticle() {

    private val logger = Utils.getLogger(this::class.java, name)
    val filterTree: TopicTree<Boolean, Boolean> = TopicTree()

    var lastValStore: IMessageStore? = null
        private set
    var archiveStore: IMessageArchive? = null
        private set

    private val timers = mutableListOf<Long>()

    // Track child verticle deployments so we can properly clean them up
    private val childDeployments = mutableListOf<String>()
    private var isStopping = false

    init {
        topicFilter.forEach { filterTree.add(it, key=true, value=true) }
    }

    override fun start(startPromise: Promise<Void>) {
        logger.info("Starting ArchiveGroup [$name] - deploying stores and waiting for LastValueStore")

        // Deploy stores and wait for critical ones to complete before reporting success
        vertx.runOnContext {
            createMessageStoreWithCallback(lastValType, "${name}Lastval") { storeReady ->
                if (storeReady) {
                    logger.info("ArchiveGroup [$name] started successfully - LastValueStore is ready")

                    // Start retention scheduling for LastVal store now that it's ready
                    startLastValRetentionScheduling()

                    startPromise.complete()

                    // Start archive store in background (less critical for immediate functionality)
                    createMessageArchiveWithCallback(archiveType, "${name}Archive") { archiveReady ->
                        if (archiveReady) {
                            // Start retention scheduling for Archive store now that it's ready
                            startArchiveRetentionScheduling()
                        }
                    }
                } else {
                    logger.severe("ArchiveGroup [$name] failed to start - LastValueStore initialization failed")
                    startPromise.fail("Failed to initialize LastValueStore")
                }
            }
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Stopping ArchiveGroup [$name] and cleaning up child deployments")

        isStopping = true
        stopRetentionScheduling()

        // Clean up child verticle deployments
        if (childDeployments.isNotEmpty()) {
            logger.info("Undeploying ${childDeployments.size} child verticles for ArchiveGroup [$name]")

            val undeployFutures = childDeployments.map { deploymentId ->
                logger.fine("Undeploying child verticle: $deploymentId")
                vertx.undeploy(deploymentId).recover { error ->
                    logger.warning("Failed to undeploy child verticle $deploymentId: ${error.message}")
                    Future.succeededFuture<Void>() // Continue even if undeploy fails
                }
            }

            Future.all<Void>(undeployFutures).onComplete { _ ->
                logger.info("Child verticle cleanup completed for ArchiveGroup [$name]")
                childDeployments.clear()
                lastValStore = null
                archiveStore = null

                logger.info("ArchiveGroup [$name] stopped")
                stopPromise.complete()
            }
        } else {
            lastValStore = null
            archiveStore = null

            logger.info("ArchiveGroup [$name] stopped")
            stopPromise.complete()
        }
    }

    fun getLastValRetentionMs(): Long? = lastValRetentionMs
    fun getArchiveRetentionMs(): Long? = archiveRetentionMs
    fun getPurgeIntervalMs(): Long? = purgeIntervalMs

    fun getLastValRetention(): String? = lastValRetentionStr
    fun getArchiveRetention(): String? = archiveRetentionStr
    fun getPurgeInterval(): String? = purgeIntervalStr
    fun getLastValType(): MessageStoreType = lastValType
    fun getArchiveType(): MessageArchiveType = archiveType

    private fun createMessageStore(storeType: MessageStoreType, storeName: String): Future<Void> {
        val promise = Promise.promise<Void>()

        when (storeType) {
            MessageStoreType.NONE -> {
                lastValStore = MessageStoreNone
                promise.complete()
            }
            MessageStoreType.MEMORY -> {
                val store = MessageStoreMemory(storeName)
                lastValStore = store
                val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onComplete { result ->
                    if (result.succeeded()) promise.complete()
                    else promise.fail(result.cause())
                }
            }
            MessageStoreType.HAZELCAST -> {
                // Note: Hazelcast instance would need to be passed from Monster.kt
                promise.fail("Hazelcast store not implemented in verticle context")
            }
            MessageStoreType.POSTGRES -> {
                val postgres = databaseConfig.getJsonObject("Postgres")
                val store = MessageStorePostgres(
                    storeName,
                    postgres.getString("Url"),
                    postgres.getString("User"),
                    postgres.getString("Pass")
                )
                lastValStore = store
                val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onComplete { result ->
                    if (result.succeeded()) promise.complete()
                    else promise.fail(result.cause())
                }
            }
            MessageStoreType.CRATEDB -> {
                val cratedb = databaseConfig.getJsonObject("CrateDB")
                val store = MessageStoreCrateDB(
                    storeName,
                    cratedb.getString("Url"),
                    cratedb.getString("User"),
                    cratedb.getString("Pass")
                )
                lastValStore = store
                val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onComplete { result ->
                    if (result.succeeded()) promise.complete()
                    else promise.fail(result.cause())
                }
            }
            MessageStoreType.MONGODB -> {
                val mongodb = databaseConfig.getJsonObject("MongoDB")
                val store = MessageStoreMongoDB(
                    storeName,
                    mongodb.getString("Url"),
                    mongodb.getString("Database", "monstermq")
                )
                lastValStore = store
                val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onComplete { result ->
                    if (result.succeeded()) {
                        if (!isStopping) {
                            childDeployments.add(result.result())
                            logger.info("MongoDB MessageStore [$storeName] deployed successfully")
                        } else {
                            // ArchiveGroup is stopping, immediately undeploy
                            vertx.undeploy(result.result())
                            logger.info("MongoDB MessageStore [$storeName] deployed but immediately undeployed due to stop")
                            lastValStore = null
                        }
                        promise.complete()
                    } else {
                        logger.warning("Failed to deploy MongoDB MessageStore [$storeName]: ${result.cause()?.message}")
                        promise.fail(result.cause())
                    }
                }
            }
            MessageStoreType.SQLITE -> {
                val sqlite = databaseConfig.getJsonObject("SQLite")
                val archiveName = storeName.removeSuffix("Lastval")
                val dbPath = "${sqlite.getString("Path", ".")}/monstermq-${archiveName}-lastval.db"
                val store = MessageStoreSQLite(
                    storeName,
                    dbPath
                )
                lastValStore = store
                val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onComplete { result ->
                    if (result.succeeded()) promise.complete()
                    else promise.fail(result.cause())
                }
            }
        }

        return promise.future()
    }

    private fun createMessageArchive(archiveType: MessageArchiveType, archiveName: String): Future<Void> {
        val promise = Promise.promise<Void>()

        when (archiveType) {
            MessageArchiveType.NONE -> {
                archiveStore = MessageArchiveNone
                promise.complete()
            }
            MessageArchiveType.POSTGRES -> {
                val postgres = databaseConfig.getJsonObject("Postgres")
                val archive = MessageArchivePostgres(
                    archiveName,
                    postgres.getString("Url"),
                    postgres.getString("User"),
                    postgres.getString("Pass")
                )
                archiveStore = archive
                val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(archive, options).onComplete { result ->
                    if (result.succeeded()) promise.complete()
                    else promise.fail(result.cause())
                }
            }
            MessageArchiveType.CRATEDB -> {
                val cratedb = databaseConfig.getJsonObject("CrateDB")
                val archive = MessageArchiveCrateDB(
                    archiveName,
                    cratedb.getString("Url"),
                    cratedb.getString("User"),
                    cratedb.getString("Pass")
                )
                archiveStore = archive
                val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(archive, options).onComplete { result ->
                    if (result.succeeded()) promise.complete()
                    else promise.fail(result.cause())
                }
            }
            MessageArchiveType.MONGODB -> {
                val mongodb = databaseConfig.getJsonObject("MongoDB")
                val archive = MessageArchiveMongoDB(
                    archiveName,
                    mongodb.getString("Url"),
                    mongodb.getString("Database", "monstermq")
                )
                archiveStore = archive
                val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(archive, options).onComplete { result ->
                    if (result.succeeded()) {
                        if (!isStopping) {
                            childDeployments.add(result.result())
                            logger.info("MongoDB MessageArchive [$archiveName] deployed successfully")
                        } else {
                            // ArchiveGroup is stopping, immediately undeploy
                            vertx.undeploy(result.result())
                            logger.info("MongoDB MessageArchive [$archiveName] deployed but immediately undeployed due to stop")
                            archiveStore = null
                        }
                        promise.complete()
                    } else {
                        logger.warning("Failed to deploy MongoDB MessageArchive [$archiveName]: ${result.cause()?.message}")
                        promise.fail(result.cause())
                    }
                }
            }
            MessageArchiveType.KAFKA -> {
                val kafka = databaseConfig.getJsonObject("Kafka")
                val bootstrapServers = kafka?.getString("Servers") ?: "localhost:9092"
                val archive = MessageArchiveKafka(archiveName, bootstrapServers)
                archiveStore = archive
                val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(archive, options).onComplete { result ->
                    if (result.succeeded()) promise.complete()
                    else promise.fail(result.cause())
                }
            }
            MessageArchiveType.SQLITE -> {
                val sqlite = databaseConfig.getJsonObject("SQLite")
                val archiveGroupName = archiveName.removeSuffix("Archive")
                val dbPath = "${sqlite.getString("Path", ".")}/monstermq-${archiveGroupName}-archive.db"
                val archive = MessageArchiveSQLite(
                    archiveName,
                    dbPath
                )
                archiveStore = archive
                val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(archive, options).onComplete { result ->
                    if (result.succeeded()) promise.complete()
                    else promise.fail(result.cause())
                }
            }
        }

        return promise.future()
    }

    // Version with callback for critical store initialization during start
    private fun createMessageStoreWithCallback(storeType: MessageStoreType, storeName: String, callback: (Boolean) -> Unit) {
        if (isStopping) {
            logger.info("Skipping MessageStore [$storeName] creation - ArchiveGroup is stopping")
            callback(false)
            return
        }
        logger.info("Creating MessageStore [$storeName] of type [$storeType] with callback")

        when (storeType) {
            MessageStoreType.NONE -> {
                lastValStore = MessageStoreNone
                logger.info("MessageStore [$storeName] set to NONE")
                callback(true)
            }
            MessageStoreType.MEMORY -> {
                val store = MessageStoreMemory(storeName)
                lastValStore = store
                val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onComplete { result ->
                    if (result.succeeded()) {
                        if (!isStopping) {
                            childDeployments.add(result.result())
                            logger.info("MessageStore [$storeName] deployed successfully")
                            callback(true)
                        } else {
                            vertx.undeploy(result.result())
                            logger.info("MessageStore [$storeName] deployed but immediately undeployed due to stop")
                            callback(false)
                        }
                    } else {
                        logger.warning("Failed to deploy MessageStore [$storeName]: ${result.cause()?.message}")
                        lastValStore = null
                        callback(false)
                    }
                }
            }
            MessageStoreType.HAZELCAST -> {
                val store = MessageStoreHazelcastDisconnected(storeName)
                lastValStore = store
                val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onComplete { result ->
                    if (result.succeeded()) {
                        if (!isStopping) {
                            childDeployments.add(result.result())
                            logger.info("Hazelcast MessageStore [$storeName] deployed as disconnected")
                            callback(true)
                        } else {
                            vertx.undeploy(result.result())
                            logger.info("Hazelcast MessageStore [$storeName] deployed but immediately undeployed due to stop")
                            callback(false)
                        }
                    } else {
                        logger.warning("Failed to deploy disconnected Hazelcast MessageStore [$storeName]: ${result.cause()?.message}")
                        lastValStore = null
                        callback(false)
                    }
                }
            }
            MessageStoreType.POSTGRES, MessageStoreType.CRATEDB, MessageStoreType.MONGODB, MessageStoreType.SQLITE -> {
                createDatabaseMessageStore(storeType, storeName) { success ->
                    callback(success)
                }
            }
        }
    }

    private fun createDatabaseMessageStore(storeType: MessageStoreType, storeName: String, callback: (Boolean) -> Unit) {
        try {
            val store = when (storeType) {
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
                    val archiveGroupName = storeName.removeSuffix("Lastval")
                    val dbPath = "${sqlite.getString("Path", ".")}/monstermq-${archiveGroupName}-lastval.db"
                    MessageStoreSQLite(
                        storeName,
                        dbPath
                    )
                }
                else -> {
                    callback(false)
                    return
                }
            }
            lastValStore = store
            val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
            vertx.deployVerticle(store, options).onComplete { result ->
                if (result.succeeded()) {
                    if (!isStopping) {
                        childDeployments.add(result.result())
                        logger.info("${storeType.name} MessageStore [$storeName] deployed successfully")
                        callback(true)
                    } else {
                        vertx.undeploy(result.result())
                        logger.info("${storeType.name} MessageStore [$storeName] deployed but immediately undeployed due to stop")
                        lastValStore = null
                        callback(false)
                    }
                } else {
                    logger.warning("Failed to deploy ${storeType.name} MessageStore [$storeName]: ${result.cause()?.message}")
                    lastValStore = null
                    callback(false)
                }
            }
        } catch (e: Exception) {
            logger.warning("Error creating ${storeType.name} MessageStore [$storeName]: ${e.message}")
            lastValStore = null
            callback(false)
        }
    }

    // Async versions that don't block the start process
    private fun createMessageStoreAsync(storeType: MessageStoreType, storeName: String) {
        if (isStopping) {
            logger.info("Skipping MessageStore [$storeName] creation - ArchiveGroup is stopping")
            return
        }
        logger.info("Creating MessageStore [$storeName] of type [$storeType] in background")

        when (storeType) {
            MessageStoreType.NONE -> {
                lastValStore = MessageStoreNone
                logger.info("MessageStore [$storeName] set to NONE")
            }
            MessageStoreType.MEMORY -> {
                val store = MessageStoreMemory(storeName)
                lastValStore = store
                val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onComplete { result ->
                    if (result.succeeded()) {
                        if (!isStopping) {
                            childDeployments.add(result.result())
                            logger.info("MessageStore [$storeName] deployed successfully")
                        } else {
                            // ArchiveGroup is stopping, immediately undeploy
                            vertx.undeploy(result.result())
                            logger.info("MessageStore [$storeName] deployed but immediately undeployed due to stop")
                        }
                    } else {
                        logger.warning("Failed to deploy MessageStore [$storeName]: ${result.cause()?.message}")
                        // Store remains null - operations will gracefully handle this
                    }
                }
            }
            MessageStoreType.HAZELCAST -> {
                // Create disconnected Hazelcast store when clustering is not enabled
                val store = MessageStoreHazelcastDisconnected(storeName)
                lastValStore = store
                val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onComplete { result ->
                    if (result.succeeded()) {
                        if (!isStopping) {
                            childDeployments.add(result.result())
                            logger.info("Hazelcast MessageStore [$storeName] deployed as disconnected")
                        } else {
                            // ArchiveGroup is stopping, immediately undeploy
                            vertx.undeploy(result.result())
                            logger.info("Hazelcast MessageStore [$storeName] deployed but immediately undeployed due to stop")
                        }
                    } else {
                        logger.warning("Failed to deploy disconnected Hazelcast MessageStore [$storeName]: ${result.cause()?.message}")
                        lastValStore = null
                    }
                }
            }
            MessageStoreType.POSTGRES -> {
                try {
                    val postgres = databaseConfig.getJsonObject("Postgres")
                    val store = MessageStorePostgres(
                        storeName,
                        postgres.getString("Url"),
                        postgres.getString("User"),
                        postgres.getString("Pass")
                    )
                    lastValStore = store
                    val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                    vertx.deployVerticle(store, options).onComplete { result ->
                        if (result.succeeded()) {
                            if (!isStopping) {
                                childDeployments.add(result.result())
                                logger.info("PostgreSQL MessageStore [$storeName] deployed successfully")
                            } else {
                                // ArchiveGroup is stopping, immediately undeploy
                                vertx.undeploy(result.result())
                                logger.info("PostgreSQL MessageStore [$storeName] deployed but immediately undeployed due to stop")
                                lastValStore = null
                            }
                        } else {
                            logger.warning("Failed to deploy PostgreSQL MessageStore [$storeName]: ${result.cause()?.message}")
                            lastValStore = null
                        }
                    }
                } catch (e: Exception) {
                    logger.warning("Error creating PostgreSQL MessageStore [$storeName]: ${e.message}")
                }
            }
            MessageStoreType.CRATEDB -> {
                try {
                    val cratedb = databaseConfig.getJsonObject("CrateDB")
                    val store = MessageStoreCrateDB(
                        storeName,
                        cratedb.getString("Url"),
                        cratedb.getString("User"),
                        cratedb.getString("Pass")
                    )
                    lastValStore = store
                    val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                    vertx.deployVerticle(store, options).onComplete { result ->
                        if (result.succeeded()) {
                            if (!isStopping) {
                                childDeployments.add(result.result())
                                logger.info("CrateDB MessageStore [$storeName] deployed successfully")
                            } else {
                                vertx.undeploy(result.result())
                                logger.info("CrateDB MessageStore [$storeName] deployed but immediately undeployed due to stop")
                                lastValStore = null
                            }
                        } else {
                            logger.warning("Failed to deploy CrateDB MessageStore [$storeName]: ${result.cause()?.message}")
                            lastValStore = null
                        }
                    }
                } catch (e: Exception) {
                    logger.warning("Error creating CrateDB MessageStore [$storeName]: ${e.message}")
                }
            }
            MessageStoreType.MONGODB -> {
                try {
                    val mongodb = databaseConfig.getJsonObject("MongoDB")
                    val store = MessageStoreMongoDB(
                        storeName,
                        mongodb.getString("Url"),
                        mongodb.getString("Database", "monstermq")
                    )
                    lastValStore = store
                    val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                    vertx.deployVerticle(store, options).onComplete { result ->
                        if (result.succeeded()) {
                            if (!isStopping) {
                                childDeployments.add(result.result())
                                logger.info("MongoDB MessageStore [$storeName] deployed successfully")
                            } else {
                                vertx.undeploy(result.result())
                                logger.info("MongoDB MessageStore [$storeName] deployed but immediately undeployed due to stop")
                                lastValStore = null
                            }
                        } else {
                            logger.warning("Failed to deploy MongoDB MessageStore [$storeName]: ${result.cause()?.message}")
                            lastValStore = null
                        }
                    }
                } catch (e: Exception) {
                    logger.warning("Error creating MongoDB MessageStore [$storeName]: ${e.message}")
                }
            }
            MessageStoreType.SQLITE -> {
                try {
                    val sqlite = databaseConfig.getJsonObject("SQLite")
                    val archiveGroupName = storeName.removeSuffix("Lastval")
                    val dbPath = "${sqlite.getString("Path", ".")}/monstermq-${archiveGroupName}-lastval.db"
                    val store = MessageStoreSQLite(
                        storeName,
                        dbPath
                    )
                    lastValStore = store
                    val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                    vertx.deployVerticle(store, options).onComplete { result ->
                        if (result.succeeded()) {
                            if (!isStopping) {
                                childDeployments.add(result.result())
                                logger.info("SQLite MessageStore [$storeName] deployed successfully")
                            } else {
                                vertx.undeploy(result.result())
                                logger.info("SQLite MessageStore [$storeName] deployed but immediately undeployed due to stop")
                                lastValStore = null
                            }
                        } else {
                            logger.warning("Failed to deploy SQLite MessageStore [$storeName]: ${result.cause()?.message}")
                            lastValStore = null
                        }
                    }
                } catch (e: Exception) {
                    logger.warning("Error creating SQLite MessageStore [$storeName]: ${e.message}")
                }
            }
        }
    }

    private fun createMessageArchiveWithCallback(archiveType: MessageArchiveType, archiveName: String, callback: (Boolean) -> Unit) {
        if (isStopping) {
            logger.info("Skipping MessageArchive [$archiveName] creation - ArchiveGroup is stopping")
            callback(false)
            return
        }
        logger.info("Creating MessageArchive [$archiveName] of type [$archiveType] with callback")

        when (archiveType) {
            MessageArchiveType.NONE -> {
                archiveStore = MessageArchiveNone
                logger.info("MessageArchive [$archiveName] set to NONE")
                callback(true)
            }
            MessageArchiveType.POSTGRES, MessageArchiveType.CRATEDB, MessageArchiveType.MONGODB, MessageArchiveType.KAFKA, MessageArchiveType.SQLITE -> {
                createDatabaseMessageArchive(archiveType, archiveName) { success ->
                    callback(success)
                }
            }
        }
    }

    private fun createDatabaseMessageArchive(archiveType: MessageArchiveType, archiveName: String, callback: (Boolean) -> Unit) {
        try {
            val archive = when (archiveType) {
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
                    val archiveGroupName = archiveName.removeSuffix("Archive")
                    val dbPath = "${sqlite.getString("Path", ".")}/monstermq-${archiveGroupName}-archive.db"
                    MessageArchiveSQLite(
                        archiveName,
                        dbPath
                    )
                }
                else -> {
                    callback(false)
                    return
                }
            }
            archiveStore = archive
            val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
            vertx.deployVerticle(archive, options).onComplete { result ->
                if (result.succeeded()) {
                    if (!isStopping) {
                        childDeployments.add(result.result())
                        logger.info("${archiveType.name} MessageArchive [$archiveName] deployed successfully")
                        callback(true)
                    } else {
                        vertx.undeploy(result.result())
                        logger.info("${archiveType.name} MessageArchive [$archiveName] deployed but immediately undeployed due to stop")
                        archiveStore = null
                        callback(false)
                    }
                } else {
                    logger.warning("Failed to deploy ${archiveType.name} MessageArchive [$archiveName]: ${result.cause()?.message}")
                    archiveStore = null
                    callback(false)
                }
            }
        } catch (e: Exception) {
            logger.warning("Error creating ${archiveType.name} MessageArchive [$archiveName]: ${e.message}")
            archiveStore = null
            callback(false)
        }
    }

    private fun createMessageArchiveAsync(archiveType: MessageArchiveType, archiveName: String) {
        if (isStopping) {
            logger.info("Skipping MessageArchive [$archiveName] creation - ArchiveGroup is stopping")
            return
        }
        logger.info("Creating MessageArchive [$archiveName] of type [$archiveType] in background")

        when (archiveType) {
            MessageArchiveType.NONE -> {
                archiveStore = MessageArchiveNone
                logger.info("MessageArchive [$archiveName] set to NONE")
            }
            MessageArchiveType.POSTGRES -> {
                try {
                    val postgres = databaseConfig.getJsonObject("Postgres")
                    val archive = MessageArchivePostgres(
                        archiveName,
                        postgres.getString("Url"),
                        postgres.getString("User"),
                        postgres.getString("Pass")
                    )
                    archiveStore = archive
                    val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                    vertx.deployVerticle(archive, options).onComplete { result ->
                        if (result.succeeded()) {
                            if (!isStopping) {
                                childDeployments.add(result.result())
                                logger.info("PostgreSQL MessageArchive [$archiveName] deployed successfully")
                            } else {
                                vertx.undeploy(result.result())
                                logger.info("PostgreSQL MessageArchive [$archiveName] deployed but immediately undeployed due to stop")
                                archiveStore = null
                            }
                        } else {
                            logger.warning("Failed to deploy PostgreSQL MessageArchive [$archiveName]: ${result.cause()?.message}")
                            archiveStore = null
                        }
                    }
                } catch (e: Exception) {
                    logger.warning("Error creating PostgreSQL MessageArchive [$archiveName]: ${e.message}")
                }
            }
            MessageArchiveType.CRATEDB -> {
                try {
                    val cratedb = databaseConfig.getJsonObject("CrateDB")
                    val archive = MessageArchiveCrateDB(
                        archiveName,
                        cratedb.getString("Url"),
                        cratedb.getString("User"),
                        cratedb.getString("Pass")
                    )
                    archiveStore = archive
                    val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                    vertx.deployVerticle(archive, options).onComplete { result ->
                        if (result.succeeded()) {
                            if (!isStopping) {
                                childDeployments.add(result.result())
                                logger.info("CrateDB MessageArchive [$archiveName] deployed successfully")
                            } else {
                                vertx.undeploy(result.result())
                                logger.info("CrateDB MessageArchive [$archiveName] deployed but immediately undeployed due to stop")
                                archiveStore = null
                            }
                        } else {
                            logger.warning("Failed to deploy CrateDB MessageArchive [$archiveName]: ${result.cause()?.message}")
                            archiveStore = null
                        }
                    }
                } catch (e: Exception) {
                    logger.warning("Error creating CrateDB MessageArchive [$archiveName]: ${e.message}")
                }
            }
            MessageArchiveType.MONGODB -> {
                try {
                    val mongodb = databaseConfig.getJsonObject("MongoDB")
                    val archive = MessageArchiveMongoDB(
                        archiveName,
                        mongodb.getString("Url"),
                        mongodb.getString("Database", "monstermq")
                    )
                    archiveStore = archive
                    val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                    vertx.deployVerticle(archive, options).onComplete { result ->
                        if (result.succeeded()) {
                            if (!isStopping) {
                                childDeployments.add(result.result())
                                logger.info("MongoDB MessageArchive [$archiveName] deployed successfully")
                            } else {
                                vertx.undeploy(result.result())
                                logger.info("MongoDB MessageArchive [$archiveName] deployed but immediately undeployed due to stop")
                                archiveStore = null
                            }
                        } else {
                            logger.warning("Failed to deploy MongoDB MessageArchive [$archiveName]: ${result.cause()?.message}")
                            archiveStore = null
                        }
                    }
                } catch (e: Exception) {
                    logger.warning("Error creating MongoDB MessageArchive [$archiveName]: ${e.message}")
                }
            }
            MessageArchiveType.KAFKA -> {
                try {
                    val kafka = databaseConfig.getJsonObject("Kafka")
                    val bootstrapServers = kafka?.getString("Servers") ?: "localhost:9092"
                    val archive = MessageArchiveKafka(archiveName, bootstrapServers)
                    archiveStore = archive
                    val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                    vertx.deployVerticle(archive, options).onComplete { result ->
                        if (result.succeeded()) {
                            if (!isStopping) {
                                childDeployments.add(result.result())
                                logger.info("Kafka MessageArchive [$archiveName] deployed successfully")
                            } else {
                                vertx.undeploy(result.result())
                                logger.info("Kafka MessageArchive [$archiveName] deployed but immediately undeployed due to stop")
                                archiveStore = null
                            }
                        } else {
                            logger.warning("Failed to deploy Kafka MessageArchive [$archiveName]: ${result.cause()?.message}")
                            archiveStore = null
                        }
                    }
                } catch (e: Exception) {
                    logger.warning("Error creating Kafka MessageArchive [$archiveName]: ${e.message}")
                }
            }
            MessageArchiveType.SQLITE -> {
                try {
                    val sqlite = databaseConfig.getJsonObject("SQLite")
                    val archiveGroupName = archiveName.removeSuffix("Archive")
                    val dbPath = "${sqlite.getString("Path", ".")}/monstermq-${archiveGroupName}-archive.db"
                    val archive = MessageArchiveSQLite(
                        archiveName,
                        dbPath
                    )
                    archiveStore = archive
                    val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                    vertx.deployVerticle(archive, options).onComplete { result ->
                        if (result.succeeded()) {
                            if (!isStopping) {
                                childDeployments.add(result.result())
                                logger.info("SQLite MessageArchive [$archiveName] deployed successfully")
                            } else {
                                vertx.undeploy(result.result())
                                logger.info("SQLite MessageArchive [$archiveName] deployed but immediately undeployed due to stop")
                                archiveStore = null
                            }
                        } else {
                            logger.warning("Failed to deploy SQLite MessageArchive [$archiveName]: ${result.cause()?.message}")
                            archiveStore = null
                        }
                    }
                } catch (e: Exception) {
                    logger.warning("Error creating SQLite MessageArchive [$archiveName]: ${e.message}")
                }
            }
        }
    }

    private fun startLastValRetentionScheduling() {
        // Schedule store purge if configured
        if (lastValStore != null && lastValRetentionMs != null && purgeIntervalMs != null) {
            val timerId = vertx.setPeriodic(purgeIntervalMs) { _ ->
                purgeLastValStore()
            }
            timers.add(timerId)

            logger.info("Scheduled LastVal purge for [$name] every ${purgeIntervalMs}ms with retention ${lastValRetentionMs}ms")
        } else {
            logger.fine { "LastVal retention scheduling not configured for [$name] - store: ${lastValStore != null}, retention: $lastValRetentionMs, interval: $purgeIntervalMs" }
        }
    }

    private fun startArchiveRetentionScheduling() {
        // Schedule archive purge if configured
        if (archiveStore != null && archiveRetentionMs != null && purgeIntervalMs != null) {
            val timerId = vertx.setPeriodic(purgeIntervalMs) { _ ->
                purgeArchiveStore()
            }
            timers.add(timerId)

            logger.info("Scheduled Archive purge for [$name] every ${purgeIntervalMs}ms with retention ${archiveRetentionMs}ms")
        } else {
            logger.fine { "Archive retention scheduling not configured for [$name] - store: ${archiveStore != null}, retention: $archiveRetentionMs, interval: $purgeIntervalMs" }
        }
    }

    private fun stopRetentionScheduling() {
        logger.fine { "Stopping retention scheduling for [$name]" }

        // Cancel all timers
        timers.forEach { timerId ->
            vertx.cancelTimer(timerId)
        }
        timers.clear()
    }

    private fun purgeLastValStore() {
        val retentionMs = lastValRetentionMs ?: return
        val messageStore = lastValStore ?: return

        // Use cluster-wide distributed lock to ensure only one node performs purging
        val lockName = "purge-lock-$name-LastVal"
        val sharedData = vertx.sharedData()

        sharedData.getLockWithTimeout(lockName, 30000).onComplete { lockResult ->
            if (lockResult.succeeded()) {
                val lock = lockResult.result()
                logger.fine { "Acquired purge lock for LastVal store [$name] - starting purge" }

                vertx.executeBlocking(Callable<PurgeResult> {
                    val olderThan = Instant.now().minusMillis(retentionMs)
                    logger.fine { "Starting LastVal purge for [$name] - removing messages older than $olderThan" }

                    messageStore.purgeOldMessages(olderThan)
                }).onComplete { asyncResult ->
                    // Always release the lock
                    lock.release()

                    if (asyncResult.succeeded()) {
                        val result = asyncResult.result()
                        if (result.deletedCount > 0 || result.elapsedTimeMs > 30000) {
                            logger.info("LastVal purge for [$name] completed: deleted ${result.deletedCount} messages in ${result.elapsedTimeMs}ms")
                        } else {
                            logger.fine { "LastVal purge for [$name] completed: deleted ${result.deletedCount} messages in ${result.elapsedTimeMs}ms" }
                        }

                        // Warn if purge takes too long
                        if (result.elapsedTimeMs > 30000) {
                            logger.warning("LastVal purge for [$name] took ${result.elapsedTimeMs}ms - consider adjusting purge interval or retention period")
                        }
                    } else {
                        logger.severe("Error during LastVal purge for [$name]: ${asyncResult.cause()?.message}")
                    }
                }
            } else {
                // Lock acquisition failed - another node is likely purging or lock timed out
                logger.fine { "Could not acquire purge lock for LastVal store [$name] - skipping purge (likely another cluster node is purging)" }
            }
        }
    }

    private fun purgeArchiveStore() {
        val retentionMs = archiveRetentionMs ?: return
        val messageArchive = archiveStore ?: return

        // Use cluster-wide distributed lock to ensure only one node performs purging
        val lockName = "purge-lock-$name-Archive"
        val sharedData = vertx.sharedData()

        sharedData.getLockWithTimeout(lockName, 30000).onComplete { lockResult ->
            if (lockResult.succeeded()) {
                val lock = lockResult.result()
                logger.fine { "Acquired purge lock for Archive store [$name] - starting purge" }

                vertx.executeBlocking(Callable<PurgeResult> {
                    val olderThan = Instant.now().minusMillis(retentionMs)
                    logger.info { "Starting Archive purge for [$name] - removing messages older than $olderThan" }

                    messageArchive.purgeOldMessages(olderThan)
                }).onComplete { asyncResult ->
                    // Always release the lock
                    lock.release()

                    if (asyncResult.succeeded()) {
                        val result = asyncResult.result()
                        if (result.deletedCount > 0 || result.elapsedTimeMs > 30000) {
                            logger.info("Archive purge for [$name] completed: deleted ${result.deletedCount} messages in ${result.elapsedTimeMs}ms")
                        } else {
                            logger.fine { "Archive purge for [$name] completed: deleted ${result.deletedCount} messages in ${result.elapsedTimeMs}ms" }
                        }

                        // Warn if purge takes too long
                        if (result.elapsedTimeMs > 30000) {
                            logger.warning("Archive purge for [$name] took ${result.elapsedTimeMs}ms - consider adjusting purge interval or retention period")
                        }
                    } else {
                        logger.severe("Error during Archive purge for [$name]: ${asyncResult.cause()?.message}")
                    }
                }
            } else {
                // Lock acquisition failed - another node is likely purging or lock timed out
                logger.fine { "Could not acquire purge lock for Archive store [$name] - skipping purge (likely another cluster node is purging)" }
            }
        }
    }

    companion object {
        fun fromConfig(config: JsonObject, databaseConfig: JsonObject, isClustered: Boolean = false): ArchiveGroup {
            val name = config.getString("Name", "ArchiveGroup")
            val topicFilter = config.getJsonArray("TopicFilter").map { it as String }
            val retainedOnly = config.getBoolean("RetainedOnly", false)

            val lastValType = MessageStoreType.valueOf(config.getString("LastValType", "NONE"))
            val archiveType = MessageArchiveType.valueOf(config.getString("ArchiveType", "NONE"))

            // Validate database configuration requirements
            validateStoreConfiguration(name, lastValType, "LastValueStore", databaseConfig, isClustered)
            validateArchiveConfiguration(name, archiveType, "Archive", databaseConfig)

            // Parse retention configuration
            val lastValRetentionStr = config.getString("LastValRetention")
            val archiveRetentionStr = config.getString("ArchiveRetention")
            val purgeIntervalStr = config.getString("PurgeInterval")

            val lastValRetentionMs = DurationParser.parse(lastValRetentionStr)
            val archiveRetentionMs = DurationParser.parse(archiveRetentionStr)
            val purgeIntervalMs = DurationParser.parse(purgeIntervalStr)

            return ArchiveGroup(
                name, topicFilter, retainedOnly,
                lastValType, archiveType,
                lastValRetentionMs, archiveRetentionMs, purgeIntervalMs,
                lastValRetentionStr, archiveRetentionStr, purgeIntervalStr,
                databaseConfig
            )
        }

        private fun validateStoreConfiguration(archiveName: String, storeType: MessageStoreType, storeTypeName: String, databaseConfig: JsonObject, isClustered: Boolean) {
            when (storeType) {
                MessageStoreType.POSTGRES -> {
                    val postgres = databaseConfig.getJsonObject("Postgres")
                    if (postgres == null || postgres.getString("Url").isNullOrEmpty()) {
                        throw IllegalArgumentException("Archive group '$archiveName' cannot use POSTGRES $storeTypeName: PostgreSQL configuration (Url) not found or empty")
                    }
                }
                MessageStoreType.CRATEDB -> {
                    val cratedb = databaseConfig.getJsonObject("CrateDB")
                    if (cratedb == null || cratedb.getString("Url").isNullOrEmpty()) {
                        throw IllegalArgumentException("Archive group '$archiveName' cannot use CRATEDB $storeTypeName: CrateDB configuration (Url) not found or empty")
                    }
                }
                MessageStoreType.MONGODB -> {
                    val mongodb = databaseConfig.getJsonObject("MongoDB")
                    if (mongodb == null || mongodb.getString("Url").isNullOrEmpty()) {
                        throw IllegalArgumentException("Archive group '$archiveName' cannot use MONGODB $storeTypeName: MongoDB configuration (Url) not found or empty")
                    }
                }
                MessageStoreType.SQLITE -> {
                    val sqlite = databaseConfig.getJsonObject("SQLite")
                    if (sqlite == null || sqlite.getString("Path").isNullOrEmpty()) {
                        throw IllegalArgumentException("Archive group '$archiveName' cannot use SQLITE $storeTypeName: SQLite configuration (Path) not found or empty")
                    }
                }
                MessageStoreType.HAZELCAST -> {
                    if (!isClustered) {
                        // Warning will be logged when the disconnected store is created
                        // Cannot log here as this is a companion object function
                    }
                }
                MessageStoreType.MEMORY, MessageStoreType.NONE -> {
                    // No validation needed for memory/none stores
                }
            }
        }

        private fun validateArchiveConfiguration(archiveName: String, archiveType: MessageArchiveType, storeTypeName: String, databaseConfig: JsonObject) {
            when (archiveType) {
                MessageArchiveType.POSTGRES -> {
                    val postgres = databaseConfig.getJsonObject("Postgres")
                    if (postgres == null || postgres.getString("Url").isNullOrEmpty()) {
                        throw IllegalArgumentException("Archive group '$archiveName' cannot use POSTGRES $storeTypeName: PostgreSQL configuration (Url) not found or empty")
                    }
                }
                MessageArchiveType.CRATEDB -> {
                    val cratedb = databaseConfig.getJsonObject("CrateDB")
                    if (cratedb == null || cratedb.getString("Url").isNullOrEmpty()) {
                        throw IllegalArgumentException("Archive group '$archiveName' cannot use CRATEDB $storeTypeName: CrateDB configuration (Url) not found or empty")
                    }
                }
                MessageArchiveType.MONGODB -> {
                    val mongodb = databaseConfig.getJsonObject("MongoDB")
                    if (mongodb == null || mongodb.getString("Url").isNullOrEmpty()) {
                        throw IllegalArgumentException("Archive group '$archiveName' cannot use MONGODB $storeTypeName: MongoDB configuration (Url) not found or empty")
                    }
                }
                MessageArchiveType.SQLITE -> {
                    val sqlite = databaseConfig.getJsonObject("SQLite")
                    if (sqlite == null || sqlite.getString("Path").isNullOrEmpty()) {
                        throw IllegalArgumentException("Archive group '$archiveName' cannot use SQLITE $storeTypeName: SQLite configuration (Path) not found or empty")
                    }
                }
                MessageArchiveType.KAFKA -> {
                    val kafka = databaseConfig.getJsonObject("Kafka")
                    if (kafka == null || kafka.getString("Servers").isNullOrEmpty()) {
                        throw IllegalArgumentException("Archive group '$archiveName' cannot use KAFKA $storeTypeName: Kafka configuration (Servers) not found or empty")
                    }
                }
                MessageArchiveType.NONE -> {
                    // No validation needed for none archive
                }
            }
        }
    }
}