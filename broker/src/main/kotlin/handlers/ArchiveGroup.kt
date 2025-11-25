package at.rocworks.handlers

import at.rocworks.Const
import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.data.PurgeResult
import at.rocworks.data.TopicTree
import at.rocworks.stores.IMessageArchive
import at.rocworks.stores.IMessageStore
import at.rocworks.stores.MessageArchiveKafka
import at.rocworks.stores.MessageArchiveNone
import at.rocworks.stores.MessageArchiveType
import at.rocworks.stores.MessageStoreMemory
import at.rocworks.stores.MessageStoreNone
import at.rocworks.stores.MessageStoreType
import at.rocworks.stores.MessageStoreHazelcast
import at.rocworks.stores.PayloadFormat
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
import kotlinx.coroutines.runBlocking
import java.time.Instant
import java.util.concurrent.Callable

class ArchiveGroup(
    val name: String,
    val topicFilter: List<String>,
    val retainedOnly: Boolean,
    private val lastValType: MessageStoreType,
    private val archiveType: MessageArchiveType,
    val payloadFormat: PayloadFormat = PayloadFormat.DEFAULT,
    private val lastValRetentionMs: Long? = null,
    private val archiveRetentionMs: Long? = null,
    private val purgeIntervalMs: Long? = null,
    private val lastValRetentionStr: String? = null,
    private val archiveRetentionStr: String? = null,
    private val purgeIntervalStr: String? = null,
    private val maxMemoryEntries: Long? = null,
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

    private fun waitForTableReady(
        storeOrArchive: Any,
        name: String,
        maxWaitSeconds: Int = 300,
        pollIntervalMs: Long = 1000
    ): Boolean {
        val startTime = System.currentTimeMillis()
        val deadline = startTime + (maxWaitSeconds * 1000)
        var lastLogTime = startTime

        while (System.currentTimeMillis() < deadline) {
            // Check if archive group is being stopped
            if (isStopping) {
                logger.info("Archive group is being stopped, stopping wait for table [$name]")
                return false
            }

            try {
                // Check if table exists using runBlocking to bridge suspend and blocking contexts
                val exists = runBlocking {
                    when (storeOrArchive) {
                        is IMessageStore -> storeOrArchive.tableExists()
                        is IMessageArchive -> storeOrArchive.tableExists()
                        else -> false
                    }
                }

                if (exists) {
                    logger.info("Table for [$name] is ready")
                    return true
                }
            } catch (e: Exception) {
                logger.fine { "Error checking if table [$name] exists: ${e.message}" }
            }

            // Log progress every 30 seconds
            val now = System.currentTimeMillis()
            if (now - lastLogTime > 30_000) {
                val elapsed = (now - startTime) / 1000
                logger.info("Still waiting for table [$name] to exist... (${elapsed}s elapsed)")
                lastLogTime = now
            }

            // Sleep for poll interval
            Thread.sleep(pollIntervalMs)
        }

        logger.warning("Timeout waiting for table [$name] to exist after ${maxWaitSeconds}s")
        return false
    }

    override fun start(startPromise: Promise<Void>) {
        logger.info("Starting ArchiveGroup [$name] - deploying stores and waiting for LastValueStore")

        // Deploy stores and wait for critical ones to complete before reporting success
        vertx.runOnContext {
            createMessageStore(lastValType, "${name}Lastval") { storeReady ->
                if (storeReady) {
                    logger.info("ArchiveGroup [$name] started successfully - LastValueStore is ready")

                    // LastVal store uses automatic LRU eviction, no scheduling needed

                    startPromise.complete()

                    // Start archive store in background (less critical for immediate functionality)
                    createMessageArchive(archiveType, "${name}Archive") { archiveReady ->
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
                logger.fine { "Undeploying child verticle: $deploymentId" }
                vertx.undeploy(deploymentId).recover { error ->
                    logger.fine("Child verticle $deploymentId already undeployed or not found: ${error.message}")
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

    private fun createMessageStore(storeType: MessageStoreType, storeName: String, callback: (Boolean) -> Unit) {
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
                val store = MessageStoreMemory(storeName, maxMemoryEntries)
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
                if (!Monster.isClustered()) {
                    logger.warning("Hazelcast store type requested for [$storeName] but clustering is not enabled. Falling back to MEMORY store. Use -cluster flag to enable Hazelcast.")
                    val store = MessageStoreMemory(storeName, maxMemoryEntries)
                    lastValStore = store
                    val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                    vertx.deployVerticle(store, options).onComplete { result ->
                        if (result.succeeded()) {
                            if (!isStopping) {
                                childDeployments.add(result.result())
                                logger.info("Memory MessageStore [$storeName] deployed as fallback for Hazelcast")
                                callback(true)
                            } else {
                                vertx.undeploy(result.result())
                                logger.info("Memory MessageStore [$storeName] deployed but immediately undeployed due to stop")
                                callback(false)
                            }
                        } else {
                            logger.warning("Failed to deploy Memory MessageStore [$storeName]: ${result.cause()?.message}")
                            lastValStore = null
                            callback(false)
                        }
                    }
                } else {
                    try {
                        val clusterManager = Monster.getClusterManager()
                        if (clusterManager != null) {
                            val store = MessageStoreHazelcast(storeName, clusterManager.hazelcastInstance)
                            lastValStore = store
                            val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                            vertx.deployVerticle(store, options).onComplete { result ->
                                if (result.succeeded()) {
                                    if (!isStopping) {
                                        childDeployments.add(result.result())
                                        logger.info("Hazelcast MessageStore [$storeName] deployed successfully")
                                        callback(true)
                                    } else {
                                        vertx.undeploy(result.result())
                                        logger.info("Hazelcast MessageStore [$storeName] deployed but immediately undeployed due to stop")
                                        callback(false)
                                    }
                                } else {
                                    logger.warning("Failed to deploy Hazelcast MessageStore [$storeName]: ${result.cause()?.message}")
                                    lastValStore = null
                                    callback(false)
                                }
                            }
                        } else {
                            logger.warning("Hazelcast store type requested for [$storeName] but cluster manager is not available. Falling back to MEMORY store.")
                            val store = MessageStoreMemory(storeName, maxMemoryEntries)
                            lastValStore = store
                            val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                            vertx.deployVerticle(store, options).onComplete { result ->
                                if (result.succeeded()) {
                                    if (!isStopping) {
                                        childDeployments.add(result.result())
                                        logger.info("Memory MessageStore [$storeName] deployed as fallback for Hazelcast")
                                        callback(true)
                                    } else {
                                        vertx.undeploy(result.result())
                                        logger.info("Memory MessageStore [$storeName] deployed but immediately undeployed due to stop")
                                        callback(false)
                                    }
                                } else {
                                    logger.warning("Failed to deploy Memory MessageStore [$storeName]: ${result.cause()?.message}")
                                    lastValStore = null
                                    callback(false)
                                }
                            }
                        }
                    } catch (e: Exception) {
                        logger.warning("Error creating Hazelcast MessageStore [$storeName]: ${e.message}. Falling back to MEMORY store.")
                        val store = MessageStoreMemory(storeName)
                        lastValStore = store
                        val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                        vertx.deployVerticle(store, options).onComplete { result ->
                            if (result.succeeded()) {
                                if (!isStopping) {
                                    childDeployments.add(result.result())
                                    logger.info("Memory MessageStore [$storeName] deployed as fallback for Hazelcast")
                                    callback(true)
                                } else {
                                    vertx.undeploy(result.result())
                                    logger.info("Memory MessageStore [$storeName] deployed but immediately undeployed due to stop")
                                    callback(false)
                                }
                            } else {
                                logger.warning("Failed to deploy Memory MessageStore [$storeName]: ${result.cause()?.message}")
                                lastValStore = null
                                callback(false)
                            }
                        }
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
                        postgres.getString("Pass"),
                        null,
                        payloadFormat
                    )
                }
                MessageStoreType.CRATEDB -> {
                    val cratedb = databaseConfig.getJsonObject("CrateDB")
                    MessageStoreCrateDB(
                        storeName,
                        cratedb.getString("Url"),
                        cratedb.getString("User"),
                        cratedb.getString("Pass"),
                        payloadFormat
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
                    val dbPath = "${sqlite.getString("Path", Const.SQLITE_DEFAULT_PATH)}/monstermq-${archiveGroupName}-lastval.db"
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

                        // Leader creates table, non-leader waits for it
                        val leaderNodeId = HealthHandler.getLeaderNodeId(vertx)
                        val currentNodeId = Monster.getClusterNodeId(vertx)
                        val isLeader = leaderNodeId == currentNodeId

                        if (isLeader) {
                            logger.info("Leader node, initializing table for store [$storeName]")
                            vertx.executeBlocking(Callable {
                                runBlocking {
                                    store.createTable()
                                }
                            }).onComplete { createResult ->
                                if (createResult.succeeded() && createResult.result()) {
                                    logger.info("Table initialization completed for store [$storeName]")
                                    callback(true)
                                } else {
                                    logger.warning("Failed to initialize table for store [$storeName]")
                                    callback(false)
                                }
                            }
                        } else {
                            logger.info("Non-leader node, waiting for store table [$storeName] to be initialized by leader")
                            vertx.executeBlocking(Callable {
                                waitForTableReady(store, storeName, maxWaitSeconds = 300, pollIntervalMs = 1000)
                            }).onComplete { waitResult ->
                                if (waitResult.succeeded() && waitResult.result()) {
                                    logger.info("Table is ready for [$storeName]")
                                    callback(true)
                                } else {
                                    logger.warning("Timeout or error waiting for table [$storeName]")
                                    callback(false)
                                }
                            }
                        }
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

    private fun createMessageArchive(archiveType: MessageArchiveType, archiveName: String, callback: (Boolean) -> Unit) {
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
                        postgres.getString("Pass"),
                        payloadFormat
                    )
                }
                MessageArchiveType.CRATEDB -> {
                    val cratedb = databaseConfig.getJsonObject("CrateDB")
                    MessageArchiveCrateDB(
                        archiveName,
                        cratedb.getString("Url"),
                        cratedb.getString("User"),
                        cratedb.getString("Pass"),
                        payloadFormat
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
                    MessageArchiveKafka(archiveName, bootstrapServers, kafkaConfig, payloadFormat)
                }
                MessageArchiveType.SQLITE -> {
                    val sqlite = databaseConfig.getJsonObject("SQLite")
                    val archiveGroupName = archiveName.removeSuffix("Archive")
                    val dbPath = "${sqlite.getString("Path", Const.SQLITE_DEFAULT_PATH)}/monstermq-${archiveGroupName}-archive.db"
                    MessageArchiveSQLite(
                        archiveName,
                        dbPath,
                        payloadFormat
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

                        // Leader creates table, non-leader waits for it
                        val leaderNodeId = HealthHandler.getLeaderNodeId(vertx)
                        val currentNodeId = Monster.getClusterNodeId(vertx)
                        val isLeader = leaderNodeId == currentNodeId

                        if (isLeader) {
                            logger.info("Leader node, initializing table for archive [$archiveName]")
                            vertx.executeBlocking(Callable {
                                runBlocking {
                                    archive.createTable()
                                }
                            }).onComplete { createResult ->
                                if (createResult.succeeded() && createResult.result()) {
                                    logger.info("Table initialization completed for archive [$archiveName]")
                                    callback(true)
                                } else {
                                    logger.warning("Failed to initialize table for archive [$archiveName]")
                                    callback(false)
                                }
                            }
                        } else {
                            logger.info("Non-leader node, waiting for archive table [$archiveName] to be initialized by leader")
                            vertx.executeBlocking(Callable {
                                waitForTableReady(archive, archiveName, maxWaitSeconds = 300, pollIntervalMs = 1000)
                            }).onComplete { waitResult ->
                                if (waitResult.succeeded() && waitResult.result()) {
                                    logger.info("Archive table [$archiveName] ready")
                                    callback(true)
                                } else {
                                    logger.warning("Timeout or error waiting for archive table [$archiveName]")
                                    callback(false)
                                }
                            }
                        }
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
                    logger.fine { "Starting Archive purge for [$name] - removing messages older than $olderThan" }

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

            val lastValRetentionMs = Utils.parseDuration(lastValRetentionStr)
            val archiveRetentionMs = Utils.parseDuration(archiveRetentionStr)
            val purgeIntervalMs = Utils.parseDuration(purgeIntervalStr)

            // Detect if LastValRetention is size-based (ends with "k") or time-based
            val maxMemoryEntries = if (lastValRetentionStr.endsWith("k")) {
                lastValRetentionMs  // Already parsed as count (e.g., "50k" → 50000)
            } else {
                // Time-based retention detected (e.g., "7d", "1h") but memory store doesn't support it
                if (lastValType == MessageStoreType.MEMORY) {
                    throw IllegalArgumentException(
                        "Archive group '$name' uses MEMORY LastValType but LastValRetention '$lastValRetentionStr' is time-based. " +
                        "Memory store requires size-based retention (e.g., '50k' for 50,000 entries). " +
                        "Use 'LastValRetention: \"50k\"' format instead, or switch to a persistent store (POSTGRES, CRATEDB, MONGODB, SQLITE)."
                    )
                }
                null  // Other store types (HAZELCAST, POSTGRES, etc.) use time-based retention
            }

            val payloadFormat = PayloadFormat.parse(config.getString("PayloadFormat", "DEFAULT"))

            return ArchiveGroup(
                name = name,
                topicFilter = topicFilter,
                retainedOnly = retainedOnly,
                lastValType = lastValType,
                archiveType = archiveType,
                payloadFormat = payloadFormat,
                lastValRetentionMs = lastValRetentionMs,
                archiveRetentionMs = archiveRetentionMs,
                purgeIntervalMs = purgeIntervalMs,
                lastValRetentionStr = lastValRetentionStr,
                archiveRetentionStr = archiveRetentionStr,
                purgeIntervalStr = purgeIntervalStr,
                maxMemoryEntries = maxMemoryEntries,
                databaseConfig = databaseConfig
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
                        // Will fall back to MEMORY store at runtime with warning
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