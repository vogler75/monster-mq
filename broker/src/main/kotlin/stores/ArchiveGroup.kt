package at.rocworks.stores

import at.rocworks.Utils
import at.rocworks.data.PurgeResult
import at.rocworks.data.TopicTree
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
import io.vertx.core.json.JsonObject
import io.vertx.core.ThreadingModel
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
    private val databaseConfig: JsonObject
) : AbstractVerticle() {
    
    private val logger = Utils.getLogger(this::class.java, name)
    val filterTree: TopicTree<Boolean, Boolean> = TopicTree()
    
    var lastValStore: IMessageStore? = null
        private set
    var archiveStore: IMessageArchive? = null
        private set
        
    private val timers = mutableListOf<Long>()
    
    init {
        topicFilter.forEach { filterTree.add(it, key=true, value=true) }
    }
    
    override fun start(startPromise: Promise<Void>) {
        logger.info("Starting ArchiveGroup [$name]")
        
        // Create and deploy stores
        val lastValFuture = createMessageStore(lastValType, "${name}Lastval")
        val archiveFuture = createMessageArchive(archiveType, "${name}Archive")
        
        Future.all(lastValFuture, archiveFuture).onComplete { result ->
            if (result.succeeded()) {
                // Start retention scheduling if configured
                startRetentionScheduling()
                
                logger.info("ArchiveGroup [$name] started successfully")
                startPromise.complete()
            } else {
                logger.severe("Failed to start ArchiveGroup [$name]: ${result.cause()?.message}")
                startPromise.fail(result.cause())
            }
        }
    }
    
    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Stopping ArchiveGroup [$name]")
        
        stopRetentionScheduling()
        
        // Note: Store verticles will be undeployed automatically by Vert.x
        lastValStore = null
        archiveStore = null
        
        logger.info("ArchiveGroup [$name] stopped")
        stopPromise.complete()
    }
    
    fun getLastValRetentionMs(): Long? = lastValRetentionMs
    fun getArchiveRetentionMs(): Long? = archiveRetentionMs  
    fun getPurgeIntervalMs(): Long? = purgeIntervalMs
    
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
                    if (result.succeeded()) promise.complete()
                    else promise.fail(result.cause())
                }
            }
            MessageStoreType.SQLITE -> {
                val sqlite = databaseConfig.getJsonObject("SQLite")
                val store = MessageStoreSQLite(
                    storeName,
                    sqlite.getString("Path", "monstermq.db")
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
                    if (result.succeeded()) promise.complete()
                    else promise.fail(result.cause())
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
                val archive = MessageArchiveSQLite(
                    archiveName,
                    sqlite.getString("Path", "monstermq.db")
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
    
    private fun startRetentionScheduling() {
        // Schedule store purge if configured
        if (lastValStore != null && lastValRetentionMs != null && purgeIntervalMs != null) {
            val timerId = vertx.setPeriodic(purgeIntervalMs) { _ ->
                purgeLastValStore()
            }
            timers.add(timerId)
            
            logger.info("Scheduled LastVal purge for [$name] every ${purgeIntervalMs}ms with retention ${lastValRetentionMs}ms")
        }
        
        // Schedule archive purge if configured
        if (archiveStore != null && archiveRetentionMs != null && purgeIntervalMs != null) {
            val timerId = vertx.setPeriodic(purgeIntervalMs) { _ ->
                purgeArchiveStore()
            }
            timers.add(timerId)
            
            logger.info("Scheduled Archive purge for [$name] every ${purgeIntervalMs}ms with retention ${archiveRetentionMs}ms")
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
        fun fromConfig(config: JsonObject, databaseConfig: JsonObject): ArchiveGroup {
            val name = config.getString("Name", "ArchiveGroup")
            val topicFilter = config.getJsonArray("TopicFilter").map { it as String }
            val retainedOnly = config.getBoolean("RetainedOnly", false)
            
            val lastValType = MessageStoreType.valueOf(config.getString("LastValType", "NONE"))
            val archiveType = MessageArchiveType.valueOf(config.getString("ArchiveType", "NONE"))
            
            // Parse retention configuration
            val lastValRetentionMs = DurationParser.parse(config.getString("LastValRetention"))
            val archiveRetentionMs = DurationParser.parse(config.getString("ArchiveRetention"))
            val purgeIntervalMs = DurationParser.parse(config.getString("PurgeInterval"))
            
            return ArchiveGroup(
                name, topicFilter, retainedOnly,
                lastValType, archiveType,
                lastValRetentionMs, archiveRetentionMs, purgeIntervalMs,
                databaseConfig
            )
        }
    }
}