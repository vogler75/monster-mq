package at.rocworks

import at.rocworks.bus.IMessageBus
import at.rocworks.bus.MessageBusKafka
import at.rocworks.bus.MessageBusVertx
import at.rocworks.data.BrokerMessage
import at.rocworks.data.BrokerMessageCodec
import at.rocworks.data.BulkClientMessage
import at.rocworks.data.BulkClientMessageCodec
import at.rocworks.data.BulkNodeMessage
import at.rocworks.data.BulkNodeMessageCodec
import at.rocworks.data.MqttSubscription
import at.rocworks.data.MqttSubscriptionCodec
import at.rocworks.extensions.graphql.GraphQLServer
import at.rocworks.extensions.McpServer
import at.rocworks.extensions.PrometheusServer
import at.rocworks.extensions.I3xServer
import at.rocworks.handlers.*
import at.rocworks.handlers.MessageHandler
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.stores.*
import at.rocworks.stores.cratedb.MessageStoreCrateDB
import at.rocworks.stores.mongodb.MessageStoreMongoDB
import at.rocworks.stores.mongodb.MongoClientPool
import at.rocworks.stores.mongodb.QueueStoreMongoDBV1
import at.rocworks.stores.mongodb.QueueStoreMongoDBV2
import at.rocworks.stores.mongodb.SessionStoreMongoDB
import at.rocworks.stores.postgres.MessageStorePostgres
import at.rocworks.stores.postgres.QueueStorePostgresV1
import at.rocworks.stores.postgres.QueueStorePostgresV2
import at.rocworks.stores.postgres.SessionStorePostgres
import at.rocworks.stores.sqlite.MessageStoreSQLite
import at.rocworks.stores.sqlite.QueueStoreSQLiteV1
import at.rocworks.stores.sqlite.QueueStoreSQLiteV2
import at.rocworks.stores.sqlite.SessionStoreSQLite
import at.rocworks.stores.sqlite.SQLiteVerticle
import io.vertx.config.ConfigRetriever
import io.vertx.config.ConfigRetrieverOptions
import io.vertx.config.ConfigStoreOptions
import io.vertx.core.*
// VertxInternal removed in Vert.x 5 - using alternative approaches
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import com.hazelcast.config.Config
import at.rocworks.devices.opcua.OpcUaExtension
import at.rocworks.devices.opcuaserver.OpcUaServerExtension
import at.rocworks.devices.mqttclient.MqttClientExtension
import at.rocworks.devices.kafkaclient.KafkaClientExtension
import at.rocworks.devices.winccoa.WinCCOaExtension
import at.rocworks.devices.winccua.WinCCUaExtension
import at.rocworks.extensions.Oa4jBridge
import at.rocworks.devices.plc4x.Plc4xExtension
import at.rocworks.devices.neo4j.Neo4jExtension
import at.rocworks.devices.sparkplugb.SparkplugBDecoderExtension
import at.rocworks.flowengine.FlowEngineExtension
import at.rocworks.logging.SysLogHandler
import at.rocworks.logging.SyslogVerticle
import handlers.MetricsHandler
import java.io.File
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    Monster(args)
}

class Monster(args: Array<String>) {
    private val logger: Logger = Utils.getLogger(this::class.java)

    private val isClustered = args.find { it == "-cluster" || it == "--cluster" } != null

    private val configFile: String
    private var configJson: JsonObject = JsonObject()
    private val archiveConfigsFile: String?
    private val archiveConfigsMergeFile: String?
    private val deviceConfigsFile: String?
    private val deviceConfigsMergeFile: String?
    private val dashboardPathArg: String?
    var archiveHandler: ArchiveHandler? = null

    // Cluster manager reference for Vert.x 5 compatibility
    private var clusterManager: HazelcastClusterManager? = null
    private var nodeName: String = ""

    private var vertx: Vertx? = null
    private var sessionHandler: SessionHandler? = null
    private var messageBus: IMessageBus? = null
    private var retainedStore: IMessageStore? = null

    private val postgresConfig = object {
        var url: String = ""
        var user: String = ""
        var pass: String = ""
        var schema: String? = null
    }

    private val crateDbConfig = object {
        var url: String = ""
        var user: String = ""
        var pass: String = ""
    }

    private val mongoDbConfig = object {
        var url: String = ""
        var database: String = ""
        var readTimeoutMs: Long = 60000
    }
    private val sqliteConfig = object {
        var path: String = ""
        var enableWAL: Boolean = true
    }

    companion object {
        private var singleton: Monster? = null
        private var sqliteVerticleDeploymentId: String? = null
        private val logger = Utils.getLogger(this::class.java)

        private fun getInstance(): Monster = if (singleton==null) throw Exception("Monster instance is not initialized.") else singleton!!

        /**
         * Get store type with DefaultStoreType fallback support
         */
        @JvmStatic
        fun getStoreTypeWithDefault(configJson: JsonObject, storeTypeKey: String, defaultFallback: String): String {
            val explicitType = configJson.getString(storeTypeKey)
            if (explicitType != null) {
                return explicitType
            }

            val defaultStoreType = configJson.getString("DefaultStoreType")
            if (defaultStoreType != null) {
                return defaultStoreType
            }

            return defaultFallback
        }

        // Convenience functions for specific store types
        @JvmStatic
        fun getSessionStoreType(configJson: JsonObject): String = getStoreTypeWithDefault(configJson, "SessionStoreType", "SQLITE")

        @JvmStatic
        fun getRetainedStoreType(configJson: JsonObject): String = getStoreTypeWithDefault(configJson, "RetainedStoreType", "SQLITE")

        @JvmStatic
        fun getConfigStoreType(configJson: JsonObject): String {
            val type = getStoreTypeWithDefault(configJson, "ConfigStoreType", "SQLITE")
            if (type == "CRATEDB") {
                logger.severe("Unsupported ConfigStoreType 'CRATEDB'. CrateDB is not supported for config storage due to eventual consistency. Use POSTGRES, MONGODB, or SQLITE.")
                exitProcess(1)
            }
            return type
        }

        @JvmStatic
        fun getStoreType(configJson: JsonObject): String = getStoreTypeWithDefault(configJson, "StoreType", "SQLITE")

        @JvmStatic
        fun getQueueStoreType(configJson: JsonObject): String {
            // If QueueStoreType is explicitly set, use it
            val explicit = configJson.getString("QueueStoreType")
            if (explicit != null) return explicit
            // Otherwise, derive from SessionStoreType for backward compatibility
            return getSessionStoreType(configJson)
        }

        fun isClustered() = getInstance().isClustered

        fun getClusterNodeId(vertx: Vertx): String {
            val instance = getInstance()
            return if (instance.isClustered && instance.clusterManager is HazelcastClusterManager) {
                // Use custom node name if available, fallback to member attribute or UUID
                val localMember = instance.clusterManager!!.hazelcastInstance.cluster.localMember
                localMember.getAttribute("nodeName") ?: instance.nodeName.ifEmpty { localMember.uuid.toString() }
            } else "local"
        }

        fun getClusterNodeIds(vertx: Vertx): List<String> {
            val instance = getInstance()
            return if (instance.isClustered && instance.clusterManager is HazelcastClusterManager) {
                instance.clusterManager!!.hazelcastInstance.cluster.members.map { member ->
                    member.getAttribute("nodeName") ?: member.uuid.toString()
                }
            } else listOf("local")
        }

        fun getClusterManager(): HazelcastClusterManager? {
            return getInstance().clusterManager
        }

        // Feature flags — populated at startup from config, read by GraphQL resolvers
        @Volatile
        private var enabledFeatures: Set<String> = emptySet()

        fun getEnabledFeatures(): Set<String> = enabledFeatures

        fun isFeatureEnabled(feature: String): Boolean = enabledFeatures.contains(feature)

        private fun publishEnabledFeatures(vertx: Vertx, features: Set<String>) {
            enabledFeatures = features
            val instance = getInstance()
            if (instance.isClustered && instance.clusterManager is HazelcastClusterManager) {
                val map = instance.clusterManager!!.hazelcastInstance
                    .getMap<String, Set<String>>("monster.enabledFeatures")
                val nodeId = getClusterNodeId(vertx)
                map[nodeId] = features
                // Check for mismatches with other nodes
                map.entries.forEach { (otherId, otherFeatures) ->
                    if (otherId != nodeId && otherFeatures != features) {
                        logger.warning(
                            "Feature flag mismatch detected! " +
                            "This node ($nodeId): $features / " +
                            "Node $otherId: $otherFeatures. " +
                            "All cluster nodes must have identical Features config."
                        )
                    }
                }
            }
        }

        fun getEnabledFeaturesForNode(nodeId: String): Set<String> {
            val instance = getInstance()
            return if (instance.isClustered && instance.clusterManager is HazelcastClusterManager) {
                instance.clusterManager!!.hazelcastInstance
                    .getMap<String, Set<String>>("monster.enabledFeatures")[nodeId] ?: enabledFeatures
            } else enabledFeatures
        }

        fun getSessionHandler(): SessionHandler? {
            return getInstance().sessionHandler
        }

        fun getArchiveHandler(): ArchiveHandler? {
            return getInstance().archiveHandler
        }

        fun getRetainedStore(): IMessageStore? {
            return getInstance().retainedStore
        }

        fun getVertx(): Vertx? {
            return getInstance().vertx
        }

        @Volatile
        private var allowRootWildcardSubscriptionFlag: Boolean = true
        @JvmStatic
        fun allowRootWildcardSubscription(): Boolean = allowRootWildcardSubscriptionFlag

        @Volatile
        private var aclCheckOnSubscriptionFlag: Boolean = true
        @JvmStatic
        fun aclCheckOnSubscription(): Boolean = aclCheckOnSubscriptionFlag

        @Volatile
        private var maxPublishRate: Int = 0
        @JvmStatic
        fun getMaxPublishRate(): Int = maxPublishRate

        @Volatile
        private var maxSubscribeRate: Int = 0
        @JvmStatic
        fun getMaxSubscribeRate(): Int = maxSubscribeRate

        @Volatile
        private var workerPoolSize: Int? = null
        @JvmStatic
        fun getWorkerPoolSize(): Int? = workerPoolSize

        @Volatile
        private var subscriptionQueueSize: Int = 50_000
        @JvmStatic
        fun getSubscriptionQueueSize(): Int = subscriptionQueueSize

        @Volatile
        private var messageQueueSize: Int = 50_000
        @JvmStatic
        fun getMessageQueueSize(): Int = messageQueueSize

        @Volatile
        private var bulkMessagingEnabled: Boolean = true
        @JvmStatic
        fun isBulkMessagingEnabled(): Boolean = bulkMessagingEnabled

        @Volatile
        private var bulkMessagingTimeoutMs: Long = 100L
        @JvmStatic
        fun getBulkMessagingTimeoutMs(): Long = bulkMessagingTimeoutMs

        @Volatile
        private var bulkMessagingBulkSize: Int = 1000
        @JvmStatic
        fun getBulkMessagingBulkSize(): Int = bulkMessagingBulkSize

        @Volatile
        private var publishBulkProcessingEnabled: Boolean = true
        @JvmStatic
        fun isPublishBulkProcessingEnabled(): Boolean = publishBulkProcessingEnabled

        @Volatile
        private var publishBulkTimeoutMs: Long = 50L
        @JvmStatic
        fun getPublishBulkTimeoutMs(): Long = publishBulkTimeoutMs

        @Volatile
        private var publishBulkSize: Int = 1000
        @JvmStatic
        fun getPublishBulkSize(): Int = publishBulkSize

        @Volatile
        private var publishWorkerThreads: Int = 4
        @JvmStatic
        fun getPublishWorkerThreads(): Int = publishWorkerThreads

        private fun ensureSQLiteVerticleDeployed(vertx: Vertx): Future<String> {
            return if (sqliteVerticleDeploymentId == null) {
                val promise = Promise.promise<String>()
                // Pass SQLite configuration to the verticle
                val sqliteVerticleConfig = JsonObject().put("EnableWAL", singleton?.sqliteConfig?.enableWAL ?: true)
                val deploymentOptions = DeploymentOptions().setConfig(sqliteVerticleConfig)
                vertx.deployVerticle(SQLiteVerticle(), deploymentOptions).onComplete { result ->
                    if (result.succeeded()) {
                        sqliteVerticleDeploymentId = result.result()
                        logger.fine("SQLiteVerticle deployed as singleton with ID: ${sqliteVerticleDeploymentId}")
                        promise.complete(result.result())
                    } else {
                        logger.severe("Failed to deploy SQLiteVerticle: ${result.cause()?.message}")
                        promise.fail(result.cause())
                    }
                }
                promise.future()
            } else {
                logger.finer("SQLiteVerticle already deployed with ID: $sqliteVerticleDeploymentId")
                Future.succeededFuture(sqliteVerticleDeploymentId)
            }
        }

    }

    init {
        Utils.getArgIndex(args, listOf("-log", "--log")).let {
            if (it != -1) {
                if (it + 1 >= args.size) {
                    println("ERROR: -log argument requires a value (e.g. INFO, FINE, FINEST)")
                    exitProcess(1)
                }
                try {
                    Const.DEBUG_LEVEL = Level.parse(args[it + 1])
                } catch (e: IllegalArgumentException) {
                    println("ERROR: Invalid log level '${args[it + 1]}'")
                    exitProcess(1)
                }
            }
        }

        // Check if we're running under JManager (MonsterOA)
        if (MonsterOA.getInstance() == null) {
            Utils.initLogging()
        }
        Const.DEBUG_LEVEL?.let { logger.level = it }

        logger.fine("Monster: Starting with ${args.size} arguments ["+args.joinToString("][")+"]")

        // Check for help argument first
        if (args.contains("-help") || args.contains("--help") || args.contains("-h")) {
            printHelp()
            exitProcess(0)
        }

        if (singleton==null) singleton = this
        else throw Exception("Monster instance is already initialized.")

        // Config file
        val configFileIndex = Utils.getArgIndex(args, listOf("-config", "--config"))
        configFile = if (configFileIndex != -1 && configFileIndex + 1 < args.size) {
            args[configFileIndex + 1]
        } else {
            System.getenv("GATEWAY_CONFIG") ?: "config.yaml"
        }

        // Archive configs file (optional) - full sync: import from file, delete orphans
        val archiveConfigsIndex = Utils.getArgIndex(args, listOf("-archiveConfigs", "--archiveConfigs"))
        archiveConfigsFile = if (archiveConfigsIndex != -1 && archiveConfigsIndex + 1 < args.size) {
            args[archiveConfigsIndex + 1]
        } else {
            null
        }

        // Archive configs merge file (optional) - merge: import/update from file, keep existing
        val archiveConfigsMergeIndex = Utils.getArgIndex(args, listOf("-archiveConfigsMerge", "--archiveConfigsMerge"))
        archiveConfigsMergeFile = if (archiveConfigsMergeIndex != -1 && archiveConfigsMergeIndex + 1 < args.size) {
            args[archiveConfigsMergeIndex + 1]
        } else {
            null
        }

        // Validate that both import modes aren't specified simultaneously
        if (archiveConfigsFile != null && archiveConfigsMergeFile != null) {
            println("ERROR: -archiveConfigs and -archiveConfigsMerge cannot be used together")
            exitProcess(1)
        }

        // Device configs file (optional) - full sync: import from file, delete orphans
        val deviceConfigsIndex = Utils.getArgIndex(args, listOf("-deviceConfigs", "--deviceConfigs"))
        deviceConfigsFile = if (deviceConfigsIndex != -1 && deviceConfigsIndex + 1 < args.size) {
            args[deviceConfigsIndex + 1]
        } else {
            null
        }

        // Device configs merge file (optional) - merge: import/update from file, keep existing
        val deviceConfigsMergeIndex = Utils.getArgIndex(args, listOf("-deviceConfigsMerge", "--deviceConfigsMerge"))
        deviceConfigsMergeFile = if (deviceConfigsMergeIndex != -1 && deviceConfigsMergeIndex + 1 < args.size) {
            args[deviceConfigsMergeIndex + 1]
        } else {
            null
        }

        // Validate that both device import modes aren't specified simultaneously
        if (deviceConfigsFile != null && deviceConfigsMergeFile != null) {
            println("ERROR: -deviceConfigs and -deviceConfigsMerge cannot be used together")
            exitProcess(1)
        }

        // Worker pool size (optional)
        Utils.getArgIndex(args, listOf("-workerPoolSize", "--workerPoolSize")).let {
            if (it != -1 && it + 1 < args.size) {
                try {
                    workerPoolSize = args[it + 1].toInt()
                } catch (e: NumberFormatException) {
                    println("ERROR: -workerPoolSize argument must be an integer")
                    exitProcess(1)
                }
            }
        }

        // Dashboard path (optional) - serve dashboard from filesystem
        val dashboardPathIndex = Utils.getArgIndex(args, listOf("-dashboardPath", "--dashboardPath"))
        dashboardPathArg = if (dashboardPathIndex != -1 && dashboardPathIndex + 1 < args.size) {
            args[dashboardPathIndex + 1]
        } else {
            null
        }

        logger.fine("Cluster: ${isClustered()}")

        val builder = Vertx.builder()
        val vertxOptions = VertxOptions()
            .setMaxWorkerExecuteTime(5 * 60 * 1_000_000_000L) // 5 min — LLM calls with tool loops can be long
        // Apply worker pool size if specified via command line
        getWorkerPoolSize()?.let { poolSize ->
            vertxOptions.setWorkerPoolSize(poolSize)
            logger.fine("Vertx worker thread pool size set to $poolSize (via -workerPoolSize argument)")
        }
        builder.with(vertxOptions)

        if (isClustered())
            clusterSetup(builder)
        else
            localSetup(builder)
    }

    private fun loadDeviceConfigs(vertx: Vertx, store: at.rocworks.stores.IDeviceConfigStore, file: String, fullSync: Boolean) {
        val mode = if (fullSync) "full sync" else "merge"
        logger.fine("Loading device configurations from: $file (mode: $mode)")

        vertx.fileSystem().readFile(file).onComplete { readResult ->
            if (readResult.failed()) {
                logger.severe("Failed to read device config file $file: ${readResult.cause()?.message}")
                return@onComplete
            }

            try {
                val jsonArray = JsonArray(readResult.result().toString())
                if (jsonArray.isEmpty) {
                    logger.warning("No device configurations found in $file")
                    return@onComplete
                }

                // Convert JsonArray to List<Map> and force enabled=false for safety
                val configs = jsonArray.filterIsInstance<JsonObject>().map { obj ->
                    val map = obj.map.toMutableMap()
                    map["enabled"] = false
                    map
                }

                val importedNames = configs.mapNotNull { it["name"] as? String }.toSet()

                store.importConfigs(configs).onComplete { importResult ->
                    if (importResult.succeeded()) {
                        val result = importResult.result()
                        logger.fine("Imported ${result.imported} device configs from $file (failed: ${result.failed})")
                        if (result.errors.isNotEmpty()) {
                            result.errors.forEach { logger.warning("Device import error: $it") }
                        }

                        if (fullSync) {
                            deleteOrphanDeviceConfigs(store, importedNames)
                        }
                    } else {
                        logger.severe("Failed to import device configs from $file: ${importResult.cause()?.message}")
                    }
                }
            } catch (e: Exception) {
                logger.severe("Failed to parse JSON from $file: ${e.message}")
            }
        }
    }

    private fun deleteOrphanDeviceConfigs(store: at.rocworks.stores.IDeviceConfigStore, importedNames: Set<String>) {
        store.getAllDevices().onComplete { getAllResult ->
            if (getAllResult.succeeded()) {
                val orphanNames = getAllResult.result()
                    .map { it.name }
                    .filter { it !in importedNames }

                if (orphanNames.isEmpty()) return@onComplete

                orphanNames.forEach { name ->
                    store.deleteDevice(name).onComplete { deleteResult ->
                        if (deleteResult.succeeded() && deleteResult.result()) {
                            logger.fine("Deleted orphan device config [$name] (full sync)")
                        } else {
                            logger.warning("Failed to delete orphan device config [$name]")
                        }
                    }
                }
                logger.fine("Full sync cleanup: deleting ${orphanNames.size} orphan device configs: $orphanNames")
            } else {
                logger.warning("Failed to get all devices for orphan cleanup: ${getAllResult.cause()?.message}")
            }
        }
    }

    private fun printHelp() {
        println("""
MonsterMQ - High-Performance MQTT Broker
Usage: java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt [OPTIONS]

OPTIONS:
  -help, --help, -h     Show this help message and exit
  
  -config <file>        Configuration file path (default: config.yaml)
                        Environment: GATEWAY_CONFIG

  -archiveConfigs <file> Load ArchiveGroups from a JSON file (full sync)
                        Imports all groups from file, deletes groups not in file
                        If ConfigStoreType is NONE: loads into memory

  -archiveConfigsMerge <file>
                        Load ArchiveGroups from a JSON file (merge)
                        Imports/updates groups from file, keeps existing groups
                        If ConfigStoreType is NONE: loads into memory
                        Cannot be used together with -archiveConfigs

  -deviceConfigs <file> Load device configurations from a JSON file (full sync)
                        Imports all devices from file, deletes devices not in file
                        Requires ConfigStoreType to be set (not NONE)
                        Imported devices are disabled by default

  -deviceConfigsMerge <file>
                        Load device configurations from a JSON file (merge)
                        Imports/updates devices from file, keeps existing devices
                        Requires ConfigStoreType to be set (not NONE)
                        Imported devices are disabled by default
                        Cannot be used together with -deviceConfigs

  -cluster              Enable Hazelcast clustering mode for multi-node deployment

  -log <level>          Override logger levels from the command line
                        Only applied when this argument is passed
                        Example levels: INFO, FINE, FINER, FINEST, ALL

  -workerPoolSize <num> Vert.x worker thread pool size
                        Default: 2×CPU count (e.g., 16 on 8-core machine)
                        Increase for high-concurrency scenarios

  -dashboardPath <path> Serve dashboard from a filesystem directory
                        Overrides Dashboard.Path in config.yaml
                        Useful for development with live dashboard builds

EXAMPLES:
  # Start with default configuration
  java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt

  # Start with custom config file
  java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt -config myconfig.yaml

  # Start with custom worker thread pool size for high load
  java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt -workerPoolSize 64

  # Start with temporary debug logging from the command line
  java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt -log FINEST

  # Start with SQLite configuration and a custom worker pool size
  java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt -config config-sqlite.yaml -workerPoolSize 128

FEATURES:
  • MQTT 3.1.1 Protocol Support with QoS 0, 1, 2
  • Multi-Database Support: PostgreSQL, SQLite, CrateDB, MongoDB
  • WebSocket Support (WS/WSS) for web applications
  • Hazelcast Clustering for horizontal scaling
  • Model Context Protocol (MCP) Server for AI integration
  • SparkplugB Extension for Industrial IoT
  • Persistent Sessions and Message Queuing

CONFIGURATION:
  Configuration is done via YAML file. See config.yaml for examples.
  Schema validation available at: broker/yaml-json-schema.json

MORE INFO:
  Documentation: README.md
  Source: https://github.com/rocworks/monstermq
  License: GNU General Public License v3.0
        """.trimIndent())
    }

    private fun getConfigRetriever(vertx: Vertx): ConfigRetriever {
        logger.fine("Monster config file: $configFile")
        return ConfigRetriever.create(
            vertx,
            ConfigRetrieverOptions()
                .setScanPeriod(0) // Disable config file scanning to avoid file system errors during shutdown
                .addStore(
                    ConfigStoreOptions()
                        .setType("file")
                        .setFormat("yaml")
                        .setConfig(JsonObject().put("path", configFile))
                )
        )
    }

    private fun getConfigAndStart(vertx: Vertx) {
        getConfigRetriever(vertx).config.onComplete { it ->
            if (it.succeeded()) {
                this.configJson = it.result()

                configJson.getJsonObject("Postgres", JsonObject()).let { pg ->
                    postgresConfig.url = pg.getString("Url", "jdbc:postgresql://localhost:5432/postgres")
                    postgresConfig.user = pg.getString("User", "system")
                    postgresConfig.pass = pg.getString("Pass", "manager")
                    postgresConfig.schema = pg.getString("Schema")
                }
                configJson.getJsonObject("CrateDB", JsonObject()).let { crate ->
                    crateDbConfig.url = crate.getString("Url", "jdbc:postgresql://localhost:5432/doc")
                    crateDbConfig.user = crate.getString("User", "crate")
                    crateDbConfig.pass = crate.getString("Pass", "")
                }
                configJson.getJsonObject("MongoDB", JsonObject()).let { mongo ->
                    mongoDbConfig.url = mongo.getString("Url", "mongodb://localhost:27017")
                    mongoDbConfig.database = mongo.getString("Database", "monster")
                    mongoDbConfig.readTimeoutMs = mongo.getLong("ReadTimeoutMs", 60000L)
                    MongoClientPool.defaultReadTimeoutMs = mongoDbConfig.readTimeoutMs
                }
                configJson.getJsonObject("SQLite", JsonObject()).let { sqlite ->
                    sqliteConfig.path = sqlite.getString("Path", Const.SQLITE_DEFAULT_PATH)
                    sqliteConfig.enableWAL = sqlite.getBoolean("EnableWAL", true)
                }

                // Validate SQLite directory if SQLite is used for any store
                validateSQLiteDirectory(configJson)

                // Read AllowRootWildcardSubscription once at startup (default true)
                allowRootWildcardSubscriptionFlag = try {
                    configJson.getBoolean("AllowRootWildcardSubscription", true)
                } catch (e: Exception) {
                    logger.warning("Config: AllowRootWildcardSubscription read failed: ${e.message}")
                    true
                }
                logger.fine("Config: AllowRootWildcardSubscription=$allowRootWildcardSubscriptionFlag")

                // Read AclCheckOnSubscription from UserManagement (default true = check at subscribe time)
                aclCheckOnSubscriptionFlag = try {
                    configJson.getJsonObject("UserManagement", JsonObject())
                        .getBoolean("AclCheckOnSubscription", true)
                } catch (e: Exception) {
                    logger.warning("Config: AclCheckOnSubscription read failed: ${e.message}")
                    true
                }
                logger.fine("Config: AclCheckOnSubscription=$aclCheckOnSubscriptionFlag")

                // Read rate limiting configuration (default 0 = unlimited)
                maxPublishRate = try {
                    configJson.getInteger("MaxPublishRate", 0)
                } catch (e: Exception) {
                    logger.warning("Config: MaxPublishRate read failed: ${e.message}")
                    0
                }
                logger.fine("Config: MaxPublishRate=$maxPublishRate msg/s" + if (maxPublishRate == 0) " (unlimited)" else "")

                maxSubscribeRate = try {
                    configJson.getInteger("MaxSubscribeRate", 0)
                } catch (e: Exception) {
                    logger.warning("Config: MaxSubscribeRate read failed: ${e.message}")
                    0
                }
                logger.fine("Config: MaxSubscribeRate=$maxSubscribeRate msg/s" + if (maxSubscribeRate == 0) " (unlimited)" else "")

                // Read queue size configuration (defaults: 50,000 for each queue type)
                configJson.getJsonObject("Queues", JsonObject()).let { queuesConfig ->
                    subscriptionQueueSize = try {
                        queuesConfig.getInteger("SubscriptionQueueSize", 50_000)
                    } catch (e: Exception) {
                        logger.warning("Config: Queues.SubscriptionQueueSize read failed: ${e.message}")
                        50_000
                    }
                    messageQueueSize = try {
                        queuesConfig.getInteger("MessageQueueSize", 50_000)
                    } catch (e: Exception) {
                        logger.warning("Config: Queues.MessageQueueSize read failed: ${e.message}")
                        50_000
                    }
                }
                logger.fine("Config: Queue sizes - Subscription=$subscriptionQueueSize, Message=$messageQueueSize")

                // Read bulk messaging configuration
                configJson.getJsonObject("BulkMessaging", JsonObject()).let { bulkConfig ->
                    bulkMessagingEnabled = try {
                        bulkConfig.getBoolean("Enabled", true)
                    } catch (e: Exception) {
                        logger.warning("Config: BulkMessaging.Enabled read failed: ${e.message}")
                        true
                    }
                    bulkMessagingTimeoutMs = try {
                        bulkConfig.getLong("TimeoutMS", 100L)
                    } catch (e: Exception) {
                        logger.warning("Config: BulkMessaging.TimeoutMS read failed: ${e.message}")
                        100L
                    }
                    bulkMessagingBulkSize = try {
                        bulkConfig.getInteger("BulkSize", 1000)
                    } catch (e: Exception) {
                        logger.warning("Config: BulkMessaging.BulkSize read failed: ${e.message}")
                        1000
                    }
                }
                if (bulkMessagingEnabled) {
                    logger.fine("Config: BulkMessaging enabled - TimeoutMS=$bulkMessagingTimeoutMs, BulkSize=$bulkMessagingBulkSize")
                } else {
                    logger.fine("Config: BulkMessaging disabled")
                }

                // Read publish bulk processing configuration
                configJson.getJsonObject("BulkProcessing", JsonObject()).let { bulkProcConfig ->
                    publishBulkProcessingEnabled = try {
                        bulkProcConfig.getBoolean("Enabled", true)
                    } catch (e: Exception) {
                        logger.warning("Config: BulkProcessing.Enabled read failed: ${e.message}")
                        true
                    }
                    publishBulkTimeoutMs = try {
                        bulkProcConfig.getLong("TimeoutMS", 50L)
                    } catch (e: Exception) {
                        logger.warning("Config: BulkProcessing.TimeoutMS read failed: ${e.message}")
                        50L
                    }
                    publishBulkSize = try {
                        bulkProcConfig.getInteger("BulkSize", 1000)
                    } catch (e: Exception) {
                        logger.warning("Config: BulkProcessing.BulkSize read failed: ${e.message}")
                        1000
                    }
                    publishWorkerThreads = try {
                        bulkProcConfig.getInteger("WorkerThreads", 4)
                    } catch (e: Exception) {
                        logger.warning("Config: BulkProcessing.WorkerThreads read failed: ${e.message}")
                        4
                    }
                }
                if (publishBulkProcessingEnabled) {
                    logger.fine("Config: BulkProcessing enabled - TimeoutMS=$publishBulkTimeoutMs, BulkSize=$publishBulkSize, Workers=$publishWorkerThreads")
                } else {
                    logger.fine("Config: BulkProcessing disabled")
                }

                startMonster(vertx)
            } else {
                logger.severe("Config loading failed: ${it.cause()}")
            }
        }
    }


    private fun loadConfigSync() {
        try {
            val configFileObj = java.io.File(configFile)
            if (configFileObj.exists()) {
                val yamlContent = configFileObj.readText()
                // Simple YAML to JSON conversion for basic properties
                val lines = yamlContent.split('\n')
                val jsonObj = JsonObject()

                lines.forEach { line ->
                    val trimmed = line.trim()
                    if (trimmed.isNotEmpty() && !trimmed.startsWith("#") && trimmed.contains(":")) {
                        val parts = trimmed.split(":", limit = 2)
                        if (parts.size == 2) {
                            val key = parts[0].trim()
                            val value = parts[1].trim().removePrefix("\"").removeSuffix("\"")
                            when {
                                value.equals("true", ignoreCase = true) -> jsonObj.put(key, true)
                                value.equals("false", ignoreCase = true) -> jsonObj.put(key, false)
                                value.toIntOrNull() != null -> jsonObj.put(key, value.toInt())
                                else -> jsonObj.put(key, value)
                            }
                        }
                    }
                }

                this.configJson = jsonObj
                logger.fine("Config loaded synchronously for cluster setup")
            }
        } catch (e: Exception) {
            logger.warning("Failed to load config synchronously: ${e.message}")
            this.configJson = JsonObject()
        }
    }

    private fun localSetup(builder: VertxBuilder) {
        val vertx = builder.build()
        this.vertx = vertx
        getConfigAndStart(vertx)
    }

    private fun clusterSetup(builder: VertxBuilder) {
        // Load config synchronously for cluster setup
        loadConfigSync()

        // Get node name from config, fallback to hostname
        this.nodeName = configJson.getString("NodeName",
            try {
                java.net.InetAddress.getLocalHost().hostName
            } catch (e: Exception) {
                "node-${System.currentTimeMillis()}"
            }
        )

        // Configure Hazelcast with custom node name
        val hazelcastConfig = Config()
        hazelcastConfig.clusterName = "MonsterMQ"
        hazelcastConfig.memberAttributeConfig.setAttribute("nodeName", this.nodeName)

        // Set instance name to the node name for easier identification
        hazelcastConfig.instanceName = this.nodeName

        this.clusterManager = HazelcastClusterManager(hazelcastConfig)

        logger.fine("Cluster setup with node name: ${this.nodeName}")

        builder.withClusterManager(this.clusterManager)
        builder.buildClustered().onComplete { res: AsyncResult<Vertx?> ->
            if (res.succeeded() && res.result() != null) {
                val vertx = res.result()!!
                this.vertx = vertx
                getConfigAndStart(vertx)
            } else {
                logger.severe("Vertx building failed: ${res.cause()}")
            }
        }
    }

    private fun startMonster(vertx: Vertx) {
        val useTcp = configJson.getInteger("TCP", 1883)
        val useWs = configJson.getInteger("WS", 0)

        val useNats = configJson.getInteger("NATS", 0)

        val useTcpSsl = configJson.getInteger("TCPS", 0)
        val useWsSsl = configJson.getInteger("WSS", 0)

        // Load SSL configuration
        val sslConfig = configJson.getJsonObject("SSL", JsonObject())
        val keyStorePath = sslConfig.getString("KeyStorePath", "server-keystore.jks")
        val keyStorePassword = sslConfig.getString("KeyStorePassword", "password")
        val keyStoreType = sslConfig.getString("KeyStoreType", "JKS")

        // Load TCP server configuration
        val tcpServerConfig = configJson.getJsonObject("MqttTcpServer", JsonObject())
        val maxMessageSize = tcpServerConfig.getInteger("MaxMessageSizeKb", 512) * 1024
        val tcpNoDelay = tcpServerConfig.getBoolean("NoDelay", true)
        val receiveBufferSize = tcpServerConfig.getInteger("ReceiveBufferSizeKb", 512) * 1024
        val sendBufferSize = tcpServerConfig.getInteger("SendBufferSizeKb", 512) * 1024

        val queuedMessagesEnabled = configJson.getBoolean("QueuedMessagesEnabled", true)
        logger.fine("TCP [$useTcp] WS [$useWs] TCPS [$useTcpSsl] WSS [$useWsSsl] NATS [$useNats] QME [$queuedMessagesEnabled]")
        logger.fine("TCP Server Config: MaxMessageSize [${maxMessageSize / 1024}KB] NoDelay [$tcpNoDelay] ReceiveBufferSize [${receiveBufferSize / 1024}KB] SendBufferSize [${sendBufferSize / 1024}KB]")

        val retainedStoreType = MessageStoreType.valueOf(Monster.getRetainedStoreType(configJson))
        logger.fine("RetainedMessageStoreType [${retainedStoreType}]")

        vertx.eventBus().registerDefaultCodec(BrokerMessage::class.java, BrokerMessageCodec())
        vertx.eventBus().registerDefaultCodec(MqttSubscription::class.java, MqttSubscriptionCodec())
        vertx.eventBus().registerDefaultCodec(BulkClientMessage::class.java, BulkClientMessageCodec())
        vertx.eventBus().registerDefaultCodec(BulkNodeMessage::class.java, BulkNodeMessageCodec())

        getSessionStore(vertx).compose { sessionStore ->
            getQueueStore(vertx).map { queueStore -> Pair(sessionStore, queueStore) }
        }.onSuccess { (sessionStore, queueStore) ->
            // Message bus
            val (messageBus, messageBusReady) = getMessageBus(vertx)

            // Retained messages
            val (retainedStore, retainedReady) = getMessageStore(vertx, "RetainedMessages", retainedStoreType)
            this.retainedStore = retainedStore

            // Archive groups
            val archiveHandler = ArchiveHandler(vertx, configJson, archiveConfigsFile, archiveConfigsMergeFile, isClustered)
            val archiveGroupsFuture = archiveHandler.initialize()

            // Wait for all stores to be ready
            Future.all<Any>(listOf(archiveGroupsFuture, retainedReady, messageBusReady)).onFailure { it ->
                logger.severe("Initialization of bus or archive groups failed: ${it.message}")
                exitProcess(-1)
            }.onComplete {
                logger.fine("Initialization of bus and archive groups completed.")
                val archiveGroups = archiveGroupsFuture.result()

                // Message handler
                val messageHandler = MessageHandler(retainedStore!!, archiveGroups)

                // Session handler
                val sessionHandler = SessionHandler(sessionStore, queueStore, messageBus, messageHandler, queuedMessagesEnabled)

                // Store ArchiveHandler, SessionHandler, and MessageBus for later access
                singleton?.let { instance ->
                    instance.archiveHandler = archiveHandler
                    archiveHandler.setMessageHandler(messageHandler)
                    instance.sessionHandler = sessionHandler
                    instance.messageBus = messageBus
                }

                // OPC UA Extension
                val opcUaExtension = OpcUaExtension()

                // MQTT Client Extension
                val mqttClientExtension = MqttClientExtension()

                // User management
                val userManager = at.rocworks.auth.UserManager(configJson)

                // Device Config Store for OPC UA Server Extension
                val configStoreType = getConfigStoreType(configJson)
                val deviceConfigStore = if (configStoreType != "NONE") {
                    try {
                        val store = at.rocworks.stores.DeviceConfigStoreFactory.create(configStoreType, configJson, vertx)
                        // Register as shared instance so other extensions use the same one
                        at.rocworks.stores.DeviceConfigStoreFactory.setSharedInstance(store)
                        // Initialize device store asynchronously
                        store?.initialize()?.onComplete { result ->
                            if (result.failed()) {
                                logger.warning("Failed to initialize device config store: ${result.cause()?.message}")
                            } else {
                                // Start Topic Schema Policy Cache after store is ready
                                at.rocworks.schema.TopicSchemaPolicyCache(vertx, store).start()

                                // Load device configs from file if specified
                                val deviceConfigFile = deviceConfigsFile ?: deviceConfigsMergeFile
                                if (deviceConfigFile != null) {
                                    val fullSync = deviceConfigsFile != null
                                    loadDeviceConfigs(vertx, store, deviceConfigFile, fullSync)
                                }
                            }
                        }
                        store
                    } catch (e: Exception) {
                        logger.warning("Failed to create device config store: ${e.message}")
                        null
                    }
                } else {
                    if (deviceConfigsFile != null || deviceConfigsMergeFile != null) {
                        logger.warning("-deviceConfigs/-deviceConfigsMerge requires ConfigStoreType to be set (not NONE)")
                    }
                    null
                }

                // OPC UA Server Extension
                val opcUaServerExtension = if (deviceConfigStore != null) {
                    OpcUaServerExtension(sessionHandler, deviceConfigStore, userManager)
                } else {
                    null
                }

                // Health handler
                val healthHandler = HealthHandler(sessionHandler)

                // MCP Server
                val mcpConfig = configJson.getJsonObject("MCP", JsonObject())
                val mcpEnabled = mcpConfig.getBoolean("Enabled", false)
                val mcpPort = mcpConfig.getInteger("Port", 3000)
                val mcpServer = if (mcpEnabled) {
                    McpServer("0.0.0.0", mcpPort, retainedStore, archiveHandler, userManager)
                } else {
                    logger.fine("MCP server is disabled in configuration")
                    null
                }

                // Prometheus-compatible metrics server (instantiated after metricsStore below)
                val prometheusConfig = configJson.getJsonObject("Prometheus", JsonObject())
                val prometheusEnabled = prometheusConfig.getBoolean("Enabled", false)
                val prometheusPort = prometheusConfig.getInteger("Port", 3001)

                // CESMII I3X API Server
                val i3xConfig = configJson.getJsonObject("I3x", JsonObject())
                val i3xEnabled = i3xConfig.getBoolean("Enabled", false)
                val i3xPort = i3xConfig.getInteger("Port", 3002)
                val i3xServer = if (i3xEnabled) {
                    I3xServer("0.0.0.0", i3xPort, archiveHandler, sessionHandler, userManager)
                } else {
                    logger.fine("I3X API server is disabled in configuration")
                    null
                }

                // Metrics Collector and Store
                val metricsConfig = configJson.getJsonObject("Metrics", JsonObject())
                val metricsEnabled = metricsConfig.getBoolean("Enabled", true)

                val (metricsStore, metricsCollector) = if (metricsEnabled) {
                    try {
                        val storeTypeStr = getStoreType(configJson)
                        val storeType = try {
                            MetricsStoreType.valueOf(storeTypeStr.uppercase())
                        } catch (e: IllegalArgumentException) {
                            logger.warning("Invalid metrics store type: $storeTypeStr. Defaulting to POSTGRES")
                            MetricsStoreType.POSTGRES
                        }

                        val store = MetricsStoreFactory.create(storeType, configJson, "metrics")
                        val collectionInterval = metricsConfig.getInteger("CollectionInterval", 10)
                        val retentionHours = metricsConfig.getInteger("RetentionHours", 24)
                        logger.fine("Starting Metrics Store: ${store.getName()} (${store.getType()}) with ${collectionInterval}s collection interval, ${retentionHours}h retention")
                        val collector = MetricsHandler(sessionHandler, store, messageBus, messageHandler, collectionInterval, retentionHours)
                        store to collector
                    } catch (e: Exception) {
                        logger.warning("Failed to create metrics store: ${e.message}")
                        e.printStackTrace()
                        null to null
                    }
                } else {
                    logger.fine("Metrics collection is disabled in configuration")
                    null to null
                }

                // Prometheus Server (needs metricsStore for historical broker metrics)
                val prometheusRawQueryLimit = prometheusConfig.getInteger("RawQueryLimit", 10000)
                val prometheusServer = if (prometheusEnabled) {
                    PrometheusServer("0.0.0.0", prometheusPort, archiveHandler, userManager, metricsStore, prometheusRawQueryLimit)
                } else {
                    logger.fine("Prometheus server is disabled in configuration")
                    null
                }

                // GenAI Provider
                val genAiProvider = try {
                    at.rocworks.genai.GenAiProviderFactory.create(vertx, configJson).get()
                } catch (e: Exception) {
                    logger.warning("Failed to initialize GenAI provider: ${e.message}")
                    null
                }

                // Dashboard config
                val dashboardConfig = configJson.getJsonObject("Dashboard", JsonObject())
                val dashboardEnabled = dashboardConfig.getBoolean("Enabled", true)
                val dashboardPath: String? = if (dashboardEnabled) {
                    dashboardPathArg ?: dashboardConfig.getString("Path", "")
                } else null

                // GraphQL Server
                val graphQLConfig = configJson.getJsonObject("GraphQL", JsonObject())
                val graphQLEnabled = graphQLConfig.getBoolean("Enabled", true)

                // REST API Server (hosted on the same port as GraphQL)
                val restApiConfig = configJson.getJsonObject("RestApi", JsonObject())
                val restApiEnabled = restApiConfig.getBoolean("Enabled", true)
                val restApiServer = if (graphQLEnabled && restApiEnabled && this.archiveHandler != null) {
                    at.rocworks.extensions.RestApiServer(
                        vertx,
                        sessionHandler,
                        this.archiveHandler!!,
                        retainedStore,
                        userManager
                    )
                } else {
                    if (!restApiEnabled) logger.fine("REST API is disabled in configuration")
                    null
                }

                val graphQLServer = if (graphQLEnabled) {
                    val archiveGroupsMap = archiveGroups.associateBy { it.name }

                    GraphQLServer(
                        vertx,
                        configJson,
                        messageBus,
                        messageHandler,
                        retainedStore,
                        archiveGroupsMap,
                        userManager,
                        sessionStore,
                        queueStore,
                        sessionHandler,
                        metricsStore,
                        this.archiveHandler,
                        deviceConfigStore,
                        genAiProvider,
                        dashboardPath,
                        restApiServer
                    )
                } else {
                    logger.fine("GraphQL server is disabled in configuration")
                    null
                }


                // MQTT Servers
                val servers = listOfNotNull(
                    if (useTcp>0) MqttServer(useTcp, false, false, maxMessageSize, tcpNoDelay, receiveBufferSize, sendBufferSize, sessionHandler, userManager) else null,
                    if (useWs>0) MqttServer(useWs, false, true, maxMessageSize, tcpNoDelay, receiveBufferSize, sendBufferSize, sessionHandler, userManager) else null,
                    if (useTcpSsl>0) MqttServer(useTcpSsl, true, false, maxMessageSize, tcpNoDelay, receiveBufferSize, sendBufferSize, sessionHandler, userManager, keyStorePath, keyStorePassword, keyStoreType) else null,
                    if (useWsSsl>0) MqttServer(useWsSsl, true, true, maxMessageSize, tcpNoDelay, receiveBufferSize, sendBufferSize, sessionHandler, userManager, keyStorePath, keyStorePassword, keyStoreType) else null,
                    if (useNats>0) NatsServer(useNats, sessionHandler, userManager) else null,
                    mcpServer,
                    prometheusServer,
                    i3xServer
                )

                // Deploy all verticles
                Future.succeededFuture<String>()
                    .compose { vertx.deployVerticle(messageHandler) }
                    .compose { vertx.deployVerticle(sessionHandler) }
                    .compose {
                        // Configure logging (in-memory log capture)
                        val loggingConfig = configJson.getJsonObject("Logging", JsonObject())
                        val memoryConfig = loggingConfig.getJsonObject("Memory", JsonObject())
                        val memoryLoggingEnabled = memoryConfig.getBoolean("Enabled", false)

                        if (memoryLoggingEnabled) {
                            try {
                                logger.fine("Installing log handler for system-wide log capture...")
                                SysLogHandler.install()
                                logger.info("Log handler installed successfully")
                            } catch (e: Exception) {
                                logger.severe("Failed to install log handler: ${e.message}")
                                throw e
                            }
                        } else {
                            logger.fine("Memory logging is disabled")
                        }

                        Future.succeededFuture<String>()
                    }
                    .compose {
                        // Deploy SyslogVerticle (independent of MQTT setting)
                        val loggingConfig = configJson.getJsonObject("Logging", JsonObject())
                        val memoryConfig = loggingConfig.getJsonObject("Memory", JsonObject())
                        val memoryEnabled = memoryConfig.getBoolean("Enabled", true)
                        val memoryEntries = memoryConfig.getInteger("Entries", 1000)

                        if (memoryEnabled) {
                            logger.fine("Deploying SyslogVerticle with memoryEntries=$memoryEntries")
                            val syslogVerticle = SyslogVerticle(true, memoryEntries)
                            vertx.deployVerticle(syslogVerticle)
                        } else {
                            logger.fine("In-memory syslog storage is disabled")
                            Future.succeededFuture<String>()
                        }
                    }
                    .compose { vertx.deployVerticle(userManager) }
                    .compose { vertx.deployVerticle(healthHandler) }
                    .compose {
                        // Resolve feature flags from top-level Features config block
                        val featuresConfig = configJson.getJsonObject("Features", JsonObject())
                        val allFeatures = listOf(
                            "OpcUa", "OpcUaServer", "MqttClient", "Kafka", "Nats", "Redis", "Telegram",
                            "WinCCOa", "WinCCUa", "Plc4x", "Neo4j", "JdbcLogger", "InfluxDBLogger", "TimeBaseLogger",
                            "SparkplugB", "FlowEngine", "Agents"
                        )
                        val enabled = allFeatures.filter { featuresConfig.getBoolean(it, true) }.toSet()
                        publishEnabledFeatures(vertx, enabled)
                        logger.info("Enabled features: $enabled")
                        Future.succeededFuture<String>()
                    }
                    .compose {
                        if (Monster.isFeatureEnabled("OpcUa")) {
                            val opcUaDeploymentOptions = DeploymentOptions().setConfig(configJson)
                            vertx.deployVerticle(opcUaExtension, opcUaDeploymentOptions)
                        } else {
                            logger.fine("OpcUa extension disabled by Features config")
                            Future.succeededFuture()
                        }
                    }
                    .compose {
                        if (opcUaServerExtension != null && Monster.isFeatureEnabled("OpcUaServer")) {
                            vertx.deployVerticle(opcUaServerExtension)
                        } else {
                            Future.succeededFuture<String>()
                        }
                    }
                    .compose {
                        if (Monster.isFeatureEnabled("MqttClient")) {
                            val mqttClientDeploymentOptions = DeploymentOptions().setConfig(configJson)
                            vertx.deployVerticle(mqttClientExtension, mqttClientDeploymentOptions)
                        } else {
                            logger.fine("MqttClient extension disabled by Features config")
                            Future.succeededFuture()
                        }
                    }
                    .compose {
                        // Kafka Subscriber Extension
                        if (Monster.isFeatureEnabled("Kafka")) {
                            val kafkaClientExtension = KafkaClientExtension()
                            val kafkaDeploymentOptions = DeploymentOptions().setConfig(configJson)
                            vertx.deployVerticle(kafkaClientExtension, kafkaDeploymentOptions)
                        } else {
                            logger.fine("Kafka extension disabled by Features config")
                            Future.succeededFuture()
                        }
                    }
                    .compose {
                        // NATS Client Bridge Extension
                        if (Monster.isFeatureEnabled("Nats")) {
                            val natsClientExtension = at.rocworks.devices.natsclient.NatsClientExtension()
                            val natsDeploymentOptions = DeploymentOptions().setConfig(configJson)
                            vertx.deployVerticle(natsClientExtension, natsDeploymentOptions)
                        } else {
                            logger.fine("Nats extension disabled by Features config")
                            Future.succeededFuture()
                        }
                    }
                    .compose {
                        // Redis Client Bridge Extension
                        if (Monster.isFeatureEnabled("Redis")) {
                            val redisClientExtension = at.rocworks.devices.redisclient.RedisClientExtension()
                            val redisDeploymentOptions = DeploymentOptions().setConfig(configJson)
                            vertx.deployVerticle(redisClientExtension, redisDeploymentOptions)
                        } else {
                            logger.fine("Redis extension disabled by Features config")
                            Future.succeededFuture()
                        }
                    }
                    .compose {
                        // Telegram Client Bridge Extension
                        if (Monster.isFeatureEnabled("Telegram")) {
                            val telegramClientExtension = at.rocworks.devices.telegramclient.TelegramClientExtension()
                            val telegramDeploymentOptions = DeploymentOptions().setConfig(configJson)
                            vertx.deployVerticle(telegramClientExtension, telegramDeploymentOptions)
                        } else {
                            logger.fine("Telegram extension disabled by Features config")
                            Future.succeededFuture()
                        }
                    }
                    .compose {
                        // WinCC OA Client Extension (GraphQL-based)
                        if (Monster.isFeatureEnabled("WinCCOa")) {
                            val winCCOaExtension = WinCCOaExtension()
                            val winCCOaDeploymentOptions = DeploymentOptions().setConfig(configJson)
                            vertx.deployVerticle(winCCOaExtension, winCCOaDeploymentOptions)
                        } else {
                            logger.fine("WinCCOa extension disabled by Features config")
                            Future.succeededFuture()
                        }
                    }
                    .compose {
                        // OA Datapoint Bridge (native oa4j dpConnect for !OA/ topics)
                        // Only starts if running as MonsterOA (JManager) and enabled in config
                        val oaBridgeConfig = configJson.getJsonObject("Oa4jBridge")
                        if (oaBridgeConfig?.getBoolean("Enabled", false) == true) {
                            val namespace = oaBridgeConfig.getString("Namespace", "")
                            if (namespace.isNotEmpty()) {
                                val attributes = oaBridgeConfig.getJsonArray("Attributes")
                                    ?: io.vertx.core.json.JsonArray()
                                        .add("_online.._value")
                                        .add("_online.._stime")
                                        .add("_online.._status")
                                val bridgeConfig = JsonObject()
                                    .put("namespace", namespace)
                                    .put("attributes", attributes)
                                val oa4jBridge = Oa4jBridge()
                                val oaDeploymentOptions = DeploymentOptions().setConfig(bridgeConfig)
                                vertx.deployVerticle(oa4jBridge, oaDeploymentOptions)
                            } else {
                                logger.warning("Oa4jBridge enabled but no namespace configured")
                                Future.succeededFuture<String>()
                            }
                        } else {
                            Future.succeededFuture<String>()
                        }
                    }
                    .compose {
                        // WinCC Unified Client Extension
                        if (Monster.isFeatureEnabled("WinCCUa")) {
                            val winCCUaExtension = WinCCUaExtension()
                            val winCCUaDeploymentOptions = DeploymentOptions().setConfig(configJson)
                            vertx.deployVerticle(winCCUaExtension, winCCUaDeploymentOptions)
                        } else {
                            logger.fine("WinCCUa extension disabled by Features config")
                            Future.succeededFuture()
                        }
                    }
                    .compose {
                        // PLC4X Client Extension
                        if (Monster.isFeatureEnabled("Plc4x")) {
                            val plc4xExtension = Plc4xExtension()
                            val plc4xDeploymentOptions = DeploymentOptions().setConfig(configJson)
                            vertx.deployVerticle(plc4xExtension, plc4xDeploymentOptions)
                        } else {
                            logger.fine("Plc4x extension disabled by Features config")
                            Future.succeededFuture()
                        }
                    }
                    .compose {
                        // Neo4j Client Extension
                        if (Monster.isFeatureEnabled("Neo4j")) {
                            val neo4jExtension = Neo4jExtension()
                            val neo4jDeploymentOptions = DeploymentOptions().setConfig(configJson)
                            vertx.deployVerticle(neo4jExtension, neo4jDeploymentOptions)
                        } else {
                            logger.fine("Neo4j extension disabled by Features config")
                            Future.succeededFuture()
                        }
                    }
                    .compose {
                        // JDBC Logger Extension
                        if (Monster.isFeatureEnabled("JdbcLogger")) {
                            val jdbcLoggerExtension = at.rocworks.logger.JDBCLoggerExtension()
                            val jdbcLoggerDeploymentOptions = DeploymentOptions().setConfig(configJson)
                            vertx.deployVerticle(jdbcLoggerExtension, jdbcLoggerDeploymentOptions)
                        } else {
                            logger.fine("JdbcLogger extension disabled by Features config")
                            Future.succeededFuture()
                        }
                    }
                    .compose {
                        // InfluxDB Logger Extension
                        if (Monster.isFeatureEnabled("InfluxDBLogger")) {
                            val influxDBLoggerExtension = at.rocworks.logger.InfluxDBLoggerExtension()
                            val influxDBLoggerDeploymentOptions = DeploymentOptions().setConfig(configJson)
                            vertx.deployVerticle(influxDBLoggerExtension, influxDBLoggerDeploymentOptions)
                        } else {
                            logger.fine("InfluxDBLogger extension disabled by Features config")
                            Future.succeededFuture()
                        }
                    }
                    .compose {
                        // TimeBase Logger Extension
                        if (Monster.isFeatureEnabled("TimeBaseLogger")) {
                            val timeBaseLoggerExtension = at.rocworks.logger.TimeBaseLoggerExtension()
                            val timeBaseLoggerDeploymentOptions = DeploymentOptions().setConfig(configJson)
                            vertx.deployVerticle(timeBaseLoggerExtension, timeBaseLoggerDeploymentOptions)
                        } else {
                            logger.fine("TimeBaseLogger extension disabled by Features config")
                            Future.succeededFuture()
                        }
                    }
                    .compose {
                        // SparkplugB Decoder Extension
                        if (Monster.isFeatureEnabled("SparkplugB")) {
                            val sparkplugBDecoderExtension = SparkplugBDecoderExtension()
                            val sparkplugBDecoderDeploymentOptions = DeploymentOptions().setConfig(configJson)
                            vertx.deployVerticle(sparkplugBDecoderExtension, sparkplugBDecoderDeploymentOptions)
                        } else {
                            logger.fine("SparkplugB extension disabled by Features config")
                            Future.succeededFuture()
                        }
                    }
                    .compose {
                        // Flow Engine Extension
                        if (Monster.isFeatureEnabled("FlowEngine")) {
                            val flowEngineExtension = FlowEngineExtension()
                            val flowEngineDeploymentOptions = DeploymentOptions().setConfig(configJson)
                            vertx.deployVerticle(flowEngineExtension, flowEngineDeploymentOptions)
                        } else {
                            logger.fine("FlowEngine extension disabled by Features config")
                            Future.succeededFuture()
                        }
                    }
                    .compose {
                        if (metricsCollector != null) {
                            vertx.deployVerticle(metricsCollector)
                        } else {
                            Future.succeededFuture<String>()
                        }
                    }
                    .compose { Future.all<String>(servers.map { vertx.deployVerticle(it) }) }
                    .compose {
                        // Agent Extension (must start AFTER MCP server and other servers are ready)
                        if (Monster.isFeatureEnabled("Agents")) {
                            val agentExtension = at.rocworks.agents.AgentExtension()
                            val agentDeploymentOptions = DeploymentOptions().setConfig(configJson)
                            vertx.deployVerticle(agentExtension, agentDeploymentOptions)
                                .recover { error ->
                                    // Non-fatal: agents are optional
                                    logger.warning("AgentExtension not started: ${error.message}")
                                    Future.succeededFuture<String>()
                                }
                        } else {
                            logger.fine("Agents extension disabled by Features config")
                            Future.succeededFuture()
                        }
                    }
                    .compose {
                        // Start GraphQL server after all other components are ready
                        if (graphQLServer != null) {
                            logger.fine("Starting GraphQL server...")
                            // Run GraphQL server start asynchronously to avoid blocking
                            vertx.runOnContext {
                                try {
                                    graphQLServer.start()
                                } catch (e: Exception) {
                                    logger.severe("Failed to start GraphQL server: ${e.message}")
                                }
                            }
                        }
                        Future.succeededFuture<Unit>()
                    }
                    .onFailure {
                        logger.severe("Startup error: ${it.message}")
                        exitProcess(-1)
                    }
                    .onComplete {
                        logger.info("The Monster is ready.")
                    }
            }
        }.onFailure {
            logger.severe("Session store creation failed: ${it.message}")
        }
    }

    private fun getMessageBus(vertx: Vertx): Pair<IMessageBus, Future<String>> {
        val kafka = configJson.getJsonObject("Kafka", JsonObject())
        val kafkaServers = kafka.getString("Servers", "")
        val kafkaConfig = kafka.getJsonObject("Config")

        val kafkaBus = kafka.getJsonObject("Bus", JsonObject())
        val kafkaBusEnabled = kafkaBus.getBoolean("Enabled", false)
        val kafkaBusTopic = kafkaBus.getString("Topic", "monster")

        return if (!kafkaBusEnabled) {
            val bus = MessageBusVertx()
            val busReady = vertx.deployVerticle(bus)
            bus to busReady
        } else {
            val bus = MessageBusKafka(kafkaServers, kafkaBusTopic, kafkaConfig)
            val busReady = vertx.deployVerticle(bus)
            bus to busReady
        }
    }

    /**
     * Helper function to get store type with DefaultStoreType fallback
     */

    private fun getSessionStore(vertx: Vertx): Future<ISessionStoreAsync> {
        val promise = Promise.promise<ISessionStoreAsync>()
        val sessionStoreTypeStr = Monster.getSessionStoreType(configJson)
        val sessionStoreType = try {
            SessionStoreType.valueOf(sessionStoreTypeStr)
        } catch (e: IllegalArgumentException) {
            val supported = SessionStoreType.entries.joinToString(", ")
            logger.severe("Unsupported SessionStoreType '$sessionStoreTypeStr'. Supported values: $supported")
            exitProcess(1)
        }
        
        // For SQLite, ensure SQLiteVerticle is deployed first
        val sqliteReady = if (sessionStoreType == SessionStoreType.SQLITE) {
            ensureSQLiteVerticleDeployed(vertx)
        } else {
            Future.succeededFuture("N/A")
        }
        
        sqliteReady.compose { _ ->
            val store = when (sessionStoreType) {
                SessionStoreType.POSTGRES -> {
                    SessionStorePostgres(postgresConfig.url, postgresConfig.user, postgresConfig.pass, postgresConfig.schema)
                }
                SessionStoreType.MONGODB -> {
                    SessionStoreMongoDB(mongoDbConfig.url, mongoDbConfig.database)
                }
                SessionStoreType.SQLITE -> {
                    val configDbPath = "${sqliteConfig.path}/monstermq.db"
                    SessionStoreSQLite(configDbPath)
                }
            }
            val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
            vertx.deployVerticle(store, options).compose { _ ->
                val async = SessionStoreAsync(store)
                vertx.deployVerticle(async).map { async }
            }
        }.onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                promise.fail(result.cause())
            }
        }

        return promise.future()
    }

    private fun getQueueStore(vertx: Vertx): Future<IQueueStoreAsync> {
        val promise = Promise.promise<IQueueStoreAsync>()
        val queueStoreTypeStr = Monster.getQueueStoreType(configJson)
        val queueStoreType = try {
            QueueStoreType.valueOf(queueStoreTypeStr)
        } catch (e: IllegalArgumentException) {
            val supported = QueueStoreType.entries.joinToString(", ")
            logger.severe("Unsupported QueueStoreType '$queueStoreTypeStr'. Supported values: $supported")
            exitProcess(1)
        }

        // For SQLite, ensure SQLiteVerticle is deployed first
        val sqliteReady = if (queueStoreType == QueueStoreType.SQLITE) {
            ensureSQLiteVerticleDeployed(vertx)
        } else {
            Future.succeededFuture("N/A")
        }

        sqliteReady.compose { _ ->
            val store: IQueueStoreSync = when (queueStoreType) {
                QueueStoreType.POSTGRES_V1 -> {
                    QueueStorePostgresV1(postgresConfig.url, postgresConfig.user, postgresConfig.pass, postgresConfig.schema)
                }
                QueueStoreType.POSTGRES, QueueStoreType.POSTGRES_V2 -> {
                    val vtSeconds = configJson.getInteger("QueueVisibilityTimeoutSeconds", 30)
                    QueueStorePostgresV2(postgresConfig.url, postgresConfig.user, postgresConfig.pass, postgresConfig.schema, vtSeconds)
                }
                QueueStoreType.MONGODB_V1 -> {
                    QueueStoreMongoDBV1(mongoDbConfig.url, mongoDbConfig.database)
                }
                QueueStoreType.MONGODB, QueueStoreType.MONGODB_V2 -> {
                    val vtSeconds = configJson.getInteger("QueueVisibilityTimeoutSeconds", 30)
                    QueueStoreMongoDBV2(mongoDbConfig.url, mongoDbConfig.database, vtSeconds)
                }
                QueueStoreType.SQLITE_V1 -> {
                    val configDbPath = "${sqliteConfig.path}/monstermq.db"
                    QueueStoreSQLiteV1(configDbPath)
                }
                QueueStoreType.SQLITE, QueueStoreType.SQLITE_V2 -> {
                    val configDbPath = "${sqliteConfig.path}/monstermq.db"
                    val vtSeconds = configJson.getInteger("QueueVisibilityTimeoutSeconds", 30)
                    QueueStoreSQLiteV2(configDbPath, vtSeconds)
                }
            }
            val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
            vertx.deployVerticle(store as AbstractVerticle, options).compose { _ ->
                val async = QueueStoreAsync(store)
                vertx.deployVerticle(async).map { async as IQueueStoreAsync }
            }
        }.onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                promise.fail(result.cause())
            }
        }

        return promise.future()
    }

    private fun getMessageStore(vertx: Vertx,
                                name: String, storeType:
                                MessageStoreType): Pair<IMessageStore?, Future<Void>> {
        val promise = Promise.promise<Void>()
        val store = when (storeType) {
            MessageStoreType.NONE -> {
                promise.complete()
                null
            }
            MessageStoreType.MEMORY -> {
                val store = MessageStoreMemory(name)
                vertx.deployVerticle(store).onSuccess { promise.complete() }.onFailure { promise.fail(it) }
                store
            }
            MessageStoreType.POSTGRES -> {
                val store = MessageStorePostgres(name, postgresConfig.url, postgresConfig.user, postgresConfig.pass, postgresConfig.schema)
                val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onSuccess { promise.complete() }.onFailure { promise.fail(it) }
                store
            }
            MessageStoreType.CRATEDB -> {
                val store = MessageStoreCrateDB(name, crateDbConfig.url, crateDbConfig.user, crateDbConfig.pass)
                val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onSuccess { promise.complete() }.onFailure { promise.fail(it) }
                store
            }
            MessageStoreType.HAZELCAST -> {
                val clusterManager = this.clusterManager
                if (clusterManager != null) {
                    val store = MessageStoreHazelcast(name, clusterManager.hazelcastInstance)
                    vertx.deployVerticle(store).onSuccess { promise.complete() }.onFailure { promise.fail(it) }
                    store
                } else {
                    logger.severe("Cannot create Hazelcast message store with this cluster manager.")
                    exitProcess(-1)
                }
            }
            MessageStoreType.MONGODB -> {
                val store = MessageStoreMongoDB(name, mongoDbConfig.url, mongoDbConfig.database)
                val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onSuccess { promise.complete() }.onFailure { promise.fail(it) }
                store
            }
            MessageStoreType.SQLITE -> {
                val configDbPath = "${sqliteConfig.path}/monstermq.db"
                val store = MessageStoreSQLite(name, configDbPath)
                val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                ensureSQLiteVerticleDeployed(vertx).compose { _ ->
                    vertx.deployVerticle(store, options)
                }.onSuccess { promise.complete() }.onFailure { promise.fail(it) }
                store
            }
        }
        return store to promise.future()
    }

    private fun validateSQLiteDirectory(configJson: JsonObject) {
        val sessionStoreType = configJson.getString("SessionStoreType")
        val retainedStoreType = configJson.getString("RetainedStoreType")
        val archiveGroups = configJson.getJsonArray("ArchiveGroups")

        // Check if SQLite is used anywhere
        val sqliteUsed = sessionStoreType == "SQLITE" ||
                        retainedStoreType == "SQLITE" ||
                        (archiveGroups?.any { group ->
                            val g = group as JsonObject
                            g.getString("LastValType") == "SQLITE" ||
                            g.getString("ArchiveType") == "SQLITE"
                        } ?: false)

        if (sqliteUsed) {
            val sqliteDir = File(sqliteConfig.path)
            if (!sqliteDir.exists()) {
                logger.severe("SQLite directory '${sqliteConfig.path}' does not exist. Please create the directory or update the SQLite.Path configuration.")
                exitProcess(1)
            }
            if (!sqliteDir.isDirectory()) {
                logger.severe("SQLite path '${sqliteConfig.path}' is not a directory. Please ensure it points to a valid directory.")
                exitProcess(1)
            }
            if (!sqliteDir.canWrite()) {
                logger.severe("SQLite directory '${sqliteConfig.path}' is not writable. Please check permissions.")
                exitProcess(1)
            }
            logger.fine("SQLite directory validated: '${sqliteConfig.path}'")
        }
    }

}
