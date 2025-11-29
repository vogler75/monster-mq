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
import at.rocworks.extensions.ApiService
import at.rocworks.handlers.*
import at.rocworks.handlers.MessageHandler
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.stores.*
import at.rocworks.stores.cratedb.MessageStoreCrateDB
import at.rocworks.stores.cratedb.SessionStoreCrateDB
import at.rocworks.stores.mongodb.MessageStoreMongoDB
import at.rocworks.stores.mongodb.SessionStoreMongoDB
import at.rocworks.stores.postgres.MessageStorePostgres
import at.rocworks.stores.postgres.SessionStorePostgres
import at.rocworks.stores.sqlite.MessageStoreSQLite
import at.rocworks.stores.sqlite.SessionStoreSQLite
import at.rocworks.stores.sqlite.SQLiteVerticle
import io.vertx.config.ConfigRetriever
import io.vertx.config.ConfigRetrieverOptions
import io.vertx.config.ConfigStoreOptions
import io.vertx.core.*
// VertxInternal removed in Vert.x 5 - using alternative approaches
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
    // Print working directory and user.dir property
    Monster(args)
}

class Monster(args: Array<String>) {
    private val logger: Logger = Utils.getLogger(this::class.java)

    private val isClustered = args.find { it == "-cluster" || it == "--cluster" } != null

    private val configFile: String
    private var configJson: JsonObject = JsonObject()
    private val archiveConfigFile: String?
    private val dashboardPath: String?
    var archiveHandler: ArchiveHandler? = null

    // Cluster manager reference for Vert.x 5 compatibility
    private var clusterManager: HazelcastClusterManager? = null
    private var nodeName: String = ""

    private var vertx: Vertx? = null
    private var sessionHandler: SessionHandler? = null
    private var messageBus: IMessageBus? = null

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
        fun getConfigStoreType(configJson: JsonObject): String = getStoreTypeWithDefault(configJson, "ConfigStoreType", "SQLITE")

        @JvmStatic
        fun getStoreType(configJson: JsonObject): String = getStoreTypeWithDefault(configJson, "StoreType", "SQLITE")

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

        fun getSessionHandler(): SessionHandler? {
            return getInstance().sessionHandler
        }

        fun getVertx(): Vertx? {
            return getInstance().vertx
        }

        @Volatile
        private var allowRootWildcardSubscriptionFlag: Boolean = true
        @JvmStatic
        fun allowRootWildcardSubscription(): Boolean = allowRootWildcardSubscriptionFlag

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
        // Check if we're running under JManager (MonsterOA)
        if (MonsterOA.getInstance() == null) {
            Utils.initLogging()
        }

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

        // Archive config file (optional)
        val archiveConfigIndex = Utils.getArgIndex(args, listOf("-archiveConfig", "--archiveConfig"))
        archiveConfigFile = if (archiveConfigIndex != -1 && archiveConfigIndex + 1 < args.size) {
            args[archiveConfigIndex + 1]
        } else {
            null
        }

        // Dashboard path for development (optional)
        val dashboardPathIndex = Utils.getArgIndex(args, listOf("-dashboardPath", "--dashboardPath"))
        dashboardPath = if (dashboardPathIndex != -1 && dashboardPathIndex + 1 < args.size) {
            args[dashboardPathIndex + 1]
        } else {
            null
        }

        Utils.getArgIndex(args, listOf("-log", "--log")).let {
            if (it != -1) {
                val level = Level.parse(args[it + 1])
                Const.DEBUG_LEVEL = level
            }
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

        logger.fine("Cluster: ${isClustered()}")

        val builder = Vertx.builder()
        // Apply worker pool size if specified via command line
        getWorkerPoolSize()?.let { poolSize ->
            val vertxOptions = VertxOptions()
                .setWorkerPoolSize(poolSize)
            builder.with(vertxOptions)
            logger.fine("Vertx worker thread pool size set to $poolSize (via -workerPoolSize argument)")
        }

        if (isClustered())
            clusterSetup(builder)
        else
            localSetup(builder)
    }

    private fun printHelp() {
        println("""
MonsterMQ - High-Performance MQTT Broker
Usage: java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt [OPTIONS]

OPTIONS:
  -help, --help, -h     Show this help message and exit
  
  -config <file>        Configuration file path (default: config.yaml)
                        Environment: GATEWAY_CONFIG

  -archiveConfig <file> Load ArchiveGroups from separate YAML file
                        If ConfigStoreType is set: imports to database
                        Otherwise: merges with main config in memory

  -cluster              Enable Hazelcast clustering mode for multi-node deployment

  -log <level>          Set logging level
                        Levels: ALL, FINEST, FINER, FINE, INFO, WARNING, SEVERE
                        Default: INFO

  -workerPoolSize <num> Vert.x worker thread pool size
                        Default: 2×CPU count (e.g., 16 on 8-core machine)
                        Increase for high-concurrency scenarios

  -dashboardPath <path> Serve dashboard files from filesystem path (development only)
                        Example: broker/src/main/resources/dashboard
                        Default: serve from classpath resources

EXAMPLES:
  # Start with default configuration
  java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt

  # Start with custom config file
  java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt -config myconfig.yaml

  # Start in cluster mode with debug logging
  java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt -cluster -log FINE

  # Start with custom worker thread pool size for high load
  java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt -workerPoolSize 64

  # Start with SQLite configuration and detailed logging
  java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt -config config-sqlite.yaml -log FINEST -workerPoolSize 128

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

        val useTcpSsl = configJson.getInteger("TCPS", 0)
        val useWsSsl = configJson.getInteger("WSS", 0)

        // Load TCP server configuration
        val tcpServerConfig = configJson.getJsonObject("MqttTcpServer", JsonObject())
        val maxMessageSize = tcpServerConfig.getInteger("MaxMessageSizeKb", 512) * 1024
        val tcpNoDelay = tcpServerConfig.getBoolean("NoDelay", true)
        val receiveBufferSize = tcpServerConfig.getInteger("ReceiveBufferSizeKb", 512) * 1024
        val sendBufferSize = tcpServerConfig.getInteger("SendBufferSizeKb", 512) * 1024

        val queuedMessagesEnabled = configJson.getBoolean("QueuedMessagesEnabled", true)
        logger.fine("TCP [$useTcp] WS [$useWs] TCPS [$useTcpSsl] WSS [$useWsSsl] QME [$queuedMessagesEnabled]")
        logger.fine("TCP Server Config: MaxMessageSize [${maxMessageSize / 1024}KB] NoDelay [$tcpNoDelay] ReceiveBufferSize [${receiveBufferSize / 1024}KB] SendBufferSize [${sendBufferSize / 1024}KB]")

        val retainedStoreType = MessageStoreType.valueOf(Monster.getRetainedStoreType(configJson))
        logger.fine("RetainedMessageStoreType [${retainedStoreType}]")

        vertx.eventBus().registerDefaultCodec(BrokerMessage::class.java, BrokerMessageCodec())
        vertx.eventBus().registerDefaultCodec(MqttSubscription::class.java, MqttSubscriptionCodec())
        vertx.eventBus().registerDefaultCodec(BulkClientMessage::class.java, BulkClientMessageCodec())
        vertx.eventBus().registerDefaultCodec(BulkNodeMessage::class.java, BulkNodeMessageCodec())

        getSessionStore(vertx).onSuccess { sessionStore ->
            // Message bus
            val (messageBus, messageBusReady) = getMessageBus(vertx)

            // Retained messages
            val (retainedStore, retainedReady) = getMessageStore(vertx, "RetainedMessages", retainedStoreType)

            // Archive groups
            val archiveHandler = ArchiveHandler(vertx, configJson, archiveConfigFile, isClustered)
            val archiveGroupsFuture = archiveHandler.initialize()

            // Wait for all stores to be ready
            Future.all<Any>(listOf(archiveGroupsFuture, retainedReady, messageBusReady) as List<Future<*>>).onFailure { it ->
                logger.severe("Initialization of bus or archive groups failed: ${it.message}")
                exitProcess(-1)
            }.onComplete {
                logger.fine("Initialization of bus and archive groups completed.")
                val archiveGroups = archiveGroupsFuture.result()

                // Message handler
                val messageHandler = MessageHandler(retainedStore!!, archiveGroups)

                // Session handler
                val sessionHandler = SessionHandler(sessionStore, messageBus, messageHandler, queuedMessagesEnabled)

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
                            }
                        }
                        store
                    } catch (e: Exception) {
                        logger.warning("Failed to create device config store: ${e.message}")
                        null
                    }
                } else {
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
                val mcpEnabled = mcpConfig.getBoolean("Enabled", true)
                val mcpPort = mcpConfig.getInteger("Port", 3000)
                val mcpServer = if (mcpEnabled) {
                    McpServer("0.0.0.0", mcpPort, retainedStore, archiveHandler)
                } else {
                    logger.fine("MCP server is disabled in configuration")
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

                // GenAI Provider
                val genAiConfig = configJson.getJsonObject("GenAI", JsonObject())
                val genAiProvider = try {
                    at.rocworks.genai.GenAiProviderFactory.create(vertx, genAiConfig).get()
                } catch (e: Exception) {
                    logger.warning("Failed to initialize GenAI provider: ${e.message}")
                    null
                }

                // GraphQL Server
                val graphQLConfig = configJson.getJsonObject("GraphQL", JsonObject())
                val graphQLEnabled = graphQLConfig.getBoolean("Enabled", true)
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
                        sessionHandler,
                        metricsStore,
                        this.archiveHandler,
                        dashboardPath,
                        deviceConfigStore,
                        genAiProvider
                    )
                } else {
                    logger.fine("GraphQL server is disabled in configuration")
                    null
                }

                // API Service (JSON-RPC 2.0 over MQTT)
                val apiEnabled = graphQLConfig.getBoolean("MqttApi", true)
                val apiService = if (apiEnabled && graphQLServer != null) {
                    val graphQLPort = graphQLConfig.getInteger("Port", 4000)
                    val graphQLPath = graphQLConfig.getString("Path", "/graphql")
                    ApiService(sessionHandler, "graphql", graphQLPort, graphQLPath)
                } else {
                    if (!apiEnabled) {
                        logger.fine("API Service is disabled in configuration")
                    } else if (graphQLServer == null) {
                        logger.fine("GraphQL server is disabled, API Service will not start")
                    }
                    null
                }

                // MQTT Servers
                val servers = listOfNotNull(
                    if (useTcp>0) MqttServer(useTcp, false, false, maxMessageSize, tcpNoDelay, receiveBufferSize, sendBufferSize, sessionHandler, userManager) else null,
                    if (useWs>0) MqttServer(useWs, false, true, maxMessageSize, tcpNoDelay, receiveBufferSize, sendBufferSize, sessionHandler, userManager) else null,
                    if (useTcpSsl>0) MqttServer(useTcpSsl, true, false, maxMessageSize, tcpNoDelay, receiveBufferSize, sendBufferSize, sessionHandler, userManager) else null,
                    if (useWsSsl>0) MqttServer(useWsSsl, true, true, maxMessageSize, tcpNoDelay, receiveBufferSize, sendBufferSize, sessionHandler, userManager) else null,
                    mcpServer
                )

                // Deploy all verticles
                Future.succeededFuture<String>()
                    .compose { vertx.deployVerticle(messageHandler) }
                    .compose { vertx.deployVerticle(sessionHandler) }
                    .compose {
                        // Configure logging (MQTT and Memory are independent)
                        val loggingConfig = configJson.getJsonObject("Logging", JsonObject())

                        // Check if either MQTT or Memory logging is enabled
                        val mqttConfig = loggingConfig.getJsonObject("Mqtt", JsonObject())
                        val mqttLoggingEnabled = mqttConfig.getBoolean("Enabled", false)
                        val memoryConfig = loggingConfig.getJsonObject("Memory", JsonObject())
                        val memoryLoggingEnabled = memoryConfig.getBoolean("Enabled", false)

                        // Install MqttLogHandler if EITHER MQTT or Memory logging is enabled
                        // (Memory logging depends on MqttLogHandler to capture logs)
                        if (mqttLoggingEnabled || memoryLoggingEnabled) {
                            try {
                                logger.fine("Installing log handler for system-wide log capture (level: ${Const.DEBUG_LEVEL})...")
                                val sysLogHandler = SysLogHandler.install()

                                // Set the log level to configured debug level
                                sysLogHandler.level = Const.DEBUG_LEVEL
                                logger.info("Log handler installed successfully at ${Const.DEBUG_LEVEL} level (MQTT: $mqttLoggingEnabled, Memory: $memoryLoggingEnabled)")
                            } catch (e: Exception) {
                                logger.severe("Failed to install log handler: ${e.message}")
                                throw e
                            }
                        } else {
                            logger.fine("Both MQTT and Memory logging are disabled")
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
                            val syslogVerticle = SyslogVerticle(true, memoryEntries, messageHandler)
                            vertx.deployVerticle(syslogVerticle)
                        } else {
                            logger.fine("In-memory syslog storage is disabled")
                            Future.succeededFuture<String>()
                        }
                    }
                    .compose { vertx.deployVerticle(userManager) }
                    .compose { vertx.deployVerticle(healthHandler) }
                    .compose {
                        val opcUaDeploymentOptions = DeploymentOptions().setConfig(configJson)
                        vertx.deployVerticle(opcUaExtension, opcUaDeploymentOptions)
                    }
                    .compose {
                        if (opcUaServerExtension != null) {
                            vertx.deployVerticle(opcUaServerExtension)
                        } else {
                            Future.succeededFuture<String>()
                        }
                    }
                    .compose {
                        val mqttClientDeploymentOptions = DeploymentOptions().setConfig(configJson)
                        vertx.deployVerticle(mqttClientExtension, mqttClientDeploymentOptions)
                    }
                    .compose {
                        // Kafka Subscriber Extension
                        val kafkaClientExtension = KafkaClientExtension()
                        val kafkaDeploymentOptions = DeploymentOptions().setConfig(configJson)
                        vertx.deployVerticle(kafkaClientExtension, kafkaDeploymentOptions)
                    }
                    .compose {
                        // WinCC OA Client Extension (GraphQL-based)
                        val winCCOaExtension = WinCCOaExtension()
                        val winCCOaDeploymentOptions = DeploymentOptions().setConfig(configJson)
                        vertx.deployVerticle(winCCOaExtension, winCCOaDeploymentOptions)
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
                        val winCCUaExtension = WinCCUaExtension()
                        val winCCUaDeploymentOptions = DeploymentOptions().setConfig(configJson)
                        vertx.deployVerticle(winCCUaExtension, winCCUaDeploymentOptions)
                    }
                    .compose {
                        // PLC4X Client Extension
                        val plc4xExtension = Plc4xExtension()
                        val plc4xDeploymentOptions = DeploymentOptions().setConfig(configJson)
                        vertx.deployVerticle(plc4xExtension, plc4xDeploymentOptions)
                    }
                    .compose {
                        // Neo4j Client Extension
                        val neo4jExtension = Neo4jExtension()
                        val neo4jDeploymentOptions = DeploymentOptions().setConfig(configJson)
                        vertx.deployVerticle(neo4jExtension, neo4jDeploymentOptions)
                    }
                    .compose {
                        // JDBC Logger Extension
                        val jdbcLoggerExtension = at.rocworks.logger.JDBCLoggerExtension()
                        val jdbcLoggerDeploymentOptions = DeploymentOptions().setConfig(configJson)
                        vertx.deployVerticle(jdbcLoggerExtension, jdbcLoggerDeploymentOptions)
                    }
                    .compose {
                        // SparkplugB Decoder Extension
                        val sparkplugBDecoderExtension = SparkplugBDecoderExtension()
                        val sparkplugBDecoderDeploymentOptions = DeploymentOptions().setConfig(configJson)
                        vertx.deployVerticle(sparkplugBDecoderExtension, sparkplugBDecoderDeploymentOptions)
                    }
                    .compose {
                        // Flow Engine Extension
                        val flowEngineExtension = FlowEngineExtension()
                        val flowEngineDeploymentOptions = DeploymentOptions().setConfig(configJson)
                        vertx.deployVerticle(flowEngineExtension, flowEngineDeploymentOptions)
                    }
                    .compose {
                        if (metricsCollector != null) {
                            vertx.deployVerticle(metricsCollector)
                        } else {
                            Future.succeededFuture<String>()
                        }
                    }
                    .compose { Future.all<String>(servers.map { vertx.deployVerticle(it) } as List<Future<String>>) }
                    .compose {
                        // Deploy API Service after other components are ready
                        if (apiService != null) {
                            logger.fine("Deploying API Service...")
                            vertx.deployVerticle(apiService)
                        } else {
                            Future.succeededFuture<String>()
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
        val sessionStoreType = SessionStoreType.valueOf(
            Monster.getSessionStoreType(configJson)
        )
        
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
                SessionStoreType.CRATEDB -> {
                    SessionStoreCrateDB(crateDbConfig.url, crateDbConfig.user, crateDbConfig.pass)
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