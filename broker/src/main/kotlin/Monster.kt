package at.rocworks

import at.rocworks.bus.IMessageBus
import at.rocworks.bus.MessageBusKafka
import at.rocworks.bus.MessageBusVertx
import at.rocworks.data.BrokerMessage
import at.rocworks.data.BrokerMessageCodec
import at.rocworks.data.MqttSubscription
import at.rocworks.data.MqttSubscriptionCodec
import at.rocworks.extensions.graphql.GraphQLServer
import at.rocworks.extensions.McpServer
import at.rocworks.extensions.SparkplugExtension
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
import at.rocworks.devices.plc4x.Plc4xExtension
import at.rocworks.devices.neo4j.Neo4jExtension
import at.rocworks.stores.DeviceConfigStoreFactory
import at.rocworks.flowengine.FlowEngineExtension
import at.rocworks.logging.MqttLogHandler
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
    private val logger: Logger = Logger.getLogger("Monster")

    private val isClustered = args.find { it == "-cluster" } != null

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
        private val logger = Logger.getLogger("Monster")

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
                logger.info("Using DefaultStoreType '$defaultStoreType' for $storeTypeKey")
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

        fun getSparkplugExtension(): SparkplugExtension? {
            return getInstance().configJson.getJsonObject("SparkplugMetricExpansion", JsonObject())?.let {
                if (it.getBoolean("Enabled", false)) SparkplugExtension(it)
                else null
            }
        }

        fun setSessionHandler(handler: SessionHandler) {
            getInstance().sessionHandler = handler
        }

        fun getSessionHandler(): SessionHandler? {
            return getInstance().sessionHandler
        }

        fun getMessageBus(): IMessageBus? {
            return getInstance().messageBus
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
        private var subscriptionQueueSize: Int = 50_000
        @JvmStatic
        fun getSubscriptionQueueSize(): Int = subscriptionQueueSize

        @Volatile
        private var messageQueueSize: Int = 50_000
        @JvmStatic
        fun getMessageQueueSize(): Int = messageQueueSize

        private fun ensureSQLiteVerticleDeployed(vertx: Vertx): Future<String> {
            return if (sqliteVerticleDeploymentId == null) {
                val promise = Promise.promise<String>()
                // Pass SQLite configuration to the verticle
                val sqliteVerticleConfig = JsonObject().put("EnableWAL", singleton?.sqliteConfig?.enableWAL ?: true)
                val deploymentOptions = DeploymentOptions().setConfig(sqliteVerticleConfig)
                vertx.deployVerticle(SQLiteVerticle(), deploymentOptions).onComplete { result ->
                    if (result.succeeded()) {
                        sqliteVerticleDeploymentId = result.result()
                        Logger.getLogger("Monster").info("SQLiteVerticle deployed as singleton with ID: ${sqliteVerticleDeploymentId}")
                        promise.complete(result.result())
                    } else {
                        Logger.getLogger("Monster").severe("Failed to deploy SQLiteVerticle: ${result.cause()?.message}")
                        promise.fail(result.cause())
                    }
                }
                promise.future()
            } else {
                Logger.getLogger("Monster").fine("SQLiteVerticle already deployed with ID: $sqliteVerticleDeploymentId")
                Future.succeededFuture(sqliteVerticleDeploymentId)
            }
        }

    }

    init {
        Utils.initLogging()

        // Check for help argument first
        if (args.contains("-help") || args.contains("--help") || args.contains("-h")) {
            printHelp()
            exitProcess(0)
        }

        // Validate command-line arguments
        val validArguments = setOf("-cluster", "-config", "-archiveConfig", "-log", "-dashboardPath", "-help", "--help", "-h")
        var i = 0
        while (i < args.size) {
            val arg = args[i]
            if (arg.startsWith("-")) {
                if (!validArguments.contains(arg)) {
                    println("ERROR: Unknown argument: $arg")
                    println("Use -help to see available options")
                    exitProcess(1)
                }
                // Check if argument expects a value
                if (arg in setOf("-config", "-archiveConfig", "-log", "-dashboardPath")) {
                    if (i + 1 >= args.size || args[i + 1].startsWith("-")) {
                        println("ERROR: Argument $arg requires a value")
                        exitProcess(1)
                    }
                    i += 2 // Skip the argument and its value
                } else {
                    i += 1 // Skip just the argument
                }
            } else {
                // This shouldn't happen if arguments are properly paired
                println("ERROR: Unexpected value without argument: $arg")
                println("Use -help to see available options")
                exitProcess(1)
            }
        }

        if (singleton==null) singleton = this
        else throw Exception("Monster instance is already initialized.")

        // Config file
        val configFileIndex = args.indexOf("-config")
        configFile = if (configFileIndex != -1 && configFileIndex + 1 < args.size) {
            args[configFileIndex + 1]
        } else {
            System.getenv("GATEWAY_CONFIG") ?: "config.yaml"
        }

        // Archive config file (optional)
        val archiveConfigIndex = args.indexOf("-archiveConfig")
        archiveConfigFile = if (archiveConfigIndex != -1 && archiveConfigIndex + 1 < args.size) {
            args[archiveConfigIndex + 1]
        } else {
            null
        }

        // Dashboard path for development (optional)
        val dashboardPathIndex = args.indexOf("-dashboardPath")
        dashboardPath = if (dashboardPathIndex != -1 && dashboardPathIndex + 1 < args.size) {
            args[dashboardPathIndex + 1]
        } else {
            null
        }

        args.indexOf("-log").let {
            if (it != -1) {
                val level = Level.parse(args[it + 1])
                println("Log Level [$level]")
                Const.DEBUG_LEVEL = level
            }
        }

        logger.info("Cluster: ${isClustered()}")

        val builder = Vertx.builder()
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
  
  # Start with SQLite configuration and detailed logging  
  java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt -config config-sqlite.yaml -log FINEST

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
        logger.info("Monster config file: $configFile")
        return ConfigRetriever.create(
            vertx,
            ConfigRetrieverOptions().addStore(
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
                logger.info("Config: AllowRootWildcardSubscription=$allowRootWildcardSubscriptionFlag")

                // Read rate limiting configuration (default 0 = unlimited)
                maxPublishRate = try {
                    configJson.getInteger("MaxPublishRate", 0)
                } catch (e: Exception) {
                    logger.warning("Config: MaxPublishRate read failed: ${e.message}")
                    0
                }
                logger.info("Config: MaxPublishRate=$maxPublishRate msg/s" + if (maxPublishRate == 0) " (unlimited)" else "")

                maxSubscribeRate = try {
                    configJson.getInteger("MaxSubscribeRate", 0)
                } catch (e: Exception) {
                    logger.warning("Config: MaxSubscribeRate read failed: ${e.message}")
                    0
                }
                logger.info("Config: MaxSubscribeRate=$maxSubscribeRate msg/s" + if (maxSubscribeRate == 0) " (unlimited)" else "")

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
                logger.info("Config: Queue sizes - Subscription=$subscriptionQueueSize, Message=$messageQueueSize")

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
                logger.info("Config loaded synchronously for cluster setup")
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

        logger.info("Cluster setup with node name: ${this.nodeName}")

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

        val maxMessageSize = configJson.getInteger("MaxMessageSizeKb", 8) * 1024
        val queuedMessagesEnabled = configJson.getBoolean("QueuedMessagesEnabled", true)
        logger.info("TCP [$useTcp] WS [$useWs] TCPS [$useTcpSsl] WSS [$useWsSsl] QME [$queuedMessagesEnabled]")

        val retainedStoreType = MessageStoreType.valueOf(Monster.getRetainedStoreType(configJson))
        logger.info("RetainedMessageStoreType [${retainedStoreType}]")

        vertx.eventBus().registerDefaultCodec(BrokerMessage::class.java, BrokerMessageCodec())
        vertx.eventBus().registerDefaultCodec(MqttSubscription::class.java, MqttSubscriptionCodec())

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
                logger.info("Initialization of bus and archive groups completed.")
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
                val mcpArchiveGroup = archiveGroups.find { it.name == Const.MCP_ARCHIVE_GROUP }
                
                val lastValStore = mcpArchiveGroup?.lastValStore
                val archiveStore = mcpArchiveGroup?.archiveStore
                val mcpServer = if (mcpEnabled && mcpArchiveGroup != null &&
                    retainedStore is IMessageStoreExtended &&
                    (lastValStore != null && lastValStore is IMessageStoreExtended) &&
                    (archiveStore != null && archiveStore is IMessageArchiveExtended)) {
                    logger.info("Starting MCP server on port $mcpPort")
                    McpServer("0.0.0.0", mcpPort, retainedStore, lastValStore, archiveStore)
                } else {
                    if (!mcpEnabled) {
                        logger.info("MCP server is disabled in configuration")
                    } else {
                        logger.warning("MCP archive group is not defined or is not an extended archive group. MCP server will not start.")
                    }
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
                        logger.info("Starting Metrics Store: ${store.getName()} (${store.getType()}) with ${collectionInterval}s collection interval, ${retentionHours}h retention")
                        val collector = MetricsHandler(sessionHandler, store, messageBus, messageHandler, collectionInterval, retentionHours)
                        store to collector
                    } catch (e: Exception) {
                        logger.warning("Failed to create metrics store: ${e.message}")
                        e.printStackTrace()
                        null to null
                    }
                } else {
                    logger.info("Metrics collection is disabled in configuration")
                    null to null
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
                        dashboardPath
                    )
                } else {
                    logger.info("GraphQL server is disabled in configuration")
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
                        logger.info("API Service is disabled in configuration")
                    } else if (graphQLServer == null) {
                        logger.info("GraphQL server is disabled, API Service will not start")
                    }
                    null
                }

                // MQTT Servers
                val servers = listOfNotNull(
                    if (useTcp>0) MqttServer(useTcp, false, false, maxMessageSize, sessionHandler, userManager) else null,
                    if (useWs>0) MqttServer(useWs, false, true, maxMessageSize, sessionHandler, userManager) else null,
                    if (useTcpSsl>0) MqttServer(useTcpSsl, true, false, maxMessageSize, sessionHandler, userManager) else null,
                    if (useWsSsl>0) MqttServer(useWsSsl, true, true, maxMessageSize, sessionHandler, userManager) else null,
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
                                logger.info("Installing log handler for system-wide log capture (fixed at INFO level)...")
                                val mqttLogHandler = MqttLogHandler.install()

                                // Set the log level to INFO (fixed)
                                mqttLogHandler.level = Level.INFO
                                logger.info("Log handler installed successfully at INFO level (MQTT: $mqttLoggingEnabled, Memory: $memoryLoggingEnabled)")
                            } catch (e: Exception) {
                                logger.severe("Failed to install log handler: ${e.message}")
                                throw e
                            }
                        } else {
                            logger.info("Both MQTT and Memory logging are disabled")
                        }

                        Future.succeededFuture<String>()
                    }
                    .compose {
                        // Deploy SyslogVerticle (independent of MQTT setting)
                        val loggingConfig = configJson.getJsonObject("Logging", JsonObject())
                        val memoryConfig = loggingConfig.getJsonObject("Memory", JsonObject())
                        val memoryEnabled = memoryConfig.getBoolean("Enabled", false)
                        val memoryEntries = memoryConfig.getInteger("Entries", 1000)

                        if (memoryEnabled) {
                            logger.info("Deploying SyslogVerticle with memoryEntries=$memoryEntries")
                            val syslogVerticle = SyslogVerticle(true, memoryEntries, messageHandler)
                            vertx.deployVerticle(syslogVerticle)
                        } else {
                            logger.info("In-memory syslog storage is disabled")
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
                        // WinCC OA Client Extension
                        val winCCOaExtension = WinCCOaExtension()
                        val winCCOaDeploymentOptions = DeploymentOptions().setConfig(configJson)
                        vertx.deployVerticle(winCCOaExtension, winCCOaDeploymentOptions)
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
                            logger.info("Deploying API Service...")
                            vertx.deployVerticle(apiService)
                        } else {
                            Future.succeededFuture<String>()
                        }
                    }
                    .compose {
                        // Start GraphQL server after all other components are ready
                        if (graphQLServer != null) {
                            logger.info("Starting GraphQL server...")
                            // Run GraphQL server start asynchronously to avoid blocking
                            vertx.runOnContext {
                                try {
                                    graphQLServer.start()
                                    logger.info("GraphQL server started successfully")
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
            logger.info("SQLite directory validated: '${sqliteConfig.path}'")
        }
    }

}