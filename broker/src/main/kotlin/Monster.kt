package at.rocworks

import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttMessageCodec
import at.rocworks.data.MqttSubscription
import at.rocworks.data.MqttSubscriptionCodec
import at.rocworks.extensions.SparkplugExtension
import at.rocworks.handlers.*
import at.rocworks.handlers.MessageHandler
import at.rocworks.stores.*
import io.vertx.config.ConfigRetriever
import io.vertx.config.ConfigRetrieverOptions
import io.vertx.config.ConfigStoreOptions
import io.vertx.core.*
import io.vertx.core.impl.VertxInternal
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.spi.cluster.ClusterManager
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.system.exitProcess

class Monster(args: Array<String>) {
    private val logger: Logger = Logger.getLogger("Monster")

    private val isClustered = args.find { it == "-cluster" } != null

    private val configFile: String
    private var configJson: JsonObject = JsonObject()

    //private var sessionHandler: SessionHandler? = null

    private val postgresConfig = object {
        var url: String = ""
        var user: String = ""
        var pass: String = ""
    }

    private val crateDbConfig = object {
        var url: String = ""
        var user: String = ""
        var pass: String = ""
    }

    companion object {
        private var singleton: Monster? = null

        private fun getInstance(): Monster = if (singleton==null) throw Exception("Monster instance is not initialized.") else singleton!!

        fun isClustered() = getInstance().isClustered
        fun isNotClustered() = !isClustered()

        //fun getSessionHandler() = getInstance().sessionHandler ?: throw Exception("SessionHandler is not initialized.")

        fun getSparkplugExtension(): SparkplugExtension? {
            return getInstance().configJson.getJsonObject("SparkplugMetricExpansion", JsonObject())?.let {
                if (it.getBoolean("Enabled", false)) SparkplugExtension(it)
                else null
            }
        }
    }

    init {
        Utils.initLogging()

        if (singleton==null) singleton = this
        else throw Exception("Monster instance is already initialized.")

        // Config file
        val configFileIndex = args.indexOf("-config")
        configFile = if (configFileIndex != -1 && configFileIndex + 1 < args.size) {
            args[configFileIndex + 1]
        } else {
            System.getenv("GATEWAY_CONFIG") ?: "config.yaml"
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
                }
                configJson.getJsonObject("CrateDB", JsonObject()).let { crate ->
                    crateDbConfig.url = crate.getString("Url", "jdbc:postgresql://localhost:5432/doc")
                    crateDbConfig.user = crate.getString("User", "crate")
                    crateDbConfig.pass = crate.getString("Pass", "")
                }
                startMonster(vertx)
            } else {
                logger.severe("Config loading failed: ${it.cause()}")
            }
        }
    }

    private fun localSetup(builder: VertxBuilder) {
        val vertx = builder.build()
        getConfigAndStart(vertx)
    }

    private fun clusterSetup(builder: VertxBuilder) {
        //val hazelcastConfig = ConfigUtil.loadConfig()
        //hazelcastConfig.setClusterName("MonsterMQ")
        val clusterManager = HazelcastClusterManager()

        //val clusterManager = ZookeeperClusterManager()
        //val clusterManager = InfinispanClusterManager()
        //val clusterManager = IgniteClusterManager();

        builder.withClusterManager(clusterManager)
        builder.buildClustered().onComplete { res: AsyncResult<Vertx?> ->
            if (res.succeeded() && res.result() != null) {
                val vertx = res.result()!!
                getConfigAndStart(vertx)
            } else {
                logger.severe("Vertx building failed: ${res.cause()}")
            }
        }
    }

    private fun startMonster(vertx: Vertx) {
        val usePort = configJson.getInteger("Port", 1883)
        val useSsl = configJson.getBoolean("SSL", false)
        val useWs = configJson.getBoolean("WS", false)
        val useTcp = configJson.getBoolean("TCP", true)
        logger.info("Port [$usePort] SSL [$useSsl] WS [$useWs] TCP [$useTcp]")

        val retainedStoreType = MessageStoreType.valueOf(configJson.getString("RetainedStoreType", "MEMORY"))
        logger.info("RetainedMessageStoreType [${retainedStoreType}]")

        vertx.eventBus().registerDefaultCodec(MqttMessage::class.java, MqttMessageCodec())
        vertx.eventBus().registerDefaultCodec(MqttSubscription::class.java, MqttSubscriptionCodec())

        getSessionStore(vertx).onSuccess { sessionStore ->
            // Retained messages
            val (retainedStore, retainedReady) = getMessageStore(vertx, "retainedmessages", retainedStoreType)

            // Archive groups
            val archiveGroups = getArchiveGroups(vertx)

            // Wait for all stores to be ready
            val futures = Future.succeededFuture<Unit>().compose { retainedReady }
            archiveGroups.forEach { archiveGroup ->
                futures.compose { archiveGroup.lastValReady }
                futures.compose { archiveGroup.archiveReady }
            }
            futures.onFailure {
                logger.severe("Initialization of archive groups failed: ${it.message}")
                exitProcess(-1)
            }.onComplete {
                // Session handler
                SessionHandler(sessionStore).let { sessionHandler ->
                    // Message handler
                    val messageHandler = MessageHandler(retainedStore!!, archiveGroups)

                    // Distributor
                    val eventHandler = getEventHandler(sessionHandler, messageHandler)

                    // Health handler
                    val healthHandler = HealthHandler(sessionHandler)

                    // MQTT Servers
                    val servers = listOfNotNull(
                        if (useTcp) MqttServer(usePort, useSsl, false, eventHandler, sessionHandler) else null,
                        if (useWs) MqttServer(usePort, useSsl, true, eventHandler, sessionHandler) else null
                    )

                    // Deploy all verticles
                    Future.succeededFuture<String>()
                        .compose { vertx.deployVerticle(sessionHandler) }
                        .compose { vertx.deployVerticle(messageHandler) }
                        .compose { vertx.deployVerticle(eventHandler) }
                        .compose { vertx.deployVerticle(healthHandler) }
                        .compose { Future.all(servers.map { vertx.deployVerticle(it) }) }
                        .onFailure {
                            logger.severe("Startup error: ${it.message}")
                            exitProcess(-1)
                        }
                        .onComplete {
                            logger.info("The Monster is ready.")
                        }
                }
            }
        }.onFailure {
            logger.severe("Session store creation failed: ${it.message}")
        }
    }

    private fun getArchiveGroups(vertx: Vertx)
    = configJson.getJsonArray("ArchiveGroups", JsonArray())
        .filterIsInstance<JsonObject>().filter { it.getBoolean("Enabled") }.map { c ->
            val name = c.getString("Name", "ArchiveGroup")
            val topicFilter = c.getJsonArray("TopicFilter", JsonArray()).toList().map { it as String }
            val retainedOnly = c.getBoolean("RetainedOnly", false)

            val lastValType = MessageStoreType.valueOf(c.getString("LastValType", "NONE"))
            val archiveType = MessageArchiveType.valueOf(c.getString("ArchiveType", "NONE"))

            val (lastValStore, lastValReady) = getMessageStore(vertx, name + "lastval", lastValType)
            val (archiveStore, archiveReady) = getMessageArchive(vertx, name + "archive", archiveType)

            ArchiveGroup(
                name,
                topicFilter, retainedOnly,
                lastValStore, lastValReady,
                archiveStore, archiveReady
            )
        }

    private fun getEventHandler(sessionHandler: SessionHandler, messageHandler: MessageHandler): EventHandler {
        val kafka = configJson.getJsonObject("Kafka", JsonObject())
        val kafkaEnabled = kafka.getBoolean("Enabled", false)
        val distributor = if (!kafkaEnabled) EventHandlerVertx(sessionHandler, messageHandler)
        else {
            val kafkaServers = kafka.getString("Servers", "")
            val kafkaTopic = kafka.getString("Topic", "monster")
            EventHandlerKafka(sessionHandler, messageHandler, kafkaServers, kafkaTopic)
        }
        return distributor
    }

    private fun getSessionStore(vertx: Vertx): Future<ISessionStore> {
        val promise = Promise.promise<ISessionStore>()
        val sessionStoreType = SessionStoreType.valueOf(
            configJson.getString("SessionStoreType", "MEMORY")
        )
        when (sessionStoreType) {
            SessionStoreType.POSTGRES -> {
                val store = SessionStorePostgres(postgresConfig.url, postgresConfig.user, postgresConfig.pass)
                val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onSuccess { promise.complete(store) }.onFailure { promise.fail(it) }
                store to promise.future()
            }
            SessionStoreType.CRATEDB -> {
                val store = SessionStoreCrateDB(crateDbConfig.url, crateDbConfig.user, crateDbConfig.pass)
                val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onSuccess { promise.complete(store) }.onFailure { promise.fail(it) }
                store to promise.future()
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
                val store = MessageStorePostgres(name, postgresConfig.url, postgresConfig.user, postgresConfig.pass)
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
                val clusterManager = (vertx as VertxInternal).clusterManager
                if (clusterManager is HazelcastClusterManager) {
                    val store = MessageStoreHazelcast(name, clusterManager.hazelcastInstance)
                    vertx.deployVerticle(store).onSuccess { promise.complete() }.onFailure { promise.fail(it) }
                    store
                } else {
                    logger.severe("Cannot create Hazelcast message store with this cluster manager.")
                    exitProcess(-1)
                }
            }
        }
        return store to promise.future()
    }

    private fun getMessageArchive(vertx: Vertx,
                                  name: String,
                                  storeType: MessageArchiveType): Pair<IMessageArchive?, Future<Void>> {
        val promise = Promise.promise<Void>()
        val archive = when (storeType) {
            MessageArchiveType.NONE -> {
                promise.complete()
                null
            }
            MessageArchiveType.POSTGRES -> {
                val store = MessageArchivePostgres(
                    name = name,
                    url = postgresConfig.url,
                    username = postgresConfig.user,
                    password = postgresConfig.pass
                )
                val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onSuccess { promise.complete() }.onFailure { promise.fail(it) }
                store
            }
            MessageArchiveType.CRATEDB -> {
                val store = MessageArchiveCrateDB(
                    name = name,
                    url = crateDbConfig.url,
                    username = crateDbConfig.user,
                    password = crateDbConfig.pass
                )
                val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onSuccess { promise.complete() }.onFailure { promise.fail(it) }
                store
            }
        }
        return archive to promise.future()
    }
}