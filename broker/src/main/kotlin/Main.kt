package at.rocworks

//import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager
//import io.vertx.ext.cluster.infinispan.InfinispanClusterManager
//import io.vertx.spi.cluster.ignite.IgniteClusterManager

import at.rocworks.data.*
import at.rocworks.stores.*
import io.vertx.config.ConfigRetriever
import io.vertx.config.ConfigRetrieverOptions
import io.vertx.config.ConfigStoreOptions
import io.vertx.core.*
import io.vertx.core.json.JsonObject
import io.vertx.core.spi.cluster.ClusterManager
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    Utils.initLogging()

    val logger = Logger.getLogger("Main")

    // Clustering
    Config.setClustered(args.find { it == "-cluster" } != null)

    // Config file
    val configFileIndex = args.indexOf("-config")
    val configFileName = if (configFileIndex != -1 && configFileIndex + 1 < args.size) {
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

    logger.info("Cluster: ${Config.isClustered()}")

    fun retrieveConfig(vertx: Vertx): ConfigRetriever {
        logger.info("Monster config file: $configFileName")
        return ConfigRetriever.create(
            vertx,
            ConfigRetrieverOptions().addStore(
                ConfigStoreOptions()
                    .setType("file")
                    .setFormat("yaml")
                    .setConfig(JsonObject().put("path", configFileName))
            )
        )
    }

    fun startMonster(vertx: Vertx, config: JsonObject, clusterManager: ClusterManager?) {
        val usePort = config.getInteger("Port", 1883)
        val useSsl = config.getBoolean("SSL", false)
        val useWs = config.getBoolean("WS", false)
        val useTcp = config.getBoolean("TCP", true)

        val sessionStoreType = SessionStoreType.valueOf(
            config.getString("SessionStoreType", "MEMORY")
        )

        val retainedMessageStoreType = MessageStoreType.valueOf(config.getString("RetainedMessageStoreType", "MEMORY"))
        val lastValueMessageStoreType = MessageStoreType.valueOf(config.getString("LastValueMessageStoreType", "NONE"))

        val retainedMessageArchiveType = MessageArchiveType.valueOf(config.getString("RetainedMessageArchiveType", "NONE"))
        val lastValueMessageArchiveType = MessageArchiveType.valueOf(config.getString("LastValueMessageArchiveType", "NONE"))

        val postgres = config.getJsonObject("Postgres", JsonObject())

        val postgresUrl = postgres.getString("Url", "jdbc:postgresql://localhost:5432/postgres")
        val postgresUser = postgres.getString("User", "system")
        val postgresPass = postgres.getString("Pass", "manager")

        logger.info("Port [$usePort] SSL [$useSsl] WS [$useWs] TCP [$useTcp]")
        logger.info("RetainedMessageStoreType [$retainedMessageStoreType]")
        logger.info("LastValueMessageStoreType [$lastValueMessageStoreType]")
        logger.info("SessionStoreType [$sessionStoreType]")

        fun getDistributor(
            sessionHandler: SessionHandler,
            messageHandler: MessageHandler
        ): Distributor {
            val kafka = config.getJsonObject("Kafka", JsonObject())
            val distributor = if (kafka.isEmpty) DistributorVertx(sessionHandler, messageHandler)
            else {
                val kafkaServers = kafka.getString("Servers", "")
                val kafkaTopic = kafka.getString("Topic", "monster")
                DistributorKafka(sessionHandler, messageHandler, kafkaServers, kafkaTopic)
            }
            return distributor
        }

        fun getSessionStore(vertx: Vertx): Future<ISessionStore> {
            val promise = Promise.promise<ISessionStore>()
            when (sessionStoreType) {
                SessionStoreType.POSTGRES -> {
                    val store = SessionStorePostgres(postgresUrl, postgresUser, postgresPass)
                    val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                    vertx.deployVerticle(store, options).onSuccess { promise.complete(store) }.onFailure { promise.fail(it) }
                }
            }
            return promise.future()
        }

        fun getMessageStore(vertx: Vertx, name: String, storeType: MessageStoreType, clusterManager: ClusterManager?): Future<IMessageStore?> {
            val promise = Promise.promise<IMessageStore?>()
            when (storeType) {
                MessageStoreType.NONE -> null
                MessageStoreType.MEMORY -> {
                    val store = MessageStoreMemory(name)
                    vertx.deployVerticle(store).onSuccess { promise.complete(store) }.onFailure { promise.fail(it) }
                }
                MessageStoreType.POSTGRES -> {
                    val store = MessageStorePostgres(
                        name = name,
                        url = postgresUrl,
                        username = postgresUser,
                        password = postgresPass
                    )
                    val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                    vertx.deployVerticle(store, options).onSuccess { promise.complete(store) }.onFailure { promise.fail(it) }
                }
                MessageStoreType.HAZELCAST -> {
                    if (clusterManager is HazelcastClusterManager) {
                        val store = MessageStoreHazelcast(name, clusterManager.hazelcastInstance)
                        vertx.deployVerticle(store).onSuccess { promise.complete(store) }.onFailure { promise.fail(it) }
                    } else {
                        logger.severe("Cannot create Hazelcast message store with this cluster manager.")
                        exitProcess(-1)
                    }
                }
            }
            return promise.future()
        }

        fun getMessageArchive(vertx: Vertx, name: String, storeType: MessageArchiveType): Future<IMessageArchive?> {
            val promise = Promise.promise<IMessageArchive?>()
            when (storeType) {
                MessageArchiveType.NONE -> null
                MessageArchiveType.POSTGRES -> {
                    val store = MessageArchivePostgres(
                        name = name,
                        url = postgresUrl,
                        username = postgresUser,
                        password = postgresPass
                    )
                    val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                    vertx.deployVerticle(store, options).onSuccess { promise.complete(store) }.onFailure { promise.fail(it) }
                }
            }
            return promise.future()
        }

        vertx.eventBus().registerDefaultCodec(MqttMessage::class.java, MqttMessageCodec())
        vertx.eventBus().registerDefaultCodec(MqttSubscription::class.java, MqttSubscriptionCodec())

        getSessionStore(vertx).onSuccess { sessionStore ->
            val retainedStore = getMessageStore(vertx, "RetainedMessages", retainedMessageStoreType, clusterManager)
            val lastValueStore = getMessageStore(vertx, "LastValueMessages", lastValueMessageStoreType, clusterManager)

            val retainedArch = getMessageArchive(vertx, "RetainedArchive", retainedMessageArchiveType)
            val lastValueArch = getMessageArchive(vertx, "LastValueArchive", lastValueMessageArchiveType)

            Future.succeededFuture<Unit>()
                .compose { retainedStore }
                .compose { lastValueStore }
                .compose { retainedArch }
                .compose { lastValueArch }
                .onFailure {
                    logger.severe("Message store creation failed: ${it.message}")
                    exitProcess(-1)
                }
                .onComplete {
                    logger.info("Message stores are ready.")
                    val messageHandler = MessageHandler(
                        retainedStore.result()!!,
                        retainedArch.result(),
                        lastValueStore.result(),
                        lastValueArch.result()
                    )
                    val sessionHandler = SessionHandler(sessionStore)

                    val distributor = getDistributor(sessionHandler, messageHandler)
                    val servers = listOfNotNull(
                        if (useTcp) MqttServer(usePort, useSsl, false, distributor) else null,
                        if (useWs) MqttServer(usePort, useSsl, true, distributor) else null
                    )

                    Future.succeededFuture<String>()
                        .compose { vertx.deployVerticle(messageHandler) }
                        .compose { vertx.deployVerticle(sessionHandler) }
                        .compose { vertx.deployVerticle(distributor) }
                        .compose { Future.all(servers.map { vertx.deployVerticle(it) }) }
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

    fun getConfigAndStart(vertx: Vertx, clusterManager: ClusterManager?) {
        retrieveConfig(vertx).config.onComplete { it ->
            if (it.succeeded()) {
                startMonster(vertx, it.result(), clusterManager)
            } else {
                logger.severe("Config loading failed: ${it.cause()}")
            }
        }
    }

    fun localSetup(builder: VertxBuilder) {
        val vertx = builder.build()
        getConfigAndStart(vertx, null)
    }

    fun clusterSetup(builder: VertxBuilder) {
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
                getConfigAndStart(vertx, clusterManager)
            } else {
                logger.severe("Vertx building failed: ${res.cause()}")
            }
        }
    }

    val builder = Vertx.builder()
    if (Config.isClustered())
        clusterSetup(builder)
    else
        localSetup(builder)
}

