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

        val retainedMessageStoreType = MessageStoreType.valueOf(
            config.getString("RetainedMessageStoreType", "MEMORY")
        )

        val sessionStoreType = SessionStoreType.valueOf(
            config.getString("SessionStoreType", "MEMORY")
        )

        val postgres = config.getJsonObject("Postgres", JsonObject())

        val postgresUrl = postgres.getString("Url", "jdbc:postgresql://localhost:5432/postgres")
        val postgresUser = postgres.getString("User", "system")
        val postgresPass = postgres.getString("Pass", "manager")

        logger.info("Port [$usePort] SSL [$useSsl] WS [$useWs] TCP [$useTcp]")
        logger.info("RetainedMessageStoreType [$retainedMessageStoreType]")
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

        fun getSessionStore(vertx: Vertx): ISessionStore
                = when (sessionStoreType) {
            SessionStoreType.MEMORY -> {
                val table = SessionStoreAsyncMap()
                vertx.deployVerticle(table)
                table
            }
            SessionStoreType.POSTGRES -> {
                val table = SessionStorePostgres(postgresUrl, postgresUser, postgresPass)
                val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(table, options)
                table
            }
        }

        fun getMessageStore(vertx: Vertx, name: String, clusterManager: ClusterManager?): IMessageStore
                = when (retainedMessageStoreType) {
            MessageStoreType.MEMORY -> {
                val store = MessageStoreMemory(name)
                vertx.deployVerticle(store)
                store
            }
            MessageStoreType.POSTGRES -> {
                val store = MessageStorePostgres(
                    name = name,
                    url = postgresUrl,
                    username = postgresUser,
                    password = postgresPass
                )
                val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options)
                store
            }
            MessageStoreType.HAZELCAST -> {
                if (clusterManager is HazelcastClusterManager) {
                    val store = MessageStoreHazelcast(name, clusterManager.hazelcastInstance)
                    vertx.deployVerticle(store)
                    store
                } else {
                    logger.severe("Cannot create Hazelcast message store with this cluster manager.")
                    exitProcess(-1)
                }
            }
        }

        vertx.eventBus().registerDefaultCodec(MqttMessage::class.java, MqttMessageCodec())
        vertx.eventBus().registerDefaultCodec(MqttSubscription::class.java, MqttSubscriptionCodec())

        val subscriptionStore = getSessionStore(vertx)
        val sessionHandler = SessionHandler(subscriptionStore)

        val messageStore = getMessageStore(vertx, "Retained", clusterManager)
        val messageHandler = MessageHandler(messageStore)

        val distributors = mutableListOf<Distributor>()
        val servers = mutableListOf<MqttServer>()

        repeat(1) {
            getDistributor(sessionHandler, messageHandler).let { distributor ->
                distributors.add(distributor)
                servers.addAll(listOfNotNull(
                    if (useTcp) MqttServer(usePort, useSsl, false, distributor) else null,
                    if (useWs) MqttServer(usePort, useSsl, true, distributor) else null,
                ))
            }
        }

        Future.succeededFuture<String>()
            .compose { vertx.deployVerticle(messageHandler) }
            .compose { vertx.deployVerticle(sessionHandler) }
            .compose { Future.all(distributors.map { vertx.deployVerticle(it) }) }
            .compose { Future.all(servers.map { vertx.deployVerticle(it) }) }
            .onFailure {
                logger.severe("Startup error: ${it.message}")
                exitProcess(-1)
            }
            .onComplete {
                logger.info("The Monster is ready.")
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

