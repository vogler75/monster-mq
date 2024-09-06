package at.rocworks

//import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager
//import io.vertx.ext.cluster.infinispan.InfinispanClusterManager
//import io.vertx.spi.cluster.ignite.IgniteClusterManager
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager

import at.rocworks.data.*
import at.rocworks.stores.*
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.VertxBuilder
import io.vertx.core.json.JsonObject
import io.vertx.core.spi.cluster.ClusterManager
import io.vertx.config.ConfigRetriever
import io.vertx.config.ConfigRetrieverOptions
import io.vertx.config.ConfigStoreOptions
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    Utils.initLogging()

    val logger = Logger.getLogger("Main")

    // Config file format
    val configFileName = if (args.isNotEmpty()) args[0] else System.getenv("GATEWAY_CONFIG") ?: "config.yaml"
    val useCluster = args.find { it == "-cluster" } != null


    args.indexOf("-log").let {
        if (it != -1) {
            val level = Level.parse(args[it + 1])
            println("Log Level [$level]")
            Const.DEBUG_LEVEL = level
        }
    }

    logger.info("Cluster: $useCluster")

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

        val retainedMessageStoreType =
            MessageStoreType.valueOf(config.getString("RetainedMessageStoreType", "POSTGRES"))
        val subscriptionStoreType =
            SubscriptionStoreType.valueOf(config.getString("SubscriptionStoreType", "POSTGRES"))

        val postgres = config.getJsonObject("Postgres", JsonObject())
        val postgresUrl = postgres.getString("Url", "jdbc:postgresql://192.168.1.3:5432/postgres")
        val postgresUser = postgres.getString("User", "system")
        val postgresPass = postgres.getString("Pass", "manager")

        logger.info("usePort: $usePort, useSsl: $useSsl, useWs: $useWs, useTcp: $useTcp")
        logger.info("retainedMessageStoreType: $retainedMessageStoreType, subscriptionStoreType: $subscriptionStoreType")
        logger.info("Postgres Url: $postgresUrl, User: $postgresUser, Pass: $postgresPass")

        fun getDistributor(
            subscriptionHandler: SubscriptionHandler,
            messageHandler: MessageHandler
        ): Distributor {
            val kafka = config.getJsonObject("Kafka", JsonObject())
            val distributor = if (kafka.isEmpty) DistributorVertx(subscriptionHandler, messageHandler)
            else {
                val kafkaServers = kafka.getString("Servers", "")
                val kafkaTopic = kafka.getString("Topic", "monster")
                DistributorKafka(subscriptionHandler, messageHandler, kafkaServers, kafkaTopic)
            }
            return distributor
        }

        fun getSubscriptionStore(vertx: Vertx, name: String): ISubscriptionStore
                = when (subscriptionStoreType) {
            SubscriptionStoreType.MEMORY -> {
                val table = SubscriptionStoreAsyncMap()
                vertx.deployVerticle(table)
                table
            }
            SubscriptionStoreType.POSTGRES -> {
                val table = SubscriptionStorePostgres(postgresUrl, postgresUser, postgresPass)
                vertx.deployVerticle(table)
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
                vertx.deployVerticle(store)
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

        val subscriptionStore = getSubscriptionStore(vertx, "SubscriptionStore")
        val subscriptionHandler = SubscriptionHandler(subscriptionStore)

        val messageStore = getMessageStore(vertx, "RetainedStore", clusterManager)
        val messageHandler = MessageHandler(messageStore)

        val distributors = mutableListOf<Distributor>()
        val servers = mutableListOf<MqttServer>()

        repeat(1) {
            getDistributor(subscriptionHandler, messageHandler).let { distributor ->
                distributors.add(distributor)
                servers.addAll(listOfNotNull(
                    if (useTcp) MqttServer(usePort, useSsl, false, distributor) else null,
                    if (useWs) MqttServer(usePort, useSsl, true, distributor) else null,
                ))
            }
        }

        Future.succeededFuture<String>()
            .compose { vertx.deployVerticle(messageHandler) }
            .compose { vertx.deployVerticle(subscriptionHandler) }
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
    if (useCluster) clusterSetup(builder)
    else localSetup(builder)
}

