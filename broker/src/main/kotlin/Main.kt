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
import io.vertx.core.spi.cluster.ClusterManager
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.system.exitProcess


fun main(args: Array<String>) {
    Utils.initLogging()

    val logger = Logger.getLogger("Main")

    val useCluster = args.find { it == "-cluster" } != null
    val usePort = args.indexOf("-port").let { if (it != -1) args.getOrNull(it+1)?.toIntOrNull()?:1883 else 1883 }
    val useSsl = args.indexOf("-ssl") != -1
    val useWs = args.indexOf("-ws") != -1
    val useTcp = args.indexOf("-tcp") != -1 || !useWs
    val kafkaServers = args.indexOf("-kafka").let { if (it != -1) args.getOrNull(it+1)?:"" else "" }
    val kafkaTopic = args.indexOf("-topic").let { if (it != -1) args.getOrNull(it+1)?:"" else "monster" }
    val retainedMessageStoreType = MessageStoreType.POSTGRES
    val subscriptionTableType = SubscriptionTableType.ASYNCMAP

    args.indexOf("-log").let {
        if (it != -1) {
            val level = Level.parse(args[it + 1])
            println("Log Level [$level]")
            Const.DEBUG_LEVEL = level
        }
    }

    logger.info("Cluster: $useCluster Port: $usePort SSL: $useSsl Websockets: $useWs Kafka: $kafkaServers")

    fun getSubscriptionTable(vertx: Vertx, name: String): ISubscriptionTable
    = when (subscriptionTableType) {
        SubscriptionTableType.ASYNCMAP -> {
            val table = SubscriptionTableAsyncMap()
            vertx.deployVerticle(table)
            table
        }
        SubscriptionTableType.POSTGRES -> {
            val table = SubscriptionTablePostgres(
                url = "jdbc:postgresql://192.168.1.30:5432/postgres",
                username = "system",
                password = "manager"
            )
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
                url = "jdbc:postgresql://192.168.1.30:5432/postgres",
                username = "system",
                password = "manager"
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

    fun getDistributor(
        subscriptionTable: ISubscriptionTable,
        retainedMessageHandler: RetainedMessageHandler
    ): Distributor {
        val distributor = if (kafkaServers.isNotBlank())
            DistributorKafka(subscriptionTable, retainedMessageHandler, kafkaServers, kafkaTopic)
        else DistributorVertx(subscriptionTable, retainedMessageHandler)
        return distributor
    }

    fun startMonster(vertx: Vertx, clusterManager: ClusterManager?) {
        vertx.eventBus().registerDefaultCodec(MqttMessage::class.java, MqttMessageCodec())
        vertx.eventBus().registerDefaultCodec(MqttSubscription::class.java, MqttSubscriptionCodec())

        val subscriptionTable = getSubscriptionTable(vertx, "SubscriptionTable")

        val retainedMessageStore = getMessageStore(vertx, "RetainedStore", clusterManager)
        val retainedMessageHandler = RetainedMessageHandler(retainedMessageStore)

        val distributors = mutableListOf<Distributor>()
        val servers = mutableListOf<MqttServer>()

        repeat(1) {
            getDistributor(subscriptionTable, retainedMessageHandler).let { distributor ->
                distributors.add(distributor)
                servers.addAll(listOfNotNull(
                    if (useTcp) MqttServer(usePort, useSsl, false, distributor) else null,
                    if (useWs) MqttServer(usePort, useSsl, true, distributor) else null,
                ))
            }
        }

        Future.succeededFuture<String>()
            .compose { vertx.deployVerticle(retainedMessageHandler) }
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

    fun localSetup(builder: VertxBuilder) {
        val vertx = builder.build()
        startMonster(vertx, null)
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
                startMonster(vertx, clusterManager)
            } else {
                logger.severe("Vertx building failed: ${res.cause()}")
            }
        }
    }

    val builder = Vertx.builder()
    if (useCluster) clusterSetup(builder)
    else localSetup(builder)
}

