package at.rocworks

//import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager
//import io.vertx.ext.cluster.infinispan.InfinispanClusterManager
//import io.vertx.spi.cluster.ignite.IgniteClusterManager
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager

import at.rocworks.data.*
import at.rocworks.shared.RetainedMessages
import at.rocworks.shared.SubscriptionTable
import at.rocworks.stores.IMessageStore
import at.rocworks.stores.MessageStoreHazelcast
import at.rocworks.stores.MessageStore
import at.rocworks.stores.MessageStorePostgres
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.VertxBuilder
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

    args.indexOf("-log").let {
        if (it != -1) {
            val level = Level.parse(args[it + 1])
            println("Log Level [$level]")
            Const.DEBUG_LEVEL = level
        }
    }

    logger.info("Cluster: $useCluster Port: $usePort SSL: $useSsl Websockets: $useWs Kafka: $kafkaServers")

    fun startMonster(vertx: Vertx, retainedStore: IMessageStore) {
        vertx.eventBus().registerDefaultCodec(MqttMessage::class.java, MqttMessageCodec())
        vertx.eventBus().registerDefaultCodec(MqttSubscription::class.java, MqttSubscriptionCodec())

        val subscriptionTable = SubscriptionTable()
        val retainedMessages = RetainedMessages(retainedStore)

        val distributors = mutableListOf<Distributor>()
        val servers = mutableListOf<MqttServer>()

        repeat(1) {
            val distributor = if (kafkaServers.isNotBlank())
                DistributorKafka(subscriptionTable, retainedMessages, kafkaServers, kafkaTopic)
            else DistributorVertx(subscriptionTable, retainedMessages)
            distributors.add(distributor)

            servers.addAll(listOfNotNull(
                if (useTcp) MqttServer(usePort, useSsl, false, distributor) else null,
                if (useWs) MqttServer(usePort, useSsl, true, distributor) else null,
            ))
        }

        Future.succeededFuture<String>()
            .compose { vertx.deployVerticle(subscriptionTable) }
            .compose { vertx.deployVerticle(retainedMessages) }
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
        //val retained = MessageStore("Retained-Store")
        val retained = MessageStorePostgres("Retained-Store", "jdbc:postgresql://linux0:5432/postgres", "system", "manager")
        vertx.deployVerticle(retained).onComplete {
            startMonster(vertx, retained)
        }
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
                val hz = clusterManager.hazelcastInstance
                val retained = MessageStoreHazelcast("Retained-Store", hz)
                vertx.deployVerticle(retained).onComplete {
                    startMonster(vertx, retained)
                }
            } else {
                logger.severe("Vertx building failed: ${res.cause()}")
            }
        }
    }

    val builder = Vertx.builder()
    if (useCluster) clusterSetup(builder)
    else localSetup(builder)
}

