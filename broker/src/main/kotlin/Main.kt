package at.rocworks

//import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager
//import io.vertx.ext.cluster.infinispan.InfinispanClusterManager
//import io.vertx.spi.cluster.ignite.IgniteClusterManager

import at.rocworks.data.*
import at.rocworks.shared.RetainedMessages
import at.rocworks.shared.SubscriptionTable
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.VertxBuilder
import io.vertx.spi.cluster.hazelcast.ConfigUtil
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
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
    val useKafka = args.indexOf("-kafka").let { if (it != -1) args.getOrNull(it+1)?:"" else "" }
    args.indexOf("-log").let { if (it != -1) Const.DEBUG_LEVEL = Level.parse(args[it+1]) }

    logger.info("Cluster: $useCluster Port: $usePort SSL: $useSsl Websockets: $useWs Kafka: $useKafka")

    fun clusterSetup(builder: VertxBuilder, then: (vertx: Vertx)->Unit) {
        val hazelcastConfig = ConfigUtil.loadConfig()
        //hazelcastConfig.setClusterName("MonsterMQ")
        val clusterManager = HazelcastClusterManager(hazelcastConfig)

        //val clusterManager = ZookeeperClusterManager()
        //val clusterManager = InfinispanClusterManager()
        //val clusterManager = IgniteClusterManager();

        builder.withClusterManager(clusterManager)
        builder.buildClustered().onComplete { res: AsyncResult<Vertx?> ->
            if (res.succeeded() && res.result() != null) {
                then(res.result()!!)
            } else {
                logger.severe("Vertx building failed: ${res.cause()}")
            }
        }
    }


    fun startMonster(vertx: Vertx) {
        vertx.eventBus().registerDefaultCodec(MqttMessage::class.java, MqttMessageCodec())
        vertx.eventBus().registerDefaultCodec(MqttTopicName::class.java, MqttTopicNameCodec())
        vertx.eventBus().registerDefaultCodec(MqttSubscription::class.java, MqttSubscriptionCodec())

        val subscriptionTable = SubscriptionTable()
        val retainedMessages = RetainedMessages()

        val distributor = Distributor(subscriptionTable, retainedMessages, useKafka.isNotBlank(), useKafka)
        val servers = listOfNotNull(
            if (useTcp) MqttServer(usePort, useSsl, false, distributor) else null,
            if (useWs) MqttServer(usePort, useSsl, true, distributor) else null,
        )

        Future.succeededFuture<String>()
            .compose { vertx.deployVerticle(subscriptionTable) }
            .compose { vertx.deployVerticle(retainedMessages) }
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

    val builder = Vertx.builder()
    if (!useCluster) startMonster(builder.build())
    else clusterSetup(builder, ::startMonster)
}

