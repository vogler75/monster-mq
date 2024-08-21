package at.rocworks

//import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager
//import io.vertx.ext.cluster.infinispan.InfinispanClusterManager
//import io.vertx.spi.cluster.ignite.IgniteClusterManager

import at.rocworks.data.*
import at.rocworks.shared.RetainedMessages
import at.rocworks.shared.SubscriptionTable
import io.vertx.core.AsyncResult
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.spi.cluster.hazelcast.ConfigUtil
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import java.util.logging.Logger
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    Utils.initLogging()

    val logger = Logger.getLogger("Main")

    val cluster = args.find { it == "-cluster" } != null
    val port = args.indexOf("-port").let { args.getOrNull(it+1) }?.toIntOrNull() ?: 1883
    val ssl = args.indexOf("-ssl") != -1
    val ws = args.indexOf("-ws") != -1

    logger.info("Cluster: $cluster Port: $port SSL: $ssl Websockets: $ws")

    fun start(vertx: Vertx) {
        vertx.eventBus().registerDefaultCodec(MqttMessage::class.java, MqttMessageCodec())
        vertx.eventBus().registerDefaultCodec(MqttTopicName::class.java, MqttTopicNameCodec())
        vertx.eventBus().registerDefaultCodec(MqttSubscription::class.java, MqttSubscriptionCodec())

        val subscriptionTable = SubscriptionTable()
        val retainedMessages = RetainedMessages()
        val distributor = Distributor(subscriptionTable, retainedMessages)
        val servers = List(5) { MqttServer(port, ssl, ws, distributor) }

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
    if (!cluster) start(builder.build())
    else {
        val hazelcastConfig = ConfigUtil.loadConfig()
        hazelcastConfig.setClusterName("MonsterMQ")
        val clusterManager = HazelcastClusterManager(hazelcastConfig)

        //val clusterManager = ZookeeperClusterManager()
        //val clusterManager = InfinispanClusterManager()
        //val clusterManager = IgniteClusterManager();

        builder.withClusterManager(clusterManager)
        builder.buildClustered().onComplete { res: AsyncResult<Vertx?> ->
            if (res.succeeded() && res.result() != null) {
                start(res.result()!!)
            } else {
                logger.severe("Vertx building failed: ${res.cause()}")
            }
        }
    }
}

