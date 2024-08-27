package at.rocworks

//import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager
//import io.vertx.ext.cluster.infinispan.InfinispanClusterManager
//import io.vertx.spi.cluster.ignite.IgniteClusterManager
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager

import at.rocworks.data.*
import at.rocworks.shared.RetainedMessages
import at.rocworks.shared.SubscriptionTable
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
    val useKafka = args.indexOf("-kafka").let { if (it != -1) args.getOrNull(it+1)?:"" else "" }

    args.indexOf("-log").let {
        if (it != -1) {
            val level = Level.parse(args[it + 1])
            println("Log Level [$level]")
            Const.DEBUG_LEVEL = level
        }
    }

    logger.info("Cluster: $useCluster Port: $usePort SSL: $useSsl Websockets: $useWs Kafka: $useKafka")

    fun startMonster(vertx: Vertx, retainedIndex: TopicTree, retainedStore: MutableMap<String, MqttMessage>) {
        vertx.eventBus().registerDefaultCodec(MqttMessage::class.java, MqttMessageCodec())
        vertx.eventBus().registerDefaultCodec(MqttTopicName::class.java, MqttTopicNameCodec())
        vertx.eventBus().registerDefaultCodec(MqttSubscription::class.java, MqttSubscriptionCodec())

        val subscriptionTable = SubscriptionTable()
        val retainedMessages = RetainedMessages(retainedIndex, retainedStore)

        val distributor = Distributor(
            subscriptionTable,
            retainedMessages,
            useKafka.isNotBlank(),
            useKafka
        )
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

    fun localSetup(builder: VertxBuilder) {
        val retainedIndex = TopicTreeLocal()
        val retainedStore = mutableMapOf<String, MqttMessage>()
        startMonster(builder.build(), retainedIndex, retainedStore)
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
                val index = TopicTreeHazelcast(hz, "Retained-Index")
                val store: MutableMap<String, MqttMessage> = hz.getMap("Retained-Store")
                startMonster(vertx, index, store)
            } else {
                logger.severe("Vertx building failed: ${res.cause()}")
            }
        }
    }

    val builder = Vertx.builder()
    if (useCluster) clusterSetup(builder)
    else localSetup(builder)
}

