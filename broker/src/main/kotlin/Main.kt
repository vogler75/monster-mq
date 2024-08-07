package at.rocworks
import at.rocworks.codecs.MqttMessageCodec
import at.rocworks.codecs.MqttMessage
import io.vertx.core.AsyncResult
import io.vertx.core.Vertx
import io.vertx.spi.cluster.hazelcast.ConfigUtil
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager

fun main(args: Array<String>) {
    MonsterServer.initLogging()

    val cluster = args.find { it == "-cluster" } != null
    val port = args.indexOf("-port").let { args.getOrNull(it+1) }?.toIntOrNull() ?: 1883
    val ssl = args.indexOf("-ssl") != -1

    println("Cluster: $cluster Port: $port SSL: $ssl")

    fun start(vertx: Vertx) {
        vertx.eventBus().registerDefaultCodec(MqttMessage::class.java, MqttMessageCodec())
        val distributor = Distributor()
        vertx.deployVerticle(distributor).onComplete{
            if (it.succeeded()) vertx.deployVerticle(MonsterServer(port, ssl, distributor))
            else println("Error in deploying distributor: ${it.cause()}")
        }
    }

    val builder = Vertx.builder()
    if (!cluster) start(builder.build())
    else {
        val hazelcastConfig = ConfigUtil.loadConfig()
        hazelcastConfig.setClusterName("MonsterMQ")
        builder.withClusterManager(HazelcastClusterManager(hazelcastConfig))
        builder.buildClustered().onComplete { res: AsyncResult<Vertx?> ->
            if (res.succeeded() && res.result() != null) {
                start(res.result()!!)
            } else {
                println("Vertx building failed: ${res.cause()}")
            }
        }
    }
}

