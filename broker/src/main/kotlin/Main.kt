package at.rocworks
import at.rocworks.codecs.MqttPublishMessageCodec
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.mqtt.messages.impl.MqttPublishMessageImpl
import io.vertx.spi.cluster.hazelcast.ConfigUtil
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager


fun main(args: Array<String>) {
    MonsterServer.initLogging()

    val cluster = args.find { it == "-cluster" } != null
    val port = args.indexOf("-port").let { args.getOrNull(it+1) }?.toIntOrNull() ?: 1883
    val ssl = args.indexOf("-ssl") != -1

    println("Cluster: $cluster Port: $port SSL: $ssl")

    fun start(vertx: Vertx) {
        vertx.eventBus().registerDefaultCodec(MqttPublishMessageImpl::class.java, MqttPublishMessageCodec())
        Future.all((1..1).map {
            vertx.deployVerticle(Distributor())
        }).onComplete {
            vertx.deployVerticle(MonsterServer(port, ssl))
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

