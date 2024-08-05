package at.rocworks
import io.vertx.core.AsyncResult
import io.vertx.core.Vertx
import io.vertx.spi.cluster.hazelcast.ConfigUtil
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager


fun main(args: Array<String>) {
    Monster.initLogging()

    val cluster = args.find { it == "-cluster" } != null
    val port = args.indexOf("-port").let { args.getOrNull(it+1) }?.toIntOrNull() ?: 1883
    val ssl = args.indexOf("-ssl") != -1

    println("Cluster: $cluster Port: $port SSL: $ssl")

    val monster = Monster(port, ssl)
    val builder = Vertx.builder()
    if (cluster) {
        val hazelcastConfig = ConfigUtil.loadConfig()
        hazelcastConfig.setClusterName("MonsterMQ")
        builder.withClusterManager(HazelcastClusterManager(hazelcastConfig))
        builder.buildClustered().onComplete { res: AsyncResult<Vertx?> ->
            if (res.succeeded()) {
                res.result()?.apply {
                    deployVerticle(monster)
                }
            } else {
                println("Vertx building failed: ${res.cause()}")
            }
        }
    } else {
        builder.build().deployVerticle(monster)
    }
}

