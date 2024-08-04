package at.rocworks
import io.vertx.core.Vertx

fun main() {
    val vertx: Vertx = Vertx.vertx()
    vertx.deployVerticle(Monster(1883, false))
}

