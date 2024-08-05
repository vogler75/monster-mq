package at.rocworks
import io.vertx.core.Vertx
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.util.logging.LogManager

fun main() {
    val vertx: Vertx = Vertx.vertx()
    vertx.deployVerticle(Monster(1883, false))
}

