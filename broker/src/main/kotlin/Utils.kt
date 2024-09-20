package at.rocworks

import com.github.f4b6a3.uuid.UuidCreator
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.impl.VertxInternal
import io.vertx.core.shareddata.AsyncMap
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.util.logging.LogManager
import java.util.logging.Logger

object Utils {
    fun toByteBuf(buffer: Buffer): ByteBuf = Unpooled.wrappedBuffer(buffer.bytes)
    fun toByteBuf(bytes: ByteArray): ByteBuf = Unpooled.wrappedBuffer(bytes)

    fun initLogging() {
        try {
            println("Loading logging.properties...")
            val initialFile = File("logging.properties")
            val targetStream: InputStream = FileInputStream(initialFile)
            LogManager.getLogManager().readConfiguration(targetStream)
        } catch (e: Exception) {
            try {
                println("Using default logging.properties...")
                val stream = this::class.java.classLoader.getResourceAsStream("logging.properties")
                LogManager.getLogManager().readConfiguration(stream)
            } catch (e: Exception) {
                println("Unable to read default logging.properties!")
            }
        }
    }

    fun getLogger(o: Class<*>, additionalName: String=""): Logger
    = Logger.getLogger(o.name.substringAfterLast(".") + if (additionalName.isEmpty()) "" else "/$additionalName")
    //= Logger.getLogger(o.name.removePrefix("at.rocworks.") + if (additionalName.isEmpty()) "" else "/$additionalName")

    fun getTopicLevels(topicName: String) = topicName.split("/")
    fun addTopicLevel(topicName: String, level: String) = "$topicName/$level"
    fun isWildCardTopic(topicName: String) = topicName.any { it == '#' || it == '+' } // TODO: check if this is faster then: topicName.contains("#") || topicName.contains("+")

    fun getCurrentFunctionName(): String = Thread.currentThread().stackTrace[2].methodName
    fun getUuid(): String = UuidCreator.getTimeOrdered().toString()

    fun getClusterNodeId(vertx: Vertx): String {
        return ((vertx as VertxInternal).clusterManager as HazelcastClusterManager).hazelcastInstance.cluster.localMember.uuid.toString()
    }

    fun <K,V> getMap(vertx: Vertx, name: String): Future<AsyncMap<K, V>> {
        val promise = Promise.promise<AsyncMap<K, V>>()
        val sharedData = vertx.sharedData()
        if (vertx.isClustered) {
            sharedData.getClusterWideMap<K, V>(name) {
                if (it.succeeded()) {
                    promise.complete(it.result())
                } else {
                    println("Failed to access the shared map [$name]: ${it.cause()}")
                    promise.fail(it.cause())
                }
            }
        } else {
            sharedData.getAsyncMap<K, V>(name) {
                if (it.succeeded()) {
                    promise.complete(it.result())
                } else {
                    println("Failed to access the shared map [$name]: ${it.cause()}")
                    promise.fail(it.cause())
                }
            }
        }
        return promise.future()
    }
}