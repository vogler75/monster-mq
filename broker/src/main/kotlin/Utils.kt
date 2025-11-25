package at.rocworks

import com.github.f4b6a3.uuid.UuidCreator
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
// VertxInternal removed in Vert.x 5 - using alternative approaches
import io.vertx.core.shareddata.AsyncMap
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.time.Duration
import java.util.logging.LogManager
import java.util.logging.Logger

object Utils {
    fun toByteBuf(buffer: Buffer): ByteBuf = Unpooled.wrappedBuffer(buffer.bytes)
    fun toByteBuf(bytes: ByteArray): ByteBuf = Unpooled.wrappedBuffer(bytes)
    fun toByteBuf(s: String): ByteBuf = Unpooled.wrappedBuffer(s.toByteArray())

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

    fun <K,V> getMap(vertx: Vertx, name: String): Future<AsyncMap<K, V>> {
        val sharedData = vertx.sharedData()
        return if (vertx.isClustered) {
            sharedData.getClusterWideMap<K, V>(name)
                .onFailure { error ->
                    println("Failed to access the shared map [$name]: ${error}")
                }
        } else {
            sharedData.getAsyncMap<K, V>(name)
                .onFailure { error ->
                    println("Failed to access the shared map [$name]: ${error}")
                }
        }
    }

    fun parseDuration(duration: String?): Long? {
        if (duration == null || duration.isEmpty()) return null

        val regex = """(\d+)([smhdwMyk])""".toRegex()
        val match = regex.matchEntire(duration.trim()) ?: throw IllegalArgumentException("Invalid duration format: $duration")

        val value = match.groupValues[1].toLong()
        val unit = match.groupValues[2]

        return when (unit) {
            "s" -> Duration.ofSeconds(value).toMillis()
            "m" -> Duration.ofMinutes(value).toMillis()
            "h" -> Duration.ofHours(value).toMillis()
            "d" -> Duration.ofDays(value).toMillis()
            "w" -> Duration.ofDays(value * 7).toMillis()
            "M" -> Duration.ofDays(value * 30).toMillis()  // Approximate month as 30 days
            "y" -> Duration.ofDays(value * 365).toMillis()  // Approximate year as 365 days
            "k" -> value * 1000  // Size-based (k=thousand), return count directly (e.g., "50k" → 50000, "100k" → 100000)
            else -> throw IllegalArgumentException("Unknown duration unit: $unit")
        }
    }

    fun formatDuration(millis: Long): String {
        val duration = Duration.ofMillis(millis)
        return when {
            duration.toDays() > 0 -> "${duration.toDays()}d"
            duration.toHours() > 0 -> "${duration.toHours()}h"
            duration.toMinutes() > 0 -> "${duration.toMinutes()}m"
            else -> "${duration.toSeconds()}s"
        }
    }
}