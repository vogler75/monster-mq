package at.rocworks
import at.rocworks.data.MqttTopicName
import at.rocworks.data.TopicTree
import at.rocworks.data.TopicTreeCache
import com.hazelcast.config.Config
import com.hazelcast.core.Hazelcast
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.ICacheManager
import io.vertx.core.AsyncResult
import io.vertx.core.Vertx
import io.vertx.core.VertxBuilder
import io.vertx.core.VertxOptions
import io.vertx.spi.cluster.hazelcast.ConfigUtil
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import javax.cache.Cache
import javax.cache.CacheManager
import javax.cache.Caching
import javax.cache.configuration.MutableConfiguration
import javax.cache.spi.CachingProvider


data class Node (
    val children: HashMap<String, String>,
    val dataset: MutableSet<String>
)

fun main() {
    // Step 1: Set up Vert.x with HazelcastClusterManager
    //val clusterManager = HazelcastClusterManager()
    //val vertxOptions = VertxOptions().setClusterManager(clusterManager)

    val hazelcastConfig = ConfigUtil.loadConfig()
    val clusterManager = HazelcastClusterManager(hazelcastConfig)

    val builder = Vertx.builder()
    builder.withClusterManager(clusterManager)
    builder.buildClustered().onComplete {  res ->
        if (res.succeeded()) {
            val vertx = res.result()
            //test1()
            test2()
        } else {
            println("Failed to start Vert.x: ${res.cause()}")
        }
    }
}

fun test1() {
    // Step 2: Use the existing Hazelcast instance to get the CacheManager
    val cachingProvider: CachingProvider = Caching.getCachingProvider()
    val cacheManager: CacheManager = cachingProvider.cacheManager

    // Step 3: Configure and create the cache using the Hazelcast instance
    val cacheConfig = MutableConfiguration<String, Node>()
        .setStatisticsEnabled(true)  // Enable statistics

    val cache: Cache<String, Node> = cacheManager.createCache("myCache", cacheConfig)

    // Step 4: Put and get values from the cache
    cache.put("key1", Node(hashMapOf("1" to "a", "2" to "b"), mutableSetOf("c1", "c2", "c3")))
    val value1 = cache.get("key1")
    println("Cache Value for key1: $value1")  // Output: Cache Value for key1: value1
    value1.dataset.clear()

    val value2 = cache.get("key1")
    println("Cache Value for key1: $value2")  // Output: Cache Value for key1: value1
}

private fun test2() {
    val tree = TopicTreeCache("test")
    val topics = listOf(
        "a/a/a",
        "a/a/b",
        "a/b/a",
        "a/b/b",
        "b/a/a",
        "b/b/a"
    )
    topics.forEach { tree.add(MqttTopicName(it)) }
    tree.add(MqttTopicName("a/a"), "test")
    tree.add(MqttTopicName("a/a"), "test2")
    println(tree)
    tree.del(MqttTopicName("a/a"), "test")
    tree.del(MqttTopicName("a/a"), "test2")
    println(tree)
    tree.del(MqttTopicName("a/a/a"))
    println(tree)
}