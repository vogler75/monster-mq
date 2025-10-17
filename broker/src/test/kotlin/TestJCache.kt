package at.rocworks.tests
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import io.vertx.core.Vertx
import io.vertx.spi.cluster.hazelcast.ConfigUtil
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import javax.cache.Cache
import javax.cache.CacheManager
import javax.cache.Caching
import javax.cache.configuration.MutableConfiguration
import javax.cache.expiry.CreatedExpiryPolicy
import javax.cache.expiry.Duration
import javax.cache.spi.CachingProvider
import kotlin.concurrent.thread
import kotlin.system.exitProcess


data class Node (
    val children: HashMap<String, String>,
    val dataset: MutableSet<String>
)

fun main(args: Array<String>) {
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
            thread {
                if (args.indexOf("1")!=-1) test1()
                if (args.indexOf("3")!=-1) test3()
                if (args.indexOf("4")!=-1) test4(clusterManager.hazelcastInstance)
            }
        } else {
            println("Failed to start Vert.x: ${res.cause()}")
        }
    }
}

private fun test1() {
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

private fun test3() {
    println("test3")

    val cachingProvider: CachingProvider = Caching.getCachingProvider()
    val cacheManager: CacheManager = cachingProvider.cacheManager

    // Step 3: Configure and create the cache using the Hazelcast instance
    val cacheConfig = MutableConfiguration<String, Node>()
        .setExpiryPolicyFactory { CreatedExpiryPolicy(Duration.ETERNAL)}
        .setStatisticsEnabled(true)  // Enable statistics

    val cache: Cache<String, Node> = cacheManager.createCache("myCache", cacheConfig)

    println("Write cache...")
    repeat(100_000) { nr ->
        if (nr % 1000==0) println(nr)
        cache.put(nr.toString(), Node(
            children = hashMapOf("nr" to nr.toString()),
            dataset = mutableSetOf("1", "2", "3")
        )
        )
        if (cache.get(nr.toString())==null) {
            println("Error! $nr not found directly after put!")
            exitProcess(-1)
        }
        if (cache.get("0")==null) {
            println("Error! 0 not found directly after put!")
            exitProcess(-1)
        }
    }
    println("Read cache...")
    repeat(1_000_000) { nr ->
        if (cache.get(nr.toString())==null) {
            println("Error! $nr not found!")
            exitProcess(-1)
        }
    }
}

private fun test4(hazelcastInstance: HazelcastInstance) {
    val cache: IMap<String, Node> = hazelcastInstance.getMap("myMap")

    println("Write cache...")
    repeat(1_000_000) { nr ->
        if (nr % 1000==0) println(nr)
        cache.put(nr.toString(), Node(
            children = hashMapOf("nr" to nr.toString()),
            dataset = mutableSetOf("1", "2", "3")
        )
        )
        if (cache.get(nr.toString())==null) {
            println("Error! $nr not found directly after put!")
            exitProcess(-1)
        }
        if (cache.get("0")==null) {
            println("Error! 0 not found directly after put!")
            exitProcess(-1)
        }
    }
    println("Read cache...")
    repeat(1_000_000) { nr ->
        if (cache.get(nr.toString())==null) {
            println("Error! $nr not found!")
            exitProcess(-1)
        }
    }
    println("Done")
}

