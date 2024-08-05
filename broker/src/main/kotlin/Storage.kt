package at.rocworks

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.core.shareddata.AsyncMap
import io.vertx.core.shareddata.SharedData

import java.time.Instant

class Storage: AbstractVerticle() {
    private var topicTree: AsyncMap<String, String>? = null
    private var retainedMessages: AsyncMap<String, Buffer>? = null

    override fun start(startPromise: Promise<Void>) {
        val f1 = getMap<String, String>("TopicTree").onComplete {
            topicTree = it.result()
        }
        val f2 = getMap<String, Buffer>("RetainedMessages").onComplete {
            retainedMessages = it.result()
        }

        Future.all(f1, f2).onComplete {
            startPromise.complete()
        }
    }

    private fun <K,V> getMap(name: String): Future<AsyncMap<K, V>> {
        val promise = Promise.promise<AsyncMap<K, V>>()
        val sharedData = vertx.sharedData()
        if (vertx.isClustered) {
            sharedData.getClusterWideMap<K, V>("TopicMap") { it ->
                if (it.succeeded()) {
                    promise.complete(it.result())
                } else {
                    println("Failed to access the shared map [$name]: ${it.cause()}")
                    promise.fail(it.cause())
                }
            }
        } else {
            sharedData.getAsyncMap<K, V>("TopicMap") {
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
    
    fun testMap() {
        topicTree?.apply {
            // Get data from the shared map
            get("key") { getResult ->
                if (getResult.succeeded()) {
                    println("Value for 'key': ${getResult.result()}")
                } else {
                    println("Failed to get data: ${getResult.cause()}")
                }
            }

            val value = Instant.now().toEpochMilli().toString()
            println("New value $value")

            // Put data into the shared map
            put("key", value) { putResult ->
                if (putResult.succeeded()) {
                    println("Data added to the shared map")
                } else {
                    println("Failed to put data: ${putResult.cause()}")
                }
            }
        }
    }
}