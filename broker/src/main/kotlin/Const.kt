package at.rocworks

import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.shareddata.AsyncMap

object Const {
    const val GLOBAL_DISTRIBUTOR_NAMESPACE = "D"
    const val GLOBAL_RETAINED_MESSAGES_NAMESPACE = "R"
    const val GLOBAL_SUBSCRIPTION_TABLE_NAMESPACE = "S"

    const val CLIENT_NAMESPACE = "C"

    const val TOPIC_KEY = "Topic"
    const val CLIENT_KEY = "Client"
    const val BROKER_KEY = "Broker"

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