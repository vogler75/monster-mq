package at.rocworks.stores

import at.rocworks.data.MqttMessage
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.pgclient.PgBuilder
import io.vertx.sqlclient.Pool

import io.vertx.sqlclient.Tuple
import java.util.logging.Logger


class MessageStorePostgres(
    private val name: String,
    private val connectionUri: String
): AbstractVerticle(), IMessageStore {
    private val logger = Logger.getLogger(this.javaClass.simpleName+"/"+name)

    private val pool: Pool = PgBuilder.pool()
        .connectingTo(connectionUri)
        .using(vertx)
        .build()

    override fun start(startPromise: Promise<Void>) {
        startPromise.complete()
    }
    override fun get(topic: String): MqttMessage? {
        TODO("Not yet implemented")
    }

    override fun addAll(messages: List<MqttMessage>) {
        logger.info("addAll ${messages.size}")
        val batch: MutableList<Tuple> = ArrayList()
        messages.forEach { message ->
            logger.info("Row ${message.topicName}")
            val levels = message.topicName.split("/")
            batch.add(Tuple.of(message.topicName, levels, message.payload))
        }
        pool.connection.onSuccess { client ->
            logger.info("addAll ${messages.size}")

            client.preparedQuery("INSERT INTO retained (topic, levels, payload) VALUES ($1, $2, $3)")
                .executeBatch(batch).onComplete { res ->
                    if (res.succeeded()) {
                        logger.info("complete")
                    } else {
                        logger.severe("Batch failed " + res.cause())
                    }
                }
        }.onFailure {
            it.printStackTrace()
        }
    }

    override fun delAll(messages: List<MqttMessage>) {
        TODO("Not yet implemented")
    }

    override fun findMatchingTopicNames(topicName: String, callback: (String) -> Boolean) {
        TODO("Not yet implemented")
    }


}