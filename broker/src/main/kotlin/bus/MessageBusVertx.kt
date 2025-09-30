package at.rocworks.bus

import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx

class MessageBusVertx(): AbstractVerticle(), IMessageBus {
    private fun messageAddress() = EventBusAddresses.Node.messageBus(deploymentID())

    override fun start(startPromise: Promise<Void>) {
        startPromise.complete()
    }

    override fun publishMessageToBus(message: BrokerMessage) {
        vertx.eventBus().publish(messageAddress(), message)
    }

    override fun subscribeToMessageBus(callback: (BrokerMessage)->Unit): Future<Void> {
        vertx.eventBus().consumer<BrokerMessage>(messageAddress()) { message ->
            message.body()?.let { payload ->
                callback(payload)
            }
        }
        return Future.succeededFuture()
    }
}