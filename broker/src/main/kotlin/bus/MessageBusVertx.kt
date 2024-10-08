package at.rocworks.bus

import at.rocworks.Const
import at.rocworks.data.MqttMessage
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx

class MessageBusVertx(): AbstractVerticle(), IMessageBus {
    private fun messageAddress() = "${Const.GLOBAL_EVENT_NAMESPACE}/${deploymentID()}/M"

    override fun start(startPromise: Promise<Void>) {
        startPromise.complete()
    }

    override fun publishMessageToBus(message: MqttMessage) {
        vertx.eventBus().publish(messageAddress(), message)
    }

    override fun subscribeToMessageBus(callback: (MqttMessage)->Unit): Future<Void> {
        vertx.eventBus().consumer<MqttMessage>(messageAddress()) { message ->
            message.body()?.let { payload ->
                callback(payload)
            }
        }
        return Future.succeededFuture()
    }
}