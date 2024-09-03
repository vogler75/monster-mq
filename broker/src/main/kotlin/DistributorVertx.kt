package at.rocworks

import at.rocworks.data.MqttMessage
import at.rocworks.stores.RetainedMessageHandler
import at.rocworks.stores.SubscriptionTableAsyncMap
import io.vertx.core.Promise
import java.util.logging.Logger

class DistributorVertx(
    subscriptionTable: SubscriptionTableAsyncMap,
    retainedMessageHandler: RetainedMessageHandler
): Distributor(subscriptionTable, retainedMessageHandler) {
    private val logger = Logger.getLogger(this.javaClass.simpleName)

    override fun start(startPromise: Promise<Void>?) {
        super.start(startPromise)
        vertx.eventBus().consumer<MqttMessage>(getDistributorMessageAddress()) { message ->
            message.body()?.let { payload ->
                logger.finest { "Received message [${payload.topicName}] retained [${payload.isRetain}]" }
                consumeMessageFromBus(payload)
            }
        }
    }
    override fun publishMessageToBus(message: MqttMessage) {
        vertx.eventBus().publish(getDistributorMessageAddress(), message)
    }
}