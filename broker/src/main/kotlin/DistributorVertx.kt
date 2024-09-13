package at.rocworks

import at.rocworks.data.MqttMessage
import at.rocworks.stores.MessageHandler
import at.rocworks.stores.SessionHandler
import io.vertx.core.Promise
import java.util.logging.Logger

class DistributorVertx(
    sessionHandler: SessionHandler,
    messageHandler: MessageHandler
): Distributor(sessionHandler, messageHandler) {
    private val logger = Utils.getLogger(this::class.java)

    override fun start(startPromise: Promise<Void>?) {
        super.start(startPromise)
        vertx.eventBus().consumer<MqttMessage>(getDistributorMessageAddress()) { message ->
            message.body()?.let { payload ->
                logger.finest { "Received message [${payload.topicName}] retained [${payload.isRetain}] [${Utils.getCurrentFunctionName()}]" }
                consumeMessageFromBus(payload)
            }
        }
    }
    override fun publishMessageToBus(message: MqttMessage) {
        vertx.eventBus().publish(getDistributorMessageAddress(), message)
    }
}