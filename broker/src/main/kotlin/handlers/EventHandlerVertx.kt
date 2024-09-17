package at.rocworks.handlers

import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import io.vertx.core.Promise

class EventHandlerVertx(
    sessionHandler: SessionHandler,
    messageHandler: MessageHandler
): EventHandler(sessionHandler, messageHandler) {
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