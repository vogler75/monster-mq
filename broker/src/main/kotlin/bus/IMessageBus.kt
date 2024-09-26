package at.rocworks.bus

import at.rocworks.data.MqttMessage
import io.vertx.core.Future

interface IMessageBus {
    fun subscribeToMessageBus(callback: (MqttMessage)->Unit): Future<Void>
    fun publishMessageToBus(message: MqttMessage)
}