package at.rocworks.bus

import at.rocworks.data.BrokerMessage
import io.vertx.core.Future

interface IMessageBus {
    fun subscribeToMessageBus(callback: (BrokerMessage)->Unit): Future<Void>
    fun publishMessageToBus(message: BrokerMessage)
}