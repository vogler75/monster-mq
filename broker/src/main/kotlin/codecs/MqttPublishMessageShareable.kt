package at.rocworks.codecs

import at.rocworks.Const
import io.netty.handler.codec.mqtt.MqttProperties
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.mqtt.messages.MqttPublishMessage
import io.vertx.mqtt.messages.impl.MqttPublishMessageImpl
import java.io.Serializable

class MqttPublishMessageShareable(message: MqttPublishMessage): Serializable {
    val messageId:Int  = message.messageId()
    val topicName: String = message.topicName()
    val payload: ByteArray = message.payload().bytes
    val qosLevel: Int = message.qosLevel().value()
    val isRetain: Boolean = message.isRetain
    val isDup: Boolean = message.isDup
    // TODO: Properties

    fun message() =
        MqttPublishMessageImpl(
            messageId,
            MqttQoS.valueOf(qosLevel),
            isDup,
            isRetain,
            topicName,
            Const.toByteBuf(payload),
            MqttProperties()
        )
}