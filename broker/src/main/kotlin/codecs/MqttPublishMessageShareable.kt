package at.rocworks.codecs

import at.rocworks.Const
import io.netty.handler.codec.mqtt.MqttProperties
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.Future
import io.vertx.core.buffer.Buffer
import io.vertx.mqtt.MqttEndpoint
import io.vertx.mqtt.MqttWill
import io.vertx.mqtt.messages.MqttPublishMessage
import io.vertx.mqtt.messages.impl.MqttPublishMessageImpl
import java.io.Serializable

class MqttPublishMessageShareable(
    val messageId: Int,
    val topicName: String,
    val payload: ByteArray,
    val qosLevel: Int,
    val isRetain: Boolean,
    val isDup: Boolean,
    // TODO: Properties
): Serializable {
    constructor(message: MqttPublishMessage): this(
        message.messageId(),
        message.topicName(),
        message.payload().bytes,
        message.qosLevel().value(),
        message.isRetain,
        message.isDup
    )

    constructor(message: MqttWill): this(
        0,
        message.willTopic,
        message.willMessageBytes,
        message.willQos,
        message.isWillRetain,
        false
    )

    private fun getPayloadAsBuffer(): Buffer = Buffer.buffer(payload)

    private fun getQoS(): MqttQoS = MqttQoS.valueOf(qosLevel)

    fun publish(endpoint: MqttEndpoint): Future<Int> = endpoint.publish(topicName, getPayloadAsBuffer(), getQoS(), isDup, isRetain)

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