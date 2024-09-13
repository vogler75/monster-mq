package at.rocworks.data

import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.Future
import io.vertx.core.buffer.Buffer
import io.vertx.mqtt.MqttEndpoint
import io.vertx.mqtt.MqttWill
import io.vertx.mqtt.messages.MqttPublishMessage
import java.io.Serializable

class MqttMessage(
    val messageId: Int,
    val topicName: String,
    val payload: ByteArray,
    val qosLevel: Int,
    val isRetain: Boolean,
    val isDup: Boolean,
    // TODO: Properties for MQTT 5.0
): Serializable {
    constructor(message: MqttPublishMessage): this(
        if (message.messageId()<0) 0 else message.messageId(),
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

    fun cloneWithNewQoS(qosLevel: Int): MqttMessage = MqttMessage(messageId, topicName, payload, qosLevel, isRetain, isDup)
    fun cloneWithNewMessageId(messageId: Int): MqttMessage = MqttMessage(messageId, topicName, payload, qosLevel, isRetain, isDup)

    private fun getPayloadAsBuffer(): Buffer = Buffer.buffer(payload)

    private fun getQoS(): MqttQoS = MqttQoS.valueOf(qosLevel)

    fun publish(endpoint: MqttEndpoint, qos: MqttQoS=getQoS()): Future<Int>
    = endpoint.publish(topicName, getPayloadAsBuffer(), qos, isDup, isRetain, messageId)
}