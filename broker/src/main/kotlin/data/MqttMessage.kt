package at.rocworks.data

import at.rocworks.Utils
import com.github.f4b6a3.uuid.UuidCreator
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.Future
import io.vertx.core.buffer.Buffer
import io.vertx.mqtt.MqttEndpoint
import io.vertx.mqtt.MqttWill
import io.vertx.mqtt.messages.MqttPublishMessage
import java.io.Serializable
import java.time.Instant
import java.util.*

class MqttMessage(
    val messageUuid: String = Utils.getUuid(),
    val messageId: Int,
    val topicName: String,
    val payload: ByteArray,
    val qosLevel: Int,
    val isRetain: Boolean,
    val isDup: Boolean,
    val clientId: String,
    val time: Instant = Instant.now(),
    // TODO: Properties for MQTT 5.0
): Serializable {
    constructor(clientId: String, message: MqttPublishMessage): this(
        Utils.getUuid(),
        if (message.messageId()<0) 0 else message.messageId(),
        message.topicName(),
        message.payload().bytes,
        message.qosLevel().value(),
        message.isRetain,
        message.isDup,
        clientId
    )

    constructor(clientId: String, message: MqttWill): this(
        Utils.getUuid(),
        0,
        message.willTopic,
        message.willMessageBytes,
        message.willQos,
        message.isWillRetain,
        false,
        clientId
    )

    fun cloneWithNewQoS(qosLevel: Int): MqttMessage = MqttMessage(messageUuid, messageId, topicName, payload, qosLevel, isRetain, isDup, clientId)
    fun cloneWithNewMessageId(messageId: Int): MqttMessage = MqttMessage(messageUuid, messageId, topicName, payload, qosLevel, isRetain, isDup, clientId)

    private fun getPayloadAsBuffer(): Buffer = Buffer.buffer(payload)

    fun getPayloadAsJson(): String? {
        return try {
            val jsonString = String(payload)
            io.vertx.core.json.JsonObject(jsonString)
            jsonString
        } catch (e: Exception) {
            null
        }
    }

    fun getPayloadAsBase64(): String = Base64.getEncoder().encodeToString(payload)

    private fun getQoS(): MqttQoS = MqttQoS.valueOf(qosLevel)

    fun publish(endpoint: MqttEndpoint, qos: MqttQoS=getQoS()): Future<Int>
    = endpoint.publish(topicName, getPayloadAsBuffer(), qos, isDup, isRetain, messageId)

    companion object {
        fun getPayloadFromBase64(s: String): ByteArray = Base64.getDecoder().decode(s)
    }
}