package at.rocworks.data

import at.rocworks.Utils
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.Future
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.Json
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
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
    val isQueued: Boolean,
    val clientId: String,
    val time: Instant = Instant.now(),
    val sender: String? = null,  // Optional sender identification for loop prevention
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
        false,
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
        false,
        clientId
    )

    constructor(clientId: String, topic: String, payload: String) : this(
        Utils.getUuid(),
        0,
        topic,
        payload.toByteArray(),
        0,
        false,
        false,
        false,
        clientId
    )

    fun cloneWithNewQoS(qosLevel: Int): MqttMessage = MqttMessage(messageUuid, messageId, topicName, payload, qosLevel, isRetain, isDup, isQueued, clientId, time, sender)
    fun cloneWithNewMessageId(messageId: Int): MqttMessage = MqttMessage(messageUuid, messageId, topicName, payload, qosLevel, isRetain, isDup, isQueued, clientId, time, sender)

    private fun getPayloadAsBuffer(): Buffer = Buffer.buffer(payload)

    fun getPayloadAsJson(): String? {
        return try {
            val jsonString = String(payload)
            Json.decodeValue(jsonString) // Check if it is a valid JSON
            jsonString
        } catch (e: Exception) {
            null
        }
    }

    fun getPayloadAsBase64(): String = Base64.getEncoder().encodeToString(payload)

    private fun getQoS(): MqttQoS = MqttQoS.valueOf(qosLevel)

    fun publishToEndpoint(endpoint: MqttEndpoint, qos: MqttQoS=getQoS()): Future<Int>
    = endpoint.publish(topicName, getPayloadAsBuffer(), qos, isDup, isRetain, messageId)

    companion object {
        fun getPayloadFromBase64(s: String): ByteArray = Base64.getDecoder().decode(s)
    }
}