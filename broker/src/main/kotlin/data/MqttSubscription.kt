package at.rocworks.data

import io.netty.handler.codec.mqtt.MqttQoS
import java.io.Serializable

data class MqttSubscription(
    val clientId: String,
    val topicName: String,
    val qos: MqttQoS,
    val noLocal: Boolean = false,       // MQTT v5: Don't send messages the client published itself
    val retainHandling: Int = 0,        // MQTT v5: 0=send retained, 1=send if new sub, 2=never send
    val retainAsPublished: Boolean = false  // MQTT v5: Preserve original retain flag (true) or clear it (false)
): Serializable
