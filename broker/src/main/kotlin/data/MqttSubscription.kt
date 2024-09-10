package at.rocworks.data

import io.netty.handler.codec.mqtt.MqttQoS
import java.io.Serializable

class MqttSubscription(
    val clientId: String,
    val topicName: String,
    val qos: MqttQoS
): Serializable
