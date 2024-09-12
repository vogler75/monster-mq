package at.rocworks.data

import io.netty.handler.codec.mqtt.MqttQoS

class MqttClientQoS(
    val client: String,
    val qosLevel: MqttQoS
)