package at.rocworks.data

import io.netty.handler.codec.mqtt.MqttQoS
import java.io.Serializable

data class MqttClientQoS( // use data class for correct hashcode and equals
    val client: String,
    val qosLevel: MqttQoS
): Serializable {
    override fun hashCode(): Int = client.hashCode() // our primary key is the clientId
    override fun equals(other: Any?): Boolean = other is MqttClientQoS && other.client == client // our primary key is the clientId
}