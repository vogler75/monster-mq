package at.rocworks.data

import java.io.Serializable

class MqttClientId(val identifier: String): Serializable {
    override fun toString() = identifier
}