package at.rocworks.codecs

import java.io.Serializable

data class MqttClientId(val identifier: String): Serializable {
    override fun toString() = identifier
}